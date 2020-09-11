use futures::{Stream, StreamExt};
use nanoid::nanoid;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::sync::{mpsc, Mutex};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::pb::workplace_server::Workplace;
use crate::pb::{Job, JobResult, JobStatus};

use crate::executor::{Executor, ExecutorCtl};
use crate::worker::Worker;

pub struct LakhWorkplace {
    executors: Mutex<HashMap<String, Executor>>,
}

impl LakhWorkplace {
    pub fn new() -> Self {
        Self {
            executors: Mutex::new(HashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl Workplace for LakhWorkplace {
    async fn work(&self, request: Request<tonic::Streaming<Job>>) -> Result<Response<()>, Status> {
        let job_names = parse_job_names(request.metadata());
        let mut executors = HashMap::with_capacity(job_names.len());
        let mut guarded_execs = self.executors.lock().await;

        for job_name in job_names {
            let exec = guarded_execs
                .entry(job_name.clone())
                .or_insert(Executor::new())
                .clone();
            executors.insert(job_name, exec);
        }
        drop(guarded_execs);

        let mut job_stream = request.into_inner();
        while let Some(job) = job_stream.next().await {
            let job = job?;
            let executor = executors.get_mut(&job.name).unwrap();
            executor.send(ExecutorCtl::WorkOn(job)).await.unwrap();
        }

        Ok(Response::new(()))
    }

    type JoinStream = Pin<Box<dyn Stream<Item = Result<Job, Status>> + Send + Sync + 'static>>;

    async fn join(
        &self,
        job_result: Request<tonic::Streaming<JobResult>>,
    ) -> Result<Response<Self::JoinStream>, Status> {
        let (tx, rx) = mpsc::channel(10);
        let w = Worker::new(nanoid!(), tx);

        let job_names = parse_job_names(job_result.metadata());
        let mut guarded_execs = self.executors.lock().await;
        let mut executors = HashMap::with_capacity(job_names.len());

        for job_name in job_names {
            let mut exec = guarded_execs
                .entry(job_name.clone())
                .or_insert(Executor::new())
                .clone();
            exec.send(ExecutorCtl::AddWorker(w.clone())).await.unwrap();
            executors.insert(job_name, exec);
        }

        tokio::spawn(async move {
            let mut result_stream = job_result.into_inner();
            while let Some(job_result) = result_stream.next().await {
                let job_result = match job_result {
                    Ok(j) => j,
                    Err(_) => break,
                };

                let exec = executors.get_mut(&job_result.job_name).unwrap();
                match JobStatus::from_i32(job_result.status).unwrap() {
                    JobStatus::Failed => exec
                        .send(ExecutorCtl::HandleJobFaliure(job_result.id))
                        .await
                        .unwrap(),
                    JobStatus::Succeeded => exec
                        .send(ExecutorCtl::EvictJob(job_result.id))
                        .await
                        .unwrap(),
                }
            }
        });

        Ok(Response::new(Box::pin(rx) as Self::JoinStream))
    }
}

fn parse_job_names(meta: &MetadataMap) -> Vec<String> {
    meta.get("job_names")
        .unwrap()
        .to_str()
        .unwrap()
        .split(";")
        .map(|s| s.to_owned())
        .collect()
}
