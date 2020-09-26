use futures::{Stream, StreamExt};
use nanoid::nanoid;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::sync::{mpsc, Mutex};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::executor::{Executor, ExecutorCtl};
use crate::pb::lakh_server::Lakh;
use crate::pb::{Job, JobResult, JobStatus};
use crate::worker::Worker;

pub struct Manager {
    executors: Mutex<HashMap<String, Executor>>,
}

impl Manager {
    pub fn new() -> Self {
        Self {
            executors: Mutex::new(HashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl Lakh for Manager {
    async fn work(&self, request: Request<tonic::Streaming<Job>>) -> Result<Response<()>, Status> {
        let job_names = parse_job_names(request.metadata());
        let job_names = match job_names {
            Ok(names) => names,
            Err(e) => return Err(Status::invalid_argument(e)),
        };

        let mut executors = HashMap::with_capacity(job_names.len());
        let mut guarded_execs = self.executors.lock().await;

        for job_name in job_names {
            let exec = guarded_execs
                .entry(job_name.clone())
                .or_insert_with(Executor::new)
                .clone();
            executors.insert(job_name, exec);
        }
        drop(guarded_execs);

        let mut job_stream = request.into_inner();
        while let Some(job) = job_stream.next().await {
            let job = job?;
            if let Some(exec) = executors.get_mut(&job.name) {
                exec.send(ExecutorCtl::WorkOn(job)).await.unwrap();
            }
        }

        Ok(Response::new(()))
    }

    type JoinStream = Pin<Box<dyn Stream<Item = Result<Job, Status>> + Send + Sync + 'static>>;

    async fn join(
        &self,
        job_result: Request<tonic::Streaming<JobResult>>,
    ) -> Result<Response<Self::JoinStream>, Status> {
        let job_names = parse_job_names(job_result.metadata());
        let job_names = match job_names {
            Ok(names) => names,
            Err(e) => return Err(Status::invalid_argument(e)),
        };

        let (tx, rx) = mpsc::channel(10);
        let w = Worker::new(nanoid!(), tx);
        let mut executors = HashMap::with_capacity(job_names.len());
        let mut guarded_execs = self.executors.lock().await;

        for job_name in job_names {
            let mut exec = guarded_execs
                .entry(job_name.clone())
                .or_insert_with(Executor::new)
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

                if let Some(exec) = executors.get_mut(&job_result.job_name) {
                    match JobStatus::from_i32(job_result.status).unwrap() {
                        JobStatus::Failed => exec
                            .send(ExecutorCtl::HandleJobFaliure(job_result.job_id))
                            .await
                            .unwrap(),
                        JobStatus::Succeeded => exec
                            .send(ExecutorCtl::HandleJobSuccess(job_result.job_id))
                            .await
                            .unwrap(),
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(rx) as Self::JoinStream))
    }
}

#[derive(Debug)]
enum ParseJobNamesError {
    MissingField,
    InvalidASCII,
}

impl std::fmt::Display for ParseJobNamesError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParseJobNamesError::MissingField => {
                write!(f, "missing `job_names` field in metadata map")
            }
            ParseJobNamesError::InvalidASCII => write!(f, "invalid ASCII in `job_names` field"),
        }
    }
}

impl Into<String> for ParseJobNamesError {
    fn into(self) -> String {
        self.to_string()
    }
}

fn parse_job_names(meta: &MetadataMap) -> Result<Vec<String>, ParseJobNamesError> {
    let res = meta
        .get("job_names")
        .ok_or(ParseJobNamesError::MissingField)?
        .to_str()
        .map_err(|_| ParseJobNamesError::InvalidASCII)?
        .split(';')
        .map(|s| s.to_owned())
        .collect();
    Ok(res)
}
