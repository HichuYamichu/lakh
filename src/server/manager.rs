use futures::{Stream, StreamExt};
use nanoid::nanoid;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::sync::{mpsc, Mutex};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use tracing::{instrument, warn};
use tracing_futures::Instrument;

use crate::executor::{Executor, ExecutorCtl, ExecutorHandle};
use crate::pb::lakh_server::Lakh;
use crate::pb::{DeadJobs, Job, JobResult};
use crate::worker::Worker;
use crate::Config;

#[derive(Debug)]
pub struct Manager {
    exec_handles: Mutex<HashMap<String, ExecutorHandle>>,
    exec_spawner: Executor,
}

impl Manager {
    pub fn new(config: Config) -> Self {
        Self {
            exec_handles: Mutex::new(HashMap::new()),
            exec_spawner: Executor::new(config.max_retry),
        }
    }
}

#[tonic::async_trait]
impl Lakh for Manager {
    #[instrument(name = "producer", err)]
    async fn work(&self, request: Request<tonic::Streaming<Job>>) -> Result<Response<()>, Status> {
        let job_names = parse_job_names(request.metadata())?;
        let mut executors = HashMap::with_capacity(job_names.len());
        let mut guarded_handles = self.exec_handles.lock().await;

        for job_name in job_names {
            let exec = guarded_handles
                .entry(job_name.clone())
                .or_insert_with(|| self.exec_spawner.spawn(job_name.clone()))
                .clone();
            executors.insert(job_name, exec);
        }
        drop(guarded_handles);

        let mut job_stream = request.into_inner();
        while let Some(job) = job_stream.next().await {
            let job = job?;
            match executors.get_mut(&job.name) {
                Some(exec) => exec.send(ExecutorCtl::WorkOn(job)).await.unwrap(),
                None => warn!(
                    message = "unknown job requested",
                    job_name = %(&job.name),
                    job_id = %(&job.id)
                ),
            }
        }

        Ok(Response::new(()))
    }

    type JoinStream = Pin<Box<dyn Stream<Item = Result<Job, Status>> + Send + Sync + 'static>>;

    #[instrument(name = "consumer", err)]
    async fn join(
        &self,
        job_result: Request<tonic::Streaming<JobResult>>,
    ) -> Result<Response<Self::JoinStream>, Status> {
        let job_names = parse_job_names(job_result.metadata())?;
        let (tx, rx) = mpsc::channel(10);
        let w = Worker::new(nanoid!(), tx);
        let mut executors = HashMap::with_capacity(job_names.len());
        let mut guarded_handles = self.exec_handles.lock().await;

        for job_name in job_names {
            let mut exec = guarded_handles
                .entry(job_name.clone())
                .or_insert_with(|| self.exec_spawner.spawn(job_name.clone()))
                .clone();
            exec.send(ExecutorCtl::AddWorker(w.clone())).await.unwrap();
            executors.insert(job_name.to_owned(), exec);
        }

        let result_handler = async move {
            let mut result_stream = job_result.into_inner();
            while let Some(job_result) = result_stream.next().await {
                let job_result = match job_result {
                    Ok(j) => j,
                    Err(_) => break,
                };

                match executors.get_mut(&job_result.job_name) {
                    Some(exec) => exec
                        .send(ExecutorCtl::HandleJobResult(job_result))
                        .await
                        .unwrap(),
                    None => warn!(
                        message = "got unknown job result",
                        job_name = %(&job_result.job_name),
                        job_id = %(&job_result.job_id)
                    ),
                }
            }
        };
        tokio::spawn(result_handler.in_current_span());

        Ok(Response::new(Box::pin(rx) as Self::JoinStream))
    }

    async fn get_dead_jobs(&self, _req: Request<()>) -> Result<Response<DeadJobs>, Status> {
        let (tx, mut rx) = mpsc::channel(5);
        let mut handles = self.exec_handles.lock().await;

        for (_, exec) in handles.iter_mut() {
            exec.send(ExecutorCtl::ReportDeadJobs(tx.clone()))
                .await
                .unwrap();
        }

        let mut jobs = Vec::new();
        for _ in 0..handles.len() {
            let v = rx.recv().await.unwrap();
            jobs.extend(v);
        }

        let res = Response::new(DeadJobs { jobs });
        Ok(res)
    }
}

fn parse_job_names(meta: &MetadataMap) -> Result<Vec<String>, Status> {
    let res = meta
        .get("job_names")
        .ok_or_else(|| Status::invalid_argument("missing `job_names` field in metadata map"))?
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid ASCII in `job_names` field"))?
        .split(';')
        .map(|s| s.to_owned())
        .collect();
    Ok(res)
}
