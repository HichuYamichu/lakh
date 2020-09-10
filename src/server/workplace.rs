use futures::{Stream, StreamExt};
use nanoid::nanoid;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::sync::{mpsc, Mutex};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::pb::workplace_server::Workplace;
use crate::pb::{Job, JobResult};

use crate::archive::{Archive, ArchiveCtl};
use crate::executor::{Executor, ExecutorCtl};
use crate::worker::Worker;

struct Department {
    executor: Executor,
    archive: Archive,
}

impl Department {
    fn new() -> Self {
        let (executor_tx, executor_rx) = mpsc::channel(10);
        let (archive_tx, archive_rx) = mpsc::channel(10);
        let e = Executor::new(executor_rx, archive_tx);
        let a = Archive::new(archive_rx, executor_tx);
        Department {
            executor: e,
            archive: a,
        }
    }
}

pub struct LakhWorkplace {
    departments: Mutex<HashMap<String, Department>>,
}

impl LakhWorkplace {
    pub fn new() -> Self {
        Self {
            departments: Mutex::new(HashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl Workplace for LakhWorkplace {
    async fn work(&self, request: Request<tonic::Streaming<Job>>) -> Result<Response<()>, Status> {
        let job_names = parse_job_names(request.metadata());
        let mut offices = HashMap::with_capacity(job_names.len());
        let mut guarded_deps = self.departments.lock().await;

        for job_name in job_names {
            let exec = guarded_deps
                .entry(job_name.clone())
                .or_insert(Department::new())
                .executor
                .clone();
            offices.insert(job_name, exec);
        }
        drop(guarded_deps);

        let mut job_stream = request.into_inner();
        while let Some(job) = job_stream.next().await {
            let job = job?;
            let office = offices.get_mut(&job.name).unwrap();
            office.send(ExecutorCtl::WorkOn(job)).await.unwrap();
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
        let mut guarded_deps = self.departments.lock().await;
        let mut archives = HashMap::with_capacity(job_names.len());

        for job_name in job_names {
            let dep = guarded_deps
                .entry(job_name.to_owned())
                .or_insert(Department::new());
            dep.executor
                .send(ExecutorCtl::HireWorker(w.clone()))
                .await
                .unwrap();
            archives.insert(job_name, dep.archive.clone());
        }

        tokio::spawn(async move {
            let mut result_stream = job_result.into_inner();
            while let Some(job_result) = result_stream.next().await {
                let job_result = match job_result {
                    Ok(j) => j,
                    Err(_) => break,
                };

                let job = match job_result.job {
                    Some(ref j) => j,
                    None => todo!(),
                };

                archives
                    .get_mut(&job.name)
                    .unwrap()
                    .send(ArchiveCtl::AddResult(job_result))
                    .await
                    .unwrap();
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
