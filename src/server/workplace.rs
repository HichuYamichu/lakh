use dashmap::DashMap;
use futures::Stream;
use nanoid::nanoid;
use std::pin::Pin;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::pb::workplace_server::Workplace;
use crate::pb::{Job, JobResult, Void};

use crate::department::*;
use crate::worker::Worker;

pub struct LakhWorkplace {
    departments: DashMap<String, Department>,
}

impl LakhWorkplace {
    pub fn new() -> Self {
        Self {
            departments: DashMap::new(),
        }
    }
}

#[tonic::async_trait]
impl Workplace for LakhWorkplace {
    async fn send(&self, request: Request<Job>) -> Result<Response<Void>, Status> {
        let job = request.into_inner();

        self.departments
            .entry(job.name.clone())
            .or_insert(Department::new())
            .send(DepartmentMail::WorkOn(job))
            .await
            .unwrap();

        Ok(Response::new(Void {}))
    }

    type WorkStream = Pin<Box<dyn Stream<Item = Result<Job, Status>> + Send + Sync + 'static>>;

    // sdsadada
    async fn work(
        &self,
        job_result: Request<tonic::Streaming<JobResult>>,
    ) -> Result<Response<Self::WorkStream>, Status> {
        let (tx, rx) = mpsc::channel(10);

        let job_name = job_result
            .metadata()
            .get("job_name")
            .unwrap()
            .to_str()
            .unwrap();

        let w = Worker::new(nanoid!(), tx);

        self.departments
            .entry(job_name.to_owned())
            .or_insert(Department::new())
            .send(DepartmentMail::HireWorker(w, job_result.into_inner()))
            .await
            .unwrap();

        Ok(Response::new(Box::pin(rx) as Self::WorkStream))
    }
}
