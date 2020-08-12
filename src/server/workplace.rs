use dashmap::DashMap;
use nanoid::nanoid;
use tonic::{Request, Response, Status};

mod archive;
mod department;
mod worker;

use department::*;

use crate::pb::worker_client::WorkerClient;
use crate::pb::workplace_server::Workplace;
use crate::pb::{Job, Jobs, ReportRequest, Void, WorkerInfo};

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
    async fn assign(&self, request: Request<Job>) -> Result<Response<Void>, Status> {
        let job = request.into_inner();

        self.departments
            .entry(job.name.clone())
            .or_insert(Department::new())
            .send(DepartmentMail::WorkOn(job))
            .await
            .unwrap();

        Ok(Response::new(Void {}))
    }

    async fn join(&self, request: Request<WorkerInfo>) -> Result<Response<Void>, Status> {
        let addr = request
            .remote_addr()
            .ok_or_else(|| Status::unimplemented("could not get remote addr"))?;

        let worker_info = request.into_inner();
        let worker = WorkerClient::connect(addr.to_string())
            .await
            .map_err(|_| Status::failed_precondition("could not connect to worker"))?;

        let id = nanoid!();
        for job_name in worker_info.available_jobs {
            self.departments
                .entry(job_name)
                .or_insert(Department::new())
                .send(DepartmentMail::HireWorker(worker.clone(), id.clone()))
                .await
                .unwrap();
        }

        Ok(Response::new(Void {}))
    }

    async fn report(&self, request: Request<ReportRequest>) -> Result<Response<Jobs>, Status> {
        let report_request = request.into_inner();
        // let (tx, mut rx) = mpsc::channel(100);

        // let to_collect = report_request
        //     .job_names
        //     .iter()
        //     .map(|k| self.departments.get(k))
        //     .filter_map(|v| v)
        //     .map(|dep| dep.send(DepartmentMail::ReportSucceeded(tx)).await.unwrap());
        // .send(DepartmentMail::ReportSucceeded(tx));

        // Ok(Response::new(Jobs {}))
        unimplemented!()
    }
}
