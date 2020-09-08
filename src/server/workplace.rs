use futures::{Stream, StreamExt};
use nanoid::nanoid;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::pb::workplace_server::Workplace;
use crate::pb::{Job, JobResult, Void};

use crate::archive::{Archive, ArchiveMail};
use crate::office::{Office, OfficeMail};
use crate::worker::Worker;

struct Department {
    office: Office,
    archive: Archive,
}

impl Department {
    fn new() -> Self {
        let o = Office::new();
        let a = Archive::new(o.clone());
        Department {
            office: o,
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
    async fn send(
        &self,
        request: Request<tonic::Streaming<Job>>,
    ) -> Result<Response<Void>, Status> {
        let job_names = parse_job_names(request.metadata());
        let mut offices = HashMap::with_capacity(job_names.len());
        let mut guarded_deps = self.departments.lock().await;

        for job_name in job_names {
            let office = guarded_deps
                .entry(job_name.clone())
                .or_insert(Department::new())
                .office
                .clone();
            offices.insert(job_name, office);
        }
        drop(guarded_deps);

        let mut job_stream = request.into_inner();
        while let Some(job) = job_stream.next().await {
            let job = job?;
            let office = offices.get_mut(&job.name).unwrap();
            office.send(OfficeMail::WorkOn(job)).await.unwrap();
        }
        Ok(Response::new(Void {}))
    }

    type WorkStream = Pin<Box<dyn Stream<Item = Result<Job, Status>> + Send + Sync + 'static>>;

    async fn work(
        &self,
        job_result: Request<tonic::Streaming<JobResult>>,
    ) -> Result<Response<Self::WorkStream>, Status> {
        let (tx, rx) = mpsc::channel(10);
        let w = Worker::new(nanoid!(), tx);

        let job_names = parse_job_names(job_result.metadata());
        let mut guarded_deps = self.departments.lock().await;
        let archives = HashMap::with_capacity(job_names.len());

        for job_name in job_names {
            let dep = guarded_deps
                .entry(job_name.to_owned())
                .or_insert(Department::new());
            dep.office
                .send(OfficeMail::HireWorker(w.clone()))
                .await
                .unwrap();
            archives.insert(job_name, dep.archive.clone());
        }

        tokio::spawn(async move {
            let result_stream = job_result.into_inner();
            while let Some(job_result) = result_stream.next().await {
                let job_result = match job_result {
                    Ok(j) => j,
                    Err(_) => break,
                };

                archives
                    .get(&job_result.job.unwrap().name)
                    .unwrap()
                    .send(ArchiveMail::AddResult(job_result));
            }
        });

        Ok(Response::new(Box::pin(rx) as Self::WorkStream))
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
