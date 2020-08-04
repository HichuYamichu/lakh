use dashmap::DashMap;
use rayon::prelude::*;
use tokio::sync::{mpsc, mpsc::error::TryRecvError};
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::pb::worker_client::WorkerClient;
use crate::pb::workplace_server::Workplace;
use crate::pb::{Job, Void, WorkerInfo};
use crate::queue::FifoQueue;

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

        for job_name in worker_info.available_jobs {
            self.departments
                .entry(job_name)
                .or_insert(Department::new())
                .send(DepartmentMail::HireWorker(worker.clone()))
                .await
                .unwrap();
        }

        Ok(Response::new(Void {}))
    }
}

#[derive(Debug)]
enum DepartmentMail {
    WorkOn(Job),
    HireWorker(WorkerClient<Channel>),
    ReportDone(mpsc::Sender<Vec<Job>>),
    ReportFailed(mpsc::Sender<Vec<Job>>),
}

struct Department {
    mail_box: mpsc::Sender<DepartmentMail>,
}

impl std::ops::Deref for Department {
    type Target = mpsc::Sender<DepartmentMail>;

    fn deref(&self) -> &Self::Target {
        &self.mail_box
    }
}

impl std::ops::DerefMut for Department {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mail_box
    }
}

impl Department {
    fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(10);
        tokio::spawn(async move {
            let mut workers = Vec::new();
            let mut queue: Vec<Job> = Vec::new();
            let mut archive = Archive::new();

            'department: loop {
                loop {
                    match rx.try_recv() {
                        Ok(op) => match op {
                            DepartmentMail::WorkOn(job) => queue.push(job),
                            DepartmentMail::HireWorker(wc) => {
                                workers.push(Worker::new(wc, archive.clone()))
                            }
                            DepartmentMail::ReportDone(tx) => {
                                archive.send(ArchiveMail::SnapshotDone(tx)).await.unwrap()
                            }
                            DepartmentMail::ReportFailed(tx) => {
                                archive.send(ArchiveMail::SnapshotFailed(tx)).await.unwrap()
                            }
                        },
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Closed) => break 'department,
                    }
                }

                let amount = std::cmp::min(workers.len() - 1, queue.len() - 1);
                for (job, mut w) in queue.drain(..amount).zip(workers.clone()) {
                    tokio::spawn(async move {
                        w.send(job).await.unwrap();
                    });
                }
            }
        });

        Self { mail_box: tx }
    }
}

#[derive(Debug)]
enum ArchiveMail {
    Active(Job),
    Done(Job),
    Failed(Job),
    SnapshotActive(mpsc::Sender<Vec<Job>>),
    SnapshotDone(mpsc::Sender<Vec<Job>>),
    SnapshotFailed(mpsc::Sender<Vec<Job>>),
}

#[derive(Clone)]
struct Archive {
    mail_box: mpsc::Sender<ArchiveMail>,
}

impl std::ops::Deref for Archive {
    type Target = mpsc::Sender<ArchiveMail>;

    fn deref(&self) -> &Self::Target {
        &self.mail_box
    }
}

impl std::ops::DerefMut for Archive {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mail_box
    }
}

impl Archive {
    fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(10);
        tokio::spawn(async move {
            let mut active: Vec<Job> = Vec::new();
            let mut failed: Vec<Job> = Vec::new();
            let mut done: Vec<Job> = Vec::new();
            while let Some(op) = rx.recv().await {
                match op {
                    ArchiveMail::Done(job) => done.push(job),
                    ArchiveMail::Failed(job) => failed.push(job),
                    ArchiveMail::Active(job) => active.push(job),
                    ArchiveMail::SnapshotActive(mut tx) => tx.send(active.clone()).await.unwrap(),
                    ArchiveMail::SnapshotDone(mut tx) => tx.send(active.clone()).await.unwrap(),
                    ArchiveMail::SnapshotFailed(mut tx) => tx.send(active.clone()).await.unwrap(),
                }
            }
        });

        Self { mail_box: tx }
    }
}

#[derive(Clone)]
struct Worker {
    mail_box: mpsc::Sender<Job>,
}

impl std::ops::Deref for Worker {
    type Target = mpsc::Sender<Job>;

    fn deref(&self) -> &Self::Target {
        &self.mail_box
    }
}

impl std::ops::DerefMut for Worker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mail_box
    }
}

impl Worker {
    fn new(mut client: WorkerClient<Channel>, mut archive: Archive) -> Self {
        let (tx, mut rx): (_, mpsc::Receiver<Job>) = mpsc::channel(10);
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                match client.work(job.clone()).await {
                    Ok(_) => archive.send(ArchiveMail::Done(job)).await.unwrap(),
                    Err(_) => archive.send(ArchiveMail::Failed(job)).await.unwrap(),
                }
            }
        });

        Self { mail_box: tx }
    }
}
