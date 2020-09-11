use futures::Stream;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tonic::Status;

use super::worker::{Worker, WorkerId};
use crate::pb::{Job, JobKind, JobResult};

type JobId = String;

#[derive(Debug)]
pub enum ExecutorCtl {
    WorkOn(Job),
    AddWorker(Worker),
    HandleWorkerFaliure(WorkerId, Job),
    HandleJobFaliure(JobId),
    EvictJob(JobId),
}

#[derive(Clone)]
pub struct Executor {
    tx: mpsc::Sender<ExecutorCtl>,
}

impl std::ops::Deref for Executor {
    type Target = mpsc::Sender<ExecutorCtl>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl std::ops::DerefMut for Executor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Executor {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(100);
        let inner = Inner::new();

        tokio::spawn(async move {
            let mut workers = HashMap::new();
            let mut workers_iter = workers.clone().into_iter().map(left);
            let mut queue = Vec::new();

            while let Some(ctl) = rx.recv().await {
                match ctl {
                    ExecutorCtl::WorkOn(j) => {
                        match JobKind::from_i32(j.kind).unwrap() {
                            JobKind::Immediate => todo!(),
                            JobKind::Scheduled => todo!(),
                            JobKind::Delayed => todo!(),
                        };
                    }
                    ExecutorCtl::AddWorker(w) => {
                        workers.insert(w.id.clone(), w);
                    }
                    ExecutorCtl::HandleWorkerFaliure(id, j) => {
                        workers.remove(&id);
                    }
                    _ => todo!(),
                }

                if !workers.is_empty() {
                    while let Some(job) = queue.pop() {
                        let mut w = if let Some(w) = workers_iter.next() {
                            w
                        } else {
                            workers_iter = workers.clone().into_iter().map(left);
                            workers_iter.next().unwrap()
                        };
                        w.work(job).await.unwrap();
                    }
                }
            }
        });

        Self { tx }
    }
}

fn left(item: (String, Worker)) -> Worker {
    item.1
}

struct Inner {
    workers: HashMap<String, Worker>,
    queue: Vec<Job>,
}

impl Inner {
    fn new() -> Self {
        let mut workers = HashMap::new();
        let mut queue = Vec::new();
        Self { workers, queue }
    }

    fn add_worker(&mut self, w: Worker) {
        self.workers.insert(w.id.clone(), w).unwrap();
    }

    fn work(&mut self) {}
}
