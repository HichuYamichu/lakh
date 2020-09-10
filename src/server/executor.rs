use futures::Stream;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tonic::Status;

use super::archive::ArchiveToExecMsg;
use super::worker::{Worker, WorkerId};
use crate::pb::{Job, JobKind, JobResult};

#[derive(Debug)]
pub enum ExecutorCtl {
    WorkOn(Job),
    HireWorker(Worker),
    HandleFaliure(WorkerId, Job),
}

#[derive(Debug)]
pub enum ExecToArchiveMsg {
    AddDead(Job),
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
    pub fn new(
        from_archive: mpsc::Receiver<ArchiveToExecMsg>,
        to_archive: mpsc::Sender<ExecToArchiveMsg>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel(100);
        let mut workers = HashMap::new();
        let mut queue = Vec::new();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(ctl) = rx.recv() => {
                        match ctl {
                            ExecutorCtl::WorkOn(j) => {
                                match JobKind::from_i32(j.kind).unwrap() {
                                    JobKind::Immediate => queue.push(j),
                                    JobKind::Scheduled => todo!(),
                                    JobKind::Delayed => todo!(),
                                };
                            }
                            ExecutorCtl::HireWorker(w) => {
                                workers.insert(w.id.clone(), w);
                            }
                            ExecutorCtl::HandleFaliure(id, j) => {
                                queue.push(j);
                                workers.remove(&id);
                            }
                        };
                    },
                    Some(msg) = from_archive.recv() => {
                        match msg {
                            ArchiveToExecMsg::Reschedule(j) => todo!()
                        }
                    },
                    else => break
                }

                if !workers.is_empty() {
                    continue;
                }
                while !queue.is_empty() {
                    let amount = std::cmp::min(workers.len(), queue.len());
                    for (job, worker) in queue.drain(..amount).zip(workers.values_mut()) {
                        worker.work(job).await.unwrap()
                    }
                }
            }
        });

        Self { tx }
    }
}
