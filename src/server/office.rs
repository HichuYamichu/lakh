use futures::Stream;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tonic::Status;

use super::archive::{Archive, ArchiveMail};
use super::worker::{Worker, WorkerId};
use crate::pb::{job::Kind as JobKind, Job, JobResult};

#[derive(Debug)]
pub enum OfficeMail {
    WorkOn(Job),
    HireWorker(Worker),
    HandleFaliure(WorkerId, Job),
}

#[derive(Clone)]
pub struct Office {
    mail_box: mpsc::Sender<OfficeMail>,
}

impl std::ops::Deref for Office {
    type Target = mpsc::Sender<OfficeMail>;

    fn deref(&self) -> &Self::Target {
        &self.mail_box
    }
}

impl std::ops::DerefMut for Office {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mail_box
    }
}

impl Office {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(100);
        let self_mail = tx.clone();
        let dep = Self { mail_box: tx };

        let mut workers = HashMap::new();
        let mut queue = Vec::new();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(mail) = rx.recv() => {
                        match mail {
                            OfficeMail::WorkOn(j) => {
                                match JobKind::from_i32(j.kind).unwrap() {
                                    JobKind::Immediate => queue.push(j),
                                    JobKind::Scheduled => todo!(),
                                    JobKind::Delayed => todo!(),
                                }                                ;
                            }
                            OfficeMail::HireWorker(w) => {
                                workers.insert(w.id.clone(), w);
                            }
                            OfficeMail::HandleFaliure(id, j) => {
                                queue.push(j);
                                workers.remove(&id);
                            }
                        };
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

        dep
    }
}
