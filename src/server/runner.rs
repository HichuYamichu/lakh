use super::worker::{Worker, WorkerId};
use crate::pb::{job::Kind as JobKind, Job, JobResult};
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum RunnerMail {
    WorkOn(Job),
    HireWorker(Worker),
    HandleFaliure(WorkerId, Job),
}

pub struct Runner {
    mail_box: mpsc::Sender<RunnerMail>,
}

impl std::ops::Deref for Runner {
    type Target = mpsc::Sender<RunnerMail>;

    fn deref(&self) -> &Self::Target {
        &self.mail_box
    }
}

impl std::ops::DerefMut for Runner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mail_box
    }
}

impl Runner {
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel(10);

        let mut workers = HashMap::new();
        let mut queue = Vec::new();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(mail) = rx.recv() => {
                        match mail {
                            RunnerMail::WorkOn(j) => {
                                // match JobKind::from_i32(job.kind).unwrap() {
                                //     JobKind::Immediate => todo!(),
                                //     JobKind::Scheduled => todo!(),
                                //     JobKind::Delayed => todo!(),
                                // }
                                queue.push(j);
                            }
                            RunnerMail::HireWorker(w) => {
                                workers.insert(w.id.clone(), w);
                            }
                            RunnerMail::HandleFaliure(id, j) => {
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

        Self { mail_box: tx }
    }
}
