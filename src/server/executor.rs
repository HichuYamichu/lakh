use futures::Stream;
use rand::Rng;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::delay_for;
use tonic::Status;

use super::worker::{Worker, WorkerId};
use crate::pb::{Job, JobKind, JobResult};

type JobId = String;

#[derive(Debug)]
pub enum ExecutorCtl {
    WorkOn(Job),
    AddWorker(Worker),
    HandleWorkerFaliure(WorkerId, Job),
    HandleJobFaliure(Job),
    ForgetTryCount(JobId),
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
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let mut workers = HashMap::new();
            let mut workers_iter = workers.clone().into_iter().map(left);
            let mut queue = Vec::new();
            let mut run_count: HashMap<_, u32> = HashMap::new();
            let (delay_tx, mut delay_rx) = mpsc::channel(10);

            loop {
                tokio::select! {
                    Some(ctl) = rx.recv() => {
                        match ctl {
                            ExecutorCtl::WorkOn(j) => {
                                match JobKind::from_i32(j.kind).unwrap() {
                                    JobKind::Immediate => queue.push(j),
                                    JobKind::Scheduled => todo!(),
                                    JobKind::Delayed => {
                                        let dur = Duration::from_secs(j.deley_duration.clone().unwrap().seconds as u64);
                                        make_delay(delay_tx.clone(), dur, j);
                                    },
                                };
                            }
                            ExecutorCtl::AddWorker(w) => {
                                workers.insert(w.id.clone(), w);
                            }
                            ExecutorCtl::HandleWorkerFaliure(id, j) => {
                                workers.remove(&id);
                                queue.push(j);
                            }
                            ExecutorCtl::HandleJobFaliure(j) => {
                                let count = run_count.get(&j.id).unwrap() + 1;
                                let r: u32 = rand::thread_rng().gen_range(0, 30);
                                let dur = 15 + count ^ 4 + (r * (count + 1));
                                let dur = Duration::from_secs(dur as u64);
                                make_delay(delay_tx.clone(), dur, j);
                            }
                            ExecutorCtl::ForgetTryCount(ref id) => {
                                run_count.remove(id);
                            }
                        }

                    },
                    Some(j) = delay_rx.recv() => {
                        queue.push(j)
                    },
                    else => break
                }

                if !workers.is_empty() {
                    while let Some(job) = queue.pop() {
                        let mut w = if let Some(w) = workers_iter.next() {
                            w
                        } else {
                            workers_iter = workers.clone().into_iter().map(left);
                            workers_iter.next().unwrap()
                        };
                        run_count.insert(job.id.clone(), 1);
                        w.work(job, tx_clone.clone()).await;
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

fn make_delay(mut tx: mpsc::Sender<Job>, dur: Duration, job: Job) {
    tokio::spawn(async move {
        delay_for(dur).await;
        tx.send(job);
    });
}
