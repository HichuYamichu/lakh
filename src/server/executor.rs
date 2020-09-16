use rand::seq::IteratorRandom;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

use crate::pb::Job;
use crate::task::{FailReason, Task, TaskCtl};
use crate::worker::{Worker, WorkerId};

type JobId = String;

#[derive(Debug)]
pub enum ExecutorCtl {
    WorkOn(Job),
    AddWorker(Worker),
    RemoveWorker(WorkerId),
    ProvideWorker(mpsc::Sender<Worker>),
    HandleJobSuccess(JobId),
    HandleJobFaliure(JobId),
    HandleDyingJob(Job, FailReason),
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
            let mut tasks = HashMap::new();
            let mut dead_jobs = Vec::new();
            let (starved_tasks_tx, _) = broadcast::channel(10);

            while let Some(ctl) = rx.recv().await {
                match ctl {
                    ExecutorCtl::WorkOn(j) => {
                        let key = j.id.clone();
                        let task = Task::new(j, tx_clone.clone());
                        tasks.insert(key, task);
                    }
                    ExecutorCtl::AddWorker(w) => {
                        workers.insert(w.id.clone(), w.clone());
                        // if this is our first worker we might have a bunch of
                        // starved tasks so we broadcast it to everyone interested
                        if workers.len() == 1 {
                            let _ = starved_tasks_tx.send(w);
                        }
                    }
                    ExecutorCtl::RemoveWorker(ref id) => {
                        workers.remove(id);
                    }
                    ExecutorCtl::HandleJobSuccess(ref id) => {
                        // someone else might have already reported completion
                        if let Some(mut task) = tasks.remove(id) {
                            // if job had no reservation task already exited
                            // and this send will fail
                            let _ = task.send(TaskCtl::Terminate).await;
                        }
                    }
                    ExecutorCtl::HandleJobFaliure(ref id) => {
                        // above applies here as well
                        if let Some(task) = tasks.get_mut(id) {
                            let _ = task.send(TaskCtl::Retry).await;
                        }
                    }
                    ExecutorCtl::HandleDyingJob(j, _) => {
                        dead_jobs.push(j);
                    }
                    ExecutorCtl::ProvideWorker(mut tx) => {
                        let w = workers.values().choose(&mut rand::thread_rng());
                        match w {
                            Some(w) => tx.send(w.clone()).await.unwrap(),
                            None => {
                                let starved_tasks_tx = starved_tasks_tx.clone();
                                tokio::spawn(async move {
                                    let mut rx = starved_tasks_tx.subscribe();
                                    // TODO: don't feed all tasks at once to prevent "thundering herd"
                                    tx.send(rx.recv().await.unwrap()).await.unwrap();
                                });
                            }
                        }
                    }
                }
            }
        });

        Self { tx }
    }
}
