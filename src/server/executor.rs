use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time::delay_for;
use tracing::{info, instrument, warn};
use tracing_futures::Instrument;

use crate::pb::{Job, JobResult, JobStatus};
use crate::task::{FailReason, Task, TaskCtl};
use crate::worker::{Worker, WorkerId};

#[derive(Debug)]
pub enum ExecutorCtl {
    WorkOn(Job),
    AddWorker(Worker),
    RemoveWorker(WorkerId),
    ProvideWorker(mpsc::Sender<Worker>),
    HandleJobResult(JobResult),
    HandleDyingJob(Job, FailReason),
    ReportDeadJobs(mpsc::Sender<Vec<Job>>),
}

#[derive(Debug)]
pub struct ExecutorHandle(mpsc::Sender<ExecutorCtl>);

impl std::ops::Deref for ExecutorHandle {
    type Target = mpsc::Sender<ExecutorCtl>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ExecutorHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone, Debug)]
pub struct Executor {
    max_retry: u8,
}

impl Executor {
    pub fn new(max_retry: u8) -> Self {
        Self { max_retry }
    }

    #[instrument(name = "executor")]
    pub fn spawn(&self, job_name: String) -> ExecutorHandle {
        let (tx, mut rx) = mpsc::channel(100);
        let task = Task::new(tx.clone(), self.max_retry);

        info!(message = "created", %job_name);
        let exec = async move {
            let mut workers = HashMap::new();
            let mut tasks = HashMap::new();
            let mut dead_jobs = Vec::new();
            let (starved_tasks_tx, _) = broadcast::channel(10);

            while let Some(ctl) = rx.recv().await {
                match ctl {
                    ExecutorCtl::WorkOn(j) => {
                        let key = j.id.clone();
                        let task = task.spawn(j);
                        tasks.insert(key, task);
                    }
                    ExecutorCtl::AddWorker(w) => {
                        workers.insert(w.id.clone(), w.clone());
                        info!(message = "worker added", id = %w.id, %job_name);
                        // if this is our first worker we might have a bunch of
                        // starved tasks so we broadcast it to everyone interested
                        if workers.len() == 1 {
                            let _ = starved_tasks_tx.send(w);
                        }
                    }
                    ExecutorCtl::RemoveWorker(ref id) => {
                        workers.remove(id);
                        info!(message = "worker removed", %id, %job_name);
                    }
                    ExecutorCtl::HandleJobResult(res) => {
                        match JobStatus::from_i32(res.status).unwrap() {
                            JobStatus::Failed => {
                                if let Some(task) = tasks.get_mut(&res.job_id) {
                                    let _ = task.send(TaskCtl::Retry).await;
                                }
                            }
                            JobStatus::Succeeded => {
                                if let Some(mut task) = tasks.remove(&res.job_id) {
                                    info!(message = "task removed", %res.job_id, %job_name);
                                    let _ = task.send(TaskCtl::Terminate).await;
                                }
                            }
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
                                let mut rx = starved_tasks_tx.subscribe();
                                let starved_count = starved_tasks_tx.receiver_count();
                                let feeder = async move {
                                    let w = rx.recv().await.unwrap();
                                    // don't feed all tasks at once to prevent "thundering herd"
                                    let delay = 100 * (starved_count as u64 - 1);
                                    delay_for(Duration::from_millis(delay)).await;
                                    tx.send(w).await.unwrap();
                                };
                                tokio::spawn(feeder.instrument(tracing::info_span!("feeder")));
                                warn!(%job_name, "starving {} tasks", starved_count);
                            }
                        }
                    }
                    ExecutorCtl::ReportDeadJobs(mut tx) => {
                        tx.send(dead_jobs.clone()).await.unwrap();
                    }
                }
            }
        };
        tokio::spawn(exec.in_current_span());

        ExecutorHandle(tx)
    }
}
