use rand::Rng;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::time::delay_for;

use crate::executor::ExecutorCtl;
use crate::pb::job::ExecutionTime;
use crate::pb::Job;

const MAX_RETRY: u8 = 30;

#[derive(Debug)]
pub enum TaskCtl {
    Retry,
    Terminate,
}

#[derive(Debug)]
pub enum FailReason {
    MaxRetryReached,
}

#[derive(Clone)]
pub struct Task {
    tx: mpsc::Sender<TaskCtl>,
}

impl std::ops::Deref for Task {
    type Target = mpsc::Sender<TaskCtl>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl std::ops::DerefMut for Task {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Task {
    pub fn new(mut job: Job, mut to_exec: mpsc::Sender<ExecutorCtl>) -> Self {
        let (tx, mut rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let mut try_count: u8 = 0;

            let res = loop {
                if try_count == MAX_RETRY {
                    break Err(FailReason::MaxRetryReached);
                };

                let wait_dur = match job.execution_time.clone() {
                    Some(ex_time) => match ex_time {
                        ExecutionTime::Immediate(_) => Duration::new(0, 0),
                        ExecutionTime::Scheduled(timestamp) => {
                            let duration_since_epoch =
                                Duration::new(timestamp.seconds as u64, timestamp.nanos as u32);
                            let system_duration_since_epoch = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap();
                            duration_since_epoch - system_duration_since_epoch
                        }
                        ExecutionTime::Delayed(dur) => {
                            Duration::new(dur.seconds as u64, dur.nanos as u32)
                        }
                    },
                    None => Duration::new(0, 0),
                };

                let mut delay = delay_for(wait_dur);
                loop {
                    tokio::select! {
                        _ = &mut delay => break,
                        Some(ctl) = rx.recv() => {
                            match ctl {
                                _ => { /* hadle status queries in the future */ },
                            }
                        }
                    }
                }

                let (worker_tx, mut worker_rx) = mpsc::channel(1);
                to_exec
                    .send(ExecutorCtl::ProvideWorker(worker_tx))
                    .await
                    .unwrap();
                let mut w = worker_rx.recv().await.unwrap();

                if w.work(job.clone()).await.is_err() {
                    to_exec.send(ExecutorCtl::RemoveWorker(w.id)).await.unwrap();
                    job.execution_time = Some(ExecutionTime::Immediate(()));
                    continue;
                };
                // inc try_count only after job was successfully sent to a worker
                // worker unavailability doesn't count as job failure
                try_count += 1;

                let reservation_time = match &job.reservation_time {
                    Some(t) => t,
                    None => {
                        // if job has no reservation time we won't wait for it's status
                        // and assume it succeeded
                        break Ok(());
                    }
                };

                let dur = Duration::from_secs(reservation_time.seconds as u64);
                let mut delay = delay_for(dur);

                tokio::select! {
                    _ = &mut delay => {},
                    Some(ctl) = rx.recv() => {
                        match ctl {
                            TaskCtl::Retry => expand_delay(&mut job, try_count),
                            TaskCtl::Terminate => break Ok(()),
                        }
                    }
                }
            };

            match res {
                Ok(_) => {}
                Err(reason) => {
                    to_exec
                        .send(ExecutorCtl::HandleDyingJob(job, reason))
                        .await
                        .unwrap();
                }
            }
        });

        Self { tx }
    }
}

fn expand_delay(job: &mut Job, try_count: u8) {
    // 15 + count ^ 4 + (rand(30) * (count + 1))
    // see https://github.com/contribsys/faktory/wiki/Job-Errors

    let r: u8 = rand::thread_rng().gen_range(0, 30);
    let seconds = 15 + (try_count ^ 4) + (r * (try_count + 1));
    let delay = prost_types::Duration {
        seconds: seconds as i64,
        nanos: 0,
    };
    job.execution_time = Some(ExecutionTime::Delayed(delay));
}
