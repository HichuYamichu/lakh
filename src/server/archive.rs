use tokio::stream::{StreamExt, StreamMap};
use tokio::sync::mpsc;
use tonic::Status;

use super::office::{Office, OfficeMail};
use super::worker::WorkerId;
use crate::pb::{job_result::Status as JobResultStatus, Job, JobResult};

#[derive(Debug)]
pub enum ArchiveMail {
    AddSource(WorkerId, tonic::Streaming<JobResult>),
    SnapshotSucceeded(mpsc::Sender<Vec<JobResult>>),
    SnapshotFailed(mpsc::Sender<Vec<JobResult>>),
}

#[derive(Clone)]
pub struct Archive {
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
    pub fn new(office_mail: Office) -> Self {
        let (tx, mut rx) = mpsc::channel(10);
        tokio::spawn(async move {
            let mut inner = Inner::new(office_mail);
            let mut results_stream = StreamMap::new();
            loop {
                tokio::select! {
                    Some(mail) = rx.recv() => {
                        match mail {
                            ArchiveMail::AddSource(id, s) => {
                                results_stream.insert(id, s);
                            },
                            ArchiveMail::SnapshotSucceeded(mut tx) => {
                                tx.send(inner.snapshot_succeeded()).await.unwrap();
                            },
                            ArchiveMail::SnapshotFailed(mut tx) => {
                                tx.send(inner.snapshot_failed()).await.unwrap();
                            },
                        };
                    },
                    Some::<(_, Result<JobResult, Status>)>((_, res)) = results_stream.next() => {
                        if let Ok(r) = res { inner.append_results(r); }
                    },
                    else => break,
                };
            }
        });

        Self { mail_box: tx }
    }
}

struct Inner {
    succeeded: Vec<JobResult>,
    failed: Vec<JobResult>,
    office_mail: Office,
}

impl Inner {
    fn new(office_mail: Office) -> Self {
        Self {
            succeeded: Vec::new(),
            failed: Vec::new(),
            office_mail,
        }
    }

    fn append_results(&mut self, job_result: JobResult) {
        match JobResultStatus::from_i32(job_result.status).unwrap() {
            JobResultStatus::Succeeded => self.succeeded.push(job_result),
            JobResultStatus::Failed => {
                if let Some(job) = job_result.job.clone() {
                    self.reschedule(job);
                }
                self.failed.push(job_result);
            }
        };
    }

    fn reschedule(&mut self, job: Job) {
        todo!()
    }

    fn snapshot_failed(&mut self) -> Vec<JobResult> {
        self.failed.clone()
    }

    fn snapshot_succeeded(&mut self) -> Vec<JobResult> {
        self.succeeded.clone()
    }
}
