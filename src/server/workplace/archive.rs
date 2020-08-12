use tokio::sync::mpsc;
use tonic::{Code, Status};

use super::department::DepartmentMail;
use crate::pb::Job;

#[derive(Debug)]
pub enum ArchiveMail {
	AppendSucceeded(Job),
	AppendFailed(JobFaliure),
	SnapshotSucceeded(mpsc::Sender<Vec<Job>>),
	SnapshotFailed(mpsc::Sender<Vec<JobFaliureDetail>>),
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
	pub fn new(mut dep_mail: mpsc::Sender<DepartmentMail>) -> Self {
		let (tx, mut rx) = mpsc::channel(100);
		tokio::spawn(async move {
			let mut succeeded: Vec<Job> = Vec::new();
			let mut failed: Vec<JobFaliureDetail> = Vec::new();
			while let Some(mail) = rx.recv().await {
				match mail {
					ArchiveMail::AppendSucceeded(job) => succeeded.push(job),
					ArchiveMail::AppendFailed(faliure) => {
						todo!();
						Archive::append_failed(&mut failed, faliure, dep_mail).await;
					}
					ArchiveMail::SnapshotSucceeded(mut tx) => {
						tx.send(succeeded.clone()).await.unwrap()
					}
					ArchiveMail::SnapshotFailed(mut tx) => {
						todo!();
						tx.send(failed).await.unwrap()
					}
				}
			}
		});

		Self { mail_box: tx }
	}

	async fn append_failed(
		dest: &mut Vec<JobFaliureDetail>,
		job_faliure: JobFaliure,
		mut dep_mail: mpsc::Sender<DepartmentMail>,
	) {
		match job_faliure {
			JobFaliure::WorkersFault(detail) => {
				dep_mail
					.send(DepartmentMail::FireWorker(detail.worker_id.clone()))
					.await
					.unwrap();
				dest.push(detail);
			}
			JobFaliure::Generic(detail) => dest.push(detail),
		}
	}
}

#[derive(Debug)]
pub enum JobFaliure {
	WorkersFault(JobFaliureDetail),
	Generic(JobFaliureDetail),
}

#[derive(Debug)]
pub struct JobFaliureDetail {
	worker_id: String,
	status: Code,
	job: Job,
}

impl JobFaliureDetail {
	pub fn new(worker_id: String, status: Code, job: Job) -> Self {
		Self {
			worker_id,
			status,
			job,
		}
	}
}
