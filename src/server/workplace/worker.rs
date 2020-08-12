use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::{Code, Status};

use super::archive::{Archive, ArchiveMail};
use super::archive::{JobFaliure, JobFaliureDetail};
use crate::pb::worker_client::WorkerClient;
use crate::pb::Job;

#[derive(Clone)]
pub struct Worker {
	mail_box: mpsc::Sender<Job>,
}

impl std::ops::Deref for Worker {
	type Target = mpsc::Sender<Job>;

	fn deref(&self) -> &Self::Target {
		&self.mail_box
	}
}

impl std::ops::DerefMut for Worker {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.mail_box
	}
}

impl Worker {
	pub fn new(mut client: WorkerClient<Channel>, id: String, mut archive: Archive) -> Self {
		let (tx, mut rx): (_, mpsc::Receiver<Job>) = mpsc::channel(200);
		tokio::spawn(async move {
			while let Some(job) = rx.recv().await {
				match client.work(job.clone()).await {
					Ok(_) => archive
						.send(ArchiveMail::AppendSucceeded(job))
						.await
						.unwrap(),
					Err(status) => match status.code() {
						Code::Aborted
						| Code::Unimplemented
						| Code::Internal
						| Code::Unavailable => {
							let detail = JobFaliureDetail::new(id.clone(), status.code(), job);
							archive
								.send(ArchiveMail::AppendFailed(JobFaliure::WorkersFault(detail)))
								.await
								.unwrap();
						}
						_ => {
							let detail = JobFaliureDetail::new(id.clone(), status.code(), job);
							archive
								.send(ArchiveMail::AppendFailed(JobFaliure::Generic(detail)))
								.await
								.unwrap()
						}
					},
				}
			}
		});

		Self { mail_box: tx }
	}
}
