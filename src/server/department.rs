use futures::Stream;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tonic::Status;

use super::archive::{Archive, ArchiveMail};
use super::worker::Worker;
use crate::pb::{job::Kind as JobKind, Job, JobResult};
use crate::queue::FifoQueue;

#[derive(Debug)]
pub enum DepartmentMail {
	WorkOn(Job),
	HireWorker(Worker, tonic::Streaming<JobResult>),
	ReportSucceeded(mpsc::Sender<Vec<JobResult>>),
	ReportFailed(mpsc::Sender<Vec<JobResult>>),
}

#[derive(Clone)]
pub struct Department {
	mail_box: mpsc::Sender<DepartmentMail>,
}

impl std::ops::Deref for Department {
	type Target = mpsc::Sender<DepartmentMail>;

	fn deref(&self) -> &Self::Target {
		&self.mail_box
	}
}

impl std::ops::DerefMut for Department {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.mail_box
	}
}

impl Department {
	pub fn new() -> Self {
		let (tx, mut rx) = mpsc::channel(100);
		let self_mail = tx.clone();
		let dep = Self { mail_box: tx };

		tokio::spawn(async move {
			let mut inner = Inner::new(self_mail);

			while let Some(mail) = rx.recv().await {
				match mail {
					DepartmentMail::WorkOn(job) => match JobKind::from_i32(job.kind).unwrap() {
						JobKind::Immediate => inner.work_now(job).await,
						JobKind::Scheduled => inner.schedule(job).await,
						JobKind::Delayed => inner.delay(job).await,
					},
					DepartmentMail::HireWorker(w, s) => {
						inner.add_source_to_archive(w.id.clone(), s).await;
						inner.hire_worker(w);
					}
					DepartmentMail::ReportSucceeded(tx) => inner.report_succeeded(tx).await,
					DepartmentMail::ReportFailed(tx) => inner.report_failed(tx).await,
				}
			}
		});

		dep
	}
}

struct Inner {
	workers: HashMap<String, Worker>,
	queue: FifoQueue,
	archive: Archive,
}

impl Inner {
	fn new(self_mail: mpsc::Sender<DepartmentMail>) -> Self {
		Self {
			workers: HashMap::new(),
			queue: FifoQueue::new(),
			archive: Archive::new(self_mail),
		}
	}

	async fn work_now(&mut self, j: Job) {
		let w: Worker = todo!();
		// w.send(j).await.unwrap();
	}

	async fn schedule(&mut self, j: Job) {
		todo!();
	}

	async fn delay(&mut self, j: Job) {
		todo!();
	}

	fn hire_worker(&mut self, w: Worker) {
		self.workers.insert(w.id.clone(), w);
	}

	async fn add_source_to_archive(
		&mut self,
		worker_id: String,
		source: tonic::Streaming<JobResult>,
	) {
		self.archive
			.send(ArchiveMail::AddSource(worker_id, source))
			.await
			.unwrap()
	}

	async fn report_succeeded(&mut self, tx: mpsc::Sender<Vec<JobResult>>) {
		self.archive
			.send(ArchiveMail::SnapshotSucceeded(tx))
			.await
			.unwrap()
	}

	async fn report_failed(&mut self, tx: mpsc::Sender<Vec<JobResult>>) {
		self.archive
			.send(ArchiveMail::SnapshotFailed(tx))
			.await
			.unwrap()
	}
}
