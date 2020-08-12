use std::collections::HashMap;
use tokio::sync::mpsc;
use tonic::transport::Channel;

use super::archive::JobFaliureDetail;
use super::archive::{Archive, ArchiveMail};
use super::worker::Worker;
use crate::pb::worker_client::WorkerClient;
use crate::pb::{job::Kind as JobKind, Job};
use crate::queue::{FifoQueue, Queue};

#[derive(Debug)]
pub enum DepartmentMail {
	WorkOn(Job),
	HireWorker(WorkerClient<Channel>, String),
	FireWorker(String),
	ReportSucceeded(mpsc::Sender<Vec<Job>>),
	ReportFailed(mpsc::Sender<Vec<JobFaliureDetail>>),
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
		let own_mail = tx.clone();
		let dep = Self { mail_box: tx };

		tokio::spawn(async move {
			let mut workers = HashMap::new();
			let mut queue = FifoQueue::new();
			let mut archive = Archive::new(own_mail);

			while let Some(mail) = rx.recv().await {
				match mail {
					DepartmentMail::WorkOn(job) => match JobKind::from_i32(job.kind).unwrap() {
						JobKind::Immediate => {}
						JobKind::Scheduled => {}
						JobKind::Delayed => {}
					},
					DepartmentMail::HireWorker(wc, id) => {
						workers.insert(id.clone(), Worker::new(wc, id, archive.clone()));
					}
					DepartmentMail::FireWorker(ref id) => {
						workers.remove(id);
					}
					DepartmentMail::ReportSucceeded(tx) => archive
						.send(ArchiveMail::SnapshotSucceeded(tx))
						.await
						.unwrap(),
					DepartmentMail::ReportFailed(tx) => {
						archive.send(ArchiveMail::SnapshotFailed(tx)).await.unwrap()
					}
				}

				// let mut queue = std::mem::replace(&mut queue, FifoQueue::new());
				// tokio::spawn(async move {
				//     while !queue.is_empty() {
				//         let amount = std::cmp::min(workers.len(), queue.len());
				//         for (job, worker) in queue.drain(..amount).zip(workers.values_mut()) {
				//             worker.send(job).await.unwrap()
				//         }
				//     }
				// });
			}
		});

		dep
	}

	fn work_now(j: Job, mut w: Worker) {
		tokio::spawn(async move { w.send(j).await.unwrap() });
	}

	fn schedule(j: Job, mut w: Worker) {
		tokio::spawn(async move { w.send(j).await.unwrap() });
	}

	fn delay(j: Job, mut w: Worker) {
		tokio::spawn(async move { w.send(j).await.unwrap() });
	}
}
