use crate::pb::Job;
use tokio::sync::mpsc;
use tonic::Status;

#[derive(Debug)]
pub struct Worker {
	pub id: String,
	inner: mpsc::Sender<Result<Job, Status>>,
}

impl Worker {
	pub fn new(id: String, inner: mpsc::Sender<Result<Job, Status>>) -> Self {
		Self { id, inner }
	}
}
