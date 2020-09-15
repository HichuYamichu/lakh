use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tonic::Status;

use crate::pb::Job;
pub type WorkerId = String;

#[derive(Debug, Clone)]
pub struct Worker {
    pub id: WorkerId,
    inner: mpsc::Sender<Result<Job, Status>>,
}

impl Worker {
    pub fn new(id: WorkerId, inner: mpsc::Sender<Result<Job, Status>>) -> Self {
        Self { id, inner }
    }

    pub async fn work(&mut self, j: Job) -> Result<(), SendError<Result<Job, Status>>> {
        self.inner.send(Ok(j)).await
    }
}
