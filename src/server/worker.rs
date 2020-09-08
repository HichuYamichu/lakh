use crate::pb::Job;
use tokio::sync::mpsc;
use tonic::{Code, Status};

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

    pub async fn work(&mut self, j: Job) -> Result<(), (WorkerId, Job)> {
        if let Err(e) = self.inner.send(Ok(j)).await {
            return Err((self.id.clone(), e.0.unwrap()));
        }
        Ok(())
    }
}
