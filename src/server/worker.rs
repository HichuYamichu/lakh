use tokio::sync::mpsc;
use tonic::{Code, Status};

use crate::executor::ExecutorCtl;
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

    pub async fn work(&mut self, j: Job, mut to_exec: mpsc::Sender<ExecutorCtl>) {
        if let Err(e) = self.inner.send(Ok(j)).await {
            to_exec
                .send(ExecutorCtl::HandleWorkerFaliure(self.id.clone(), e.0.unwrap()))
                .await
                .unwrap();
        }
    }
}
