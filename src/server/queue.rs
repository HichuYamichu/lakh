use crate::pb::Job;
use async_trait::async_trait;
use tokio::sync::mpsc;

#[async_trait]
pub trait Queue {
    async fn enque(&self, job: Job);
    async fn deque(&mut self) -> Job;
}

pub struct FifoQueue {
    tx: mpsc::Sender<Job>,
    rx: mpsc::Receiver<Job>,
}

#[async_trait]
impl Queue for FifoQueue {
    async fn enque(&self, job: Job) {
        self.tx.clone().send(job).await.unwrap();
    }

    async fn deque(&mut self) -> Job {
        self.rx.recv().await.unwrap()
    }
}
