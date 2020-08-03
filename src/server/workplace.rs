use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::pb::workplace_server::Workplace;
use crate::pb::{Job, Offer, Void};

#[derive(Default)]
pub struct LakhWorkplace {}

#[tonic::async_trait]
impl Workplace for LakhWorkplace {
    async fn assign(
        &self,
        request: Request<tonic::Streaming<Job>>,
    ) -> Result<Response<Void>, Status> {
        Ok(Response::new(Void {}))
    }

    type JoinStream = mpsc::Receiver<Result<Job, Status>>;

    async fn join(&self, request: Request<Offer>) -> Result<Response<Self::JoinStream>, Status> {
        let (mut tx, rx) = mpsc::channel(10);
        Ok(Response::new(rx))
    }
}
