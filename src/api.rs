use crate::queue::Queue;
use crate::worker_pool::{Worker, WorkerPool, WorkerState};
use crate::Result;
use dashmap::DashMap;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Request, Response, StatusCode};
use std::sync::Arc;

struct Payload {
    state: WorkerState,
    available_jobs: Vec<String>,
}

pub async fn heartbeat<'a>(
    req: Request<Body>,
    worker_pool: Arc<DashMap<String, WorkerPool>>,
    client: Client<HttpConnector>,
) -> Result<Response<Body>> {
    let w = Worker::new("".into(), WorkerState::Available, client);
    let mut wp = worker_pool.entry("".into()).or_insert(WorkerPool::new());

    wp.add_or_update(w);

    let res = Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body("err".into())
        .unwrap();
    Ok(res)
}

pub async fn job(req: Request<Body>) -> Result<Response<Body>> {
    let res = Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body("err".into())
        .unwrap();
    Ok(res)
}
