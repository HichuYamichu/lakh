use crate::worker::{Job, Worker, WorkerPool, WorkerPoolCtl, WorkerState};
use crate::Result;
use bytes::buf::ext::BufExt;
use dashmap::DashMap;
use hyper::{Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize)]
struct Heartbeat {
    state: WorkerState,
    available_jobs: Vec<String>,
}

pub async fn heartbeat(
    req: Request<Body>,
    worker_pool: Arc<DashMap<String, WorkerPoolCtl>>,
    addr: SocketAddr,
) -> Result<Response<()>> {
    let body = hyper::body::aggregate(req).await?;
    let hb: Heartbeat = serde_json::from_reader(body.reader())?;

    for job in hb.available_jobs {
        let wp = worker_pool.entry(job).or_insert(WorkerPool::new());
        let w = Worker::new(addr.to_string(), hb.state);
        wp.hearbeat(w);
    }

    let res = Response::builder().status(StatusCode::OK).body(()).unwrap();
    Ok(res)
}

pub async fn job(
    req: Request<Body>,
    worker_pool: Arc<DashMap<String, WorkerPoolCtl>>,
) -> Result<Response<()>> {
    let body = hyper::body::aggregate(req).await?;
    let job: Job = serde_json::from_reader(body.reader())?;
    let workCtl = worker_pool
        .entry(job.name.clone())
        .or_insert(WorkerPool::new());

    workCtl.work(job);
    let res = Response::builder().status(StatusCode::OK).body(()).unwrap();
    Ok(res)
}
