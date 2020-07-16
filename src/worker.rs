use hyper::client::HttpConnector;
use hyper::{header, Client, Error, Method, Request};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json;
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

#[derive(Debug, Deserialize, Serialize)]
struct Job {
    name: String,
    args: Vec<String>,
}

#[derive(Debug, Copy, Clone)]
pub enum WorkerState {
    Bussy,
    Available,
}

#[derive(Debug)]
pub struct Worker {
    addr: String,
    state: WorkerState,
    last_heartbeat: Instant,
    client: Client<HttpConnector>,
}

impl Worker {
    pub fn new(addr: String, state: WorkerState, client: Client<HttpConnector>) -> Self {
        Self {
            addr,
            state,
            last_heartbeat: Instant::now(),
            client,
        }
    }

    pub fn update(&mut self, state: WorkerState) {
        self.state = state;
        self.last_heartbeat = Instant::now();
    }

    async fn work(&self, job: Job) -> Result<(), Error> {
        let j = serde_json::to_string(&job).unwrap();

        let req = Request::builder()
            .method(Method::POST)
            .uri(&self.addr)
            .header(header::CONTENT_TYPE, "application/json")
            .body(j.into())
            .unwrap();

        let _ = self.client.request(req).await?;
        Ok(())
    }
}

pub struct WorkerPool {
    workers: Vec<Arc<Worker>>,
}

impl WorkerPool {
    pub fn new() -> PoolCtl {
        use self::PoolOp::*;

        let wp = Self {
            workers: Vec::new(),
        };

        let (tx, rx) = channel();
        let pc = PoolCtl::new(tx);

        thread::spawn(move || loop {
            let then = Instant::now();
            while then.elapsed().as_secs() > 1 {
                if let Ok(op) = rx.try_recv() {
                    match op {
                        Get(tx) => wp.get_worker(tx),
                        AddOrUpdate(w) => wp.add_or_update(w),
                    }
                }
            }
            wp.clear_idlers();
        });

        pc
    }

    fn clear_idlers(&mut self) {
        self.workers
            .retain(|worker| worker.last_heartbeat.elapsed().as_secs() < 1)
    }

    fn get_worker(&mut self, tx: Sender<Arc<Worker>>) {
        let w = self.workers.choose(&mut rand::thread_rng());
        tx.send(*w.unwrap()).unwrap();
    }

    fn add_or_update(&mut self, w: Worker) {
        let mut did_update = false;
        for worker in self.workers.iter_mut() {
            if worker.addr == w.addr {
                worker.update(w.state);
                did_update = true;
            }
        }

        if !did_update {
            self.workers.push(Arc::new(w));
        }
    }
}

pub enum PoolOp {
    Get(Sender<Arc<Worker>>),
    AddOrUpdate(Worker),
}

pub struct PoolCtl {
    tx: Sender<PoolOp>,
}

impl PoolCtl {
    fn new(tx: Sender<PoolOp>) -> Self {
        Self { tx }
    }

    pub fn add_or_update(&self, w: Worker) {
        let op = PoolOp::AddOrUpdate(w);
        self.tx.send(op).unwrap();
    }

    pub fn get_worker(&self) -> Arc<Worker> {
        let (tx, rx) = channel();
        let self_tx = self.tx.clone();
        thread::spawn(move || {
            self_tx.send(PoolOp::Get(tx)).unwrap();
        });
        rx.recv().unwrap()
    }
}
