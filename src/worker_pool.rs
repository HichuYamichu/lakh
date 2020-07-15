use hyper::client::HttpConnector;
use hyper::{header, Body, Client, Error, Method, Request, Response, Server, StatusCode};
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::{self};

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

    async fn work(&self) -> Result<(), Error> {
        let req = Request::builder()
            .method(Method::POST)
            .uri(&self.addr)
            .header(header::CONTENT_TYPE, "application/json")
            .body("job details".into())
            .unwrap();

        let _ = self.client.request(req).await?;
        Ok(())
    }
}

pub struct WorkerPool {
    workers: Vec<Worker>,
}

impl WorkerPool {
    pub fn new() -> Self {
        Self {
            workers: Vec::new(),
        }
    }

    fn tick(&mut self) {
        self.workers
            .retain(|worker| worker.last_heartbeat.elapsed().as_secs() < 1)
    }

    async fn run(&mut self) {
        let mut interval = time::interval(Duration::from_millis(10));
        loop {
            interval.tick().await;
            self.tick();
        }
    }

    pub fn add_or_update(&mut self, w: Worker) {
        let mut did_update = false;
        for worker in self.workers.iter_mut() {
            if worker.addr == w.addr {
                worker.update(w.state);
                did_update = true;
            }
        }

        if !did_update {
            self.workers.push(w);
        }
    }
}
