use hyper::client::HttpConnector;
use hyper::{header, Client, Error, Method, Request};
use serde::{Deserialize, Serialize};
use serde_json;
use std::hash::{Hash, Hasher};
use std::time::Instant;

#[derive(Debug, Deserialize, Serialize)]
pub struct Job {
    pub name: String,
    args: Vec<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum WorkerState {
    Bussy,
    Available,
}

#[derive(Debug)]
pub struct Worker {
    pub addr: String,
    pub state: WorkerState,
    last_heartbeat: Instant,
    client: Client<HttpConnector>,
}

impl Hash for Worker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
        self.state.hash(state);
        self.last_heartbeat.hash(state);
    }
}

impl Worker {
    pub fn new(addr: String, state: WorkerState) -> Self {
        Self {
            addr,
            state,
            last_heartbeat: Instant::now(),
            client: Client::new(),
        }
    }

    pub fn update(&mut self, state: WorkerState) {
        self.state = state;
        self.last_heartbeat = Instant::now();
    }

    pub fn is_alive(&self) -> bool {
        self.last_heartbeat.elapsed().as_secs() < 1
    }

    pub async fn work(&self, job: Job) -> Result<(), Error> {
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
