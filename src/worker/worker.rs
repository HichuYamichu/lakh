use hyper::client::HttpConnector;
use hyper::{header, Client, Error, Method, Request};
use serde::{Deserialize, Serialize};
use serde_json;
use std::time::Instant;

#[derive(Debug, Deserialize, Serialize)]
struct Job {
    name: String,
    args: Vec<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WorkerState {
    Bussy,
    Available,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Worker {
    pub addr: String,
    pub state: WorkerState,
    last_heartbeat: Instant,
    // client: Box<Client<HttpConnector>>,
}

impl Worker {
    pub fn new(addr: String, state: WorkerState, client: Client<HttpConnector>) -> Self {
        Self {
            addr,
            state,
            last_heartbeat: Instant::now(),
            // client,
        }
    }

    pub fn update(&mut self, state: WorkerState) {
        self.state = state;
        self.last_heartbeat = Instant::now();
    }

    pub fn is_alive(&self) -> bool {
        self.last_heartbeat.elapsed().as_secs() < 1
    }

    // async fn work(&self, job: Job) -> Result<(), Error> {
    //     let j = serde_json::to_string(&job).unwrap();

    //     let req = Request::builder()
    //         .method(Method::POST)
    //         .uri(&self.addr)
    //         .header(header::CONTENT_TYPE, "application/json")
    //         .body(j.into())
    //         .unwrap();

    //     let _ = self.client.request(req).await?;
    //     Ok(())
    // }
}
