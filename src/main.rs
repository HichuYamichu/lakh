use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error, Response, Server};

use dashmap::DashMap;
use hyper::client::HttpConnector;
use hyper::{Client, Method, Request, StatusCode};
use std::sync::Arc;
use std::sync::Mutex;

static NOTFOUND: &[u8] = b"Not Found";

use lakh::api;
use lakh::queue::Queue;
use lakh::worker_pool::WorkerPool;
use lakh::Result;

#[tokio::main]
async fn main() {
    let addr = ([127, 0, 0, 1], 3000).into();
    let client = Client::new();
    let wp = Arc::new(DashMap::new());
    let q = Arc::new(Mutex::new(Queue::new()));

    let make_service = make_service_fn(move |_| {
        let client = client.clone();
        let wp = wp.clone();
        async move {
            Ok::<_, Error>(service_fn(move |req| {
                let wp = wp.clone();
                handle_request(req, client.to_owned(), wp)
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_service);

    println!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

async fn handle_request<'a>(
    req: Request<Body>,
    client: Client<HttpConnector>,
    worker_pool: Arc<DashMap<String, WorkerPool>>,
) -> Result<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/job") => api::job(req).await,
        (&Method::POST, "/heartbeat") => api::heartbeat(req, worker_pool, client).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(NOTFOUND.into())
            .unwrap()),
    }
}
