use tonic::transport::Server;

pub mod pb {
    tonic::include_proto!("lakh");
}
use pb::lakh_server::LakhServer;

mod executor;
mod manager;
mod task;
mod worker;

use manager::Manager;

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .compact()
        .init();

    let addr = "[::1]:50051".parse().unwrap();
    println!("Lakh server listening on {}", addr);
    Server::builder()
        .add_service(LakhServer::new(Manager::new()))
        .serve(addr)
        .await?;

    Ok(())
}
