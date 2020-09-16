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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    println!("Lakh server listening on {}", addr);
    Server::builder()
        .add_service(LakhServer::new(Manager::new()))
        .serve(addr)
        .await?;

    Ok(())
}
