use tonic::transport::Server;

pub mod pb {
    tonic::include_proto!("workplace");
}
use pb::workplace_server::WorkplaceServer;

mod workplace;
use workplace::LakhWorkplace;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let w = LakhWorkplace::default();

    println!("GreeterServer listening on {}", addr);

    Server::builder()
        .add_service(WorkplaceServer::new(w))
        .serve(addr)
        .await?;

    Ok(())
}
