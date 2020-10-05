use serde::Deserialize;
use tokio::fs;
use tonic::transport::Server;
use tracing::info;

pub mod pb {
    tonic::include_proto!("lakh");
}
use pb::lakh_server::LakhServer;

mod executor;
mod manager;
mod task;
mod worker;

use manager::Manager;

#[derive(Deserialize)]
pub struct Config {
    addr: String,
    max_retry: u8,
}

type Error = Box<dyn std::error::Error>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .compact()
        .init();

    let toml_str = fs::read_to_string("config.toml").await?;
    let conf: Config = toml::from_str(&toml_str)?;
    let addr = conf.addr.parse()?;

    info!("listening on {}", addr);
    Server::builder()
        .add_service(LakhServer::new(Manager::new(conf)))
        .serve(addr)
        .await?;

    Ok(())
}
