use std::collections::HashMap;
use tokio::sync::mpsc;
use tonic::metadata::MetadataValue;
use tonic::Request;

pub mod pb {
    tonic::include_proto!("workplace");
}

use pb::workplace_client::WorkplaceClient;
use pb::{JobResult, JobStatus};

type JobHandler = fn(Vec<String>);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut jobs = HashMap::new();
    jobs.insert("add", add as JobHandler);
    jobs.insert("sub", sub as JobHandler);

    let mut client = WorkplaceClient::connect("http://[::1]:50051").await?;

    let (mut tx, rx) = mpsc::channel(10);
    let mut req = Request::new(rx);
    req.metadata_mut()
        .insert("job_names", MetadataValue::from_static("add;sub"));

    let res = client.join(req).await?;
    let mut inbound = res.into_inner();

    while let Some(job) = inbound.message().await? {
        let f = jobs.get(job.name.as_str()).unwrap();
        f(job.args.clone());
        tx.send(JobResult {
            job_id: job.id,
            job_name: job.name,
            status: JobStatus::Succeeded.into(),
        })
        .await?
    }

    Ok(())
}

fn add(args: Vec<String>) {
    let a = args[0].parse::<i32>().unwrap();
    let b = args[1].parse::<i32>().unwrap();
    let res = a + b;
    println!("add result: {}", res);
}

fn sub(args: Vec<String>) {
    let a = args[0].parse::<i32>().unwrap();
    let b = args[1].parse::<i32>().unwrap();
    let res = a - b;
    println!("sub result: {}", res);
}
