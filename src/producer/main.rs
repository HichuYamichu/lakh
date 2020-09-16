use futures::stream;
use nanoid::nanoid;
use std::time::{Duration, SystemTime};
use tonic::metadata::MetadataValue;
use tonic::Request;
pub mod pb {
    tonic::include_proto!("lakh");
}

use pb::job::ExecutionTime;
use pb::lakh_client::LakhClient;
use pb::Job;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let job1 = Job {
        id: nanoid!(),
        name: "add".into(),
        args: vec!["1".into(), "1".into()],
        execution_time: Some(ExecutionTime::Immediate(())),
        reservation_time: Some(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
    };
    let job2 = Job {
        id: nanoid!(),
        name: "sub".into(),
        args: vec!["1".into(), "1".into()],
        execution_time: Some(ExecutionTime::Delayed(prost_types::Duration {
            seconds: 5,
            nanos: 0,
        })),
        reservation_time: None,
    };

    // create timestamp 10s into the future
    let system_duration_since_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let dur = Duration::from_secs(10);
    let timestamp = prost_types::Timestamp {
        seconds: (system_duration_since_epoch + dur).as_secs() as i64,
        nanos: 0,
    };

    let job3 = Job {
        id: nanoid!(),
        name: "sub".into(),
        args: vec!["2".into(), "2".into()],
        execution_time: Some(ExecutionTime::Scheduled(timestamp)),
        reservation_time: Some(prost_types::Duration {
            seconds: 20,
            nanos: 0,
        }),
    };

    let mut client = LakhClient::connect("http://[::1]:50051").await?;
    let mut req = Request::new(stream::iter(vec![job1, job2, job3]));
    req.metadata_mut()
        .insert("job_names", MetadataValue::from_static("add;sub"));

    match client.work(req).await {
        Ok(_) => {}
        Err(e) => println!("something went wrong: {:?}", e),
    }

    Ok(())
}
