use futures::stream;
use nanoid::nanoid;
use tonic::metadata::MetadataValue;
use tonic::Request;
pub mod pb {
    tonic::include_proto!("workplace");
}

use pb::workplace_client::WorkplaceClient;
use pb::{Job, JobKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let job1 = Job {
        id: nanoid!(),
        name: "add".into(),
        args: vec!["1".into(), "1".into()],
        deley_duration: None,
        sheduled_date: None,
        reservation_period: Some(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        kind: JobKind::Immediate.into(),
    };
    let job2 = Job {
        id: nanoid!(),
        name: "sub".into(),
        args: vec!["1".into(), "1".into()],
        deley_duration: Some(prost_types::Duration {
            seconds: 5,
            nanos: 0,
        }),
        sheduled_date: None,
        reservation_period: Some(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        kind: JobKind::Delayed.into(),
    };

    let mut client = WorkplaceClient::connect("http://[::1]:50051").await?;
    let mut req = Request::new(stream::iter(vec![job1, job2]));
    req.metadata_mut()
        .insert("job_names", MetadataValue::from_static("add;sub"));

    match client.work(req).await {
        Ok(_) => {}
        Err(e) => println!("something went wrong: {:?}", e),
    }

    Ok(())
}
