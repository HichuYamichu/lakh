use tokio::stream::{StreamExt, StreamMap};
use tokio::sync::mpsc;
use tonic::Status;

use super::executor::ExecToArchiveMsg;
use super::worker::WorkerId;
use crate::pb::{Job, JobResult};

#[derive(Debug)]
pub enum ArchiveCtl {
    AddResult(JobResult),
}

#[derive(Debug)]
pub enum ArchiveToExecMsg {
    Reschedule(Job),
}

#[derive(Clone)]
pub struct Archive {
    tx: mpsc::Sender<ArchiveCtl>,
}

impl std::ops::Deref for Archive {
    type Target = mpsc::Sender<ArchiveCtl>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl std::ops::DerefMut for Archive {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Archive {
    pub fn new(
        from_executor: mpsc::Receiver<ExecToArchiveMsg>,
        to_executor: mpsc::Sender<ArchiveToExecMsg>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel(10);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(ctl) = rx.recv() => {
                        match ctl {
                            ArchiveCtl::AddResult(r) => todo!(),
                        };
                    }
                    Some(msg) = from_executor.recv() => {
                        match msg {
                            ExecToArchiveMsg::AddDead(j) => todo!()
                        }
                    }
                    else => break,
                }
            }
        });

        Self { tx }
    }
}
