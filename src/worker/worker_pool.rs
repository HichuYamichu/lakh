use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::time::Instant;

use super::{Job, Worker};

pub struct WorkerPool {
    workers: HashMap<String, Worker>,
}

impl WorkerPool {
    pub fn new() -> WorkerPoolCtl {
        let wp = Self {
            workers: HashMap::new(),
        };
        let (tx, rx) = channel();

        thread::spawn(move || loop {
            let then = Instant::now();
            while then.elapsed().as_secs() < 5 {
                if let Ok(op) = rx.try_recv() {
                    match op {
                        Work(job) => wp.work(job),
                        Hearbeat(w) => wp.add_or_update(w),
                    }
                }
            }
            wp.clear_idlers();
        });

        WorkerPoolCtl::new(tx)
    }

    fn clear_idlers(&mut self) {
        self.workers.retain(|_, worker| worker.is_alive())
    }

    fn work(&mut self, job: Job) {
        self.workers
            .values()
            .filter(|w| w.is_alive())
            .choose(&mut rand::thread_rng())
            .unwrap()
            .work(job);
    }

    fn add_or_update(&mut self, w: Worker) {
        self.workers.entry(w.addr).or_insert(w).update(w.state);
    }
}

pub enum WorkerPoolOp {
    Work(Job),
    Hearbeat(Worker),
}

pub struct WorkerPoolCtl {
    tx: Sender<WorkerPoolOp>,
}

use self::WorkerPoolOp::*;

impl WorkerPoolCtl {
    fn new(tx: Sender<WorkerPoolOp>) -> Self {
        Self { tx }
    }

    pub fn hearbeat(&self, w: Worker) {
        self.tx.send(WorkerPoolOp::Hearbeat(w)).unwrap();
    }

    pub fn work(&self, job: Job) {
        self.tx.send(WorkerPoolOp::Work(job)).unwrap();
    }
}
