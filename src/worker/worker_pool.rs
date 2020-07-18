use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use super::worker::Worker;

pub struct WorkerPool {
    workers: HashMap<String, Arc<Worker>>,
}

impl WorkerPool {
    pub fn new() -> PoolCtl {
        use self::PoolOp::*;

        let wp = Self {
            workers: HashMap::new(),
        };

        let (tx, rx) = channel();
        let pc = PoolCtl::new(tx);

        thread::spawn(move || loop {
            let then = Instant::now();
            while then.elapsed().as_secs() < 1 {
                if let Ok(op) = rx.try_recv() {
                    match op {
                        Get(tx) => wp.get_worker(tx),
                        AddOrUpdate(w) => wp.add_or_update(w),
                    }
                }
            }
            wp.clear_idlers();
        });

        pc
    }

    fn clear_idlers(&mut self) {
        self.workers.retain(|_, worker| worker.is_alive())
    }

    fn get_worker(&mut self, tx: Sender<Option<Arc<Worker>>>) {
        let w = self
            .workers
            .values()
            .filter(|w| w.is_alive())
            .choose(&mut rand::thread_rng())
            .map(|w| *w);
        tx.send(w).unwrap();
    }

    fn add_or_update(&mut self, w: Worker) {
        self.workers
            .entry(w.addr)
            .or_insert(Arc::new(w))
            .update(w.state);
    }
}

pub enum PoolOp {
    Get(Sender<Option<Arc<Worker>>>),
    AddOrUpdate(Worker),
}

pub struct PoolCtl {
    tx: Sender<PoolOp>,
}

impl PoolCtl {
    fn new(tx: Sender<PoolOp>) -> Self {
        Self { tx }
    }

    pub fn add_or_update(&self, w: Worker) {
        let op = PoolOp::AddOrUpdate(w);
        self.tx.send(op).unwrap();
    }

    pub fn get_worker(&self) -> Option<Arc<Worker>> {
        let (tx, rx) = channel();
        let self_tx = self.tx.clone();
        thread::spawn(move || {
            self_tx.send(PoolOp::Get(tx)).unwrap();
        });
        rx.recv().unwrap()
    }
}
