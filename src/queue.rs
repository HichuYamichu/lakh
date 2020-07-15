use crate::worker_pool::WorkerPool;
use std::collections::HashMap;

pub struct Queue {
    inner_q: HashMap<String, WorkerPool>,
}

impl Queue {
    pub fn new() -> Self {
        Self {
            inner_q: HashMap::new(),
        }
    }

    fn heartbeat(&self, job_names: Vec<String>) {
        for job in job_names {
            for worker in self.inner_q.get(&job) {
                // worker.update()
            }
        }
    }
}
