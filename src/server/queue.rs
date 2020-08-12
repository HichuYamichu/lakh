use crate::pb::Job;

pub trait Queue {
    fn enque(&mut self, job: Job);
    fn deque(&mut self) -> Option<Job>;
}

pub struct FifoQueue {
    jobs: Vec<Job>,
}

impl Queue for FifoQueue {
    fn enque(&mut self, job: Job) {
        self.jobs.push(job);
    }

    fn deque(&mut self) -> Option<Job> {
        self.jobs.pop()
    }
}

impl std::ops::Deref for FifoQueue {
    type Target = Vec<Job>;

    fn deref(&self) -> &Self::Target {
        &self.jobs
    }
}

impl std::ops::DerefMut for FifoQueue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.jobs
    }
}

impl FifoQueue {
    pub fn new() -> Self {
        Self { jobs: Vec::new() }
    }
}
