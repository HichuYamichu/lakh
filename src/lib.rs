pub mod api;
pub mod worker_pool;
pub mod queue;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, GenericError>;
