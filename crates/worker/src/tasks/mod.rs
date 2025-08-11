mod controller;
mod finalize;
mod prove_shard;
mod recursion;
mod setup;
mod shrink_wrap;
mod types;

pub use controller::{
    shards::{ShardEventData, ShardEventsMode},
    CommonTaskInput,
};
use serde::{Deserialize, Serialize};
pub use types::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskMetadata {
    pub gpu_ms: u32,
}

impl TaskMetadata {
    pub fn new(gpu_ms: u32) -> Self {
        Self { gpu_ms }
    }
}
