use lazy_static::lazy_static;

use crate::proto::TaskType;

/// The weight of a task. This is a simple way to measure the resources a task consumes. The task
/// weight is measured in GB of RAM.
pub fn task_weight(task_type: TaskType) -> usize {
    match task_type {
        TaskType::UnspecifiedTaskType => 0,
        TaskType::Controller => *CONTROLLER_WEIGHT,
        TaskType::ProveShard => 4,
        TaskType::RecursionDeferred => 3,
        TaskType::RecursionReduce => 3,
        TaskType::ShrinkWrap => 4,
        TaskType::SetupVkey => 1,
        TaskType::MarkerDeferredRecord => 0,
        TaskType::PlonkWrap => *PLONK_WRAP_WEIGHT,
        TaskType::Groth16Wrap => *GROTH16_WRAP_WEIGHT,
    }
}

lazy_static! {
    /// Task weight for controller task.
    pub static ref CONTROLLER_WEIGHT: usize = std::env::var("WORKER_CONTROLLER_WEIGHT")
        .map(|s| s.parse().unwrap())
        .unwrap_or(4);

    /// Task weight for groth16 wrap task.
    pub static ref GROTH16_WRAP_WEIGHT: usize = std::env::var("WORKER_GROTH16_WRAP_WEIGHT")
        .map(|s| s.parse().unwrap())
        .unwrap_or(14);

    /// Task weight for plonk wrap task.
    pub static ref PLONK_WRAP_WEIGHT: usize = std::env::var("WORKER_PLONK_WRAP_WEIGHT")
        .map(|s| s.parse().unwrap())
        .unwrap_or(32);
}
