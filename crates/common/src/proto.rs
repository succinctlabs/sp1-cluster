use eyre::{eyre, Result};

tonic::include_proto!("worker");
tonic::include_proto!("cluster");

impl WorkerType {
    pub fn from_task_type(task_type: TaskType) -> Self {
        match task_type {
            TaskType::Controller | TaskType::PlonkWrap | TaskType::Groth16Wrap => WorkerType::Cpu,
            TaskType::ProveShard
            | TaskType::RecursionReduce
            | TaskType::RecursionDeferred
            | TaskType::ShrinkWrap
            | TaskType::SetupVkey => WorkerType::Gpu,
            TaskType::MarkerDeferredRecord | TaskType::UnspecifiedTaskType => WorkerType::None,
        }
    }
}

impl WorkerTask {
    pub fn data(&self) -> Result<&TaskData> {
        self.data.as_ref().ok_or(eyre!("no task data"))
    }
}
