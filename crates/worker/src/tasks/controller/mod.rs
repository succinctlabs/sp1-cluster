#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

use crate::error::TaskError;
use crate::utils::worker_task_to_raw_task_request;
use crate::SP1ClusterWorker;
use anyhow::Result;
use opentelemetry::Context;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{ProofRequestStatus, WorkerTask};
use sp1_prover::worker::{ProofId, TaskId, TaskMetadata, WorkerClient};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    /// The controller task for an SP1 proof. This task does all of the coordination for a full
    /// SP1 proof which can have mode core, compressed, plonk, and groth16.
    pub async fn process_sp1_controller(
        self: &Arc<Self>,
        parent: Context,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let now = std::time::Instant::now();
        let mut data = task.data()?.clone();

        if data.inputs.is_empty() {
            log::info!("no inputs for task");
            return Err(TaskError::Fatal(anyhow::anyhow!("no inputs for task")));
        }

        // Truncate the cycle limit and TODO thing for now.
        data.inputs = data.inputs.into_iter().take(3).collect();

        let raw_task_request = worker_task_to_raw_task_request(&data, Some(parent));

        self.worker.controller().run(raw_task_request).await?;

        // Mark proof as completed.
        self.worker
            .worker_client()
            .complete_proof(
                ProofId::new(data.proof_id.clone()),
                Some(TaskId::new(task.task_id.clone())),
                ProofRequestStatus::Completed,
            )
            .await?;

        let duration = now.elapsed().as_secs_f64();

        tracing::info!("PROOF {} COMPLETED in {} seconds", data.proof_id, duration);

        // Write to CSV file if path is configured
        if let Ok(csv_path) = std::env::var("PROOF_DURATIONS_CSV") {
            if let Err(e) = Self::append_to_csv(&csv_path, &data.proof_id, &task.task_id, duration)
            {
                tracing::warn!("Failed to write to CSV: {}", e);
            }
        }

        Ok(TaskMetadata { gpu_time: None })
    }

    fn append_to_csv(path: &str, proof_id: &str, task_id: &str, duration: f64) -> Result<()> {
        let file_exists = Path::new(path).exists();

        let mut file = OpenOptions::new().create(true).append(true).open(path)?;

        // Write header if file is new
        if !file_exists {
            writeln!(file, "timestamp,proof_id,task_id,duration_secs")?;
        }

        // Write data row
        let timestamp = chrono::Utc::now().to_rfc3339();
        writeln!(file, "{},{},{},{}", timestamp, proof_id, task_id, duration)?;

        Ok(())
    }
}
