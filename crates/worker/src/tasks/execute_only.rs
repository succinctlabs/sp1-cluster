use std::sync::Arc;

use crate::error::TaskError;
use crate::SP1ClusterWorker;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{
    ExecutionFailureCause, ExecutionResult, ExecutionStatus, ProofRequestStatus, WorkerTask,
};
use sp1_core_executor::{ExecutionError, Program, SP1CoreOpts};
use sp1_core_machine::executor::MachineExecutor;
use sp1_primitives::SP1Field;
use sp1_prover::worker::{ProofId, TaskId, TaskMetadata, WorkerClient};
use sp1_sdk::network::proto::types::ExecuteFailureCause;
use sp1_sdk::{SP1Context, SP1Stdin};
use sysinfo::System;

impl<W: WorkerClient, A: ArtifactClient> SP1ClusterWorker<W, A> {
    /// Does execution only - used for the execution oracle.
    pub async fn process_sp1_execute_only(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data.as_ref().expect("no task data");
        if data.inputs.is_empty() {
            log::info!("no inputs for task");
            return Ok(TaskMetadata::default());
        }

        // ELF, stdin, mode, cycle_limit
        let artifact_client = self.worker.artifact_client();
        let (elf, stdin): (Vec<u8>, SP1Stdin) = tokio::try_join!(
            artifact_client.download_program(&data.inputs[0]),
            artifact_client.download_stdin(&data.inputs[1]),
        )?;
        let cycle_limit = if data.inputs.len() > 2 {
            // [2] is mode, [3] is cycle_limit
            data.inputs[3].parse::<u64>().ok()
        } else {
            None
        };

        let proof_id = data.proof_id.clone();

        // Create executor per-task so memory is freed after task completes.
        let executor = Arc::new(create_executor());

        // Execute the program.
        let mut context = SP1Context::default();
        // Stop execution if the cycle limit is reached, + 1 to account for >= executor max_cycles check:
        // https://github.com/succinctlabs/sp1-wip/blob/7a3e3b25298f665a31a7aea0901e1739b4574324/crates/core/executor/src/executor.rs#L1786-L1791
        if let Some(cycle_limit) = cycle_limit {
            if cycle_limit != 0 {
                context.max_cycles = Some(cycle_limit + 1);
            }
        }

        let execution_result = tokio::task::spawn_blocking(move || {
            let program = Arc::new(Program::from(&elf).unwrap());
            let (public_values, filler, report) = executor
                .execute_sync(program, stdin, context)
                .map_err(|e| ExecutionError::Other(e.to_string()))?;
            Ok((public_values, filler, report))
        })
        .await
        .map_err(|e| TaskError::Fatal(anyhow::anyhow!(e)))?;

        let result_obj = match execution_result {
            Ok((pv, _, execution_report)) => ExecutionResult {
                status: ExecutionStatus::Executed as i32,
                failure_cause: ExecutionFailureCause::Unspecified as i32,
                cycles: execution_report.total_instruction_count(),
                gas: execution_report.gas().unwrap_or(0),
                public_values_hash: pv.hash(),
            },
            Err(err) => {
                // Determine the cause of failure.
                #[allow(unreachable_patterns)]
                let execution_fail_cause = match err {
                    ExecutionError::ExceededCycleLimit(..) => {
                        ExecuteFailureCause::ExceededCycleLimit
                    }
                    ExecutionError::InvalidMemoryAccess(..) => {
                        ExecuteFailureCause::InvalidMemoryAccess
                    }
                    ExecutionError::UnsupportedSyscall(..) => {
                        ExecuteFailureCause::UnsupportedSyscall
                    }
                    ExecutionError::Breakpoint(..) => ExecuteFailureCause::Breakpoint,
                    ExecutionError::InvalidSyscallUsage(..) => {
                        ExecuteFailureCause::InvalidSyscallUsage
                    }
                    ExecutionError::Unimplemented(..) => ExecuteFailureCause::Unimplemented,
                    ExecutionError::InvalidMemoryAccessUntrustedProgram(..)
                    | ExecutionError::EndInUnconstrained(..)
                    | ExecutionError::UnexpectedExitCode(..)
                    | ExecutionError::InstructionNotFound(..)
                    | ExecutionError::InvalidShardingState(..) => {
                        // TODO: Add error types in the network.
                        ExecuteFailureCause::UnspecifiedExecutionFailureCause
                    }
                    _ => ExecuteFailureCause::UnspecifiedExecutionFailureCause,
                };

                log::error!(
                    "execution failed with cause {:?}: {:?}",
                    execution_fail_cause,
                    err
                );

                ExecutionResult {
                    status: ExecutionStatus::Failed as i32,
                    failure_cause: execution_fail_cause as i32,
                    cycles: 0,
                    gas: 0,
                    public_values_hash: vec![],
                }
            }
        };

        self.worker
            .worker_client()
            .complete_proof(
                ProofId::new(proof_id),
                Some(TaskId::new(task.task_id.clone())),
                ProofRequestStatus::Completed,
                serde_json::to_string(&result_obj)
                    .map_err(|e| TaskError::Fatal(anyhow::anyhow!(e)))?,
            )
            .await?;

        Ok(Default::default())
    }
}

/// Creates a new MachineExecutor sized based on available system memory.
///
/// Uses half of free memory for the records buffer to balance between
/// execution performance and leaving headroom for other operations.
fn create_executor() -> MachineExecutor<SP1Field> {
    let sysinfo = System::new_all();
    let total_memory = sysinfo.total_memory();
    let used_memory = sysinfo.used_memory();
    let free_memory = total_memory.saturating_sub(used_memory);

    let records_buffer_size = (free_memory / 2) as usize;
    let num_record_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    tracing::info!(
        free_memory_bytes = free_memory,
        records_buffer_size,
        num_record_workers,
        "Creating executor"
    );

    MachineExecutor::new(
        records_buffer_size,
        num_record_workers,
        SP1CoreOpts::default(),
    )
}
