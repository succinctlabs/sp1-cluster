use std::sync::Arc;

use crate::{client::WorkerService, error::TaskError, tasks::TaskMetadata, SP1Worker};
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{ExecutionResult, ProofRequestUpdateRequest, WorkerTask};
use sp1_core_executor::ExecutionError;
use sp1_sdk::network::proto::types::ExecuteFailureCause;
use sp1_sdk::{SP1Context, SP1Stdin};

impl<W: WorkerService, A: ArtifactClient> SP1Worker<W, A> {
    /// Does execution only - used for the execution oracle.
    pub async fn process_sp1_util_execute_only(
        self: &Arc<Self>,
        task: &WorkerTask,
    ) -> Result<TaskMetadata, TaskError> {
        let data = task.data.as_ref().expect("no task data");

        if data.inputs.is_empty() {
            log::info!("no inputs for task");
            return Ok(TaskMetadata::default());
        }

        let (elf, stdin): (Vec<u8>, SP1Stdin) = tokio::try_join!(
            self.artifact_client.download_program(&data.inputs[0]),
            self.artifact_client.download_stdin(&data.inputs[1]),
        )?;
        let cycle_limit = if data.inputs.len() > 2 {
            data.inputs[2].parse::<u64>().ok()
        } else {
            None
        };

        let stdin_clone = stdin.clone();
        let proof_id = data.proof_id.clone();
        let prover = self.prover.clone();

        // Execute the program.
        let mut context = SP1Context::default();
        // Stop execution if the cycle limit is reached, + 1 to account for >= executor max_cycles check:
        // https://github.com/succinctlabs/sp1-wip/blob/7a3e3b25298f665a31a7aea0901e1739b4574324/crates/core/executor/src/executor.rs#L1786-L1791
        if let Some(cycle_limit) = cycle_limit {
            context.max_cycles = Some(cycle_limit + 1);
        }

        let execution_result = prover.execute(&elf, &stdin_clone, context);

        match execution_result {
            Ok((_, public_values_hash, execution_report)) => {
                let cycles = execution_report.total_instruction_count();
                let gas_used = execution_report.gas;

                self.cluster_client
                    .update_proof_request(ProofRequestUpdateRequest {
                        proof_id,
                        proof_status: Some(0), // TODO
                        execution_result: Some(ExecutionResult {
                            status: 0,        // TODO
                            failure_cause: 0, // TODO
                            cycles,
                            gas: gas_used.unwrap_or(0),
                            public_values_hash: Vec::from(public_values_hash),
                        }),
                        deadline: None,
                        handled: Some(true),
                        metadata: None,
                    })
                    .await?;
            }
            Err(err) => {
                // Determine the cause of failure.
                #[allow(unreachable_patterns)]
                let execution_fail_cause = match err {
                    ExecutionError::ExceededCycleLimit(..) => {
                        ExecuteFailureCause::ExceededCycleLimit
                    }
                    ExecutionError::HaltWithNonZeroExitCode(..) => {
                        ExecuteFailureCause::HaltWithNonZeroExitCode
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
                    ExecutionError::EndInUnconstrained(..) => {
                        ExecuteFailureCause::EndInUnconstrained
                    }
                    _ => ExecuteFailureCause::UnspecifiedExecutionFailureCause,
                };

                log::error!(
                    "execution failed with cause {:?}: {:?}",
                    execution_fail_cause,
                    err
                );

                self.cluster_client
                    .update_proof_request(ProofRequestUpdateRequest {
                        proof_id,
                        proof_status: None,     // TODO
                        execution_result: None, // TODO
                        deadline: None,
                        handled: Some(false),
                        metadata: Some(format!("{:?}", execution_fail_cause)),
                    })
                    .await?;

                return Err(TaskError::Fatal(anyhow::anyhow!(
                    "execution failed: {:?}",
                    err
                )));
            }
        }

        Ok(Default::default())
    }
}