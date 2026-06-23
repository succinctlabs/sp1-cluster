use std::sync::Arc;

use crate::error::TaskError;
use crate::SP1ClusterWorker;
use sp1_cluster_artifact::ArtifactClient;
use sp1_cluster_common::proto::{
    ExecutionFailureCause, ExecutionResult, ExecutionStatus, ProofRequestStatus, WorkerTask,
};
use sp1_core_executor::{ExecutionError, Program, SP1CoreOpts};
use sp1_prover::worker::{
    execute_with_options, ControllerInputs, ProofId, SP1ExecutorConfig, TaskId, TaskMetadata,
    WorkerClient,
};
use sp1_prover::SP1ProverComponents;
use sp1_sdk::network::proto::types::ExecuteFailureCause;
use sp1_sdk::{SP1Context, SP1Stdin};

impl<W: WorkerClient, A: ArtifactClient, C: SP1ProverComponents> SP1ClusterWorker<W, A, C> {
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

        let ControllerInputs {
            elf,
            stdin_artifact,
            cycle_limit,
            metadata,
            ..
        } = TryFrom::try_from(data.inputs.as_slice())?;

        let artifact_client = self.worker.artifact_client();
        let (elf, stdin): (Vec<u8>, SP1Stdin) = tokio::try_join!(
            artifact_client.download_program(&elf),
            artifact_client
                .download_with_type::<SP1Stdin>(&stdin_artifact, metadata.stdin_artifact_type()),
        )?;

        let proof_id = data.proof_id.clone();

        // Execute the program.
        let mut context = SP1Context::default();
        // Stop execution if the cycle limit is reached, + 1 to account for >= executor max_cycles check.
        // saturating_add so the u64::MAX "unlimited" sentinel doesn't wrap to 0 (which would enforce a
        // 0-cycle limit and make every request unfulfillable) in release builds.
        if let Some(cycle_limit) = cycle_limit {
            if cycle_limit != 0 {
                context.max_cycles = Some(cycle_limit.saturating_add(1));
            }
        }

        let execution_result = execute_with_options(
            Arc::new(Program::from(&elf).unwrap()),
            stdin,
            context,
            SP1CoreOpts::default(),
            SP1ExecutorConfig::default(),
        )
        .await
        .map_err(|e| {
            e.downcast::<ExecutionError>()
                .unwrap_or_else(|e| ExecutionError::Other(e.to_string()))
        });

        // `failure_message` (a bounded `Err(ExecutionError)` detail) and `exit_code`
        // (non-zero halt) are additive enrichment serialized alongside the
        // `ExecutionResult` into `extra_data`; consumers that don't know them
        // ignore them. The actual guest panic payload/location is NOT captured
        // here (that needs a separate SP1 executor stderr-capture change).
        let (result_obj, failure_message, exit_code): (
            ExecutionResult,
            Option<String>,
            Option<u64>,
        ) = match execution_result {
            Ok((pv, _, execution_report)) if execution_report.exit_code == 0 => (
                ExecutionResult {
                    status: ExecutionStatus::Executed as i32,
                    failure_cause: ExecutionFailureCause::Unspecified as i32,
                    cycles: execution_report.total_instruction_count(),
                    gas: execution_report.gas().unwrap_or(0),
                    public_values_hash: pv.hash(),
                },
                None,
                None,
            ),
            Ok((_, _, execution_report)) => {
                // Non-zero exit code means the guest program panicked.
                let exit_code = execution_report.exit_code;
                (
                    ExecutionResult {
                        status: ExecutionStatus::Failed as i32,
                        failure_cause: ExecuteFailureCause::HaltWithNonZeroExitCode as i32,
                        cycles: execution_report.total_instruction_count(),
                        gas: execution_report.gas().unwrap_or(0),
                        public_values_hash: vec![],
                    },
                    None,
                    Some(exit_code),
                )
            }
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

                (
                    ExecutionResult {
                        status: ExecutionStatus::Failed as i32,
                        failure_cause: execution_fail_cause as i32,
                        cycles: 0,
                        gas: 0,
                        public_values_hash: vec![],
                    },
                    Some(bound_failure_message(&err.to_string())),
                    None,
                )
            }
        };

        // Increment the cycles counter if any cycles were executed.
        if result_obj.cycles > 0 {
            if let Some(m) = self.metrics.as_ref() {
                m.num_cycles_executed.increment(result_obj.cycles);
            }
        }

        // Determine proof status based on execution result
        let proof_status = if result_obj.status == ExecutionStatus::Executed as i32 {
            ProofRequestStatus::Completed
        } else {
            ProofRequestStatus::Failed
        };

        let extra_data =
            build_execute_extra_data(&result_obj, failure_message.as_deref(), exit_code)
                .map_err(|e| TaskError::Fatal(anyhow::anyhow!(e)))?;

        self.worker
            .worker_client()
            .complete_proof(
                ProofId::new(proof_id),
                Some(TaskId::new(task.task_id.clone())),
                proof_status,
                extra_data,
            )
            .await?;

        Ok(Default::default())
    }
}

/// Producer-side cap for the enrichment message written into `extra_data`. The
/// network re-bounds and redacts on ingest, but we keep the cluster DB write
/// conservative and free of unbounded logs.
const FAILURE_MESSAGE_MAX: usize = 2048;

/// Bound an `Err(ExecutionError)` display string to [`FAILURE_MESSAGE_MAX`] bytes
/// on a UTF-8 char boundary. Not a substitute for receiver-side sanitization.
fn bound_failure_message(message: &str) -> String {
    if message.len() <= FAILURE_MESSAGE_MAX {
        return message.to_string();
    }
    let mut cut = FAILURE_MESSAGE_MAX;
    while cut > 0 && !message.is_char_boundary(cut) {
        cut -= 1;
    }
    message[..cut].to_string()
}

/// Serialize the `ExecutionResult` plus optional execute-only enrichment
/// (`failure_message`, `exit_code`) as additive JSON keys. Older consumers that
/// deserialize only `ExecutionResult` ignore the extra keys; the network's
/// `ErrorTrace::from_cluster_extra_data` reads them when present.
fn build_execute_extra_data(
    result: &ExecutionResult,
    failure_message: Option<&str>,
    exit_code: Option<u64>,
) -> serde_json::Result<String> {
    // Fast path: no enrichment (e.g. successful execution) serializes exactly as
    // before, keeping the common-case `extra_data` byte-identical.
    if failure_message.is_none() && exit_code.is_none() {
        return serde_json::to_string(result);
    }
    let mut value = serde_json::to_value(result)?;
    if let serde_json::Value::Object(map) = &mut value {
        if let Some(message) = failure_message {
            map.insert(
                "failure_message".to_string(),
                serde_json::Value::String(message.to_string()),
            );
        }
        if let Some(code) = exit_code {
            map.insert(
                "exit_code".to_string(),
                serde_json::Value::Number(code.into()),
            );
        }
    }
    serde_json::to_string(&value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn failed_result(failure_cause: i32) -> ExecutionResult {
        ExecutionResult {
            status: ExecutionStatus::Failed as i32,
            failure_cause,
            cycles: 0,
            gas: 0,
            public_values_hash: vec![],
        }
    }

    #[test]
    fn err_execution_error_serializes_bounded_failure_message() {
        let msg = bound_failure_message("exceeded cycle limit of 1000000");
        let extra = build_execute_extra_data(
            &failed_result(ExecuteFailureCause::ExceededCycleLimit as i32),
            Some(&msg),
            None,
        )
        .unwrap();
        let v: Value = serde_json::from_str(&extra).unwrap();
        assert_eq!(v["failure_message"], "exceeded cycle limit of 1000000");
        assert!(v.get("exit_code").is_none());
        // Still a valid ExecutionResult shape for legacy consumers.
        assert_eq!(
            v["failure_cause"],
            ExecuteFailureCause::ExceededCycleLimit as i32
        );
    }

    #[test]
    fn failure_message_is_bounded() {
        let huge = "x".repeat(FAILURE_MESSAGE_MAX * 4);
        let bounded = bound_failure_message(&huge);
        assert!(bounded.len() <= FAILURE_MESSAGE_MAX);
    }

    #[test]
    fn non_zero_exit_serializes_exit_code() {
        let extra = build_execute_extra_data(
            &failed_result(ExecuteFailureCause::HaltWithNonZeroExitCode as i32),
            None,
            Some(101),
        )
        .unwrap();
        let v: Value = serde_json::from_str(&extra).unwrap();
        assert_eq!(v["exit_code"], 101);
        assert!(v.get("failure_message").is_none());
    }

    #[test]
    fn success_has_no_enrichment_keys() {
        let result = ExecutionResult {
            status: ExecutionStatus::Executed as i32,
            failure_cause: ExecutionFailureCause::Unspecified as i32,
            cycles: 10,
            gas: 20,
            public_values_hash: vec![1, 2, 3],
        };
        let extra = build_execute_extra_data(&result, None, None).unwrap();
        let v: Value = serde_json::from_str(&extra).unwrap();
        assert!(v.get("failure_message").is_none());
        assert!(v.get("exit_code").is_none());
    }
}
