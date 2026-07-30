mod controller;
mod core_execute;
mod execute_only;
mod finalize;
mod prove_shard;
mod recursion;
mod setup;
mod shrink_wrap;
mod vk_gen;

use sp1_cluster_artifact::ArtifactClient;
use sp1_prover::worker::{RawTaskRequest, TaskError, TaskMetadata};

async fn recover_task_result(
    request: &RawTaskRequest,
    result: Result<TaskMetadata, TaskError>,
    artifact_client: &impl ArtifactClient,
) -> Result<TaskMetadata, TaskError> {
    request.recover_if_complete(result, artifact_client).await
}

#[cfg(test)]
mod tests {
    use sp1_cluster_artifact::{ArtifactClient, ArtifactType, InMemoryArtifactClient};
    use sp1_prover::worker::{ProofId, RequesterId, TaskContext};
    use sp1_prover_types::Artifact;

    use super::*;

    fn request(outputs: Vec<Artifact>) -> RawTaskRequest {
        RawTaskRequest {
            inputs: vec![],
            outputs,
            context: TaskContext {
                proof_id: ProofId::new("test-proof"),
                parent_id: None,
                parent_context: None,
                requester_id: RequesterId::new("test-requester"),
            },
        }
    }

    fn execution_error() -> Result<TaskMetadata, TaskError> {
        Err(TaskError::Fatal(anyhow::anyhow!("missing input")))
    }

    #[tokio::test]
    async fn recovers_completed_redelivery() {
        let artifact_client = InMemoryArtifactClient::new();
        let output_a = Artifact::from("output-a".to_string());
        let output_b = Artifact::from("output-b".to_string());
        let request = request(vec![output_a.clone(), output_b.clone()]);

        artifact_client
            .upload_raw(&output_a, ArtifactType::UnspecifiedArtifactType, vec![1])
            .await
            .unwrap();

        let error = recover_task_result(&request, execution_error(), &artifact_client)
            .await
            .unwrap_err();
        match error {
            TaskError::Fatal(error) => assert_eq!(error.to_string(), "missing input"),
            error => panic!("expected original fatal error, got {error}"),
        }

        artifact_client
            .upload_raw(&output_b, ArtifactType::UnspecifiedArtifactType, vec![1])
            .await
            .unwrap();

        let recovered = recover_task_result(&request, execution_error(), &artifact_client)
            .await
            .unwrap();
        assert_eq!(recovered.gpu_ms, None);

        let success = recover_task_result(
            &request,
            Ok(TaskMetadata { gpu_ms: Some(7) }),
            &artifact_client,
        )
        .await
        .unwrap();
        assert_eq!(success.gpu_ms, Some(7));
    }
}
