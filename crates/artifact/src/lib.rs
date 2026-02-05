pub mod redis;
pub mod s3;
pub mod s3_rest;
pub mod s3_sdk;

use anyhow::Result;
pub use sp1_prover_types::{ArtifactClient, ArtifactId, ArtifactType};

/// Upload pre-compressed data directly, bypassing application-layer zstd compression.
///
/// Used for cross-bucket copies where the source data is already zstd-compressed.
pub trait CompressedUpload: ArtifactClient {
    fn upload_raw_compressed(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}
