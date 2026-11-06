pub mod redis;
pub mod s3;
pub mod s3_rest;
pub mod s3_sdk;

use anyhow::Result;
use serde::de::DeserializeOwned;
pub use sp1_prover_types::{ArtifactClient, ArtifactId, ArtifactType, InMemoryArtifactClient};
use std::fmt;
use std::io::Read;

/// Programs are customer bytes stored as is, so a small zstd bomb must not exhaust a node's memory.
const MAX_PROGRAM_BYTES: u64 = 512 * 1024 * 1024;

/// A download refused for its content, which fails the same way on every retry.
#[derive(Debug)]
pub enum ArtifactError {
    TooLarge(ArtifactType),
    Corrupt(ArtifactType, anyhow::Error),
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge(t) => write!(f, "{t:?} exceeds {} bytes", max_bytes(*t)),
            Self::Corrupt(t, e) => write!(f, "{t:?} is corrupt: {e}"),
        }
    }
}

impl std::error::Error for ArtifactError {}

fn decompress(compressed: &[u8], artifact_type: ArtifactType) -> Result<Vec<u8>, ArtifactError> {
    let cap = max_bytes(artifact_type).saturating_add(1);
    let mut decoded = Vec::new();
    if cap < u64::MAX {
        decoded.reserve_exact(cap as usize);
    }
    zstd::Decoder::with_buffer(compressed)
        .and_then(|d| d.take(cap).read_to_end(&mut decoded))
        .map_err(|e| ArtifactError::Corrupt(artifact_type, e.into()))?;
    check_size(decoded.len() as u64, artifact_type)?;
    Ok(decoded)
}

async fn deserialize<T: DeserializeOwned + Send + 'static>(
    bytes: Vec<u8>,
    artifact_type: ArtifactType,
) -> Result<T> {
    let deserialized = tokio::task::spawn_blocking(move || bincode::deserialize(&bytes)).await?;
    Ok(deserialized.map_err(|e| ArtifactError::Corrupt(artifact_type, e.into()))?)
}

fn check_size(size: u64, artifact_type: ArtifactType) -> Result<(), ArtifactError> {
    if size > max_bytes(artifact_type) {
        return Err(ArtifactError::TooLarge(artifact_type));
    }
    Ok(())
}

fn max_bytes(artifact_type: ArtifactType) -> u64 {
    match artifact_type {
        ArtifactType::Program => MAX_PROGRAM_BYTES,
        _ => u64::MAX,
    }
}

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

impl CompressedUpload for InMemoryArtifactClient {
    async fn upload_raw_compressed(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> Result<()> {
        self.upload_raw(artifact, artifact_type, data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zeros(len: u64) -> Vec<u8> {
        let mut compressed = Vec::new();
        zstd::stream::copy_encode(std::io::repeat(0).take(len), &mut compressed, 3).unwrap();
        compressed
    }

    #[test]
    fn decompress_caps_program_at_max_bytes() {
        let at_cap = decompress(&zeros(MAX_PROGRAM_BYTES), ArtifactType::Program).unwrap();
        assert_eq!(at_cap.len() as u64, MAX_PROGRAM_BYTES);
        assert!(matches!(
            decompress(&zeros(MAX_PROGRAM_BYTES + 1), ArtifactType::Program),
            Err(ArtifactError::TooLarge(ArtifactType::Program))
        ));
    }

    #[test]
    fn decompress_allocates_no_more_than_the_cap() {
        let decoded = decompress(&zeros(MAX_PROGRAM_BYTES), ArtifactType::Program).unwrap();
        assert!(decoded.capacity() as u64 <= MAX_PROGRAM_BYTES + 1);
    }

    #[test]
    fn decompress_rejects_corrupt_input() {
        assert!(matches!(
            decompress(b"not zstd", ArtifactType::Program),
            Err(ArtifactError::Corrupt(ArtifactType::Program, _))
        ));
    }

    #[tokio::test]
    async fn deserialize_rejects_invalid_bincode() {
        let err = deserialize::<Vec<u8>>(b"not bincode".to_vec(), ArtifactType::Program)
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref(),
            Some(ArtifactError::Corrupt(ArtifactType::Program, _))
        ));
    }
}
