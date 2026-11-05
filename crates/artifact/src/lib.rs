pub mod redis;
pub mod s3;
pub mod s3_rest;
pub mod s3_sdk;

use anyhow::{anyhow, Result};
pub use sp1_prover_types::{ArtifactClient, ArtifactId, ArtifactType, InMemoryArtifactClient};
use std::io::Read;

/// Programs are customer bytes stored as is, so a small zstd bomb must not exhaust a node's memory.
const MAX_PROGRAM_BYTES: u64 = 512 * 1024 * 1024;

fn max_bytes(artifact_type: ArtifactType) -> u64 {
    match artifact_type {
        ArtifactType::Program => MAX_PROGRAM_BYTES,
        _ => u64::MAX,
    }
}

fn decompress(compressed: &[u8], cap: u64) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    zstd::Decoder::with_buffer(compressed)
        .and_then(|d| d.take(cap.saturating_add(1)).read_to_end(&mut decoded))
        .map_err(|e| anyhow!("Failed to decompress artifact: {}", e))?;
    if decoded.len() as u64 > cap {
        return Err(anyhow!("Artifact exceeds {cap} bytes"));
    }
    Ok(decoded)
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

    #[test]
    fn decompress_caps_output_at_max_bytes() {
        let compressed = zstd::encode_all(&[0u8; 16][..], 3).unwrap();
        assert_eq!(decompress(&compressed, 16).unwrap(), [0u8; 16]);
        assert!(decompress(&compressed, 15).is_err());
    }

    #[test]
    fn decompress_rejects_corrupt_input() {
        assert!(decompress(b"not zstd", 16).is_err());
    }
}
