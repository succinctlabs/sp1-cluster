use anyhow::Result;
use serde::{Deserialize, Serialize};
use sp1_cluster_artifact::{ArtifactClient, ArtifactType};

/// Sidecar blob stored at `program_sidecar_<hex(vk_hash)>` in the artifact store.
/// Indexes a cluster program artifact id by its verification-key hash so that
/// `get_program` can be answered without a database.
#[derive(Clone, Serialize, Deserialize)]
pub struct ProgramSidecar {
    pub vk_bytes: Vec<u8>,
    pub program_artifact_id: String,
}

pub fn sidecar_id(vk_hash: &[u8]) -> String {
    format!("program_sidecar_{}", hex::encode(vk_hash))
}

pub async fn store<A: ArtifactClient>(
    client: &A,
    vk_hash: &[u8],
    sidecar: ProgramSidecar,
) -> Result<()> {
    let id = sidecar_id(vk_hash);
    client
        .upload_with_type(&id, ArtifactType::UnspecifiedArtifactType, sidecar)
        .await
}

pub async fn load<A: ArtifactClient>(client: &A, vk_hash: &[u8]) -> Result<Option<ProgramSidecar>> {
    let id = sidecar_id(vk_hash);
    if !client
        .exists(&id, ArtifactType::UnspecifiedArtifactType)
        .await?
    {
        return Ok(None);
    }
    let sidecar: ProgramSidecar = client
        .download_with_type(&id, ArtifactType::UnspecifiedArtifactType)
        .await?;
    Ok(Some(sidecar))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sp1_cluster_artifact::InMemoryArtifactClient;

    #[tokio::test]
    async fn sidecar_roundtrip() {
        let client = InMemoryArtifactClient::new();
        let vk_hash = vec![1u8, 2, 3, 4];
        let sidecar = ProgramSidecar {
            vk_bytes: b"vk-bytes".to_vec(),
            program_artifact_id: "artifact_abcdef".into(),
        };

        assert!(load(&client, &vk_hash).await.unwrap().is_none());
        store(&client, &vk_hash, sidecar.clone()).await.unwrap();
        let got = load(&client, &vk_hash).await.unwrap().unwrap();
        assert_eq!(got.vk_bytes, sidecar.vk_bytes);
        assert_eq!(got.program_artifact_id, sidecar.program_artifact_id);
    }
}
