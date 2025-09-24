use crate::util::{await_blocking, await_scoped_vec};
use anyhow::{anyhow, Result};
use mti::prelude::{MagicTypeIdExt, V7};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::Instrument;

pub mod redis;
pub mod s3;
pub mod s3_rest;
pub mod s3_sdk;
pub mod util;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ArtifactType {
    UnspecifiedArtifactType,
    Program,
    Stdin,
    Proof,
    Groth16Circuit,
    PlonkCircuit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact(pub String);

impl Artifact {
    pub fn to_id(self) -> String {
        self.0
    }
}

impl From<String> for Artifact {
    fn from(value: String) -> Self {
        Self(value)
    }
}

pub trait ArtifactId: Send + Sync {
    fn id(&self) -> &str;
}

impl ArtifactId for Artifact {
    fn id(&self) -> &str {
        &self.0
    }
}

impl ArtifactId for String {
    fn id(&self) -> &str {
        self
    }
}

pub struct ArtifactBatch(Vec<Artifact>);

impl ArtifactBatch {
    pub async fn upload<T: Serialize + Send + Sync>(
        &self,
        client: &impl ArtifactClient,
        items: &[T],
    ) -> Result<()> {
        await_scoped_vec(self.0.iter().zip(items).map(|(artifact, item)| {
            let client = client.clone();
            let artifact = artifact.clone();
            async move { client.upload(&artifact, item).await }
        }))
        .await?;
        Ok(())
    }

    pub async fn download<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        client: &impl ArtifactClient,
    ) -> Result<Vec<T>> {
        let result = await_scoped_vec(self.0.iter().map(|artifact| {
            let client = client.clone();
            let artifact = artifact.clone();
            async move { client.download::<T>(&artifact).await }
        }))
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(result)
    }

    pub fn to_vec(self) -> Vec<Artifact> {
        self.0
    }
}

impl From<ArtifactBatch> for Vec<Artifact> {
    fn from(val: ArtifactBatch) -> Self {
        val.0
    }
}

impl From<Vec<Artifact>> for ArtifactBatch {
    fn from(value: Vec<Artifact>) -> Self {
        Self(value)
    }
}

#[async_trait::async_trait]
pub trait ArtifactClient: Send + Sync + Clone + 'static {
    async fn upload_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> Result<()>;

    async fn download_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
    ) -> Result<Vec<u8>>;

    async fn exists(&self, artifact: &impl ArtifactId, artifact_type: ArtifactType)
        -> Result<bool>;

    async fn delete(&self, artifact: &impl ArtifactId, artifact_type: ArtifactType) -> Result<()>;

    async fn delete_batch(
        &self,
        artifacts: &[impl ArtifactId],
        artifact_type: ArtifactType,
    ) -> Result<()>;

    async fn try_delete(&self, artifact: &impl ArtifactId, artifact_type: ArtifactType) {
        if let Err(e) = self.delete(artifact, artifact_type).await {
            tracing::warn!("Failed to delete artifact {}: {:?}", artifact.id(), e);
        }
    }

    async fn try_delete_batch(&self, artifacts: &[impl ArtifactId], artifact_type: ArtifactType) {
        if let Err(e) = self.delete_batch(artifacts, artifact_type).await {
            tracing::warn!("Failed to delete artifact batch: {:?}", e);
        }
    }

    async fn upload_with_type<T: Serialize + Send + Sync>(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        item: T,
    ) -> Result<()> {
        let data = await_blocking(|| bincode::serialize(&item))
            .instrument(tracing::info_span!("serialize"))
            .await
            .unwrap()?;
        drop(item);
        self.upload_raw(artifact, artifact_type, data).await
    }

    async fn download_with_type<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
    ) -> Result<T> {
        let bytes = self.download_raw(artifact, artifact_type).await?;
        let deserialized =
            tokio::task::spawn_blocking(move || bincode::deserialize(&bytes)).await??;
        Ok(deserialized)
    }

    fn create_artifact(&self) -> Result<Artifact> {
        Ok("artifact".create_type_id::<V7>().to_string().into())
    }

    fn create_artifacts(&self, count: usize) -> Result<ArtifactBatch> {
        Ok((0..count)
            .map(|_| "artifact".create_type_id::<V7>().to_string().into())
            .collect::<Vec<_>>()
            .into())
    }

    async fn upload<T: Serialize + Send + Sync>(
        &self,
        artifact: &impl ArtifactId,
        item: T,
    ) -> Result<()> {
        self.upload_with_type(artifact, ArtifactType::UnspecifiedArtifactType, item)
            .await
    }

    async fn upload_proof<T: Serialize + Send + Sync>(
        &self,
        artifact: &impl ArtifactId,
        item: T,
    ) -> Result<()> {
        self.upload_with_type(artifact, ArtifactType::Proof, item)
            .await
    }

    async fn download<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        artifact: &impl ArtifactId,
    ) -> Result<T> {
        self.download_with_type(artifact, ArtifactType::UnspecifiedArtifactType)
            .await
    }

    async fn download_program(&self, artifact: &impl ArtifactId) -> Result<Vec<u8>> {
        self.download_with_type(artifact, ArtifactType::Program)
            .await
    }

    async fn download_stdin<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        artifact: &impl ArtifactId,
    ) -> Result<T> {
        self.download_with_type(artifact, ArtifactType::Stdin).await
    }

    async fn download_stdin_bytes(&self, artifact: &impl ArtifactId) -> Result<Vec<u8>> {
        self.download_with_type(artifact, ArtifactType::Stdin).await
    }

    /// Add task reference for an artifact
    async fn add_ref(&self, _artifact: &impl ArtifactId, _task_id: &str) -> Result<()> {
        // Default implementation does nothing (for non-Redis clients)
        Ok(())
    }

    /// Remove task reference and delete artifact if no references remain
    async fn remove_ref(
        &self,
        _artifact: &impl ArtifactId,
        _artifact_type: ArtifactType,
        _task_id: &str,
    ) -> Result<bool> {
        Ok(false)
    }
}

#[derive(Clone)]
pub struct InMemoryArtifactClient {
    pub artifacts: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl InMemoryArtifactClient {
    pub fn new() -> Self {
        Self {
            artifacts: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryArtifactClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ArtifactClient for InMemoryArtifactClient {
    async fn upload_raw(
        &self,
        artifact: &impl ArtifactId,
        _artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> Result<()> {
        let mut artifacts = self.artifacts.lock().await;
        artifacts.insert(artifact.id().to_string(), data.clone());
        Ok(())
    }

    async fn download_raw(
        &self,
        artifact: &impl ArtifactId,
        _artifact_type: ArtifactType,
    ) -> Result<Vec<u8>> {
        let artifacts = self.artifacts.lock().await;
        let bytes = artifacts
            .get(artifact.id())
            .ok_or_else(|| anyhow!("artifact not found"))?;
        Ok(bytes.clone())
    }

    async fn exists(
        &self,
        artifact: &impl ArtifactId,
        _artifact_type: ArtifactType,
    ) -> Result<bool> {
        let artifacts = self.artifacts.lock().await;
        Ok(artifacts.contains_key(artifact.id()))
    }

    async fn delete(&self, artifact: &impl ArtifactId, _artifact_type: ArtifactType) -> Result<()> {
        let mut artifacts = self.artifacts.lock().await;
        artifacts.remove(artifact.id());
        Ok(())
    }

    async fn delete_batch(
        &self,
        artifacts: &[impl ArtifactId],
        _artifact_type: ArtifactType,
    ) -> Result<()> {
        let mut artifact_map = self.artifacts.lock().await;
        for artifact in artifacts {
            artifact_map.remove(artifact.id());
        }
        Ok(())
    }
}
