use std::time::Duration;

use anyhow::{anyhow, Result};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Config, Connection as RedisConnection, Pool, PoolConfig, Runtime};
use sp1_cluster_common::util::backoff_retry;
use tokio::task::JoinSet;
use tracing::instrument;

use crate::s3::S3ArtifactClient;
use crate::{ArtifactClient, ArtifactId, ArtifactType};

const CHUNK_SIZE: usize = 32 * 1024 * 1024;

/// FNV-1a hash
#[inline]
fn hash_string(s: &str) -> usize {
    s.bytes().fold(0, |hash, byte| {
        hash.wrapping_mul(16777619).wrapping_add(byte as usize)
    })
}

#[inline]
fn get_connection_idx(id: &str, num_redis_nodes: usize) -> usize {
    let hash = hash_string(id);
    hash % num_redis_nodes
}

#[derive(Clone)]
pub struct RedisArtifactClient {
    pub connection_pools: Vec<Pool>,
    backoff: ExponentialBackoff,
}

impl RedisArtifactClient {
    pub fn new(node_ips: Vec<String>, pool_max_size: usize) -> Self {
        tracing::info!("initializing redis pool");
        let pools = node_ips
            .iter()
            .map(|url| {
                let mut config = Config::from_url(url);
                config.pool = Some(PoolConfig::new(pool_max_size));
                config.create_pool(Some(Runtime::Tokio1)).unwrap()
            })
            .collect();
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(100))
            .with_max_interval(Duration::from_secs(4))
            .with_max_elapsed_time(None)
            .build();
        Self {
            connection_pools: pools,
            backoff,
        }
    }

    async fn get_redis_connection(
        &self,
        id: &str,
    ) -> Result<RedisConnection, backoff::Error<anyhow::Error>> {
        let idx = get_connection_idx(id, self.connection_pools.len());
        let now = std::time::Instant::now();
        tracing::debug!("getting redis connection: idx: {}", idx);
        let result = self.connection_pools[idx].get().await.map_err(|e| {
            tracing::warn!("Failed to get redis connection: {:?}", e);
            backoff::Error::transient(e.into())
        })?;
        tracing::debug!("got redis connection, elapsed: {:?}", now.elapsed());
        Ok(result)
    }

    async fn par_download_file(
        &self,
        artifact_type: ArtifactType,
        id: &str,
    ) -> Result<Vec<u8>, backoff::Error<anyhow::Error>> {
        let mut conn = self.get_redis_connection(id).await?;
        let now = std::time::Instant::now();
        let key = S3ArtifactClient::get_s3_key_from_id(artifact_type, id);

        // Check if it's a hash (chunked) or regular key
        let total_chunks: usize = conn
            .hlen(format!("{}:chunks", key))
            .await
            .map_err(|e| backoff::Error::transient(e.into()))?;

        if total_chunks == 0 {
            let result = conn
                .get::<_, Vec<u8>>(key)
                .await
                .map_err(|e| backoff::Error::transient(e.into()))?;
            let result = zstd::decode_all(result.as_slice()).unwrap();
            tracing::debug!("download took {:?}, size: {}", now.elapsed(), result.len());
            return Ok(result);
        }

        // Get total chunks
        let mut result = Vec::new();

        let mut join_set = JoinSet::new();

        // Download chunks in parallel
        for chunk_idx in 0..total_chunks {
            let key = key.clone();
            let id_clone = id.to_string();
            let mut conn = self.get_redis_connection(&id_clone).await?;
            join_set.spawn(async move {
                let chunk: Vec<u8> = conn.hget(format!("{}:chunks", key), chunk_idx).await?;
                Ok::<(usize, Vec<u8>), anyhow::Error>((chunk_idx, chunk))
            });
        }

        tracing::debug!(
            "total_chunks: {}, elapsed: {:?}",
            total_chunks,
            now.elapsed()
        );

        // Collect chunks in order
        let mut chunks = vec![Vec::new(); total_chunks];
        while let Some(res) = join_set.join_next().await {
            let (idx, chunk) = res.map_err(|e| backoff::Error::transient(e.into()))??;
            tracing::debug!(
                "idx: {}, chunk: {}, elapsed: {:?}",
                idx,
                chunk.len(),
                now.elapsed()
            );
            chunks[idx] = chunk;
        }

        // Combine chunks
        result.extend(chunks.into_iter().flatten());
        let decoded = tracing::info_span!("decoding").in_scope(|| {
            let decoded = zstd::decode_all(result.as_slice()).unwrap();
            tracing::debug!("decoded size: {}", decoded.len());
            decoded
        });
        tracing::debug!("download took {:?}, size: {}", now.elapsed(), decoded.len());
        Ok(decoded)
    }

    async fn par_upload_file(
        &self,
        artifact_type: ArtifactType,
        id: &str,
        serialized: &[u8],
    ) -> Result<(), backoff::Error<anyhow::Error>> {
        let mut conn = self.get_redis_connection(id).await?;
        let now = std::time::Instant::now();
        let key = S3ArtifactClient::get_s3_key_from_id(artifact_type, id);
        // TODO: only compress if it's larger than some threshold.
        let serialized =
            zstd::encode_all(serialized, 0).map_err(|e| backoff::Error::permanent(e.into()))?;
        let size = serialized.len();

        if serialized.len() <= CHUNK_SIZE {
            conn.set::<_, _, ()>(key, serialized)
                .await
                .map_err(|e| backoff::Error::transient(e.into()))?;
        } else {
            drop(conn);
            let chunks = serialized.chunks(CHUNK_SIZE);
            let mut join_set = JoinSet::new();

            // Upload chunks in parallel
            for (chunk_idx, chunk) in chunks.enumerate() {
                let key = key.clone();
                let id_clone = id.to_string();
                let chunk = chunk.to_vec();
                let mut conn = self.get_redis_connection(&id_clone).await?;
                join_set.spawn(async move {
                    let _: () = conn
                        .hset(format!("{}:chunks", key), chunk_idx, chunk)
                        .await?;
                    Ok::<(), anyhow::Error>(())
                });
            }
            tracing::debug!("spawned all chunks, elapsed: {:?}", now.elapsed());

            // Wait for all uploads to complete
            while let Some(res) = join_set.join_next().await {
                res.map_err(|e| backoff::Error::transient(e.into()))??;
                tracing::debug!("joined chunk, elapsed: {:?}", now.elapsed());
            }
        }

        tracing::debug!("upload took {:?}, size: {}", now.elapsed(), size);
        Ok(())
    }
}

#[async_trait::async_trait]
impl ArtifactClient for RedisArtifactClient {
    #[instrument(name = "upload", level = "info", fields(id = artifact.id()), skip(self, artifact, data))]
    async fn upload_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> Result<()> {
        backoff_retry(self.backoff.clone(), || async {
            self.par_upload_file(artifact_type, artifact.id(), &data)
                .await
        })
        .await
        .map_err(|e| anyhow!(e))
    }

    #[instrument(name = "download", level = "info", fields(id = artifact.id()), skip(self, artifact))]
    async fn download_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
    ) -> Result<Vec<u8>> {
        backoff_retry(self.backoff.clone(), || async {
            self.par_download_file(artifact_type, artifact.id()).await
        })
        .await
        .map_err(|e| anyhow!(e))
    }

    async fn exists(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
    ) -> Result<bool> {
        let mut conn = self
            .get_redis_connection(artifact.id())
            .await
            .map_err(|e| anyhow!(e))?;
        let mut conn2 = conn.clone();
        let key = S3ArtifactClient::get_s3_key_from_id(artifact_type, artifact.id());
        let (res, chunks) =
            tokio::try_join!(conn.exists(&key), conn2.exists(format!("{}:chunks", key)))?;
        Ok(res || chunks)
    }
}
