use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use deadpool_redis::redis::{AsyncCommands, SetExpiry, SetOptions};
use deadpool_redis::{Config, Connection as RedisConnection, Pool, PoolConfig, Runtime};
use sp1_cluster_common::util::backoff_retry;
use sp1_prover_types::{ArtifactClient, ArtifactId, ArtifactType, ShardPermit};
use tokio::sync::{Mutex, OnceCell, Semaphore};
use tokio::task::JoinSet;
use tracing::{instrument, Instrument};

const CHUNK_SIZE: usize = 32 * 1024 * 1024;
const ARTIFACT_TIMEOUT_SECONDS: u64 = 4 * 60 * 60; // 4 hours

/// Conservative cap on a single shard artifact. Override via `PROVE_SHARD_MAX_BYTES`.
const DEFAULT_SHARD_MAX_BYTES: u64 = 20 * 1024 * 1024;

/// Admission ceiling as fraction of `maxmemory`; remainder is headroom for allocator bloat.
pub const MEMORY_BUDGET_FRACTION: f64 = 0.80;

/// Permit pool ceiling; gap below admission absorbs non-permit-gated writes.
const INPUT_BUDGET_FRACTION: f64 = 0.50;

const _: () = assert!(
    INPUT_BUDGET_FRACTION < MEMORY_BUDGET_FRACTION,
    "permit pool must reserve a gap below the admission ceiling"
);

/// Uploads ≤ this size bypass admission; bounded structurally by concurrent task count.
const ADMISSION_BYPASS_BYTES: u64 = DEFAULT_SHARD_MAX_BYTES / 10;

/// Floor when `maxmemory` is unreadable / 0 — prevents wedging the pipeline.
const MIN_PERMITS_PER_NODE: usize = 4;

/// Bound on first-acquire `INFO memory`; falls back to the floor on timeout.
const MAXMEMORY_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Admission re-check cadence while blocked.
const ADMISSION_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Hard cap on admission wait — past this, fail fast.
const ADMISSION_MAX_WAIT: Duration = Duration::from_secs(2 * 60);

/// Throttle for "blocking upload" warn logs while waiting.
const ADMISSION_LOG_INTERVAL: Duration = Duration::from_secs(10);

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

/// Per-node admission state. Mutex serializes the check-then-reserve;
/// atomic carries reservations across the async-to-sync boundary
/// (incremented under the lock, decremented in [`AdmissionGuard::drop`]).
#[derive(Default)]
struct Admission {
    decide: Mutex<()>,
    in_flight: AtomicU64,
}

/// RAII reservation token. Must outlive the upload so `in_flight` stays
/// counted until the write lands; `None` is a no-op for fail-open paths.
#[must_use = "AdmissionGuard must be held until the upload completes; binding to `_` reintroduces the TOCTOU bug"]
pub struct AdmissionGuard {
    inner: Option<Reservation>,
}

struct Reservation {
    admission: Arc<Vec<Admission>>,
    idx: usize,
    bytes: u64,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if let Some(r) = self.inner.take() {
            r.admission[r.idx]
                .in_flight
                .fetch_sub(r.bytes, Ordering::Release);
        }
    }
}

#[derive(Clone)]
pub struct RedisArtifactClient {
    pub connection_pools: Vec<Pool>,
    backoff: ExponentialBackoff,
    /// One permit pool per Redis shard node. Lazily sized on first use from
    /// `INFO memory` (maxmemory); one permit represents one worst-case shard.
    node_semaphores: Arc<Vec<OnceCell<Arc<Semaphore>>>>,
    /// One admission state per Redis shard node.
    admission: Arc<Vec<Admission>>,
}

impl RedisArtifactClient {
    pub fn new(node_ips: Vec<String>, pool_max_size: usize) -> Self {
        tracing::info!("initializing redis pool");
        let pools: Vec<_> = node_ips
            .iter()
            .map(|url| {
                let mut config = Config::from_url(url);
                config.pool = Some(PoolConfig::new(pool_max_size));
                config.create_pool(Some(Runtime::Tokio1)).unwrap()
            })
            .collect();
        let backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(100))
            .with_max_interval(Duration::from_secs(1))
            .with_max_elapsed_time(Some(Duration::from_secs(1)))
            .build();
        let node_semaphores = Arc::new((0..pools.len()).map(|_| OnceCell::new()).collect());
        let admission = Arc::new((0..pools.len()).map(|_| Admission::default()).collect());
        Self {
            connection_pools: pools,
            backoff,
            node_semaphores,
            admission,
        }
    }

    /// Return the per-node shard semaphore, initializing it on first call.
    async fn node_semaphore(&self, idx: usize) -> Arc<Semaphore> {
        self.node_semaphores[idx]
            .get_or_init(|| async move {
                let permits = self.compute_permits_for_node(idx).await;
                Arc::new(Semaphore::new(permits))
            })
            .await
            .clone()
    }

    /// Refuse to start when `maxmemory` is unset on any node. Without this,
    /// the lazy path silently floors at [`MIN_PERMITS_PER_NODE`] and caps
    /// throughput at ~4 concurrent shards regardless of GPU count.
    pub async fn validate_config(&self) -> Result<()> {
        for idx in 0..self.connection_pools.len() {
            let (_, maxmemory) =
                tokio::time::timeout(MAXMEMORY_QUERY_TIMEOUT, self.query_memory(idx))
                    .await
                    .map_err(|_| anyhow!("Redis shard {idx}: INFO memory timed out"))??;
            if !matches!(maxmemory, Some(v) if v > 0) {
                return Err(anyhow!(
                    "Redis shard {idx}: maxmemory is unset (0). \
                     Set it (e.g. `CONFIG SET maxmemory <N>gb`, `--maxmemory <N>gb`, \
                     or `REDIS_EXTRA_FLAGS=--maxmemory <N>gb` on Bitnami images). \
                     Size below the host/cgroup limit to leave allocator headroom."
                ));
            }
        }
        Ok(())
    }

    /// Permit count for shard `idx`, derived from `maxmemory`. Falls back to
    /// [`MIN_PERMITS_PER_NODE`] when `maxmemory` is unreadable or 0.
    async fn compute_permits_for_node(&self, idx: usize) -> usize {
        let query = tokio::time::timeout(MAXMEMORY_QUERY_TIMEOUT, self.query_memory(idx)).await;
        let maxmemory = match query {
            Ok(Ok((_, Some(v)))) if v > 0 => v,
            Ok(Ok(_)) => {
                tracing::error!(
                    shard = idx,
                    "Redis maxmemory=0; throughput capped at {MIN_PERMITS_PER_NODE} permits. \
                     Set maxmemory and restart (see validate_config)."
                );
                return MIN_PERMITS_PER_NODE;
            }
            Ok(Err(e)) => {
                tracing::warn!(shard = idx, error = %e, "maxmemory query failed; using permit floor");
                return MIN_PERMITS_PER_NODE;
            }
            Err(_) => {
                tracing::warn!(shard = idx, "maxmemory query timed out; using permit floor");
                return MIN_PERMITS_PER_NODE;
            }
        };
        let max_shard_bytes = std::env::var("PROVE_SHARD_MAX_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_SHARD_MAX_BYTES);
        let input_budget = (maxmemory as f64 * INPUT_BUDGET_FRACTION) as u64;
        let permits = ((input_budget / max_shard_bytes) as usize).max(MIN_PERMITS_PER_NODE);
        tracing::info!(
            shard = idx,
            maxmemory_bytes = maxmemory,
            max_shard_bytes,
            input_budget_fraction = INPUT_BUDGET_FRACTION,
            admission_budget_fraction = MEMORY_BUDGET_FRACTION,
            permits,
            "ProveShard permit pool sized"
        );
        permits
    }

    /// Return `(used_memory, maxmemory)` from `INFO memory` on shard `idx`.
    /// Either field is `None` if the line is missing / unparseable.
    async fn query_memory(&self, idx: usize) -> Result<(Option<u64>, Option<u64>)> {
        let mut conn = self.connection_pools[idx]
            .get()
            .await
            .map_err(|e| anyhow!("pool get: {e}"))?;
        let info: String = deadpool_redis::redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut *conn)
            .await
            .map_err(|e| anyhow!("INFO memory: {e}"))?;
        let parse = |key: &str| {
            info.lines()
                .find_map(|line| line.strip_prefix(key))
                .and_then(|v| v.trim().parse::<u64>().ok())
        };
        Ok((parse("used_memory:"), parse("maxmemory:")))
    }

    /// Reserve `incoming_bytes` against the admission budget. Blocks while
    /// `used + in_flight + incoming > budget`; caller must hold the guard
    /// until the write lands. Fail-open on INFO errors / unlimited mode.
    /// Writes ≤ [`ADMISSION_BYPASS_BYTES`] skip the gate (see const docs).
    async fn check_admission(&self, key: &str, incoming_bytes: u64) -> Result<AdmissionGuard> {
        if incoming_bytes <= ADMISSION_BYPASS_BYTES {
            return Ok(AdmissionGuard { inner: None });
        }
        let idx = get_connection_idx(key, self.connection_pools.len());
        let admission = &self.admission[idx];
        let noop = || AdmissionGuard { inner: None };
        let start = std::time::Instant::now();
        let mut last_log = start;
        let mut waiting = false;
        loop {
            let guard = admission.decide.lock().await;
            let (used, max) = match self.query_memory(idx).await {
                Ok((Some(used), Some(max))) if max > 0 => (used, max),
                Ok(_) => return Ok(noop()), // unlimited / parse miss
                Err(e) => {
                    tracing::warn!(shard = idx, error = %e, "admission INFO memory failed; fail-open");
                    return Ok(noop());
                }
            };
            let budget = (max as f64 * MEMORY_BUDGET_FRACTION) as u64;
            let in_flight = admission.in_flight.load(Ordering::Acquire);
            let projected = used
                .saturating_add(in_flight)
                .saturating_add(incoming_bytes);
            if projected <= budget {
                admission
                    .in_flight
                    .fetch_add(incoming_bytes, Ordering::AcqRel);
                drop(guard);
                if waiting {
                    tracing::info!(
                        shard = idx,
                        waited_ms = start.elapsed().as_millis() as u64,
                        "admission cleared; upload resumed"
                    );
                }
                return Ok(AdmissionGuard {
                    inner: Some(Reservation {
                        admission: Arc::clone(&self.admission),
                        idx,
                        bytes: incoming_bytes,
                    }),
                });
            }
            drop(guard);

            let elapsed = start.elapsed();
            if elapsed > ADMISSION_MAX_WAIT {
                tracing::error!(
                    shard = idx,
                    used,
                    max,
                    budget,
                    in_flight,
                    incoming = incoming_bytes,
                    waited_secs = elapsed.as_secs(),
                    "admission wait cap exceeded; failing upload"
                );
                return Err(anyhow!(
                    "Redis shard {idx} admission wait > {:?}: used={used} in_flight={in_flight} budget={budget} incoming={incoming_bytes}",
                    ADMISSION_MAX_WAIT
                ));
            }

            if !waiting || last_log.elapsed() >= ADMISSION_LOG_INTERVAL {
                tracing::warn!(
                    shard = idx,
                    used,
                    max,
                    budget,
                    in_flight,
                    incoming = incoming_bytes,
                    waited_ms = elapsed.as_millis() as u64,
                    "Redis near capacity; blocking upload (backpressure)"
                );
                last_log = std::time::Instant::now();
            }
            waiting = true;
            tokio::time::sleep(ADMISSION_POLL_INTERVAL).await;
        }
    }

    async fn get_redis_connection(
        &self,
        id: &str,
    ) -> Result<RedisConnection, backoff::Error<anyhow::Error>> {
        let idx = get_connection_idx(id, self.connection_pools.len());
        let result = self.connection_pools[idx]
            .get()
            .instrument(tracing::info_span!("get_redis_connection",))
            .await
            .map_err(|e| {
                tracing::warn!("Failed to get redis connection: {:?}", e);
                backoff::Error::transient(e.into())
            })?;
        Ok(result)
    }

    async fn par_download_file(
        &self,
        _: ArtifactType,
        key: &str,
    ) -> Result<Vec<u8>, backoff::Error<anyhow::Error>> {
        let mut conn = self.get_redis_connection(key).await?;
        let now = std::time::Instant::now();
        let key = key.to_string();

        // Check if it's a hash (chunked) or regular key
        let total_chunks: usize = conn
            .hlen(format!("{key}:chunks"))
            .await
            .map_err(|e| backoff::Error::transient(e.into()))?;

        if total_chunks == 0 {
            let result = conn
                .get::<_, Vec<u8>>(key)
                .await
                .map_err(|e| backoff::Error::transient(e.into()))?;
            tracing::info!("download took {:?}, size: {}", now.elapsed(), result.len());
            return Ok(result);
        }

        // Get total chunks
        let mut result = Vec::new();

        let mut join_set = JoinSet::new();

        // Download chunks in parallel
        for chunk_idx in 0..total_chunks {
            let key = key.clone();
            let id_clone = key.to_string();
            let mut conn = self.get_redis_connection(&id_clone).await?;
            join_set.spawn(async move {
                let chunk: Vec<u8> = conn.hget(format!("{key}:chunks"), chunk_idx).await?;
                Ok::<(usize, Vec<u8>), anyhow::Error>((chunk_idx, chunk))
            });
        }

        tracing::info!(
            "total_chunks: {}, elapsed: {:?}",
            total_chunks,
            now.elapsed()
        );

        // Collect chunks in order
        let mut chunks = vec![Vec::new(); total_chunks];
        while let Some(res) = join_set.join_next().await {
            let (idx, chunk) = res.map_err(|e| backoff::Error::transient(e.into()))??;
            tracing::info!(
                "idx: {}, chunk: {}, elapsed: {:?}",
                idx,
                chunk.len(),
                now.elapsed()
            );
            chunks[idx] = chunk;
        }

        // Combine chunks
        result.extend(chunks.into_iter().flatten());
        tracing::info!("download took {:?}, size: {}", now.elapsed(), result.len());
        Ok(result)
    }

    async fn par_upload_file(
        &self,
        artifact_type: ArtifactType,
        key: &str,
        serialized: &[u8],
    ) -> Result<(), backoff::Error<anyhow::Error>> {
        let mut conn = self.get_redis_connection(key).await?;
        let now = std::time::Instant::now();
        let key = key.to_string();
        let size = serialized.len();

        if serialized.len() <= CHUNK_SIZE {
            let mut options = SetOptions::default();
            if !matches!(artifact_type, ArtifactType::Program) {
                options = options.with_expiration(SetExpiry::EX(ARTIFACT_TIMEOUT_SECONDS));
            }

            conn.set_options::<_, _, ()>(key, serialized, options)
                .await
                .map_err(|e| backoff::Error::transient(e.into()))?;
        } else {
            drop(conn);
            let chunks = serialized.chunks(CHUNK_SIZE);
            let mut join_set = JoinSet::new();

            // HSET + EXPIRE because HSETEX isn't available on managed Redis.
            // One artifact = one hash, so whole-hash TTL ≡ per-field TTL.
            for (chunk_idx, chunk) in chunks.enumerate() {
                let key = key.clone();
                let chunk = chunk.to_vec();
                let mut conn = self.get_redis_connection(&key).await?;
                join_set.spawn(async move {
                    let hash_key = format!("{key}:chunks");
                    let _: usize = conn.hset(&hash_key, chunk_idx, chunk).await?;
                    if !matches!(artifact_type, ArtifactType::Program) {
                        let _: bool = conn
                            .expire(&hash_key, ARTIFACT_TIMEOUT_SECONDS as i64)
                            .await?;
                    }
                    Ok::<(), anyhow::Error>(())
                });
            }
            tracing::info!("spawned all chunks, elapsed: {:?}", now.elapsed());

            // Wait for all uploads to complete
            while let Some(res) = join_set.join_next().await {
                res.map_err(|e| backoff::Error::transient(e.into()))??;
                tracing::info!("joined chunk, elapsed: {:?}", now.elapsed());
            }
        }

        tracing::info!("upload took {:?}, size: {}", now.elapsed(), size);
        Ok(())
    }
}

const TRANSFER_TIMEOUT: Duration = Duration::from_secs(60);

impl RedisArtifactClient {
    /// Admit (block on backpressure), then backoff+timeout the actual write.
    /// Admission sits outside `TRANSFER_TIMEOUT` so block-waits don't trip
    /// the per-attempt timeout; retries reuse the initial reservation.
    async fn upload_to_transport(
        &self,
        artifact_type: ArtifactType,
        artifact_id: &str,
        data: &[u8],
    ) -> Result<()> {
        // `_guard` (not `_`) binds to scope end so the reservation is held
        // for the full duration of the upload, not just the admission call.
        let _guard = self.check_admission(artifact_id, data.len() as u64).await?;

        backoff_retry(self.backoff.clone(), || async {
            match tokio::time::timeout(
                TRANSFER_TIMEOUT,
                self.par_upload_file(artifact_type, artifact_id, data),
            )
            .await
            {
                Ok(result) => result,
                Err(e) => {
                    tracing::warn!(
                        "Upload attempt timed out after {:?} for artifact: {}",
                        e,
                        artifact_id
                    );
                    Err(backoff::Error::transient(anyhow!(
                        "Upload timed out after {:?}",
                        e
                    )))
                }
            }
        })
        .await
        .map_err(|e| {
            let err_msg = e.to_string();
            if err_msg.contains("timed out") {
                anyhow!(
                    "Upload operation timed out after all retries for artifact: {} (timeout: {:?} per attempt)",
                    artifact_id,
                    TRANSFER_TIMEOUT
                )
            } else {
                anyhow!("Upload failed for artifact {}: {}", artifact_id, e)
            }
        })
    }
}

impl ArtifactClient for RedisArtifactClient {
    /// Reserve a permit on the node that will host `artifact`. Held until
    /// the artifact is deleted; release frees the slot for the next caller.
    async fn acquire_shard_permit(&self, artifact: &impl ArtifactId) -> ShardPermit {
        let num_nodes = self.connection_pools.len();
        if num_nodes == 0 {
            return ShardPermit::noop();
        }
        let idx = get_connection_idx(artifact.id(), num_nodes);
        let sem = self.node_semaphore(idx).await;
        match sem.acquire_owned().await {
            Ok(permit) => ShardPermit::new(permit),
            Err(_) => {
                // Semaphore closed: fail-open rather than wedge the producer.
                tracing::warn!(shard = idx, "semaphore closed, releasing unbounded permit");
                ShardPermit::noop()
            }
        }
    }

    #[instrument(name = "upload", level = "debug", fields(id = artifact.id()), skip(self, artifact, data))]
    async fn upload_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> Result<()> {
        // zstd level 0: fast compression for ephemeral Redis storage (4-hour TTL)
        let compressed = zstd::encode_all(data.as_slice(), 0)
            .map_err(|e| anyhow!("Failed to compress artifact: {}", e))?;
        self.upload_to_transport(artifact_type, artifact.id(), &compressed)
            .await
    }

    #[instrument(name = "download", level = "debug", fields(id = artifact.id()), skip(self, artifact))]
    async fn download_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
    ) -> Result<Vec<u8>> {
        let artifact_id = artifact.id();
        let timeout_duration = Duration::from_secs(60);

        let compressed = backoff_retry(self.backoff.clone(), || async {
            match tokio::time::timeout(
                timeout_duration,
                self.par_download_file(artifact_type, artifact_id),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!(
                        "Download attempt timed out after {:?} for artifact: {}",
                        timeout_duration,
                        artifact_id
                    );
                    Err(backoff::Error::transient(anyhow!(
                        "Download timed out after {:?}",
                        timeout_duration
                    )))
                }
            }
        })
        .await
        .map_err(|e| {
            let err_msg = e.to_string();
            if err_msg.contains("timed out") {
                anyhow!(
                    "Download operation timed out after all retries for artifact: {} (timeout: {:?} per attempt)",
                    artifact_id,
                    timeout_duration
                )
            } else {
                anyhow!("Download failed for artifact {}: {}", artifact_id, e)
            }
        })?;

        let decoded = zstd::decode_all(compressed.as_slice())
            .map_err(|e| anyhow!("Failed to decompress artifact: {}", e))?;
        Ok(decoded)
    }

    async fn exists(&self, artifact: &impl ArtifactId, _: ArtifactType) -> Result<bool> {
        let mut conn = self
            .get_redis_connection(artifact.id())
            .await
            .map_err(|e| anyhow!(e))?;
        let mut conn2 = conn.clone();
        let key = artifact.id();
        let (res, chunks) =
            tokio::try_join!(conn.exists(key), conn2.exists(format!("{key}:chunks")))?;
        Ok(res || chunks)
    }

    async fn delete(&self, artifact: &impl ArtifactId, _: ArtifactType) -> Result<()> {
        let mut conn = self
            .get_redis_connection(artifact.id())
            .await
            .map_err(|e| anyhow!(e))?;
        let mut conn2 = conn.clone();
        let key = artifact.id();
        let _: (u64, u64) =
            tokio::try_join!(conn.unlink(key), conn2.unlink(format!("{key}:chunks")))?;
        Ok(())
    }

    async fn delete_batch(&self, artifacts: &[impl ArtifactId], _: ArtifactType) -> Result<()> {
        if artifacts.is_empty() {
            return Ok(());
        }

        // Group artifacts by Redis node
        let mut node_groups: std::collections::HashMap<usize, Vec<String>> =
            std::collections::HashMap::new();
        for artifact in artifacts {
            let node_idx = get_connection_idx(artifact.id(), self.connection_pools.len());
            let entry = node_groups.entry(node_idx).or_default();
            entry.push(artifact.id().to_string());
            entry.push(format!("{}:chunks", artifact.id()));
        }

        // Delete from each node in parallel
        let mut tasks = Vec::new();
        for (node_idx, keys) in node_groups {
            let pool = self.connection_pools[node_idx].clone();
            let keys = keys.clone();
            tasks.push(tokio::spawn(async move {
                let mut conn = pool.get().await?;
                let deleted_count: u64 = conn.unlink(&keys).await?;
                Ok::<u64, anyhow::Error>(deleted_count)
            }));
        }

        // Wait for all deletions to complete
        for task in tasks {
            task.await??;
        }

        Ok(())
    }

    /// Add task reference for an artifact
    async fn add_ref(&self, artifact: &impl ArtifactId, key: &str) -> Result<()> {
        let id = artifact.id();
        let redis_key = format!("refs:{id}");

        backoff_retry(self.backoff.clone(), || async {
            let mut conn = self.get_redis_connection(id).await?;

            // Add task_id to the set of references
            let _: () = conn
                .sadd(&redis_key, key)
                .await
                .map_err(|e| backoff::Error::transient(e.into()))?;

            // Set expiration to prevent memory leaks
            conn.expire::<_, ()>(&redis_key, ARTIFACT_TIMEOUT_SECONDS as i64)
                .await
                .map_err(|e| backoff::Error::transient(e.into()))?;

            Ok(())
        })
        .await
        .map_err(|e: backoff::Error<anyhow::Error>| anyhow!(e))
    }

    /// Remove task reference and delete artifact if no references remain
    async fn remove_ref(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        key: &str,
    ) -> Result<bool> {
        let artifact_id = artifact.id();

        let should_delete = backoff_retry(self.backoff.clone(), || async {
            let mut conn = self.get_redis_connection(artifact_id).await?;

            let redis_key = format!("refs:{artifact_id}");

            // Remove task_id from the set
            let _: () = conn
                .srem(&redis_key, key)
                .await
                .map_err(|e| backoff::Error::transient(e.into()))?;

            // Check if set is empty
            let count: i64 = conn
                .scard(&redis_key)
                .await
                .map_err(|e| backoff::Error::transient(e.into()))?;

            if count <= 0 {
                // Clean up the set key
                conn.del::<_, ()>(&redis_key)
                    .await
                    .map_err(|e| backoff::Error::transient(e.into()))?;

                Ok(true) // Should delete
            } else {
                Ok(false) // Still has references
            }
        })
        .await
        .map_err(|e: backoff::Error<anyhow::Error>| anyhow!(e))?;

        if should_delete {
            // Delete the artifact since no references remain
            self.try_delete(artifact, artifact_type).await?;
        }
        Ok(should_delete)
    }
}

impl crate::CompressedUpload for RedisArtifactClient {
    #[instrument(name = "upload_compressed", level = "debug", fields(id = artifact.id()), skip(self, artifact, data))]
    async fn upload_raw_compressed(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> Result<()> {
        // Data is already zstd-compressed, write directly to transport layer
        self.upload_to_transport(artifact_type, artifact.id(), &data)
            .await
    }
}
