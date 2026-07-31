use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use deadpool_redis::redis::{AsyncCommands, SetExpiry, SetOptions};
use deadpool_redis::{Config, Connection as RedisConnection, Pool, PoolConfig, Runtime};
use mti::prelude::{MagicTypeIdExt, V7};
use sp1_cluster_common::util::backoff_retry;
use sp1_prover_types::{ArtifactClient, ArtifactId, ArtifactType, ShardPermit};
use tokio::sync::{Mutex, OnceCell, Semaphore};
use tokio::task::JoinSet;
use tracing::{instrument, Instrument};

/// Values above this go into a chunk hash instead of one Redis string.
/// Override via `ARTIFACT_CHUNK_SIZE_BYTES`, which lets a test environment
/// reach the chunked path on workloads whose artifacts never approach the
/// default.
const DEFAULT_CHUNK_SIZE: usize = 32 * 1024 * 1024;

/// Resolved once; a zero or unparseable override falls back to the default,
/// since a zero chunk size would split every artifact into empty fields.
static CHUNK_SIZE: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("ARTIFACT_CHUNK_SIZE_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_CHUNK_SIZE)
});

/// Default Redis artifact retention. Override via `ARTIFACT_TIMEOUT_SECONDS`.
///
/// Caps a proof's wall-clock: stdin and intermediate artifacts expire this long after write
/// (Program artifacts are exempt). Longer retention raises steady-state Redis memory.
const DEFAULT_ARTIFACT_TIMEOUT_SECONDS: u64 = 6 * 60 * 60; // 6 hours

/// Resolved once; clamped to `[1, i64::MAX]` so the `EXPIRE` cast can't go negative.
static ARTIFACT_TIMEOUT_SECONDS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("ARTIFACT_TIMEOUT_SECONDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&v| v > 0 && v <= i64::MAX as u64)
        .unwrap_or(DEFAULT_ARTIFACT_TIMEOUT_SECONDS)
});

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

/// Per-attempt deadline for one artifact write.
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(60);

/// Staging keys outlive one upload attempt, nothing more: an abandoned attempt
/// (timeout, crash) reclaims its artifact-sized memory in minutes, not the 6h
/// retention window. Retention is set on the published key after RENAME.
const STAGING_TTL_SECONDS: i64 = 2 * TRANSFER_TIMEOUT.as_secs() as i64;

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

/// Hash field holding the chunk count the writer committed to. Its presence
/// marks a hash as fully published, which no count of data fields can prove:
/// an interrupted writer that appended straight to the final key leaves a
/// short hash whose length may still match some other upload's count. Never
/// collides with a chunk field, which is a decimal index.
const CHUNK_COUNT_FIELD: &str = "__n";

#[inline]
fn chunk_key(id: &str) -> String {
    format!("{id}:chunks")
}

/// Fresh per attempt, so concurrent or abandoned uploads of the same artifact
/// never share staging state.
#[inline]
fn staging_chunk_key(id: &str) -> String {
    format!("{}:{}", chunk_key(id), "stage".create_type_id::<V7>())
}

/// Marks a failure retryable; whether a retry actually runs is the backoff
/// policy's budget, not this classification.
#[inline]
fn transient(e: impl Into<anyhow::Error>) -> backoff::Error<anyhow::Error> {
    backoff::Error::transient(e.into())
}

/// HSET + EXPIRE in one MULTI: HSETEX isn't available on managed Redis, and
/// atomicity keeps a cancelled task from leaving the hash without its TTL.
/// One artifact = one hash, so whole-hash TTL ≡ per-field TTL.
async fn stage_chunk(
    mut conn: RedisConnection,
    staging_key: String,
    chunk_idx: usize,
    chunk: Vec<u8>,
) -> Result<()> {
    deadpool_redis::redis::pipe()
        .atomic()
        .hset(&staging_key, chunk_idx, chunk)
        .ignore()
        .expire(&staging_key, STAGING_TTL_SECONDS)
        .ignore()
        .query_async::<()>(&mut conn)
        .await?;
    Ok(())
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
                transient(e)
            })?;
        Ok(result)
    }

    /// Chunked reads are not isolated — HLEN and the HGETs are separate
    /// commands — but a published hash is immutable (first writer wins in
    /// `publish_staged`), so the hash under a reader can only vanish, never
    /// change. A vanished hash surfaces as a loud absent-field error.
    async fn par_download_file(
        &self,
        _: ArtifactType,
        key: &str,
    ) -> Result<Vec<u8>, backoff::Error<anyhow::Error>> {
        let mut conn = self.get_redis_connection(key).await?;
        let now = std::time::Instant::now();
        let key = key.to_string();

        // The declared count is authoritative. Falling back to HLEN reads a
        // hash an older writer wrote field-by-field into the final key, which
        // can be short.
        let declared: Option<usize> = conn
            .hget(chunk_key(&key), CHUNK_COUNT_FIELD)
            .await
            .map_err(transient)?;
        let mut total_chunks = match declared {
            Some(n) => n,
            None => conn.hlen(chunk_key(&key)).await.map_err(transient)?,
        };

        if total_chunks == 0 {
            let inline: Option<Vec<u8>> = conn.get(&key).await.map_err(transient)?;
            if let Some(result) = inline {
                tracing::info!("download took {:?}, size: {}", now.elapsed(), result.len());
                return Ok(result);
            }
            // A chunked publish can land between the lookup and the GET,
            // evicting the inline value; one re-check separates that race
            // from a genuinely absent artifact.
            total_chunks = conn
                .hget::<_, _, Option<usize>>(chunk_key(&key), CHUNK_COUNT_FIELD)
                .await
                .map_err(transient)?
                .unwrap_or(0);
            if total_chunks == 0 {
                return Err(backoff::Error::permanent(anyhow!(
                    "artifact not found: {}",
                    key
                )));
            }
        }
        // Return this connection before the chunk loop borrows more from the
        // same pool — holding it across those acquisitions can wedge a
        // saturated pool.
        drop(conn);

        // Get total chunks
        let mut result = Vec::new();

        let mut join_set = JoinSet::new();

        // Download chunks in parallel
        for chunk_idx in 0..total_chunks {
            let key = key.clone();
            let id_clone = key.to_string();
            let mut conn = self.get_redis_connection(&id_clone).await?;
            join_set.spawn(async move {
                // An absent field deserializes as an empty Vec, not an error —
                // Option is the only way to notice the hash vanished mid-read.
                let chunk: Option<Vec<u8>> = conn.hget(chunk_key(&key), chunk_idx).await?;
                let chunk = chunk
                    .ok_or_else(|| anyhow!("chunk {chunk_idx} of artifact {key} disappeared"))?;
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
            let (idx, chunk) = res.map_err(transient)??;
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
        let now = std::time::Instant::now();

        if serialized.len() <= *CHUNK_SIZE {
            self.upload_inline(artifact_type, key, serialized).await?;
        } else {
            self.upload_chunked(artifact_type, key, serialized).await?;
        }

        tracing::info!(
            "upload took {:?}, size: {}",
            now.elapsed(),
            serialized.len()
        );
        Ok(())
    }

    /// One SET, with retention baked into the write. The UNLINK evicts a
    /// stale chunked copy — readers prefer the chunk hash, so leaving one
    /// behind would shadow this write.
    async fn upload_inline(
        &self,
        artifact_type: ArtifactType,
        key: &str,
        serialized: &[u8],
    ) -> Result<(), backoff::Error<anyhow::Error>> {
        let mut conn = self.get_redis_connection(key).await?;
        let mut options = SetOptions::default();
        if !matches!(artifact_type, ArtifactType::Program) {
            options = options.with_expiration(SetExpiry::EX(*ARTIFACT_TIMEOUT_SECONDS));
        }
        deadpool_redis::redis::pipe()
            .atomic()
            .set_options(key, serialized, options)
            .ignore()
            .unlink(chunk_key(key))
            .ignore()
            .query_async::<()>(&mut conn)
            .await
            .map_err(transient)
    }

    /// Two-phase write for artifacts above [`CHUNK_SIZE`]: stage every chunk
    /// under a private key, then publish atomically. Readers can never observe
    /// a partial artifact at its final key.
    async fn upload_chunked(
        &self,
        artifact_type: ArtifactType,
        artifact_id: &str,
        serialized: &[u8],
    ) -> Result<(), backoff::Error<anyhow::Error>> {
        let staging_key = staging_chunk_key(artifact_id);
        let chunk_count = serialized.len().div_ceil(*CHUNK_SIZE);
        self.stage_chunks(artifact_id, &staging_key, serialized)
            .await?;
        self.publish_staged(artifact_type, artifact_id, &staging_key, chunk_count)
            .await
    }

    /// Write every chunk into the staging hash concurrently; on a failed
    /// write, reclaim the hash and report the first error. A spawn-path
    /// failure skips reclaim — the staging TTL covers it.
    async fn stage_chunks(
        &self,
        artifact_id: &str,
        staging_key: &str,
        serialized: &[u8],
    ) -> Result<(), backoff::Error<anyhow::Error>> {
        let now = std::time::Instant::now();
        let mut join_set = JoinSet::new();

        for (chunk_idx, chunk) in serialized.chunks(*CHUNK_SIZE).enumerate() {
            let conn = self.get_redis_connection(artifact_id).await?;
            join_set.spawn(stage_chunk(
                conn,
                staging_key.to_string(),
                chunk_idx,
                chunk.to_vec(),
            ));
        }
        tracing::info!("spawned all chunks, elapsed: {:?}", now.elapsed());

        // Drain every task before touching the staging key below — exiting
        // early would let an in-flight HSET recreate it after the unlink.
        let mut first_error: Option<anyhow::Error> = None;
        while let Some(joined) = join_set.join_next().await {
            match joined.unwrap_or_else(|join_error| Err(join_error.into())) {
                Ok(()) => tracing::info!("joined chunk, elapsed: {:?}", now.elapsed()),
                Err(error) if first_error.is_some() => {
                    tracing::warn!("additional chunk failure: {error}")
                }
                Err(error) => first_error = Some(error),
            }
        }

        let Some(error) = first_error else {
            return Ok(());
        };
        self.reclaim_staging(artifact_id, staging_key).await;
        Err(transient(error))
    }

    /// Best-effort: a missed unlink expires with the staging TTL.
    async fn reclaim_staging(&self, artifact_id: &str, staging_key: &str) {
        let Ok(mut conn) = self.get_redis_connection(artifact_id).await else {
            return;
        };
        if let Err(e) = conn.unlink::<_, usize>(staging_key).await {
            tracing::warn!("failed to unlink staging key {staging_key}: {e}");
        }
    }

    /// Publish the staged hash in one server-side script: first-writer
    /// guard, completeness check, RENAME, retention. A cancelled client
    /// leaves nothing or everything — split commands could publish a
    /// complete artifact stuck on the staging TTL, which `exists()`-gated
    /// callers would never re-upload.
    ///
    /// A published hash never changes until delete or expiry; readers' non-
    /// isolated lookups can see it vanish (loud absent-field error) but never
    /// mutate. A repeat publish reclaims its staging and succeeds.
    /// [`CHUNK_COUNT_FIELD`] is what marks a hash published, so a short hash
    /// left at the final key by an interrupted older writer is always debris
    /// and gets replaced, whatever its length. RENAME needs both keys on one
    /// node — true here (routed by artifact id), impossible on Redis Cluster.
    async fn publish_staged(
        &self,
        artifact_type: ArtifactType,
        artifact_id: &str,
        staging_key: &str,
        chunk_count: usize,
    ) -> Result<(), backoff::Error<anyhow::Error>> {
        // UNLINK, never RENAME's implicit delete, frees a replaced value
        // asynchronously; the trailing UNLINK evicts a stale inline twin.
        const PUBLISH_SCRIPT: &str = r"
            if redis.call('HEXISTS', KEYS[2], ARGV[3]) == 1 then
                redis.call('UNLINK', KEYS[1], KEYS[3])
                return 2
            end
            if redis.call('HLEN', KEYS[1]) ~= tonumber(ARGV[1]) then
                redis.call('UNLINK', KEYS[1])
                return 0
            end
            redis.call('HSET', KEYS[1], ARGV[3], ARGV[1])
            redis.call('UNLINK', KEYS[2])
            redis.call('RENAME', KEYS[1], KEYS[2])
            if tonumber(ARGV[2]) > 0 then
                redis.call('EXPIRE', KEYS[2], ARGV[2])
            else
                redis.call('PERSIST', KEYS[2])
            end
            redis.call('UNLINK', KEYS[3])
            return 1
        ";

        let retention_seconds = if matches!(artifact_type, ArtifactType::Program) {
            0
        } else {
            *ARTIFACT_TIMEOUT_SECONDS as i64
        };
        let final_key = chunk_key(artifact_id);
        let mut conn = self.get_redis_connection(artifact_id).await?;
        let published: i64 = deadpool_redis::redis::cmd("EVAL")
            .arg(PUBLISH_SCRIPT)
            .arg(3)
            .arg(staging_key)
            .arg(&final_key)
            .arg(artifact_id)
            .arg(chunk_count)
            .arg(retention_seconds)
            .arg(CHUNK_COUNT_FIELD)
            .query_async(&mut conn)
            .await
            .map_err(transient)?;
        match published {
            2 => tracing::debug!("artifact {artifact_id} already published; kept the first copy"),
            1 => {}
            _ => {
                return Err(transient(anyhow!(
                    "staging hash {staging_key} incomplete or missing at publish"
                )));
            }
        }
        Ok(())
    }
}

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
                    Err(transient(anyhow!(
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
        // zstd level 0: fast compression for ephemeral, TTL'd Redis storage
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
                    Err(transient(anyhow!(
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
        // The marker, not the key: a hash an interrupted older writer left at
        // the final key exists but is short, and reporting it present is what
        // makes callers skip the re-upload that would repair it.
        let (inline, published) = tokio::try_join!(
            conn.exists(key),
            conn2.hexists(chunk_key(key), CHUNK_COUNT_FIELD)
        )?;
        Ok(inline || published)
    }

    async fn delete(&self, artifact: &impl ArtifactId, _: ArtifactType) -> Result<()> {
        let mut conn = self
            .get_redis_connection(artifact.id())
            .await
            .map_err(|e| anyhow!(e))?;
        let mut conn2 = conn.clone();
        let key = artifact.id();
        let _: (u64, u64) = tokio::try_join!(conn.unlink(key), conn2.unlink(chunk_key(key)))?;
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
            entry.push(chunk_key(artifact.id()));
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
            let _: () = conn.sadd(&redis_key, key).await.map_err(transient)?;

            // Set expiration to prevent memory leaks
            conn.expire::<_, ()>(&redis_key, *ARTIFACT_TIMEOUT_SECONDS as i64)
                .await
                .map_err(transient)?;

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
            let _: () = conn.srem(&redis_key, key).await.map_err(transient)?;

            // Check if set is empty
            let count: i64 = conn.scard(&redis_key).await.map_err(transient)?;

            if count <= 0 {
                // Clean up the set key
                conn.del::<_, ()>(&redis_key).await.map_err(transient)?;

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

#[cfg(test)]
mod tests {
    //! Live-Redis tests, in-file because they exercise private items.
    //! Run with a local Redis:
    //! `cargo test -p sp1-cluster-artifact --release -- --ignored --test-threads=1`

    use sp1_prover_types::Artifact;

    use super::*;

    /// One artifact under test: a unique key, its client, and the Redis
    /// probes the assertions need. Keys are typeid-unique per run, so tests
    /// neither collide nor need a database reset.
    struct Fixture {
        client: RedisArtifactClient,
        key: String,
        artifact_type: ArtifactType,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            let url =
                std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
            Self {
                client: RedisArtifactClient::new(vec![url], 2),
                key: name.create_type_id::<V7>().to_string(),
                artifact_type: ArtifactType::UnspecifiedArtifactType,
            }
        }

        fn program(name: &str) -> Self {
            Self {
                artifact_type: ArtifactType::Program,
                ..Self::new(name)
            }
        }

        fn artifact(&self) -> Artifact {
            Artifact::from(self.key.clone())
        }

        async fn upload(&self, data: &[u8]) -> Result<(), backoff::Error<anyhow::Error>> {
            self.client
                .par_upload_file(self.artifact_type, &self.key, data)
                .await
        }

        async fn download(&self) -> Vec<u8> {
            self.client
                .par_download_file(self.artifact_type, &self.key)
                .await
                .unwrap()
        }

        async fn exists(&self) -> bool {
            self.client
                .exists(&self.artifact(), self.artifact_type)
                .await
                .unwrap()
        }

        async fn delete(&self) {
            self.client
                .delete(&self.artifact(), self.artifact_type)
                .await
                .unwrap();
        }

        async fn conn(&self) -> RedisConnection {
            self.client.get_redis_connection(&self.key).await.unwrap()
        }

        async fn chunk_fields(&self) -> usize {
            self.conn().await.hlen(chunk_key(&self.key)).await.unwrap()
        }

        /// The count the publisher committed to, absent on an unpublished hash.
        async fn declared_chunks(&self) -> Option<usize> {
            self.conn()
                .await
                .hget(chunk_key(&self.key), CHUNK_COUNT_FIELD)
                .await
                .unwrap()
        }

        async fn chunk_ttl(&self) -> i64 {
            self.conn().await.ttl(chunk_key(&self.key)).await.unwrap()
        }

        /// Staging keys left under this artifact's chunk namespace.
        async fn staging_keys(&self) -> Vec<String> {
            self.conn()
                .await
                .keys(format!("{}:*", chunk_key(&self.key)))
                .await
                .unwrap()
        }
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn chunked_upload_publishes_complete_and_readable() {
        let fx = Fixture::new("chunked-upload");
        let data = vec![42; *CHUNK_SIZE + 1];
        fx.upload(&data).await.unwrap();

        assert!(fx.exists().await, "a complete upload must satisfy exists()");
        assert_eq!(fx.download().await, data);
        let chunks = data.len().div_ceil(*CHUNK_SIZE);
        assert_eq!(
            fx.declared_chunks().await,
            Some(chunks),
            "publish declares the count"
        );
        assert_eq!(
            fx.chunk_fields().await,
            chunks + 1,
            "the published hash holds the chunks plus the count marker"
        );
        let ttl = fx.chunk_ttl().await;
        assert!(
            ttl > STAGING_TTL_SECONDS,
            "the published hash must carry retention, not the staging TTL (got {ttl})"
        );
        let leftovers = fx.staging_keys().await;
        assert!(
            leftovers.is_empty(),
            "staging keys must not survive publish: {leftovers:?}"
        );

        fx.delete().await;
        assert!(!fx.exists().await, "delete must remove the chunk hash");
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn chunked_program_upload_persists() {
        let fx = Fixture::program("chunked-program");
        fx.upload(&vec![7; *CHUNK_SIZE + 1]).await.unwrap();

        assert_eq!(
            fx.chunk_ttl().await,
            -1,
            "program artifacts are retained without expiry"
        );

        fx.delete().await;
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn concurrent_same_id_uploads_never_mix() {
        let fx = std::sync::Arc::new(Fixture::new("concurrent-upload"));
        let mut writers = tokio::task::JoinSet::new();
        for fill in [1u8, 2, 3, 4] {
            let fx = fx.clone();
            writers.spawn(async move { fx.upload(&vec![fill; *CHUNK_SIZE + 7]).await });
        }
        while let Some(res) = writers.join_next().await {
            res.unwrap().unwrap();
        }

        let downloaded = fx.download().await;
        assert_eq!(downloaded.len(), *CHUNK_SIZE + 7);
        assert!(
            downloaded.iter().all(|b| *b == downloaded[0]),
            "the final artifact must be exactly one writer's data, never a mix"
        );
        let ttl = fx.chunk_ttl().await;
        assert!(
            ttl > STAGING_TTL_SECONDS,
            "retention must win, got ttl {ttl}"
        );
        assert!(fx.staging_keys().await.is_empty());

        fx.delete().await;
    }

    /// Deterministic companion to the cancellation test: the TTL invariant
    /// itself, without depending on where a race lands.
    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn staged_chunk_always_carries_ttl() {
        let fx = Fixture::new("staged-chunk-ttl");
        let staging_key = staging_chunk_key(&fx.key);
        stage_chunk(fx.conn().await, staging_key.clone(), 0, vec![1, 2, 3])
            .await
            .unwrap();

        let mut conn = fx.conn().await;
        let ttl: i64 = conn.ttl(&staging_key).await.unwrap();
        assert!(
            ttl > 0 && ttl <= STAGING_TTL_SECONDS,
            "every staged chunk write must leave the hash expiring, got ttl {ttl}"
        );
        let _: usize = conn.unlink(&staging_key).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn cancelled_upload_leaves_only_expiring_staging() {
        let fx = Fixture::new("cancelled-upload");
        let cancelled = tokio::time::timeout(
            Duration::from_millis(5),
            fx.upload(&vec![9; 2 * *CHUNK_SIZE + 1]),
        )
        .await;
        assert!(cancelled.is_err(), "5ms must cancel a 64MB upload");

        assert!(!fx.exists().await, "a cancelled upload must never publish");
        // Best-effort sweep: the cancel may land before any staging write, so
        // this loop can be empty — staged_chunk_always_carries_ttl pins the
        // TTL invariant deterministically.
        let mut conn = fx.conn().await;
        for staging_key in fx.staging_keys().await {
            let ttl: i64 = conn.ttl(&staging_key).await.unwrap();
            assert!(
                ttl > 0 && ttl <= STAGING_TTL_SECONDS,
                "orphaned staging key {staging_key} must carry the staging TTL, got {ttl}"
            );
            let _: usize = conn.unlink(&staging_key).await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn incomplete_staging_never_publishes() {
        let fx = Fixture::new("incomplete-staging");
        let staging_key = staging_chunk_key(&fx.key);
        let mut conn = fx.conn().await;
        let _: usize = conn.hset(&staging_key, 0, b"only-chunk").await.unwrap();

        fx.client
            .publish_staged(
                ArtifactType::UnspecifiedArtifactType,
                &fx.key,
                &staging_key,
                2,
            )
            .await
            .unwrap_err();
        assert!(
            !fx.exists().await,
            "an incomplete staging hash must never publish"
        );
        let staging_remains: bool = conn.exists(&staging_key).await.unwrap();
        assert!(
            !staging_remains,
            "a failed publish must reclaim its staging"
        );
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn partial_debris_at_final_key_is_repaired() {
        let fx = Fixture::new("debris-repair");
        let data = vec![3; *CHUNK_SIZE + 1];
        let chunks = data.len().div_ceil(*CHUNK_SIZE);

        // Field count matching the incoming upload's: the case a length check
        // alone cannot tell apart from a complete hash.
        let mut conn = fx.conn().await;
        for idx in 0..chunks {
            let _: usize = conn
                .hset(chunk_key(&fx.key), idx, b"old-writer partial")
                .await
                .unwrap();
        }
        assert!(!fx.exists().await, "an unmarked hash is not published");
        drop(conn);

        fx.upload(&data).await.unwrap();
        assert_eq!(
            fx.download().await,
            data,
            "debris is replaced, never adopted as the first writer"
        );
        assert_eq!(fx.declared_chunks().await, Some(chunks));

        fx.delete().await;
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn published_hash_is_immutable() {
        let fx = Fixture::new("first-writer");
        let first = vec![1; *CHUNK_SIZE + 1];
        fx.upload(&first).await.unwrap();
        fx.upload(&vec![2; *CHUNK_SIZE + 1]).await.unwrap();

        assert_eq!(
            fx.download().await,
            first,
            "a repeat publish keeps the first copy"
        );
        assert!(
            fx.staging_keys().await.is_empty(),
            "a repeat publish reclaims its staging"
        );

        fx.delete().await;
    }

    #[tokio::test]
    #[ignore = "requires Redis at REDIS_URL"]
    async fn overwrite_across_chunk_boundary_evicts_other_representation() {
        let fx = Fixture::new("boundary-crossing");

        let chunked_data = vec![8; *CHUNK_SIZE + 1];
        fx.upload(&chunked_data).await.unwrap();
        // Exactly CHUNK_SIZE: the largest inline artifact.
        let inline_data = vec![9; *CHUNK_SIZE];
        fx.upload(&inline_data).await.unwrap();
        assert_eq!(
            fx.chunk_fields().await,
            0,
            "exactly CHUNK_SIZE goes inline and evicts the chunk hash"
        );
        assert_eq!(
            fx.download().await,
            inline_data,
            "chunked→inline serves the inline value"
        );

        fx.upload(&chunked_data).await.unwrap();
        assert_eq!(
            fx.download().await,
            chunked_data,
            "inline→chunked serves the chunk hash"
        );
        // Acquired after download: holding a fixture connection across it
        // starves the pool of 2.
        let mut conn = fx.conn().await;
        let inline_remains: bool = conn.exists(&fx.key).await.unwrap();
        assert!(!inline_remains, "chunked publish evicts the inline value");
        drop(conn);

        fx.delete().await;
    }
}
