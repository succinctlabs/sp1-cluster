//! Admission block-wait integration tests for [`RedisArtifactClient`].
//!
//! ```sh
//! cargo test --release -p sp1-cluster-artifact --test admission -- \
//!     --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngCore;
use redis::aio::MultiplexedConnection;
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sp1_prover_types::{Artifact, ArtifactClient, ArtifactType};

const MB: usize = 1024 * 1024;
const MEMORY_BUDGET_PCT: u64 = (sp1_cluster_artifact::redis::MEMORY_BUDGET_FRACTION * 100.0) as u64;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

async fn conn(url: &str) -> MultiplexedConnection {
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn reset(url: &str, maxmemory: u64) {
    let mut c = conn(url).await;
    let _: () = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();
    for (k, v) in [
        ("maxmemory", maxmemory.to_string()),
        ("maxmemory-policy", "noeviction".into()),
    ] {
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg(k)
            .arg(v)
            .query_async(&mut c)
            .await
            .unwrap();
    }
}

async fn flushdb(url: &str) {
    let _: () = redis::cmd("FLUSHDB")
        .query_async(&mut conn(url).await)
        .await
        .unwrap();
}

/// Simulate consumer drain by widening `maxmemory` (avoids jemalloc retaining
/// freed pages post-`FLUSHDB`).
async fn raise_maxmemory(url: &str, new_bytes: u64) {
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(new_bytes.to_string())
        .query_async(&mut conn(url).await)
        .await
        .unwrap();
}

/// `used_memory` from `INFO memory` (verifies tests hit the targeted band).
async fn used_memory(url: &str) -> u64 {
    let info: String = redis::cmd("INFO")
        .arg("memory")
        .query_async(&mut conn(url).await)
        .await
        .unwrap();
    info.lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .and_then(|v| v.trim().parse().ok())
        .expect("used_memory present in INFO output")
}

/// Incompressible bytes (zstd can't shrink below the admission budget).
fn rand_bytes(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    rand::rngs::ThreadRng::default().fill_bytes(&mut buf);
    buf
}

async fn upload(client: &RedisArtifactClient, key: &str, size_mb: usize) -> anyhow::Result<()> {
    let a: Artifact = key.to_string().into();
    client
        .upload_raw(
            &a,
            ArtifactType::UnspecifiedArtifactType,
            rand_bytes(size_mb * MB),
        )
        .await
}

async fn seed(client: &RedisArtifactClient, count: usize, size_mb: usize) {
    for i in 0..count {
        upload(client, &format!("seed-{i}"), size_mb)
            .await
            .unwrap_or_else(|e| panic!("seed {i}: {e:#}"));
    }
}

/// `maxmemory=0` (unlimited): admission passes through.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn admits_when_unlimited() {
    let url = redis_url();
    reset(&url, 0).await;
    let client = RedisArtifactClient::new(vec![url], 4);
    upload(&client, "unlimited", 5)
        .await
        .expect("upload admitted");
}

/// Permit pool must reserve headroom below the admission ceiling so
/// non-permit-gated outputs can land even when inputs are at saturation.
/// Without the gap, input saturation == admission ceiling → outputs
/// block → consumers can't delete inputs → deadlock.
///
/// 800 MB cap keeps allocator overhead representative of larger deployments.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn output_admits_when_inputs_at_full_pool() {
    let url = redis_url();
    let cap = 800 * MB as u64;
    reset(&url, cap).await; // 800 MB → 560 MB admission, 400 MB input pool
    std::env::set_var("PROVE_SHARD_MAX_BYTES", (40 * MB).to_string()); // 10 permits
    let client = Arc::new(RedisArtifactClient::new(vec![url.clone()], 16));

    // Acquire until pool saturates (per-acquire timeout = pool exhausted).
    let mut permits = Vec::new();
    loop {
        let a: Artifact = format!("input-{}", permits.len()).into();
        match tokio::time::timeout(Duration::from_millis(200), client.acquire_shard_permit(&a))
            .await
        {
            Ok(permit) => permits.push(permit),
            Err(_) => break, // pool saturated
        }
        if permits.len() > 50 {
            panic!("permit pool unexpectedly large (>50); test sizing is off");
        }
    }
    eprintln!("acquired {} permits before saturation", permits.len());

    // Tight per-upload timeout: surfaces a sizing regression as a fast
    // panic instead of letting it cook until ADMISSION_MAX_WAIT.
    for i in 0..permits.len() {
        let a: Artifact = format!("input-{i}").into();
        tokio::time::timeout(
            Duration::from_secs(5),
            client.upload_raw(
                &a,
                ArtifactType::UnspecifiedArtifactType,
                rand_bytes(40 * MB),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "input {i} upload deadlocked at admission — input pool saturates beyond \
                 admission ceiling (allocator overhead exceeds INPUT/MEMORY budget gap)"
            )
        })
        .unwrap_or_else(|e| panic!("input {i}: {e:#}"));
    }

    // Inputs alone must stay under admission ceiling even after allocator overhead.
    let used = used_memory(&url).await;
    let admission_ceiling = cap * MEMORY_BUDGET_PCT / 100;
    assert!(
        used < admission_ceiling,
        "inputs alone overshot admission ceiling: used={} MB > {} MB",
        used / MB as u64,
        admission_ceiling / MB as u64,
    );

    // Unpermitted output must admit promptly even with inputs saturated.
    let output: Artifact = "shard-output".to_string().into();
    let t0 = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(3),
        client.upload_raw(
            &output,
            ArtifactType::UnspecifiedArtifactType,
            rand_bytes(MB),
        ),
    )
    .await
    .expect("output upload deadlocked — tail-deadlock regression!")
    .expect("output upload failed");
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "output upload was throttled (took {elapsed:?}); admission ceiling too tight",
    );

    drop(permits);
    flushdb(&url).await;
    std::env::remove_var("PROVE_SHARD_MAX_BYTES");
}

/// Producer blocks at admission ceiling, resumes when capacity opens.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn blocks_and_resumes_on_drain() {
    let url = redis_url();
    reset(&url, 200 * MB as u64).await; // 200 MB cap → 140 MB budget
    let client = RedisArtifactClient::new(vec![url.clone()], 4);

    seed(&client, 15, 5).await; // ~80 MB, below budget

    // 80 MB upload → projected ~160 MB > 140 MB → blocks.
    let client_p = client.clone();
    let producer = tokio::spawn(async move {
        let t0 = Instant::now();
        upload(&client_p, "blocked", 80)
            .await
            .expect("producer must resume and succeed");
        t0.elapsed()
    });

    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(!producer.is_finished(), "admission did not block");

    // Widening budget == consumer draining, from admission's POV.
    raise_maxmemory(&url, 500 * MB as u64).await;

    let waited = tokio::time::timeout(Duration::from_secs(8), producer)
        .await
        .expect("producer did not resume within 8s")
        .expect("producer panicked");
    assert!(
        waited >= Duration::from_secs(2) && waited <= Duration::from_secs(10),
        "unexpected wait: {waited:?}"
    );
}
