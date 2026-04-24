//! Integration tests for admission block-wait in [`RedisArtifactClient`].
//!
//! V3 admission is backpressure, not failure — callers block until the
//! target shard has room. `#[ignore]`'d by default; run with:
//!
//! ```sh
//! cargo test --release -p sp1-cluster-artifact --test admission -- \
//!     --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::RngCore;
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sp1_prover_types::{Artifact, ArtifactClient, ArtifactType};

const MB: usize = 1024 * 1024;

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

async fn reset(url: &str, maxmemory: u64) -> Result<()> {
    let client = redis::Client::open(url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHALL").query_async(&mut conn).await?;
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(maxmemory.to_string())
        .query_async(&mut conn)
        .await?;
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("noeviction")
        .query_async(&mut conn)
        .await?;
    Ok(())
}

async fn flushdb(url: &str) {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();
}

/// Simulate consumer drain by raising `maxmemory`: `used_memory` stays
/// the same but the admission budget widens. Avoids jemalloc fragmentation
/// surprises that `FLUSHDB` would introduce post-flush (used reports near
/// zero but allocator retains pages → Redis OOMs on next write).
async fn raise_maxmemory(url: &str, new_bytes: u64) {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg(new_bytes.to_string())
        .query_async(&mut conn)
        .await
        .unwrap();
}

/// Parse `used_memory` from `INFO memory` — for test-side assertions that
/// the scenario actually hits the targeted memory band (not a happy path
/// masquerading as one if zstd compression varies across versions).
async fn used_memory(url: &str) -> u64 {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let info: String = redis::cmd("INFO")
        .arg("memory")
        .query_async(&mut conn)
        .await
        .unwrap();
    info.lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .and_then(|v| v.trim().parse().ok())
        .expect("used_memory present in INFO output")
}

/// Random bytes — incompressible so zstd can't shrink below the admission budget.
fn rand_bytes(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    rand::rngs::ThreadRng::default().fill_bytes(&mut buf);
    buf
}

async fn upload(client: &RedisArtifactClient, key: &str, size_mb: usize) -> Result<()> {
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

/// `maxmemory=0` means unlimited — admission passes through without checking.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn admits_when_unlimited() {
    let url = redis_url();
    reset(&url, 0).await.expect("reset");
    let client = RedisArtifactClient::new(vec![url], 4);
    upload(&client, "unlimited", 5)
        .await
        .expect("upload admitted");
}

/// Regression for the TOCTOU race: concurrent callers must not collectively
/// overshoot the budget. With atomic admission, `in_flight` bookkeeping
/// makes each reservation see prior ones; parallel callers serialize.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn admission_atomic_no_overshoot_under_concurrency() {
    let url = redis_url();
    reset(&url, 100 * MB as u64).await.expect("reset"); // 100 MB cap, 70 MB budget
    let client = Arc::new(RedisArtifactClient::new(vec![url.clone()], 16));

    seed(&client, 5, 5).await; // ~25 MB, below budget

    // 20 × 3 MB concurrent = 60 MB would overshoot (25 + 60 > 70). Atomic
    // admission serializes; pre-fix all 20 would see stale `used` and admit.
    let mut handles = Vec::new();
    for i in 0..20 {
        let c = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            let a: Artifact = format!("race-{i}").into();
            tokio::time::timeout(
                Duration::from_secs(3),
                c.upload_raw(
                    &a,
                    ArtifactType::UnspecifiedArtifactType,
                    rand_bytes(3 * MB),
                ),
            )
            .await
        }));
    }

    tokio::time::sleep(Duration::from_millis(800)).await;
    let used = used_memory(&url).await;
    let budget = 70 * MB as u64;
    assert!(
        used <= budget + 3 * MB as u64,
        "budget overshoot: used={} MB > {} MB — TOCTOU race",
        used / MB as u64,
        (budget + 3 * MB as u64) / MB as u64,
    );

    flushdb(&url).await;
    for h in handles {
        let _ = h.await;
    }
}

/// Producer blocks when Redis is over the admission threshold, resumes
/// once capacity opens up.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn blocks_and_resumes_on_drain() {
    let url = redis_url();
    reset(&url, 200 * MB as u64).await.expect("reset"); // 200 MB cap, 140 MB budget
    let client = RedisArtifactClient::new(vec![url.clone()], 4);

    seed(&client, 15, 5).await; // ~80 MB, below budget

    // 80 MB upload → projected ~160 MB > 140 MB budget → blocks.
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

    // Simulate consumer drain by widening the budget — equivalent to data
    // deletion from admission's POV.
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
