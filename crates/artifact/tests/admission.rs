//! Integration tests for the admission block-wait path in [`RedisArtifactClient`].
//!
//! V3 admission is a backpressure mechanism — callers block until the target
//! shard has room, not error out. These tests exercise the wait-and-resume
//! behaviour against a real Redis. `#[ignore]`'d by default — run with:
//!
//! ```sh
//! cargo test --release -p sp1-cluster-artifact --test admission -- \
//!     --ignored --test-threads=1
//! ```

use std::time::{Duration, Instant};

use anyhow::Result;
use rand::RngCore;
use sp1_cluster_artifact::redis::RedisArtifactClient;
use sp1_prover_types::{Artifact, ArtifactClient, ArtifactType};

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

fn bytes(n: usize) -> Vec<u8> {
    // Random so zstd can't collapse it below the admission budget — the
    // admission check runs on post-compression bytes.
    let mut buf = vec![0u8; n];
    rand::rngs::ThreadRng::default().fill_bytes(&mut buf);
    buf
}

/// Unlimited Redis (`maxmemory=0`) passes through instantly — no admission.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn admission_no_limit_when_maxmemory_unset() {
    let url = redis_url();
    reset(&url, 0).await.expect("reset");
    let client = RedisArtifactClient::new(vec![url], 4);
    let a: Artifact = "no-limit".to_string().into();
    client
        .upload_raw(
            &a,
            ArtifactType::UnspecifiedArtifactType,
            bytes(5 * 1024 * 1024),
        )
        .await
        .expect("unlimited redis admits upload");
}

/// Under-budget upload is admitted without blocking — regression check that
/// `used + incoming ≤ budget` is treated as fast-path `Ok(())`.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn admission_admits_under_budget() {
    let url = redis_url();
    reset(&url, 50 * 1024 * 1024).await.expect("reset");
    let client = RedisArtifactClient::new(vec![url], 4);
    let t0 = Instant::now();
    for i in 0..2 {
        let a: Artifact = format!("under-{i}").into();
        client
            .upload_raw(
                &a,
                ArtifactType::UnspecifiedArtifactType,
                bytes(15 * 1024 * 1024),
            )
            .await
            .unwrap_or_else(|e| panic!("upload {i} rejected under budget: {e:#}"));
    }
    assert!(
        t0.elapsed() < Duration::from_secs(5),
        "under-budget uploads took unexpectedly long: {:?} — admission may be blocking spuriously",
        t0.elapsed()
    );
}

/// **Main test.** Admission blocks when Redis is over budget, and resumes
/// when the budget opens up. Spawns a producer that tries to upload a large
/// artifact while Redis is at capacity, waits for it to be blocking, then
/// `FLUSHDB`s Redis to release the backpressure. The upload should finish
/// promptly once admission clears.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn admission_blocks_and_resumes_on_drain() {
    let url = redis_url();
    // 200 MB cap × 0.8 = 160 MB budget. Seed 6 × 20 MB (120 MB used) under
    // budget, then a 60 MB upload pushes projected 180 MB > 160 MB budget.
    reset(&url, 200 * 1024 * 1024).await.expect("reset");

    let client = RedisArtifactClient::new(vec![url.clone()], 4);

    // Seed: 6 × 20 MB — each well under CHUNK_SIZE so each is a single SET.
    for i in 0..6 {
        let seed: Artifact = format!("seed-{i}").into();
        client
            .upload_raw(
                &seed,
                ArtifactType::UnspecifiedArtifactType,
                bytes(20 * 1024 * 1024),
            )
            .await
            .unwrap_or_else(|e| panic!("seed {i} rejected: {e:#}"));
    }

    // Producer: a 60 MB upload that would push projected usage over budget —
    // admission should block, not fail.
    let client_p = client.clone();
    let producer = tokio::spawn(async move {
        let a: Artifact = "blocked".to_string().into();
        let started = Instant::now();
        client_p
            .upload_raw(
                &a,
                ArtifactType::UnspecifiedArtifactType,
                bytes(60 * 1024 * 1024),
            )
            .await
            .expect("producer should resume and succeed, not error");
        started.elapsed()
    });

    // Give the producer time to enter the admission wait loop (>=1 poll).
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        !producer.is_finished(),
        "producer finished too quickly — admission did not block"
    );

    // Drain Redis (simulate consumer deletes).
    let redis_client = redis::Client::open(url.clone()).unwrap();
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();

    // Producer should finish within the next poll interval plus a margin.
    let waited = tokio::time::timeout(Duration::from_secs(8), producer)
        .await
        .expect("producer did not finish within 8s after drain")
        .expect("producer task panicked");

    // Sanity: waited at least ~2.5s (the pre-drain sleep) but not forever.
    assert!(
        waited >= Duration::from_secs(2) && waited <= Duration::from_secs(10),
        "unexpected wait duration: {waited:?}"
    );
}
