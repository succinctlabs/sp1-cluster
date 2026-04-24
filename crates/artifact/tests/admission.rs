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

/// Regression test for the consumer-side deadlock: when Redis is above the
/// permit-pool budget (80%) but below the admission block threshold (95%),
/// consumer uploads (proof outputs, markers) must flow through. Otherwise
/// GPU workers can't complete shards, artifacts never delete, and the
/// pipeline deadlocks.
///
/// 500 MB cap × 0.95 = 475 MB block threshold. Seed 440 MB — above the
/// 400 MB permit budget (80%) but below the 475 MB admission threshold.
/// A consumer-sized ~800 KB upload should admit without blocking.
#[tokio::test]
#[ignore = "requires Redis at REDIS_URL"]
async fn admission_admits_consumer_upload_in_reserve_slack() {
    let url = redis_url();
    reset(&url, 500 * 1024 * 1024).await.expect("reset");
    let client = RedisArtifactClient::new(vec![url], 4);

    // Seed 440 MB — in the 80–95% "drainer lane" between permit-pool budget
    // and admission block threshold. Admission must still admit small
    // consumer-side uploads.
    for i in 0..22 {
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

    // Consumer-sized upload matching observed `normalize prove shard`
    // proof-output p50 (~800 KB). Would block if admission used the 80%
    // permit-pool budget instead of the 95% block threshold.
    let t0 = Instant::now();
    let small: Artifact = "consumer-output".to_string().into();
    client
        .upload_raw(
            &small,
            ArtifactType::UnspecifiedArtifactType,
            bytes(800 * 1024),
        )
        .await
        .expect("consumer upload in 80-95% drainer lane must admit");
    assert!(
        t0.elapsed() < Duration::from_secs(3),
        "consumer upload took {:?} — admission blocked when it shouldn't have",
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
    // 1 GB cap × 0.95 = 950 MB block threshold. Seed 800 MB well below
    // threshold, then a 200 MB upload pushes projected 1 GB past the
    // 950 MB block threshold, triggering the wait loop. Larger cap avoids
    // Redis allocator overhead pushing `used_memory` above the threshold
    // prematurely (was flaky at 500 MB cap).
    reset(&url, 1024 * 1024 * 1024).await.expect("reset");

    let client = RedisArtifactClient::new(vec![url.clone()], 4);

    // Seed: 40 × 20 MB = 800 MB — each under CHUNK_SIZE so single SET.
    for i in 0..40 {
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

    // Producer: a 200 MB upload that pushes projected usage past the 95%
    // block threshold — admission should block, not fail.
    let client_p = client.clone();
    let producer = tokio::spawn(async move {
        let a: Artifact = "blocked".to_string().into();
        let started = Instant::now();
        client_p
            .upload_raw(
                &a,
                ArtifactType::UnspecifiedArtifactType,
                bytes(200 * 1024 * 1024),
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
