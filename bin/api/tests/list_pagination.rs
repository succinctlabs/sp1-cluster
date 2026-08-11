//! Postgres-backed regression tests for `proof_request_list` ordering and
//! pagination.
//!
//! `#[ignore]`'d by default — run with:
//!
//!     cargo test --release -p sp1-cluster-api --tests -- --ignored --test-threads=1
//!
//! Requires Postgres at `DATABASE_URL` (schema is migrated and the
//! `proof_requests` table truncated on each run).

use sp1_cluster_api::ClusterServiceImpl;
use sp1_cluster_common::proto::{
    cluster_service_server::ClusterService, ProofRequestCreateRequest, ProofRequestListRequest,
};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::Request;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".into())
}

const PAGE: u32 = 1000;
/// Enough rows that a single page can't hold them.
const SEEDED: u32 = PAGE + 200;

async fn service_with_seeded_rows() -> ClusterServiceImpl {
    let pool = PgPool::connect(&database_url()).await.unwrap();
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    sqlx::query("TRUNCATE proof_requests")
        .execute(&pool)
        .await
        .unwrap();

    let service = ClusterServiceImpl::new(Arc::new(pool));
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 4 * 3600;
    for i in 0..SEEDED {
        service
            .proof_request_create(Request::new(ProofRequestCreateRequest {
                proof_id: format!("req_{i:06}"),
                requester: vec![0x9b],
                stdin_artifact_id: "s".into(),
                program_artifact_id: "p".into(),
                deadline,
                ..Default::default()
            }))
            .await
            .unwrap();
    }
    service
}

async fn list(service: &ClusterServiceImpl, offset: u32) -> Vec<String> {
    service
        .proof_request_list(Request::new(ProofRequestListRequest {
            limit: Some(PAGE),
            offset: Some(offset),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .proof_requests
        .into_iter()
        .map(|r| r.id)
        .collect()
}

/// The failure mode behind stuck-at-assigned: with more matching rows than the
/// limit, the newest row must still be reachable — on a later page, at a
/// stable position.
#[tokio::test]
#[ignore = "requires Postgres at DATABASE_URL"]
async fn rows_past_the_limit_are_reachable_via_offset() {
    let service = service_with_seeded_rows().await;
    let newest = format!("req_{:06}", SEEDED - 1);

    let first = list(&service, 0).await;
    assert_eq!(first.len(), PAGE as usize);
    assert!(
        !first.contains(&newest),
        "seed too small to exercise truncation"
    );

    let second = list(&service, PAGE).await;
    assert!(
        second.contains(&newest),
        "row past the first page is unreachable — the stuck-request regression"
    );
    assert_eq!(first.len() + second.len(), SEEDED as usize);
}

/// Pages must not shuffle under row churn. An UPDATE relocates the heap tuple,
/// so without ORDER BY the updated row jumps to the last page and another row
/// silently drops out of view.
#[tokio::test]
#[ignore = "requires Postgres at DATABASE_URL"]
async fn page_membership_survives_row_updates() {
    let service = service_with_seeded_rows().await;
    let before = list(&service, 0).await;

    let pool = PgPool::connect(&database_url()).await.unwrap();
    sqlx::query("UPDATE proof_requests SET updated_at = now() WHERE id = $1")
        .bind(&before[0])
        .execute(&pool)
        .await
        .unwrap();

    let after = list(&service, 0).await;
    assert_eq!(before, after, "an UPDATE reshuffled page membership");

    let second = list(&service, PAGE).await;
    let mut all = after;
    all.extend(second);
    let mut deduped = all.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(deduped.len(), all.len(), "pages overlap");
    assert_eq!(all.len(), SEEDED as usize, "pages have gaps");
}
