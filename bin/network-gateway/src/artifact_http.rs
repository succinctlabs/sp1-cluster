use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::put,
    Router,
};
use sp1_cluster_artifact::{ArtifactClient, CompressedUpload};
use tracing::{error, info};

use crate::ids::parse_artifact_type_segment;

pub struct ArtifactHttpState<A> {
    pub client: A,
}

pub fn router<A>(state: Arc<ArtifactHttpState<A>>) -> Router
where
    A: ArtifactClient + CompressedUpload,
{
    // Axum's default 2 MiB request body limit is much smaller than a realistic
    // SP1 ELF or stdin (hundreds of MB is common, e.g. the rsp bench). Lift
    // the cap entirely — upstream backpressure comes from the artifact store.
    Router::new()
        .route(
            "/artifacts/{type_seg}/{id}",
            put(put_artifact::<A>).get(get_artifact::<A>),
        )
        .layer(DefaultBodyLimit::disable())
        .with_state(state)
}

async fn put_artifact<A>(
    Path((type_seg, id)): Path<(String, String)>,
    State(state): State<Arc<ArtifactHttpState<A>>>,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)>
where
    A: ArtifactClient + CompressedUpload,
{
    let artifact_type = parse_artifact_type_segment(&type_seg).ok_or((
        StatusCode::BAD_REQUEST,
        format!("unknown artifact type: {type_seg}"),
    ))?;

    info!(id, ?artifact_type, size = body.len(), "PUT artifact");
    state
        .client
        .upload_raw_compressed(&id, artifact_type, body.to_vec())
        .await
        .map_err(internal)?;
    Ok(StatusCode::OK)
}

async fn get_artifact<A>(
    Path((type_seg, id)): Path<(String, String)>,
    State(state): State<Arc<ArtifactHttpState<A>>>,
) -> Result<impl IntoResponse, (StatusCode, String)>
where
    A: ArtifactClient + CompressedUpload,
{
    let artifact_type = parse_artifact_type_segment(&type_seg).ok_or((
        StatusCode::BAD_REQUEST,
        format!("unknown artifact type: {type_seg}"),
    ))?;

    info!(id, ?artifact_type, "GET artifact");
    let bytes = state
        .client
        .download_raw(&id, artifact_type)
        .await
        .map_err(|e| {
            let msg = format!("download failed: {e}");
            error!("{msg}");
            (StatusCode::NOT_FOUND, msg)
        })?;
    Ok(bytes)
}

fn internal(e: anyhow::Error) -> (StatusCode, String) {
    let msg = format!("internal error: {e}");
    error!("{msg}");
    (StatusCode::INTERNAL_SERVER_ERROR, msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use sp1_cluster_artifact::{ArtifactClient, InMemoryArtifactClient};
    use tower::ServiceExt;

    #[tokio::test]
    async fn http_put_get_roundtrip_preserves_zstd_bincode_body() {
        // Mimic what the SDK does: bincode then zstd.
        let payload: Vec<u8> = (0..10_000u16).flat_map(|x| x.to_le_bytes()).collect();
        let bincoded = bincode::serialize(&payload).unwrap();
        let zstd_body = zstd::encode_all(bincoded.as_slice(), 3).unwrap();

        let client = InMemoryArtifactClient::new();
        let artifact = client.create_artifact().unwrap();
        let id = artifact.to_id();

        let state = Arc::new(ArtifactHttpState {
            client: client.clone(),
        });
        let app = router(state);

        // PUT
        let put_req = Request::builder()
            .method("PUT")
            .uri(format!("/artifacts/stdin/{id}"))
            .body(Body::from(zstd_body.clone()))
            .unwrap();
        let resp = app.clone().oneshot(put_req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // GET: the InMemoryArtifactClient's download_raw returns what upload_raw stored
        // (no zstd layer). After upload_raw_compressed the store holds the zstd bytes;
        // download_raw returns them verbatim — which is what the SDK expects (it feeds
        // GET bytes directly to bincode::deserialize for proofs, but this mock doesn't
        // zstd-decode). To mirror production's zstd-decode on GET end-to-end, we do it
        // manually and assert round-trip.
        let get_req = Request::builder()
            .method("GET")
            .uri(format!("/artifacts/stdin/{id}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(get_req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let got = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            got.as_ref(),
            zstd_body.as_slice(),
            "InMemoryArtifactClient stores verbatim; bytes should match"
        );

        // Confirm the underlying zstd payload decodes back to the bincode input.
        let decoded = zstd::decode_all(got.as_ref()).unwrap();
        assert_eq!(decoded, bincoded);
    }

    #[tokio::test]
    async fn http_get_unknown_type_returns_400() {
        let client = InMemoryArtifactClient::new();
        let state = Arc::new(ArtifactHttpState { client });
        let app = router(state);

        let req = Request::builder()
            .method("GET")
            .uri("/artifacts/bogus/some-id")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
