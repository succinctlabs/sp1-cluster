use sp1_cluster_artifact::ArtifactClient;
use sp1_sdk::network::proto::artifact::{
    artifact_store_server::ArtifactStore, CreateArtifactRequest, CreateArtifactResponse,
};
use tonic::{Request, Response, Status};
use tracing::info;

use crate::auth::Auth;
use crate::ids::{artifact_uri, proto_to_cluster_type};

/// The fixed message the SDK signs for `create_artifact` authentication.
/// See sp1/crates/sdk/src/network/client.rs where `sign_message(b"create_artifact", ...)`
/// produces the signature that lands on the wire.
const CREATE_ARTIFACT_MESSAGE: &[u8] = b"create_artifact";

pub struct ArtifactStoreImpl<A> {
    client: A,
    public_http_url: String,
    auth: Auth,
}

impl<A> ArtifactStoreImpl<A> {
    pub fn new(client: A, public_http_url: String, auth: Auth) -> Self {
        Self { client, public_http_url, auth }
    }
}

#[tonic::async_trait]
impl<A> ArtifactStore for ArtifactStoreImpl<A>
where
    A: ArtifactClient + 'static,
{
    async fn create_artifact(
        &self,
        request: Request<CreateArtifactRequest>,
    ) -> Result<Response<CreateArtifactResponse>, Status> {
        let req = request.into_inner();
        let requester = self.auth.authorize(CREATE_ARTIFACT_MESSAGE, &req.signature)?;
        let artifact_type = proto_to_cluster_type(req.artifact_type);

        let artifact = self
            .client
            .create_artifact()
            .map_err(|e| Status::internal(format!("create_artifact failed: {e}")))?;
        let id = artifact.to_id();
        let uri = artifact_uri(&self.public_http_url, artifact_type, &id);
        info!(id, ?artifact_type, uri, %requester, "create_artifact");

        Ok(Response::new(CreateArtifactResponse {
            artifact_uri: uri.clone(),
            artifact_presigned_url: uri,
        }))
    }
}
