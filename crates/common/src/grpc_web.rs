use std::time::Duration;

use http::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    HeaderName,
};
use tonic::Status;
use tonic_web::GrpcWebLayer;
use tower::layer::util::Stack;
use tower_http::cors::{AllowOrigin, CorsLayer};

const PREFLIGHT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

// Lets browser grpc-web clients on any origin call the server, with
// credentials. CORS sits outside grpc-web so preflight requests never reach
// the gRPC router.
pub fn grpc_web_layer() -> Stack<GrpcWebLayer, CorsLayer> {
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::mirror_request())
        .allow_credentials(true)
        .max_age(PREFLIGHT_MAX_AGE)
        .expose_headers([
            Status::GRPC_STATUS,
            Status::GRPC_MESSAGE,
            Status::GRPC_STATUS_DETAILS,
        ])
        .allow_headers([
            HeaderName::from_static("x-grpc-web"),
            AUTHORIZATION,
            CONTENT_TYPE,
            HeaderName::from_static("x-user-agent"),
            HeaderName::from_static("grpc-timeout"),
        ]);
    Stack::new(GrpcWebLayer::new(), cors)
}

#[cfg(test)]
mod tests {
    use http::{Method, Request, Response, StatusCode};
    use tonic::body::Body;
    use tower::{service_fn, Layer, ServiceExt};

    use super::grpc_web_layer;

    // tonic-web rejects HTTP/1.1 requests that are not grpc-web, so CORS must
    // answer the preflight before it reaches the grpc-web layer.
    #[tokio::test]
    async fn cors_answers_preflight_before_grpc_web() {
        let service = grpc_web_layer().layer(service_fn(|_: Request<Body>| async {
            Ok::<_, std::convert::Infallible>(Response::new(Body::empty()))
        }));
        let preflight = Request::builder()
            .method(Method::OPTIONS)
            .uri("/cluster.ClusterService/ProofRequestList")
            .header("origin", "https://explorer.example")
            .header("access-control-request-method", "POST")
            .header("access-control-request-headers", "x-grpc-web,content-type")
            .body(Body::empty())
            .unwrap();

        let response = service.oneshot(preflight).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["access-control-allow-origin"],
            "https://explorer.example"
        );
    }
}
