//! Bearer-token auth for the cluster API and the coordinator.
//!
//! Servers reject a missing or wrong token only when `CLUSTER_AUTH_REQUIRED=true`.
//! A fleet can deploy the clients first and turn on the check later.

use std::fmt;
use std::task::{Context, Poll};

use futures::future::{ready, Either, Ready};
use http::header::AUTHORIZATION;
use http::HeaderMap;
use subtle::ConstantTimeEq;
use tonic::body::Body;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::{Request, Status};
use tower::{Layer, Service};

pub const TOKEN_ENV: &str = "CLUSTER_ACCESS_TOKEN";
pub const REQUIRED_ENV: &str = "CLUSTER_AUTH_REQUIRED";

const BEARER_PREFIX: &str = "Bearer ";

pub type AuthChannel = InterceptedService<Channel, BearerInterceptor>;

/// Reads `<PREFIX>_CLUSTER_ACCESS_TOKEN`, then `CLUSTER_ACCESS_TOKEN`.
/// An empty value counts as unset.
pub fn token_from_env(prefix: Option<&str>) -> Option<String> {
    let prefixed = prefix.and_then(|p| std::env::var(format!("{p}_{TOKEN_ENV}")).ok());
    resolve_token(prefixed, std::env::var(TOKEN_ENV).ok())
}

fn resolve_token(prefixed: Option<String>, bare: Option<String>) -> Option<String> {
    prefixed
        .into_iter()
        .chain(bare)
        .find(|token| !token.is_empty())
}

#[derive(Debug)]
pub enum AuthConfigError {
    RequiredWithoutToken,
    InvalidRequiredFlag,
    InvalidToken,
}

impl fmt::Display for AuthConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequiredWithoutToken => {
                write!(
                    f,
                    "{REQUIRED_ENV} is true but no token is set; set {TOKEN_ENV}"
                )
            }
            Self::InvalidRequiredFlag => write!(f, "{REQUIRED_ENV} must be true or false"),
            Self::InvalidToken => {
                write!(f, "{TOKEN_ENV} must be printable ASCII with no whitespace")
            }
        }
    }
}

impl std::error::Error for AuthConfigError {}

#[derive(Clone, Default)]
pub struct AuthConfig {
    pub token: Option<String>,
    pub required: bool,
}

impl AuthConfig {
    pub fn from_env(prefix: Option<&str>) -> Result<Self, AuthConfigError> {
        let required = std::env::var(REQUIRED_ENV).ok();
        Self::resolve(token_from_env(prefix), required)
    }

    fn resolve(token: Option<String>, required: Option<String>) -> Result<Self, AuthConfigError> {
        let required = parse_required_flag(required.as_deref())?;
        if let Some(token) = &token {
            bearer_header(token)?;
        }
        if required && token.is_none() {
            return Err(AuthConfigError::RequiredWithoutToken);
        }
        Ok(Self { token, required })
    }

    fn token_matches(&self, headers: &HeaderMap) -> bool {
        let Some(expected) = &self.token else {
            return false;
        };
        headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix(BEARER_PREFIX))
            .is_some_and(|presented| bool::from(expected.as_bytes().ct_eq(presented.as_bytes())))
    }
}

/// Any value other than true or false stops startup, so a typo cannot turn the check off.
fn parse_required_flag(value: Option<&str>) -> Result<bool, AuthConfigError> {
    match value {
        None | Some("") => Ok(false),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(_) => Err(AuthConfigError::InvalidRequiredFlag),
    }
}

/// Servers and clients both run this check. A token that can never match stops startup
/// on both sides. A trailing newline from a secret file is the usual cause.
fn bearer_header(token: &str) -> Result<MetadataValue<Ascii>, AuthConfigError> {
    if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(AuthConfigError::InvalidToken);
    }
    MetadataValue::try_from(format!("{BEARER_PREFIX}{token}"))
        .map_err(|_| AuthConfigError::InvalidToken)
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthConfig")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("required", &self.required)
            .finish()
    }
}

#[derive(Clone)]
pub struct BearerAuthLayer {
    config: AuthConfig,
}

impl BearerAuthLayer {
    pub fn new(config: AuthConfig) -> Self {
        if !config.required {
            tracing::warn!("{REQUIRED_ENV} is not true; accepting unauthenticated RPCs");
        }
        Self { config }
    }
}

impl<S> Layer<S> for BearerAuthLayer {
    type Service = BearerAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BearerAuth {
            inner,
            config: self.config.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BearerAuth<S> {
    inner: S,
    config: AuthConfig,
}

impl<S, B> Service<http::Request<B>> for BearerAuth<S>
where
    S: Service<http::Request<B>, Response = http::Response<Body>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Either<Ready<Result<S::Response, S::Error>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        if self.config.token_matches(req.headers()) {
            return Either::Right(self.inner.call(req));
        }
        let path = req.uri().path();
        if !self.config.required {
            // Shows which clients the check will reject after it is turned on.
            if self.config.token.is_some() {
                tracing::info!(
                    path,
                    "accepted RPC without a valid bearer token; {REQUIRED_ENV}=true will reject it"
                );
            }
            return Either::Right(self.inner.call(req));
        }
        tracing::warn!(path, "rejected RPC: missing or invalid bearer token");
        let status = Status::unauthenticated("missing or invalid bearer token");
        Either::Left(ready(Ok(status.into_http())))
    }
}

#[derive(Clone, Default)]
pub struct BearerInterceptor {
    header: Option<MetadataValue<Ascii>>,
}

impl BearerInterceptor {
    pub fn new(token: Option<&str>) -> Result<Self, AuthConfigError> {
        let header = token.map(bearer_header).transpose()?;
        Ok(Self { header })
    }
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(header) = &self.header {
            request
                .metadata_mut()
                .insert("authorization", header.clone());
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use http::{Method, Request, Response, StatusCode, Version};
    use tower::{service_fn, Layer, ServiceExt};

    use super::*;

    const TOKEN: &str = "s3cret";

    fn config(token: Option<&str>, required: bool) -> AuthConfig {
        AuthConfig {
            token: token.map(str::to_string),
            required,
        }
    }

    /// The counter shows how many calls passed the check.
    fn stack(
        config: AuthConfig,
    ) -> (
        BearerAuth<
            impl Service<Request<Body>, Response = Response<Body>, Error = Infallible> + Clone,
        >,
        Arc<AtomicUsize>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let inner_calls = calls.clone();
        let inner = service_fn(move |_: Request<Body>| {
            inner_calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, Infallible>(Response::new(Body::empty())) }
        });
        (BearerAuthLayer::new(config).layer(inner), calls)
    }

    fn grpc_request(authorization: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri("/cluster.ClusterService/ProofRequestList")
            .version(Version::HTTP_2)
            .header("content-type", "application/grpc");
        if let Some(value) = authorization {
            builder = builder.header("authorization", value);
        }
        builder.body(Body::empty()).unwrap()
    }

    fn grpc_status(response: &Response<Body>) -> Option<&str> {
        response
            .headers()
            .get("grpc-status")
            .map(|value| value.to_str().unwrap())
    }

    #[tokio::test]
    async fn required_rejects_missing_wrong_and_misschemed_tokens() {
        for authorization in [None, Some("Bearer wrong"), Some("Token s3cret")] {
            let (service, calls) = stack(config(Some(TOKEN), true));
            let response = service.oneshot(grpc_request(authorization)).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(grpc_status(&response), Some("16"), "{authorization:?}");
            assert_eq!(calls.load(Ordering::SeqCst), 0, "{authorization:?}");
        }
    }

    #[tokio::test]
    async fn not_required_accepts_everything() {
        for authorization in [None, Some("Bearer wrong")] {
            let (service, calls) = stack(config(Some(TOKEN), false));
            let response = service.oneshot(grpc_request(authorization)).await.unwrap();
            assert_eq!(grpc_status(&response), None, "{authorization:?}");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{authorization:?}");
        }
    }

    #[test]
    fn required_flag_accepts_only_true_or_false() {
        assert!(!parse_required_flag(None).unwrap());
        assert!(!parse_required_flag(Some("")).unwrap());
        assert!(parse_required_flag(Some("TRUE")).unwrap());
        assert!(!parse_required_flag(Some("false")).unwrap());
        for typo in ["1", "yes", " true", "ture"] {
            let err = parse_required_flag(Some(typo)).unwrap_err();
            assert!(
                matches!(err, AuthConfigError::InvalidRequiredFlag),
                "{typo:?}"
            );
        }
    }

    #[test]
    fn tokens_a_client_cannot_send_fail_at_startup() {
        for token in ["s3cret\n", " s3cret", "s3cret ", "sécret"] {
            let err = AuthConfig::resolve(Some(token.into()), None).unwrap_err();
            assert!(matches!(err, AuthConfigError::InvalidToken), "{token:?}");
            assert!(
                matches!(
                    BearerInterceptor::new(Some(token)),
                    Err(AuthConfigError::InvalidToken)
                ),
                "{token:?}"
            );
        }
    }

    #[test]
    fn prefixed_token_wins_and_empty_counts_as_unset() {
        assert_eq!(
            resolve_token(Some("a".into()), Some("b".into())).as_deref(),
            Some("a")
        );
        assert_eq!(
            resolve_token(Some(String::new()), Some("b".into())).as_deref(),
            Some("b")
        );
        assert_eq!(resolve_token(None, Some(String::new())), None);
    }

    #[test]
    fn debug_redacts_the_token() {
        let printed = format!("{:?}", config(Some(TOKEN), true));
        assert!(!printed.contains(TOKEN));
        assert!(printed.contains("<redacted>"));
    }
}
