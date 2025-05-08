use futures::TryFutureExt;
use std::{future::Future, io, net::IpAddr, time::Duration};

use backoff::{backoff::Backoff, Notify};
use tonic::{Code, Status};

/// Convert a gRPC status to a backoff error.
pub fn status_to_backoff_error(e: Status) -> backoff::Error<Status> {
    tracing::warn!("Handling backoff error {:?}", e);
    match e.code() {
        Code::Internal
        | Code::Unavailable
        | Code::Unknown
        | Code::Cancelled
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::DataLoss => backoff::Error::transient(e),
        _ => backoff::Error::permanent(e),
    }
}

pub trait BackoffError: Sized {
    fn to_backoff_error(self) -> backoff::Error<Self>;
}

impl BackoffError for tonic::Status {
    fn to_backoff_error(self) -> backoff::Error<Self> {
        match self.code() {
            Code::Internal
            | Code::Unavailable
            | Code::Unknown
            | Code::Cancelled
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::DataLoss => backoff::Error::transient(self),
            _ => backoff::Error::permanent(self),
        }
    }
}

impl<E> BackoffError for backoff::Error<E> {
    fn to_backoff_error(self) -> backoff::Error<Self> {
        match self {
            backoff::Error::Permanent(_) => backoff::Error::permanent(self),
            backoff::Error::Transient { .. } => backoff::Error::transient(self),
        }
    }
}

/// No-op implementation of [`Notify`]. Literally does nothing.
#[derive(Debug, Clone, Copy)]
pub struct NoopNotify;

impl<E> Notify<E> for NoopNotify {
    fn notify(&mut self, _: E, _: Duration) {}
}

pub fn backoff_retry<I, E, Fn, Fut, B>(
    backoff: B,
    mut operation: Fn,
) -> impl Future<Output = Result<I, E>>
where
    B: Backoff,
    Fn: FnMut() -> Fut,
    Fut: Future<Output = Result<I, E>>,
    E: BackoffError,
{
    backoff::future::retry_notify(
        backoff,
        move || operation().map_err(|e| e.to_backoff_error()),
        NoopNotify,
    )
}

pub fn get_private_ip() -> io::Result<Option<IpAddr>> {
    let interfaces = if_addrs::get_if_addrs()?;

    // Look for private IPv4 addresses
    for interface in interfaces {
        if let IpAddr::V4(ip) = interface.ip() {
            // Check if it's a private IP address
            if ip.is_private() {
                return Ok(Some(IpAddr::V4(ip)));
            }
        }
    }

    Ok(None)
}
