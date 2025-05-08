use async_scoped::TokioScope;
use std::future::Future;
use tokio::task::JoinError;

// TODO: Should this be in sp1-cluster-common?

/// Spawn a blocking task and immediately await its result. This function is similar to
/// `tokio::task::spawn_blocking`, but it does not require F to be 'static.
pub async fn await_blocking<F: FnMut() -> T + Send, T: Send + 'static>(
    f: F,
) -> Result<T, JoinError> {
    // Safety: This is safe as long as we do not `std::mem::forget` the returned future.
    unsafe { TokioScope::scope_and_collect(|scope| scope.spawn_blocking(f)) }
        .await
        .1
        .pop()
        .unwrap()
}

/// Spawn blocking tasks and immediately await their results.
///
/// This function is similar to `tokio::task::spawn_blocking`, but it does not require F to be
/// 'static. This function is useful for spawning multiple tasks at once.
pub async fn await_scoped_vec<F: Future<Output = T> + Send, T: Send + 'static>(
    f: impl IntoIterator<Item = F>,
) -> Result<Vec<T>, JoinError> {
    // Safety: This is safe as long as we do not `std::mem::forget` the returned future.
    unsafe {
        TokioScope::scope_and_collect(|scope| {
            f.into_iter().map(|f| scope.spawn(f)).collect::<Vec<_>>()
        })
    }
    .await
    .1
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
}
