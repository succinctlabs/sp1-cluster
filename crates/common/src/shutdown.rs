use tokio_util::sync::CancellationToken;

pub async fn wait_for_shutdown_signal(token: CancellationToken) {
    tokio::select! {
        signal = shutdown_signal() => {
            if signal == "CTRL+C" {
                tracing::info!("Received ctrl-c, shutting down... (ctrl-c again to force exit)")
            } else {
                tracing::info!("Received signal {signal}. Shutting down...");
            }
            token.cancel();
        }
        _ = token.cancelled() => {
            tracing::info!("Cancellation signal received. Shutting down...")
        },
    }
}

async fn shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        let mut sighup = signal(SignalKind::hangup()).expect("install SIGHUP handler");
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => "CTRL+C",
            _ = sigterm.recv() => "SIGTERM",
            _ = sigint.recv() => "SIGINT",
            _ = sighup.recv() => "SIGHUP",
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl-c handler");
        "CTRL+C"
    }
}
