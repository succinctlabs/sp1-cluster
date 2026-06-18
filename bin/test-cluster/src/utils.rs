/// Poll-connect to `addr` until it accepts a TCP connection (or time out).
pub async fn wait_for_tcp(addr: &str, what: &str) -> anyhow::Result<()> {
    for _ in 0..600 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            tracing::info!("{what} ready at {addr}");
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    anyhow::bail!("timed out waiting for {what} at {addr}");
}
