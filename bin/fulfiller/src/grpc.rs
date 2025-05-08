use std::time::Duration;
use tonic::transport::{Endpoint, Error};

pub fn configure_endpoint(addr: String) -> Result<Endpoint, Error> {
    Ok(Endpoint::new(addr)?
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(30))))
}
