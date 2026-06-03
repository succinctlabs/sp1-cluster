use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio_util::{future::FutureExt, sync::CancellationToken};

pub struct Redis {
    _container: ContainerAsync<GenericImage>,
    pub addr: String,
}

impl Redis {
    pub async fn start(token: CancellationToken) -> anyhow::Result<Self> {
        let t0 = std::time::Instant::now();
        tracing::info!("Starting bitnamisecure/redis:latest image");
        let password = "redispassword";
        let init = WaitFor::message_on_stdout("Ready to accept connections");
        let container = GenericImage::new("bitnamisecure/redis", "latest")
            .with_exposed_port(6379.tcp())
            .with_wait_for(init)
            .with_env_var("REDIS_PASSWORD", password)
            .with_env_var("REDIS_AOF_ENABLED", "no")
            .with_env_var("REDIS_EXTRA_FLAGS", "--maxmemory 28gb")
            .start()
            .with_cancellation_token_owned(token)
            .await
            .ok_or(anyhow::anyhow!("redis startup cancelled"))??;
        let addr = {
            let host = container.get_host().await?;
            let port = container.get_host_port_ipv4(6379.tcp()).await?;
            format!("redis://:{password}@{host}:{port}")
        };

        tracing::info!("Redis ready {addr} (startup time: {:?})", t0.elapsed());

        Ok(Self {
            addr,
            _container: container,
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }
}

pub struct Postgres {
    _container: ContainerAsync<GenericImage>,
    pub addr: String,
}

impl Postgres {
    pub async fn start(token: CancellationToken) -> anyhow::Result<Self> {
        let t0 = std::time::Instant::now();
        tracing::info!("Starting bitnamisecure/postgresql:latest image");
        let db = "postgres";
        let user = "postgres";
        let password = "postgrespassword";
        let init = WaitFor::message_on_stderr("database system is ready to accept connections");
        let container = GenericImage::new("bitnamisecure/postgresql", "latest")
            .with_exposed_port(5432.tcp())
            .with_wait_for(init)
            .with_env_var("POSTGRES_DB", db)
            .with_env_var("POSTGRES_USER", user)
            .with_env_var("POSTGRES_PASSWORD", password)
            .start()
            .with_cancellation_token_owned(token)
            .await
            .ok_or(anyhow::anyhow!("postgres startup cancelled"))??;
        let addr = {
            let host = container.get_host().await?;
            let port = container.get_host_port_ipv4(5432.tcp()).await?;
            format!("postgres://{user}:{password}@{host}:{port}/{db}")
        };

        tracing::info!("PostgreSQL ready {addr} (startup time: {:?})", t0.elapsed());

        Ok(Self {
            addr,
            _container: container,
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }
}
