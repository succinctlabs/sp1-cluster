use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio_util::{future::FutureExt, sync::CancellationToken};

pub const REDIS_POOL_MAX_SIZE: usize = 1;

pub struct Redis {
    _container: ContainerAsync<GenericImage>,
    pub addr: String,
}

impl Redis {
    pub async fn start(token: CancellationToken) -> anyhow::Result<Self> {
        let image = "bitnamisecure/redis";
        let tag = "latest@sha256:d842c434ff617b84f954700b60fd99ac8b567ce16292daccb18cfc214cdcc2ec";
        let password = "redispassword";
        let init = WaitFor::message_on_stdout("Ready to accept connections");
        tracing::info!("Starting {image}:{tag} image");
        let t0 = std::time::Instant::now();
        let container = GenericImage::new(image, tag)
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

pub struct Minio {
    _container: ContainerAsync<GenericImage>,
    addr: String,
    user: String,
    password: String,
    bucket: String,
}

impl Minio {
    pub async fn start(token: CancellationToken) -> anyhow::Result<Self> {
        let image = "minio/minio";
        let tag = "latest@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e";
        let user = "minioadmin".to_string();
        let password = "miniopassword".to_string();
        let bucket = "sp1-test-cluster-artifacts".to_string();
        let init = WaitFor::message_on_stderr("API:");

        tracing::info!("Starting {image}:{tag} image");
        let t0 = std::time::Instant::now();
        let container = GenericImage::new(image, tag)
            .with_exposed_port(9000.tcp())
            .with_wait_for(init)
            .with_env_var("MINIO_ROOT_USER", &user)
            .with_env_var("MINIO_ROOT_PASSWORD", &password)
            .with_cmd(["server", "/data"])
            .start()
            .with_cancellation_token_owned(token)
            .await
            .ok_or(anyhow::anyhow!("minio startup cancelled"))??;
        let addr = {
            let host = container.get_host().await?;
            let port = container.get_host_port_ipv4(9000.tcp()).await?;
            format!("http://{host}:{port}")
        };

        tracing::info!("MinIO ready {addr} (startup time: {:?})", t0.elapsed());

        Ok(Self {
            addr,
            user,
            password,
            bucket,
            _container: container,
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }
}

pub struct Postgres {
    _container: ContainerAsync<GenericImage>,
    pub addr: String,
}

impl Postgres {
    pub async fn start(token: CancellationToken) -> anyhow::Result<Self> {
        let image = "bitnamisecure/postgresql";
        let tag = "latest@sha256:f60aa249d5be4ec1a9651ab05515f2eb9bb2b6eb664cd300abc19f13a208900d";
        let db = "postgres";
        let user = "postgres";
        let password = "postgrespassword";
        let init = WaitFor::message_on_stderr("database system is ready to accept connections");
        tracing::info!("Starting {image}:{tag} image");
        let t0 = std::time::Instant::now();
        let container = GenericImage::new(image, tag)
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
