use crate::{s3_rest::S3RestClient, s3_sdk::S3SDKClient, ArtifactClient, ArtifactId, ArtifactType};

use anyhow::{anyhow, Result};
use aws_config::{retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion, Region};
use aws_sdk_s3::{
    config::{IdentityCache, StalledStreamProtectionConfig},
    primitives::{ByteStream, SdkBody},
    Client as S3Client,
};
use aws_smithy_async::rt::sleep::default_async_sleep;
use backoff::{exponential::ExponentialBackoff, ExponentialBackoffBuilder, SystemClock};
use bytes::Bytes;
use lazy_static::lazy_static;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::instrument;

lazy_static! {
    static ref BACKOFF: ExponentialBackoff<SystemClock> = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(100))
        .with_max_elapsed_time(Some(Duration::from_secs(120)))
        .build();
}

const CHUNK_SIZE: usize = 32 * 1024 * 1024;
const MAX_RETRY_COUNT: u32 = 7;

#[derive(Clone)]
pub enum S3DownloadMode {
    REST(Arc<S3RestClient>),
    AwsSDK(Arc<S3SDKClient>),
}

#[derive(Clone)]
pub struct S3ArtifactClient {
    pub sdk_client: Arc<S3Client>,
    pub bucket: String,
    pub region: String,
    semaphore: Arc<Semaphore>,
    concurrency: usize,
    s3_dl_mode: S3DownloadMode,
}

impl S3ArtifactClient {
    pub async fn new(
        region: String,
        bucket: String,
        concurrency: usize,
        s3_dl_mode: S3DownloadMode,
    ) -> Self {
        Self {
            sdk_client: Arc::new(Self::create_sdk_client(region.clone()).await),
            s3_dl_mode,
            bucket,
            region,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            concurrency,
        }
    }

    async fn create_sdk_client(region: String) -> S3Client {
        let mut base = aws_config::load_defaults(BehaviorVersion::latest())
            .await
            .to_builder();
        let timeout_config = TimeoutConfig::builder()
            .operation_attempt_timeout(Duration::from_secs(120))
            .build();
        base.set_retry_config(Some(
            RetryConfig::standard()
                .with_max_attempts(MAX_RETRY_COUNT)
                .with_max_backoff(Duration::from_secs(30)),
        ))
        .set_timeout_config(Some(timeout_config))
        .set_sleep_impl(default_async_sleep())
        .set_region(Some(Region::new(region.clone())));
        base.set_stalled_stream_protection(Some(
            StalledStreamProtectionConfig::enabled()
                .grace_period(Duration::from_secs(20))
                .build(),
        ));
        // Refresh identity slightly more frequently than the default to avoid ExpiredToken errors
        base.set_identity_cache(Some(
            IdentityCache::lazy()
                .load_timeout(Duration::from_secs(10))
                .buffer_time(Duration::from_secs(300))
                .build(),
        ));
        let config = base.build();
        S3Client::new(&config)
    }

    pub async fn create_s3_sdk_download_client(region: String) -> Arc<S3SDKClient> {
        Arc::new(S3SDKClient::new(Self::create_sdk_client(region).await))
    }

    /// Different artifact types have different S3 prefixes.
    ///
    /// This is so that different types are artifacts can have different expiration times. In S3, each
    /// prefix can have a different expiration time.
    pub fn get_s3_prefix(artifact_type: ArtifactType) -> &'static str {
        match artifact_type {
            ArtifactType::UnspecifiedArtifactType => "artifacts",
            ArtifactType::Program => "programs",
            ArtifactType::Stdin => "stdins",
            ArtifactType::Proof => "proofs",
            ArtifactType::Groth16Circuit => "",
            ArtifactType::PlonkCircuit => "",
        }
    }

    /// Gets the REST API URL for an artifact.
    pub fn get_s3_url_from_id(&self, artifact_type: ArtifactType, id: &str) -> String {
        let url_base = format!("https://{}.s3.{}.amazonaws.com", self.bucket, self.region);
        match artifact_type {
            ArtifactType::Groth16Circuit => format!("{}/{}-groth16.tar.gz", url_base, id),
            ArtifactType::PlonkCircuit => format!("{}/{}-plonk.tar.gz", url_base, id),
            _ => format!("{}/{}/{}", url_base, Self::get_s3_prefix(artifact_type), id),
        }
    }

    pub fn get_s3_key_from_id(artifact_type: ArtifactType, id: &str) -> String {
        match artifact_type {
            ArtifactType::Groth16Circuit => format!("{}-groth16.tar.gz", id),
            ArtifactType::PlonkCircuit => format!("{}-plonk.tar.gz", id),
            _ => format!("{}/{}", Self::get_s3_prefix(artifact_type), id),
        }
    }

    pub async fn par_download_file(
        &self,
        artifact_type: ArtifactType,
        id: &str,
    ) -> Result<Vec<u8>> {
        let key = Self::get_s3_key_from_id(artifact_type, id);

        let size = match self.s3_dl_mode.clone() {
            S3DownloadMode::REST(dl_client_rest) => {
                dl_client_rest.get_object_size(&self.bucket, &key).await?
            }
            S3DownloadMode::AwsSDK(dl_client_sdk) => {
                dl_client_sdk.get_object_size(&self.bucket, &key).await?
            }
        };

        // If the file is smaller than the chunk size, just download it.
        if size <= CHUNK_SIZE as i64 {
            let bytes = backoff::future::retry(BACKOFF.clone(), || {
                let bucket = self.bucket.clone();
                let key = key.clone();
                async move {
                    let ret = match self.s3_dl_mode.clone() {
                        S3DownloadMode::REST(dl_client_rest) => {
                            dl_client_rest.read_bytes(&bucket, &key, None).await
                        }
                        S3DownloadMode::AwsSDK(dl_client_sdk) => {
                            dl_client_sdk.read_bytes(&bucket, &key, None).await
                        }
                    };
                    ret.map_err(|e| backoff::Error::permanent(anyhow!(e)))
                }
            })
            .await?;
            return Ok(bytes.to_vec());
        }

        let starts = (0..size)
            .step_by(CHUNK_SIZE)
            .enumerate()
            .collect::<Vec<_>>();
        let threads = std::cmp::min(self.concurrency, starts.len());
        let thread_size = std::cmp::max(starts.len().div_ceil(threads), 1);
        // Split into up to S3_CONCURRENCY threads. For each thread, acquire a permit and download chunks.
        let mut set = JoinSet::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(threads);
        starts.chunks(thread_size).for_each(|thread_starts| {
            let s3_dl_mode = self.s3_dl_mode.clone();
            let key = key.clone();
            let tx = tx.clone();
            let semaphore = self.semaphore.clone();
            let thread_starts = thread_starts.to_vec();
            let bucket = self.bucket.clone();
            set.spawn(async move {
                let _permit = semaphore.acquire().await;
                for (index, start) in thread_starts {
                    let end = std::cmp::min(start + CHUNK_SIZE as i64, size) - 1;
                    let body = backoff::future::retry(BACKOFF.clone(), || async {
                        let key = key.clone();
                        let bucket = bucket.clone();
                        let ret = match s3_dl_mode.clone() {
                            S3DownloadMode::REST(s3_dl_client_rest) => {
                                s3_dl_client_rest
                                    .read_bytes(&bucket, &key, Some((start, end)))
                                    .await
                            }
                            S3DownloadMode::AwsSDK(s3_dl_client_sdk) => {
                                s3_dl_client_sdk
                                    .read_bytes(&bucket, &key, Some((start, end)))
                                    .await
                            }
                        };

                        ret.map_err(|e| backoff::Error::permanent(anyhow!(e)))
                    })
                    .await?;
                    tx.send((index, body)).await?;
                }
                Ok::<(), anyhow::Error>(())
            });
        });
        drop(tx);
        let mut result = vec![0_u8; size as usize];
        while let Some((index, chunk)) = rx.recv().await {
            let end = std::cmp::min(index * CHUNK_SIZE + chunk.len(), size as usize);
            result[index * CHUNK_SIZE..end].copy_from_slice(&chunk);
        }
        // Make sure all threads did not panic
        while !set.is_empty() {
            let res = set.join_next().await.unwrap();
            match res {
                Ok(inner) => match inner {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("artifact download thread panicked: {:?}", e);
                    }
                },
                Err(e) => {
                    panic!("artifact download thread panicked: {:?}", e);
                }
            }
        }

        Ok(result)
    }

    async fn par_upload_file(
        &self,
        artifact_type: ArtifactType,
        id: &str,
        data: Vec<u8>,
    ) -> Result<()> {
        let key = Self::get_s3_key_from_id(artifact_type, id);

        // If the file is smaller than the chunk size, just upload it.
        if data.len() <= CHUNK_SIZE {
            self.sdk_client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(data.clone().into())
                .send()
                .await?;
            return Ok(());
        }
        let data = Arc::new(data);

        let create_multipart_upload = self
            .sdk_client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key.clone())
            .send()
            .await?;

        let upload_id = create_multipart_upload.upload_id().unwrap().to_string();

        // Upload in parallel
        let threads = std::cmp::min(self.concurrency, data.len());
        // Split into up to S3_CONCURRENCY threads. For each thread, acquire a permit and upload chunks.
        let num_chunks = std::cmp::max(data.len().div_ceil(CHUNK_SIZE), 1);
        let chunk_starts = (0..num_chunks)
            .map(|i| i * CHUNK_SIZE)
            .enumerate()
            .collect::<Vec<_>>();
        let mut set = JoinSet::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(num_chunks);
        chunk_starts.chunks(threads).for_each(|chunk_inputs| {
            let client = self.sdk_client.clone();
            let key = key.clone();
            let tx = tx.clone();
            let upload_id = upload_id.clone();
            let data = data.clone();
            let chunk_inputs = chunk_inputs.to_vec();
            let bucket = self.bucket.clone();
            let semaphore = self.semaphore.clone();
            set.spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                for (i, start) in chunk_inputs {
                    let upload_id = upload_id.clone();
                    let end = std::cmp::min(start + CHUNK_SIZE, data.len());
                    let chunk = (*data)[start..end].to_vec();
                    let bytes = Bytes::from(chunk.to_vec());
                    let body = ByteStream::new(SdkBody::from(bytes));
                    let upload_part = client
                        .upload_part()
                        .bucket(&bucket)
                        .key(key.clone())
                        .upload_id(upload_id)
                        .body(body)
                        .part_number(i as i32 + 1)
                        .send();
                    let part = upload_part.await.unwrap();

                    tx.send((
                        i,
                        aws_sdk_s3::types::CompletedPart::builder()
                            .e_tag(part.e_tag().unwrap())
                            .part_number(i as i32 + 1)
                            .build(),
                    ))
                    .await
                    .unwrap();
                }
            });
        });
        drop(tx);
        drop(data);
        let mut parts = vec![None; num_chunks];
        while let Some((i, part)) = rx.recv().await {
            parts[i] = Some(part);
        }
        let upload_parts = parts
            .into_iter()
            .map(|part_option| part_option.unwrap())
            .collect::<Vec<_>>();

        self.sdk_client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key.clone())
            .upload_id(upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(upload_parts))
                    .build(),
            )
            .send()
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl ArtifactClient for S3ArtifactClient {
    #[instrument(name = "upload", level = "info", fields(id = artifact.id()), skip(self, artifact, data))]
    async fn upload_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
        data: Vec<u8>,
    ) -> Result<()> {
        self.par_upload_file(artifact_type, artifact.id(), data)
            .await
    }

    #[instrument(name = "download", level = "info", fields(id = artifact.id()), skip(self, artifact))]
    async fn download_raw(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
    ) -> Result<Vec<u8>> {
        self.par_download_file(artifact_type, artifact.id()).await
    }

    async fn exists(
        &self,
        artifact: &impl ArtifactId,
        artifact_type: ArtifactType,
    ) -> Result<bool> {
        let key = Self::get_s3_key_from_id(artifact_type, artifact.id());
        match self
            .sdk_client
            .head_object()
            .bucket(&self.bucket)
            .key(key.clone())
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.as_service_error().map(|e| e.is_not_found()) == Some(true) {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    async fn delete(
        &self,
        _artifact: &impl ArtifactId,
        _artifact_type: ArtifactType,
    ) -> Result<()> {
        Ok(())
    }

    async fn delete_batch(
        &self,
        _artifacts: &[impl ArtifactId],
        _artifact_type: ArtifactType,
    ) -> Result<()> {
        Ok(())
    }
}
