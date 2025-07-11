use crate::{ArtifactClient, ArtifactId, ArtifactType};

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

#[derive(Clone)]
pub struct S3ArtifactClient {
    pub inner: Arc<S3Client>,
    pub bucket: String,
    semaphore: Arc<Semaphore>,
    concurrency: usize,
}

impl S3ArtifactClient {
    pub async fn new(region: String, bucket: String, concurrency: usize) -> Self {
        let s3_client = {
            let mut base = aws_config::load_defaults(BehaviorVersion::latest())
                .await
                .to_builder();
            let timeout_config = TimeoutConfig::builder()
                .operation_attempt_timeout(Duration::from_secs(120))
                .build();
            base.set_retry_config(Some(
                RetryConfig::standard()
                    .with_max_attempts(7)
                    .with_max_backoff(Duration::from_secs(30)),
            ))
            .set_timeout_config(Some(timeout_config))
            .set_sleep_impl(default_async_sleep())
            .set_region(Some(Region::new(region)));
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
        };
        let semaphore = Arc::new(Semaphore::new(concurrency));
        Self {
            inner: Arc::new(s3_client),
            bucket,
            semaphore,
            concurrency,
        }
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
        }
    }

    pub fn get_s3_key_from_id(artifact_type: ArtifactType, id: &str) -> String {
        format!("{}/{}", Self::get_s3_prefix(artifact_type), id)
    }

    async fn par_download_file(&self, artifact_type: ArtifactType, id: &str) -> Result<Vec<u8>> {
        let key = Self::get_s3_key_from_id(artifact_type, id);

        let size = self
            .inner
            .head_object()
            .bucket(&self.bucket)
            .key(key.clone())
            .send()
            .await?
            .content_length
            .unwrap();

        // If the file is smaller than the chunk size, just download it.
        if size <= CHUNK_SIZE as i64 {
            let bytes = backoff::future::retry(BACKOFF.clone(), || {
                let client = self.inner.clone();
                let bucket = self.bucket.clone();
                let key = key.clone();
                async move {
                    let res = client
                        .get_object()
                        .bucket(&bucket)
                        .key(key)
                        .send()
                        .await
                        .map_err(|e| backoff::Error::permanent(anyhow!(e)))?;
                    let body = res
                        .body
                        .collect()
                        .await
                        .map_err(|e| backoff::Error::transient(anyhow!(e)))?;
                    let bytes = body.into_bytes();
                    Ok::<_, backoff::Error<anyhow::Error>>(bytes)
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
            let client = self.inner.clone();
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
                        let res = client
                            .get_object()
                            .bucket(&bucket)
                            .key(key.clone())
                            .range(format!("bytes={}-{}", start, end))
                            .send()
                            .await
                            .map_err(|e| backoff::Error::permanent(anyhow!(e)))?;
                        res.body
                            .collect()
                            .await
                            .map_err(|e| backoff::Error::transient(anyhow!(e)))
                    })
                    .await?;
                    tx.send((index, body.into_bytes())).await?;
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
        tracing::debug!("size: {}", data.len());

        // If the file is smaller than the chunk size, just upload it.
        if data.len() <= CHUNK_SIZE {
            self.inner
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
            .inner
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
            let client = self.inner.clone();
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

        self.inner
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
            .inner
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
