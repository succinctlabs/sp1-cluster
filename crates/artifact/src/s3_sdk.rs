use anyhow::Result;
use aws_sdk_s3::Client as S3Client;

use crate::s3::S3DownloadClient;

pub struct S3SDKClient {
    s3_client: S3Client,
}

impl S3SDKClient {
    pub async fn new(s3_sdk_client: aws_sdk_s3::client::Client) -> Self {
        S3SDKClient {
            s3_client: s3_sdk_client,
        }
    }
}

impl S3DownloadClient for S3SDKClient {
    fn get_object_size(&self, bucket: String, key: String) -> Result<i64> {
        let rt_handle = tokio::runtime::Handle::current();
        rt_handle.block_on(async {
            self.s3_client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map(|r| r.content_length.unwrap())
                .map_err(anyhow::Error::new)
        })
    }

    fn read_byte_range(
        &self,
        bucket: String,
        key: String,
        start: i64,
        end: i64,
    ) -> Result<Vec<u8>> {
        let rt_handle = tokio::runtime::Handle::current();
        rt_handle.block_on(async {
            let result = self
                .s3_client
                .get_object()
                .bucket(&bucket)
                .key(key)
                .range(format!("bytes={}-{}", start, end))
                .send()
                .await;
            if let Err(err) = result {
                return Err(anyhow::Error::new(err));
            }

            let ret_bytes = result.unwrap().body.collect().await;
            ret_bytes
                .map(|bytes| bytes.into_bytes().into())
                .map_err(anyhow::Error::new)
        })
    }
}
