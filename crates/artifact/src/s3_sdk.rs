use anyhow::Result;
use aws_sdk_s3::Client as S3Client;

pub struct S3SDKClient {
    s3_client: S3Client,
}

impl S3SDKClient {
    pub fn new(s3_sdk_client: aws_sdk_s3::client::Client) -> Self {
        S3SDKClient {
            s3_client: s3_sdk_client,
        }
    }

    pub async fn get_object_size(&self, bucket: &str, key: &str) -> Result<i64> {
        let result = self
            .s3_client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(res) => match res.content_length() {
                Some(len) => Ok(len),
                None => Err(anyhow::Error::msg(format!(
                    "failed to get content size of s3 obj {}/{}",
                    bucket, key
                ))),
            },
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }

    pub async fn read_bytes(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(i64, i64)>,
    ) -> Result<Vec<u8>> {
        let mut builder = self.s3_client.get_object().bucket(bucket).key(key);
        if let Some((start, end)) = range {
            builder = builder.range(format!("bytes={}-{}", start, end));
        }

        let result = builder.send().await;
        match result {
            Ok(res) => {
                let ret_bytes = res.body.collect().await;
                ret_bytes
                    .map(|bytes| bytes.into_bytes().into())
                    .map_err(anyhow::Error::new)
            }
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }
}
