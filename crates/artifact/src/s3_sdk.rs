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
        self.s3_client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map(|r| r.content_length.unwrap())
            .map_err(anyhow::Error::new)
    }

    pub async fn read_bytes(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(i64, i64)>,
    ) -> Result<Vec<u8>> {
        let mut builder = self.s3_client.get_object().bucket(bucket).key(key);
        if range.is_some() {
            builder = builder.range(format!("bytes={}-{}", range.unwrap().0, range.unwrap().1));
        }

        let result = builder.send().await;
        if let Err(err) = result {
            return Err(anyhow::Error::new(err));
        }

        let ret_bytes = result.unwrap().body.collect().await;
        ret_bytes
            .map(|bytes| bytes.into_bytes().into())
            .map_err(anyhow::Error::new)
    }
}
