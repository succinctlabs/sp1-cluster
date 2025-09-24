use anyhow::Result;

use crate::s3::S3DownloadClient;
pub struct S3RestClient {
    client: reqwest::Client,
    region: String,
}

fn get_s3_url_from_id(bucket: String, region: String, key: String) -> String {
    format!("https://{}.s3.{}.amazonaws.com/{}", bucket, region, key)
}

impl S3RestClient {
    pub async fn new(region: String) -> Self {
        S3RestClient {
            region,
            client: reqwest::Client::new(),
        }
    }
}

impl S3DownloadClient for S3RestClient {
    fn get_object_size(&self, bucket: String, key: String) -> Result<i64> {
        let obj_url = get_s3_url_from_id(bucket, self.region.clone(), key);

        let rt_handle = tokio::runtime::Handle::current();
        rt_handle.block_on(async {
            let result = self.client.get(obj_url).send().await;

            result
                .map(|res| res.content_length().unwrap() as i64)
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
        let obj_url = get_s3_url_from_id(bucket, self.region.clone(), key);

        let rt_handle = tokio::runtime::Handle::current();
        rt_handle.block_on(async {
            let range_header = format!("bytes={}-{}", start, end);

            let result = self
                .client
                .get(obj_url)
                .header("Range", range_header)
                .send()
                .await;

            if let Err(err) = result {
                return Err(anyhow::Error::new(err));
            }

            let bytes = result.unwrap().bytes().await;
            bytes.map(|b| b.to_vec()).map_err(anyhow::Error::new)
        })
    }
}
