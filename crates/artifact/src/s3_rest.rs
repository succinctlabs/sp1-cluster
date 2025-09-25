use anyhow::Result;

pub struct S3RestClient {
    client: reqwest::Client,
    region: String,
}

fn get_s3_url_from_id(bucket: &str, region: &str, key: &str) -> String {
    format!("https://{}.s3.{}.amazonaws.com/{}", bucket, region, key)
}

impl S3RestClient {
    pub fn new(region: String) -> Self {
        S3RestClient {
            region,
            client: reqwest::Client::new(),
        }
    }

    pub async fn get_object_size(&self, bucket: &str, key: &str) -> Result<i64> {
        let obj_url = get_s3_url_from_id(bucket, &self.region, key);

        let result = self.client.get(obj_url).send().await;
        result
            .map(|res| res.content_length().unwrap() as i64)
            .map_err(anyhow::Error::new)
    }

    pub async fn read_byte_range(
        &self,
        bucket: &str,
        key: &str,
        range: (i64, i64),
    ) -> Result<Vec<u8>> {
        let obj_url = get_s3_url_from_id(bucket, &self.region, key);
        let range_header = format!("bytes={}-{}", range.0, range.1);

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
    }
}
