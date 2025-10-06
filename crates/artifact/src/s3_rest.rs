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

        let result = self.client.get(obj_url.clone()).send().await;
        match result {
            Ok(res) => match res.content_length() {
                Some(len) => Ok(len as i64),
                None => Err(anyhow::Error::msg(format!(
                    "failed to get content size of {}",
                    obj_url
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
        let obj_url = get_s3_url_from_id(bucket, &self.region, key);

        let mut builder = self.client.get(obj_url);
        if let Some((start, end)) = range {
            let range_header = format!("bytes={}-{}", start, end);
            builder = builder.header("Range", range_header);
        }

        let result = builder.send().await;
        match result {
            Ok(res) => {
                let bytes = res.bytes().await;
                bytes.map(|b| b.to_vec()).map_err(anyhow::Error::new)
            }
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }
}
