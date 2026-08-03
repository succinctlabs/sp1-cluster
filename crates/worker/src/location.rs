//! Where a worker runs, and how it finds out.
//!
//! Two sources, in order: the worker's own ECS task metadata, and the region
//! its task definition puts in the environment. The coordinator groups workers
//! by the result, and the multi-region autoscaler credits each one to the pool
//! matching its region. A worker that reports nothing is credited to no pool,
//! and since this resolves once at startup, that lasts the life of the process.
//!
//! Outside ECS both sources are absent and no location is reported, which is
//! the right answer — a single-pool cluster never reads the field.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Duration;

/// A worker waits on the metadata endpoint before it can register, and
/// `reqwest` applies no timeout of its own. An endpoint that accepts the
/// connection but never answers would otherwise stall startup indefinitely.
const METADATA_TIMEOUT: Duration = Duration::from_secs(2);

/// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4-response.html
#[derive(Debug, Serialize, Deserialize)]
pub struct ECSTaskInfo {
    #[serde(rename = "Cluster")]
    pub cluster: String,
    #[serde(rename = "TaskARN")]
    pub task_arn: String,
}

impl ECSTaskInfo {
    /// The task's own id: the last segment of the task ARN.
    pub fn task_id(&self) -> &str {
        match self.task_arn.rsplit_once('/') {
            Some((_, id)) => id,
            None => &self.task_arn,
        }
    }

    /// The AWS region this task runs in, read out of its own ARN.
    ///
    /// An ARN is six colon-separated fields —
    /// `arn:aws:ecs:us-west-2:123456789012:task/cluster/id` — and field 4
    /// (index 3) is the region.
    ///
    /// `None` for anything that is not a well-formed ARN with a non-empty
    /// region field.
    pub fn region(&self) -> Option<String> {
        let mut fields = self.task_arn.split(':');
        if fields.next()? != "arn" {
            return None;
        }
        let region = fields.nth(2).filter(|r| !r.is_empty())?;
        // Fields 5 (account) and 6 (resource) must exist, or this is not an ARN.
        fields.next()?;
        fields.next()?;
        Some(region.to_string())
    }
}

pub async fn get_ecs_task_info(client: &Client) -> anyhow::Result<ECSTaskInfo> {
    let metadata_url = env::var("ECS_CONTAINER_METADATA_URI_V4")?;
    let response = client
        .get(metadata_url + "/task")
        .timeout(METADATA_TIMEOUT)
        .send()
        .await?;
    response.json().await.map_err(|e| e.into())
}

/// The region every worker task definition sets, used when task metadata could
/// not be reached. It names the region the task itself runs in, which is the
/// same value the task ARN carries.
pub fn ambient_region() -> Option<String> {
    env::var("AWS_REGION").ok()
}

/// The first source that names a region. Blank counts as absent, so a variable
/// that exists but was never given a value does not become a location.
pub fn resolve(from_task_arn: Option<String>, ambient: Option<String>) -> Option<String> {
    [from_task_arn, ambient]
        .into_iter()
        .flatten()
        .find(|region| !region.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{resolve, ECSTaskInfo};

    fn info(task_arn: &str) -> ECSTaskInfo {
        ECSTaskInfo {
            cluster: "c".into(),
            task_arn: task_arn.into(),
        }
    }

    #[test]
    fn task_id_is_the_last_arn_segment() {
        assert_eq!(
            info("arn:aws:ecs:us-west-2:123456789012:task/my-cluster/abc123").task_id(),
            "abc123"
        );
        // Nothing to split on: the whole string is the id.
        assert_eq!(info("abc123").task_id(), "abc123");
    }

    #[test]
    fn region_is_field_four_of_a_task_arn() {
        assert_eq!(
            info("arn:aws:ecs:us-west-2:123456789012:task/my-cluster/abc123").region(),
            Some("us-west-2".to_string())
        );
    }

    #[test]
    fn region_reads_the_arn_partition_agnostically() {
        assert_eq!(
            info("arn:aws-us-gov:ecs:us-gov-west-1:1:task/c/a").region(),
            Some("us-gov-west-1".to_string())
        );
    }

    #[test]
    fn malformed_arns_have_no_region() {
        // Not an ARN at all.
        assert_eq!(info("").region(), None);
        assert_eq!(info("my-cluster/abc123").region(), None);
        // Right shape, wrong prefix.
        assert_eq!(info("urn:aws:ecs:us-west-2:1:task/c/a").region(), None);
        // Truncated: no account or resource field.
        assert_eq!(info("arn:aws:ecs:us-west-2").region(), None);
        assert_eq!(info("arn:aws:ecs:us-west-2:1").region(), None);
        // Present but empty region field.
        assert_eq!(info("arn:aws:ecs::1:task/c/a").region(), None);
    }

    #[test]
    fn the_task_arn_region_wins_over_the_ambient_one() {
        let location = resolve(Some("us-west-2".into()), Some("us-east-1".into()));
        assert_eq!(location.as_deref(), Some("us-west-2"));
    }

    #[test]
    fn the_ambient_region_stands_in_when_metadata_is_unavailable() {
        let location = resolve(None, Some("us-east-1".into()));
        assert_eq!(location.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn a_blank_region_from_either_source_is_skipped() {
        assert_eq!(resolve(None, Some(String::new())), None);
        assert_eq!(
            resolve(Some(String::new()), Some("us-east-1".into())),
            Some("us-east-1".to_string())
        );
        assert_eq!(resolve(None, None), None);
    }
}
