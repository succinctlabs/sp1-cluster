//! Self-reported build identity for cluster prover components.
//!
//! Prover-component build reporting: the fulfiller reports build identity
//! (version / git sha / image tag) to the SPN via the public `ReportProverInfo`
//! contract. It sends its own component plus the cluster manifest (coordinator +
//! every connected worker) that the coordinator periodically pushes to the cluster
//! API — the fulfiller reads it back over its existing cluster client, so no
//! topology-specific coordinator address is needed. This is best-effort debugging
//! telemetry — never block or fail fulfillment on it.

use sp1_cluster_common::proto::{
    ClusterCapacitySnapshot as ClusterCapacity, ClusterComponentInfo, GpuClassCount as ClusterGpu,
};
use spn_network_types::{
    ClusterCapacitySnapshot, ComponentInfo, GpuClassCount, ReportProverInfoRequestBody,
};

/// The git commit this binary was built from. Supplied by the `VERGEN_GIT_SHA`
/// build ARG in Docker builds; read from `.git` for local builds (see build.rs).
pub const VERGEN_GIT_SHA: &str = env!("VERGEN_GIT_SHA");

/// The component name the fulfiller reports itself as. Must be in the network's
/// component allowlist {fulfiller, coordinator, gpu-node, cpu-node}.
pub const FULFILLER_COMPONENT: &str = "fulfiller";

/// How often the fulfiller re-reports, in addition to the one-shot report at startup.
///
/// This interval also sets the width of every utilization window: the capacity snapshot is
/// part of the same report, and the server differences consecutive snapshots' counters. If
/// you change this interval, you change the resolution of published utilization.
pub const REPORT_INTERVAL_SECS: u64 = 15 * 60;

/// The fulfiller's static build identity, resolved once at startup.
#[derive(Clone, Debug)]
pub struct BuildIdentity {
    /// Crate version (`CARGO_PKG_VERSION`).
    pub version: String,
    /// Git commit the binary was built from.
    pub git_sha: String,
    /// Container image tag the component is running (from the `IMAGE_TAG` env var).
    pub image_tag: String,
}

impl BuildIdentity {
    /// Resolve the fulfiller's build identity from compile-time and runtime sources.
    pub fn resolve() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: VERGEN_GIT_SHA.to_string(),
            image_tag: std::env::var("IMAGE_TAG").unwrap_or_default(),
        }
    }
}

/// The fulfiller's own component entry. The fulfiller is a logical singleton, so a
/// report includes exactly one.
pub fn fulfiller_component(identity: &BuildIdentity) -> ComponentInfo {
    ComponentInfo {
        component: FULFILLER_COMPONENT.to_string(),
        version: identity.version.clone(),
        git_sha: identity.git_sha.clone(),
        image_tag: identity.image_tag.clone(),
    }
}

/// Map a coordinator-reported SP1 v6.3.1 `ClusterComponentInfo` onto the public
/// network `ComponentInfo`. Both are keyed by build identity (component +
/// version/git_sha/image_tag), so this is a straight field copy.
pub fn component_from_cluster(c: ClusterComponentInfo) -> ComponentInfo {
    ComponentInfo {
        component: c.component,
        version: c.version,
        git_sha: c.git_sha,
        image_tag: c.image_tag,
    }
}

/// Map the coordinator's cluster-internal GPU capacity snapshot onto the public network type.
///
/// A field-for-field copy with equal units: GPU time is GPU-milliseconds on both sides. Do
/// not add a conversion.
///
/// This must not go through [`crate::assemble_components`]: its build-identity dedupe
/// removes the per-node counts this snapshot reports.
pub fn capacity_from_cluster(capacity: ClusterCapacity) -> ClusterCapacitySnapshot {
    ClusterCapacitySnapshot {
        observed_at: capacity.observed_at,
        counters_since: capacity.counters_since,
        gpu_nodes: capacity.gpu_nodes,
        gpu_available_ms_total: capacity.gpu_available_ms_total,
        gpu_busy_ms_total: capacity.gpu_busy_ms_total,
        gpus: capacity.gpus.into_iter().map(gpu_class_count).collect(),
    }
}

/// Map one cluster GPU class count onto the public type. Direct field copy.
fn gpu_class_count(gpu: ClusterGpu) -> GpuClassCount {
    GpuClassCount {
        name: gpu.name,
        memory_total_bytes: gpu.memory_total_bytes,
        node_count: gpu.node_count,
    }
}

/// Build the `ReportProverInfoRequestBody` carrying the given component list and capacity
/// snapshot.
///
/// The fulfiller assembles `[fulfiller self] ++ (coordinator manifest)` and sends
/// it in ONE request, so the SPN sees the whole prover cluster's build identity
/// atomically. `ReportProverInfo` carries no nonce and writes no ledger tx.
///
/// `capacity` is `None` if the cluster reported none; an absent snapshot must not fail a
/// report.
pub fn build_report_prover_info_body(
    domain: &[u8],
    prover: &[u8],
    components: Vec<ComponentInfo>,
    capacity: Option<ClusterCapacitySnapshot>,
) -> ReportProverInfoRequestBody {
    ReportProverInfoRequestBody {
        domain: domain.to_vec(),
        prover: prover.to_vec(),
        components,
        capacity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    fn identity() -> BuildIdentity {
        BuildIdentity {
            version: "2.5.0".to_string(),
            git_sha: "abc1234".to_string(),
            image_tag: "base-abc1234".to_string(),
        }
    }

    /// A heterogeneous cluster: eight L4 nodes, two H100 nodes, one unidentified node.
    fn cluster_capacity() -> ClusterCapacity {
        ClusterCapacity {
            observed_at: 1_700_000_500,
            counters_since: 1_700_000_000,
            gpu_nodes: 11,
            gpu_available_ms_total: 5_500_000,
            gpu_busy_ms_total: 1_375_000,
            gpus: vec![
                ClusterGpu {
                    name: "NVIDIA H100 80GB HBM3".to_string(),
                    memory_total_bytes: 80 * GIB,
                    node_count: 2,
                },
                ClusterGpu {
                    name: "NVIDIA L4".to_string(),
                    memory_total_bytes: 24 * GIB,
                    node_count: 8,
                },
                ClusterGpu {
                    name: String::new(),
                    memory_total_bytes: 0,
                    node_count: 1,
                },
            ],
        }
    }

    #[test]
    fn build_body_carries_component_list_verbatim() {
        let domain = [0xaau8; 32];
        let prover = [0x11u8; 20];

        let body = build_report_prover_info_body(
            &domain,
            &prover,
            vec![fulfiller_component(&identity())],
            None,
        );

        assert_eq!(body.domain, domain.to_vec());
        assert_eq!(body.prover, prover.to_vec());
        assert_eq!(body.components.len(), 1);
        assert_eq!(body.components[0].component, "fulfiller");
        assert_eq!(body.components[0].git_sha, "abc1234");
        assert!(body.capacity.is_none(), "no capacity => field absent");
    }

    #[test]
    fn build_body_carries_capacity_when_present() {
        let body = build_report_prover_info_body(
            &[0xaau8; 32],
            &[0x11u8; 20],
            vec![fulfiller_component(&identity())],
            Some(capacity_from_cluster(cluster_capacity())),
        );

        let capacity = body.capacity.expect("capacity forwarded on the body");
        assert_eq!(capacity.gpu_nodes, 11);
        assert_eq!(capacity.gpus.len(), 3);
    }

    #[test]
    fn capacity_maps_every_field_without_conversion() {
        let cluster = cluster_capacity();
        let mapped = capacity_from_cluster(cluster.clone());

        assert_eq!(mapped.observed_at, cluster.observed_at);
        assert_eq!(mapped.counters_since, cluster.counters_since);
        assert_eq!(mapped.gpu_nodes, cluster.gpu_nodes);
        // Milliseconds on both sides: the numbers must be identical, not rescaled.
        assert_eq!(
            mapped.gpu_available_ms_total,
            cluster.gpu_available_ms_total
        );
        assert_eq!(mapped.gpu_busy_ms_total, cluster.gpu_busy_ms_total);
    }

    #[test]
    fn capacity_preserves_every_gpu_class_without_deduping() {
        let mapped = capacity_from_cluster(cluster_capacity());

        // The mapping is entry for entry: it must not merge classes like the component dedupe
        // merges same-build workers.
        assert_eq!(mapped.gpus.len(), 3, "every class survives the mapping");
        let l4 = mapped
            .gpus
            .iter()
            .find(|g| g.name == "NVIDIA L4")
            .expect("L4 class present");
        assert_eq!(l4.node_count, 8, "eight same-model nodes stay eight");
        assert_eq!(l4.memory_total_bytes, 24 * GIB, "bytes stay bytes");

        // The breakdown still reconciles with the total after mapping.
        let counted: u32 = mapped.gpus.iter().map(|g| g.node_count).sum();
        assert_eq!(counted, mapped.gpu_nodes);
    }

    #[test]
    fn capacity_preserves_an_unidentified_gpu_class() {
        let mapped = capacity_from_cluster(cluster_capacity());

        // An unidentified node is still a device: it maps to an unknown class, not to a drop,
        // so `node_count` keeps summing to `gpu_nodes`.
        let unknown = mapped
            .gpus
            .iter()
            .find(|g| g.name.is_empty())
            .expect("unidentified class present");
        assert_eq!(unknown.node_count, 1);
        assert_eq!(unknown.memory_total_bytes, 0);
    }

    #[test]
    fn capacity_with_no_gpu_nodes_maps_to_an_empty_breakdown() {
        let empty = ClusterCapacity {
            observed_at: 1_700_000_500,
            counters_since: 1_700_000_000,
            gpu_nodes: 0,
            gpu_available_ms_total: 0,
            gpu_busy_ms_total: 0,
            gpus: vec![],
        };
        let mapped = capacity_from_cluster(empty);
        assert_eq!(mapped.gpu_nodes, 0);
        assert!(mapped.gpus.is_empty());
    }
}
