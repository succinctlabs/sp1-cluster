//! Self-reported build identity for cluster prover components.
//!
//! Prover-component build reporting: the fulfiller reports build identity
//! (version / git sha / image tag) to the SPN via the `ReportProverInfo` RPC
//! (network#232). It always reports its own component (fulfiller-only) and,
//! when a coordinator client is wired (`FULFILLER_COORDINATOR_RPC` is set),
//! also forwards the coordinator + every connected worker it fronts
//! (cluster-wide reporting). This is best-effort debugging telemetry — never
//! block or fail fulfillment on it.

use sp1_cluster_common::proto::ClusterComponentInfo;
use spn_network_types::{ComponentInfo, ReportProverInfoRequestBody};

/// The git commit this binary was built from. Supplied by the `VERGEN_GIT_SHA`
/// build ARG in Docker builds; read from `.git` for local builds (see build.rs).
pub const VERGEN_GIT_SHA: &str = env!("VERGEN_GIT_SHA");

/// The component name the fulfiller reports itself as. Must be in the network's
/// component allowlist {fulfiller, coordinator, gpu-node, cpu-node}.
pub const FULFILLER_COMPONENT: &str = "fulfiller";

/// How often the fulfiller re-reports its build identity, in addition to the
/// one-shot report at startup. Low frequency: this is static-ish telemetry.
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

/// Map a coordinator-reported `ClusterComponentInfo` (sp1#2850) onto the public
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

/// Build the `ReportProverInfoRequestBody` carrying the given component list.
///
/// The fulfiller assembles `[fulfiller self] ++ (coordinator manifest)` and sends
/// it in ONE request, so the SPN sees the whole prover cluster's build identity
/// atomically. `ReportProverInfo` carries no nonce and writes no ledger tx.
pub fn build_report_prover_info_body(
    domain: &[u8],
    prover: &[u8],
    components: Vec<ComponentInfo>,
) -> ReportProverInfoRequestBody {
    ReportProverInfoRequestBody {
        domain: domain.to_vec(),
        prover: prover.to_vec(),
        components,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fulfiller_component_has_build_fields() {
        let identity = BuildIdentity {
            version: "2.5.0".to_string(),
            git_sha: "abc1234".to_string(),
            image_tag: "base-abc1234".to_string(),
        };

        let c = fulfiller_component(&identity);
        assert_eq!(c.component, "fulfiller");
        assert_eq!(c.version, "2.5.0");
        assert_eq!(c.git_sha, "abc1234");
        assert_eq!(c.image_tag, "base-abc1234");
    }

    #[test]
    fn build_body_carries_component_list_verbatim() {
        let identity = BuildIdentity {
            version: "2.5.0".to_string(),
            git_sha: "abc1234".to_string(),
            image_tag: "base-abc1234".to_string(),
        };
        let domain = [0xaau8; 32];
        let prover = [0x11u8; 20];

        let body =
            build_report_prover_info_body(&domain, &prover, vec![fulfiller_component(&identity)]);

        assert_eq!(body.domain, domain.to_vec());
        assert_eq!(body.prover, prover.to_vec());
        assert_eq!(body.components.len(), 1);
        assert_eq!(body.components[0].component, "fulfiller");
        assert_eq!(body.components[0].git_sha, "abc1234");
    }

    #[test]
    fn component_from_cluster_copies_build_fields() {
        let cluster = ClusterComponentInfo {
            component: "gpu-node".to_string(),
            version: "2.5.0".to_string(),
            git_sha: "gpusha".to_string(),
            image_tag: "node-gpu-gpusha".to_string(),
        };
        let c = component_from_cluster(cluster);
        assert_eq!(c.component, "gpu-node");
        assert_eq!(c.version, "2.5.0");
        assert_eq!(c.git_sha, "gpusha");
        assert_eq!(c.image_tag, "node-gpu-gpusha");
    }

    #[test]
    fn resolve_uses_cargo_pkg_version() {
        let identity = BuildIdentity::resolve();
        assert_eq!(identity.version, env!("CARGO_PKG_VERSION"));
    }
}
