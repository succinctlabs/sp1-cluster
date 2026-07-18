//! Thin adapter from the wire `GetProverRequirementsResponse` to the shared
//! [`spn_bidding`] requirements math. The policy itself (parsing rules,
//! `perf_deadline`) lives in `spn-bidding` so both SPN bidders share one
//! implementation.

use spn_network_types::GetProverRequirementsResponse;

pub use spn_bidding::{perf_deadline, PerformanceRequirements};

/// Parse the wire response. Returns `None` when the network enforces no requirements
/// (absent sub-message) or the rate doesn't parse — both degrade to unclamped bidding.
pub fn from_response(resp: &GetProverRequirementsResponse) -> Option<PerformanceRequirements> {
    let req = resp.requirements.as_ref()?;
    PerformanceRequirements::from_parts(&req.min_mgas_per_second, req.floor_latency_seconds)
}

#[cfg(test)]
mod tests {
    use spn_network_types::ProverRequirements;

    use super::*;

    fn response() -> GetProverRequirementsResponse {
        GetProverRequirementsResponse {
            requirements: Some(ProverRequirements {
                min_mgas_per_second: "24".to_string(),
                floor_latency_seconds: 420,
                min_success_rate_percentage: "98".to_string(),
                performance_lookback_hours: 1,
                min_resolved_requests_to_evaluate: 1,
            }),
        }
    }

    #[test]
    fn parses_enabled_response() {
        assert_eq!(
            from_response(&response()),
            Some(PerformanceRequirements {
                min_mgas_per_second: 24.0,
                floor_latency_seconds: 420,
            })
        );
    }

    #[test]
    fn absent_requirements_parse_to_none() {
        let resp = GetProverRequirementsResponse { requirements: None };
        assert_eq!(from_response(&resp), None);
    }
}
