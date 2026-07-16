use spn_network_types::GetProverRequirementsResponse;

/// The network's published prover performance requirements, fetched from
/// `GetProverRequirements`. A fulfilled request counts toward the prover's success
/// rate only if fulfilled within `perf_deadline` of its creation; falling below the
/// success-rate bar leads to suspension.
#[derive(Debug, Clone, PartialEq)]
pub struct PerformanceRequirements {
    /// Whole-request proving rate in MGas/s required whenever it grants more time
    /// than the floor.
    pub min_mgas_per_second: f64,
    /// Minimum time budget in seconds granted to every request.
    pub floor_latency_seconds: u64,
}

impl PerformanceRequirements {
    /// Parse the wire response. Returns `None` when the network enforces no
    /// requirements (absent sub-message) or the rate doesn't parse — both degrade to
    /// unclamped bidding.
    pub fn from_response(resp: &GetProverRequirementsResponse) -> Option<Self> {
        let req = resp.requirements.as_ref()?;
        let min_mgas_per_second: f64 = req.min_mgas_per_second.parse().ok()?;
        if !min_mgas_per_second.is_finite() || min_mgas_per_second <= 0.0 {
            return None;
        }
        Some(Self {
            min_mgas_per_second,
            floor_latency_seconds: req.floor_latency_seconds,
        })
    }
}

/// Absolute unix deadline by which a request of `gas_limit` must be fulfilled to
/// count as successful under the network's performance requirements: the budget is
/// `max(floor, gas / rate)`.
///
/// The network judges on actual `gas_used`, unknown before execution; `gas_limit` is
/// its upper bound, so when the rate term governs, the computed budget can be looser
/// than the one the network will apply. Tolerated: the fulfillment check estimates
/// completion time from the same `gas_limit`, so both sides overestimate together
/// and the committed pace stays honest.
pub fn perf_deadline(created_at: u64, gas_limit: u64, req: &PerformanceRequirements) -> u64 {
    let rate_secs = (gas_limit as f64 / (req.min_mgas_per_second * 1e6)) as u64;
    created_at.saturating_add(rate_secs.max(req.floor_latency_seconds))
}

#[cfg(test)]
mod tests {
    use spn_network_types::ProverRequirements;

    use super::*;

    fn requirements() -> PerformanceRequirements {
        PerformanceRequirements {
            min_mgas_per_second: 24.0,
            floor_latency_seconds: 420,
        }
    }

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
            PerformanceRequirements::from_response(&response()),
            Some(requirements())
        );
    }

    #[test]
    fn absent_requirements_parse_to_none() {
        let resp = GetProverRequirementsResponse { requirements: None };
        assert_eq!(PerformanceRequirements::from_response(&resp), None);
    }

    #[test]
    fn invalid_rate_parses_to_none() {
        for rate in ["", "abc", "0", "-24", "NaN", "inf"] {
            let mut resp = response();
            resp.requirements.as_mut().unwrap().min_mgas_per_second = rate.to_string();
            assert_eq!(
                PerformanceRequirements::from_response(&resp),
                None,
                "rate {rate}"
            );
        }
    }

    #[test]
    fn small_gas_gets_floor_budget() {
        // The rate would grant 0s and ~42s respectively; the 420s floor governs.
        assert_eq!(perf_deadline(1_000, 0, &requirements()), 1_420);
        assert_eq!(perf_deadline(1_000, 1_000_000_000, &requirements()), 1_420);
    }

    #[test]
    fn large_gas_gets_rate_budget() {
        // 14.4B gas / 24 MGas/s = 600s, above the 420s floor.
        assert_eq!(
            perf_deadline(1_000, 14_400_000_000, &requirements()),
            1_000 + 600
        );
    }

    #[test]
    fn saturates_instead_of_overflowing() {
        assert_eq!(perf_deadline(u64::MAX, u64::MAX, &requirements()), u64::MAX);
    }
}
