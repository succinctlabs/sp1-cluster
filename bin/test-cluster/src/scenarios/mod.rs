mod api_outage;
mod cancel;
mod coordinator_restart;
mod execute_only;
mod fatal_failure;
mod mixed_load;
mod multi_worker;
mod proof_modes;
mod retryable_prove_shard;
mod s3_artifacts;
mod shutdown_drain;
mod worker_death;
mod worker_restart;

use crate::scenario::{Scenario, Tier};

pub fn get(tier: Tier) -> Vec<Scenario> {
    match tier {
        Tier::Full => all(),
        Tier::Smoke => all()
            .into_iter()
            .filter(|s| matches!(s.tier, Tier::Smoke))
            .collect(),
    }
}

pub fn all() -> Vec<Scenario> {
    let mut res = Vec::new();
    res.extend(proof_modes::scenarios());
    res.push(execute_only::scenario());
    res.push(execute_only::scenario_rsp());
    res.push(multi_worker::scenario());
    res.push(mixed_load::scenario());
    res.push(worker_death::scenario());
    res.push(retryable_prove_shard::scenario());
    res.push(fatal_failure::scenario());
    res.extend(cancel::scenarios());
    res.push(coordinator_restart::scenario());
    res.push(worker_restart::scenario());
    res.push(api_outage::scenario());
    res.push(shutdown_drain::scenario());
    res.push(s3_artifacts::scenario());
    res
}
