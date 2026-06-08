mod api_outage;
mod cancel;
mod coordinator_restart;
mod execute_only;
mod fatal_failure;
mod mixed_load;
mod multi_worker;
mod proof_modes;
mod quick;
mod retryable;
mod s3_artifacts;
mod shutdown_drain;
mod worker_death;
mod worker_restart;

use sp1_sdk::{Elf, SP1Stdin};

use crate::programs;
use crate::scenario::{Flavor, Scenario};

/// Iteration count for the "long" fibonacci workload on the cpu-only flavor: big enough
/// that proving takes minutes (a window to inject faults into), small enough to keep the
/// full tier bounded AND the CPU prover's peak RSS around ~10GB (500k iterations peaked at
/// ~25GB anon RSS, which OOMs a busy 128GB dev box and leaves no headroom on CI runners).
pub const LONG_FIB_N: u32 = 200_000;

/// Per-flavor "long/large" workload (spec: workload parametrization). gpu => rsp (real
/// block, large artifacts); cpu-only => fibonacci with a large iteration count (rsp on CPU
/// is impractically slow).
pub fn long_program() -> (Elf, SP1Stdin) {
    match Flavor::current() {
        Flavor::Gpu => (programs::RSP_ELF.clone(), programs::RSP_STDIN.clone()),
        Flavor::CpuOnly => (
            programs::FIBONACCI_ELF.clone(),
            programs::fibonacci_stdin(LONG_FIB_N),
        ),
    }
}

pub fn all() -> Vec<Scenario> {
    let mut scenarios = vec![quick::scenario()];
    scenarios.extend(proof_modes::scenarios());
    scenarios.push(execute_only::scenario());
    scenarios.push(multi_worker::scenario());
    scenarios.push(mixed_load::scenario());
    scenarios.push(worker_death::scenario());
    scenarios.push(retryable::scenario());
    scenarios.push(fatal_failure::scenario());
    scenarios.extend(cancel::scenarios());
    scenarios.push(coordinator_restart::scenario());
    scenarios.push(worker_restart::scenario());
    scenarios.push(api_outage::scenario());
    scenarios.push(shutdown_drain::scenario());
    scenarios.push(s3_artifacts::scenario());
    scenarios
}
