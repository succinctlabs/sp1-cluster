use std::pin::Pin;
use std::time::Duration;

/// Which build flavor this binary is. `SP1_CLUSTER_CPU_ONLY` is compile-time, so the
/// flavor is fixed per build: gpu feature => Gpu, otherwise CpuOnly.
/// (main.rs keeps the existing panic if neither gpu feature nor SP1_CLUSTER_CPU_ONLY is set.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    Gpu,
    CpuOnly,
}

impl Flavor {
    pub fn current() -> Self {
        if cfg!(feature = "gpu") {
            Flavor::Gpu
        } else {
            Flavor::CpuOnly
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Flavor::Gpu => "gpu",
            Flavor::CpuOnly => "cpu-only",
        }
    }
}

/// Which flavors a scenario can run under. Every current scenario is `Both`; the
/// flavor-specific variants exist for full-tier scenarios that only make sense on one
/// flavor (kept for Plan 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavors {
    #[allow(dead_code)]
    Gpu,
    #[allow(dead_code)]
    CpuOnly,
    Both,
}

impl Flavors {
    pub fn supports(self, flavor: Flavor) -> bool {
        match self {
            Flavors::Both => true,
            Flavors::Gpu => flavor == Flavor::Gpu,
            Flavors::CpuOnly => flavor == Flavor::CpuOnly,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Smoke,
    Full,
}

pub type ScenarioFuture = Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>;

pub struct Scenario {
    pub name: &'static str,
    pub flavors: Flavors,
    /// Hard per-scenario timeout enforced by the suite runner (generous: first runs
    /// include circuit-artifact downloads).
    pub timeout: Duration,
    /// Excluded from `suite full` (still runnable via `run <name>`): for scenarios with
    /// known environment-level blockers, with the reason documented at the definition.
    pub skip_in_full: bool,
    pub run: fn() -> ScenarioFuture,
}

/// Smoke tier is an explicit per-flavor list (spec Section 2 "Tiers"). Proof modes are
/// one scenario each so every mode runs in its own process against a fresh cluster.
///
/// plonk/groth16 are full-tier only: their SHRINK_WRAP stage needs roughly >=16GB of free
/// GPU memory, which shared/desktop GPUs often lack (prod wrap runs on headless 24GB
/// g6/g5 workers). The full tier targets dedicated runners.
pub fn smoke_names(flavor: Flavor) -> &'static [&'static str] {
    match flavor {
        Flavor::Gpu => &["proof-mode-core", "proof-mode-compressed", "execute-only"],
        Flavor::CpuOnly => &["quick", "execute-only"],
    }
}

/// Resolve the scenario list for (tier, flavor). Panics if a smoke name is missing from
/// the registry — that is a programmer error, caught by the unit tests below.
pub fn resolve(all: &[Scenario], tier: Tier, flavor: Flavor) -> Vec<&Scenario> {
    match tier {
        Tier::Smoke => smoke_names(flavor)
            .iter()
            .map(|name| {
                all.iter()
                    .find(|s| s.name == *name)
                    .unwrap_or_else(|| panic!("smoke scenario {name} not in registry"))
            })
            .collect(),
        Tier::Full => all
            .iter()
            .filter(|s| s.flavors.supports(flavor) && !s.skip_in_full)
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy(name: &'static str, flavors: Flavors) -> Scenario {
        Scenario {
            name,
            flavors,
            timeout: Duration::from_secs(1),
            skip_in_full: false,
            run: || Box::pin(async { Ok(()) }),
        }
    }

    #[test]
    fn full_tier_filters_by_flavor() {
        let all = vec![
            dummy("a", Flavors::Both),
            dummy("b", Flavors::Gpu),
            dummy("c", Flavors::CpuOnly),
        ];
        let gpu: Vec<_> = resolve(&all, Tier::Full, Flavor::Gpu)
            .iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(gpu, vec!["a", "b"]);
        let cpu: Vec<_> = resolve(&all, Tier::Full, Flavor::CpuOnly)
            .iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(cpu, vec!["a", "c"]);
    }

    #[test]
    fn smoke_tier_is_explicit_per_flavor() {
        let all = vec![
            dummy("quick", Flavors::Both),
            dummy("proof-mode-core", Flavors::Both),
            dummy("proof-mode-compressed", Flavors::Both),
            dummy("proof-mode-plonk", Flavors::Both),
            dummy("proof-mode-groth16", Flavors::Both),
            dummy("execute-only", Flavors::Both),
        ];
        let gpu: Vec<_> = resolve(&all, Tier::Smoke, Flavor::Gpu)
            .iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(
            gpu,
            vec!["proof-mode-core", "proof-mode-compressed", "execute-only"]
        );
        let cpu: Vec<_> = resolve(&all, Tier::Smoke, Flavor::CpuOnly)
            .iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(cpu, vec!["quick", "execute-only"]);
    }

    #[test]
    #[should_panic(expected = "not in registry")]
    fn smoke_panics_on_missing_scenario() {
        let all = vec![dummy("quick", Flavors::Both)];
        resolve(&all, Tier::Smoke, Flavor::Gpu);
    }
}
