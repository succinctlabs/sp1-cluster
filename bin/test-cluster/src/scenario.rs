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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Smoke,
    Full,
}

pub type ScenarioFuture = Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>;

/// Every scenario runs under both build flavors (gpu and cpu-only); per-flavor
/// differences live inside the scenario bodies (cluster shape, workload size).
pub struct Scenario {
    pub name: &'static str,
    /// Hard timeout on the cpu-only flavor, enforced by the suite runner. Generous:
    /// CPU proving is slow, and first runs include circuit-artifact downloads.
    pub cpu_timeout: Duration,
    /// Hard timeout on the gpu flavor. Tight on purpose: GPU proving is fast (full-tier
    /// scenarios measure ~15s-10m on a g6.8xlarge), and the historical GPU failure mode
    /// is a wedged worker that never registers — that should fail the suite in minutes,
    /// not consume the job timeout.
    pub gpu_timeout: Duration,
    pub run: fn() -> ScenarioFuture,
}

impl Scenario {
    pub fn timeout(&self, flavor: Flavor) -> Duration {
        match flavor {
            Flavor::Gpu => self.gpu_timeout,
            Flavor::CpuOnly => self.cpu_timeout,
        }
    }
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
        Tier::Full => all.iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy(name: &'static str) -> Scenario {
        Scenario {
            name,
            cpu_timeout: Duration::from_secs(1),
            gpu_timeout: Duration::from_secs(1),
            run: || Box::pin(async { Ok(()) }),
        }
    }

    #[test]
    fn full_tier_includes_everything() {
        let all = vec![dummy("a"), dummy("b"), dummy("c")];
        let names: Vec<_> = resolve(&all, Tier::Full, Flavor::Gpu)
            .iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn smoke_tier_is_explicit_per_flavor() {
        let all = vec![
            dummy("quick"),
            dummy("proof-mode-core"),
            dummy("proof-mode-compressed"),
            dummy("proof-mode-plonk"),
            dummy("proof-mode-groth16"),
            dummy("execute-only"),
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
        let all = vec![dummy("quick")];
        resolve(&all, Tier::Smoke, Flavor::Gpu);
    }
}
