use std::pin::Pin;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Smoke,
    Full,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Smoke => "smoke",
            Tier::Full => "full",
        }
    }
}

pub type ScenarioFuture = Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>;

pub struct Scenario {
    pub name: &'static str,
    /// Wall-clock budget for the scenario child process, sized for gpu runners
    /// (tight, to catch wedged workers fast).
    pub timeout: Duration,
    pub tier: Tier,
    pub run: fn() -> ScenarioFuture,
}
