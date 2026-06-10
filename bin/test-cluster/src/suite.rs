use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::scenario::{resolve, Flavor, Scenario, Tier};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Pass,
    Fail,
    Timeout,
}

pub struct ScenarioResult {
    pub name: &'static str,
    pub outcome: Outcome,
    pub duration: Duration,
    pub log_path: std::path::PathBuf,
}

/// Run every (tier, flavor) scenario as a child process of this same binary.
/// A failure does not stop the suite. Returns true iff everything passed.
pub async fn run_suite(all: &[Scenario], tier: Tier, flavor: Flavor) -> Result<bool> {
    let scenarios = resolve(all, tier, flavor);
    prefetch_circuit_artifacts(&scenarios).await;
    let logs_dir = std::path::PathBuf::from("target/test-cluster-logs");
    std::fs::create_dir_all(&logs_dir).context("create logs dir")?;
    let exe = std::env::current_exe().context("current_exe")?;

    let mut results = Vec::new();
    for s in scenarios {
        let log_path = logs_dir.join(format!("{}.log", s.name));
        let log = std::fs::File::create(&log_path)
            .with_context(|| format!("create {}", log_path.display()))?;
        let timeout = s.timeout(flavor);
        tracing::info!(
            "=== running scenario {} (timeout {:?}) ===",
            s.name,
            timeout
        );
        let started = Instant::now();
        let mut child = tokio::process::Command::new(&exe)
            .arg("run")
            .arg(s.name)
            .stdout(Stdio::from(log.try_clone().context("clone log handle")?))
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawn scenario {}", s.name))?;

        let outcome = match tokio::time::timeout(timeout, child.wait()).await {
            Ok(Ok(status)) if status.success() => Outcome::Pass,
            Ok(Ok(_)) => Outcome::Fail,
            Ok(Err(e)) => return Err(e).context("wait for scenario child"),
            Err(_elapsed) => {
                // Kill the scenario process; its testcontainers are reaped by ryuk
                // once the connection drops.
                let _ = child.kill().await;
                Outcome::Timeout
            }
        };
        let duration = started.elapsed();
        tracing::info!(
            "=== scenario {} -> {:?} in {:?} ===",
            s.name,
            outcome,
            duration
        );
        if outcome != Outcome::Pass {
            // 2000 lines, not less: the cluster's 2s status polling fills ~150
            // lines/minute, and a 200-line tail held nothing but that noise when
            // the plonk scenario wedged.
            print_log_tail(&log_path, s.name, 2000);
        }
        results.push(ScenarioResult {
            name: s.name,
            outcome,
            duration,
            log_path,
        });
    }

    println!("{}", render_table(&results));
    Ok(results.iter().all(|r| r.outcome == Outcome::Pass))
}

/// The wrap-mode trusted-setup artifacts are a tens-of-GB download on a cold
/// ~/.sp1/circuits. Download them up front, outside the per-scenario timeouts: those
/// are sized for proving work (the gpu ones tightly, to catch wedged workers fast),
/// not for first-run downloads on an uncached runner.
async fn prefetch_circuit_artifacts(scenarios: &[&Scenario]) {
    for (scenario_name, kind) in [
        ("proof-mode-plonk", "plonk"),
        ("proof-mode-groth16", "groth16"),
    ] {
        if scenarios.iter().any(|s| s.name == scenario_name) {
            let started = Instant::now();
            tracing::info!("prefetching {kind} circuit artifacts (not counted against scenario timeouts)");
            match sp1_sdk::install::try_install_circuit_artifacts(kind).await {
                Ok(_) => {
                    tracing::info!("{kind} circuit artifacts ready in {:?}", started.elapsed())
                }
                // Don't fail the suite from here: the scenario downloads on demand as
                // well, and if the artifacts are truly unfetchable it fails with the
                // real error in its own log.
                Err(e) => tracing::warn!("prefetch of {kind} circuit artifacts failed: {e:#}"),
            }
        }
    }
}

/// Dump the tail of a failed scenario's log to stdout so CI failures are diagnosable
/// inline, without downloading the log artifact.
fn print_log_tail(path: &std::path::Path, name: &str, max_lines: usize) {
    match std::fs::read_to_string(path) {
        Ok(content) => {
            let lines: Vec<&str> = content.lines().collect();
            let start = lines.len().saturating_sub(max_lines);
            println!(
                "\n===== {name} FAILED — last {} of {} log lines ({}) =====",
                lines.len() - start,
                lines.len(),
                path.display()
            );
            for line in &lines[start..] {
                println!("{line}");
            }
            println!("===== end of {name} log =====\n");
        }
        Err(e) => println!("could not read scenario log {}: {e}", path.display()),
    }
}

pub fn render_table(results: &[ScenarioResult]) -> String {
    let mut out = String::from(
        "\n  scenario                  result    duration    log\n  \
         ------------------------------------------------------------\n",
    );
    for r in results {
        out.push_str(&format!(
            "  {:<25} {:<9} {:>8.0?}    {}\n",
            r.name,
            format!("{:?}", r.outcome).to_uppercase(),
            r.duration,
            r.log_path.display()
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_renders_all_outcomes() {
        let results = vec![
            ScenarioResult {
                name: "quick",
                outcome: Outcome::Pass,
                duration: Duration::from_secs(61),
                log_path: "target/test-cluster-logs/quick.log".into(),
            },
            ScenarioResult {
                name: "execute-only",
                outcome: Outcome::Timeout,
                duration: Duration::from_secs(2700),
                log_path: "target/test-cluster-logs/execute-only.log".into(),
            },
        ];
        let table = render_table(&results);
        assert!(table.contains("quick"));
        assert!(table.contains("PASS"));
        assert!(table.contains("TIMEOUT"));
        assert!(table.contains("execute-only.log"));
    }
}
