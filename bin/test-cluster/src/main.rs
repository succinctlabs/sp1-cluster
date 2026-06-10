mod assert;
mod cluster;
mod env;
mod programs;
mod request;
mod scenario;
mod scenarios;
mod suite;
mod utils;

use scenario::{Flavor, Tier};

#[derive(Debug)]
enum Cmd {
    Run(String),
    Suite(Tier),
    List,
}

fn parse_args(args: &[String]) -> Result<Cmd, String> {
    match args {
        [] => Ok(Cmd::Suite(Tier::Smoke)),
        [cmd, name] if cmd == "run" => Ok(Cmd::Run(name.clone())),
        [cmd, tier] if cmd == "suite" => match tier.as_str() {
            "smoke" => Ok(Cmd::Suite(Tier::Smoke)),
            "full" => Ok(Cmd::Suite(Tier::Full)),
            other => Err(format!("unknown tier {other:?} (smoke|full)")),
        },
        [cmd] if cmd == "list" => Ok(Cmd::List),
        other => Err(format!(
            "usage: sp1-test-cluster [run <scenario> | suite <smoke|full> | list], got {other:?}"
        )),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(not(feature = "gpu"))]
    if option_env!("SP1_CLUSTER_CPU_ONLY").is_none() {
        panic!(
            "You need to either enable the \"gpu\" feature or set the \"SP1_CLUSTER_CPU_ONLY\" env"
        )
    }

    sp1_cluster_common::logger::init(opentelemetry_sdk::Resource::empty());

    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = parse_args(&args).map_err(|e| anyhow::anyhow!(e))?;
    let flavor = Flavor::current();

    match cmd {
        Cmd::List => {
            for s in scenarios::all() {
                println!(
                    "{:<24} cpu_timeout={:?} gpu_timeout={:?}",
                    s.name, s.cpu_timeout, s.gpu_timeout
                );
            }
            Ok(())
        }
        Cmd::Run(name) => {
            let all = scenarios::all();
            let s = all
                .iter()
                .find(|s| s.name == name)
                .ok_or_else(|| anyhow::anyhow!("unknown scenario {name:?} (see `list`)"))?;
            tracing::info!("running scenario {name} (flavor {})", flavor.as_str());
            (s.run)().await?;
            tracing::info!("scenario {name} PASSED");
            Ok(())
        }
        Cmd::Suite(tier) => {
            let all = scenarios::all();
            let ok = suite::run_suite(&all, tier, flavor).await?;
            if !ok {
                std::process::exit(1);
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn parses_commands() {
        assert!(matches!(parse_args(&s(&["run", "quick"])), Ok(Cmd::Run(n)) if n == "quick"));
        assert!(matches!(
            parse_args(&s(&["suite", "smoke"])),
            Ok(Cmd::Suite(Tier::Smoke))
        ));
        assert!(matches!(
            parse_args(&s(&["suite", "full"])),
            Ok(Cmd::Suite(Tier::Full))
        ));
        assert!(matches!(parse_args(&s(&["list"])), Ok(Cmd::List)));
        assert!(matches!(parse_args(&s(&[])), Ok(Cmd::Suite(Tier::Smoke))));
        assert!(parse_args(&s(&["bogus"])).is_err());
        assert!(parse_args(&s(&["suite", "bogus"])).is_err());
    }
}
