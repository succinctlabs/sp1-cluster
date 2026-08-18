use clap::{Parser, Subcommand};
use commands::bench::BenchCommand;

use crate::commands::vk_gen::BuildVkeys;

mod commands;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(subcommand)]
    Bench(BenchCommand),
    #[command(subcommand)]
    VkGen(BuildVkeys),
}

#[tokio::main]
async fn main() {
    if let Err(e) = dotenv::dotenv() {
        eprintln!("not loading .env file: {}", e);
    }
    sp1_sdk::setup_logger();
    let cli = Cli::parse();

    if let Err(e) = match &cli.command {
        Commands::Bench(bench_command) => bench_command.run().await,
        Commands::VkGen(build_vkeys) => build_vkeys.run().await,
    } {
        tracing::info!("Error: {:?}", e);
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Cli;

    #[test]
    fn count_rejects_zero_and_values_above_the_batch_limit() {
        for count in ["0", "65536"] {
            let result = Cli::try_parse_from([
                "test",
                "bench",
                "fibonacci",
                "20",
                "--cluster-rpc",
                "http://127.0.0.1:50051",
                "--count",
                count,
            ]);

            assert!(result.is_err(), "accepted --count {count}");
        }
    }
}
