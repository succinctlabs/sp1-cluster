use clap::{Parser, Subcommand};
use commands::bench::BenchCommand;
use opentelemetry_sdk::Resource;
use sp1_cluster_common::logger;

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
}

#[tokio::main]
async fn main() {
    logger::init(Resource::empty());
    let cli = Cli::parse();

    if let Err(e) = match &cli.command {
        Commands::Bench(bench_command) => bench_command.run().await,
    } {
        tracing::info!("Error: {:?}", e);
    }
}
