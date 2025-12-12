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
    VkGen,
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
        Commands::VkGen => {
            let build_vkeys = BuildVkeys::parse();
            build_vkeys.run().await
        }
    } {
        tracing::info!("Error: {:?}", e);
    }
}
