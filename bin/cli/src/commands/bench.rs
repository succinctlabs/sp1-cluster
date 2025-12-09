use std::path::PathBuf;

use clap::{Args, Subcommand};
use eyre::Result;
use sp1_cluster_bench_utils::{run_benchmark_from_env, BenchmarkResults, ClusterElf};
use sp1_sdk::{network::proto::types::ProofMode, SP1Stdin};
use sp1_sdk::{CpuProver, Prover, ProvingKey};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Args)]
pub struct CommonArgs {
    /// The cluster API gRPC endpoint.
    #[arg(long, env = "CLI_CLUSTER_RPC")]
    pub cluster_rpc: String,

    /// Whether to execute the proof request before submitting it to the cluster.
    #[arg(long, default_value_t = false)]
    pub execute: bool,

    /// The S3 bucket the cluster artifact store is using.
    #[arg(long, env = "CLI_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// The S3 region the cluster artifact store is using.
    #[arg(long, env = "CLI_S3_REGION")]
    pub s3_region: Option<String>,

    /// The Redis nodes the cluster artifact store is using. If not specified, the artifact store will be S3.
    #[arg(long, env = "CLI_REDIS_NODES")]
    pub redis_nodes: Option<String>,

    #[arg(short, long, default_value="compressed", value_parser = parse_proof_mode)]
    pub mode: ProofMode,

    #[arg(short, long, default_value_t = 1)]
    pub count: u32,
}

pub fn parse_proof_mode(s: &str) -> Result<ProofMode> {
    ProofMode::from_str_name(&s.to_ascii_uppercase())
        .ok_or_else(|| eyre::eyre!("Invalid proof mode"))
}

#[derive(Subcommand, Debug)]
pub enum BenchCommand {
    Fibonacci {
        /// Number of cycles in millions to run the benchmark for.
        #[clap(default_value_t = 5)]
        mcycles: u32,
        #[clap(flatten)]
        common: CommonArgs,
    },
    Input {
        elf_file: PathBuf,
        stdin_file: PathBuf,
        #[clap(flatten)]
        common: CommonArgs,
    },
    S3 {
        /// S3 path to the program (e.g., "path/to/program" for s3://bucket/path/to/program/)
        s3_path: String,
        #[arg(short, long, default_value = "")]
        param: String,
        /// S3 bucket to download from
        #[arg(long, env = "CLI_BENCH_S3_BUCKET", default_value = "sp1-testing-suite")]
        bucket: String,
        #[clap(flatten)]
        common: CommonArgs,
    },
}

impl BenchCommand {
    pub async fn run(&self) -> Result<()> {
        // Run the actual benchmark
        match self {
            BenchCommand::Fibonacci { mcycles, common } => {
                tracing::info!(
                    "Running {}x Fibonacci {:?} for {} million cycles...",
                    common.count,
                    common.mode,
                    mcycles
                );
                let mut stdin = SP1Stdin::new();
                stdin.write(&(mcycles * 83333));

                let elf = include_bytes!("../../../../artifacts/fibonacci.bin");
                Self::run_benchmark(
                    elf.to_vec(),
                    stdin.clone(),
                    common,
                    Some(*mcycles as u64 * 1_000_000),
                )
                .await?;
            }
            BenchCommand::Input {
                elf_file,
                stdin_file,
                common,
            } => {
                tracing::info!(
                    "Running {:?} / {:?} {:?} benchmark...",
                    elf_file,
                    stdin_file,
                    common.mode
                );
                let elf = std::fs::read(elf_file)?;
                let stdin = bincode::deserialize::<SP1Stdin>(&std::fs::read(stdin_file)?)?;
                let proof_ids = Self::run_benchmark(elf.to_vec(), stdin, common, None).await?;

                let elf_name = elf_file.file_name().unwrap().to_str().unwrap();
                let workload_name = stdin_file.file_name().unwrap().to_str().unwrap();
                // Write all proof IDs to CSV
                for proof_id in proof_ids {
                    if let Err(e) = Self::append_to_csv(&proof_id, elf_name, Some(workload_name)) {
                        tracing::warn!("Failed to write to CSV: {}", e);
                    }
                }
            }
            BenchCommand::S3 {
                s3_path,
                bucket,
                common,
                param,
            } => {
                tracing::info!("Downloading program from s3://{}/{}...", bucket, s3_path);
                let (elf, stdin) = Self::download_from_s3(bucket, s3_path, &param).await?;
                tracing::info!("Running S3 program {:?} benchmark...", s3_path);
                let proof_ids = Self::run_benchmark(elf, stdin, common, None).await?;

                // Write all proof IDs to CSV
                for proof_id in proof_ids {
                    if let Err(e) = Self::append_to_csv(&proof_id, s3_path, Some(param.as_str())) {
                        tracing::warn!("Failed to write to CSV: {}", e);
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_benchmark(
        elf: Vec<u8>,
        stdin: SP1Stdin,
        common: &CommonArgs,
        cycles_estimate: Option<u64>,
    ) -> Result<Vec<String>> {
        let client = CpuProver::new_experimental().await;

        let elf_arc = sp1_sdk::Elf::from(elf.clone());
        let pk = client.setup(elf_arc).await.expect("failed to setup elf");

        let cluster_elf = ClusterElf::NewElf(elf);

        let BenchmarkResults { proof_ids, proofs } =
            run_benchmark_from_env(common.mode, 4, cluster_elf, stdin, cycles_estimate).await?;

        // Write all proofs to CSV
        for proof in proofs {
            client
                .verify(&proof.into(), pk.verifying_key(), None)
                .expect("failed to verify proof");
        }

        Ok(proof_ids)
    }

    async fn download_from_s3(
        bucket: &str,
        s3_path: &str,
        param: &str,
    ) -> Result<(Vec<u8>, SP1Stdin)> {
        // Download program.bin from S3
        let program_output = std::process::Command::new("aws")
            .args([
                "s3",
                "cp",
                &format!("s3://{}/{}/program.bin", bucket, s3_path),
                "program.bin",
            ])
            .output()?;

        if !program_output.status.success() {
            return Err(eyre::eyre!(
                "Failed to download program.bin: {}",
                String::from_utf8_lossy(&program_output.stderr)
            ));
        }

        // Download stdin.bin from S3
        let stdin_output = if param.is_empty() {
            std::process::Command::new("aws")
                .args([
                    "s3",
                    "cp",
                    &format!("s3://{}/{}/stdin.bin", bucket, s3_path),
                    "stdin.bin",
                ])
                .output()?
        } else {
            std::process::Command::new("aws")
                .args([
                    "s3",
                    "cp",
                    &format!("s3://{}/{}/input/{}.bin", bucket, s3_path, param),
                    "stdin.bin",
                ])
                .output()?
        };

        if !stdin_output.status.success() {
            return Err(eyre::eyre!(
                "Failed to download stdin.bin: {}",
                String::from_utf8_lossy(&stdin_output.stderr)
            ));
        }

        // Read the downloaded files
        let program = std::fs::read("program.bin")?;
        let stdin_bytes = std::fs::read("stdin.bin")?;
        let stdin: SP1Stdin = bincode::deserialize(&stdin_bytes)?;

        // Clean up the downloaded files
        std::fs::remove_file("program.bin").ok();
        std::fs::remove_file("stdin.bin").ok();

        Ok((program, stdin))
    }

    fn append_to_csv(proof_id: &str, s3_path: &str, param: Option<&str>) -> Result<()> {
        let csv_path = "data/bench_results.csv";

        // Create data directory if it doesn't exist
        if let Some(parent) = Path::new(csv_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file_exists = Path::new(csv_path).exists();

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(csv_path)?;

        // Write header if file is new
        if !file_exists {
            writeln!(file, "proof_id,s3_path,param")?;
        }

        // Write data row
        writeln!(file, "{},{},{}", proof_id, s3_path, param.unwrap_or(""))?;

        Ok(())
    }
}
