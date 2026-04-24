//! End-to-end test against a running docker-compose stack.
//!
//! Assumes the cluster is already up (api + coordinator + at least one worker)
//! and a `sp1-cluster-network-gateway` process is serving on `GATEWAY_RPC_URL`
//! (default `http://127.0.0.1:50061`). Marked `#[ignore]` so `cargo test` in
//! CI or during local hack-sessions doesn't block on it; run explicitly with:
//!
//! ```bash
//! # one-time cluster bring-up
//! docker compose -f infra/docker-compose.local.yml up -d redis postgresql api coordinator cpu-node gpu0
//! # run the gateway in another shell (see bin/network-gateway/README.md)
//!
//! cargo test -p sp1-cluster-network-gateway --test docker_e2e -- --ignored --nocapture
//! ```
//!
//! Requires a GPU-capable worker in the stack to actually produce a proof in
//! reasonable time. A CPU-only stack will execute but core proving will take
//! considerably longer.

use sp1_sdk::{
    network::{FulfillmentStrategy, NetworkMode},
    Elf, ProveRequest, Prover, ProverClient, ProvingKey, SP1Stdin,
};

/// The ELF is the same fibonacci test program the CLI bench command uses
/// (`bin/cli/src/commands/bench.rs`).
const FIBONACCI_ELF: &[u8] = include_bytes!("../../../artifacts/fibonacci.bin");

fn gateway_url() -> String {
    std::env::var("GATEWAY_RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:50061".to_string())
}

/// Any parseable key works when the gateway runs with `auth_mode=none`.
fn signer_key() -> String {
    std::env::var("NETWORK_PRIVATE_KEY").unwrap_or_else(|_| {
        "0x0000000000000000000000000000000000000000000000000000000000000001".to_string()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires running docker-compose stack + gateway; see module docs"]
async fn fibonacci_core_proof_roundtrip_via_gateway() {
    sp1_sdk::utils::setup_logger();

    let mut stdin = SP1Stdin::new();
    // Small fibonacci input so the proof completes quickly on a single worker.
    stdin.write(&100_000u32);

    let prover = ProverClient::builder()
        .network_for(NetworkMode::Reserved)
        .rpc_url(&gateway_url())
        .private_key(&signer_key())
        .build()
        .await;

    let pk = prover
        .setup(Elf::Static(FIBONACCI_ELF))
        .await
        .expect("setup failed");

    let proof = prover
        .prove(&pk, stdin)
        .strategy(FulfillmentStrategy::Reserved)
        .core()
        .await
        .expect("prove failed");

    prover
        .verify(&proof, pk.verifying_key(), None)
        .expect("proof verification failed");
}
