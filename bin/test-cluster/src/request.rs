use std::sync::Arc;

use anyhow::Result;
use rand::Rng;
use sp1_sdk::network::signer::NetworkSigner;
use sp1_sdk::prelude::*;
use sp1_sdk::ProverClient;
use sp1_sdk::SP1ProofMode;

pub struct ClusterProofRequests {
    pub rpc_url: String,
    pub requests: Vec<ClusterProofRequest>,
}

pub struct ClusterProofRequest {
    pub elf: Elf,
    pub stdin: SP1Stdin,
    pub modes: Vec<SP1ProofMode>,
}

/// Submit each (request, mode) one at a time, awaiting and verifying each proof before
/// submitting the next. Returns the cluster `proof_id` (`req_...`) for each submission,
/// in submission order. A single in-flight proof keeps GPU memory at the proven
/// single-proof footprint, so scenarios stay green on shared/desktop GPUs (concurrent
/// ProveShard tasks from multiple proofs can exceed free VRAM there — observed
/// AllocError on a desktop RTX 4090). Concurrency is exercised by the scheduling
/// scenarios via [`request_only`] instead.
pub async fn submit_proof_requests_sequential(input: ClusterProofRequests) -> Result<Vec<String>> {
    let prover = Arc::new(
        ProverClient::builder()
            .network()
            .hosted()
            .rpc_url(&input.rpc_url)
            .signer(random_local_signer())
            .build()
            .await,
    );

    let mut proof_ids = Vec::new();
    for req in input.requests {
        let pk = Arc::new(prover.setup(req.elf).await?);
        for mode in req.modes {
            let request_id = prover
                .prove(&pk, req.stdin.clone())
                .mode(mode)
                .request()
                .await?;
            let proof_id = proof_id_from_request_id(request_id.as_slice());
            tracing::info!("submitted {proof_id} ({mode:?}), awaiting before next submission");
            let proof = prover.wait_proof(request_id, None, None).await?;
            prover.verify(&proof, &pk.verifying_key().clone(), None)?;
            proof_ids.push(proof_id);
        }
    }

    Ok(proof_ids)
}

/// Submit a proof request through the gateway WITHOUT waiting for a proof. Returns the
/// cluster proof_id. Used against execute-only clusters, where no proof is ever produced
/// and the terminal state is observed via the cluster API instead.
pub async fn request_only(
    rpc_url: &str,
    elf: Elf,
    stdin: SP1Stdin,
    mode: SP1ProofMode,
) -> Result<String> {
    request_with_cycle_limit(rpc_url, elf, stdin, mode, None).await
}

/// Like [`request_only`], with an explicit cycle limit (fatal-failure scenario: a limit
/// below the program's real cycle count makes execution fail with ExceededCycleLimit).
pub async fn request_with_cycle_limit(
    rpc_url: &str,
    elf: Elf,
    stdin: SP1Stdin,
    mode: SP1ProofMode,
    cycle_limit: Option<u64>,
) -> Result<String> {
    let prover = ProverClient::builder()
        .network()
        .hosted()
        .rpc_url(rpc_url)
        .signer(random_local_signer())
        .build()
        .await;
    let pk = prover.setup(elf).await?;
    let mut req = prover.prove(&pk, stdin).mode(mode);
    if let Some(limit) = cycle_limit {
        req = req.cycle_limit(limit);
    }
    let request_id = req.request().await?;
    Ok(proof_id_from_request_id(request_id.as_slice()))
}

/// Recover the cluster `proof_id` string (e.g. `req_01k...`) from a network request id, which is
/// the proof_id's UTF-8 bytes right-padded to 32 bytes with trailing NULs.
fn proof_id_from_request_id(request_id: &[u8]) -> String {
    let end = request_id
        .iter()
        .rposition(|&b| b != 0)
        .map(|i| i + 1)
        .unwrap_or(0);
    String::from_utf8_lossy(&request_id[..end]).into_owned()
}

pub fn random_local_signer() -> NetworkSigner {
    NetworkSigner::local(&format!(
        "{:032x}{:032x}",
        rand::rng().random::<u128>(),
        rand::rng().random::<u128>()
    ))
    .expect("should be a valid signer")
}
