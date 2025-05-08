use p3_baby_bear::BabyBear;
use serde::{Deserialize, Serialize};
use sp1_core_executor::SP1ReduceProof;
use sp1_prover::{CoreSC, InnerSC};
use sp1_recursion_circuit::machine::SP1MerkleProofWitnessValues;
use sp1_stark::{Challenger, StarkVerifyingKey};
use sp1_stark::{ShardProof, Word};

/// Input data for a core batch task.
#[derive(Serialize, Deserialize)]
pub struct CoreBatchData {
    pub shard_proofs: Vec<ShardProof<InnerSC>>,
    pub initial_reconstruct_challenger: Challenger<CoreSC>,
    pub is_complete: bool,
    pub total_execution_shards: usize,
    pub is_first_shard: bool,
}

/// Input data for a deferred batch task.
#[derive(Serialize, Deserialize)]
pub struct DeferredBatchData {
    pub vks_and_proofs: Vec<(StarkVerifyingKey<InnerSC>, ShardProof<InnerSC>)>,
    pub vk_merkle_data: SP1MerkleProofWitnessValues<InnerSC>,
    pub start_reconstruct_deferred_digest: [BabyBear; 8],
    pub is_complete: bool,
    pub committed_value_digest: [Word<BabyBear>; 8],
    pub deferred_proofs_digest: [BabyBear; 8],
    pub leaf_challenger: Challenger<CoreSC>,
    pub end_pc: BabyBear,
    pub end_shard: BabyBear,
    pub end_execution_shard: BabyBear,
    pub total_execution_shards: usize,
    pub init_addr_bits: [BabyBear; 32],
    pub finalize_addr_bits: [BabyBear; 32],
}

/// Input data for a reduce batch task.
#[derive(Serialize, Deserialize)]
pub struct ReduceBatchData {
    pub vks_and_proofs: Vec<SP1ReduceProof<InnerSC>>,
    pub is_complete: bool,
    pub total_execution_shards: usize,
}
