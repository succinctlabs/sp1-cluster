pub use sp1_prover_types::cluster::*;
pub use sp1_prover_types::worker::*;

pub mod events {
    //! Push-channel proto for proof_request lifecycle events.
    //!
    //! Lives alongside [`ClusterService`] on the bin/api gRPC server but is a
    //! separate service so we can iterate without rev-bumping
    //! sp1-prover-types' cluster.proto. Translate the int `proof_status`
    //! field via [`super::status_from_i32`] / [`super::status_to_i32`] so
    //! callers stay in the strongly-typed [`super::ProofRequestStatus`] world.
    tonic::include_proto!("cluster_events");
}

/// Translate the wire `proof_status: i32` on [`events::ProofEvent`] into the
/// strongly-typed [`ProofRequestStatus`].
pub fn status_from_i32(value: i32) -> ProofRequestStatus {
    ProofRequestStatus::try_from(value).unwrap_or(ProofRequestStatus::Unspecified)
}

/// Inverse of [`status_from_i32`].
pub fn status_to_i32(status: ProofRequestStatus) -> i32 {
    status as i32
}
