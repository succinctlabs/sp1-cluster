use sp1_cluster_artifact::ArtifactType;
use sp1_sdk::network::proto::artifact::ArtifactType as ProtoArtifactType;

/// Map the SDK's proto artifact type to the cluster's rust enum.
pub fn proto_to_cluster_type(proto: i32) -> ArtifactType {
    match ProtoArtifactType::try_from(proto).unwrap_or(ProtoArtifactType::UnspecifiedArtifactType) {
        ProtoArtifactType::Program => ArtifactType::Program,
        ProtoArtifactType::Stdin => ArtifactType::Stdin,
        ProtoArtifactType::Proof => ArtifactType::Proof,
        ProtoArtifactType::UnspecifiedArtifactType => ArtifactType::UnspecifiedArtifactType,
    }
}

/// URL path segment for an artifact type. Must round-trip with `parse_artifact_type_segment`.
pub fn artifact_type_segment(t: ArtifactType) -> &'static str {
    match t {
        ArtifactType::UnspecifiedArtifactType => "unspecified",
        ArtifactType::Program => "program",
        ArtifactType::Stdin => "stdin",
        ArtifactType::Proof => "proof",
        ArtifactType::Groth16Circuit => "groth16",
        ArtifactType::PlonkCircuit => "plonk",
    }
}

pub fn parse_artifact_type_segment(seg: &str) -> Option<ArtifactType> {
    Some(match seg {
        "unspecified" => ArtifactType::UnspecifiedArtifactType,
        "program" => ArtifactType::Program,
        "stdin" => ArtifactType::Stdin,
        "proof" => ArtifactType::Proof,
        "groth16" => ArtifactType::Groth16Circuit,
        "plonk" => ArtifactType::PlonkCircuit,
        _ => return None,
    })
}

/// Build the artifact URI the gateway hands back to SDK callers.
pub fn artifact_uri(public_base: &str, artifact_type: ArtifactType, id: &str) -> String {
    let base = public_base.trim_end_matches('/');
    format!(
        "{base}/artifacts/{}/{id}",
        artifact_type_segment(artifact_type)
    )
}

/// Extract the cluster artifact id from a URI produced by `artifact_uri`.
/// The last path segment is the id.
pub fn artifact_id_from_uri(uri: &str) -> Option<&str> {
    let path = uri.rsplit('/').next()?;
    if path.is_empty() {
        None
    } else {
        Some(path)
    }
}

/// Deterministic cluster artifact id for a program ELF, derived from `vk_hash`.
///
/// The cluster artifact store is keyed by opaque strings, so we can sidestep
/// `create_artifact()` minting and address the same ELF across many proofs.
/// As long as the SDK proves at least once per Redis TTL window (4h), the ELF
/// stays warm and `request_proof` can skip the re-upload entirely. On a cache
/// miss `request_proof` re-uploads from `ProgramStore` under the same id,
/// re-warming for subsequent calls.
pub fn program_artifact_id(vk_hash: &[u8]) -> String {
    format!("program_{}", hex::encode(vk_hash))
}

/// Maximum byte-length for a request id.
///
/// The SDK decodes the returned `request_id` via `B256::from_slice` (see
/// `sp1/crates/sdk/src/network/prover.rs`), so it MUST be exactly 32 bytes.
const REQUEST_ID_LEN: usize = 32;

/// Mint a fresh 32-byte request id that keeps the cluster's typeid convention.
///
/// We generate a V7 typeid string (e.g. `req_01h...`, ~30 bytes of UTF-8) and
/// zero-pad it to 32 bytes. The SDK round-trips those bytes on every subsequent
/// poll; `proof_id_from_request_id` strips the trailing zeros so the cluster
/// sees the typeid string verbatim.
pub fn mint_request_id() -> Vec<u8> {
    use mti::prelude::{MagicTypeIdExt, V7};
    let typeid = "req".create_type_id::<V7>().to_string();
    let mut buf = typeid.into_bytes();
    assert!(
        buf.len() <= REQUEST_ID_LEN,
        "typeid unexpectedly exceeded {REQUEST_ID_LEN} bytes: {} bytes",
        buf.len()
    );
    buf.resize(REQUEST_ID_LEN, 0);
    buf
}

/// Cluster `proof_id` derived from an SDK request_id — strips the zero padding
/// added by `mint_request_id`.
pub fn proof_id_from_request_id(request_id: &[u8]) -> String {
    let end = request_id
        .iter()
        .rposition(|&b| b != 0)
        .map(|i| i + 1)
        .unwrap_or(0);
    String::from_utf8_lossy(&request_id[..end]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_is_32_bytes_and_round_trips_to_typeid_proof_id() {
        let rid = mint_request_id();
        assert_eq!(rid.len(), REQUEST_ID_LEN, "SDK requires exactly 32 bytes");

        let proof_id = proof_id_from_request_id(&rid);
        assert!(
            proof_id.starts_with("req_"),
            "expected typeid prefix, got {proof_id}"
        );

        // The zero-padded tail must be stripped before the cluster sees it.
        assert!(!proof_id.contains('\0'));
        assert!(proof_id.len() <= REQUEST_ID_LEN);
    }

    #[test]
    fn request_ids_are_unique() {
        let a = mint_request_id();
        let b = mint_request_id();
        assert_ne!(a, b);
    }
}
