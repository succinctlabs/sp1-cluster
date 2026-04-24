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

/// Mint a fresh request id matching cluster convention: a typeid-prefixed UUIDv7.
///
/// The SDK treats `request_id` as opaque bytes and passes them back on every
/// subsequent status poll. We store it as UTF-8 of the typeid string so that the
/// cluster `proof_id` (see `proof_id_from_request_id`) reads like every other
/// cluster-minted id (e.g. `req_01h...`).
pub fn mint_request_id() -> Vec<u8> {
    use mti::prelude::{MagicTypeIdExt, V7};
    "req".create_type_id::<V7>().to_string().into_bytes()
}

/// Cluster `proof_id` derived from an SDK request_id.
pub fn proof_id_from_request_id(request_id: &[u8]) -> String {
    String::from_utf8_lossy(request_id).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_is_typeid_and_round_trips_to_proof_id() {
        let rid = mint_request_id();
        let s = String::from_utf8(rid.clone()).expect("typeid is valid UTF-8");
        assert!(s.starts_with("req_"), "expected typeid prefix, got {s}");
        assert_eq!(proof_id_from_request_id(&rid), s);
    }

    #[test]
    fn request_ids_are_unique() {
        let a = mint_request_id();
        let b = mint_request_id();
        assert_ne!(a, b);
    }
}
