use std::collections::HashSet;
use std::str::FromStr;

use alloy_primitives::{Address, Signature};
use clap::ValueEnum;
use tonic::Status;

/// How the gateway validates incoming signed requests.
///
/// Applies only to RPCs that carry a signed body (`create_artifact`,
/// `create_program`, `request_proof`). Read-only RPCs are unauthenticated.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum AuthMode {
    /// Accept all requests; `requester` is reported as the zero address.
    #[default]
    None,
    /// Verify the signature and set `requester` to the recovered address.
    Verify,
    /// `Verify` + require the recovered address to be in the allowlist.
    Allowlist,
}

#[derive(Clone, Debug, Default)]
pub struct Auth {
    pub mode: AuthMode,
    pub allowlist: HashSet<Address>,
}

impl Auth {
    /// Verify a signature over `message` and return the requester address.
    ///
    /// - `None` → always returns the zero address; signature untouched.
    /// - `Verify`/`Allowlist` → parses the signature as 65-byte `[r||s||v]`, recovers the
    ///   signer via EIP-191, and (for `Allowlist`) checks membership.
    #[allow(clippy::result_large_err)] // matches tonic's Status return shape
    pub fn authorize(&self, message: &[u8], signature: &[u8]) -> Result<Address, Status> {
        match self.mode {
            AuthMode::None => Ok(Address::ZERO),
            AuthMode::Verify | AuthMode::Allowlist => {
                let sig = Signature::from_raw(signature).map_err(|e| {
                    Status::unauthenticated(format!("invalid signature bytes: {e}"))
                })?;
                let addr = sig.recover_address_from_msg(message).map_err(|e| {
                    Status::unauthenticated(format!("signature recovery failed: {e}"))
                })?;
                if self.mode == AuthMode::Allowlist && !self.allowlist.contains(&addr) {
                    return Err(Status::permission_denied(format!(
                        "signer {addr} not in allowlist"
                    )));
                }
                Ok(addr)
            }
        }
    }
}

/// Parse a comma-separated list of 0x-prefixed addresses.
pub fn parse_allowlist(input: &str) -> Result<HashSet<Address>, String> {
    input
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| Address::from_str(s).map_err(|e| format!("invalid address {s}: {e}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;

    fn sign(message: &[u8], signer: &PrivateKeySigner) -> Vec<u8> {
        signer.sign_message_sync(message).unwrap().as_bytes().to_vec()
    }

    #[test]
    fn none_mode_returns_zero_address() {
        let auth = Auth { mode: AuthMode::None, allowlist: Default::default() };
        let addr = auth.authorize(b"anything", b"garbage").unwrap();
        assert_eq!(addr, Address::ZERO);
    }

    #[test]
    fn verify_recovers_signer() {
        let signer = PrivateKeySigner::random();
        let expected = signer.address();
        let msg = b"hello gateway";
        let sig = sign(msg, &signer);

        let auth = Auth { mode: AuthMode::Verify, allowlist: Default::default() };
        assert_eq!(auth.authorize(msg, &sig).unwrap(), expected);
    }

    #[test]
    fn verify_rejects_wrong_signature() {
        let msg = b"hello";
        let auth = Auth { mode: AuthMode::Verify, allowlist: Default::default() };
        let err = auth.authorize(msg, &[0u8; 65]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn allowlist_admits_members_and_rejects_others() {
        let insider = PrivateKeySigner::random();
        let outsider = PrivateKeySigner::random();
        let msg = b"payload";

        let allowlist: HashSet<_> = std::iter::once(insider.address()).collect();
        let auth = Auth { mode: AuthMode::Allowlist, allowlist };

        assert_eq!(auth.authorize(msg, &sign(msg, &insider)).unwrap(), insider.address());
        let err = auth.authorize(msg, &sign(msg, &outsider)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn parse_allowlist_handles_mixed_whitespace() {
        let parsed = parse_allowlist(
            "0x0000000000000000000000000000000000000001, 0x0000000000000000000000000000000000000002",
        )
        .unwrap();
        assert_eq!(parsed.len(), 2);
        assert!(parsed.contains(&Address::from_str("0x0000000000000000000000000000000000000001").unwrap()));
    }
}
