use crate::{GenerationCredential, RecoveryLogPosition, RequestIdentity};
use ring::signature::{Ed25519KeyPair, UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const PUBLICATION_COMMAND_MAGIC: &[u8] = b"OKVP1";
const PUBLICATION_POP_CAPABILITY_MAGIC: &[u8] = b"OKV-PUBLICATION-POP-CAPABILITY-V1\0";

pub use okv_publication::{
    AuthorityContext as PublicationAuthorityContext,
    AuthorityPosition as PublicationAuthorityPosition, CollectionJobToken, CollectionReceipt,
    DeletePermit, ObjectIdentity, ObjectKind as PublicationObjectKind,
    ObjectReference as PublicationObjectReference, PreparedPublication, PublicationAction,
    PublicationAuthorityFaults, PublicationAuthorityState, PublicationCommandStatus,
    PublicationIntent, PublicationOutcome, RevisionToken, SnapshotClosure, SnapshotLeaseToken,
    SnapshotLeaseValidationError,
};

/// One generation-fenced publication command replicated by the authority log.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationCommand {
    pub identity: RequestIdentity,
    pub credential: GenerationCredential,
    pub action: PublicationAction,
}

impl PublicationCommand {
    /// Encode this command into objectKV-owned application bytes.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the command cannot be encoded.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut encoded = PUBLICATION_COMMAND_MAGIC.to_vec();
        encoded.extend(serde_json::to_vec(self)?);
        Ok(encoded)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        bytes
            .strip_prefix(PUBLICATION_COMMAND_MAGIC)
            .map(serde_json::from_slice)
            .transpose()
    }
}

/// Response reconstructed by replaying one committed publication command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationApplyResponse {
    pub status: PublicationCommandStatus,
    pub outcome: Option<PublicationOutcome>,
    pub state: PublicationAuthorityState,
    pub applied_log_position: RecoveryLogPosition,
}

/// Bounded unsafe authority behavior owned outside the pure state domain.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationFenceFaults {
    pub bypass_generation_fence: bool,
    pub local_stale_outcome_read: bool,
    pub prepare_as_previous_generation: bool,
}

/// Exact published root and object frontier authorized for tagged-log pop.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationPopCapabilityStatement {
    pub format_version: u16,
    pub authority_cell_id: u64,
    pub generation: u64,
    pub transaction_system_id: String,
    pub destination_root: String,
    pub manifest: PublicationObjectReference,
    pub object_frontier: u64,
    pub pop_epoch: u64,
}

impl PublicationPopCapabilityStatement {
    fn signing_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = PUBLICATION_POP_CAPABILITY_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(self)?);
        Ok(bytes)
    }
}

/// One publication-authority process signature over an exact pop capability.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationPopCapabilityAttestation {
    pub signer_id: u64,
    pub signature: Vec<u8>,
}

/// Quorum proof that replicated publication authority owns an exact root.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationPopCapabilityCertificate {
    pub statement: PublicationPopCapabilityStatement,
    pub attestations: Vec<PublicationPopCapabilityAttestation>,
}

/// Sign one locally verified publication-root capability.
///
/// # Errors
///
/// Returns an error when the seed or canonical statement is invalid.
pub fn sign_publication_pop_capability(
    signer_id: u64,
    private_key_seed: &[u8],
    statement: &PublicationPopCapabilityStatement,
) -> Result<PublicationPopCapabilityAttestation, String> {
    let pair = Ed25519KeyPair::from_seed_unchecked(private_key_seed)
        .map_err(|_| "publication signing seed must contain exactly 32 bytes".to_owned())?;
    let bytes = statement
        .signing_bytes()
        .map_err(|error| error.to_string())?;
    Ok(PublicationPopCapabilityAttestation {
        signer_id,
        signature: pair.sign(&bytes).as_ref().to_vec(),
    })
}

/// Verify distinct publication-authority signatures against pinned membership.
#[must_use]
pub fn verify_publication_pop_capability(
    certificate: &PublicationPopCapabilityCertificate,
    members: &BTreeMap<u64, Vec<u8>>,
    quorum_size: u16,
) -> bool {
    let statement = &certificate.statement;
    if statement.format_version != 1
        || statement.authority_cell_id == 0
        || statement.generation == 0
        || statement.transaction_system_id.is_empty()
        || statement.destination_root.is_empty()
        || statement.manifest.key.is_empty()
        || statement.manifest.sha256.is_empty()
        || statement.object_frontier == 0
        || statement.pop_epoch == 0
        || quorum_size == 0
    {
        return false;
    }
    let Ok(bytes) = statement.signing_bytes() else {
        return false;
    };
    let mut distinct = BTreeSet::new();
    for attestation in &certificate.attestations {
        if !distinct.insert(attestation.signer_id) {
            return false;
        }
        let Some(public_key) = members.get(&attestation.signer_id) else {
            return false;
        };
        if UnparsedPublicKey::new(&ED25519, public_key)
            .verify(&bytes, &attestation.signature)
            .is_err()
        {
            return false;
        }
    }
    distinct.len() >= usize::from(quorum_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery_public_key;

    #[test]
    fn publication_command_has_a_distinct_versioned_prefix() {
        let command = PublicationCommand {
            identity: RequestIdentity {
                client_id: 17,
                request_id: 19,
            },
            credential: GenerationCredential {
                generation: 7,
                transaction_system_id: "txn-7".to_owned(),
            },
            action: PublicationAction::Unpin {
                pin_id: "snapshot-1".to_owned(),
                expected: PublicationObjectReference {
                    kind: PublicationObjectKind::Manifest,
                    key: "objects/manifest".to_owned(),
                    length: 10,
                    sha256: "a".repeat(64),
                },
            },
        };
        let encoded = command.encode().unwrap();
        assert_eq!(Some(command), PublicationCommand::decode(&encoded).unwrap());
    }

    #[test]
    fn publication_pop_capability_requires_distinct_pinned_signers() {
        let statement = PublicationPopCapabilityStatement {
            format_version: 1,
            authority_cell_id: 17,
            generation: 3,
            transaction_system_id: "cell-17-g3".to_owned(),
            destination_root: "cell-17/ranges/all".to_owned(),
            manifest: PublicationObjectReference {
                kind: PublicationObjectKind::Manifest,
                key: "objects/manifest-12".to_owned(),
                length: 128,
                sha256: "a".repeat(64),
            },
            object_frontier: 12,
            pop_epoch: 4,
        };
        let seed_1 = [1_u8; 32];
        let seed_2 = [2_u8; 32];
        let members = BTreeMap::from([
            (1, recovery_public_key(&seed_1).unwrap()),
            (2, recovery_public_key(&seed_2).unwrap()),
        ]);
        let attestation_1 = sign_publication_pop_capability(1, &seed_1, &statement).unwrap();
        let attestation_2 = sign_publication_pop_capability(2, &seed_2, &statement).unwrap();
        let certificate = PublicationPopCapabilityCertificate {
            statement: statement.clone(),
            attestations: vec![attestation_1.clone(), attestation_2],
        };
        assert!(verify_publication_pop_capability(&certificate, &members, 2));

        let duplicate = PublicationPopCapabilityCertificate {
            statement: statement.clone(),
            attestations: vec![attestation_1.clone(), attestation_1],
        };
        assert!(!verify_publication_pop_capability(&duplicate, &members, 2));

        let mut tampered = certificate;
        tampered.statement.object_frontier = 13;
        assert!(!verify_publication_pop_capability(&tampered, &members, 2));
    }
}
