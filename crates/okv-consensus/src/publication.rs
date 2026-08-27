use crate::{GenerationCredential, RecoveryLogPosition, RequestIdentity};
use serde::{Deserialize, Serialize};

const PUBLICATION_COMMAND_MAGIC: &[u8] = b"OKVP1";

pub use okv_publication::{
    AuthorityContext as PublicationAuthorityContext,
    AuthorityPosition as PublicationAuthorityPosition, DeletePermit, ObjectFrontierAttestation,
    ObjectFrontierCertificate, ObjectFrontierCertificateStatement, ObjectFrontierLogPosition,
    ObjectFrontierRecord, ObjectIdentity, ObjectKind as PublicationObjectKind,
    ObjectReference as PublicationObjectReference, PreparedPublication, PublicationAction,
    PublicationAuthorityFaults, PublicationAuthorityState, PublicationAuthorization,
    PublicationCommandStatus, PublicationIntent, PublicationOutcome, RevisionToken,
    OBJECT_FRONTIER_CERTIFICATE_VERSION,
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
