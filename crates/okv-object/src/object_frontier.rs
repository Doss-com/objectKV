use crate::{validate_full_row_object, ObjectClient, RowObjectManifestV1, RowSegmentIndex};
use okv_consensus::{
    GenerationCredential, ObjectFrontierAdvance, ObjectFrontierApplyResponse, ObjectFrontierRecord,
    RequestIdentity, TransactionLogClient,
};

/// Opaque proof that one immutable row-object closure was read and fully
/// validated before txLog reclamation.
#[derive(Clone, Debug)]
pub struct ValidatedRowObjectFrontier {
    advance: ObjectFrontierAdvance,
    manifest: RowObjectManifestV1,
    closure_objects: u64,
    closure_bytes: u64,
}

impl ValidatedRowObjectFrontier {
    #[must_use]
    pub const fn record(&self) -> &ObjectFrontierRecord {
        &self.advance.frontier
    }

    #[must_use]
    pub const fn manifest(&self) -> &RowObjectManifestV1 {
        &self.manifest
    }

    #[must_use]
    pub const fn closure_objects(&self) -> u64 {
        self.closure_objects
    }

    #[must_use]
    pub const fn closure_bytes(&self) -> u64 {
        self.closure_bytes
    }
}

/// Read and validate the exact manifest, every sparse index, and every row-data
/// block named by one pending publication frontier.
///
/// # Errors
///
/// Returns an error for identity, envelope, generation, coverage, index, data,
/// block, or arithmetic violations.
pub async fn validate_row_object_frontier(
    client: &ObjectClient,
    frontier: &ObjectFrontierRecord,
) -> Result<ValidatedRowObjectFrontier, String> {
    if !frontier.is_valid() {
        return Err("object frontier record is invalid".to_owned());
    }
    let (manifest_bytes, _) = client
        .read_full_verified(
            &frontier.manifest.key,
            None,
            frontier.manifest.length,
            &frontier.manifest.sha256,
        )
        .await
        .map_err(|error| error.to_string())?;
    let manifest = RowObjectManifestV1::decode(&manifest_bytes)?;
    if manifest.generation != frontier.owner_generation
        || manifest.covered_through != frontier.covered_through
    {
        return Err(
            "row manifest generation or coverage differs from the pending frontier".to_owned(),
        );
    }

    let mut closure_objects = 1_u64;
    let mut closure_bytes = frontier.manifest.length;
    for reference in &manifest.segments {
        let (index_bytes, _) = client
            .read_full_verified(
                &reference.index_key,
                None,
                reference.index_bytes,
                &reference.index_sha256,
            )
            .await
            .map_err(|error| error.to_string())?;
        let index = RowSegmentIndex::decode(&index_bytes)?;
        reference.validate_index(&index_bytes, &index)?;
        let (data_bytes, _) = client
            .read_full_verified(
                &reference.data_key,
                None,
                reference.data_bytes,
                &reference.data_sha256,
            )
            .await
            .map_err(|error| error.to_string())?;
        validate_full_row_object(&data_bytes, &index)?;
        closure_objects = closure_objects.saturating_add(2);
        closure_bytes = closure_bytes
            .saturating_add(reference.index_bytes)
            .saturating_add(reference.data_bytes);
    }
    Ok(ValidatedRowObjectFrontier {
        advance: ObjectFrontierAdvance {
            frontier: frontier.clone(),
        },
        manifest,
        closure_objects,
        closure_bytes,
    })
}

/// Apply one previously validated row-object frontier to the fenced data
/// authority.
///
/// # Errors
///
/// Returns an error when publication authorization, generation fencing, or
/// replicated safe-pop apply fails.
pub async fn advance_validated_row_object_frontier(
    transaction_log: &TransactionLogClient,
    identity: RequestIdentity,
    credential: &GenerationCredential,
    validated: &ValidatedRowObjectFrontier,
) -> Result<ObjectFrontierApplyResponse, String> {
    transaction_log
        .advance_object_frontier(identity, credential, &validated.advance)
        .await
}
