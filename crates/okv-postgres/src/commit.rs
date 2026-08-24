//! Subordinate objectKV transaction identity for admitted `PostgreSQL` pages.

use crate::{PostgresPageWriteAdmission, PostgresRelationExtent, PostgresRelationForkIdentity};
use okv_consensus::{
    cell_partitioned_transaction_sha256, ApplyResponse, CellKeyRange, CellMutation,
    CellReadVersion, CellTransactionCommand, CellTransactionStatus, RequestIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};

/// Relation-extent semantics attached to one admitted page batch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPageCommitOperation {
    /// Replace pages strictly inside the unchanged current extent.
    WriteExisting,
    /// Append one consecutive batch beginning at the prior extent.
    Extend,
}

/// Cell identity and routing metadata for one subordinate page transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresPageCommitContext {
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub accepted_resolvers: Vec<u16>,
    pub durable_log_tags: Vec<u16>,
}

/// Exact transaction command prepared from one admitted page batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresPageCommitPlan {
    pub operation: PostgresPageCommitOperation,
    pub relation: PostgresRelationForkIdentity,
    pub previous_nblocks: u32,
    pub resulting_nblocks: u32,
    pub maximum_page_lsn: u64,
    pub admission_mutation_sha256: [u8; 32],
    pub command_sha256: [u8; 32],
    pub command: CellTransactionCommand,
}

/// Verified subordinate commit identity returned by the Cell transaction path.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresPageCommitReceipt {
    pub transaction_identity: RequestIdentity,
    pub generation: u64,
    pub previous_objectkv_version: u64,
    pub committed_objectkv_version: u64,
    pub previous_nblocks: u32,
    pub resulting_nblocks: u32,
    pub maximum_page_lsn: u64,
    pub admission_mutation_sha256: [u8; 32],
    pub command_sha256: [u8; 32],
    pub committed_envelope_sha256: [u8; 32],
}

/// Page-commit planning or receipt-verification refusal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PostgresPageCommitError {
    InvalidContext,
    BlockRangeOverflow,
    PagesBeyondExtent {
        exclusive_end: u32,
        nblocks: u32,
    },
    ExtentChangedForExistingWrite {
        previous: u32,
        resulting: u32,
    },
    NonContiguousExtend {
        first_block: u32,
        page_count: usize,
        previous: u32,
        resulting: u32,
    },
    CommandEncoding(String),
    ResponseIdentityMismatch,
    AuthorityRefused(String),
    MissingTransactionOutcome,
    TransactionNotCommitted(CellTransactionStatus),
    GenerationMismatch {
        expected: u64,
        observed: u64,
    },
    MissingCommitVersion,
    NonAdvancingCommitVersion {
        read_version: u64,
        commit_version: u64,
    },
    MissingCommittedEnvelope,
}

impl Display for PostgresPageCommitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for PostgresPageCommitError {}

/// Bind admitted pages and authoritative relation extent into one Cell command.
///
/// Every command reads and writes the extent key. Existing writes keep the
/// extent unchanged. Extend begins exactly at the previous extent and publishes
/// the new count in the same transaction as its pages.
///
/// # Errors
///
/// Returns a typed refusal for invalid Cell metadata, extent mismatch, range
/// overflow, or canonical command encoding failure.
pub fn plan_postgres_page_commit(
    admission: &PostgresPageWriteAdmission,
    operation: PostgresPageCommitOperation,
    previous_nblocks: u32,
    resulting_nblocks: u32,
    context: &PostgresPageCommitContext,
) -> Result<PostgresPageCommitPlan, PostgresPageCommitError> {
    if context.generation == 0
        || context.accepted_resolvers.is_empty()
        || context.durable_log_tags.is_empty()
    {
        return Err(PostgresPageCommitError::InvalidContext);
    }
    let page_count = u32::try_from(admission.page_count)
        .map_err(|_| PostgresPageCommitError::BlockRangeOverflow)?;
    let exclusive_end = admission
        .first_block
        .checked_add(page_count)
        .ok_or(PostgresPageCommitError::BlockRangeOverflow)?;
    match operation {
        PostgresPageCommitOperation::WriteExisting => {
            if previous_nblocks != resulting_nblocks {
                return Err(PostgresPageCommitError::ExtentChangedForExistingWrite {
                    previous: previous_nblocks,
                    resulting: resulting_nblocks,
                });
            }
            if exclusive_end > previous_nblocks {
                return Err(PostgresPageCommitError::PagesBeyondExtent {
                    exclusive_end,
                    nblocks: previous_nblocks,
                });
            }
        }
        PostgresPageCommitOperation::Extend => {
            if admission.first_block != previous_nblocks
                || exclusive_end != resulting_nblocks
                || resulting_nblocks <= previous_nblocks
            {
                return Err(PostgresPageCommitError::NonContiguousExtend {
                    first_block: admission.first_block,
                    page_count: admission.page_count,
                    previous: previous_nblocks,
                    resulting: resulting_nblocks,
                });
            }
        }
    }

    let extent_key = admission.relation.encode_extent_key();
    let mut mutations = admission.mutations.clone();
    mutations.push(CellMutation::Set {
        key: extent_key.clone(),
        value: PostgresRelationExtent {
            nblocks: resulting_nblocks,
        }
        .encode(),
    });
    let mut write_conflicts = mutations
        .iter()
        .map(|mutation| match mutation {
            CellMutation::Clear { key } | CellMutation::Set { key, .. } => CellKeyRange::point(key),
        })
        .collect::<Vec<_>>();
    write_conflicts.sort();
    write_conflicts.dedup();
    let command = CellTransactionCommand {
        identity: request_identity(admission.request_id),
        credential: None,
        cell_id: context.cell_id,
        tenant_id: context.tenant_id,
        generation: context.generation,
        read_version: CellReadVersion {
            generation: context.generation,
            sequence: admission.expected_objectkv_version,
        },
        read_conflicts: vec![CellKeyRange::point(&extent_key)],
        write_conflicts,
        mutations,
        partitioned_resolution: None,
        accepted_resolvers: context.accepted_resolvers.clone(),
        durable_log_tags: context.durable_log_tags.clone(),
    };
    let command_sha256 = cell_partitioned_transaction_sha256(&command)
        .map_err(PostgresPageCommitError::CommandEncoding)?;
    Ok(PostgresPageCommitPlan {
        operation,
        relation: admission.relation,
        previous_nblocks,
        resulting_nblocks,
        maximum_page_lsn: admission.maximum_page_lsn,
        admission_mutation_sha256: admission.mutation_sha256,
        command_sha256,
        command,
    })
}

/// Verify the replicated Cell response and produce one bridge-level receipt.
///
/// # Errors
///
/// Returns a typed refusal when identity, generation, commit version, status, or
/// committed envelope does not match the planned subordinate transaction.
pub fn verify_postgres_page_commit(
    plan: &PostgresPageCommitPlan,
    response: &ApplyResponse,
) -> Result<PostgresPageCommitReceipt, PostgresPageCommitError> {
    if response.identity != Some(plan.command.identity) {
        return Err(PostgresPageCommitError::ResponseIdentityMismatch);
    }
    if let Some(error) = response.error {
        return Err(PostgresPageCommitError::AuthorityRefused(format!(
            "{error:?}"
        )));
    }
    let outcome = response
        .cell_transaction
        .as_ref()
        .ok_or(PostgresPageCommitError::MissingTransactionOutcome)?;
    if outcome.status != CellTransactionStatus::Committed {
        return Err(PostgresPageCommitError::TransactionNotCommitted(
            outcome.status,
        ));
    }
    if outcome.generation != plan.command.generation {
        return Err(PostgresPageCommitError::GenerationMismatch {
            expected: plan.command.generation,
            observed: outcome.generation,
        });
    }
    let committed_objectkv_version = outcome
        .commit_sequence
        .ok_or(PostgresPageCommitError::MissingCommitVersion)?;
    if committed_objectkv_version <= plan.command.read_version.sequence {
        return Err(PostgresPageCommitError::NonAdvancingCommitVersion {
            read_version: plan.command.read_version.sequence,
            commit_version: committed_objectkv_version,
        });
    }
    let envelope = outcome
        .envelope
        .as_ref()
        .ok_or(PostgresPageCommitError::MissingCommittedEnvelope)?;
    Ok(PostgresPageCommitReceipt {
        transaction_identity: plan.command.identity,
        generation: outcome.generation,
        previous_objectkv_version: plan.command.read_version.sequence,
        committed_objectkv_version,
        previous_nblocks: plan.previous_nblocks,
        resulting_nblocks: plan.resulting_nblocks,
        maximum_page_lsn: plan.maximum_page_lsn,
        admission_mutation_sha256: plan.admission_mutation_sha256,
        command_sha256: plan.command_sha256,
        committed_envelope_sha256: Sha256::digest(envelope).into(),
    })
}

fn request_identity(request_id: [u8; 16]) -> RequestIdentity {
    let mut client_id = [0_u8; 8];
    let mut sequence = [0_u8; 8];
    client_id.copy_from_slice(&request_id[..8]);
    sequence.copy_from_slice(&request_id[8..]);
    RequestIdentity {
        client_id: u64::from_be_bytes(client_id),
        request_id: u64::from_be_bytes(sequence),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        admit_postgres_page_write, PostgresPage, PostgresPageIdentity, PostgresPageWriteBatch,
        POSTGRES_PAGE_SIZE,
    };
    use okv_consensus::CellTransactionApplyResponse;

    #[test]
    fn plans_atomic_page_and_extent_extend() {
        let admission = admission(7);
        let plan = plan_postgres_page_commit(
            &admission,
            PostgresPageCommitOperation::Extend,
            7,
            9,
            &context(),
        )
        .unwrap();
        assert_eq!(plan.command.read_version.sequence, 41);
        assert_eq!(plan.command.mutations.len(), 3);
        assert_eq!(plan.command.read_conflicts.len(), 1);
        assert_eq!(plan.command.write_conflicts.len(), 3);
        let extent = plan
            .command
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                CellMutation::Set { key, value }
                    if key == &admission.relation.encode_extent_key() =>
                {
                    Some(PostgresRelationExtent::decode(value).unwrap())
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(extent.nblocks, 9);
        assert_ne!(plan.command_sha256, [0; 32]);
    }

    #[test]
    fn refuses_extent_change_or_page_resurrection_shape() {
        let admission = admission(7);
        assert!(matches!(
            plan_postgres_page_commit(
                &admission,
                PostgresPageCommitOperation::WriteExisting,
                8,
                8,
                &context(),
            ),
            Err(PostgresPageCommitError::PagesBeyondExtent { .. })
        ));
        assert!(matches!(
            plan_postgres_page_commit(
                &admission,
                PostgresPageCommitOperation::Extend,
                6,
                9,
                &context(),
            ),
            Err(PostgresPageCommitError::NonContiguousExtend { .. })
        ));
    }

    #[test]
    fn verifies_exact_advancing_cell_receipt() {
        let plan = plan_postgres_page_commit(
            &admission(7),
            PostgresPageCommitOperation::Extend,
            7,
            9,
            &context(),
        )
        .unwrap();
        let response = ApplyResponse {
            identity: Some(plan.command.identity),
            cell_transaction: Some(CellTransactionApplyResponse {
                status: CellTransactionStatus::Committed,
                generation: 3,
                commit_sequence: Some(42),
                envelope: Some(vec![1, 2, 3]),
                row_count: 3,
            }),
            ..ApplyResponse::default()
        };
        let receipt = verify_postgres_page_commit(&plan, &response).unwrap();
        assert_eq!(receipt.previous_objectkv_version, 41);
        assert_eq!(receipt.committed_objectkv_version, 42);
        assert_eq!(receipt.maximum_page_lsn, 900);
        assert_ne!(receipt.committed_envelope_sha256, [0; 32]);
    }

    fn admission(first_block: u32) -> PostgresPageWriteAdmission {
        admit_postgres_page_write(&PostgresPageWriteBatch {
            first: PostgresPageIdentity {
                cluster_id: [0x71; 16],
                tablespace_oid: 1663,
                database_oid: 5,
                relation_number: 16_402,
                temporary_backend_id: 0,
                fork_number: 0,
                block_number: first_block,
            },
            expected_objectkv_version: 41,
            postgres_wal_flush_lsn: 900,
            request_id: [0x81; 16],
            pages: vec![page(899), page(900)],
        })
        .unwrap()
    }

    fn page(page_lsn: u64) -> PostgresPage {
        PostgresPage::new(page_lsn, 0, vec![0x31; POSTGRES_PAGE_SIZE]).unwrap()
    }

    fn context() -> PostgresPageCommitContext {
        PostgresPageCommitContext {
            cell_id: [0x11; 16],
            tenant_id: [0x22; 16],
            generation: 3,
            accepted_resolvers: vec![1, 2],
            durable_log_tags: vec![10, 20],
        }
    }
}
