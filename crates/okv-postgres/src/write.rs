//! `PostgreSQL` WAL-before-page admission contract.

use crate::{PostgresPage, PostgresPageIdentity, PostgresRelationForkIdentity};
use okv_consensus::CellMutation;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

const MAXIMUM_PAGES_PER_BATCH: usize = 128;

/// Unsafe subject selected by the page-write admission suite.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPageWriteGateMode {
    Correct,
    WalBehindPage,
    ZeroObjectKvVersion,
    OversizedBatch,
    AcceptWrongDigest,
}

impl PostgresPageWriteGateMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::WalBehindPage => "wal_behind_page",
            Self::ZeroObjectKvVersion => "zero_objectkv_version",
            Self::OversizedBatch => "oversized_batch",
            Self::AcceptWrongDigest => "accept_wrong_digest",
        }
    }
}

/// Stable semantic receipt for one page-write admission history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresPageWriteGateReceipt {
    pub seed: u64,
    pub mode: PostgresPageWriteGateMode,
    pub expected_objectkv_version: u64,
    pub postgres_wal_flush_lsn: u64,
    pub maximum_page_lsn: u64,
    pub admitted_batches: u64,
    pub admitted_mutations: u64,
    pub refusal: Option<String>,
    pub mutation_sha256: Option<String>,
    pub checks: BTreeMap<String, bool>,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// One permanent-relation page batch presented to the objectKV write path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresPageWriteBatch {
    /// First physical block in the consecutive batch.
    pub first: PostgresPageIdentity,
    /// Exact objectKV view against which this mutation was prepared.
    pub expected_objectkv_version: u64,
    /// `PostgreSQL` WAL position known durable before the storage callback.
    pub postgres_wal_flush_lsn: u64,
    /// Caller-stable identity used for retry deduplication downstream.
    pub request_id: [u8; 16],
    /// Consecutive native `PostgreSQL` pages.
    pub pages: Vec<PostgresPage>,
}

/// A validated mutation batch ready for the subordinate objectKV commit path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresPageWriteAdmission {
    pub relation: PostgresRelationForkIdentity,
    pub expected_objectkv_version: u64,
    pub postgres_wal_flush_lsn: u64,
    pub maximum_page_lsn: u64,
    pub first_block: u32,
    pub page_count: usize,
    pub request_id: [u8; 16],
    pub mutation_sha256: [u8; 32],
    pub mutations: Vec<CellMutation>,
}

/// Fail-closed `PostgreSQL` page-write admission error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PostgresPageWriteError {
    ZeroObjectKvVersion,
    EmptyBatch,
    BatchTooLarge {
        maximum: usize,
        actual: usize,
    },
    BlockRangeOverflow,
    WalBehindPage {
        block_number: u32,
        page_lsn: u64,
        wal_flush_lsn: u64,
    },
}

impl Display for PostgresPageWriteError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for PostgresPageWriteError {}

/// Validate WAL-before-page ordering and encode one deterministic objectKV
/// mutation batch.
///
/// This admits a batch to the subordinate objectKV commit path. It does not
/// claim that the mutations are committed, objectified, or checkpoint-stable.
///
/// # Errors
///
/// Returns a typed refusal before producing any mutations when the objectKV
/// view is absent, the batch is empty or oversized, its range overflows, or
/// `PostgreSQL` WAL is not durable through every page LSN.
pub fn admit_postgres_page_write(
    batch: &PostgresPageWriteBatch,
) -> Result<PostgresPageWriteAdmission, PostgresPageWriteError> {
    if batch.expected_objectkv_version == 0 {
        return Err(PostgresPageWriteError::ZeroObjectKvVersion);
    }
    if batch.pages.is_empty() {
        return Err(PostgresPageWriteError::EmptyBatch);
    }
    if batch.pages.len() > MAXIMUM_PAGES_PER_BATCH {
        return Err(PostgresPageWriteError::BatchTooLarge {
            maximum: MAXIMUM_PAGES_PER_BATCH,
            actual: batch.pages.len(),
        });
    }
    let page_count_u32 =
        u32::try_from(batch.pages.len()).map_err(|_| PostgresPageWriteError::BlockRangeOverflow)?;
    batch
        .first
        .block_number
        .checked_add(page_count_u32)
        .ok_or(PostgresPageWriteError::BlockRangeOverflow)?;

    let mut maximum_page_lsn = 0_u64;
    for (offset, page) in batch.pages.iter().enumerate() {
        let offset =
            u32::try_from(offset).map_err(|_| PostgresPageWriteError::BlockRangeOverflow)?;
        let block_number = batch
            .first
            .block_number
            .checked_add(offset)
            .ok_or(PostgresPageWriteError::BlockRangeOverflow)?;
        if page.page_lsn > batch.postgres_wal_flush_lsn {
            return Err(PostgresPageWriteError::WalBehindPage {
                block_number,
                page_lsn: page.page_lsn,
                wal_flush_lsn: batch.postgres_wal_flush_lsn,
            });
        }
        maximum_page_lsn = maximum_page_lsn.max(page.page_lsn);
    }

    let mutations = batch
        .pages
        .iter()
        .enumerate()
        .map(|(offset, page)| {
            let offset =
                u32::try_from(offset).map_err(|_| PostgresPageWriteError::BlockRangeOverflow)?;
            let block_number = batch
                .first
                .block_number
                .checked_add(offset)
                .ok_or(PostgresPageWriteError::BlockRangeOverflow)?;
            Ok(CellMutation::Set {
                key: batch.first.with_block(block_number).encode_key(),
                value: page.encode(),
            })
        })
        .collect::<Result<Vec<_>, PostgresPageWriteError>>()?;
    let mutation_sha256 = mutation_digest(batch, &mutations);

    Ok(PostgresPageWriteAdmission {
        relation: batch.first.relation_fork(),
        expected_objectkv_version: batch.expected_objectkv_version,
        postgres_wal_flush_lsn: batch.postgres_wal_flush_lsn,
        maximum_page_lsn,
        first_block: batch.first.block_number,
        page_count: batch.pages.len(),
        request_id: batch.request_id,
        mutation_sha256,
        mutations,
    })
}

/// Run one deterministic WAL-before-page admission history.
///
/// # Errors
///
/// Returns an error only when the contract fixture itself cannot be encoded.
#[allow(clippy::too_many_lines)]
pub fn run_postgres_page_write_gate_contract(
    seed: u64,
    mode: PostgresPageWriteGateMode,
) -> Result<PostgresPageWriteGateReceipt, String> {
    const OBJECTKV_VERSION: u64 = 41;
    const WAL_FLUSH_LSN: u64 = 900;
    let expected_batch = contract_batch(seed, OBJECTKV_VERSION, WAL_FLUSH_LSN, 2)?;
    let expected = admit_postgres_page_write(&expected_batch).map_err(|error| error.to_string())?;
    let mut subject = match mode {
        PostgresPageWriteGateMode::Correct | PostgresPageWriteGateMode::AcceptWrongDigest => {
            admit_postgres_page_write(&expected_batch)
        }
        PostgresPageWriteGateMode::WalBehindPage => {
            let batch = contract_batch(seed, OBJECTKV_VERSION, WAL_FLUSH_LSN - 1, 2)?;
            admit_postgres_page_write(&batch)
        }
        PostgresPageWriteGateMode::ZeroObjectKvVersion => {
            let batch = contract_batch(seed, 0, WAL_FLUSH_LSN, 2)?;
            admit_postgres_page_write(&batch)
        }
        PostgresPageWriteGateMode::OversizedBatch => {
            let batch = contract_batch(
                seed,
                OBJECTKV_VERSION,
                WAL_FLUSH_LSN,
                MAXIMUM_PAGES_PER_BATCH + 1,
            )?;
            admit_postgres_page_write(&batch)
        }
    };
    if mode == PostgresPageWriteGateMode::AcceptWrongDigest {
        if let Ok(admission) = &mut subject {
            admission.mutation_sha256[0] ^= 0xff;
        }
    }
    let subject_behavior_exact = match mode {
        PostgresPageWriteGateMode::Correct | PostgresPageWriteGateMode::AcceptWrongDigest => {
            subject.is_ok()
        }
        PostgresPageWriteGateMode::WalBehindPage => matches!(
            &subject,
            Err(PostgresPageWriteError::WalBehindPage {
                block_number,
                page_lsn,
                wal_flush_lsn,
            }) if *block_number == 8
                && *page_lsn == WAL_FLUSH_LSN
                && *wal_flush_lsn == WAL_FLUSH_LSN - 1
        ),
        PostgresPageWriteGateMode::ZeroObjectKvVersion => {
            subject == Err(PostgresPageWriteError::ZeroObjectKvVersion)
        }
        PostgresPageWriteGateMode::OversizedBatch => matches!(
            &subject,
            Err(PostgresPageWriteError::BatchTooLarge {
                maximum: MAXIMUM_PAGES_PER_BATCH,
                actual,
            }) if *actual == MAXIMUM_PAGES_PER_BATCH + 1
        ),
    };
    let admitted = subject.as_ref().ok();
    let refusal = subject.as_ref().err().map(ToString::to_string);
    let checks = BTreeMap::from([
        ("admission_succeeded".to_owned(), admitted.is_some()),
        (
            "mutation_batch_exact".to_owned(),
            admitted.is_some_and(|admission| admission.mutations == expected.mutations),
        ),
        (
            "mutation_digest_exact".to_owned(),
            admitted.is_some_and(|admission| admission.mutation_sha256 == expected.mutation_sha256),
        ),
        (
            "objectkv_version_exact".to_owned(),
            admitted
                .is_some_and(|admission| admission.expected_objectkv_version == OBJECTKV_VERSION),
        ),
        ("subject_behavior_exact".to_owned(), subject_behavior_exact),
    ]);
    let failed = checks
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let maximum_page_lsn = admitted.map_or(WAL_FLUSH_LSN, |admission| admission.maximum_page_lsn);
    let admitted_batches = u64::from(admitted.is_some());
    let admitted_mutations = admitted.map_or(0, |admission| {
        u64::try_from(admission.mutations.len()).unwrap_or(u64::MAX)
    });
    let mutation_sha256 = admitted.map(|admission| hex(&admission.mutation_sha256));
    let semantic = (
        seed,
        mode,
        OBJECTKV_VERSION,
        WAL_FLUSH_LSN,
        maximum_page_lsn,
        admitted_batches,
        admitted_mutations,
        &refusal,
        &mutation_sha256,
        &checks,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    Ok(PostgresPageWriteGateReceipt {
        seed,
        mode,
        expected_objectkv_version: OBJECTKV_VERSION,
        postgres_wal_flush_lsn: WAL_FLUSH_LSN,
        maximum_page_lsn,
        admitted_batches,
        admitted_mutations,
        refusal,
        mutation_sha256,
        checks,
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

fn contract_batch(
    seed: u64,
    expected_objectkv_version: u64,
    postgres_wal_flush_lsn: u64,
    page_count: usize,
) -> Result<PostgresPageWriteBatch, String> {
    let first = PostgresPageIdentity {
        cluster_id: [0x71; 16],
        tablespace_oid: 1663,
        database_oid: 5,
        relation_number: 16_402,
        temporary_backend_id: 0,
        fork_number: 0,
        block_number: 7,
    };
    let pages = (0..page_count)
        .map(|offset| {
            let offset_u64 = u64::try_from(offset).unwrap_or(u64::MAX);
            let page_lsn = if offset == 0 {
                899
            } else if offset == 1 {
                900
            } else {
                800_u64.saturating_add(offset_u64.min(99))
            };
            let byte = seed.to_le_bytes()[0].wrapping_add(u8::try_from(offset).unwrap_or(u8::MAX));
            PostgresPage::new(page_lsn, 0, vec![byte; crate::POSTGRES_PAGE_SIZE])
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PostgresPageWriteBatch {
        first,
        expected_objectkv_version,
        postgres_wal_flush_lsn,
        request_id: seed_request_id(seed),
        pages,
    })
}

fn seed_request_id(seed: u64) -> [u8; 16] {
    let digest = Sha256::digest(seed.to_be_bytes());
    let mut request_id = [0_u8; 16];
    request_id.copy_from_slice(&digest[..16]);
    request_id
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn mutation_digest(batch: &PostgresPageWriteBatch, mutations: &[CellMutation]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"objectkv/postgres/page-write-admission/v1");
    digest.update(batch.expected_objectkv_version.to_be_bytes());
    digest.update(batch.postgres_wal_flush_lsn.to_be_bytes());
    digest.update(batch.request_id);
    for mutation in mutations {
        match mutation {
            CellMutation::Set { key, value } => {
                digest.update([1]);
                digest.update(u64::try_from(key.len()).unwrap_or(u64::MAX).to_be_bytes());
                digest.update(key);
                digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
                digest.update(value);
            }
            CellMutation::Clear { key } => {
                digest.update([2]);
                digest.update(u64::try_from(key.len()).unwrap_or(u64::MAX).to_be_bytes());
                digest.update(key);
            }
        }
    }
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::POSTGRES_PAGE_SIZE;

    #[test]
    fn admits_permanent_pages_only_after_wal_is_durable() {
        let batch = batch(900);
        let admitted = admit_postgres_page_write(&batch).unwrap();
        assert_eq!(admitted.expected_objectkv_version, 41);
        assert_eq!(admitted.postgres_wal_flush_lsn, 900);
        assert_eq!(admitted.maximum_page_lsn, 900);
        assert_eq!(admitted.first_block, 7);
        assert_eq!(admitted.page_count, 2);
        assert_eq!(admitted.mutations.len(), 2);
        let CellMutation::Set { key, .. } = &admitted.mutations[0] else {
            panic!("page write must encode a set mutation");
        };
        assert_eq!(key, &batch.first.encode_key());
        assert_ne!(admitted.mutation_sha256, [0; 32]);
        assert_eq!(
            admit_postgres_page_write(&batch).unwrap().mutation_sha256,
            admitted.mutation_sha256
        );
    }

    #[test]
    fn refuses_the_complete_batch_when_one_page_is_ahead_of_wal() {
        assert_eq!(
            admit_postgres_page_write(&batch(899)),
            Err(PostgresPageWriteError::WalBehindPage {
                block_number: 8,
                page_lsn: 900,
                wal_flush_lsn: 899,
            })
        );
    }

    #[test]
    fn refuses_empty_oversized_and_overflowing_batches() {
        let mut empty = batch(900);
        empty.pages.clear();
        assert_eq!(
            admit_postgres_page_write(&empty),
            Err(PostgresPageWriteError::EmptyBatch)
        );

        let mut oversized = batch(900);
        oversized.pages = (0..=MAXIMUM_PAGES_PER_BATCH).map(|_| page(700)).collect();
        assert_eq!(
            admit_postgres_page_write(&oversized),
            Err(PostgresPageWriteError::BatchTooLarge {
                maximum: MAXIMUM_PAGES_PER_BATCH,
                actual: MAXIMUM_PAGES_PER_BATCH + 1,
            })
        );

        let mut overflow = batch(900);
        overflow.first.block_number = u32::MAX;
        assert_eq!(
            admit_postgres_page_write(&overflow),
            Err(PostgresPageWriteError::BlockRangeOverflow)
        );
    }

    #[test]
    fn gate_receipt_is_exact_and_replayable() {
        let first = run_postgres_page_write_gate_contract(1103, PostgresPageWriteGateMode::Correct)
            .unwrap();
        let replay =
            run_postgres_page_write_gate_contract(1103, PostgresPageWriteGateMode::Correct)
                .unwrap();
        assert_eq!(first, replay);
        assert_eq!(first.anomaly_count, 0);
        assert_eq!(first.admitted_batches, 1);
        assert_eq!(first.admitted_mutations, 2);
        assert!(first.checks.values().all(|passed| *passed));
    }

    #[test]
    fn gate_receipt_detects_each_poison_control() {
        for mode in [
            PostgresPageWriteGateMode::WalBehindPage,
            PostgresPageWriteGateMode::ZeroObjectKvVersion,
            PostgresPageWriteGateMode::OversizedBatch,
            PostgresPageWriteGateMode::AcceptWrongDigest,
        ] {
            let receipt = run_postgres_page_write_gate_contract(2207, mode).unwrap();
            assert!(receipt.anomaly_count > 0, "mode {} escaped", mode.id());
            assert!(receipt.first_mismatch.is_some());
        }
    }

    fn batch(wal_flush_lsn: u64) -> PostgresPageWriteBatch {
        PostgresPageWriteBatch {
            first: PostgresPageIdentity {
                cluster_id: [0x71; 16],
                tablespace_oid: 1663,
                database_oid: 5,
                relation_number: 16_402,
                temporary_backend_id: 0,
                fork_number: 0,
                block_number: 7,
            },
            expected_objectkv_version: 41,
            postgres_wal_flush_lsn: wal_flush_lsn,
            request_id: [0x81; 16],
            pages: vec![page(899), page(900)],
        }
    }

    fn page(page_lsn: u64) -> PostgresPage {
        PostgresPage::new(page_lsn, 0, vec![0x31; POSTGRES_PAGE_SIZE]).unwrap()
    }
}
