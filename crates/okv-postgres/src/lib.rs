//! Read-only `PostgreSQL` relation-page mapping over the objectKV direct-read client.
//!
//! `PostgreSQL` WAL, LSNs, tuple MVCC, and checkpoints remain authoritative. The
//! objectKV read version selects one immutable physical-page view and is never
//! presented as a `PostgreSQL` LSN.

use okv_object::{KvReadClient, KvReadClientError};
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

mod commit;
mod commit_process;
mod extent;
mod process;
mod smgr_durable;
mod smgr_service;
mod smgr_stable;
mod smgr_transaction_authority;
mod smgr_write_service;
mod write;

pub use commit::{
    plan_postgres_page_commit, verify_postgres_page_commit, PostgresPageCommitContext,
    PostgresPageCommitError, PostgresPageCommitOperation, PostgresPageCommitPlan,
    PostgresPageCommitReceipt,
};
pub use commit_process::{
    run_postgres_page_commit_process_contract, PostgresPageCommitProcessMode,
    PostgresPageCommitProcessReceipt,
};
pub use extent::{
    PostgresRelationExtent, PostgresRelationExtentError, PostgresRelationForkIdentity,
};
pub use process::{
    run_postgres_page_read_process_contract, run_postgres_page_read_process_worker,
    PostgresPageReadProcessConfig, PostgresPageReadProcessMode, PostgresPageReadProcessReceipt,
};
pub use smgr_durable::{
    prepare_postgres_worker_readiness_fixture, run_postgres_object_delta_contract,
    run_postgres_object_delta_contract_with_config, run_postgres_worker_readiness_process,
    PostgresObjectDeltaContractConfig, PostgresObjectDeltaMode, PostgresObjectDeltaReport,
    PostgresWorkerReadinessConfig, PostgresWorkerReadinessMode, PostgresWorkerReadinessReceipt,
};
pub use smgr_service::{
    run_postgres_smgr_page_service, PostgresSmgrPageServiceConfig, PostgresSmgrPageServiceReady,
};
pub use smgr_stable::{
    run_postgres_stable_authority, PostgresPublicationAuthorityConfig,
    PostgresPublicationPopConfig, PostgresStableAuthorityConfig, PostgresStableAuthorityStatus,
};
pub use smgr_transaction_authority::{
    run_postgres_transaction_authority, PostgresTransactionAuthorityConfig,
    PostgresTransactionAuthorityHarnessConfig, PostgresTransactionAuthorityStatus,
};
pub use smgr_write_service::{
    run_postgres_smgr_write_service, PostgresSmgrWriteServiceConfig, PostgresSmgrWriteServiceStatus,
};
pub use write::{
    admit_postgres_page_write, run_postgres_page_write_gate_contract, PostgresPageWriteAdmission,
    PostgresPageWriteBatch, PostgresPageWriteError, PostgresPageWriteGateMode,
    PostgresPageWriteGateReceipt,
};

pub const POSTGRES_PAGE_SIZE: usize = 8 * 1024;
const PAGE_KEY_PREFIX: &[u8] = b"\x01okv/pg/page/v1/";
const PAGE_VALUE_MAGIC: &[u8; 8] = b"OKVPGP01";
const PAGE_VALUE_FORMAT_VERSION: u16 = 1;
const PAGE_VALUE_HEADER_BYTES: usize = 8 + 2 + 8 + 2 + 4 + 32;

/// Stable physical identity corresponding to one `PostgreSQL` relation block.
///
/// `temporary_backend_id` is zero for permanent and unlogged relations. The
/// fork number uses `PostgreSQL`'s physical fork numbering, but this contract
/// deliberately does not interpret individual fork meanings.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PostgresPageIdentity {
    pub cluster_id: [u8; 16],
    pub tablespace_oid: u32,
    pub database_oid: u32,
    pub relation_number: u32,
    pub temporary_backend_id: u32,
    pub fork_number: u8,
    pub block_number: u32,
}

impl PostgresPageIdentity {
    /// Encode a fixed-width ordered objectKV key.
    ///
    /// Big-endian numeric fields preserve `PostgreSQL` block order within one
    /// cluster, relation, backend, and fork prefix.
    #[must_use]
    pub fn encode_key(self) -> Vec<u8> {
        let mut key = Vec::with_capacity(PAGE_KEY_PREFIX.len() + 37);
        key.extend_from_slice(PAGE_KEY_PREFIX);
        key.extend_from_slice(&self.cluster_id);
        key.extend_from_slice(&self.tablespace_oid.to_be_bytes());
        key.extend_from_slice(&self.database_oid.to_be_bytes());
        key.extend_from_slice(&self.relation_number.to_be_bytes());
        key.extend_from_slice(&self.temporary_backend_id.to_be_bytes());
        key.push(self.fork_number);
        key.extend_from_slice(&self.block_number.to_be_bytes());
        key
    }

    /// Select the relation fork containing this block.
    #[must_use]
    pub const fn relation_fork(self) -> PostgresRelationForkIdentity {
        PostgresRelationForkIdentity {
            cluster_id: self.cluster_id,
            tablespace_oid: self.tablespace_oid,
            database_oid: self.database_oid,
            relation_number: self.relation_number,
            temporary_backend_id: self.temporary_backend_id,
            fork_number: self.fork_number,
        }
    }

    fn with_block(self, block_number: u32) -> Self {
        Self {
            block_number,
            ..self
        }
    }
}

/// One `PostgreSQL` page plus the physical metadata needed at the bridge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresPage {
    pub page_lsn: u64,
    pub postgres_checksum: u16,
    pub bytes: Vec<u8>,
}

impl PostgresPage {
    /// Create one exact 8 KiB page.
    ///
    /// # Errors
    ///
    /// Returns an error when the supplied physical page is not exactly 8 KiB.
    pub fn new(
        page_lsn: u64,
        postgres_checksum: u16,
        bytes: Vec<u8>,
    ) -> Result<Self, PostgresPageBridgeError> {
        if bytes.len() != POSTGRES_PAGE_SIZE {
            return Err(PostgresPageBridgeError::InvalidPageLength {
                expected: POSTGRES_PAGE_SIZE,
                actual: bytes.len(),
            });
        }
        Ok(Self {
            page_lsn,
            postgres_checksum,
            bytes,
        })
    }

    /// Encode a versioned, checksummed bridge value.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(PAGE_VALUE_HEADER_BYTES + self.bytes.len());
        encoded.extend_from_slice(PAGE_VALUE_MAGIC);
        encoded.extend_from_slice(&PAGE_VALUE_FORMAT_VERSION.to_be_bytes());
        encoded.extend_from_slice(&self.page_lsn.to_be_bytes());
        encoded.extend_from_slice(&self.postgres_checksum.to_be_bytes());
        encoded.extend_from_slice(
            &u32::try_from(self.bytes.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        encoded.extend_from_slice(&Sha256::digest(&self.bytes));
        encoded.extend_from_slice(&self.bytes);
        encoded
    }

    /// Decode and authenticate one physical-page value.
    ///
    /// # Errors
    ///
    /// Returns a typed refusal for format, length, or payload corruption.
    pub fn decode(encoded: &[u8]) -> Result<Self, PostgresPageBridgeError> {
        if encoded.len() < PAGE_VALUE_HEADER_BYTES {
            return Err(PostgresPageBridgeError::MalformedPageValue);
        }
        if &encoded[..8] != PAGE_VALUE_MAGIC {
            return Err(PostgresPageBridgeError::MalformedPageValue);
        }
        let format_version = read_u16(encoded, 8)?;
        if format_version != PAGE_VALUE_FORMAT_VERSION {
            return Err(PostgresPageBridgeError::UnsupportedPageFormat {
                requested: format_version,
                supported: PAGE_VALUE_FORMAT_VERSION,
            });
        }
        let page_lsn = read_u64(encoded, 10)?;
        let postgres_checksum = read_u16(encoded, 18)?;
        let payload_length = usize::try_from(read_u32(encoded, 20)?)
            .map_err(|_| PostgresPageBridgeError::MalformedPageValue)?;
        if payload_length != POSTGRES_PAGE_SIZE
            || encoded.len() != PAGE_VALUE_HEADER_BYTES.saturating_add(payload_length)
        {
            return Err(PostgresPageBridgeError::InvalidPageLength {
                expected: POSTGRES_PAGE_SIZE,
                actual: payload_length,
            });
        }
        let expected_sha256 = encoded
            .get(24..56)
            .ok_or(PostgresPageBridgeError::MalformedPageValue)?;
        let bytes = encoded[PAGE_VALUE_HEADER_BYTES..].to_vec();
        if expected_sha256 != Sha256::digest(&bytes).as_slice() {
            return Err(PostgresPageBridgeError::PagePayloadChecksumMismatch);
        }
        Ok(Self {
            page_lsn,
            postgres_checksum,
            bytes,
        })
    }
}

/// The two independent clocks needed for a physical `PostgreSQL` page read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostgresPageReadSnapshot {
    /// Exact objectKV physical view selected by the bridge root.
    pub objectkv_version: u64,
    /// Highest `PostgreSQL` page LSN admitted by the selected bridge root.
    pub maximum_page_lsn: u64,
}

/// Read-only relation-page adapter above the stable objectKV client boundary.
pub struct PostgresPageReader {
    client: Arc<KvReadClient>,
}

impl PostgresPageReader {
    #[must_use]
    pub fn new(client: Arc<KvReadClient>) -> Self {
        Self { client }
    }

    /// Read one page at one exact objectKV view.
    ///
    /// # Errors
    ///
    /// Returns routing, format, checksum, or LSN-frontier failures.
    pub async fn read_page(
        &self,
        identity: PostgresPageIdentity,
        snapshot: PostgresPageReadSnapshot,
    ) -> Result<Option<PostgresPage>, PostgresPageBridgeError> {
        let value = self
            .client
            .point_at(&identity.encode_key(), snapshot.objectkv_version)
            .await
            .map_err(PostgresPageBridgeError::ObjectKvRead)?;
        value
            .map(|encoded| decode_at_snapshot(&encoded, snapshot))
            .transpose()
    }

    /// Read consecutive blocks through one ordered range at an unchanged
    /// objectKV version. Missing or extra blocks are refused.
    ///
    /// # Errors
    ///
    /// Returns routing, range, format, checksum, missing-page, or LSN failures.
    pub async fn read_pages(
        &self,
        first: PostgresPageIdentity,
        block_count: usize,
        snapshot: PostgresPageReadSnapshot,
    ) -> Result<Vec<PostgresPage>, PostgresPageBridgeError> {
        if block_count == 0 {
            return Ok(Vec::new());
        }
        let block_count_u32 =
            u32::try_from(block_count).map_err(|_| PostgresPageBridgeError::BlockRangeOverflow)?;
        let exclusive_block = first
            .block_number
            .checked_add(block_count_u32)
            .ok_or(PostgresPageBridgeError::BlockRangeOverflow)?;
        let start = first.encode_key();
        let end = first.with_block(exclusive_block).encode_key();
        let rows = self
            .client
            .scan_at(&start, &end, snapshot.objectkv_version, block_count)
            .await
            .map_err(PostgresPageBridgeError::ObjectKvRead)?;
        decode_page_rows(first, block_count, snapshot, &rows)
    }
}

/// Physical-page adapter failure. None of these errors changes `PostgreSQL`
/// transaction outcome or advances a checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PostgresPageBridgeError {
    InvalidPageLength { expected: usize, actual: usize },
    UnsupportedPageFormat { requested: u16, supported: u16 },
    MalformedPageValue,
    PagePayloadChecksumMismatch,
    PageLsnBeyondSnapshot { page_lsn: u64, maximum: u64 },
    BlockRangeOverflow,
    MissingPage { block_number: u32 },
    UnexpectedPageKey { block_number: u32 },
    ObjectKvRead(KvReadClientError),
}

impl Display for PostgresPageBridgeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for PostgresPageBridgeError {}

fn decode_at_snapshot(
    encoded: &[u8],
    snapshot: PostgresPageReadSnapshot,
) -> Result<PostgresPage, PostgresPageBridgeError> {
    let page = PostgresPage::decode(encoded)?;
    if page.page_lsn > snapshot.maximum_page_lsn {
        return Err(PostgresPageBridgeError::PageLsnBeyondSnapshot {
            page_lsn: page.page_lsn,
            maximum: snapshot.maximum_page_lsn,
        });
    }
    Ok(page)
}

fn decode_page_rows(
    first: PostgresPageIdentity,
    block_count: usize,
    snapshot: PostgresPageReadSnapshot,
    rows: &[(Vec<u8>, Vec<u8>)],
) -> Result<Vec<PostgresPage>, PostgresPageBridgeError> {
    let mut pages = Vec::with_capacity(block_count);
    for offset in 0..block_count {
        let offset_u32 =
            u32::try_from(offset).map_err(|_| PostgresPageBridgeError::BlockRangeOverflow)?;
        let block_number = first
            .block_number
            .checked_add(offset_u32)
            .ok_or(PostgresPageBridgeError::BlockRangeOverflow)?;
        let expected_key = first.with_block(block_number).encode_key();
        let Some((key, value)) = rows.get(offset) else {
            return Err(PostgresPageBridgeError::MissingPage { block_number });
        };
        if key != &expected_key {
            if key > &expected_key {
                return Err(PostgresPageBridgeError::MissingPage { block_number });
            }
            return Err(PostgresPageBridgeError::UnexpectedPageKey { block_number });
        }
        pages.push(decode_at_snapshot(value, snapshot)?);
    }
    if rows.len() != block_count {
        let offset =
            u32::try_from(block_count).map_err(|_| PostgresPageBridgeError::BlockRangeOverflow)?;
        let block_number = first
            .block_number
            .checked_add(offset)
            .ok_or(PostgresPageBridgeError::BlockRangeOverflow)?;
        return Err(PostgresPageBridgeError::UnexpectedPageKey { block_number });
    }
    Ok(pages)
}

fn read_u16(encoded: &[u8], offset: usize) -> Result<u16, PostgresPageBridgeError> {
    let bytes = encoded
        .get(offset..offset.saturating_add(2))
        .ok_or(PostgresPageBridgeError::MalformedPageValue)?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32(encoded: &[u8], offset: usize) -> Result<u32, PostgresPageBridgeError> {
    let bytes = encoded
        .get(offset..offset.saturating_add(4))
        .ok_or(PostgresPageBridgeError::MalformedPageValue)?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u64(encoded: &[u8], offset: usize) -> Result<u64, PostgresPageBridgeError> {
    let bytes = encoded
        .get(offset..offset.saturating_add(8))
        .ok_or(PostgresPageBridgeError::MalformedPageValue)?;
    Ok(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_keys_preserve_block_order_and_relation_boundaries() {
        let first = identity(41, 7);
        let second = identity(41, 8);
        let other_relation = identity(42, 0);
        assert!(first.encode_key() < second.encode_key());
        assert!(second.encode_key() < other_relation.encode_key());
        assert_eq!(first.encode_key().len(), PAGE_KEY_PREFIX.len() + 37);
    }

    #[test]
    fn page_value_round_trips_and_refuses_payload_corruption() {
        let page = page(900, 0x31);
        let encoded = page.encode();
        assert_eq!(PostgresPage::decode(&encoded).unwrap(), page);

        let mut corrupted = encoded;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        assert_eq!(
            PostgresPage::decode(&corrupted),
            Err(PostgresPageBridgeError::PagePayloadChecksumMismatch)
        );
    }

    #[test]
    fn vectored_decode_requires_every_exact_block_at_the_lsn_frontier() {
        let first = identity(41, 7);
        let snapshot = PostgresPageReadSnapshot {
            objectkv_version: 12,
            maximum_page_lsn: 901,
        };
        let rows = vec![
            (first.encode_key(), page(900, 0x11).encode()),
            (first.with_block(8).encode_key(), page(901, 0x22).encode()),
        ];
        let decoded = decode_page_rows(first, 2, snapshot, &rows).unwrap();
        assert_eq!(decoded.len(), 2);

        assert_eq!(
            decode_page_rows(first, 3, snapshot, &rows),
            Err(PostgresPageBridgeError::MissingPage { block_number: 9 })
        );
        let future_rows = vec![(first.encode_key(), page(902, 0x33).encode())];
        assert_eq!(
            decode_page_rows(first, 1, snapshot, &future_rows),
            Err(PostgresPageBridgeError::PageLsnBeyondSnapshot {
                page_lsn: 902,
                maximum: 901,
            })
        );
    }

    fn identity(relation_number: u32, block_number: u32) -> PostgresPageIdentity {
        PostgresPageIdentity {
            cluster_id: [0x51; 16],
            tablespace_oid: 1663,
            database_oid: 5,
            relation_number,
            temporary_backend_id: 0,
            fork_number: 0,
            block_number,
        }
    }

    fn page(page_lsn: u64, byte: u8) -> PostgresPage {
        PostgresPage::new(page_lsn, 77, vec![byte; POSTGRES_PAGE_SIZE]).unwrap()
    }
}
