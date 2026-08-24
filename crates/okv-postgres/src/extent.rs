//! Authoritative `PostgreSQL` relation-fork extent over objectKV.

use crate::{PostgresPageIdentity, PostgresPageReader};
use okv_object::KvReadClientError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};

const EXTENT_KEY_PREFIX: &[u8] = b"\x01okv/pg/extent/v1/";
const EXTENT_VALUE_MAGIC: &[u8; 8] = b"OKVPGX01";
const EXTENT_VALUE_FORMAT_VERSION: u16 = 1;
const EXTENT_VALUE_BYTES: usize = 8 + 2 + 4 + 32;

/// Stable physical identity for one `PostgreSQL` relation fork.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PostgresRelationForkIdentity {
    pub cluster_id: [u8; 16],
    pub tablespace_oid: u32,
    pub database_oid: u32,
    pub relation_number: u32,
    pub temporary_backend_id: u32,
    pub fork_number: u8,
}

impl PostgresRelationForkIdentity {
    /// Encode the authoritative fork-extent key.
    #[must_use]
    pub fn encode_extent_key(self) -> Vec<u8> {
        let mut key = Vec::with_capacity(EXTENT_KEY_PREFIX.len() + 33);
        key.extend_from_slice(EXTENT_KEY_PREFIX);
        key.extend_from_slice(&self.cluster_id);
        key.extend_from_slice(&self.tablespace_oid.to_be_bytes());
        key.extend_from_slice(&self.database_oid.to_be_bytes());
        key.extend_from_slice(&self.relation_number.to_be_bytes());
        key.extend_from_slice(&self.temporary_backend_id.to_be_bytes());
        key.push(self.fork_number);
        key
    }

    /// Select one physical block in this relation fork.
    #[must_use]
    pub const fn page(self, block_number: u32) -> PostgresPageIdentity {
        PostgresPageIdentity {
            cluster_id: self.cluster_id,
            tablespace_oid: self.tablespace_oid,
            database_oid: self.database_oid,
            relation_number: self.relation_number,
            temporary_backend_id: self.temporary_backend_id,
            fork_number: self.fork_number,
            block_number,
        }
    }
}

impl From<PostgresPageIdentity> for PostgresRelationForkIdentity {
    fn from(page: PostgresPageIdentity) -> Self {
        Self {
            cluster_id: page.cluster_id,
            tablespace_oid: page.tablespace_oid,
            database_oid: page.database_oid,
            relation_number: page.relation_number,
            temporary_backend_id: page.temporary_backend_id,
            fork_number: page.fork_number,
        }
    }
}

/// Versioned block count for one relation fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostgresRelationExtent {
    pub nblocks: u32,
}

impl PostgresRelationExtent {
    /// Encode a checksummed extent value.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(EXTENT_VALUE_BYTES);
        encoded.extend_from_slice(EXTENT_VALUE_MAGIC);
        encoded.extend_from_slice(&EXTENT_VALUE_FORMAT_VERSION.to_be_bytes());
        encoded.extend_from_slice(&self.nblocks.to_be_bytes());
        encoded.extend_from_slice(&extent_digest(self.nblocks));
        encoded
    }

    /// Decode and authenticate an extent value.
    ///
    /// # Errors
    ///
    /// Returns a typed refusal for format, version, length, or digest mismatch.
    pub fn decode(encoded: &[u8]) -> Result<Self, PostgresRelationExtentError> {
        if encoded.len() != EXTENT_VALUE_BYTES || &encoded[..8] != EXTENT_VALUE_MAGIC {
            return Err(PostgresRelationExtentError::MalformedValue);
        }
        let format_version = u16::from_be_bytes(
            encoded[8..10]
                .try_into()
                .map_err(|_| PostgresRelationExtentError::MalformedValue)?,
        );
        if format_version != EXTENT_VALUE_FORMAT_VERSION {
            return Err(PostgresRelationExtentError::UnsupportedFormat {
                requested: format_version,
                supported: EXTENT_VALUE_FORMAT_VERSION,
            });
        }
        let nblocks = u32::from_be_bytes(
            encoded[10..14]
                .try_into()
                .map_err(|_| PostgresRelationExtentError::MalformedValue)?,
        );
        if encoded[14..] != extent_digest(nblocks) {
            return Err(PostgresRelationExtentError::DigestMismatch);
        }
        Ok(Self { nblocks })
    }
}

impl PostgresPageReader {
    /// Read authoritative relation-fork block count at one exact objectKV view.
    ///
    /// # Errors
    ///
    /// Returns routing, format, or digest failures without changing the selected
    /// objectKV version.
    pub async fn read_nblocks(
        &self,
        relation: PostgresRelationForkIdentity,
        objectkv_version: u64,
    ) -> Result<Option<PostgresRelationExtent>, PostgresRelationExtentError> {
        self.client
            .point_at(&relation.encode_extent_key(), objectkv_version)
            .await
            .map_err(PostgresRelationExtentError::ObjectKvRead)?
            .map(|encoded| PostgresRelationExtent::decode(&encoded))
            .transpose()
    }
}

/// Relation-extent read or decoding failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PostgresRelationExtentError {
    MalformedValue,
    UnsupportedFormat { requested: u16, supported: u16 },
    DigestMismatch,
    ObjectKvRead(KvReadClientError),
}

impl Display for PostgresRelationExtentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for PostgresRelationExtentError {}

fn extent_digest(nblocks: u32) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"objectkv/postgres/relation-extent/v1");
    digest.update(nblocks.to_be_bytes());
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extent_key_ignores_block_and_sorts_apart_from_pages() {
        let relation = identity();
        assert_eq!(
            PostgresRelationForkIdentity::from(relation.page(7)),
            relation
        );
        assert_eq!(
            relation.encode_extent_key(),
            PostgresRelationForkIdentity::from(relation.page(99)).encode_extent_key()
        );
        assert_ne!(relation.encode_extent_key(), relation.page(0).encode_key());
    }

    #[test]
    fn extent_round_trips_and_refuses_corruption() {
        let extent = PostgresRelationExtent { nblocks: 148 };
        let encoded = extent.encode();
        assert_eq!(PostgresRelationExtent::decode(&encoded).unwrap(), extent);

        let mut corrupt = encoded;
        corrupt[14] ^= 0xff;
        assert_eq!(
            PostgresRelationExtent::decode(&corrupt),
            Err(PostgresRelationExtentError::DigestMismatch)
        );
    }

    fn identity() -> PostgresRelationForkIdentity {
        PostgresRelationForkIdentity {
            cluster_id: [0x71; 16],
            tablespace_oid: 1663,
            database_oid: 5,
            relation_number: 16_402,
            temporary_backend_id: 0,
            fork_number: 0,
        }
    }
}
