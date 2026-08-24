//! Authority-pinned immutable base plus authenticated txLog overlay.

use object_store::ObjectStore;
use okv_consensus::{
    verify_tagged_log_envelope_certificate, CellLogSetPolicy, CellMutation,
    CellTaggedLogCertificate, PublicationAuthorityState, PublicationObjectReference,
    SnapshotLeaseToken, SnapshotLeaseValidationError,
};
use okv_model::{Row, Version};
use okv_sim::CommitEnvelope;
use okv_slate::{AdapterError, AuthorityBoundSlateReader, AuthorityManifestReference};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::db_cache::DbCache;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter, Write as _};
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Exact immutable-base identity and commit-chain frontier selected by the
/// replicated publication authority for one Range Engine assignment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorityRangeRoot {
    pub cell_id: [u8; 16],
    pub tenant_id: [u8; 16],
    pub generation: u64,
    pub manifest: AuthorityManifestReference,
    pub covered_through: u64,
    pub minimum_readable_version: u64,
    pub log_chain_sha256: [u8; 32],
}

/// One exact committed txLog envelope plus the quorum certificates for every
/// required log set named by that envelope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CertifiedTxLogRecord {
    pub envelope: Vec<u8>,
    pub certificates: Vec<CellTaggedLogCertificate>,
}

/// Exact process-local identity of one fully authenticated serving view.
///
/// The immutable manifest alone is insufficient because several successive
/// txLog frontiers can legitimately share the same object base. Publication
/// compares this complete token so a stale controller cannot overwrite a
/// newer tail view through a same-manifest ABA transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeServingViewToken {
    pub root: AuthorityRangeRoot,
    pub target_version: u64,
    pub final_log_chain_sha256: [u8; 32],
}

/// A bounded failure at the immutable-base plus txLog serving boundary.
#[derive(Debug, Eq, PartialEq)]
pub enum RangeServingViewError {
    Base(AdapterError),
    BaseFrontierMismatch { expected: u64, observed: u64 },
    CertificateCoverageMismatch { sequence: u64 },
    Envelope(String),
    IdentityMismatch { sequence: u64 },
    InvalidReadRange { start: Vec<u8>, end: Vec<u8> },
    InvalidRoot(String),
    LeaseAuthority(SnapshotLeaseValidationError),
    LeaseRootMismatch(String),
    LogChainMismatch { sequence: u64 },
    NonMonotonicTail { prior: u64, observed: u64 },
    SnapshotExpired { requested: u64, minimum: u64 },
    SnapshotUnavailable { requested: u64, applied: u64 },
    StaleRootSwap { expected: String, observed: String },
    StatePoisoned,
    TailDoesNotReachTarget { target: u64, observed: u64 },
}

impl Display for RangeServingViewError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(error) => write!(formatter, "immutable base read failed: {error}"),
            Self::BaseFrontierMismatch { expected, observed } => write!(
                formatter,
                "authority base frontier {expected} differs from manifest frontier {observed}"
            ),
            Self::CertificateCoverageMismatch { sequence } => write!(
                formatter,
                "txLog certificates do not exactly cover commit {sequence}"
            ),
            Self::Envelope(detail) => write!(formatter, "invalid txLog envelope: {detail}"),
            Self::IdentityMismatch { sequence } => {
                write!(
                    formatter,
                    "txLog commit {sequence} names another range domain"
                )
            }
            Self::InvalidReadRange { start, end } => {
                write!(formatter, "invalid read range {start:?}..{end:?}")
            }
            Self::InvalidRoot(detail) => write!(formatter, "invalid authority root: {detail}"),
            Self::LeaseAuthority(error) => {
                write!(
                    formatter,
                    "snapshot lease authority refused access: {error}"
                )
            }
            Self::LeaseRootMismatch(detail) => {
                write!(
                    formatter,
                    "snapshot lease does not authorize root: {detail}"
                )
            }
            Self::LogChainMismatch { sequence } => {
                write!(formatter, "txLog chain breaks at commit {sequence}")
            }
            Self::NonMonotonicTail { prior, observed } => write!(
                formatter,
                "txLog commit {observed} does not follow increasing frontier {prior}"
            ),
            Self::SnapshotExpired { requested, minimum } => write!(
                formatter,
                "snapshot {requested} precedes minimum-readable version {minimum}"
            ),
            Self::SnapshotUnavailable { requested, applied } => write!(
                formatter,
                "snapshot {requested} exceeds authenticated frontier {applied}"
            ),
            Self::StaleRootSwap { expected, observed } => write!(
                formatter,
                "serving root compare failed: expected {expected}, observed {observed}"
            ),
            Self::StatePoisoned => formatter.write_str("serving root state is poisoned"),
            Self::TailDoesNotReachTarget { target, observed } => write!(
                formatter,
                "txLog tail stops at commit {observed}, below target {target}"
            ),
        }
    }
}

impl Error for RangeServingViewError {}

impl From<AdapterError> for RangeServingViewError {
    fn from(error: AdapterError) -> Self {
        Self::Base(error)
    }
}

impl From<SnapshotLeaseValidationError> for RangeServingViewError {
    fn from(error: SnapshotLeaseValidationError) -> Self {
        Self::LeaseAuthority(error)
    }
}

/// One immutable serving view. Reads use the authority-selected `SlateDB` base
/// and overlay only the quorum-certified txLog suffix above that base.
pub struct AuthorityBoundRangeView {
    base: AuthorityBoundSlateReader,
    root: AuthorityRangeRoot,
    target_version: u64,
    tail: BTreeMap<Vec<u8>, BTreeMap<u64, Option<Vec<u8>>>>,
    authenticated_tail_records: u64,
    final_log_chain_sha256: [u8; 32],
    base_open_seconds: f64,
    tail_auth_seconds: f64,
}

enum TailValue {
    Untouched,
    Cleared,
    Set(Vec<u8>),
}

impl AuthorityBoundRangeView {
    /// Open and fully authenticate one immutable base plus txLog suffix.
    ///
    /// # Errors
    ///
    /// Fails closed when the root metadata differs from the selected `SlateDB`
    /// manifest, a commit is absent or out of order, the log chain breaks, or
    /// any required log set lacks one valid quorum certificate.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        database_path: &str,
        store: Arc<dyn ObjectStore>,
        root: AuthorityRangeRoot,
        target_version: u64,
        records: Vec<CertifiedTxLogRecord>,
        policies: &BTreeMap<u16, CellLogSetPolicy>,
        seed: u64,
    ) -> Result<Self, RangeServingViewError> {
        Self::open_inner(
            database_path,
            store,
            root,
            target_version,
            records,
            policies,
            seed,
            None,
        )
        .await
    }

    /// Open a historical view only after revalidating its exact snapshot lease
    /// against a current publication-authority snapshot.
    ///
    /// # Errors
    ///
    /// Fails before object or cache access when the lease is absent, expired,
    /// changed, or names another manifest or snapshot version.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_historical(
        database_path: &str,
        store: Arc<dyn ObjectStore>,
        publication_root: &PublicationObjectReference,
        root: AuthorityRangeRoot,
        target_version: u64,
        records: Vec<CertifiedTxLogRecord>,
        policies: &BTreeMap<u16, CellLogSetPolicy>,
        seed: u64,
        authority: &PublicationAuthorityState,
        lease: &SnapshotLeaseToken,
    ) -> Result<Self, RangeServingViewError> {
        validate_snapshot_lease_for_root(
            authority,
            lease,
            publication_root,
            &root,
            target_version,
        )?;
        Self::open(
            database_path,
            store,
            root,
            target_version,
            records,
            policies,
            seed,
        )
        .await
    }

    /// Open a cache-backed historical view only after current authority
    /// revalidation of its exact snapshot lease.
    ///
    /// # Errors
    ///
    /// Fails before object or cache access when the lease is absent, expired,
    /// changed, or names another manifest or snapshot version.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_historical_with_cache(
        database_path: &str,
        store: Arc<dyn ObjectStore>,
        publication_root: &PublicationObjectReference,
        root: AuthorityRangeRoot,
        target_version: u64,
        records: Vec<CertifiedTxLogRecord>,
        policies: &BTreeMap<u16, CellLogSetPolicy>,
        seed: u64,
        cache: Arc<dyn DbCache>,
        authority: &PublicationAuthorityState,
        lease: &SnapshotLeaseToken,
    ) -> Result<Self, RangeServingViewError> {
        validate_snapshot_lease_for_root(
            authority,
            lease,
            publication_root,
            &root,
            target_version,
        )?;
        Self::open_with_cache(
            database_path,
            store,
            root,
            target_version,
            records,
            policies,
            seed,
            cache,
        )
        .await
    }

    /// Open one immutable base plus certified tail with a caller-owned decoded
    /// cache shared by the containing KV Runtime.
    ///
    /// # Errors
    ///
    /// Fails under the same authority, certificate, and storage conditions as
    /// [`Self::open`].
    #[allow(clippy::too_many_arguments)]
    pub async fn open_with_cache(
        database_path: &str,
        store: Arc<dyn ObjectStore>,
        root: AuthorityRangeRoot,
        target_version: u64,
        records: Vec<CertifiedTxLogRecord>,
        policies: &BTreeMap<u16, CellLogSetPolicy>,
        seed: u64,
        cache: Arc<dyn DbCache>,
    ) -> Result<Self, RangeServingViewError> {
        Self::open_inner(
            database_path,
            store,
            root,
            target_version,
            records,
            policies,
            seed,
            Some(cache),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_inner(
        database_path: &str,
        store: Arc<dyn ObjectStore>,
        root: AuthorityRangeRoot,
        target_version: u64,
        records: Vec<CertifiedTxLogRecord>,
        policies: &BTreeMap<u16, CellLogSetPolicy>,
        seed: u64,
        cache: Option<Arc<dyn DbCache>>,
    ) -> Result<Self, RangeServingViewError> {
        validate_root(&root, target_version)?;
        let base_started = Instant::now();
        let base = match cache {
            Some(cache) => {
                AuthorityBoundSlateReader::open_with_cache(
                    database_path,
                    store,
                    &root.manifest,
                    seed,
                    cache,
                )
                .await?
            }
            None => {
                AuthorityBoundSlateReader::open(database_path, store, &root.manifest, seed).await?
            }
        };
        let observed = base.latest_version().await?.sequence();
        if observed != root.covered_through {
            return Err(RangeServingViewError::BaseFrontierMismatch {
                expected: root.covered_through,
                observed,
            });
        }
        let base_open_seconds = base_started.elapsed().as_secs_f64();

        let tail_started = Instant::now();
        let mut observed_frontier = root.covered_through;
        let mut previous_chain = root.log_chain_sha256;
        let mut tail = BTreeMap::<Vec<u8>, BTreeMap<u64, Option<Vec<u8>>>>::new();
        for record in &records {
            let envelope = CommitEnvelope::decode(&record.envelope)
                .map_err(|error| RangeServingViewError::Envelope(error.to_string()))?;
            let sequence = envelope.version().sequence();
            if sequence <= observed_frontier || sequence > target_version {
                return Err(RangeServingViewError::NonMonotonicTail {
                    prior: observed_frontier,
                    observed: sequence,
                });
            }
            if envelope.cell_id() != root.cell_id
                || envelope.tenant_id() != root.tenant_id
                || envelope.generation() != root.generation
            {
                return Err(RangeServingViewError::IdentityMismatch { sequence });
            }
            if envelope.previous_log_chain() != previous_chain {
                return Err(RangeServingViewError::LogChainMismatch { sequence });
            }
            authenticate_record(record, &envelope, policies)?;
            let mutations: Vec<CellMutation> =
                serde_json::from_slice(envelope.canonical_mutations())
                    .map_err(|error| RangeServingViewError::Envelope(error.to_string()))?;
            for mutation in mutations {
                match mutation {
                    CellMutation::Clear { key } => {
                        tail.entry(key).or_default().insert(sequence, None);
                    }
                    CellMutation::Set { key, value } => {
                        tail.entry(key).or_default().insert(sequence, Some(value));
                    }
                }
            }
            previous_chain = Sha256::digest(&record.envelope).into();
            observed_frontier = sequence;
        }
        if observed_frontier != target_version {
            return Err(RangeServingViewError::TailDoesNotReachTarget {
                target: target_version,
                observed: observed_frontier,
            });
        }
        let tail_auth_seconds = tail_started.elapsed().as_secs_f64();
        Ok(Self {
            base,
            root,
            target_version,
            tail,
            authenticated_tail_records: u64::try_from(records.len()).unwrap_or(u64::MAX),
            final_log_chain_sha256: previous_chain,
            base_open_seconds,
            tail_auth_seconds,
        })
    }

    /// Exact selected physical manifest key.
    #[must_use]
    pub fn manifest_key(&self) -> &str {
        &self.root.manifest.key
    }

    /// Cell identity bound into the authority root.
    #[must_use]
    pub const fn cell_id(&self) -> [u8; 16] {
        self.root.cell_id
    }

    /// Tenant transaction domain bound into the authority root.
    #[must_use]
    pub const fn tenant_id(&self) -> [u8; 16] {
        self.root.tenant_id
    }

    /// Transaction-system generation bound into the authority root.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.root.generation
    }

    /// Immutable object-base frontier.
    #[must_use]
    pub const fn base_frontier(&self) -> u64 {
        self.root.covered_through
    }

    /// Highest commit authenticated into this view.
    #[must_use]
    pub const fn target_version(&self) -> u64 {
        self.target_version
    }

    /// Count of authenticated txLog records above the base frontier.
    #[must_use]
    pub const fn authenticated_tail_records(&self) -> u64 {
        self.authenticated_tail_records
    }

    /// Time spent opening and identity-checking the authority-selected base.
    #[must_use]
    pub const fn base_open_seconds(&self) -> f64 {
        self.base_open_seconds
    }

    /// Time spent decoding, authenticating, and indexing the certified tail.
    #[must_use]
    pub const fn tail_auth_seconds(&self) -> f64 {
        self.tail_auth_seconds
    }

    /// Commit-chain digest after the final authenticated tail record.
    #[must_use]
    pub const fn final_log_chain_sha256(&self) -> [u8; 32] {
        self.final_log_chain_sha256
    }

    /// Exact compare token for atomically replacing this serving view.
    #[must_use]
    pub fn publication_token(&self) -> RangeServingViewToken {
        RangeServingViewToken {
            root: self.root.clone(),
            target_version: self.target_version,
            final_log_chain_sha256: self.final_log_chain_sha256,
        }
    }

    /// Read one key at an exact version from the base plus authenticated tail.
    ///
    /// # Errors
    ///
    /// Returns an availability error outside the root's retained window or an
    /// immutable-base error.
    pub async fn get_at(
        &self,
        key: &[u8],
        read_version: u64,
    ) -> Result<Option<Vec<u8>>, RangeServingViewError> {
        self.require_readable(read_version)?;
        if read_version > self.root.covered_through {
            match self.tail_value(key, read_version) {
                TailValue::Untouched => {}
                TailValue::Cleared => return Ok(None),
                TailValue::Set(value) => return Ok(Some(value)),
            }
        }
        let base_version = read_version.min(self.root.covered_through);
        self.base
            .get_at_retained(
                key,
                Version::new(base_version),
                Version::new(self.root.minimum_readable_version),
            )
            .await
            .map_err(Into::into)
    }

    /// Scan one half-open key range at an exact version.
    ///
    /// The implementation advances an ordered immutable-base cursor and the
    /// ordered in-memory tail together, stopping at the logical row limit.
    ///
    /// # Errors
    ///
    /// Returns an invalid-range, snapshot-availability, or base-read error.
    pub async fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        read_version: u64,
        limit: usize,
    ) -> Result<Vec<Row>, RangeServingViewError> {
        if start >= end {
            return Err(RangeServingViewError::InvalidReadRange {
                start: start.to_vec(),
                end: end.to_vec(),
            });
        }
        self.require_readable(read_version)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let base_version = read_version.min(self.root.covered_through);
        let mut base_cursor = self
            .base
            .scan_cursor_at_retained(
                start,
                end,
                Version::new(base_version),
                Version::new(self.root.minimum_readable_version),
            )
            .await?;
        let mut base_next = base_cursor.next().await?;
        let mut tail_iter =
            self.tail
                .range(start.to_vec()..end.to_vec())
                .filter_map(|(key, versions)| {
                    versions
                        .range(..=read_version)
                        .next_back()
                        .map(|(_, value)| (key, value))
                });
        let mut tail_next = tail_iter.next();
        let mut rows = Vec::with_capacity(limit.min(1_024));
        while rows.len() < limit {
            match (base_next.as_ref(), tail_next.as_ref()) {
                (Some((base_key, _)), Some((tail_key, _))) => {
                    match base_key.as_slice().cmp(tail_key.as_slice()) {
                        Ordering::Less => {
                            if let Some(row) = base_next.take() {
                                rows.push(row);
                            }
                            base_next = base_cursor.next().await?;
                        }
                        Ordering::Equal => {
                            if let Some(value) =
                                tail_next.as_ref().and_then(|(_, value)| value.as_ref())
                            {
                                rows.push((base_key.clone(), value.clone()));
                            }
                            base_next = base_cursor.next().await?;
                            tail_next = tail_iter.next();
                        }
                        Ordering::Greater => {
                            if let Some((key, Some(value))) = tail_next {
                                rows.push((key.clone(), value.clone()));
                            }
                            tail_next = tail_iter.next();
                        }
                    }
                }
                (Some(_), None) => {
                    if let Some(row) = base_next.take() {
                        rows.push(row);
                    }
                    base_next = base_cursor.next().await?;
                }
                (None, Some(_)) => {
                    if let Some((key, Some(value))) = tail_next {
                        rows.push((key.clone(), value.clone()));
                    }
                    tail_next = tail_iter.next();
                }
                (None, None) => break,
            }
        }
        Ok(rows)
    }

    /// Close the selected immutable base.
    ///
    /// # Errors
    ///
    /// Returns a `SlateDB` adapter error when the reader cannot close.
    pub async fn close(&self) -> Result<(), RangeServingViewError> {
        self.base.close().await.map_err(Into::into)
    }

    fn require_readable(&self, requested: u64) -> Result<(), RangeServingViewError> {
        if requested < self.root.minimum_readable_version {
            return Err(RangeServingViewError::SnapshotExpired {
                requested,
                minimum: self.root.minimum_readable_version,
            });
        }
        if requested > self.target_version {
            return Err(RangeServingViewError::SnapshotUnavailable {
                requested,
                applied: self.target_version,
            });
        }
        Ok(())
    }

    fn tail_value(&self, key: &[u8], read_version: u64) -> TailValue {
        match self
            .tail
            .get(key)
            .and_then(|versions| versions.range(..=read_version).next_back())
            .map(|(_, value)| value)
        {
            None => TailValue::Untouched,
            Some(None) => TailValue::Cleared,
            Some(Some(value)) => TailValue::Set(value.clone()),
        }
    }
}

/// Process-local atomic pointer to one fully built serving view.
///
/// Existing readers keep the old `Arc` alive while new readers observe the
/// replacement. The replicated publication authority remains responsible for
/// deciding when the compare-and-swap is permitted.
pub struct RangeServingState {
    current: RwLock<Arc<AuthorityBoundRangeView>>,
}

impl RangeServingState {
    #[must_use]
    pub fn new(initial: AuthorityBoundRangeView) -> Self {
        Self {
            current: RwLock::new(Arc::new(initial)),
        }
    }

    /// Clone the current view without holding the state lock during I/O.
    ///
    /// # Errors
    ///
    /// Fails only after a poisoned process-local lock.
    pub fn current(&self) -> Result<Arc<AuthorityBoundRangeView>, RangeServingViewError> {
        self.current
            .read()
            .map(|current| Arc::clone(&current))
            .map_err(|_| RangeServingViewError::StatePoisoned)
    }

    /// Install one prevalidated replacement only when the current root is the
    /// exact authority root expected by the publication transition.
    ///
    /// # Errors
    ///
    /// Fails closed on a stale expected root or poisoned process-local lock.
    pub fn install_if_current(
        &self,
        expected: &RangeServingViewToken,
        replacement: AuthorityBoundRangeView,
    ) -> Result<Arc<AuthorityBoundRangeView>, RangeServingViewError> {
        let mut current = self
            .current
            .write()
            .map_err(|_| RangeServingViewError::StatePoisoned)?;
        let observed = current.publication_token();
        if &observed != expected {
            return Err(RangeServingViewError::StaleRootSwap {
                expected: serving_token_summary(expected),
                observed: serving_token_summary(&observed),
            });
        }
        Ok(std::mem::replace(&mut *current, Arc::new(replacement)))
    }
}

fn serving_token_summary(token: &RangeServingViewToken) -> String {
    format!(
        "generation={}, manifest={}, base={}, target={}, chain={}",
        token.root.generation,
        token.root.manifest.key,
        token.root.covered_through,
        token.target_version,
        hex_prefix(&token.final_log_chain_sha256),
    )
}

fn hex_prefix(value: &[u8; 32]) -> String {
    let mut prefix = String::with_capacity(16);
    for byte in &value[..8] {
        write!(&mut prefix, "{byte:02x}").expect("writing to a String cannot fail");
    }
    prefix
}

fn validate_snapshot_lease_for_root(
    authority: &PublicationAuthorityState,
    lease: &SnapshotLeaseToken,
    publication_root: &PublicationObjectReference,
    root: &AuthorityRangeRoot,
    target_version: u64,
) -> Result<(), RangeServingViewError> {
    authority.validate_active_snapshot_lease(lease)?;
    if lease.snapshot_version != target_version {
        return Err(RangeServingViewError::LeaseRootMismatch(format!(
            "lease snapshot {} differs from target {}",
            lease.snapshot_version, target_version
        )));
    }
    if &lease.closure.manifest != publication_root {
        return Err(RangeServingViewError::LeaseRootMismatch(
            "lease manifest identity differs from published range root".to_owned(),
        ));
    }
    if !lease.closure.object_keys.contains(&publication_root.key) {
        return Err(RangeServingViewError::LeaseRootMismatch(
            "lease closure omits the published range root".to_owned(),
        ));
    }
    if !lease.closure.object_keys.contains(&root.manifest.key) {
        return Err(RangeServingViewError::LeaseRootMismatch(
            "lease closure omits the immutable-base manifest".to_owned(),
        ));
    }
    Ok(())
}

fn validate_root(root: &AuthorityRangeRoot, target: u64) -> Result<(), RangeServingViewError> {
    if root.generation == 0 {
        return Err(RangeServingViewError::InvalidRoot(
            "generation must be nonzero".to_owned(),
        ));
    }
    if root.covered_through == 0 || root.minimum_readable_version > root.covered_through {
        return Err(RangeServingViewError::InvalidRoot(
            "base frontier must be nonzero and retain its minimum version".to_owned(),
        ));
    }
    if target < root.covered_through {
        return Err(RangeServingViewError::InvalidRoot(
            "target precedes the immutable base frontier".to_owned(),
        ));
    }
    Ok(())
}

fn authenticate_record(
    record: &CertifiedTxLogRecord,
    envelope: &CommitEnvelope,
    policies: &BTreeMap<u16, CellLogSetPolicy>,
) -> Result<(), RangeServingViewError> {
    let sequence = envelope.version().sequence();
    let required = envelope
        .required_log_tags()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let supplied = record
        .certificates
        .iter()
        .map(|certificate| certificate.statement.log_set_id)
        .collect::<BTreeSet<_>>();
    if required.len() != record.certificates.len() || required != supplied {
        return Err(RangeServingViewError::CertificateCoverageMismatch { sequence });
    }
    for certificate in &record.certificates {
        let Some(policy) = policies.get(&certificate.statement.log_set_id) else {
            return Err(RangeServingViewError::CertificateCoverageMismatch { sequence });
        };
        if !verify_tagged_log_envelope_certificate(certificate, policy, &record.envelope) {
            return Err(RangeServingViewError::CertificateCoverageMismatch { sequence });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorityBoundRangeView, AuthorityRangeRoot, CertifiedTxLogRecord, RangeServingState,
        RangeServingViewError,
    };
    use crate::{
        request_range_read, serve_range_read_listener, KvReadRouter, KvReadRouterConfig,
        RangeEngineId, RangeReadAssignment, RangeReadProtocolConfig, RoutedRangeReadError,
        RoutedRangeReadReply, RoutedRangeReadRequest,
    };
    use futures_util::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use okv_consensus::{
        sign_tagged_log_statement, tagged_log_public_key, CellLogSetMember, CellLogSetPolicy,
        CellMutation, CellTaggedLogCertificate, CellTaggedLogStatement, RequestIdentity,
    };
    use okv_model::{CommitBatch, CommitIdentity, Mutation, Version};
    use okv_sim::{CommitEnvelope, CommitEnvelopeParts};
    use okv_slate::{AuthorityManifestReference, SlateEngine};
    use sha2::{Digest, Sha256};
    use slatedb::config::Settings;
    use slatedb::Db;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::Barrier;

    const DATABASE_PATH: &str = "range-view";
    const CELL_ID: [u8; 16] = [0x11; 16];
    const TENANT_ID: [u8; 16] = [0x22; 16];
    const GENERATION: u64 = 7;
    const LOG_SET_ID: u16 = 10;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn swaps_authority_base_without_losing_exact_old_or_new_reads() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = build_engine(Arc::clone(&store)).await;
        let mutations = fixture_mutations();
        let envelopes = fixture_envelopes(&mutations);
        for sequence in 1_u64..=2 {
            engine
                .apply(model_batch(sequence, &mutations))
                .await
                .expect("apply M0 commit");
        }
        engine.flush().await.expect("flush M0");
        let manifest_m0 = latest_manifest_reference(Arc::clone(&store)).await;

        engine
            .apply(model_batch(5, &mutations))
            .await
            .expect("apply M1 commit");
        engine.flush().await.expect("flush M1");
        let manifest_m1 = latest_manifest_reference(Arc::clone(&store)).await;
        engine.close().await.expect("close SlateDB writer");
        assert_ne!(manifest_m0, manifest_m1);

        let (policy, signing_seeds) = log_policy();
        let policies = BTreeMap::from([(LOG_SET_ID, policy.clone())]);
        let certified = envelopes
            .iter()
            .skip(2)
            .map(|envelope| certified_record(envelope, &policy, &signing_seeds))
            .collect::<Vec<_>>();
        let root_m0 = range_root(
            manifest_m0.clone(),
            2,
            Sha256::digest(envelopes[1].encode()).into(),
        );
        let root_m1 = range_root(
            manifest_m1.clone(),
            5,
            Sha256::digest(envelopes[2].encode()).into(),
        );

        let old_view = AuthorityBoundRangeView::open(
            DATABASE_PATH,
            Arc::clone(&store),
            root_m0.clone(),
            8,
            certified.clone(),
            &policies,
            1103,
        )
        .await
        .expect("open M0 plus commits 5 and 8");
        assert_eq!(old_view.base_frontier(), 2);
        assert_eq!(old_view.authenticated_tail_records(), 2);
        assert_eq!(
            old_view.scan_at(b"a", b"z", 8, 10).await.unwrap(),
            expected()
        );

        let new_view = AuthorityBoundRangeView::open(
            DATABASE_PATH,
            Arc::clone(&store),
            root_m1,
            8,
            vec![certified[1].clone()],
            &policies,
            2207,
        )
        .await
        .expect("open M1 plus commit 8");
        assert_eq!(new_view.base_frontier(), 5);
        assert_eq!(new_view.authenticated_tail_records(), 1);

        let old_token = old_view.publication_token();
        let state = RangeServingState::new(old_view);
        let retained_old_reader = state.current().expect("retain M0 view");
        let replaced = state
            .install_if_current(&old_token, new_view)
            .expect("publish M1 serving view");
        let current = state.current().expect("read M1 serving view");
        assert_eq!(current.manifest_key(), manifest_m1.key);
        assert_eq!(
            current.scan_at(b"a", b"z", 8, 10).await.unwrap(),
            expected()
        );
        assert_eq!(
            retained_old_reader
                .scan_at(b"a", b"z", 8, 10)
                .await
                .unwrap(),
            expected()
        );
        assert_eq!(replaced.manifest_key(), manifest_m0.key);

        let mut tampered = certified;
        tampered[0].certificates[0].statement.envelope_sha256 = [0; 32];
        let error = AuthorityBoundRangeView::open(
            DATABASE_PATH,
            Arc::clone(&store),
            root_m0,
            8,
            tampered,
            &policies,
            3301,
        )
        .await
        .err()
        .expect("tampered txLog certificate must fail");
        assert!(matches!(
            error,
            RangeServingViewError::CertificateCoverageMismatch { sequence: 5 }
        ));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn publishes_same_base_tail_advance_without_mixing_concurrent_readers() {
        const READER_COUNT: usize = 16;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = build_engine(Arc::clone(&store)).await;
        let mutations = fixture_mutations();
        let envelopes = fixture_envelopes(&mutations);
        for sequence in 1_u64..=2 {
            engine
                .apply(model_batch(sequence, &mutations))
                .await
                .expect("apply immutable-base commit");
        }
        engine.flush().await.expect("flush immutable base");
        let manifest = latest_manifest_reference(Arc::clone(&store)).await;
        engine.close().await.expect("close SlateDB writer");

        let (policy, signing_seeds) = log_policy();
        let policies = BTreeMap::from([(LOG_SET_ID, policy.clone())]);
        let certified = envelopes
            .iter()
            .skip(2)
            .map(|envelope| certified_record(envelope, &policy, &signing_seeds))
            .collect::<Vec<_>>();
        let root = range_root(manifest, 2, Sha256::digest(envelopes[1].encode()).into());

        let old_view = AuthorityBoundRangeView::open(
            DATABASE_PATH,
            Arc::clone(&store),
            root.clone(),
            5,
            vec![certified[0].clone()],
            &policies,
            4409,
        )
        .await
        .expect("open base plus commit 5");
        let old_token = old_view.publication_token();
        let new_view = AuthorityBoundRangeView::open(
            DATABASE_PATH,
            Arc::clone(&store),
            root.clone(),
            8,
            certified.clone(),
            &policies,
            5501,
        )
        .await
        .expect("open same base plus commits 5 and 8");
        let new_token = new_view.publication_token();
        assert_eq!(old_token.root.manifest, new_token.root.manifest);
        assert_ne!(old_token, new_token);

        let stale_replacement = AuthorityBoundRangeView::open(
            DATABASE_PATH,
            Arc::clone(&store),
            root,
            5,
            vec![certified[0].clone()],
            &policies,
            6607,
        )
        .await
        .expect("open stale same-base replacement");
        let state = Arc::new(RangeServingState::new(old_view));
        let retained = Arc::new(Barrier::new(READER_COUNT + 1));
        let published = Arc::new(Barrier::new(READER_COUNT + 1));
        let mut readers = Vec::with_capacity(READER_COUNT);
        for _ in 0..READER_COUNT {
            let state = Arc::clone(&state);
            let retained = Arc::clone(&retained);
            let published = Arc::clone(&published);
            readers.push(tokio::spawn(async move {
                let old = state.current().expect("retain old serving view");
                retained.wait().await;
                published.wait().await;
                let old_rows = old
                    .scan_at(b"a", b"z", old.target_version(), 10)
                    .await
                    .expect("old reader remains exact");
                let new = state.current().expect("load new serving view");
                let new_rows = new
                    .scan_at(b"a", b"z", new.target_version(), 10)
                    .await
                    .expect("new reader is exact");
                (
                    old.publication_token(),
                    old_rows,
                    new.publication_token(),
                    new_rows,
                )
            }));
        }

        retained.wait().await;
        let replaced = state
            .install_if_current(&old_token, new_view)
            .expect("publish fully authenticated tail advance");
        assert_eq!(replaced.publication_token(), old_token);
        published.wait().await;

        for reader in readers {
            let (observed_old, old_rows, observed_new, new_rows) =
                reader.await.expect("reader task completes");
            assert_eq!(observed_old, old_token);
            assert_eq!(old_rows, expected_at_5());
            assert_eq!(observed_new, new_token);
            assert_eq!(new_rows, expected());
        }

        let error = state
            .install_if_current(&old_token, stale_replacement)
            .err()
            .expect("same-manifest stale token must be fenced");
        assert!(matches!(error, RangeServingViewError::StaleRootSwap { .. }));
        assert_eq!(
            state.current().unwrap().publication_token(),
            new_token,
            "failed stale publication must not disturb the current view"
        );

        let router = Arc::new(
            KvReadRouter::new(KvReadRouterConfig {
                cell_id: CELL_ID,
                max_in_flight: 8,
                max_key_bytes: 256,
                max_scan_rows: 100,
            })
            .unwrap(),
        );
        let range_id = RangeEngineId(71);
        let right_range_id = RangeEngineId(72);
        router
            .assign(
                RangeReadAssignment {
                    tenant_id: TENANT_ID,
                    range_id,
                    routing_epoch: 9,
                    start: b"a".to_vec(),
                    end: b"m".to_vec(),
                },
                Arc::clone(&state),
            )
            .unwrap();
        router
            .assign(
                RangeReadAssignment {
                    tenant_id: TENANT_ID,
                    range_id: right_range_id,
                    routing_epoch: 10,
                    start: b"m".to_vec(),
                    end: b"z".to_vec(),
                },
                Arc::clone(&state),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let protocol = RangeReadProtocolConfig {
            max_frame_bytes: 16 * 1024,
            request_timeout_millis: 2_000,
        };
        let server = tokio::spawn(serve_range_read_listener(
            listener,
            protocol,
            Arc::clone(&router),
        ));

        let point = request_range_read(
            &endpoint,
            &RoutedRangeReadRequest::point(CELL_ID, TENANT_ID, range_id, 9, 8, b"a".to_vec()),
            protocol,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            point,
            RoutedRangeReadReply::Point {
                value: Some(ref value),
                ref stamp,
            } if value == b"a8" && stamp.applied_frontier == 8 && stamp.routing_epoch == 9
        ));

        let scan = request_range_read(
            &endpoint,
            &RoutedRangeReadRequest::scan(
                CELL_ID,
                TENANT_ID,
                range_id,
                9,
                8,
                b"a".to_vec(),
                b"m".to_vec(),
                10,
            ),
            protocol,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(scan, RoutedRangeReadReply::Scan { rows, .. } if rows == expected()));

        let right_point = request_range_read(
            &endpoint,
            &RoutedRangeReadRequest::point(
                CELL_ID,
                TENANT_ID,
                right_range_id,
                10,
                8,
                b"x".to_vec(),
            ),
            protocol,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            right_point,
            RoutedRangeReadReply::Point { value: None, .. }
        ));

        let stale_reply = request_range_read(
            &endpoint,
            &RoutedRangeReadRequest::point(CELL_ID, TENANT_ID, range_id, 8, 8, b"a".to_vec()),
            protocol,
        )
        .await
        .unwrap();
        assert!(matches!(
            stale_reply,
            Err(RoutedRangeReadError::StaleRoute { .. })
        ));

        let crossing = request_range_read(
            &endpoint,
            &RoutedRangeReadRequest::scan(
                CELL_ID,
                TENANT_ID,
                range_id,
                9,
                8,
                b"a".to_vec(),
                vec![0xff],
                10,
            ),
            protocol,
        )
        .await
        .unwrap();
        assert_eq!(
            crossing,
            Err(RoutedRangeReadError::ScanCrossesRange {
                split_at: b"m".to_vec(),
            })
        );

        let unavailable = request_range_read(
            &endpoint,
            &RoutedRangeReadRequest::point(CELL_ID, TENANT_ID, range_id, 9, 9, b"a".to_vec()),
            protocol,
        )
        .await
        .unwrap();
        assert_eq!(
            unavailable,
            Err(RoutedRangeReadError::SnapshotUnavailable {
                requested: 9,
                applied: 8,
            })
        );
        server.abort();
    }

    async fn build_engine(store: Arc<dyn ObjectStore>) -> SlateEngine {
        let settings = Settings {
            flush_interval: None,
            wal_enabled: false,
            compactor_options: None,
            garbage_collector_options: None,
            ..Settings::default()
        };
        Db::builder(DATABASE_PATH, store)
            .with_settings(settings)
            .with_seed(997)
            .build()
            .await
            .map(SlateEngine::new)
            .expect("open SlateDB fixture")
    }

    fn fixture_mutations() -> BTreeMap<u64, Vec<CellMutation>> {
        BTreeMap::from([
            (
                1,
                vec![
                    CellMutation::Set {
                        key: b"a".to_vec(),
                        value: b"a1".to_vec(),
                    },
                    CellMutation::Set {
                        key: b"b".to_vec(),
                        value: b"b1".to_vec(),
                    },
                ],
            ),
            (
                2,
                vec![
                    CellMutation::Set {
                        key: b"a".to_vec(),
                        value: b"a2".to_vec(),
                    },
                    CellMutation::Set {
                        key: b"c".to_vec(),
                        value: b"c2".to_vec(),
                    },
                ],
            ),
            (
                5,
                vec![
                    CellMutation::Clear { key: b"b".to_vec() },
                    CellMutation::Set {
                        key: b"d".to_vec(),
                        value: b"d5".to_vec(),
                    },
                ],
            ),
            (
                8,
                vec![
                    CellMutation::Set {
                        key: b"a".to_vec(),
                        value: b"a8".to_vec(),
                    },
                    CellMutation::Set {
                        key: b"b".to_vec(),
                        value: b"b8".to_vec(),
                    },
                ],
            ),
        ])
    }

    fn fixture_envelopes(mutations: &BTreeMap<u64, Vec<CellMutation>>) -> Vec<CommitEnvelope> {
        let mut previous_log_chain = [0_u8; 32];
        let mut envelopes = Vec::new();
        for sequence in [1_u64, 2, 5, 8] {
            let mut client_id = [0_u8; 16];
            client_id[8..].copy_from_slice(&sequence.to_be_bytes());
            let envelope = CommitEnvelope::from_parts(CommitEnvelopeParts {
                cell_id: CELL_ID,
                tenant_id: TENANT_ID,
                generation: GENERATION,
                version: Version::from_parts(GENERATION, sequence),
                log_index: sequence,
                client_id,
                request_id: sequence,
                resolver_set_id: [0x33; 16],
                read_conflicts: Vec::new(),
                write_conflicts: Vec::new(),
                canonical_mutations: serde_json::to_vec(&mutations[&sequence])
                    .expect("encode mutations"),
                required_resolvers: vec![1],
                required_log_tags: vec![LOG_SET_ID],
                previous_log_chain,
            });
            previous_log_chain = Sha256::digest(envelope.encode()).into();
            envelopes.push(envelope);
        }
        envelopes
    }

    fn model_batch(sequence: u64, mutations: &BTreeMap<u64, Vec<CellMutation>>) -> CommitBatch {
        let mutations = mutations[&sequence]
            .iter()
            .map(|mutation| match mutation {
                CellMutation::Clear { key } => Mutation::Clear { key: key.clone() },
                CellMutation::Set { key, value } => Mutation::Set {
                    key: key.clone(),
                    value: value.clone(),
                },
            })
            .collect();
        CommitBatch {
            version: Version::new(sequence),
            identity: CommitIdentity::for_test(sequence),
            mutations,
        }
    }

    async fn latest_manifest_reference(store: Arc<dyn ObjectStore>) -> AuthorityManifestReference {
        let prefix = Path::from(format!("{DATABASE_PATH}/manifest"));
        let manifests = store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .expect("list SlateDB manifests");
        let latest = manifests
            .into_iter()
            .max_by(|left, right| left.location.cmp(&right.location))
            .expect("one SlateDB manifest");
        let bytes = store
            .get(&latest.location)
            .await
            .expect("get SlateDB manifest")
            .bytes()
            .await
            .expect("read SlateDB manifest");
        AuthorityManifestReference {
            key: latest.location.to_string(),
            length: u64::try_from(bytes.len()).expect("manifest length fits u64"),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
        }
    }

    fn log_policy() -> (CellLogSetPolicy, BTreeMap<u64, Vec<u8>>) {
        let seeds = BTreeMap::from([
            (101, vec![0x11; 32]),
            (102, vec![0x22; 32]),
            (103, vec![0x33; 32]),
        ]);
        let members = seeds
            .iter()
            .map(|(node_id, seed)| CellLogSetMember {
                node_id: *node_id,
                public_key: tagged_log_public_key(seed).expect("derive public key"),
            })
            .collect();
        (
            CellLogSetPolicy {
                format_version: 1,
                generation: GENERATION,
                policy_epoch: 1,
                log_set_id: LOG_SET_ID,
                quorum_size: 2,
                ratekeeper_soft_limit_bytes: 4096,
                members,
            },
            seeds,
        )
    }

    fn certified_record(
        envelope: &CommitEnvelope,
        policy: &CellLogSetPolicy,
        seeds: &BTreeMap<u64, Vec<u8>>,
    ) -> CertifiedTxLogRecord {
        let encoded = envelope.encode();
        let (encoded_client_id, request_id) = envelope.client_identity();
        let mut client_id = [0_u8; 8];
        client_id.copy_from_slice(&encoded_client_id[8..]);
        let statement = CellTaggedLogStatement {
            format_version: 1,
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            generation: GENERATION,
            transaction_identity: RequestIdentity {
                client_id: u64::from_be_bytes(client_id),
                request_id,
            },
            commit_sequence: envelope.version().sequence(),
            log_set_id: LOG_SET_ID,
            policy_epoch: policy.policy_epoch,
            envelope_sha256: Sha256::digest(&encoded).into(),
            durable_position: envelope.version().sequence(),
        };
        let attestations = seeds
            .iter()
            .take(2)
            .map(|(node_id, seed)| {
                sign_tagged_log_statement(*node_id, seed, &statement).expect("sign txLog record")
            })
            .collect();
        CertifiedTxLogRecord {
            envelope: encoded,
            certificates: vec![CellTaggedLogCertificate {
                statement,
                attestations,
            }],
        }
    }

    fn range_root(
        manifest: AuthorityManifestReference,
        covered_through: u64,
        log_chain_sha256: [u8; 32],
    ) -> AuthorityRangeRoot {
        AuthorityRangeRoot {
            cell_id: CELL_ID,
            tenant_id: TENANT_ID,
            generation: GENERATION,
            manifest,
            covered_through,
            minimum_readable_version: 1,
            log_chain_sha256,
        }
    }

    fn expected() -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![
            (b"a".to_vec(), b"a8".to_vec()),
            (b"b".to_vec(), b"b8".to_vec()),
            (b"c".to_vec(), b"c2".to_vec()),
            (b"d".to_vec(), b"d5".to_vec()),
        ]
    }

    fn expected_at_5() -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![
            (b"a".to_vec(), b"a2".to_vec()),
            (b"c".to_vec(), b"c2".to_vec()),
            (b"d".to_vec(), b"d5".to_vec()),
        ]
    }
}
