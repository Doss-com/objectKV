//! Real-process Cell transaction proof for `PostgreSQL` page plus extent writes.

use crate::{
    admit_postgres_page_write, plan_postgres_page_commit, verify_postgres_page_commit,
    PostgresPage, PostgresPageCommitContext, PostgresPageCommitOperation,
    PostgresPageCommitReceipt, PostgresPageIdentity, PostgresPageWriteBatch,
    PostgresRelationExtent, POSTGRES_PAGE_SIZE,
};
use okv_consensus::{
    cell_partitioned_transaction_sha256, CellMutation, CellProcessFixture,
    CellProcessPrototypeMode, CellTransactionClient, RequestIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;

/// Unsafe subject selected by the page-commit process suite.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPageCommitProcessMode {
    Correct,
    OmitExtentMutation,
    ChangeRetryIdentity,
    WrongReceiptIdentity,
    NonAdvancingCommitVersion,
}

impl PostgresPageCommitProcessMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::OmitExtentMutation => "omit_extent_mutation",
            Self::ChangeRetryIdentity => "change_retry_identity",
            Self::WrongReceiptIdentity => "wrong_receipt_identity",
            Self::NonAdvancingCommitVersion => "non_advancing_commit_version",
        }
    }
}

/// Stable semantic receipt for one real-process page commit and leader handoff.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PostgresPageCommitProcessReceipt {
    pub seed: u64,
    pub mode: PostgresPageCommitProcessMode,
    pub cell_process_starts: u64,
    pub leader_handoffs: u64,
    pub expected_objectkv_version: u64,
    pub committed_objectkv_version: u64,
    pub final_objectkv_version: u64,
    pub committed_page_mutations: u64,
    pub committed_extent_mutations: u64,
    pub retry_responses_exact: u64,
    pub pages_visible_after_failover: u64,
    pub extent_nblocks_after_failover: Option<u32>,
    pub commit_receipt: Option<PostgresPageCommitReceipt>,
    pub receipt_error: Option<String>,
    pub checks: BTreeMap<String, bool>,
    pub anomaly_count: u64,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// Commit one admitted extend through a real three-process Cell, retry the same
/// command, kill the leader, and verify page plus extent state on the successor.
///
/// # Errors
///
/// Returns an error when the bounded process fixture, transaction client, or
/// relation-page encoding cannot complete.
pub fn run_postgres_page_commit_process_contract(
    seed: u64,
    mode: PostgresPageCommitProcessMode,
    executable: &Path,
) -> Result<PostgresPageCommitProcessReceipt, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_contract(seed, mode, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_contract(
    seed: u64,
    mode: PostgresPageCommitProcessMode,
    executable: &Path,
) -> Result<PostgresPageCommitProcessReceipt, String> {
    let mut fixture =
        CellProcessFixture::start(seed, CellProcessPrototypeMode::Correct, executable)?;
    let baseline = fixture.run_history().await?;
    let before = fixture.linearizable_cell_snapshot().await?;
    let expected_objectkv_version = before.latest_sequence;
    let batch = contract_batch(seed, expected_objectkv_version)?;
    let admission = admit_postgres_page_write(&batch).map_err(|error| error.to_string())?;
    let context = PostgresPageCommitContext {
        cell_id: before.cell_id,
        tenant_id: before.tenant_id,
        generation: before.generation,
        accepted_resolvers: vec![1, 2],
        durable_log_tags: vec![10, 20],
    };
    let mut plan = plan_postgres_page_commit(
        &admission,
        PostgresPageCommitOperation::Extend,
        0,
        2,
        &context,
    )
    .map_err(|error| error.to_string())?;
    let extent_key = admission.relation.encode_extent_key();
    if mode == PostgresPageCommitProcessMode::OmitExtentMutation {
        plan.command.mutations.retain(|mutation| match mutation {
            CellMutation::Set { key, .. } | CellMutation::Clear { key } => key != &extent_key,
        });
    }
    let observed_command_sha256 = cell_partitioned_transaction_sha256(&plan.command)?;
    let command_digest_exact = observed_command_sha256 == plan.command_sha256;
    let command = plan.command.encode().map_err(|error| error.to_string())?;
    let client = CellTransactionClient::new(fixture.endpoints())?;
    let first_response = client.commit_app_data(&command).await?;
    let mut receipt_response = first_response.clone();
    if mode == PostgresPageCommitProcessMode::WrongReceiptIdentity {
        receipt_response.identity = Some(RequestIdentity {
            client_id: plan.command.identity.client_id,
            request_id: plan.command.identity.request_id.saturating_add(1),
        });
    }
    if mode == PostgresPageCommitProcessMode::NonAdvancingCommitVersion {
        if let Some(outcome) = &mut receipt_response.cell_transaction {
            outcome.commit_sequence = Some(expected_objectkv_version);
        }
    }
    let verified = verify_postgres_page_commit(&plan, &receipt_response);
    let (commit_receipt, receipt_error) = match verified {
        Ok(receipt) => (Some(receipt), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let committed_objectkv_version = first_response
        .cell_transaction
        .as_ref()
        .and_then(|outcome| outcome.commit_sequence)
        .ok_or_else(|| "page commit omitted its commit version".to_owned())?;

    let retry_command = if mode == PostgresPageCommitProcessMode::ChangeRetryIdentity {
        let mut changed = plan.command.clone();
        changed.identity.request_id = changed.identity.request_id.saturating_add(1);
        changed.encode().map_err(|error| error.to_string())?
    } else {
        command
    };
    let retry_response = client.commit_app_data(&retry_command).await?;
    let retry_responses_exact = u64::from(retry_response == first_response);
    let after_retry = fixture.linearizable_cell_snapshot().await?;
    let before_failover_shape = relation_shape(&after_retry.rows, &admission)?;
    fixture.kill_leader_and_elect_successor().await?;
    let after_failover = fixture.linearizable_cell_snapshot().await?;
    let after_failover_shape = relation_shape(&after_failover.rows, &admission)?;
    let page_count = u64::try_from(admission.mutations.len()).unwrap_or(u64::MAX);
    let committed_extent_mutations =
        u64::from(plan.command.mutations.iter().any(
            |mutation| matches!(mutation, CellMutation::Set { key, .. } if key == &extent_key),
        ));
    let checks = BTreeMap::from([
        (
            "baseline_cell_clean".to_owned(),
            baseline.anomaly_count == 0,
        ),
        ("command_digest_exact".to_owned(), command_digest_exact),
        (
            "commit_receipt_verified".to_owned(),
            commit_receipt.is_some(),
        ),
        (
            "commit_version_strictly_advanced".to_owned(),
            committed_objectkv_version > expected_objectkv_version,
        ),
        (
            "retry_response_exact".to_owned(),
            retry_responses_exact == 1,
        ),
        (
            "retry_did_not_advance_version".to_owned(),
            after_retry.latest_sequence == committed_objectkv_version,
        ),
        (
            "page_and_extent_atomic_before_failover".to_owned(),
            before_failover_shape == (page_count, Some(2)),
        ),
        (
            "page_and_extent_exact_after_failover".to_owned(),
            after_failover_shape == (page_count, Some(2))
                && after_failover.latest_sequence == after_retry.latest_sequence,
        ),
    ]);
    let failed = checks
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let semantic = (
        seed,
        mode,
        baseline.process_starts,
        expected_objectkv_version,
        committed_objectkv_version,
        after_failover.latest_sequence,
        page_count,
        committed_extent_mutations,
        retry_responses_exact,
        after_failover_shape,
        &receipt_error,
        &checks,
    );
    let trace = serde_json::to_vec(&semantic).map_err(|error| error.to_string())?;
    Ok(PostgresPageCommitProcessReceipt {
        seed,
        mode,
        cell_process_starts: baseline.process_starts,
        leader_handoffs: 1,
        expected_objectkv_version,
        committed_objectkv_version,
        final_objectkv_version: after_failover.latest_sequence,
        committed_page_mutations: page_count,
        committed_extent_mutations,
        retry_responses_exact,
        pages_visible_after_failover: after_failover_shape.0,
        extent_nblocks_after_failover: after_failover_shape.1,
        commit_receipt,
        receipt_error,
        checks,
        anomaly_count: u64::try_from(failed.len()).unwrap_or(u64::MAX),
        first_mismatch: failed.first().cloned(),
        trace_sha256: format!("{:x}", Sha256::digest(trace)),
    })
}

fn relation_shape(
    rows: &[(Vec<u8>, Vec<u8>)],
    admission: &crate::PostgresPageWriteAdmission,
) -> Result<(u64, Option<u32>), String> {
    let page_keys = admission
        .mutations
        .iter()
        .filter_map(|mutation| match mutation {
            CellMutation::Set { key, .. } => Some(key),
            CellMutation::Clear { .. } => None,
        })
        .collect::<Vec<_>>();
    let visible_pages = rows
        .iter()
        .filter(|(key, value)| {
            page_keys.contains(&key)
                && admission.mutations.iter().any(|mutation| {
                    matches!(mutation, CellMutation::Set { key: expected_key, value: expected_value } if expected_key == key && expected_value == value)
                })
        })
        .count();
    let extent_key = admission.relation.encode_extent_key();
    let extent = rows
        .iter()
        .find(|(key, _)| key == &extent_key)
        .map(|(_, value)| PostgresRelationExtent::decode(value).map_err(|error| error.to_string()))
        .transpose()?;
    Ok((
        u64::try_from(visible_pages).unwrap_or(u64::MAX),
        extent.map(|value| value.nblocks),
    ))
}

fn contract_batch(
    seed: u64,
    expected_objectkv_version: u64,
) -> Result<PostgresPageWriteBatch, String> {
    let request_digest = Sha256::digest(seed.to_be_bytes());
    let mut request_id = [0_u8; 16];
    request_id.copy_from_slice(&request_digest[..16]);
    let first = PostgresPageIdentity {
        cluster_id: [0x71; 16],
        tablespace_oid: 1663,
        database_oid: 5,
        relation_number: 16_402,
        temporary_backend_id: 0,
        fork_number: 0,
        block_number: 0,
    };
    let pages = [899_u64, 900]
        .into_iter()
        .enumerate()
        .map(|(offset, page_lsn)| {
            let byte = seed.to_le_bytes()[0].wrapping_add(u8::try_from(offset).unwrap_or(u8::MAX));
            PostgresPage::new(page_lsn, 0, vec![byte; POSTGRES_PAGE_SIZE])
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PostgresPageWriteBatch {
        first,
        expected_objectkv_version,
        postgres_wal_flush_lsn: 900,
        request_id,
        pages,
    })
}
