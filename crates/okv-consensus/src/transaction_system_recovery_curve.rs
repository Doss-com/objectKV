use crate::multi_proxy_ordering::run_child_json;
use crate::{CellProcessFixture, CellProcessPrototypeMode};
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const INVENTORY_RECORD_BYTES: u64 = 40;
const INVENTORY_RECORD_BYTES_USIZE: usize = 40;
const GENERATION: u64 = 1;
const SUCCESSOR_GENERATION: u64 = 2;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Fault subjects for RFC-0054's isolated recovery curve.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionSystemRecoveryCurveMode {
    Correct,
    ScanPermanentDatabase,
    TrustOneTlogSet,
    QuadraticInventoryScan,
    ResumeBeforeRoleRecruitment,
}

impl TransactionSystemRecoveryCurveMode {
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Correct => "correct",
            Self::ScanPermanentDatabase => "scan_permanent_database",
            Self::TrustOneTlogSet => "trust_one_tlog_set",
            Self::QuadraticInventoryScan => "quadratic_inventory_scan",
            Self::ResumeBeforeRoleRecruitment => "resume_before_role_recruitment",
        }
    }
}

/// One frozen RFC-0054 curve point.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionSystemRecoveryCurveConfig {
    pub seed: u64,
    pub scale_class: String,
    pub pending_tickets: u64,
    pub retained_records_per_tlog: u64,
    pub commit_proxy_roles: u64,
    pub resolver_roles: u64,
    pub tlog_sets: u64,
    pub tlog_nodes_per_set: u64,
    pub logical_database_bytes: u64,
}

impl TransactionSystemRecoveryCurveConfig {
    fn validate(&self) -> Result<(), String> {
        if self.scale_class.is_empty()
            || self.pending_tickets == 0
            || self.retained_records_per_tlog == 0
            || self.commit_proxy_roles == 0
            || self.resolver_roles == 0
            || self.tlog_sets < 2
            || self.tlog_nodes_per_set < 3
            || self.logical_database_bytes == 0
        {
            return Err("transaction-system recovery curve configuration is incomplete".to_owned());
        }
        if self.pending_tickets > 4_096
            || self.retained_records_per_tlog > 262_144
            || self.commit_proxy_roles > 32
            || self.resolver_roles > 128
            || self.tlog_sets > 8
            || self.tlog_nodes_per_set > 9
        {
            return Err(
                "transaction-system recovery curve configuration exceeds the bounded eval envelope"
                    .to_owned(),
            );
        }
        Ok(())
    }

    fn tlog_nodes(&self) -> u64 {
        self.tlog_sets.saturating_mul(self.tlog_nodes_per_set)
    }

    fn successor_roles(&self) -> u64 {
        self.commit_proxy_roles
            .saturating_add(self.resolver_roles)
            .saturating_add(self.tlog_nodes())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryCurveRoleKind {
    CommitProxy,
    Resolver,
    Tlog,
}

/// One-shot child-process work for the recovery curve.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "task", rename_all = "snake_case")]
pub enum RecoveryCurveRoleConfig {
    WriteInventory {
        seed: u64,
        log_set_id: u16,
        node_id: u16,
        record_count: u64,
        output_path: PathBuf,
    },
    Ready {
        seed: u64,
        generation: u64,
        role: RecoveryCurveRoleKind,
        role_id: u16,
        log_set_id: Option<u16>,
    },
    ObserveFailure {
        seed: u64,
        generation: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecoveryCurveInventoryReceipt {
    pub format_version: u16,
    pub seed: u64,
    pub generation: u64,
    pub log_set_id: u16,
    pub node_id: u16,
    pub record_count: u64,
    pub byte_count: u64,
    pub inventory_sha256: [u8; 32],
    pub signature: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecoveryCurveReadyReceipt {
    pub format_version: u16,
    pub seed: u64,
    pub generation: u64,
    pub role: RecoveryCurveRoleKind,
    pub role_id: u16,
    pub log_set_id: Option<u16>,
    pub signature: Vec<u8>,
}

/// JSON response from one disposable RFC-0054 child process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "receipt", rename_all = "snake_case")]
pub enum RecoveryCurveRoleReceipt {
    Inventory(RecoveryCurveInventoryReceipt),
    Ready(RecoveryCurveReadyReceipt),
}

/// Deterministic receipt separated from wall-clock measurements.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionSystemRecoveryReceipt {
    pub seed: u64,
    pub scale_class: String,
    pub old_generation: u64,
    pub successor_generation: u64,
    pub old_issued_high_watermark: u64,
    pub recovered_boundary: u64,
    pub pending_tickets_classified: u64,
    pub successor_roles_recruited: u64,
    pub authenticated_inventory_receipts: u64,
    pub inventory_bytes_examined: u64,
    pub inventory_work_units: u64,
    pub pending_work_units: u64,
    pub permanent_database_bytes_read: u64,
    pub old_generation_fenced_before_inventory: bool,
    pub every_required_tlog_set_authenticated: bool,
    pub recovered_boundary_is_maximal_contiguous_quorum_prefix: bool,
    pub all_declared_successor_roles_recruited_before_resume: bool,
    pub successor_version_exceeds_old_issued_high_watermark: bool,
    pub inventory_scan_is_linear: bool,
    pub pending_classification_is_linear: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TransactionSystemRecoverySample {
    pub total_seconds: f64,
    pub phase_seconds: BTreeMap<String, f64>,
}

/// RFC-0054 report for one seed and curve point.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TransactionSystemRecoveryCurveReport {
    pub config: TransactionSystemRecoveryCurveConfig,
    pub mode: TransactionSystemRecoveryCurveMode,
    pub repetitions: u32,
    pub samples: Vec<TransactionSystemRecoverySample>,
    pub receipt: TransactionSystemRecoveryReceipt,
    pub exact_untimed_receipts: bool,
    pub phase_receipts_complete: bool,
    pub anomaly_count: u64,
    pub negative_control_detected: bool,
    pub first_mismatch: Option<String>,
    pub trace_sha256: String,
}

/// Execute one disposable inventory, readiness, or failure-observation process.
///
/// # Errors
///
/// Returns an error when the configuration or durable inventory is invalid. The
/// failure-observation task intentionally returns an error after observing loss.
pub fn run_recovery_curve_role_process(
    config: &RecoveryCurveRoleConfig,
) -> Result<RecoveryCurveRoleReceipt, String> {
    match config {
        RecoveryCurveRoleConfig::WriteInventory {
            seed,
            log_set_id,
            node_id,
            record_count,
            output_path,
        } => write_inventory(*seed, *log_set_id, *node_id, *record_count, output_path),
        RecoveryCurveRoleConfig::Ready {
            seed,
            generation,
            role,
            role_id,
            log_set_id,
        } => {
            if *generation != SUCCESSOR_GENERATION || *role_id == 0 {
                return Err("successor role identity is invalid".to_owned());
            }
            let mut receipt = RecoveryCurveReadyReceipt {
                format_version: 1,
                seed: *seed,
                generation: *generation,
                role: *role,
                role_id: *role_id,
                log_set_id: *log_set_id,
                signature: Vec::new(),
            };
            let bytes = serde_json::to_vec(&receipt).map_err(|error| error.to_string())?;
            receipt.signature = ready_key_pair(&receipt)?.sign(&bytes).as_ref().to_vec();
            Ok(RecoveryCurveRoleReceipt::Ready(receipt))
        }
        RecoveryCurveRoleConfig::ObserveFailure { seed, generation } => Err(format!(
            "observed commit-proxy loss: seed={seed}, generation={generation}"
        )),
    }
}

/// Run the frozen RFC-0054 curve point.
///
/// # Errors
///
/// Returns an error when the authority, role processes, or receipt validation
/// cannot complete.
pub fn run_transaction_system_recovery_curve_contract(
    config: &TransactionSystemRecoveryCurveConfig,
    mode: TransactionSystemRecoveryCurveMode,
    repetitions: u32,
    executable: &Path,
) -> Result<TransactionSystemRecoveryCurveReport, String> {
    config.validate()?;
    if !(1..=100).contains(&repetitions) {
        return Err("recovery curve repetitions must be between 1 and 100".to_owned());
    }
    if !executable.is_file() {
        return Err("recovery curve executable is absent".to_owned());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(run_curve(config, mode, repetitions, executable))
}

#[allow(clippy::too_many_lines)]
async fn run_curve(
    config: &TransactionSystemRecoveryCurveConfig,
    mode: TransactionSystemRecoveryCurveMode,
    repetitions: u32,
    executable: &Path,
) -> Result<TransactionSystemRecoveryCurveReport, String> {
    let root = TempRoot::new(config.seed, &config.scale_class, mode)?;
    let database_path = root.path().join("permanent-database.sparse");
    let database = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&database_path)
        .map_err(|error| error.to_string())?;
    database
        .set_len(config.logical_database_bytes)
        .map_err(|error| format!("cannot create sparse logical database extent: {error}"))?;
    database.sync_all().map_err(|error| error.to_string())?;
    drop(database);

    let inventory_paths = prepare_inventories(config, root.path(), executable)?;
    let mut authority = CellProcessFixture::start(
        config.seed ^ 0x0054_0054,
        CellProcessPrototypeMode::Correct,
        executable,
    )?;
    authority.run_history().await?;

    let mut samples = Vec::with_capacity(repetitions as usize);
    let mut receipts = Vec::with_capacity(repetitions as usize);
    for repetition in 0..repetitions {
        let (sample, receipt) = run_one_recovery(
            config,
            mode,
            repetition,
            executable,
            &database_path,
            &inventory_paths,
            &authority,
        )
        .await?;
        samples.push(sample);
        receipts.push(receipt);
    }

    let receipt = receipts
        .first()
        .cloned()
        .ok_or_else(|| "recovery curve produced no receipt".to_owned())?;
    let exact_untimed_receipts = receipts.iter().all(|candidate| candidate == &receipt);
    let expected_phases = [
        "failure_observation",
        "generation_fence",
        "tlog_inventory",
        "role_recruitment",
        "successor_admission",
    ];
    let phase_receipts_complete = samples.iter().all(|sample| {
        sample.phase_seconds.len() == expected_phases.len()
            && expected_phases
                .iter()
                .all(|phase| sample.phase_seconds.contains_key(*phase))
    });
    let checks = [
        ("exact_untimed_receipts", exact_untimed_receipts),
        ("phase_receipts_complete", phase_receipts_complete),
        (
            "old_generation_fenced_before_inventory",
            receipt.old_generation_fenced_before_inventory,
        ),
        (
            "every_required_tlog_set_authenticated",
            receipt.every_required_tlog_set_authenticated,
        ),
        (
            "maximal_contiguous_quorum_prefix",
            receipt.recovered_boundary_is_maximal_contiguous_quorum_prefix,
        ),
        (
            "roles_recruited_before_resume",
            receipt.all_declared_successor_roles_recruited_before_resume,
        ),
        (
            "successor_version_floor",
            receipt.successor_version_exceeds_old_issued_high_watermark,
        ),
        (
            "zero_permanent_database_reads",
            receipt.permanent_database_bytes_read == 0,
        ),
        ("linear_inventory_scan", receipt.inventory_scan_is_linear),
        (
            "linear_pending_classification",
            receipt.pending_classification_is_linear,
        ),
    ];
    let failures = checks
        .iter()
        .filter(|(_, passed)| !*passed)
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    let anomaly_count = failures.len() as u64;
    let negative_control_detected =
        mode != TransactionSystemRecoveryCurveMode::Correct && anomaly_count > 0;
    let trace_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&receipt).map_err(|error| error.to_string())?)
    );
    Ok(TransactionSystemRecoveryCurveReport {
        config: config.clone(),
        mode,
        repetitions,
        samples,
        receipt,
        exact_untimed_receipts,
        phase_receipts_complete,
        anomaly_count,
        negative_control_detected,
        first_mismatch: failures.first().map(|failure| (*failure).to_owned()),
        trace_sha256,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_one_recovery(
    config: &TransactionSystemRecoveryCurveConfig,
    mode: TransactionSystemRecoveryCurveMode,
    repetition: u32,
    executable: &Path,
    database_path: &Path,
    inventory_paths: &[(PathBuf, PathBuf)],
    authority: &CellProcessFixture<'_>,
) -> Result<
    (
        TransactionSystemRecoverySample,
        TransactionSystemRecoveryReceipt,
    ),
    String,
> {
    let total_started = Instant::now();
    let mut phase_seconds = BTreeMap::new();

    let phase_started = Instant::now();
    observe_expected_failure(config.seed, executable)?;
    phase_seconds.insert(
        "failure_observation".to_owned(),
        phase_started.elapsed().as_secs_f64(),
    );

    let old_issued_high_watermark = config
        .retained_records_per_tlog
        .saturating_add(config.pending_tickets);
    let marker_base = 54_000_000_u64
        .saturating_add((config.seed % 10_000).saturating_mul(1_000))
        .saturating_add(u64::from(repetition).saturating_mul(2));
    let phase_started = Instant::now();
    let fence = serde_json::to_vec(&(
        "transaction-system-generation-fence-v1",
        config.seed,
        GENERATION,
        SUCCESSOR_GENERATION,
        old_issued_high_watermark,
    ))
    .map_err(|error| error.to_string())?;
    authority
        .replicate_sequencer_marker(marker_base, &fence)
        .await?;
    phase_seconds.insert(
        "generation_fence".to_owned(),
        phase_started.elapsed().as_secs_f64(),
    );

    let phase_started = Instant::now();
    let inventory = inspect_inventories(config, mode, inventory_paths)?;
    let pending_work_units = classify_pending(config.pending_tickets, mode);
    let permanent_database_bytes_read =
        if mode == TransactionSystemRecoveryCurveMode::ScanPermanentDatabase {
            read_database_probe(database_path)?
        } else {
            0
        };
    phase_seconds.insert(
        "tlog_inventory".to_owned(),
        phase_started.elapsed().as_secs_f64(),
    );

    let early_resume = mode == TransactionSystemRecoveryCurveMode::ResumeBeforeRoleRecruitment;
    let mut recruited = 0_u64;
    let phase_started = Instant::now();
    if !early_resume {
        recruited = recruit_successor_roles(config, executable)?;
    }
    phase_seconds.insert(
        "role_recruitment".to_owned(),
        phase_started.elapsed().as_secs_f64(),
    );

    let successor_version = old_issued_high_watermark.saturating_add(1);
    let phase_started = Instant::now();
    let admission = serde_json::to_vec(&(
        "transaction-system-successor-admission-v1",
        config.seed,
        SUCCESSOR_GENERATION,
        successor_version,
        recruited,
    ))
    .map_err(|error| error.to_string())?;
    authority
        .replicate_sequencer_marker(marker_base.saturating_add(1), &admission)
        .await?;
    phase_seconds.insert(
        "successor_admission".to_owned(),
        phase_started.elapsed().as_secs_f64(),
    );

    if early_resume {
        recruited = recruit_successor_roles(config, executable)?;
    }
    let total_seconds = total_started.elapsed().as_secs_f64();
    let expected_inventory_work = inventory
        .authenticated_receipts
        .saturating_add(inventory.records_examined);
    let receipt = TransactionSystemRecoveryReceipt {
        seed: config.seed,
        scale_class: config.scale_class.clone(),
        old_generation: GENERATION,
        successor_generation: SUCCESSOR_GENERATION,
        old_issued_high_watermark,
        recovered_boundary: inventory.recovered_boundary,
        pending_tickets_classified: config.pending_tickets,
        successor_roles_recruited: recruited,
        authenticated_inventory_receipts: inventory.authenticated_receipts,
        inventory_bytes_examined: inventory.bytes_examined,
        inventory_work_units: inventory.work_units,
        pending_work_units,
        permanent_database_bytes_read,
        old_generation_fenced_before_inventory: true,
        every_required_tlog_set_authenticated: inventory.required_sets_authenticated,
        recovered_boundary_is_maximal_contiguous_quorum_prefix: inventory.recovered_boundary
            == config.retained_records_per_tlog,
        all_declared_successor_roles_recruited_before_resume: !early_resume
            && recruited == config.successor_roles(),
        successor_version_exceeds_old_issued_high_watermark: successor_version
            > old_issued_high_watermark,
        inventory_scan_is_linear: inventory.work_units == expected_inventory_work,
        pending_classification_is_linear: pending_work_units == config.pending_tickets,
    };
    Ok((
        TransactionSystemRecoverySample {
            total_seconds,
            phase_seconds,
        },
        receipt,
    ))
}

struct InventoryObservation {
    recovered_boundary: u64,
    authenticated_receipts: u64,
    records_examined: u64,
    bytes_examined: u64,
    work_units: u64,
    required_sets_authenticated: bool,
}

fn prepare_inventories(
    config: &TransactionSystemRecoveryCurveConfig,
    root: &Path,
    executable: &Path,
) -> Result<Vec<(PathBuf, PathBuf)>, String> {
    let mut paths = Vec::new();
    for set_offset in 0..config.tlog_sets {
        let log_set_id = u16::try_from(set_offset.saturating_add(1))
            .map_err(|_| "tLog set identity exceeds u16".to_owned())?;
        for node_offset in 0..config.tlog_nodes_per_set {
            let node_id = u16::try_from(node_offset.saturating_add(1))
                .map_err(|_| "tLog node identity exceeds u16".to_owned())?;
            let record_count = if node_offset.saturating_add(1) == config.tlog_nodes_per_set {
                config
                    .retained_records_per_tlog
                    .saturating_sub(config.retained_records_per_tlog / 8)
            } else {
                config.retained_records_per_tlog
            };
            let data_path = root.join(format!("inventory-{log_set_id}-{node_id}.bin"));
            let receipt_path = root.join(format!("inventory-{log_set_id}-{node_id}.json"));
            let role = RecoveryCurveRoleConfig::WriteInventory {
                seed: config.seed,
                log_set_id,
                node_id,
                record_count,
                output_path: data_path.clone(),
            };
            let response: RecoveryCurveRoleReceipt =
                run_child_json(executable, "recovery-curve-role-node", &role)?;
            let RecoveryCurveRoleReceipt::Inventory(receipt) = response else {
                return Err("inventory writer returned a readiness receipt".to_owned());
            };
            write_json_sync(&receipt_path, &receipt)?;
            paths.push((data_path, receipt_path));
        }
    }
    Ok(paths)
}

fn inspect_inventories(
    config: &TransactionSystemRecoveryCurveConfig,
    mode: TransactionSystemRecoveryCurveMode,
    paths: &[(PathBuf, PathBuf)],
) -> Result<InventoryObservation, String> {
    let trusted_sets = if mode == TransactionSystemRecoveryCurveMode::TrustOneTlogSet {
        1
    } else {
        config.tlog_sets
    };
    let mut frontiers: BTreeMap<u16, Vec<u64>> = BTreeMap::new();
    let mut authenticated_receipts = 0_u64;
    let mut records_examined = 0_u64;
    let mut bytes_examined = 0_u64;
    for (data_path, receipt_path) in paths {
        let receipt_bytes = fs::read(receipt_path).map_err(|error| error.to_string())?;
        bytes_examined =
            bytes_examined.saturating_add(u64::try_from(receipt_bytes.len()).unwrap_or(u64::MAX));
        let receipt: RecoveryCurveInventoryReceipt =
            serde_json::from_slice(&receipt_bytes).map_err(|error| error.to_string())?;
        if u64::from(receipt.log_set_id) > trusted_sets {
            continue;
        }
        if !verify_inventory_receipt(&receipt) {
            return Err("tLog inventory receipt signature is invalid".to_owned());
        }
        let observed = verify_inventory_file(data_path, &receipt)?;
        records_examined = records_examined.saturating_add(receipt.record_count);
        bytes_examined = bytes_examined.saturating_add(observed);
        authenticated_receipts = authenticated_receipts.saturating_add(1);
        frontiers
            .entry(receipt.log_set_id)
            .or_default()
            .push(receipt.record_count);
    }
    let quorum_index = usize::try_from(config.tlog_nodes_per_set / 2)
        .map_err(|_| "tLog quorum index exceeds usize".to_owned())?;
    let mut set_frontiers = Vec::new();
    for frontiers_for_set in frontiers.values_mut() {
        frontiers_for_set.sort_unstable_by(|left, right| right.cmp(left));
        set_frontiers.push(*frontiers_for_set.get(quorum_index).unwrap_or(&0));
    }
    let recovered_boundary = set_frontiers.into_iter().min().unwrap_or(0);
    let mut work_units = authenticated_receipts.saturating_add(records_examined);
    if mode == TransactionSystemRecoveryCurveMode::QuadraticInventoryScan {
        let quadratic_span = config.retained_records_per_tlog.min(512);
        let mut comparisons = 0_u64;
        for left in 0..quadratic_span {
            for right in 0..quadratic_span {
                comparisons = comparisons.saturating_add(u64::from(left <= right));
            }
        }
        work_units = work_units.saturating_add(comparisons);
    }
    Ok(InventoryObservation {
        recovered_boundary,
        authenticated_receipts,
        records_examined,
        bytes_examined,
        work_units,
        required_sets_authenticated: frontiers.len()
            == usize::try_from(config.tlog_sets).unwrap_or(usize::MAX),
    })
}

fn recruit_successor_roles(
    config: &TransactionSystemRecoveryCurveConfig,
    executable: &Path,
) -> Result<u64, String> {
    let mut recruited = 0_u64;
    for role_id in 1..=config.commit_proxy_roles {
        recruit_role(
            config.seed,
            RecoveryCurveRoleKind::CommitProxy,
            role_id,
            None,
            executable,
        )?;
        recruited = recruited.saturating_add(1);
    }
    for role_id in 1..=config.resolver_roles {
        recruit_role(
            config.seed,
            RecoveryCurveRoleKind::Resolver,
            role_id,
            None,
            executable,
        )?;
        recruited = recruited.saturating_add(1);
    }
    for set_offset in 0..config.tlog_sets {
        let set_id = u16::try_from(set_offset.saturating_add(1))
            .map_err(|_| "successor tLog set identity exceeds u16".to_owned())?;
        for node_offset in 0..config.tlog_nodes_per_set {
            let role_id = set_offset
                .saturating_mul(config.tlog_nodes_per_set)
                .saturating_add(node_offset)
                .saturating_add(1);
            recruit_role(
                config.seed,
                RecoveryCurveRoleKind::Tlog,
                role_id,
                Some(set_id),
                executable,
            )?;
            recruited = recruited.saturating_add(1);
        }
    }
    Ok(recruited)
}

fn recruit_role(
    seed: u64,
    role: RecoveryCurveRoleKind,
    role_id: u64,
    log_set_id: Option<u16>,
    executable: &Path,
) -> Result<(), String> {
    let role_id =
        u16::try_from(role_id).map_err(|_| "successor role identity exceeds u16".to_owned())?;
    let config = RecoveryCurveRoleConfig::Ready {
        seed,
        generation: SUCCESSOR_GENERATION,
        role,
        role_id,
        log_set_id,
    };
    let response: RecoveryCurveRoleReceipt =
        run_child_json(executable, "recovery-curve-role-node", &config)?;
    let RecoveryCurveRoleReceipt::Ready(receipt) = response else {
        return Err("successor role returned an inventory receipt".to_owned());
    };
    if receipt.seed != seed
        || receipt.role != role
        || receipt.role_id != role_id
        || receipt.log_set_id != log_set_id
        || !verify_ready_receipt(&receipt)
    {
        return Err("successor role readiness receipt is invalid".to_owned());
    }
    Ok(())
}

fn observe_expected_failure(seed: u64, executable: &Path) -> Result<(), String> {
    let config = RecoveryCurveRoleConfig::ObserveFailure {
        seed,
        generation: GENERATION,
    };
    let config_json = serde_json::to_string(&config).map_err(|error| error.to_string())?;
    let output = Command::new(executable)
        .arg("recovery-curve-role-node")
        .arg("--config-json")
        .arg(config_json)
        .output()
        .map_err(|error| format!("failed to observe role loss: {error}"))?;
    if output.status.success()
        || !String::from_utf8_lossy(&output.stderr).contains("observed commit-proxy loss")
    {
        return Err("failure observer did not report the frozen role loss".to_owned());
    }
    Ok(())
}

fn write_inventory(
    seed: u64,
    log_set_id: u16,
    node_id: u16,
    record_count: u64,
    output_path: &Path,
) -> Result<RecoveryCurveRoleReceipt, String> {
    if log_set_id == 0 || node_id == 0 || record_count == 0 {
        return Err("inventory writer identity is invalid".to_owned());
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output_path)
        .map_err(|error| error.to_string())?;
    let mut inventory_hash = Sha256::new();
    for version in 1..=record_count {
        let version_bytes = version.to_be_bytes();
        let record_digest = inventory_record_digest(seed, log_set_id, node_id, version);
        file.write_all(&version_bytes)
            .and_then(|()| file.write_all(&record_digest))
            .map_err(|error| error.to_string())?;
        inventory_hash.update(version_bytes);
        inventory_hash.update(record_digest);
    }
    file.sync_all().map_err(|error| error.to_string())?;
    let mut receipt = RecoveryCurveInventoryReceipt {
        format_version: 1,
        seed,
        generation: GENERATION,
        log_set_id,
        node_id,
        record_count,
        byte_count: record_count.saturating_mul(INVENTORY_RECORD_BYTES),
        inventory_sha256: inventory_hash.finalize().into(),
        signature: Vec::new(),
    };
    let bytes = serde_json::to_vec(&receipt).map_err(|error| error.to_string())?;
    receipt.signature = inventory_key_pair(seed, log_set_id, node_id)?
        .sign(&bytes)
        .as_ref()
        .to_vec();
    Ok(RecoveryCurveRoleReceipt::Inventory(receipt))
}

fn verify_inventory_file(
    path: &Path,
    receipt: &RecoveryCurveInventoryReceipt,
) -> Result<u64, String> {
    let mut file = File::open(path).map_err(|error| error.to_string())?;
    let mut hash = Sha256::new();
    let mut record = [0_u8; INVENTORY_RECORD_BYTES_USIZE];
    for expected_version in 1..=receipt.record_count {
        file.read_exact(&mut record)
            .map_err(|error| error.to_string())?;
        let version = u64::from_be_bytes(
            record[..8]
                .try_into()
                .map_err(|_| "inventory version frame is invalid".to_owned())?,
        );
        let expected_digest = inventory_record_digest(
            receipt.seed,
            receipt.log_set_id,
            receipt.node_id,
            expected_version,
        );
        if version != expected_version || record[8..] != expected_digest {
            return Err("tLog inventory record is not exact and contiguous".to_owned());
        }
        hash.update(record);
    }
    let mut extra = [0_u8; 1];
    if file.read(&mut extra).map_err(|error| error.to_string())? != 0
        || receipt.byte_count != receipt.record_count.saturating_mul(INVENTORY_RECORD_BYTES)
        || <[u8; 32]>::from(hash.finalize()) != receipt.inventory_sha256
    {
        return Err("tLog inventory file does not match its signed receipt".to_owned());
    }
    Ok(receipt.byte_count)
}

fn verify_inventory_receipt(receipt: &RecoveryCurveInventoryReceipt) -> bool {
    let Ok(key) = inventory_key_pair(receipt.seed, receipt.log_set_id, receipt.node_id) else {
        return false;
    };
    let mut unsigned = receipt.clone();
    let signature = std::mem::take(&mut unsigned.signature);
    let Ok(bytes) = serde_json::to_vec(&unsigned) else {
        return false;
    };
    UnparsedPublicKey::new(&ED25519, key.public_key().as_ref())
        .verify(&bytes, &signature)
        .is_ok()
}

fn verify_ready_receipt(receipt: &RecoveryCurveReadyReceipt) -> bool {
    let Ok(key) = ready_key_pair(receipt) else {
        return false;
    };
    let mut unsigned = receipt.clone();
    let signature = std::mem::take(&mut unsigned.signature);
    let Ok(bytes) = serde_json::to_vec(&unsigned) else {
        return false;
    };
    UnparsedPublicKey::new(&ED25519, key.public_key().as_ref())
        .verify(&bytes, &signature)
        .is_ok()
}

fn inventory_key_pair(seed: u64, log_set_id: u16, node_id: u16) -> Result<Ed25519KeyPair, String> {
    key_pair(&(
        b"okv-eval-rfc0054-inventory-key-v1".as_slice(),
        seed,
        log_set_id,
        node_id,
    ))
}

fn ready_key_pair(receipt: &RecoveryCurveReadyReceipt) -> Result<Ed25519KeyPair, String> {
    key_pair(&(
        b"okv-eval-rfc0054-ready-key-v1".as_slice(),
        receipt.seed,
        receipt.generation,
        receipt.role,
        receipt.role_id,
        receipt.log_set_id,
    ))
}

fn key_pair<T: Serialize>(identity: &T) -> Result<Ed25519KeyPair, String> {
    let bytes = serde_json::to_vec(identity).map_err(|error| error.to_string())?;
    let seed: [u8; 32] = Sha256::digest(bytes).into();
    Ed25519KeyPair::from_seed_unchecked(&seed)
        .map_err(|_| "recovery curve signing seed is invalid".to_owned())
}

fn inventory_record_digest(seed: u64, log_set_id: u16, node_id: u16, version: u64) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"okv-eval-rfc0054-inventory-record-v1");
    hash.update(seed.to_be_bytes());
    hash.update(log_set_id.to_be_bytes());
    hash.update(node_id.to_be_bytes());
    hash.update(version.to_be_bytes());
    hash.finalize().into()
}

fn classify_pending(pending_tickets: u64, mode: TransactionSystemRecoveryCurveMode) -> u64 {
    let mut work = 0_u64;
    for _ in 0..pending_tickets {
        work = work.saturating_add(1);
    }
    if mode == TransactionSystemRecoveryCurveMode::QuadraticInventoryScan {
        for left in 0..pending_tickets {
            for right in 0..pending_tickets {
                work = work.saturating_add(u64::from(left <= right));
            }
        }
    }
    work
}

fn read_database_probe(path: &Path) -> Result<u64, String> {
    let mut file = File::open(path).map_err(|error| error.to_string())?;
    let mut bytes = [0_u8; 4_096];
    file.read_exact(&mut bytes)
        .map_err(|error| error.to_string())?;
    Ok(bytes.len() as u64)
}

fn write_json_sync<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(&bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

struct TempRoot(PathBuf);

impl TempRoot {
    fn new(
        seed: u64,
        scale_class: &str,
        mode: TransactionSystemRecoveryCurveMode,
    ) -> Result<Self, String> {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "okv-recovery-curve-{}-{seed}-{scale_class}-{}-{sequence}",
            std::process::id(),
            mode.id()
        ));
        fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_large_point_remains_inside_eval_envelope() {
        let config = TransactionSystemRecoveryCurveConfig {
            seed: 1103,
            scale_class: "large".to_owned(),
            pending_tickets: 512,
            retained_records_per_tlog: 65_536,
            commit_proxy_roles: 3,
            resolver_roles: 33,
            tlog_sets: 4,
            tlog_nodes_per_set: 5,
            logical_database_bytes: 1_125_899_906_842_624,
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.successor_roles(), 56);
    }

    #[test]
    fn inventory_receipt_signature_detects_mutation() {
        let root = std::env::temp_dir().join(format!(
            "okv-rfc0054-unit-{}-{}",
            std::process::id(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let response = write_inventory(1103, 1, 1, 4, &root).expect("write inventory");
        let RecoveryCurveRoleReceipt::Inventory(mut receipt) = response else {
            panic!("expected inventory receipt");
        };
        assert!(verify_inventory_receipt(&receipt));
        receipt.record_count += 1;
        assert!(!verify_inventory_receipt(&receipt));
        let _ = fs::remove_file(root);
    }
}
