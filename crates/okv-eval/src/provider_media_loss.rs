//! Receipt contract for externally orchestrated provider-media-loss recovery.

use chrono::{DateTime, FixedOffset};
use serde::Deserialize;

const RECEIPT_KIND: &str = "foundationdb_objectkv_media_loss_r0";
const PROVIDER_REVISION: &str = "foundationdb-7.4.6@e77b64d4c5d01d240931c08c5384a834cae27337";
const HIDDEN_SOURCE_CONTROL: &str = "restore_with_hidden_source_media";

/// One named assertion emitted by the provider-media-loss controller.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Gate {
    pub id: String,
    pub passed: bool,
    pub detail: String,
}

/// One measured lifecycle phase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Timing {
    pub id: String,
    pub duration_ns: u64,
}

/// Identity of one FoundationDB cluster and its GCP media.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProviderIdentity {
    pub cluster_id: String,
    pub cluster_file_sha256: String,
    pub instance_name: String,
    pub instance_id: String,
    pub boot_disk_name: String,
    pub boot_disk_id: String,
    pub data_disk_name: String,
    pub data_disk_id: String,
}

/// Controller observations made after the source Terraform phase ended.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MediaLoss {
    pub observed_at: String,
    pub source_instance_absent: bool,
    pub source_boot_disk_absent: bool,
    pub source_data_disk_absent: bool,
    pub removed_before_restore: bool,
}

/// Exact immutable object authority used by the destination restore.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ObjectClosure {
    pub manifest_uri: String,
    pub manifest_generation: String,
    pub manifest_sha256: String,
    pub manifest_bytes: u64,
    pub closure_uri: String,
    pub closure_generation: String,
    pub closure_sha256: String,
    pub closure_bytes: u64,
    pub state_digest: String,
    pub through_provider_stamp: String,
    pub record_count: u64,
}

/// Destination observations made while reconstructing the object closure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Restore {
    pub started_at: String,
    pub finished_at: String,
    pub destination_empty_before_restore: bool,
    pub named_object_hashes_match: bool,
    pub restored_chunks: u64,
    pub replayed_chunks: u64,
    pub restored_record_count: u64,
    pub restored_state_digest: String,
    pub activated_after_ready: bool,
    pub fresh_commit_succeeded: bool,
    pub source_provider_inputs: Vec<String>,
}

/// Machine-readable result of the externally controlled GP2.5.3 lifecycle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema_version: u32,
    pub kind: String,
    pub provider: String,
    pub run_id: String,
    pub source: ProviderIdentity,
    pub destination: ProviderIdentity,
    pub media_loss: MediaLoss,
    pub object_closure: ObjectClosure,
    pub restore: Restore,
    pub correctness_anomalies: u64,
    pub media_loss_verified: bool,
    pub negative_control: Option<String>,
    pub gates: Vec<Gate>,
    pub timings: Vec<Timing>,
    pub scope: String,
}

impl Receipt {
    /// Parse the stable receipt schema and pinned provider identity.
    ///
    /// # Errors
    ///
    /// Returns a stable reason when bytes do not satisfy the R0 contract.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let receipt: Self = serde_json::from_slice(bytes)
            .map_err(|error| format!("parse provider-media-loss receipt: {error}"))?;
        if receipt.schema_version != 1 {
            return Err(format!(
                "provider-media-loss receipt schema {} is unsupported",
                receipt.schema_version
            ));
        }
        if receipt.kind != RECEIPT_KIND {
            return Err(format!(
                "unexpected provider-media-loss receipt kind {}",
                receipt.kind
            ));
        }
        if receipt.provider != PROVIDER_REVISION {
            return Err(format!(
                "unexpected provider-media-loss provider {}",
                receipt.provider
            ));
        }
        if receipt.run_id.is_empty() || receipt.scope.is_empty() {
            return Err("provider-media-loss receipt identity is empty".to_owned());
        }
        if receipt
            .negative_control
            .as_deref()
            .is_some_and(|control| control != HIDDEN_SOURCE_CONTROL)
        {
            return Err("provider-media-loss receipt names an unknown negative control".to_owned());
        }
        receipt.validate_shapes()?;
        Ok(receipt)
    }

    fn validate_shapes(&self) -> Result<(), String> {
        for (name, digest) in [
            ("source cluster file", &self.source.cluster_file_sha256),
            (
                "destination cluster file",
                &self.destination.cluster_file_sha256,
            ),
            ("manifest", &self.object_closure.manifest_sha256),
            ("closure", &self.object_closure.closure_sha256),
            ("state", &self.object_closure.state_digest),
            ("restored state", &self.restore.restored_state_digest),
        ] {
            if !is_lower_hex(digest, 64) {
                return Err(format!("{name} SHA-256 is malformed"));
            }
        }
        for (name, cluster_id) in [
            ("source", &self.source.cluster_id),
            ("destination", &self.destination.cluster_id),
        ] {
            if !is_lower_hex(cluster_id, 32) {
                return Err(format!("{name} cluster ID is malformed"));
            }
        }
        for (name, value) in [
            ("source instance name", &self.source.instance_name),
            ("source instance ID", &self.source.instance_id),
            ("source boot-disk name", &self.source.boot_disk_name),
            ("source boot-disk ID", &self.source.boot_disk_id),
            ("source data-disk name", &self.source.data_disk_name),
            ("source data-disk ID", &self.source.data_disk_id),
            ("destination instance name", &self.destination.instance_name),
            ("destination instance ID", &self.destination.instance_id),
            (
                "destination boot-disk name",
                &self.destination.boot_disk_name,
            ),
            ("destination boot-disk ID", &self.destination.boot_disk_id),
            (
                "destination data-disk name",
                &self.destination.data_disk_name,
            ),
            ("destination data-disk ID", &self.destination.data_disk_id),
        ] {
            if value.is_empty() {
                return Err(format!("{name} is empty"));
            }
        }
        parse_timestamp("media-loss observation", &self.media_loss.observed_at)?;
        parse_timestamp("restore start", &self.restore.started_at)?;
        parse_timestamp("restore finish", &self.restore.finished_at)?;
        if !self.object_closure.manifest_uri.starts_with("gs://")
            || !self.object_closure.closure_uri.starts_with("gs://")
        {
            return Err("provider-media-loss object URI is not a GCS URI".to_owned());
        }
        if !is_positive_decimal(&self.object_closure.manifest_generation)
            || !is_positive_decimal(&self.object_closure.closure_generation)
        {
            return Err("provider-media-loss GCS generation is malformed".to_owned());
        }
        if !is_lower_hex(&self.object_closure.through_provider_stamp, 20) {
            return Err("provider-media-loss provider stamp is malformed".to_owned());
        }
        if self.object_closure.manifest_bytes == 0
            || self.object_closure.closure_bytes == 0
            || self.object_closure.record_count == 0
        {
            return Err("provider-media-loss object closure is empty".to_owned());
        }
        let failed_gates = self.gates.iter().filter(|gate| !gate.passed).count();
        if u64::try_from(failed_gates).unwrap_or(u64::MAX) != self.correctness_anomalies {
            return Err("provider-media-loss anomaly count does not match failed gates".to_owned());
        }
        Ok(())
    }

    /// True only when the positive provider-media-loss subject passes every
    /// independently recomputable receipt invariant.
    #[must_use]
    pub fn candidate_passed(&self) -> bool {
        self.negative_control.is_none()
            && self.correctness_anomalies == 0
            && self.media_loss_verified
            && self.identities_are_distinct()
            && self.source_media_is_absent()
            && self.loss_precedes_restore()
            && self.restore_is_exact()
            && self.restore.source_provider_inputs.is_empty()
            && !self.gates.is_empty()
            && self.gates.iter().all(|gate| gate.passed)
    }

    /// True when the named poison executed an exact logical restore but failed
    /// the physical source-media-removal invariant.
    #[must_use]
    pub fn negative_control_detected(&self, expected: &str) -> bool {
        expected == HIDDEN_SOURCE_CONTROL
            && self.negative_control.as_deref() == Some(expected)
            && self.correctness_anomalies > 0
            && !self.media_loss_verified
            && !self.source_media_is_absent()
            && self.restore_is_exact()
            && !self.restore.source_provider_inputs.is_empty()
            && self.gates.iter().any(|gate| !gate.passed)
    }

    /// Find one measured phase in seconds.
    #[must_use]
    pub fn timing_seconds(&self, id: &str) -> Option<f64> {
        self.timings
            .iter()
            .find(|timing| timing.id == id)
            .map(|timing| std::time::Duration::from_nanos(timing.duration_ns).as_secs_f64())
    }

    fn identities_are_distinct(&self) -> bool {
        self.source.cluster_id != self.destination.cluster_id
            && self.source.cluster_file_sha256 != self.destination.cluster_file_sha256
            && self.source.instance_id != self.destination.instance_id
            && self.source.boot_disk_id != self.destination.boot_disk_id
            && self.source.data_disk_id != self.destination.data_disk_id
    }

    fn source_media_is_absent(&self) -> bool {
        self.media_loss.source_instance_absent
            && self.media_loss.source_boot_disk_absent
            && self.media_loss.source_data_disk_absent
            && self.media_loss.removed_before_restore
    }

    fn loss_precedes_restore(&self) -> bool {
        let Ok(observed_at) =
            parse_timestamp("media-loss observation", &self.media_loss.observed_at)
        else {
            return false;
        };
        let Ok(started_at) = parse_timestamp("restore start", &self.restore.started_at) else {
            return false;
        };
        let Ok(finished_at) = parse_timestamp("restore finish", &self.restore.finished_at) else {
            return false;
        };
        observed_at <= started_at && started_at <= finished_at
    }

    fn restore_is_exact(&self) -> bool {
        self.restore.destination_empty_before_restore
            && self.restore.named_object_hashes_match
            && self.restore.restored_chunks > 0
            && self.restore.replayed_chunks == self.restore.restored_chunks
            && self.restore.restored_record_count == self.object_closure.record_count
            && self.restore.restored_state_digest == self.object_closure.state_digest
            && self.restore.activated_after_ready
            && self.restore.fresh_commit_succeeded
    }
}

fn parse_timestamp(name: &str, timestamp: &str) -> Result<DateTime<FixedOffset>, String> {
    DateTime::parse_from_rfc3339(timestamp)
        .map_err(|error| format!("{name} timestamp is malformed: {error}"))
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_positive_decimal(value: &str) -> bool {
    !value.is_empty() && !value.starts_with('0') && value.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(prefix: &str) -> ProviderIdentity {
        let source = prefix == "source";
        ProviderIdentity {
            cluster_id: if source { "1" } else { "2" }.repeat(32),
            cluster_file_sha256: if source { "a" } else { "b" }.repeat(64),
            instance_name: format!("{prefix}-instance"),
            instance_id: format!("{prefix}-instance-id"),
            boot_disk_name: format!("{prefix}-boot"),
            boot_disk_id: format!("{prefix}-boot-id"),
            data_disk_name: format!("{prefix}-data"),
            data_disk_id: format!("{prefix}-data-id"),
        }
    }

    fn receipt() -> Receipt {
        Receipt {
            schema_version: 1,
            kind: RECEIPT_KIND.to_owned(),
            provider: PROVIDER_REVISION.to_owned(),
            run_id: "media-loss-1".to_owned(),
            source: identity("source"),
            destination: identity("destination"),
            media_loss: MediaLoss {
                observed_at: "2026-08-27T10:00:00Z".to_owned(),
                source_instance_absent: true,
                source_boot_disk_absent: true,
                source_data_disk_absent: true,
                removed_before_restore: true,
            },
            object_closure: ObjectClosure {
                manifest_uri: "gs://bucket/manifest.json".to_owned(),
                manifest_generation: "1".to_owned(),
                manifest_sha256: "c".repeat(64),
                manifest_bytes: 100,
                closure_uri: "gs://bucket/closure.json".to_owned(),
                closure_generation: "2".to_owned(),
                closure_sha256: "d".repeat(64),
                closure_bytes: 1_000,
                state_digest: "e".repeat(64),
                through_provider_stamp: "00000000000000000000".to_owned(),
                record_count: 10,
            },
            restore: Restore {
                started_at: "2026-08-27T10:01:00Z".to_owned(),
                finished_at: "2026-08-27T10:02:00Z".to_owned(),
                destination_empty_before_restore: true,
                named_object_hashes_match: true,
                restored_chunks: 2,
                replayed_chunks: 2,
                restored_record_count: 10,
                restored_state_digest: "e".repeat(64),
                activated_after_ready: true,
                fresh_commit_succeeded: true,
                source_provider_inputs: vec![],
            },
            correctness_anomalies: 0,
            media_loss_verified: true,
            negative_control: None,
            gates: vec![Gate {
                id: "all".to_owned(),
                passed: true,
                detail: "passed".to_owned(),
            }],
            timings: vec![Timing {
                id: "restore".to_owned(),
                duration_ns: 1_000_000_000,
            }],
            scope: "R0 physical media-loss reconstruction".to_owned(),
        }
    }

    #[test]
    fn positive_requires_distinct_identities_and_loss_before_restore() {
        let mut value = receipt();
        assert!(value.candidate_passed());
        value.destination.data_disk_id = value.source.data_disk_id.clone();
        assert!(!value.candidate_passed());
    }

    #[test]
    fn positive_rejects_restore_that_names_source_provider_input() {
        let mut value = receipt();
        value
            .restore
            .source_provider_inputs
            .push("source-cluster".to_owned());
        assert!(!value.candidate_passed());
    }

    #[test]
    fn hidden_source_poison_must_be_detected() {
        let mut value = receipt();
        value.negative_control = Some(HIDDEN_SOURCE_CONTROL.to_owned());
        value.correctness_anomalies = 1;
        value.media_loss_verified = false;
        value.media_loss.source_instance_absent = false;
        value.media_loss.source_boot_disk_absent = false;
        value.media_loss.source_data_disk_absent = false;
        value.media_loss.removed_before_restore = false;
        value
            .restore
            .source_provider_inputs
            .push("source-cluster".to_owned());
        value.gates[0].passed = false;
        assert!(value.negative_control_detected(HIDDEN_SOURCE_CONTROL));
        assert!(!value.candidate_passed());
    }
}
