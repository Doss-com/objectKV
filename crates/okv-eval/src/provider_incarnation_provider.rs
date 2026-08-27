//! Receipt contract for a resurrected FoundationDB provider incarnation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

const RECEIPT_KIND: &str = "foundationdb_objectkv_provider_incarnation_r0";
const PROVIDER: &str = "foundationdb-7.4.6@e77b64d4c5d01d240931c08c5384a834cae27337";
const NEGATIVE_CONTROL: &str = "accept_stale_source_incarnation";

/// One GCP and FoundationDB provider identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

/// Named immutable closure reconstructed by the destination.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

/// Provider-local fence observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceFence {
    pub started_at: String,
    pub finished_at: String,
    pub fence_value: String,
    pub fence_provider_stamp: String,
    pub concurrent_stale_commit_error_code: u64,
    pub concurrent_stale_commit_rejected: bool,
    pub post_fence_adapter_commit_rejected: bool,
}

/// Destination activation observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Activation {
    pub started_at: String,
    pub finished_at: String,
    pub authority_trace_sha256: String,
    pub source_fence_provider_stamp: String,
    pub state_digest: String,
    pub fresh_commit_succeeded: bool,
    pub fresh_commit_provider_stamp: String,
}

/// Probe executed after the source VM restarts with the same media.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Resurrection {
    pub probed_at: String,
    pub fence_value: String,
    pub fence_provider_stamp: String,
    pub fence_persisted: bool,
    pub stale_source_adapter_commit_rejected: bool,
}

/// Source VM stop/start observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Restart {
    pub stopped_at: String,
    pub started_at: String,
    pub stop_succeeded: bool,
    pub start_succeeded: bool,
    pub identities_retained: bool,
}

/// One named provider-incarnation assertion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Gate {
    pub id: String,
    pub passed: bool,
    pub detail: String,
}

/// Controller-assembled real-provider receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Receipt {
    pub schema_version: u64,
    pub kind: String,
    pub provider: String,
    pub run_id: String,
    pub source: ProviderIdentity,
    pub destination: ProviderIdentity,
    pub object_closure: ObjectClosure,
    pub authority_trace_sha256: String,
    pub source_fence: Option<SourceFence>,
    pub activation: Option<Activation>,
    pub resurrection: Option<Resurrection>,
    pub restart: Option<Restart>,
    pub correctness_anomalies: u64,
    pub incarnation_fencing_verified: bool,
    pub negative_control: Option<String>,
    pub gates: Vec<Gate>,
    pub scope: String,
}

impl Receipt {
    /// Decode and validate one receipt.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let receipt = serde_json::from_slice::<Self>(bytes)
            .map_err(|error| format!("parse provider-incarnation receipt: {error}"))?;
        receipt.validate()?;
        Ok(receipt)
    }

    fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err(format!(
                "provider-incarnation receipt schema {} is unsupported",
                self.schema_version
            ));
        }
        if self.kind != RECEIPT_KIND {
            return Err(format!(
                "unexpected provider-incarnation receipt kind {}",
                self.kind
            ));
        }
        if self.provider != PROVIDER {
            return Err(format!(
                "unexpected provider-incarnation provider {}",
                self.provider
            ));
        }
        if self.run_id.trim().is_empty() || self.scope.trim().is_empty() {
            return Err("provider-incarnation receipt identity or scope is empty".to_owned());
        }
        if self
            .negative_control
            .as_deref()
            .is_some_and(|control| control != NEGATIVE_CONTROL)
        {
            return Err("provider-incarnation receipt names an unknown control".to_owned());
        }
        validate_hex("authority trace", &self.authority_trace_sha256, 64)?;
        validate_hex("state digest", &self.object_closure.state_digest, 64)?;
        validate_hex("closure digest", &self.object_closure.closure_sha256, 64)?;
        validate_hex("manifest digest", &self.object_closure.manifest_sha256, 64)?;
        validate_hex("source cluster", &self.source.cluster_id, 32)?;
        validate_hex("destination cluster", &self.destination.cluster_id, 32)?;
        if !self.object_closure.manifest_uri.starts_with("gs://")
            || !self.object_closure.closure_uri.starts_with("gs://")
            || self.object_closure.record_count == 0
            || self.object_closure.closure_bytes == 0
            || self.object_closure.manifest_bytes == 0
        {
            return Err("provider-incarnation object closure is malformed".to_owned());
        }
        let failures = self.gates.iter().filter(|gate| !gate.passed).count();
        let failures = u64::try_from(failures).unwrap_or(u64::MAX);
        if failures != self.correctness_anomalies {
            return Err("provider-incarnation anomaly count does not match gates".to_owned());
        }
        if self.negative_control.is_none() {
            self.validate_positive_timeline()?;
        }
        Ok(())
    }

    fn validate_positive_timeline(&self) -> Result<(), String> {
        let fence = self
            .source_fence
            .as_ref()
            .ok_or_else(|| "positive receipt has no source fence".to_owned())?;
        let activation = self
            .activation
            .as_ref()
            .ok_or_else(|| "positive receipt has no activation".to_owned())?;
        let resurrection = self
            .resurrection
            .as_ref()
            .ok_or_else(|| "positive receipt has no resurrection".to_owned())?;
        let restart = self
            .restart
            .as_ref()
            .ok_or_else(|| "positive receipt has no restart".to_owned())?;
        for (name, value) in [
            ("fence start", fence.started_at.as_str()),
            ("fence finish", fence.finished_at.as_str()),
            ("activation start", activation.started_at.as_str()),
            ("activation finish", activation.finished_at.as_str()),
            ("resurrection probe", resurrection.probed_at.as_str()),
            ("restart stop", restart.stopped_at.as_str()),
            ("restart start", restart.started_at.as_str()),
        ] {
            parse_timestamp(name, value)?;
        }
        if parse_timestamp("fence finish", &fence.finished_at)?
            > parse_timestamp("activation start", &activation.started_at)?
            || parse_timestamp("activation finish", &activation.finished_at)?
                > parse_timestamp("resurrection probe", &resurrection.probed_at)?
        {
            return Err("provider-incarnation receipt timeline is out of order".to_owned());
        }
        for (name, value) in [
            ("fence provider stamp", fence.fence_provider_stamp.as_str()),
            (
                "activation provider stamp",
                activation.fresh_commit_provider_stamp.as_str(),
            ),
            (
                "resurrection fence stamp",
                resurrection.fence_provider_stamp.as_str(),
            ),
        ] {
            validate_hex(name, value, 20)?;
        }
        Ok(())
    }

    /// True only when every real-provider gate passes.
    #[must_use]
    pub fn positive_passed(&self) -> bool {
        self.negative_control.is_none()
            && self.incarnation_fencing_verified
            && self.correctness_anomalies == 0
            && self.required_gates_pass()
            && self.source.cluster_id != self.destination.cluster_id
            && self.source.instance_id != self.destination.instance_id
            && self.source.data_disk_id != self.destination.data_disk_id
            && self.source_fence.as_ref().is_some_and(|fence| {
                fence.concurrent_stale_commit_error_code == 1020
                    && fence.concurrent_stale_commit_rejected
                    && fence.post_fence_adapter_commit_rejected
            })
            && self.activation.as_ref().is_some_and(|activation| {
                activation.fresh_commit_succeeded
                    && activation.state_digest == self.object_closure.state_digest
                    && activation.authority_trace_sha256 == self.authority_trace_sha256
            })
            && self.resurrection.as_ref().is_some_and(|resurrection| {
                resurrection.fence_persisted && resurrection.stale_source_adapter_commit_rejected
            })
            && self.restart.as_ref().is_some_and(|restart| {
                restart.stop_succeeded && restart.start_succeeded && restart.identities_retained
            })
    }

    /// True when the real unfenced-source control is rejected for all three surfaces.
    #[must_use]
    pub fn negative_control_detected(&self) -> bool {
        self.negative_control.as_deref() == Some(NEGATIVE_CONTROL)
            && !self.incarnation_fencing_verified
            && self.correctness_anomalies >= 3
            && !self.gate_passed("newer_incarnation_fences_old_commit_authority")
            && !self.gate_passed("newer_incarnation_fences_old_routing")
            && !self.gate_passed("newer_incarnation_fences_old_object_publication")
    }

    fn required_gates_pass(&self) -> bool {
        [
            "provider_identities_distinct",
            "external_authority_process_contract",
            "source_provider_fence_committed",
            "source_fence_precedes_destination_activation",
            "destination_exact_and_writable",
            "source_vm_restarted_without_media_replacement",
            "resurrection_follows_destination_activation",
            "newer_incarnation_fences_old_commit_authority",
            "newer_incarnation_fences_old_routing",
            "newer_incarnation_fences_old_object_publication",
            "destination_incarnation_routes_commits_and_publishes",
        ]
        .into_iter()
        .all(|id| self.gate_passed(id))
    }

    fn gate_passed(&self, id: &str) -> bool {
        self.gates
            .iter()
            .find(|gate| gate.id == id)
            .is_some_and(|gate| gate.passed)
    }
}

fn parse_timestamp(name: &str, value: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|error| format!("invalid {name} timestamp: {error}"))
}

fn validate_hex(name: &str, value: &str, length: usize) -> Result<(), String> {
    if value.len() != length
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("{name} must be {length} lowercase hex characters"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(cluster: char, suffix: &str) -> ProviderIdentity {
        ProviderIdentity {
            cluster_id: cluster.to_string().repeat(32),
            cluster_file_sha256: cluster.to_string().repeat(64),
            instance_name: format!("instance-{suffix}"),
            instance_id: format!("instance-id-{suffix}"),
            boot_disk_name: format!("boot-{suffix}"),
            boot_disk_id: format!("boot-id-{suffix}"),
            data_disk_name: format!("data-{suffix}"),
            data_disk_id: format!("data-id-{suffix}"),
        }
    }

    fn positive() -> Receipt {
        let gate_ids = [
            "provider_identities_distinct",
            "external_authority_process_contract",
            "source_provider_fence_committed",
            "source_fence_precedes_destination_activation",
            "destination_exact_and_writable",
            "source_vm_restarted_without_media_replacement",
            "resurrection_follows_destination_activation",
            "newer_incarnation_fences_old_commit_authority",
            "newer_incarnation_fences_old_routing",
            "newer_incarnation_fences_old_object_publication",
            "destination_incarnation_routes_commits_and_publishes",
        ];
        Receipt {
            schema_version: 1,
            kind: RECEIPT_KIND.to_owned(),
            provider: PROVIDER.to_owned(),
            run_id: "provider-incarnation-1".to_owned(),
            source: identity('a', "source"),
            destination: identity('b', "destination"),
            object_closure: ObjectClosure {
                manifest_uri: "gs://bucket/manifest".to_owned(),
                manifest_generation: "1".to_owned(),
                manifest_sha256: "c".repeat(64),
                manifest_bytes: 1,
                closure_uri: "gs://bucket/closure".to_owned(),
                closure_generation: "2".to_owned(),
                closure_sha256: "d".repeat(64),
                closure_bytes: 2,
                state_digest: "e".repeat(64),
                through_provider_stamp: "1".repeat(20),
                record_count: 10,
            },
            authority_trace_sha256: "f".repeat(64),
            source_fence: Some(SourceFence {
                started_at: "2026-08-27T14:00:00Z".to_owned(),
                finished_at: "2026-08-27T14:00:01Z".to_owned(),
                fence_value: "fenced:2".to_owned(),
                fence_provider_stamp: "1".repeat(20),
                concurrent_stale_commit_error_code: 1020,
                concurrent_stale_commit_rejected: true,
                post_fence_adapter_commit_rejected: true,
            }),
            activation: Some(Activation {
                started_at: "2026-08-27T14:00:02Z".to_owned(),
                finished_at: "2026-08-27T14:00:03Z".to_owned(),
                authority_trace_sha256: "f".repeat(64),
                source_fence_provider_stamp: "1".repeat(20),
                state_digest: "e".repeat(64),
                fresh_commit_succeeded: true,
                fresh_commit_provider_stamp: "2".repeat(20),
            }),
            resurrection: Some(Resurrection {
                probed_at: "2026-08-27T14:00:05Z".to_owned(),
                fence_value: "fenced:2".to_owned(),
                fence_provider_stamp: "1".repeat(20),
                fence_persisted: true,
                stale_source_adapter_commit_rejected: true,
            }),
            restart: Some(Restart {
                stopped_at: "2026-08-27T14:00:03Z".to_owned(),
                started_at: "2026-08-27T14:00:04Z".to_owned(),
                stop_succeeded: true,
                start_succeeded: true,
                identities_retained: true,
            }),
            correctness_anomalies: 0,
            incarnation_fencing_verified: true,
            negative_control: None,
            gates: gate_ids
                .into_iter()
                .map(|id| Gate {
                    id: id.to_owned(),
                    passed: true,
                    detail: "passed".to_owned(),
                })
                .collect(),
            scope: "bounded test".to_owned(),
        }
    }

    #[test]
    fn positive_receipt_requires_all_fenced_surfaces() {
        let receipt = positive();
        let encoded = serde_json::to_vec(&receipt).unwrap();
        let decoded = Receipt::from_json(&encoded).unwrap();
        assert!(decoded.positive_passed());
    }

    #[test]
    fn positive_receipt_matches_frozen_json_schema() {
        let schema_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../evals/schema/provider-incarnation-receipt-v1.schema.json");
        let schema =
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(schema_path).unwrap())
                .unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        validator
            .validate(&serde_json::to_value(positive()).unwrap())
            .unwrap();
    }

    #[test]
    fn poison_requires_commit_route_and_publication_failures() {
        let mut receipt = positive();
        receipt.source_fence = None;
        receipt.activation = None;
        receipt.resurrection = None;
        receipt.restart = None;
        receipt.negative_control = Some(NEGATIVE_CONTROL.to_owned());
        receipt.incarnation_fencing_verified = false;
        for gate in &mut receipt.gates {
            if [
                "newer_incarnation_fences_old_commit_authority",
                "newer_incarnation_fences_old_routing",
                "newer_incarnation_fences_old_object_publication",
            ]
            .contains(&gate.id.as_str())
            {
                gate.passed = false;
            }
        }
        receipt.correctness_anomalies = 3;
        let encoded = serde_json::to_vec(&receipt).unwrap();
        let decoded = Receipt::from_json(&encoded).unwrap();
        assert!(decoded.negative_control_detected());
    }
}
