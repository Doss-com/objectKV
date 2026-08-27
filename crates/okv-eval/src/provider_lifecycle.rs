//! Receipt contract for the external `FoundationDB` plus GCS lifecycle probe.

use serde::Deserialize;

/// One named assertion emitted by the provider lifecycle probe.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Gate {
    pub id: String,
    pub passed: bool,
    pub detail: String,
}

/// One measured lifecycle phase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Timing {
    pub id: String,
    pub duration_ns: u64,
}

/// Machine-readable output of `foundationdb_lifecycle_r0.py`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Receipt {
    pub schema_version: u32,
    pub kind: String,
    pub provider: String,
    pub run_id: String,
    pub duration_ns: u64,
    pub correctness_anomalies: u64,
    pub empty_logical_generation_lifecycle_passed: bool,
    pub media_loss_verified: bool,
    pub ha_verified: bool,
    pub record_count_requested: u64,
    pub restored_chunks: u64,
    pub replayed_chunks: u64,
    pub closure_bytes: u64,
    pub manifest_bytes: u64,
    pub closure_uri: String,
    pub manifest_uri: String,
    pub frontier_manifest_uri: String,
    pub through_provider_stamp: String,
    pub negative_control: Option<String>,
    pub gates: Vec<Gate>,
    pub timings: Vec<Timing>,
    pub scope: String,
}

impl Receipt {
    /// Parse and validate the stable receipt identity.
    ///
    /// # Errors
    ///
    /// Returns a stable reason when the bytes are not the R0 lifecycle schema.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let receipt: Self = serde_json::from_slice(bytes)
            .map_err(|error| format!("parse FoundationDB lifecycle receipt: {error}"))?;
        if receipt.schema_version != 1 {
            return Err(format!(
                "FoundationDB lifecycle receipt schema {} is unsupported",
                receipt.schema_version
            ));
        }
        if receipt.kind != "foundationdb_objectkv_lifecycle_r0" {
            return Err(format!(
                "unexpected FoundationDB lifecycle receipt kind {}",
                receipt.kind
            ));
        }
        if receipt.provider != "foundationdb-7.4.6@e77b64d4c5d01d240931c08c5384a834cae27337" {
            return Err(format!(
                "unexpected FoundationDB lifecycle provider {}",
                receipt.provider
            ));
        }
        if receipt.media_loss_verified || receipt.ha_verified {
            return Err(
                "logical R0 receipt must not claim media-loss or HA verification".to_owned(),
            );
        }
        Ok(receipt)
    }

    /// Find one measured phase in seconds.
    #[must_use]
    pub fn timing_seconds(&self, id: &str) -> Option<f64> {
        self.timings
            .iter()
            .find(|timing| timing.id == id)
            .map(|timing| std::time::Duration::from_nanos(timing.duration_ns).as_secs_f64())
    }

    /// True only when the positive lifecycle subject passed every assertion.
    #[must_use]
    pub fn candidate_passed(&self) -> bool {
        self.negative_control.is_none()
            && self.correctness_anomalies == 0
            && self.empty_logical_generation_lifecycle_passed
            && !self.gates.is_empty()
            && self.gates.iter().all(|gate| gate.passed)
            && self.restored_chunks > 0
            && self.replayed_chunks == self.restored_chunks
            && self.manifest_uri == self.frontier_manifest_uri
    }

    /// True when the named poison was injected and failed at least one gate.
    #[must_use]
    pub fn negative_control_detected(&self, expected: &str) -> bool {
        self.negative_control.as_deref() == Some(expected)
            && self.correctness_anomalies > 0
            && !self.empty_logical_generation_lifecycle_passed
            && self.gates.iter().any(|gate| !gate.passed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(negative_control: Option<&str>, gate_passed: bool) -> Receipt {
        Receipt {
            schema_version: 1,
            kind: "foundationdb_objectkv_lifecycle_r0".to_owned(),
            provider: "foundationdb-7.4.6@e77b64d4c5d01d240931c08c5384a834cae27337".to_owned(),
            run_id: "run-1".to_owned(),
            duration_ns: 10,
            correctness_anomalies: u64::from(!gate_passed),
            empty_logical_generation_lifecycle_passed: gate_passed,
            media_loss_verified: false,
            ha_verified: false,
            record_count_requested: 10,
            restored_chunks: u64::from(gate_passed),
            replayed_chunks: u64::from(gate_passed),
            closure_bytes: 100,
            manifest_bytes: 50,
            closure_uri: "gs://bucket/closure-a.json".to_owned(),
            manifest_uri: "gs://bucket/manifest-a.json".to_owned(),
            frontier_manifest_uri: "gs://bucket/manifest-a.json".to_owned(),
            through_provider_stamp: "0001".to_owned(),
            negative_control: negative_control.map(ToOwned::to_owned),
            gates: vec![Gate {
                id: "gate".to_owned(),
                passed: gate_passed,
                detail: "detail".to_owned(),
            }],
            timings: vec![Timing {
                id: "objectify".to_owned(),
                duration_ns: 1_000_000_000,
            }],
            scope: "logical".to_owned(),
        }
    }

    #[test]
    fn positive_receipt_requires_exact_replay_and_frontier() {
        assert!(receipt(None, true).candidate_passed());
    }

    #[test]
    fn negative_receipt_requires_named_detected_failure() {
        let value = receipt(Some("omit_retained_change"), false);
        assert!(value.negative_control_detected("omit_retained_change"));
        assert!(!value.negative_control_detected("restore_without_generation"));
    }
}
