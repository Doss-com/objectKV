//! Typed read-only IAM receipt for RFC-0046 GCS consumers.

use chrono::{DateTime, Utc};
use okv_object::content_sha256;
use serde::{Deserialize, Serialize};

/// Runner identity bound to one IAM observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28IamRunnerV1 {
    pub instance_name: String,
    pub instance_id: String,
    pub zone: String,
}

/// Runtime principal resolved from the GCE metadata server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28IamPrincipalV1 {
    pub email: String,
    pub unique_id: String,
    pub disabled: bool,
    pub runtime_metadata_email_matches: bool,
    pub credential_source: String,
    pub oauth_scopes: Vec<String>,
}

/// Non-secret token-lifetime observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28IamTokenObservationV1 {
    pub token_type: String,
    pub expires_in_seconds: u64,
    pub observed_epoch_seconds: u64,
    pub expires_at: String,
    pub access_token_recorded: bool,
}

/// Direct project and bucket roles held by the reader principal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28IamDirectBindingsV1 {
    pub project_roles: Vec<String>,
    pub bucket_roles: Vec<String>,
    pub storage_writer_roles: Vec<String>,
}

/// Effective storage permissions returned by Policy Troubleshooter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28IamEffectivePermissionsV1 {
    #[serde(rename = "storage.objects.get")]
    pub get: String,
    #[serde(rename = "storage.objects.list")]
    pub list: String,
    #[serde(rename = "storage.objects.create")]
    pub create: String,
    #[serde(rename = "storage.objects.delete")]
    pub delete: String,
}

/// Unique create-only probe executed by the reader principal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28IamDeniedWriteProbeV1 {
    pub object: String,
    pub preexisting: bool,
    pub create_only: bool,
    pub exit_code: u32,
    pub error_class: String,
    pub required_permission: String,
    pub object_absent_after_probe: bool,
}

/// Explicit conclusions carried by the receipt producer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28IamConclusionsV1 {
    pub exact_bucket_viewer_binding: bool,
    pub no_direct_storage_writer_binding: bool,
    pub effective_create_denied: bool,
    pub effective_delete_denied: bool,
    pub runtime_principal_matches: bool,
}

/// Complete read-only identity receipt used by a T28 execution plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28ReaderIamReceiptV1 {
    pub schema_version: u32,
    pub observed_at: String,
    pub project: String,
    pub bucket: String,
    pub region: String,
    pub runner: T28IamRunnerV1,
    pub principal: T28IamPrincipalV1,
    pub token_observation: T28IamTokenObservationV1,
    pub direct_bindings: T28IamDirectBindingsV1,
    pub effective_permissions: T28IamEffectivePermissionsV1,
    pub denied_write_probe: T28IamDeniedWriteProbeV1,
    pub conclusions: T28IamConclusionsV1,
}

impl T28ReaderIamReceiptV1 {
    /// Decode and validate one receipt against its raw content digest.
    ///
    /// # Errors
    ///
    /// Returns an error for a raw digest mismatch, schema violation, invalid
    /// time relation, provider scope mismatch, writer capability, or failed
    /// denied-write conclusion.
    pub fn decode(bytes: &[u8], expected_sha256: &str) -> Result<Self, String> {
        if !valid_sha256(expected_sha256) || content_sha256(bytes) != expected_sha256 {
            return Err("T28 IAM receipt raw identity mismatch".to_owned());
        }
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/t28-reader-iam-receipt-v1.schema.json"
        ))
        .map_err(|error| error.to_string())?;
        jsonschema::validator_for(&schema)
            .map_err(|error| error.to_string())?
            .validate(&value)
            .map_err(|error| error.to_string())?;
        let receipt: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        receipt.validate_semantics()?;
        Ok(receipt)
    }

    fn validate_semantics(&self) -> Result<(), String> {
        let observed = self
            .observed_at
            .parse::<DateTime<Utc>>()
            .map_err(|error| error.to_string())?;
        let expires = self
            .token_observation
            .expires_at
            .parse::<DateTime<Utc>>()
            .map_err(|error| error.to_string())?;
        let observed_epoch = u64::try_from(observed.timestamp()).unwrap_or(u64::MAX);
        let expires_epoch = u64::try_from(expires.timestamp()).unwrap_or(u64::MAX);
        if self.schema_version != 1
            || observed_epoch != self.token_observation.observed_epoch_seconds
            || expires_epoch
                != observed_epoch.saturating_add(self.token_observation.expires_in_seconds)
            || !self.runner.zone.starts_with(&format!("{}-", self.region))
            || !self.principal.email.ends_with(".iam.gserviceaccount.com")
            || self.bucket.trim().is_empty()
            || !self
                .denied_write_probe
                .object
                .starts_with(&format!("gs://{}/", self.bucket))
            || self.direct_bindings.bucket_roles != ["roles/storage.objectViewer"]
            || !self.direct_bindings.storage_writer_roles.is_empty()
            || self.effective_permissions.get != "GRANTED"
            || self.effective_permissions.create != "NOT_GRANTED"
            || self.effective_permissions.delete != "NOT_GRANTED"
            || !self.denied_write_probe.object_absent_after_probe
        {
            return Err("T28 IAM receipt semantic boundary mismatch".to_owned());
        }
        Ok(())
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::T28ReaderIamReceiptV1;
    use okv_object::content_sha256;

    const RECEIPT: &[u8] = include_bytes!(
        "../../../docs/artifacts/eval-receipts/rfc0046-t28-point-preflight-gcp-r0-2026-08-30/reader-iam-receipt-v1.json"
    );

    #[test]
    fn actual_reader_receipt_is_typed_schema_valid_and_read_only() {
        let digest = content_sha256(RECEIPT);
        let receipt = T28ReaderIamReceiptV1::decode(RECEIPT, &digest).expect("valid IAM receipt");
        assert_eq!(
            receipt.principal.email,
            "objectkv-eval-runner@doss-objectkv-dev.iam.gserviceaccount.com"
        );
        assert_eq!(
            receipt.direct_bindings.bucket_roles,
            ["roles/storage.objectViewer"]
        );
        assert!(receipt.direct_bindings.storage_writer_roles.is_empty());
    }

    #[test]
    fn writer_role_and_raw_digest_drift_are_rejected() {
        assert!(T28ReaderIamReceiptV1::decode(RECEIPT, &"0".repeat(64)).is_err());
        let mut value: serde_json::Value = serde_json::from_slice(RECEIPT).expect("receipt JSON");
        value["direct_bindings"]["storage_writer_roles"] =
            serde_json::json!(["roles/storage.objectCreator"]);
        let poisoned = serde_json::to_vec(&value).expect("poisoned receipt");
        let digest = content_sha256(&poisoned);
        assert!(T28ReaderIamReceiptV1::decode(&poisoned, &digest).is_err());
    }
}
