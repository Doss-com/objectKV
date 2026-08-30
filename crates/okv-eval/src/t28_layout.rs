//! RFC-0048 typed C0/C5 fixture and generation-pinned placement contract.

use async_trait::async_trait;
use bytes::Bytes;
use okv_object::{
    content_sha256, Backend, BackendDescriptor, BackendRead, ErrorClass, RevisionToken, StoreError,
    WriteCondition,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;

const FIXTURE_SCHEMA_VERSION: u32 = 1;
const FIXTURE_ID_MAGIC: &[u8] = b"OKVTLFI1";
const CHILD_MAGIC: &[u8] = b"OKVTLCH1";
const ROOT_MAGIC: &[u8] = b"OKVTLR1";
const PLACEMENT_MAGIC: &[u8] = b"OKVTLPL1";

/// Frozen SHA-256 of the RFC-0048 abstract workload plan.
pub const T28_LAYOUT_WORKLOAD_PLAN_SHA256: &str =
    "fa337ae95089b7c9e5771575568480769267468c271778e6781e18b99de337e1";

/// Summary of the independently generated typed MVCC fixture.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28LayoutOracleFixtureV1 {
    pub seed: u64,
    pub key_count: u64,
    pub record_count: u64,
    pub live_row_count: u64,
    pub covered_through_version: u64,
    pub canonical_history_sha256: String,
    pub ordered_projection_sha256: String,
    pub aggregate: T28LayoutOracleAggregateV1,
}

/// Aggregate returned by the frozen projected-scan query.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28LayoutOracleAggregateV1 {
    pub row_count: u64,
    pub quantity_sum: String,
}

/// One field in the frozen typed-row schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28LayoutOracleColumnV1 {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub nullable: bool,
}

/// Logical schema independently bound by the oracle artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28LayoutOracleSchemaV1 {
    pub id: String,
    pub columns: Vec<T28LayoutOracleColumnV1>,
}

/// One deterministic point-operation and expected-outcome trace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28LayoutOracleTraceV1 {
    pub seed: u64,
    pub operation_sequence_sha256: String,
    pub expected_outcomes_sha256: String,
}

/// Checked-in output of the standalone RFC-0048 reference generator.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct T28LayoutOracleV1 {
    pub schema_version: u32,
    pub generator: String,
    pub fixture: T28LayoutOracleFixtureV1,
    pub schema: T28LayoutOracleSchemaV1,
    pub schema_sha256: String,
    pub traces: Vec<T28LayoutOracleTraceV1>,
    pub workload_plan_sha256: String,
}

impl T28LayoutOracleV1 {
    /// Validate the exact frozen oracle shape and internal schema digest.
    ///
    /// # Errors
    ///
    /// Returns an error when any fixture, schema, aggregate, trace, or digest
    /// differs from the pre-implementation artifact reviewed in RFC-0048.
    pub fn validate(&self) -> Result<(), String> {
        let expected_columns = [
            ("key", "u64", false),
            ("tenant", "u32", false),
            ("category", "u16", false),
            ("quantity", "i64", false),
            ("opaque_payload", "bytes[480]", false),
        ];
        if self.schema_version != 1
            || self.generator != "evals/oracles/t28-layout-geometry-v1.mjs"
            || self.fixture.seed != 5_699
            || self.fixture.key_count != 16_384
            || self.fixture.record_count != 25_014
            || self.fixture.live_row_count != 15_742
            || self.fixture.aggregate.row_count != self.fixture.live_row_count
            || self.fixture.covered_through_version != 5
            || self.fixture.aggregate.quantity_sum != "67524278"
            || self.fixture.canonical_history_sha256
                != "d4be64434f6b69990a2787876f514c6036727b41dcf1c5e120f91b6ce968ecd4"
            || self.fixture.ordered_projection_sha256
                != "7fb3fbb637ac93942620d287899dcebfec54e0f50ee9eeb9414ebff022cab39e"
            || self.schema.id != "objectkv.t28.typed-row.v1"
            || self.schema.columns.len() != expected_columns.len()
            || self
                .schema
                .columns
                .iter()
                .zip(expected_columns)
                .any(|(observed, expected)| {
                    (
                        observed.name.as_str(),
                        observed.data_type.as_str(),
                        observed.nullable,
                    ) != expected
                })
            || self.schema_sha256
                != "967d37734d36729543c0ae50303eb6ff530ddddb367fd143c335faedf6c8eb6d"
            || self.schema_sha256 != calculated_json_sha256(&self.schema)?
            || self.workload_plan_sha256 != T28_LAYOUT_WORKLOAD_PLAN_SHA256
            || self.traces.len() != 3
        {
            return Err("invalid RFC-0048 independent oracle".to_owned());
        }
        let expected_traces = [
            (
                5_701,
                "30a2c0d5b78fabf1a5446186e9bcf2ba03252d48b9f860e7c56cc0cfcebf6f35",
                "f15dff9e4ec92a23dbbb1235ccf92b8d5011e7ca0dba4188edd1e1b40a548329",
            ),
            (
                5_702,
                "0ec26dea2ae888506ad32a0aa3104844d4fdd9012a4e108f4459bc3944128653",
                "798ed4b417a5ac9b28f1ac202defa395313bb44eaae335ce88e113b54ecc59d6",
            ),
            (
                5_703,
                "8017cf17a9ac519c976d25916e218fc815c84594ad7afab0bafddf1789d7ccbd",
                "b133425752749df93f2fb7c04f8fab266c59172adcf8d0c0b62bf01a222e3178",
            ),
        ];
        for (trace, expected) in self.traces.iter().zip(expected_traces) {
            if trace.seed != expected.0
                || trace.operation_sequence_sha256 != expected.1
                || trace.expected_outcomes_sha256 != expected.2
            {
                return Err("invalid RFC-0048 oracle trace".to_owned());
            }
        }
        Ok(())
    }
}

/// Physical representation authenticated by one typed-layout root.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedLayoutSubjectV1 {
    C0IndexedRow,
    C5ColumnarMain,
}

impl TypedLayoutSubjectV1 {
    const fn id(self) -> &'static str {
        match self {
            Self::C0IndexedRow => "c0_indexed_row",
            Self::C5ColumnarMain => "c5_columnar_main",
        }
    }

    const fn expected_format(self) -> &'static str {
        match self {
            Self::C0IndexedRow => "okv.row-object.v1",
            Self::C5ColumnarMain => "okv.columnar-overlay.v1",
        }
    }

    fn expected_capabilities(self) -> &'static [TypedLayoutCapabilityV1] {
        match self {
            Self::C0IndexedRow => &[
                TypedLayoutCapabilityV1::Point,
                TypedLayoutCapabilityV1::ProjectedScan,
            ],
            Self::C5ColumnarMain => &[
                TypedLayoutCapabilityV1::Point,
                TypedLayoutCapabilityV1::ProjectedScan,
                TypedLayoutCapabilityV1::OpaquePayloadSplit,
            ],
        }
    }
}

/// Operation class exposed by one child layout.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedLayoutCapabilityV1 {
    Point,
    ProjectedScan,
    OpaquePayloadSplit,
}

impl TypedLayoutCapabilityV1 {
    const fn id(self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::ProjectedScan => "projected_scan",
            Self::OpaquePayloadSplit => "opaque_payload_split",
        }
    }
}

/// Semantic purpose of one immutable child object.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TypedLayoutObjectRoleV1 {
    Manifest,
    Data,
    Index,
    Projection,
    Payload,
}

impl TypedLayoutObjectRoleV1 {
    const fn id(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::Data => "data",
            Self::Index => "index",
            Self::Projection => "projection",
            Self::Payload => "payload",
        }
    }
}

/// Exact cloud identity of one object reachable from a child manifest.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypedLayoutObjectIdentityV1 {
    pub role: TypedLayoutObjectRoleV1,
    pub key: String,
    pub generation: String,
    pub length: u64,
    pub sha256: String,
}

impl TypedLayoutObjectIdentityV1 {
    /// Convert the frozen numeric generation into an object-backend precondition.
    #[must_use]
    pub fn revision(&self) -> RevisionToken {
        RevisionToken {
            e_tag: None,
            version: Some(self.generation.clone()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if !valid_object_key(&self.key)
            || self.generation.is_empty()
            || !self.generation.bytes().all(|byte| byte.is_ascii_digit())
            || self.length == 0
            || !valid_sha256(&self.sha256)
        {
            return Err("invalid RFC-0048 child object identity".to_owned());
        }
        Ok(())
    }
}

/// Complete immutable closure for one physical layout subject.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypedLayoutChildV1 {
    pub subject: TypedLayoutSubjectV1,
    pub bucket: String,
    pub format_id: String,
    pub format_version: u32,
    pub canonical_history_sha256: String,
    pub schema_id: String,
    pub schema_sha256: String,
    pub covered_through_version: u64,
    pub manifest_key: String,
    pub capabilities: Vec<TypedLayoutCapabilityV1>,
    pub objects: Vec<TypedLayoutObjectIdentityV1>,
    pub closure_sha256: String,
}

impl TypedLayoutChildV1 {
    /// Seal one already sorted and complete child-object inventory.
    ///
    /// # Errors
    ///
    /// Returns an error when identity, capabilities, roles, ordering, or media
    /// fields do not describe the frozen C0 or C5 format.
    #[allow(clippy::too_many_arguments)]
    pub fn seal(
        subject: TypedLayoutSubjectV1,
        bucket: String,
        canonical_history_sha256: String,
        schema_id: String,
        schema_sha256: String,
        covered_through_version: u64,
        manifest_key: String,
        capabilities: Vec<TypedLayoutCapabilityV1>,
        objects: Vec<TypedLayoutObjectIdentityV1>,
    ) -> Result<Self, String> {
        let mut child = Self {
            subject,
            bucket,
            format_id: subject.expected_format().to_owned(),
            format_version: 1,
            canonical_history_sha256,
            schema_id,
            schema_sha256,
            covered_through_version,
            manifest_key,
            capabilities,
            objects,
            closure_sha256: String::new(),
        };
        child.closure_sha256 = child.calculated_closure_sha256();
        child.validate()?;
        Ok(child)
    }

    /// Calculate the semantic digest over every child field and object identity.
    #[must_use]
    pub fn calculated_closure_sha256(&self) -> String {
        content_sha256(&encode_child(self))
    }

    /// Find one exact object by its child-relative key.
    #[must_use]
    pub fn object(&self, key: &str) -> Option<&TypedLayoutObjectIdentityV1> {
        self.objects.iter().find(|object| object.key == key)
    }

    /// Validate the closed subject inventory and its semantic digest.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown format shape, missing roles, duplicates,
    /// mutable generations, or a closure mismatch.
    pub fn validate(&self) -> Result<(), String> {
        if !valid_bucket(&self.bucket)
            || self.format_id != self.subject.expected_format()
            || self.format_version != 1
            || !valid_sha256(&self.canonical_history_sha256)
            || !valid_schema_id(&self.schema_id)
            || !valid_sha256(&self.schema_sha256)
            || self.covered_through_version == 0
            || !valid_object_key(&self.manifest_key)
            || self.capabilities != self.subject.expected_capabilities()
            || self.objects.is_empty()
            || !valid_sha256(&self.closure_sha256)
            || self.closure_sha256 != self.calculated_closure_sha256()
        {
            return Err("invalid RFC-0048 typed-layout child".to_owned());
        }

        let mut previous: Option<(&str, TypedLayoutObjectRoleV1)> = None;
        let mut keys = BTreeSet::new();
        let mut roles = BTreeSet::new();
        let mut manifest_count = 0_u32;
        for object in &self.objects {
            object.validate()?;
            let current = (object.key.as_str(), object.role);
            if previous.is_some_and(|value| value >= current) || !keys.insert(&object.key) {
                return Err("RFC-0048 child object inventory is unsorted or duplicated".to_owned());
            }
            previous = Some(current);
            roles.insert(object.role);
            if object.role == TypedLayoutObjectRoleV1::Manifest {
                manifest_count = manifest_count.saturating_add(1);
                if object.key != self.manifest_key {
                    return Err("RFC-0048 child manifest identity mismatch".to_owned());
                }
            }
        }
        if manifest_count != 1 || !required_roles(self.subject).is_subset(&roles) {
            return Err("RFC-0048 child role closure is incomplete".to_owned());
        }
        if forbidden_roles(self.subject)
            .iter()
            .any(|role| roles.contains(role))
        {
            return Err("RFC-0048 child contains a role from another layout".to_owned());
        }
        Ok(())
    }
}

/// One eval-only root authenticating C0 and C5 representations of one history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypedLayoutFixtureV1 {
    pub schema_version: u32,
    pub fixture_id: String,
    pub fixture_seed: u64,
    pub key_count: u64,
    pub record_count: u64,
    pub live_row_count: u64,
    pub canonical_history_sha256: String,
    pub schema_id: String,
    pub schema_sha256: String,
    pub covered_through_version: u64,
    pub oracle_sha256: String,
    pub workload_plan_sha256: String,
    pub provider: String,
    pub project: String,
    pub bucket: String,
    pub region: String,
    pub children: Vec<TypedLayoutChildV1>,
    pub root_sha256: String,
}

impl TypedLayoutFixtureV1 {
    /// Seal a shared root after both independently materialized children exist.
    ///
    /// # Errors
    ///
    /// Returns an error unless both children describe the same history, schema,
    /// version, bucket, and closed immutable object inventories.
    #[allow(clippy::too_many_arguments)]
    pub fn seal(
        fixture_seed: u64,
        key_count: u64,
        record_count: u64,
        live_row_count: u64,
        canonical_history_sha256: String,
        schema_id: String,
        schema_sha256: String,
        covered_through_version: u64,
        oracle_sha256: String,
        workload_plan_sha256: String,
        project: String,
        bucket: String,
        region: String,
        children: Vec<TypedLayoutChildV1>,
    ) -> Result<Self, String> {
        let mut fixture = Self {
            schema_version: FIXTURE_SCHEMA_VERSION,
            fixture_id: String::new(),
            fixture_seed,
            key_count,
            record_count,
            live_row_count,
            canonical_history_sha256,
            schema_id,
            schema_sha256,
            covered_through_version,
            oracle_sha256,
            workload_plan_sha256,
            provider: "gcs".to_owned(),
            project,
            bucket,
            region,
            children,
            root_sha256: String::new(),
        };
        fixture.fixture_id = fixture.calculated_fixture_id();
        fixture.root_sha256 = fixture.calculated_root_sha256();
        fixture.validate()?;
        Ok(fixture)
    }

    /// Calculate the logical fixture identity independently of physical media.
    #[must_use]
    pub fn calculated_fixture_id(&self) -> String {
        content_sha256(&encode_fixture_id(self))
    }

    /// Calculate the semantic root over both complete child closures.
    #[must_use]
    pub fn calculated_root_sha256(&self) -> String {
        content_sha256(&encode_root(self))
    }

    /// Return the exact child for one subject.
    #[must_use]
    pub fn child(&self, subject: TypedLayoutSubjectV1) -> Option<&TypedLayoutChildV1> {
        self.children.iter().find(|child| child.subject == subject)
    }

    /// Validate the root and both authenticated child closures.
    ///
    /// # Errors
    ///
    /// Returns an error for any malformed identity, missing subject, child drift,
    /// duplicate object name, or semantic digest mismatch.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != FIXTURE_SCHEMA_VERSION
            || self.fixture_seed == 0
            || self.key_count == 0
            || self.record_count < self.live_row_count
            || self.live_row_count > self.key_count
            || !valid_sha256(&self.canonical_history_sha256)
            || !valid_schema_id(&self.schema_id)
            || !valid_sha256(&self.schema_sha256)
            || self.covered_through_version == 0
            || !valid_sha256(&self.oracle_sha256)
            || !valid_sha256(&self.workload_plan_sha256)
            || self.provider != "gcs"
            || !valid_project(&self.project)
            || !valid_bucket(&self.bucket)
            || !valid_region(&self.region)
            || self.children.len() != 2
            || !valid_sha256(&self.fixture_id)
            || self.fixture_id != self.calculated_fixture_id()
            || !valid_sha256(&self.root_sha256)
            || self.root_sha256 != self.calculated_root_sha256()
        {
            return Err("invalid RFC-0048 typed-layout root".to_owned());
        }

        let expected_subjects = [
            TypedLayoutSubjectV1::C0IndexedRow,
            TypedLayoutSubjectV1::C5ColumnarMain,
        ];
        let mut object_keys = BTreeSet::new();
        for (child, expected_subject) in self.children.iter().zip(expected_subjects) {
            child.validate()?;
            if child.subject != expected_subject
                || child.bucket != self.bucket
                || child.canonical_history_sha256 != self.canonical_history_sha256
                || child.schema_id != self.schema_id
                || child.schema_sha256 != self.schema_sha256
                || child.covered_through_version != self.covered_through_version
            {
                return Err("RFC-0048 child differs from its shared root".to_owned());
            }
            for object in &child.objects {
                if !object_keys.insert(&object.key) {
                    return Err("RFC-0048 child closures share an object name".to_owned());
                }
            }
        }
        Ok(())
    }
}

/// Exact GCS placement of one serialized typed-layout root object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypedLayoutPlacementLocatorV1 {
    pub schema_version: u32,
    pub fixture_id: String,
    pub root_sha256: String,
    pub provider: String,
    pub project: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub root_key: String,
    pub root_generation: String,
    pub root_length: u64,
    pub root_object_sha256: String,
    pub envelope_sha256: String,
}

impl TypedLayoutPlacementLocatorV1 {
    /// Seal one generation-pinned placement of a serialized root.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe placement, absent generation, or malformed
    /// logical and physical root identities.
    #[allow(clippy::too_many_arguments)]
    pub fn seal(
        fixture_id: String,
        root_sha256: String,
        project: String,
        bucket: String,
        region: String,
        prefix: String,
        root_key: String,
        root_generation: String,
        root_length: u64,
        root_object_sha256: String,
    ) -> Result<Self, String> {
        let mut locator = Self {
            schema_version: FIXTURE_SCHEMA_VERSION,
            fixture_id,
            root_sha256,
            provider: "gcs".to_owned(),
            project,
            bucket,
            region,
            prefix,
            root_key,
            root_generation,
            root_length,
            root_object_sha256,
            envelope_sha256: String::new(),
        };
        locator.envelope_sha256 = locator.calculated_envelope_sha256();
        locator.validate()?;
        Ok(locator)
    }

    /// Calculate the locator digest without trusting its serialized digest field.
    #[must_use]
    pub fn calculated_envelope_sha256(&self) -> String {
        content_sha256(&encode_placement(self))
    }

    /// Return the GCS generation precondition for the root object.
    #[must_use]
    pub fn root_revision(&self) -> RevisionToken {
        RevisionToken {
            e_tag: None,
            version: Some(self.root_generation.clone()),
        }
    }

    /// Validate the complete generation-pinned placement identity.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed identity, placement, or envelope fields.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != FIXTURE_SCHEMA_VERSION
            || !valid_sha256(&self.fixture_id)
            || !valid_sha256(&self.root_sha256)
            || self.provider != "gcs"
            || !valid_project(&self.project)
            || !valid_bucket(&self.bucket)
            || !valid_region(&self.region)
            || !valid_object_key(&self.prefix)
            || !valid_object_key(&self.root_key)
            || !self.root_key.starts_with(&format!("{}/", self.prefix))
            || self.root_generation.is_empty()
            || !self
                .root_generation
                .bytes()
                .all(|byte| byte.is_ascii_digit())
            || self.root_length == 0
            || !valid_sha256(&self.root_object_sha256)
            || !valid_sha256(&self.envelope_sha256)
            || self.envelope_sha256 != self.calculated_envelope_sha256()
        {
            return Err("invalid RFC-0048 typed-layout placement locator".to_owned());
        }
        Ok(())
    }
}

/// Read-only backend that translates a closed child descriptor into mandatory
/// provider generation preconditions.
pub struct GenerationPinnedChildBackend {
    inner: Arc<dyn Backend>,
    subject: String,
    objects: BTreeMap<String, TypedLayoutObjectIdentityV1>,
}

impl Debug for GenerationPinnedChildBackend {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GenerationPinnedChildBackend")
            .field("subject", &self.subject)
            .field("objects", &self.objects.len())
            .finish_non_exhaustive()
    }
}

impl GenerationPinnedChildBackend {
    /// Wrap one child-scoped backend with its exact immutable object inventory.
    ///
    /// # Errors
    ///
    /// Returns an error when the child descriptor is invalid.
    pub fn new(inner: Arc<dyn Backend>, child: &TypedLayoutChildV1) -> Result<Self, String> {
        child.validate()?;
        Self::from_inventory(inner, child.subject.id(), &child.objects)
    }

    /// Wrap an RFC-reviewed object inventory that is not represented by the
    /// RFC-0048 child enum.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty label, invalid object identity, or a
    /// duplicate object key.
    pub(crate) fn from_inventory(
        inner: Arc<dyn Backend>,
        subject: &str,
        objects: &[TypedLayoutObjectIdentityV1],
    ) -> Result<Self, String> {
        if subject.is_empty() || objects.is_empty() {
            return Err("generation-pinned object inventory is empty".to_owned());
        }
        let mut inventory = BTreeMap::new();
        for object in objects {
            object.validate()?;
            if inventory
                .insert(object.key.clone(), object.clone())
                .is_some()
            {
                return Err("generation-pinned object inventory is duplicated".to_owned());
            }
        }
        Ok(Self {
            inner,
            subject: subject.to_owned(),
            objects: inventory,
        })
    }

    fn denied(operation: &str) -> StoreError {
        store_error(
            ErrorClass::PermissionDenied,
            format!("RFC-0048 measured backend denies {operation}"),
        )
    }
}

#[async_trait]
impl Backend for GenerationPinnedChildBackend {
    fn descriptor(&self) -> BackendDescriptor {
        self.inner.descriptor()
    }

    async fn put(
        &self,
        _key: &str,
        _bytes: Bytes,
        _condition: WriteCondition,
    ) -> Result<RevisionToken, StoreError> {
        Err(Self::denied("PUT"))
    }

    async fn get(
        &self,
        key: &str,
        range: Option<Range<u64>>,
        expected: Option<&RevisionToken>,
    ) -> Result<BackendRead, StoreError> {
        let identity = self.objects.get(key).ok_or_else(|| {
            store_error(
                ErrorClass::PermissionDenied,
                "RFC-0048 read is outside the selected child closure",
            )
        })?;
        let revision = identity.revision();
        if expected.is_some_and(|value| value != &revision) {
            return Err(store_error(
                ErrorClass::PreconditionFailed,
                "RFC-0048 caller revision differs from the child descriptor",
            ));
        }
        if range
            .as_ref()
            .is_some_and(|value| value.start >= value.end || value.end > identity.length)
        {
            return Err(store_error(
                ErrorClass::PreconditionFailed,
                "RFC-0048 requested range exceeds the child object",
            ));
        }
        let read = self.inner.get(key, range.clone(), Some(&revision)).await?;
        let expected_range = range.unwrap_or(0..identity.length);
        if read.revision.version.as_deref() != Some(identity.generation.as_str())
            || read.object_length != identity.length
            || read.returned_range != expected_range
            || u64::try_from(read.bytes.len()).unwrap_or(u64::MAX)
                != expected_range.end.saturating_sub(expected_range.start)
            || (expected_range == (0..identity.length)
                && content_sha256(&read.bytes) != identity.sha256)
        {
            return Err(store_error(
                ErrorClass::Corrupt,
                "RFC-0048 generation-pinned child read identity mismatch",
            ));
        }
        Ok(read)
    }

    async fn delete(
        &self,
        _key: &str,
        _expected: Option<&RevisionToken>,
    ) -> Result<(), StoreError> {
        Err(Self::denied("DELETE"))
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>, StoreError> {
        Err(Self::denied("LIST"))
    }
}

/// Decode and authenticate one serialized typed-layout root.
///
/// # Errors
///
/// Returns an error for malformed JSON, unknown fields, invalid closure, or an
/// independently supplied root digest mismatch.
pub fn decode_typed_layout_fixture(
    bytes: &[u8],
    expected_root_sha256: &str,
) -> Result<TypedLayoutFixtureV1, String> {
    if !valid_sha256(expected_root_sha256) {
        return Err("expected RFC-0048 root SHA-256 is invalid".to_owned());
    }
    let fixture: TypedLayoutFixtureV1 =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    fixture.validate()?;
    if fixture.root_sha256 != expected_root_sha256 {
        return Err("RFC-0048 root identity mismatch".to_owned());
    }
    Ok(fixture)
}

/// Decode and authenticate one serialized generation-pinned root locator.
///
/// # Errors
///
/// Returns an error for malformed JSON, invalid placement, or an independently
/// supplied envelope digest mismatch.
pub fn decode_typed_layout_placement(
    bytes: &[u8],
    expected_envelope_sha256: &str,
) -> Result<TypedLayoutPlacementLocatorV1, String> {
    if !valid_sha256(expected_envelope_sha256) {
        return Err("expected RFC-0048 placement SHA-256 is invalid".to_owned());
    }
    let locator: TypedLayoutPlacementLocatorV1 =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    locator.validate()?;
    if locator.envelope_sha256 != expected_envelope_sha256 {
        return Err("RFC-0048 placement identity mismatch".to_owned());
    }
    Ok(locator)
}

/// Decode one independently generated RFC-0048 oracle artifact.
///
/// # Errors
///
/// Returns an error when the serialized bytes differ from the expected digest,
/// contain unknown fields, or violate the frozen fixture, schema, or trace
/// contract.
pub fn decode_t28_layout_oracle(
    bytes: &[u8],
    expected_sha256: &str,
) -> Result<T28LayoutOracleV1, String> {
    if !valid_sha256(expected_sha256) || content_sha256(bytes) != expected_sha256 {
        return Err("RFC-0048 oracle artifact identity mismatch".to_owned());
    }
    let oracle: T28LayoutOracleV1 =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    oracle.validate()?;
    Ok(oracle)
}

fn required_roles(subject: TypedLayoutSubjectV1) -> BTreeSet<TypedLayoutObjectRoleV1> {
    match subject {
        TypedLayoutSubjectV1::C0IndexedRow => [
            TypedLayoutObjectRoleV1::Manifest,
            TypedLayoutObjectRoleV1::Data,
            TypedLayoutObjectRoleV1::Index,
        ]
        .into_iter()
        .collect(),
        TypedLayoutSubjectV1::C5ColumnarMain => [
            TypedLayoutObjectRoleV1::Manifest,
            TypedLayoutObjectRoleV1::Index,
            TypedLayoutObjectRoleV1::Projection,
            TypedLayoutObjectRoleV1::Payload,
        ]
        .into_iter()
        .collect(),
    }
}

fn forbidden_roles(subject: TypedLayoutSubjectV1) -> &'static [TypedLayoutObjectRoleV1] {
    match subject {
        TypedLayoutSubjectV1::C0IndexedRow => &[
            TypedLayoutObjectRoleV1::Projection,
            TypedLayoutObjectRoleV1::Payload,
        ],
        TypedLayoutSubjectV1::C5ColumnarMain => &[TypedLayoutObjectRoleV1::Data],
    }
}

fn encode_fixture_id(fixture: &TypedLayoutFixtureV1) -> Vec<u8> {
    let mut bytes = FIXTURE_ID_MAGIC.to_vec();
    push_u64(&mut bytes, fixture.fixture_seed);
    push_string(&mut bytes, &fixture.canonical_history_sha256);
    push_string(&mut bytes, &fixture.schema_id);
    push_string(&mut bytes, &fixture.schema_sha256);
    push_u64(&mut bytes, fixture.covered_through_version);
    push_string(&mut bytes, &fixture.oracle_sha256);
    push_string(&mut bytes, &fixture.workload_plan_sha256);
    bytes
}

fn encode_child(child: &TypedLayoutChildV1) -> Vec<u8> {
    let mut bytes = CHILD_MAGIC.to_vec();
    push_string(&mut bytes, child.subject.id());
    push_string(&mut bytes, &child.bucket);
    push_string(&mut bytes, &child.format_id);
    push_u32(&mut bytes, child.format_version);
    push_string(&mut bytes, &child.canonical_history_sha256);
    push_string(&mut bytes, &child.schema_id);
    push_string(&mut bytes, &child.schema_sha256);
    push_u64(&mut bytes, child.covered_through_version);
    push_string(&mut bytes, &child.manifest_key);
    push_u64(
        &mut bytes,
        u64::try_from(child.capabilities.len()).unwrap_or(u64::MAX),
    );
    for capability in &child.capabilities {
        push_string(&mut bytes, capability.id());
    }
    push_u64(
        &mut bytes,
        u64::try_from(child.objects.len()).unwrap_or(u64::MAX),
    );
    for object in &child.objects {
        push_string(&mut bytes, object.role.id());
        push_string(&mut bytes, &object.key);
        push_string(&mut bytes, &object.generation);
        push_u64(&mut bytes, object.length);
        push_string(&mut bytes, &object.sha256);
    }
    bytes
}

fn encode_root(fixture: &TypedLayoutFixtureV1) -> Vec<u8> {
    let mut bytes = ROOT_MAGIC.to_vec();
    push_u32(&mut bytes, fixture.schema_version);
    push_string(&mut bytes, &fixture.fixture_id);
    push_u64(&mut bytes, fixture.fixture_seed);
    push_u64(&mut bytes, fixture.key_count);
    push_u64(&mut bytes, fixture.record_count);
    push_u64(&mut bytes, fixture.live_row_count);
    push_string(&mut bytes, &fixture.canonical_history_sha256);
    push_string(&mut bytes, &fixture.schema_id);
    push_string(&mut bytes, &fixture.schema_sha256);
    push_u64(&mut bytes, fixture.covered_through_version);
    push_string(&mut bytes, &fixture.oracle_sha256);
    push_string(&mut bytes, &fixture.workload_plan_sha256);
    push_string(&mut bytes, &fixture.provider);
    push_string(&mut bytes, &fixture.project);
    push_string(&mut bytes, &fixture.bucket);
    push_string(&mut bytes, &fixture.region);
    push_u64(
        &mut bytes,
        u64::try_from(fixture.children.len()).unwrap_or(u64::MAX),
    );
    for child in &fixture.children {
        push_string(&mut bytes, child.subject.id());
        push_string(&mut bytes, &child.closure_sha256);
    }
    bytes
}

fn encode_placement(locator: &TypedLayoutPlacementLocatorV1) -> Vec<u8> {
    let mut bytes = PLACEMENT_MAGIC.to_vec();
    push_u32(&mut bytes, locator.schema_version);
    push_string(&mut bytes, &locator.fixture_id);
    push_string(&mut bytes, &locator.root_sha256);
    push_string(&mut bytes, &locator.provider);
    push_string(&mut bytes, &locator.project);
    push_string(&mut bytes, &locator.bucket);
    push_string(&mut bytes, &locator.region);
    push_string(&mut bytes, &locator.prefix);
    push_string(&mut bytes, &locator.root_key);
    push_string(&mut bytes, &locator.root_generation);
    push_u64(&mut bytes, locator.root_length);
    push_string(&mut bytes, &locator.root_object_sha256);
    bytes
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_string(bytes: &mut Vec<u8>, value: &str) {
    push_u64(bytes, u64::try_from(value.len()).unwrap_or(u64::MAX));
    bytes.extend_from_slice(value.as_bytes());
}

fn calculated_json_sha256<T: Serialize>(value: &T) -> Result<String, String> {
    let value = serde_json::to_value(value).map_err(|error| error.to_string())?;
    Ok(content_sha256(canonical_json(&value)?.as_bytes()))
}

fn canonical_json(value: &Value) -> Result<String, String> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).map_err(|error| error.to_string())
        }
        Value::Array(values) => Ok(format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Result<Vec<_>, _>>()?
                .join(",")
        )),
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let fields = keys
                .into_iter()
                .map(|key| {
                    Ok(format!(
                        "{}:{}",
                        serde_json::to_string(key).map_err(|error| error.to_string())?,
                        canonical_json(&values[key])?
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(format!("{{{}}}", fields.join(",")))
        }
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn store_error(class: ErrorClass, detail: impl Into<String>) -> StoreError {
    StoreError {
        class,
        detail: detail.into(),
    }
}

fn valid_object_key(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.ends_with('/')
        && value
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

fn valid_bucket(value: &str) -> bool {
    (3..=222).contains(&value.len())
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn valid_project(value: &str) -> bool {
    (6..=30).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
}

fn valid_region(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_schema_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::{
        decode_t28_layout_oracle, decode_typed_layout_fixture, decode_typed_layout_placement,
        store_error, GenerationPinnedChildBackend, TypedLayoutCapabilityV1, TypedLayoutChildV1,
        TypedLayoutFixtureV1, TypedLayoutObjectIdentityV1, TypedLayoutObjectRoleV1,
        TypedLayoutPlacementLocatorV1, TypedLayoutSubjectV1,
    };
    use crate::provider_attempt::{ProviderAttemptBackend, ProviderAttemptPhase};
    use bytes::Bytes;
    use okv_object::content_sha256;
    use okv_object::{
        Backend, BackendDescriptor, BackendRead, ErrorClass, RevisionToken, StoreError,
        WriteCondition,
    };
    use std::collections::BTreeMap;
    use std::ops::Range;
    use std::sync::Arc;

    #[derive(Debug)]
    struct TestGenerationBackend {
        objects: BTreeMap<String, (Bytes, String)>,
    }

    #[async_trait::async_trait]
    impl Backend for TestGenerationBackend {
        fn descriptor(&self) -> BackendDescriptor {
            BackendDescriptor {
                id: "test-generation".to_owned(),
                driver: "test".to_owned(),
                driver_version: "1".to_owned(),
                server_version: "1".to_owned(),
                conditional_primitive: "generation-match".to_owned(),
                guarded_delete: true,
                delete_strategy: "test".to_owned(),
            }
        }

        async fn put(
            &self,
            _key: &str,
            _bytes: Bytes,
            _condition: WriteCondition,
        ) -> Result<RevisionToken, StoreError> {
            Err(store_error(ErrorClass::PermissionDenied, "read only"))
        }

        async fn get(
            &self,
            key: &str,
            range: Option<Range<u64>>,
            expected: Option<&RevisionToken>,
        ) -> Result<BackendRead, StoreError> {
            let (bytes, generation) = self
                .objects
                .get(key)
                .ok_or_else(|| store_error(ErrorClass::NotFound, "missing"))?;
            if expected.and_then(|revision| revision.version.as_deref())
                != Some(generation.as_str())
            {
                return Err(store_error(
                    ErrorClass::PreconditionFailed,
                    "generation mismatch",
                ));
            }
            let object_length = u64::try_from(bytes.len()).expect("test object length");
            let returned_range = range.unwrap_or(0..object_length);
            let start = usize::try_from(returned_range.start).expect("test range start");
            let end = usize::try_from(returned_range.end).expect("test range end");
            Ok(BackendRead {
                bytes: bytes.slice(start..end),
                revision: RevisionToken {
                    e_tag: None,
                    version: Some(generation.clone()),
                },
                object_length,
                returned_range,
            })
        }

        async fn delete(
            &self,
            _key: &str,
            _expected: Option<&RevisionToken>,
        ) -> Result<(), StoreError> {
            Err(store_error(ErrorClass::PermissionDenied, "read only"))
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>, StoreError> {
            Err(store_error(ErrorClass::PermissionDenied, "no list"))
        }
    }

    fn object(
        role: TypedLayoutObjectRoleV1,
        key: &str,
        generation: u64,
        byte: u8,
    ) -> TypedLayoutObjectIdentityV1 {
        TypedLayoutObjectIdentityV1 {
            role,
            key: key.to_owned(),
            generation: generation.to_string(),
            length: 4_096,
            sha256: format!("{byte:02x}").repeat(32),
        }
    }

    fn fixture() -> TypedLayoutFixtureV1 {
        let history = "aa".repeat(32);
        let schema = "bb".repeat(32);
        let bucket = "doss-objectkv-dev-okv-evals".to_owned();
        let c0 = TypedLayoutChildV1::seal(
            TypedLayoutSubjectV1::C0IndexedRow,
            bucket.clone(),
            history.clone(),
            "objectkv.t28.typed-row.v1".to_owned(),
            schema.clone(),
            5,
            "c0/manifest.json".to_owned(),
            vec![
                TypedLayoutCapabilityV1::Point,
                TypedLayoutCapabilityV1::ProjectedScan,
            ],
            vec![
                object(TypedLayoutObjectRoleV1::Data, "c0/data.okvb", 101, 0x11),
                object(TypedLayoutObjectRoleV1::Index, "c0/index.okvi", 102, 0x12),
                object(
                    TypedLayoutObjectRoleV1::Manifest,
                    "c0/manifest.json",
                    103,
                    0x13,
                ),
            ],
        )
        .expect("seal C0 child");
        let c5 = TypedLayoutChildV1::seal(
            TypedLayoutSubjectV1::C5ColumnarMain,
            bucket.clone(),
            history.clone(),
            "objectkv.t28.typed-row.v1".to_owned(),
            schema.clone(),
            5,
            "c5/manifest.json".to_owned(),
            vec![
                TypedLayoutCapabilityV1::Point,
                TypedLayoutCapabilityV1::ProjectedScan,
                TypedLayoutCapabilityV1::OpaquePayloadSplit,
            ],
            vec![
                object(TypedLayoutObjectRoleV1::Index, "c5/index.okvi", 201, 0x21),
                object(
                    TypedLayoutObjectRoleV1::Manifest,
                    "c5/manifest.json",
                    202,
                    0x22,
                ),
                object(
                    TypedLayoutObjectRoleV1::Payload,
                    "c5/payload.okvp",
                    203,
                    0x23,
                ),
                object(
                    TypedLayoutObjectRoleV1::Projection,
                    "c5/projection.okvc",
                    204,
                    0x24,
                ),
            ],
        )
        .expect("seal C5 child");
        TypedLayoutFixtureV1::seal(
            5_699,
            16_384,
            25_014,
            15_742,
            history,
            "objectkv.t28.typed-row.v1".to_owned(),
            schema,
            5,
            "cc".repeat(32),
            "dd".repeat(32),
            "doss-objectkv-dev".to_owned(),
            bucket,
            "us-central1".to_owned(),
            vec![c0, c5],
        )
        .expect("seal fixture")
    }

    #[test]
    fn shared_root_is_field_sensitive_and_generation_pinned() {
        let fixture = fixture();
        fixture.validate().expect("valid root");
        let object = fixture
            .child(TypedLayoutSubjectV1::C5ColumnarMain)
            .and_then(|child| child.object("c5/payload.okvp"))
            .expect("payload object");
        assert_eq!(object.revision().version.as_deref(), Some("203"));

        let mut changed = fixture.clone();
        changed.children[1].objects[2].generation = "205".to_owned();
        changed.children[1].closure_sha256 = changed.children[1].calculated_closure_sha256();
        assert!(changed.validate().is_err());
    }

    #[test]
    fn root_rejects_missing_duplicate_cross_bucket_and_schema_drift() {
        let fixture = fixture();
        let mut missing = fixture.clone();
        missing.children[1].objects.pop();
        missing.children[1].closure_sha256 = missing.children[1].calculated_closure_sha256();
        missing.root_sha256 = missing.calculated_root_sha256();
        assert!(missing.validate().is_err());

        let mut duplicate = fixture.clone();
        duplicate.children[1].subject = TypedLayoutSubjectV1::C0IndexedRow;
        duplicate.children[1].closure_sha256 = duplicate.children[1].calculated_closure_sha256();
        duplicate.root_sha256 = duplicate.calculated_root_sha256();
        assert!(duplicate.validate().is_err());

        let mut cross_bucket = fixture.clone();
        cross_bucket.children[1].bucket = "different-bucket".to_owned();
        cross_bucket.children[1].closure_sha256 =
            cross_bucket.children[1].calculated_closure_sha256();
        cross_bucket.root_sha256 = cross_bucket.calculated_root_sha256();
        assert!(cross_bucket.validate().is_err());

        let mut schema = fixture;
        schema.children[0].schema_sha256 = "ee".repeat(32);
        schema.children[0].closure_sha256 = schema.children[0].calculated_closure_sha256();
        schema.root_sha256 = schema.calculated_root_sha256();
        assert!(schema.validate().is_err());
    }

    #[test]
    fn placement_binds_serialized_root_generation_and_content() {
        let fixture = fixture();
        let bytes = serde_json::to_vec(&fixture).expect("serialize fixture");
        let locator = TypedLayoutPlacementLocatorV1::seal(
            fixture.fixture_id.clone(),
            fixture.root_sha256.clone(),
            fixture.project.clone(),
            fixture.bucket.clone(),
            fixture.region.clone(),
            "runs/rfc0048-v1".to_owned(),
            "runs/rfc0048-v1/root.json".to_owned(),
            "1788000000000001".to_owned(),
            u64::try_from(bytes.len()).expect("root length"),
            content_sha256(&bytes),
        )
        .expect("seal placement");
        let encoded = serde_json::to_vec(&locator).expect("serialize placement");
        let decoded = decode_typed_layout_placement(&encoded, &locator.envelope_sha256)
            .expect("decode placement");
        assert_eq!(
            decoded.root_revision().version.as_deref(),
            Some("1788000000000001")
        );
    }

    #[test]
    fn checked_in_compatibility_fixture_and_corruption_are_stable() {
        let expected = fixture();
        let bytes = include_bytes!("../fixtures/typed-layout-fixture-v1.json");
        let decoded = decode_typed_layout_fixture(bytes, &expected.root_sha256)
            .expect("decode checked-in fixture");
        assert_eq!(decoded, expected);

        let corrupt = include_bytes!("../fixtures/typed-layout-fixture-v1-corrupt.json");
        assert!(decode_typed_layout_fixture(corrupt, &expected.root_sha256).is_err());

        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../evals/schema/typed-layout-fixture-v1.schema.json"
        ))
        .expect("fixture schema JSON");
        let validator = jsonschema::validator_for(&schema).expect("fixture schema");
        let value: serde_json::Value = serde_json::from_slice(bytes).expect("fixture JSON");
        validator.validate(&value).expect("schema-valid fixture");
    }

    #[test]
    fn independent_oracle_digest_schema_and_traces_are_frozen() {
        let bytes = include_bytes!("../../../evals/oracles/t28-layout-geometry-v1-oracle.json");
        let oracle = decode_t28_layout_oracle(
            bytes,
            "b09eeeb482509b24ccb5e7f0c4a4d905983a612b0dbac2253519d9d82a98df86",
        )
        .expect("decode independent oracle");
        assert_eq!(
            oracle.fixture.canonical_history_sha256,
            "d4be64434f6b69990a2787876f514c6036727b41dcf1c5e120f91b6ce968ecd4"
        );
        assert_eq!(oracle.fixture.live_row_count, 15_742);

        let mut poisoned = serde_json::to_value(&oracle).expect("oracle value");
        poisoned["traces"][0]["expected_outcomes_sha256"] =
            serde_json::Value::String("00".repeat(32));
        let bytes = serde_json::to_vec(&poisoned).expect("poisoned oracle");
        assert!(decode_t28_layout_oracle(&bytes, &content_sha256(&bytes)).is_err());
    }

    #[tokio::test]
    async fn measured_child_reads_always_reach_provider_with_generation() {
        let mut objects = Vec::new();
        let mut stored = BTreeMap::new();
        for (role, key, generation, bytes) in [
            (
                TypedLayoutObjectRoleV1::Data,
                "c0/data.okvb",
                "101",
                b"data".as_slice(),
            ),
            (
                TypedLayoutObjectRoleV1::Index,
                "c0/index.okvi",
                "102",
                b"index".as_slice(),
            ),
            (
                TypedLayoutObjectRoleV1::Manifest,
                "c0/manifest.json",
                "103",
                b"manifest".as_slice(),
            ),
        ] {
            stored.insert(
                key.to_owned(),
                (Bytes::copy_from_slice(bytes), generation.to_owned()),
            );
            objects.push(TypedLayoutObjectIdentityV1 {
                role,
                key: key.to_owned(),
                generation: generation.to_owned(),
                length: u64::try_from(bytes.len()).expect("object length"),
                sha256: content_sha256(bytes),
            });
        }
        let child = TypedLayoutChildV1::seal(
            TypedLayoutSubjectV1::C0IndexedRow,
            "doss-objectkv-dev-okv-evals".to_owned(),
            "aa".repeat(32),
            "objectkv.t28.typed-row.v1".to_owned(),
            "bb".repeat(32),
            5,
            "c0/manifest.json".to_owned(),
            vec![
                TypedLayoutCapabilityV1::Point,
                TypedLayoutCapabilityV1::ProjectedScan,
            ],
            objects,
        )
        .expect("seal child");
        let raw: Arc<dyn Backend> = Arc::new(TestGenerationBackend { objects: stored });
        let observed = Arc::new(ProviderAttemptBackend::new(raw, "c0").expect("attempt backend"));
        let backend = GenerationPinnedChildBackend::new(observed.clone(), &child)
            .expect("generation backend");

        let read = backend
            .get("c0/data.okvb", Some(1..3), None)
            .await
            .expect("pinned range read");
        assert_eq!(read.bytes, Bytes::from_static(b"at"));
        let events = observed.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].phase, ProviderAttemptPhase::Started);
        assert_eq!(
            events[0]
                .expected_revision
                .as_ref()
                .and_then(|revision| revision.version.as_deref()),
            Some(child.objects[0].generation.as_str())
        );

        let wrong = RevisionToken {
            e_tag: None,
            version: Some("999".to_owned()),
        };
        assert!(backend
            .get("c0/data.okvb", Some(1..3), Some(&wrong))
            .await
            .is_err());
        assert!(backend.list("c0").await.is_err());
        assert!(backend
            .put("c0/new", Bytes::from_static(b"new"), WriteCondition::Create)
            .await
            .is_err());
        assert_eq!(observed.events().len(), 2);
    }
}
