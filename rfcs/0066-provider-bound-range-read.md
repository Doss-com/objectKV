# RFC 0066: Provider-bound range reads

- Status: active work, local identity controls admitted
- Authors: objectKV contributors
- Created: 2026-08-24
- Supersedes: none

## Decision

Add a version-2 persistent range root that binds every immutable object in the
selected closure to a provider namespace, an exact provider revision, its
length, and its publication-time SHA-256. A lazy Range Engine may serve through
that root only when its object-store adapter enforces exact-revision reads on
every touched object. The version-1 root remains readable through the existing
eager full-closure audit and is never silently promoted to lazy serving.

For GCS, the exact revision is the immutable object generation exposed by
Apache `object_store` as `ObjectMeta::version` and sent back on reads as the
`generation` query parameter. The current driver does not expose GCS CRC32C.
This contract therefore binds GCS generation plus objectKV's application
SHA-256. It does not claim that a provider checksum was observed.

## Context and invariant

RFC 0065 separates replacement-worker readiness from a complete physical
audit. Its local result reaches an exact 512 MiB relation view in 4.75 ms and
serves the first immutable-base point in 0.142 ms, while the complete 555 MB
closure audit takes 1.046 seconds. That establishes that SlateDB open and
lookup do not intrinsically scan the relation. It does not authorize the lazy
path on remote object storage because a manifest digest alone does not prove
that later SST range reads still name the published objects.

The invariant is:

```text
authority-selected provider-bound root
  -> exact provider namespace
  -> exact manifest generation and application digest
  -> exact generation, length, and application digest for every live SST
  -> every touched read requests the selected generation
  -> exact base plus authenticated txLog overlay at T
```

No same-key overwrite, missing revision, changed provider namespace, changed
bytes, stale descriptor, or unversioned fallback may become visible state.

## Proposed contract

### Persisted identity

The version-2 root contains:

```text
ProviderBoundAuthorityRangeRootV2
  format_version = 2
  logical_root = AuthorityRangeRoot
  provider = gcs | s3 | versioned-test
  namespace = bucket or equivalent immutable storage scope
  manifest_id
  manifest = { key, revision, length, sha256 }
  live_ssts[] = { key, revision, length, sha256 }
  provider_closure_sha256
```

The closure digest uses a canonical, versioned encoding over the provider,
namespace, manifest ID, manifest receipt, and live-SST receipts sorted by key.
The authority selects this digest as part of the root. Credentials, endpoint
tokens, trace IDs, and cache paths are never persisted.

### Read policy

The provider-bound object-store view is read-only. For each named object it:

1. refuses a missing or duplicate receipt;
2. requires an exact provider revision supported by the active backend;
3. applies the receipt revision to every full or range GET;
4. compares the returned revision, object length, and returned range;
5. relies on the publication SHA-256 for full-object audit and on the immutable
   provider generation for later range identity;
6. refuses reads for objects outside the authority-selected closure.

The first candidate does not make a cryptographic claim about an individual
range response. A provider that can mutate bytes within one generation is
outside the admitted threat model and requires authenticated blocks.

### Cache states

The same root and point/range workload runs in three states:

```text
metadata-warm/data-cold
persistent-NVMe-warm/decoded-RAM-cold
empty-cache
```

Metadata-warm retains the authority root and SlateDB metadata but no requested
data block. Persistent-NVMe-warm starts a new process against a retained,
previously authenticated local object cache. Empty-cache starts a new process
with neither decoded nor persistent object cache. OS page-cache state must be
reported and cannot be described as empty when it is merely process-cold.

### Economic receipt

Every state reports successful and failed request counts by API class, response
bytes, cache hit bytes, time to view ready, first exact point, first exact
eight-page range, p50/p99 after warmup, peak RSS, and estimated request cost.
The cost uses a pinned price snapshot named in the result. Transfer, storage,
and telemetry cost are reported separately or explicitly marked unavailable.

## Failure model

- Same key and same bytes at a new generation: refuse the stale selected
  generation if unavailable; never accept the new generation by digest alone.
- Same key and changed bytes at a new generation: refuse before visibility.
- Missing version or a backend incapable of version-selected reads: refuse lazy
  serving and retain the eager version-1 path.
- Wrong provider namespace or changed closure receipt: refuse root activation.
- Short range, wrong object length, or returned revision mismatch: refuse the
  read and revoke readiness for that view.
- Object-store timeout, throttle, or permission failure: expose a classified
  failed read. Do not fall back to an unversioned GET.
- Worker death: a replacement begins from the selected root and declared cache
  state. No durable state changes.
- Background full audit failure: mark the view failed and stop admitting new
  reads. Drain behavior remains future policy.

## Alternatives

### Full closure audit before readiness

Optimizes for the simplest fail-closed proof. Gives up dataset-size-independent
replacement readiness and pays remote read cost before the first request.

### Manifest identity only

Optimizes for minimal metadata. Gives up exact identity for the SSTs read after
the manifest is selected. Rejected.

### Provider revision without application digest

Optimizes for fewer publication reads. Gives up portable end-to-end identity
and makes correctness depend entirely on provider semantics. Rejected.

### Per-block Merkle authentication

Optimizes for a stronger storage threat model and portable touched-byte proof.
Gives up format simplicity and requires a new block index and migration. Keep as
the next option if provider revisions are unavailable or insufficient.

## Eval plan

The owning suite is `evals/suites/provider-bound-range-read.toml`. Five fixed
seeds run the three cache states locally through a deterministic versioned
object store and, when credentials exist, through `gcs-dev`. The primary metric
is empty-cache first-point latency. Correctness failures are never blended into
latency or cost.

Hard gates require exact point and range results, one authority-selected
provider closure, an exact revision on every remote read, zero unversioned
fallbacks, a bounded request and byte receipt, deterministic semantic replay,
and complete scratch-prefix cleanup. Controls for a changed generation, same
bytes at a new generation, missing revision, changed bytes, changed namespace,
and skipped revision enforcement must all discard.

Calibration targets for the first remote run are at most eight object GETs and
512 KiB transferred for an empty-cache first 8 KiB point, no dataset-sized
transfer before first read, p99 below 100 ms in-region, and request cost below
$0.01 per million warmed point reads under the pinned cache-hit model. These are
continue-or-redesign thresholds, not production claims. The suite must expose
the raw counts needed to recompute cost when provider prices change.

## Compatibility and migration

Version-1 fixtures remain byte-exact and decode through the eager-audit loader.
A version-2 reader accepts version 1 only in eager mode. Lazy mode requires
version 2 and a backend capability row that admits exact-revision reads. A
version-1 reader rejects version 2. Unknown root or provider-identity versions
fail closed.

Rollout is additive: publish and dual-read version 2, retain version 1 rollback,
then change production policy only after local controls and the GCS curve pass.
Rollback selects the prior version-1 root and restores the eager audit. No SST
rewrite is required because the new format binds existing immutable objects.

Frozen fixtures:

- `crates/okv-object/fixtures/persistent-range-base-v1.json`
- `crates/okv-object/fixtures/provider-bound-range-root-v2.json`

## Local identity and cache-state result

Candidate `35ef183` implements the version-2 root, publication-time full-byte
binding, canonical provider-closure digest, and read-only exact-revision object
facade. Four focused tests pass. They retain byte-exact v1 and v2 fixtures,
accept an exact range read, refuse the same bytes after an overwrite changes
revision, refuse changed bytes during binding, reject a changed namespace, and
keep v1 eager-only.

Candidate `ae515ec` executes the local profile with five fixed seeds and a
deterministic replay. Persistent-NVMe-warm, metadata-warm, and empty-cache
states all kept. Their first-point medians were 83.8 us, 0.362 ms, and 0.339 ms.
Warm p99 medians were 85.0 us, 86.1 us, and 84.1 us. The empty-cache path used
eight exact-revision provider GETs and 380,519 bytes from activation through
the first point. Persistent NVMe reduced post-reopen data GETs to zero, while
retaining two 788-byte metadata GETs. All six frozen controls discarded.

Candidate `be78904` replaces the GCS profile's discard stub with the same
process-isolated worker. It scopes every run to a guarded GCS prefix, binds
immutable reads to exact generations, requires the frozen OTel profile and
price snapshot, deletes all live objects before success, and lets the
controller retry cleanup after worker failure or timeout. Focused tests and
the local contract pass. This is implementation readiness, not a remote
result.

No GCS performance claim follows from this result. The current `gcloud`
identity cannot refresh its token non-interactively; prior inventory could not
access project `doss-objectkv-dev`, and candidate bucket
`doss-objectkv-dev-okv-evals` was absent. GCS latency, OTel, cleanup, and price
gates remain unexecuted. The 32 MiB profile also does not yet prove
provider-bound activation-byte independence across dataset sizes or a
production cache hit rate.

## Unresolved questions

1. Does GCS in-region empty-cache p99 meet the bound without a regional cache?
2. What fraction of reads hit persistent NVMe at realistic worker churn?
3. Is provider generation plus publication SHA-256 sufficient for the target
   threat model, or must the SST format add authenticated blocks?
4. Should a failed background audit drain existing readers or revoke them
   immediately?
