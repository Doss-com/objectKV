# Provider-bound performance readout

Status: `[EXISTS]` five-seed provider-bound local cache curve and six identity
controls, `[ACTIVE-WORK]` remote GCS latency, OTel, cleanup, and cost.

## Verdict

Continue. The measured local curves support objectKV as a cached compute system
whose authoritative rebuild state lives on objects. They do not support an
architecture that performs object-store I/O on every OLTP operation.

The strongest result is that relation size is no longer on the first-read
critical path. A fresh process opened an exact 512 MiB PostgreSQL relation view
in 4.75 ms, served an immutable-base point in 0.142 ms, and used less than 68
MiB median RSS. A complete 555 MB byte audit still took 1.046 seconds. The
correct performance shape is therefore selected metadata plus touched blocks,
with whole-closure audit and compaction outside request latency.

The economic result is conditional. The new provider-bound run shows that RAM
and persistent NVMe remove backend data GETs after a 128-point working set is
filled. Eviction causes backend refill, so hit rate, reuse distance, and the 64
KiB cache part size are the controlling variables. GCS has not run. The current
`gcloud` identity now fails non-interactive token refresh, so project, bucket,
latency, OTel, cleanup, and price gates remain unverified.

## Provider-bound local baseline

Candidate `ae515ec` ran the frozen 32 MiB, 4,096-key workload through an exact
revision facade in release mode. Each state used five fixed seeds, 128 warmup
reads, 1,000 measured reads over that same working set, and one deterministic
replay in a separate process. Every correct-state gate passed. All six controls
discarded.

| Cache state | View ready p50 | First point p50 | Warm p50 | Warm p99 | First 8-point range p50 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Persistent NVMe warm, decoded RAM cold | 0.831 ms | 83.8 us | 65.7 us | 85.0 us | 0.566 ms |
| Metadata warm, data cold | 0.557 ms | 0.362 ms | 63.5 us | 86.1 us | 0.944 ms |
| Empty process and object cache | 2.873 ms | 0.339 ms | 62.8 us | 84.1 us | 1.086 ms |

The request receipt is as important as latency:

| Cache state | Provider work through first point | One-time working-set fill | Measured warm reads |
| --- | ---: | ---: | ---: |
| Persistent NVMe warm | 2 metadata GETs, 788 B | 40 GETs, 2.35 MB before reopen | 0 GETs, 0 B |
| Metadata warm | 3 GETs, 66.3 KB after prepared metadata | 40 GETs, 2.35 MB including preparation | 0 GETs, 0 B |
| Empty cache | 8 GETs, 380,519 B from activation through first point | 38 GETs, 2.35 MB through fill and scan | 0 GETs, 0 B |

At the current listed GCS Class B price, eight cold-start GETs cost about
$0.0000032 per replacement worker and a 38-GET working-set fill costs about
$0.0000152. Two metadata GETs per NVMe-warm reopen cost $0.80 per million
reopens. The measured warm reads add no provider request charge. These are
request-only projections from the local count; GCS latency, transfer policy,
storage, compute, compaction, and telemetry are still excluded.

The empty-cache activation plus first point meets the frozen local shape at the
request boundary, exactly eight provider GETs and below 512 KiB. This is a
local versioned-store count, not a GCS latency pass. The maximum observed RSS
was 451 MB. That is below the 1 GiB safety bound, but it includes the monolithic
eval binary and does not establish the Range Engine's production memory floor.

The six discarded controls were changed manifest generation, same SST bytes at
a new generation, missing revision, changed SST bytes, changed namespace, and
skipped revision enforcement. Every admitted provider GET had a matching exact
revision check. SlateDB's bootstrap `LIST` is now synthesized only from the
authority-selected closure, and its mutable `manifest.boundary` discovery hint
cannot alter an immutable serving view.

## What is fast now

All values below are optimized local arm64 measurements with exact-result hard
gates. They are not cloud latency claims.

| Path | Measured result | Interpretation |
| --- | ---: | --- |
| Provider-bound persistent-NVMe reopen, first point | 83.8 us p50, 123.8 us five-seed max | local data tier can serve an exact data read without a remote data GET |
| Provider-bound persistent-NVMe reopen, warm p99 | 85.0 us median across seeds | current engine lookup overhead is comfortably sub-millisecond |
| Persistent-NVMe reopen, backend data after open | 0 bytes, 0 range GETs | authenticated local blocks can remove object data from the hot path |
| Raw 16K-key first point | 111 us median | local OS-warm object lookup is small; remote RTT is absent |
| Shared-cache 16K-key scan | 236K to 238K rows/s | one cached ordered scan stays exact with 0 or 64 tail records |
| Raw 16K-key scan with 1,024 tail records | 186K rows/s, 80 GETs | streaming merge prevents analytical tail size from multiplying base reads |
| 512 MiB PostgreSQL view ready | 4.75 ms | selected metadata work grew only 2.04x while closure bytes grew about 511x |
| 512 MiB first base point | 0.142 ms | point lookup does not scan the relation |
| 512 MiB first eight-page range | 0.621 ms | bounded local range read remains sub-millisecond |
| 512 MiB complete closure audit | 1.046 s | whole-byte proof scales with bytes and must stay off readiness |

The provider-bound persistent-NVMe result still needed two metadata GETs and
788 bytes during open. It proves data acceleration, not offline serving. The
provider-bound root added in candidate `35ef183`, and exercised end to end by
candidate `ae515ec`, makes those metadata and block requests exact rather than
trusted.

## Request and byte shape

The configured 64 MiB SlateDB candidate reduced fresh-open bytes from 210.8 MB
to 402 bytes. It increased the first point from three to five requests while
reducing first-point bytes from 1.40 MB to 210 KB. This is the right trade when
remote bytes and startup matter, provided the request price and latency fit.

The bounded eviction gate is the adverse case. A 192 KiB cache served a roughly
2 MiB working set across eight logical ranges. Reverse rereads of 64 values
issued 130 backend range GETs and transferred 8.41 MB, about 2.03 GETs and 131
KB per logical read. The results stayed exact and the cache stayed bounded, but
this is the remote-refill curve that can make the product slow if working sets
do not fit the RAM and NVMe budget.

Compaction is currently favorable locally. The admitted 8 MiB path read 8.61
MB, wrote 8.62 MB, and produced 1.027x maintenance write amplification through
both local filesystem and MinIO. Public-cloud compaction throughput and cost
remain unmeasured.

## GCS request economics

The current GCS Standard single-region flat-namespace list price is $0.0004 per
1,000 Class B operations and $0.005 per 1,000 Class A operations. GET and object
metadata reads are Class B; object writes and listings are generally Class A.
See the [official Cloud Storage pricing table](https://cloud.google.com/storage/pricing).

At that price, request cost alone is:

| Shape | Request cost per million logical operations |
| --- | ---: |
| One GCS GET per point | $0.40 |
| Five GCS GETs per cold point | $2.00 |
| Eight GCS GETs per cold point | $3.20 |
| One PUT per transaction | $5.00 |
| 97.5% cache hits, one GET per miss | $0.01 |

The frozen $0.01 per million warmed-point target therefore means at least a
97.5 percent object-data cache hit rate when a miss costs one GET. It is not a
claim that GCS GETs themselves cost $0.01 per million.

The larger economic danger is one object PUT per transaction. At 10,000 writes
per second, that shape would create 864 million Class A operations per day and
about $4,320 per day in request charges before storage or compaction. Immutable
flush objects must batch many mutations. For illustration, 10 MiB/s of logical
ingest packed into 8 MiB objects is roughly 108,000 PUTs per day, about $0.54
per day in Class A request charges. This arithmetic excludes manifests,
compaction, deletes, replication, compute, and bytes. It establishes why flush
size is an architectural performance parameter.

## Provider-bound integrity now exists locally

RFC 0066 and candidate `35ef183` add:

- a version-2 authority root that binds provider, namespace, exact revision,
  object length, and publication SHA-256 for the manifest and live SSTs;
- a read-only object-store facade that forces every full or range GET onto the
  selected revision;
- byte-exact version-1 and version-2 fixtures;
- refusal for same-key overwrite with identical bytes, changed bytes, missing
  identity, and changed provider namespace;
- request, revision-check, refusal, byte, latency, and cost metric contracts.

GCS exposes immutable object generation through Apache `object_store` as the
generic version field. The adapter sends that version back as the `generation`
query parameter. Google documents generation-match failure as HTTP 412 when
the selected generation differs. See [Cloud Storage request preconditions](https://cloud.google.com/storage/docs/request-preconditions).

The driver does not expose GCS CRC32C on the current read result. The admitted
identity is therefore GCS generation plus objectKV's publication-time SHA-256.
No provider-checksum claim is made.

## Performance bounds and stop conditions

`[BOUND]` Hot OLTP can be fast only when RAM or NVMe absorbs the request. A GCS
miss adds network and tail latency that local measurements do not contain.

`[BOUND]` Cold point work must remain proportional to index depth and touched
blocks, not database size. The first GCS gate is at most eight GETs, at most 512
KiB, and in-region p99 below 100 ms for one cold 8 KiB point.

`[BOUND]` The $0.01 per million warmed-read target requires at least 97.5
percent cache hits under a one-GET miss model. A materially worse hit rate means
either spend more NVMe, add a regional cache, accept a higher cost target, or
narrow objectKV to colder workloads.

`[BOUND]` Immutable page deltas prove the architectural crossover but not the
encoding. One changed 8 KiB page remains about 91 KB in JSON v1, 11.106x the
changed bytes. Compact binary v2 must reach at most 2x before the format is
admitted.

`[STOP]` Redesign the read layout if the 32 MiB GCS fixture scans the dataset
before first read, exceeds eight GETs or 512 KiB for a cold point, or cannot
reject a changed generation.

`[STOP]` Redesign caching or narrow the workload if steady-state miss ratios
make remote p99 dominate OLTP or fail the declared cost envelope.

`[STOP]` Reject one-object-per-transaction flushing. The physical writer must
batch and compact against measured request, latency, and write-amplification
curves.

## Next exact experiment

Run `provider-bound-range-read-v0` on `gcs-dev` in three states:

1. metadata warm, data cold;
2. persistent NVMe warm, decoded RAM cold;
3. decoded RAM and persistent object cache empty, with OS state reported.

For every state, record view-ready, first point, first eight-page range, warm
p50/p99, GET count, transferred bytes, cache bytes, exact generation checks,
refusals, peak RSS, and estimated cost. Run six identity controls and require
OTel metrics, traces, logs, and scratch-prefix cleanup.

The external blocker is specific: project `doss-objectkv-dev` returns a
permission denial to the previously available application-default identity,
and the current `gcloud` account cannot refresh its token non-interactively.
The candidate bucket `doss-objectkv-dev-okv-evals` was not present in the last
successful inventory. Local implementation can continue, but no GCS latency or
dollar result is valid until authentication, project access, and the scratch
bucket are available.

After that run, add a reuse-distance and cache-capacity curve. The current
1,000 measured reads repeat a 128-point warmed working set and therefore prove
the cache-hit path, not a production hit ratio. Also repeat provider-bound
activation at multiple dataset sizes. The existing suite bounds 32 MiB startup
bytes but does not yet prove that provider-bound pre-first-read bytes stay flat
as the closure grows.
