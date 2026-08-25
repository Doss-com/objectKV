# RFC 0067: Provider-bound cache economics

- Status: implemented; first local 25 percent stop-condition points discard
- Authors: objectKV contributors
- Created: 2026-08-24
- Supersedes: none

## Decision

Measure provider-bound point reads under fixed access traces and physically
bounded RAM plus persistent NVMe. The experiment varies cache capacity, access
skew, reuse distance, moving-hotset churn, and decoded-view reopen frequency.
It reports exact results, latency by hit tier, provider requests and bytes,
resident cache bytes, and request cost in one receipt.

Do not infer a production cache hit ratio from the 128-point warmed loop in RFC
0066. Do not optimize latency by increasing cache capacity during an experiment.

## Question

The first in-region GCS gate established three useful facts:

```text
GCS data miss                   48.6 ms median
persistent-NVMe point          0.295 ms median
warm point p99                 0.245 to 0.284 ms median
```

One GCS GET per miss costs $0.40 per million misses under the pinned price
snapshot. The frozen $0.01 per million logical read target therefore requires:

```text
miss ratio <= 0.025
hit ratio  >= 0.975
```

Keeping a 40 to 50 ms miss out of a sub-millisecond p99 generally requires a
stricter hit ratio above 0.99. The next gate must determine whether a practical
RAM and NVMe budget can produce either ratio under named, reproducible traces.

## Scope

The first contract is a read-only YCSB-C-style point workload over 4,096 keys
with 8 KiB values. It uses the RFC 0066 authority-selected provider closure,
exact provider revisions, a 64 KiB persistent-cache part, one bounded decoded
cache, and one bounded persistent cache.

The access traces are:

1. uniform random, the adverse no-locality control;
2. Zipfian with theta `0.99`, the named skewed baseline;
3. a moving hot set where 90 percent of reads target 10 percent of keys and the
   hot interval advances every 1,000 measured reads.

The capacity curve allocates persistent NVMe equal to 1, 5, 10, or 25 percent
of logical dataset bytes. Decoded RAM is fixed by the profile and reported
separately. One additional trace reopens the immutable view every 1,000 reads
with fresh decoded RAM while retaining the same persistent cache. This is a
view-churn result, not a replacement-process result.

## Measurement contract

For every fixed seed, the receipt records:

- dataset keys and logical bytes;
- trace distribution, parameters, and SHA-256;
- warmup and measured operation counts;
- cache capacity, settled bytes, and part count;
- exact logical results and oracle digest;
- cache hits, cache misses, and classified logical reads;
- hit ratio, miss ratio, and reuse-distance distribution;
- p50 and p99 for all points, cache hits, and provider misses;
- provider GETs, exact-revision checks, refused requests, and bytes;
- calculated request cost per million logical reads;
- decoded-view reopen count, peak RSS, and scratch cleanup.

A logical operation is classified as a provider miss only when its provider
request counter advances during that operation. Metadata work at view open is
reported separately and cannot be counted as a point miss or hit.

## Frozen profiles

The local profile executes 20,000 measured reads after 2,000 warmup reads for
five fixed seeds. The GCS profile executes 2,000 measured reads after 200
warmup reads for the same seeds. Cross-profile latency is not compared. Request
and byte ratios are comparable because the trace generator, value size, cache
part size, and semantic oracle are identical.

The suite may run one workload at a time. A cloud run is not required to replay
every knowingly adverse local curve. The first selected cloud points are the
10 percent Zipfian baseline, the 25 percent moving-hotset baseline, and the
decoded-view reopen trace.

## Hard gates

- Five fixed seeds and one exact replay are required.
- Every logical point result must match the deterministic value oracle.
- Every provider GET must carry the authority-selected exact revision.
- The classified hit and miss counts must sum to measured logical reads.
- Persistent cache bytes must remain within the workload capacity.
- The observed trace hash and reuse distances must replay exactly.
- Requested decoded-view reopens must execute with fresh decoded RAM.
- Request, byte, latency, RSS, and cost receipts are mandatory.
- GCS metrics, traces, logs, and scratch cleanup are mandatory.
- Four unsafe controls must discard.

The unsafe controls disable the persistent-cache bound, skip the exact-result
oracle, skip provider-revision enforcement, or perturb the replay trace.

## Calibration and stop conditions

The primary metric is provider miss ratio. The practical threshold is 2.5
percent, derived from the pinned request-cost target. This threshold is not a
correctness gate. A correct curve above it is a valid discarded economic
candidate and remains in the ledger.

Additional targets are:

```text
persistent-cache hit p99       <= 1 ms in-region
provider miss p99              <= 100 ms in-region
provider bytes per miss        <= 128 KiB
peak worker RSS                <= 1 GiB
```

Stop and revisit the product scope when the 25 percent cache cannot approach
the request-cost target under the named skewed or moving-hotset trace. Revisit
the 64 KiB part size when provider bytes per miss exceed the bound or byte
amplification dominates request cost. Do not hide an uneconomic curve with a
larger machine or cache profile.

## First local stop-condition results

`[EXISTS]` Candidate `5545bf5` ran both 25 percent stop-condition workloads
through five fresh worker processes and one exact trace replay. Every value,
provider identity, physical cache bound, trace, reuse-distance, RSS, and scratch
cleanup gate passed. Both economic constraints discarded.

| Trace | Miss ratio p50 | MAD | Provider GETs / logical read p50 | Projected GCS request cost / million reads | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| Zipfian `0.99`, 25 percent NVMe | 26.820% | 0.165 pp | 0.28915 | $0.11566 | discard |
| Moving 10 percent hotset, 25 percent NVMe | 14.535% | 0.075 pp | 0.15565 | $0.06226 | discard |

The request-cost projection applies the frozen $0.40 per million GCS Class B
GET price to the measured provider GETs per logical read. It is not a remote
latency measurement. Median provider bytes per miss were about 71 KiB for both
traces, below the 128 KiB bound. Transfer amplification is therefore not the
dominant failure in these subjects. Insufficient locality under passive demand
caching is.

The result rejects passive demand caching as the complete serving policy. It
does not reject object storage as the authoritative durability and rebuild
tier. The next mechanism must change locality explicitly, for example through
range placement, workload-informed prefetch, or a declared larger local-data
fraction. Do not spend a GCS run on the same admitted-bad miss curve.

Candidate `b9c8078` also made all four controls produce schema-valid discard
receipts. Disabled cache bounds, skipped exact-result checks, skipped provider
revision enforcement, and perturbed replay traces were each detected. During
this run the generic eval runner was also corrected to enforce lane metric
constraints. Before candidate `5545bf5`, a 26.71 percent miss result could be
incorrectly labeled `keep` because lane constraints were validated but not
executed.

## Alternatives

### Use only cache microbenchmarks

Optimizes for quick local numbers. Gives up provider request accounting,
immutable-view identity, view-open metadata, and exact logical results.
Rejected.

### Use an application trace immediately

Optimizes for direct product relevance. Gives up a reproducible public baseline
and may encode private workload data. Add public PostgreSQL, Redis, and search
traces after the synthetic contract is stable.

### Run every adverse curve on GCS

Optimizes for direct remote latency evidence. Gives up experiment cost and time
without changing the miss-ratio conclusion. Run representative cloud points
after the local curve identifies useful boundaries.

## Opens after this gate

- Actual process replacement with retained NVMe and empty decoded RAM.
- Concurrent readers sharing one cache budget across many tenant ranges.
- PostgreSQL buffer-access traces and DataFusion scan coexistence.
- Adaptive prefetch and admission policies as separate candidate experiments.
- Sustained write objectification and compaction against the same cost model.
