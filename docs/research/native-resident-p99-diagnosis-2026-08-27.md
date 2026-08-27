# Native resident p99 diagnosis, 2026-08-27

Status: `[EVALUATING]` local mechanism diagnosis. No cloud admission claim.

## Question

Does GP3.1 measure the native snapshot boundary, or does it also charge the
candidate for six live authority processes that the direct RocksDB control does
not run?

## Reproduction

The verified GCP R0 receipt compared the full recovered native path with a bare
direct RocksDB process. Candidate throughput passed at `0.8411x` and `0.8268x`.
P99 failed at `1.2104x` and `1.2717x`.

A local engine-only probe then put the same deterministic data behind
`ResidentRangeEngine::snapshot` without the authority topology. Its point-read
latency did not show a stable penalty against direct owned-value RocksDB. This
lowered the probability that column families, dynamic dispatch, or the two
transition-epoch loads alone explained the GCP miss.

## Matched-topology control

The new control performs exact owned-value `DB::get` operations inside the same
replacement worker after the same object-base verification, txLog catch-up,
one killed worker, one empty replacement, and six live authority processes. It
does not change the old suite or receipt.

```text
same object base + same txLog + same six authorities
                         |
                 recovered worker
                   /           \
       native bound snapshot   direct owned DB::get
                   \           /
             same keys, values, order, samples
```

The first dirty local order used nine samples per subject:

| Metric | Native | Matched control | Ratio |
|---|---:|---:|---:|
| Throughput | 1,455,937 reads/s | 1,657,583 reads/s | 0.878x |
| p50 | 625 ns | 542 ns | 1.153x |
| p95 | 875 ns | 750 ns | 1.167x |
| p99 | 1,041 ns | 916 ns | 1.136x |
| p99.9 | 1,584 ns | 1,292 ns | 1.226x |

The reverse dirty local order also used nine samples per subject:

| Metric | Native | Matched control | Ratio |
|---|---:|---:|---:|
| Throughput | 1,435,714 reads/s | 1,663,092 reads/s | 0.863x |
| p50 | 625 ns | 542 ns | 1.153x |
| p95 | 875 ns | 750 ns | 1.167x |
| p99 | 1,083 ns | 875 ns | 1.238x |
| p99.9 | 2,208 ns | 1,292 ns | 1.709x |

All workload gates passed. The first order cleared the throughput and p99
bounds. The reverse order cleared throughput but failed p99. These are dirty
laptop diagnostics, so neither result is an admission receipt.

## Failure found during preflight

One multi-seed attempt failed while measuring local bytes. RocksDB removed an
obsolete file between directory enumeration and metadata lookup. The byte
snapshot now suspends RocksDB file deletion during enumeration and always
resumes it. Four `okv-serving-rocksdb` tests passed, and the failure did not
recur across the two complete multi-seed orders.

## Decision

The native snapshot has a visible fixed cost, about 83 ns at p50 and 125 ns at
p95 on this host. The local p99 is order-sensitive and is not sufficient to
accept or reject it. GP3.1 now uses a separate matched-topology suite with the
original `0.80x` throughput floor and `1.20x` p99 ceiling.

Next action: run both orders from one clean revision on the existing GCP R0
local-NVMe topology, with fifteen samples per subject, one machine receipt, one
batch per order, and required OTel logs, metrics, and traces.
