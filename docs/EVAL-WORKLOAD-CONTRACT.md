# Evaluation workload contract

Status: `[CODE-COMPLETE]` in the evaluator, `[EVALUATING]` across the full suite catalog.

## Decision

An objectKV performance or economics result is comparable only when both the
candidate and control receipts carry `evidence_class = "workload"` and a
content hash of a validated workload profile. A smoke run proves that the
binary, deployment, credentials, and telemetry path work. It cannot verify a
performance curve.

The three evidence classes are:

| Class | What it can prove | What it cannot prove |
|---|---|---|
| `contract` | Semantics, invariants, negative controls, and component behavior | Deployment health or representative performance |
| `smoke` | End-to-end wiring and a bounded preflight | Performance or economics admission |
| `workload` | A named performance or economics point against a matched control | Behavior outside its declared envelope |

Older receipts remain schema-readable. If they omit the evidence class or
workload-profile hash, the comparison engine marks a performance or economics
comparison invalid.

## Required workload envelope

Every workload profile declares and hashes all of the following:

```text
dataset identity and size
  + operation mix
  + access distribution
  + client concurrency
  + warmup window
  + measured window
  + cache state
  + failure schedule
  + resource limits
  + matched control
  + required metrics
  + at least five repeats
  = one bounded workload claim
```

Validation fails closed on missing fields, an operation mix that does not sum
to 1.0, duplicate or zero concurrency points, empty failure schedules,
non-positive resource limits, duplicate required metrics, unknown datasets,
or fewer than five repeats.

At run selection, the evaluator also rejects a workload whose declared
distribution or client count is outside the hashed workload envelope. This
prevents a suite from naming Zipf or high concurrency while executing a hotset
or single-client command.

## Execution rule

Setup is outside the measured window. Dataset construction, object download,
worker activation, compaction, and cache preparation are reported separately.
Counters used for admission are sampled immediately before and after the
measured window. Candidate and control use the same dataset, trace, operation
budget, resource ceiling, machine receipt, source revision, and durability
scope unless the gate explicitly names the difference under test.

Every workload receipt must answer:

1. What exact system state existed before warmup?
2. What exact operations ran, in what proportions, and at what concurrency?
3. What changed during measurement, including scheduled failures?
4. Which CPU, memory, local-media, network, and object-store limits applied?
5. Which correctness, latency, throughput, resource, and cost metrics were required?
6. What matched control makes the reported ratio meaningful?

## Calibration and admission

A workload runner moves through three stages:

```text
[SMOKE] wiring preflight
  -> [EVALUATING] calibration workload
  -> [VERIFIED] frozen admission workload and matched control
```

Calibration uses the same workload schema but does not move a program gate to
`[VERIFIED]`. It finds steady-state window sizes, validates metric attribution,
and establishes expected variance. Admission freezes the suite hash, source,
machine, dataset, control, and thresholds before execution.

## Workload families

The suite catalog should not collapse different products into one blended
benchmark. Each family gets an independent envelope and specialist control:

| Family | First representative operations | First control |
|---|---|---|
| KV and Redis-like | point get/set, mixed read-write, scan, hot-key contention | direct RocksDB or Redis-compatible specialist |
| Log and WAL | append, group commit, tail read, retention, recovery | local replicated log with the same durability |
| PostgreSQL OLTP | indexed point read, short transaction, secondary index, constraint conflict | PostgreSQL or TiKV-backed relational path on matched durability |
| DataFusion OLAP | selective scan, broad scan, aggregate, join, base-plus-tail overlay | Parquet-only DataFusion and row-store control |
| Virtual filesystem | metadata lookup, small-file create, range read, directory scan, snapshot branch | local filesystem or object-backed specialist |
| Lifecycle | empty-worker rebuild, objectification debt, branch, restore, media loss | full hydration and provider-native restore |

The performance matrix records a cell only after its own workload envelope and
control pass. Results from one family do not stand in for another.

## Current application

`single-range-native-concurrency-admission-v1` is the first existing suite
annotated with the contract. Its local profile is `smoke`; its GCP profile is a
five-repeat workload covering 8 and 32 clients, 24 million measured point
reads across the admitted receipt set, explicit worker replacement before
warmup, a resident 128 MiB cache and local-byte ceiling, and direct owned-value
RocksDB as control.

T27 remains `[EVALUATING]`. Its next workload profile expands this baseline to
larger-than-cache fixtures, deterministic Zipf traces, measured-window cache
and I/O deltas, and the 64 MiB calibration then 1 GiB admission sequence frozen
in RFC-0043.

`native-resident-cache-pressure-calibration-v1` now freezes the first of those
points as GP3.1.2: 64 MiB logical data, 32 MiB block cache, Zipf 1.4, eight
clients, one million measured reads per sample, three seeds, five repeats, and
direct owned-value RocksDB control. Fixture reuse, process CPU and I/O
attribution, and mismatched-cache plus counter-reset negative controls are
`[CODE-COMPLETE]`. It remains `[EVALUATING]` until the paired clean GCP run
exports required OTel signals and preserves its bounded evidence.
