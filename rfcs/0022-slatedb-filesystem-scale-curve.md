# RFC-0022: SlateDB filesystem scale curve

- Status: proposed
- Authors: DOSS
- Created: 2026-08-23

## Decision

Measure the RFC-0021 SlateDB filesystem incumbent at fixed 1 MiB, 8 MiB, and
64 MiB logical datasets before interpreting its reopen time or object-I/O
totals. The scale curve is an early directional indicator, not a Gate 1 pass.

## Context and invariant

One dataset size cannot distinguish bounded metadata bootstrap from a reopen
path that grows with all durable bytes. It also cannot show whether request and
write amplification are fixed costs or dataset-proportional costs. Every point
on the curve must preserve the RFC-0021 logical oracle and fresh-cache gate.

## Proposed contract

The three profiles use the same deterministic generator and one seed, `1103`:

| Profile | Logical bytes | Keys | Budget |
|---|---:|---:|---:|
| `scale-1mib` | 1 MiB | 1,024 | 30 seconds |
| `scale-8mib` | 8 MiB | 8,192 | 60 seconds |
| `scale-64mib` | 64 MiB | 65,536 | 300 seconds |

Each profile runs deterministic ingest plus flush, warm point reads, a 100-row
ordered scan, close, new-instance reopen, first exact read, and cold point
reads. The result records the exact candidate, profile hash, request classes,
bytes, and logical receipt.

## Failure model

The curve covers only local filesystem objects and process-local cache
replacement. It does not cover remote latency, compaction debt, process death,
disk faults, or provider throttling.

## Alternatives

- Repeat only 8 MiB. This measures noise but not scale shape.
- Jump directly to 10 GiB. This increases realism but makes an instrumentation
  or oracle error expensive before the curve is understood.
- Compare across different machines. This increases samples but confounds the
  physical shape with hardware and filesystem differences.

## Eval plan

The fixed suite is `evals/suites/phase0-slate-filesystem-scale.toml`. The
primary metric remains first-correct-read duration. All logical and cache-state
gates remain hard. The one-time overnight interpretation is:

- continue if all points are exact and reopen growth is sublinear in bytes;
- narrow toward metadata/index redesign if reopen time or requests grow close
  to linearly with logical bytes;
- stop the current SlateDB incumbent path if correct reopen requires scanning
  or copying the complete dataset.

No numeric product ceiling is frozen until the first curve exists.

## Compatibility and migration

Any change to the dataset sizes, key counts, generator, SlateDB revision,
object-store accounting, or fresh-cache procedure changes the suite hash and
starts a new curve.

## Unresolved questions

- Which curve point first triggers SlateDB compaction under controlled settings?
- Which remote dataset sizes are sufficient to separate latency from request
  amplification on MinIO and GCS?
- What named PostgreSQL or Redis workload should set the first cost ceiling?
