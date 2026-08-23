# objectKV 12-hour strategy audit

Status: `[ACTIVE-WORK]` pinned overnight evidence run.

## Directional decision

Continue objectKV for one bounded vertical proof cycle. Narrow the current
claim and the current SlateDB posture:

- `[EXISTS]` Individual semantics, replicated recovery, publication, S3
  authority, and HTAP overlay mechanisms pass their isolated gates.
- `[ACTIVE-WORK]` The untuned SlateDB physical incumbent crosses its own stop
  threshold at 64 MiB because reopen scans more than the logical dataset.
- `[PROPOSED]` No complete transaction path yet connects `CommitEnvelope`, OCC,
  the real OpenRaft process log, objectification, the `C/O` frontier, WAL pop,
  and an empty-cache serving read.
- `[PROPOSED]` PostgreSQL and ZebraDB HTAP are two independent proofs. A literal
  PostgreSQL bridge keeps PostgreSQL WAL, LSN, and tuple MVCC authoritative;
  the object page store is subordinate materialization. The HTAP path separately
  requires a durable snapshot manifest, lease, and analytical-tail source.

Confidence is directional, not a product feasibility score: 58 percent that a
bounded FoundationDB-like cell with object-native permanent bulk storage is
feasible, and 20 percent that the current implementation proves a complete
cell.

## Evidence entering the run

Candidate `361a0fd` repairs the Phase 0 measurement boundary. It measures old
instance close, new instance open, first correct read, cold reads, and final
close independently. Raw artifacts include `run_id` and cannot overwrite a
prior run.

| Phase | Current result | Interpretation |
|---|---|---|
| Physical scale | 1 MiB: 4.85 ms; 8 MiB: 6.19 ms; 64 MiB: 424.13 ms | 64 MiB reopen reads 210,773,938 bytes before the first point read. Stop the untuned incumbent and permit one bounded layout/configuration pass. |
| S3 authority | MinIO run `3f3489e0` keeps 44 checks with zero anomalies | S3 protocol semantics are plausible locally. This is not cloud durability or latency evidence. |
| Generation recovery | Run `cf46b738` keeps 48 checks with zero anomalies | Certificate fencing and process replay are credible in isolation. |
| Publication recovery | Run `c2beebfe` keeps 42 checks with zero anomalies | Lost Publish reply, authority failover, and empty-scratch publisher recovery are credible in isolation. |
| HTAP overlay | Run `3b43b102` keeps 24 checks, four peak buffered rows, and no spill | The streaming merge is directionally correct. It is not yet a durable snapshot source or a PostgreSQL integration. |

The warm-cache, single-signer, convergence-only publication, and materialized
HTAP controls must each discard once at the start of the overnight run.

## What the 12 hours measure

`experiments/overnight_strategy_audit.sh` pins one clean Git candidate and one
suite hash per lane. At 30-minute cadence it runs:

1. the repaired SlateDB 1, 8, and 64 MiB scale points;
2. the pinned MinIO authority contract;
3. real-process generation certificate handoff;
4. lost-publication-response recovery through real OpenRaft processes;
5. the bounded DataFusion streaming overlay.

Every run exports OTel signals and writes a compact result, raw physical
artifact, log, append-only JSONL record, rolling summary, and status file under
one `/tmp/okv-overnight-strategy-*` directory. Source identity drift stops the
run instead of producing incomparable receipts.

This run measures reproducibility, safety regressions, latency dispersion, and
physical scale shape in mechanisms that already exist. It cannot prove the
missing semantic-to-Raft transaction path, `C/O/WAL` recovery composition,
PostgreSQL page bridge, or durable HTAP snapshot source.

## Morning decision table

| Outcome | Decision |
|---|---|
| Every normal run keeps; every control discards; repaired counters vary under 2 percent; relative latency MAD is at most 5 percent | Continue to one SlateDB layout/configuration pass, then MinIO physical and cloud profiles. Continue the vertical transaction proof. |
| Existing safety gates hold, but physical reopen remains dataset-sized or noise exceeds the thresholds | Narrow SlateDB to a reference or replace it. Continue objectKV semantics and publication work. Do not claim Gate 1. |
| Any acknowledged loss, stale-generation acceptance, publication reconstruction error, mixed HTAP snapshot, or negative control keeps | Stop the affected lane immediately and diagnose before adding scope. |
| The next vertical proof requires synchronous object publication on every commit, unbounded retained WAL, or two commit authorities | Narrow the architecture to an object-native storage/publication layer over an existing transaction authority. |

## Next falsifiers after the overnight receipts

1. Send real multi-range `CommitEnvelope` transactions through a co-located
   read-version service, commit proxy, resolver, and the existing three-process
   Raft log. Differentially check 1,000 concurrent histories across three
   seeds, including leader death and lost replies.
2. Materialize committed envelopes, publish a root, advance `O`, pop only the
   proven WAL prefix, and reconstruct exact `Database(C)` from objects plus the
   retained suffix on an empty worker.
3. Run one bounded SlateDB block/index/compaction configuration pass. Stop using
   SlateDB as the incumbent if 64 MiB reopen still reads the dataset or cold
   1 KiB points exceed eight requests or 512 KiB.
4. Pin a PostgreSQL revision and implement a tracing `smgr` dispatch wrapper
   before routing one non-default tablespace through a Rust page server.
5. Put the existing DataFusion operator behind an immutable manifest, snapshot
   lease, schema transform, partition epoch, and storage-level `(min(Wp), T]`
   tail reads.

## Tradeoff

This sequence optimizes for discovering a fatal composition problem before
building more database roles. It gives up a broader demo and a single blended
score. Each lane can continue, narrow, or stop independently.
