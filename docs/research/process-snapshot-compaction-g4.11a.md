# G4.11a durable process snapshot and journal compaction

- Status: `[CODE-COMPLETE]` mechanism, `[EVALUATING]` local dirty-source receipts
- Date: 2026-08-26
- Suite: `process-snapshot-compaction-v1`
- Candidate run: `7eeaa179-4b49-465a-9886-34af7501b0f4`
- Poison run: `8fb8a75a-d3d9-4f43-82d9-eb235cabbf34`

## Decision

Retain the crash-safe snapshot and canonical journal-rewrite mechanism. Do not
admit it as bounded Cell v0 storage yet. The journal curve collapsed, but the
unfrontiered state snapshot copied lifetime transaction recovery and retry
state and reached 38.66082x physical bytes per logical workload byte.

The next local gate must compose all four state frontiers before snapshot and
purge. The three-zone GCS run follows only after that bounded-state curve
passes locally.

## Constructed path

```text
3 OpenRaft data processes
  -> 1,024 transactions in 32-item application entries
  -> durable OKVS state snapshot on every voter
  -> reject purge unless snapshot covers target
  -> OpenRaft purge
  -> canonical OKVR node-journal rewrite
  -> stop all voters
  -> reopen snapshot + retained journal
  -> elect, retry, and commit a new suffix
```

The durable snapshot write order is:

```text
encode metadata + state + checksum
  -> write state-machine.snapshot.next
  -> synchronize candidate file
  -> atomic same-directory replacement
  -> synchronize parent directory
  -> expose snapshot as current
```

A stale pre-replacement candidate is ignored only after the authoritative
snapshot validates. A corrupt authoritative snapshot fails closed.

## Frozen observations

| Observation | Candidate | Purge-before-snapshot poison |
|---|---:|---:|
| Real data processes | 3 | 3 |
| Transactions per seed | 1,024 | 32 |
| Journal bytes before, maximum across seeds | 6,391,575 | 199,725 |
| Journal bytes after, maximum across seeds | 879 | 199,725 |
| Snapshot bytes, maximum across seeds | 5,066,472 | 0 |
| Physical amplification, maximum | 38.66082x | 1.52378x |
| Wall time, maximum receipt observation | 3.826 s | 0.538 s |
| Correctness anomalies | 0 | 0 |

Every candidate seed reopened exact transaction state and retained stream after
all three processes stopped. An old batch retry returned its exact outcome and
a new batch committed above the pre-snapshot high watermark. Every poison seed
rejected purge before any purged marker, compaction counter, or journal byte
changed, then reopened from the intact journal.

## What the result proves

- `[CODE-COMPLETE]` A process voter has a checksummed, versioned, crash-safe
  state snapshot instead of a memory-only OpenRaft snapshot.
- `[CODE-COMPLETE]` Physical journal purge is guarded by the local durable
  snapshot position.
- `[CODE-COMPLETE]` Canonical replacement reclaims obsolete vote, commit,
  append, truncate, and purge history while preserving the live suffix.
- `[EVALUATING]` The full-quorum restart composition passed three fixed local
  seeds in a release build.

It does not prove independent media, remote object durability, OTel delivery,
clean-source reproducibility, a production SLO, or bounded state.

## Why 38.66x is useful bad news

The experiment intentionally snapshots before advancing `R`, `Q(client)`, or
`O`. Each voter therefore persists:

```text
latest serving values
+ lifetime OCC conflict history
+ 1,024 retry outcomes and fingerprints
+ 1,024 retained recovery commands
+ current control and frontier state
```

The three replicated snapshots total about 5.07 MB for 128 KiB of logical
workload bytes. The journal is no longer the growth source. The state ownership
and frontier composition are.

G4.6 already projected one voter at about 131 KiB after aligned `R`, `Q`, and
ideal `O` at 4,096 commits. G4.11a now establishes that projection is not
enough. The real process maintenance sequence must apply those frontiers,
persist the resulting state, compact the physical journal, and reopen it.

## Frozen next gate: G4.11a.1

Run four local frontier cycles over a fixed 256-key live set. Each cycle must:

1. publish and validate an immutable object closure through frozen `O = C`;
2. advance the resolver floor `R = C`;
3. retain only the newest 64 request identities through `Q(client)`;
4. apply and certify the authenticated object frontier;
5. activate the publication frontier;
6. snapshot every data voter at its resulting applied log position;
7. purge and compact only through that snapshot position;
8. restart the full data quorum and compare exact object-plus-suffix state.

The frozen boundedness gates are:

- snapshot plus retained journal amplification is at most 8x one logical copy;
- cycle-four snapshot bytes are at most 1.25x cycle-one bytes;
- physical snapshot and journal bytes do not grow with objectified history;
- a retry below `Q(client)` is rejected without mutation;
- a retry inside the 64-request window returns its exact outcome;
- full restart and a new suffix commit remain exact.

The 8x ceiling includes three durable replicas and the current JSON snapshot
codec. It is a bootstrap gate, not a product cost target. If the curve fails,
separate serving image state from transaction-authority snapshot state before
the independent-media run.

## Remaining implementation risk

Snapshot serialization and file synchronization currently run inside the
state-machine snapshot task. G4.11a measured correctness with commits stopped.
The sustained gate must move serialization and writes off the commit executor,
rate-limit maintenance, and record commit-latency interference while the
frontier advances.

## Evidence

- Receipts: `docs/artifacts/eval-receipts/process-snapshot-compaction-g4.11a-v1/`
- Suite: `evals/suites/process-snapshot-compaction.toml`
- Owning RFC: `rfcs/0036-independent-media-frontier-convergence.md`
