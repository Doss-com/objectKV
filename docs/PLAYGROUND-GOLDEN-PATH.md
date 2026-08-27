# objectKV application-history golden path

Status: `[VERIFIED]` for GP-G0 through GP-G6 in the bounded scopes below.
Production admission remains `[EVALUATING]`.

## Purpose

Tetris and Chess are the executable product boundary for objectKV history.
Tetris stresses a long, high-rate action stream. Chess makes exact snapshots,
forks, divergent suffixes, branch switching, replay, and garbage collection
easy to inspect. The apps expose the same lineage as a compact Git-like tree,
and every node can be opened at its exact version.

The prefix `GP-` distinguishes these playground rungs from the wider program's
systems and infrastructure gates.

## Construction

```text
application action or move
  -> canonical transaction envelope
     -> replicated txLog
        -> disposable RAM serving image
        -> application record stream
           -> checkpoint + immutable history segments
              -> root and child manifests
                 -> publication roots, pins, and reserved GC
```

`txLog` is the kernel recovery history. It contains committed KV effects and
does not invoke application reducers. The application record stream contains
small reducer-specific deltas. One committed envelope aligns both atomically.
This is why the application record is 2 bytes for Tetris and 4 bytes for Chess
while the current materialized txLog remains much larger.

## Verified rungs

| Rung | Status | Verified scope | What the receipt proves |
| --- | --- | --- | --- |
| GP-G0 | `[VERIFIED]` | Frozen local encodings | Deterministic reducer, schema, trace, and state fingerprint. |
| GP-G1 | `[VERIFIED]` | Volatile ordered history | Checkpoint plus bounded tail equals uninterrupted replay; seven poison controls per game reject false history. |
| GP-G2 | `[VERIFIED]` | Single-process transaction model | Application record and materialized mutations commit or abort together. |
| GP-G3 | `[VERIFIED]` | Three OpenRaft processes on one host | Canonical envelopes survive lost reply, leader kill, retry, restart, and catch-up with zero anomalies. |
| GP-G4 | `[VERIFIED]` | Single-process RAM serving image | Hot point reads, exact historical reads, complete image discard, and replay rebuild. SSD remains `[PROPOSED]`. |
| GP-G5 | `[VERIFIED]` | In-memory object adapter plus pure publication authority | Recursive checkpoint, segment, and manifest verification before root publication; cold reopen is exact. GCS remains `[PROPOSED]`. |
| GP-G6 | `[VERIFIED]` | In-memory object adapter plus pure publication authority | A child stores only its manifest and divergent suffix; pin, exact root removal, reserved delete, and main-branch reopen are exact. |
| GP-G7 | `[FUTURE]` | None | Integrated cell, independent hosts, real GCS, admitted SSD control, and economics. |

No rung inherits proof from an earlier rung. The current receipts do not yet
prove one continuously integrated production cell. GP-G3, GP-G4, and GP-G5/6
exercise the same formats at intentionally separate system boundaries.

## Current release metrics

Release-profile run on 2026-08-25:

| Metric | Tetris | Chess |
| --- | ---: | ---: |
| Materialized txLog bytes | 2,524,571 | 3,155 |
| Delta plus encoded checkpoints | 5,904 | 126 |
| Logical size ratio | 427.6x | 25.0x |
| Application record bytes per commit | 2 | 4 |
| RAM point-read p99 | 125 ns | 84 ns |
| RAM measured reads/s | 8.79M | 11.95M |
| RAM image rebuild | 19.244 ms | 35 us |
| GP-G3 scenario duration | 561 ms | 573 ms |
| Branch-only object puts | 2 | 2 |
| Child-only objects reclaimed | 2 | 2 |

The nanosecond read values are in-process release-harness measurements, not
network database latency. GP-G3 throughput is a three-commit fault scenario,
not a steady-state throughput curve. These values are baselines for regression
and composition, not product claims.

## History and lineage model

```text
main:    v1 -- v2 -- v3 -- v4
                      \
line-1:                v3a -- v4a
```

A branch records `parent`, `fork_version`, and `latest_version`. Immutable
prefix history is referenced through the parent manifest; only a divergent
suffix and child manifest are new objects. The web apps render the relationship
as a branch rail. Selecting a version performs an exact historical read;
selecting a branch changes the active write head.

## Hard gates

1. Materialized and delta paths end at the same fingerprint.
2. Checkpoint identity covers the claimed reducer, schema, and position.
3. Missing, reordered, corrupt, wrongly typed, or falsely positioned history
   fails closed.
4. Application record and business mutations are atomic.
5. Failover and retry produce one recovered outcome per request identity.
6. A serving image can be deleted and rebuilt exactly.
7. A publication root is installed only after recursive object verification.
8. Branch construction copies zero prefix objects.
9. Garbage collection deletes only unreachable objects under an accepted
   reservation and preserves every remaining root.

Run the complete bounded suite:

```bash
./experiments/run-okv-playground-golden-path.sh
```

The runner builds under `/private/tmp`, removes its build directory on exit,
and emits one JSON receipt per workload plus a final admission receipt.

## Next verification slice

The next slice composes the boundaries already proven separately:

```text
three independent hosts
  -> replicated publication authority and txLog
     -> RAM and admitted SSD serving profiles
        -> real GCS immutable objects
           -> empty-worker rebuild, fork, and GC under failure
```

The slice advances only if the same frozen histories remain exact and produces
bounded cold-read, recovery, objectification-debt, and cost curves. GP-G7 stays
`[FUTURE]` until those results exist.

## Decisions

| Decision | Choice | Tradeoff |
| --- | --- | --- |
| D1 | Keep recovery txLog and reducer-specific application history distinct. | More plumbing; kernel recovery never depends on arbitrary application code. |
| D2 | Make RAM serving disposable and objects authoritative only after verified publication. | Fast hot path; restart cost must remain bounded by checkpoint and tail. |
| D3 | Share branch prefixes by manifest reference. | Cheap forks; reachability and GC become correctness-critical. |
| D4 | Keep SSD as a separate control, not an implied success. | Avoids hiding RAM economics; leaves a production-serving comparison unverified. |
| D5 | Stop at GP-G6 for architectural review. | Delays distributed product work; exposes composition risk before expanding scope. |
