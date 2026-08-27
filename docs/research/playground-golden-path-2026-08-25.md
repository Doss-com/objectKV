# Tetris and Chess developer golden path

Status: `[VERIFIED]` for the single-process, volatile `okv-model` plus
`okv-log` scope. No stable-media, consensus, object-store, or distributed claim
is implied.

## Decision

D1. Keep two application workloads across the materialized
`objectkv-boundary-v0` control and application-delta candidate.

- Tetris owns state-transition rate, log amplification, recovery cost, and
  later sustained-load curves.
- Chess owns exact snapshot, historical fork, divergence, named branch switch,
  and replay semantics.

The workloads share receipt fields and correctness gates. They do not share a
game abstraction.

D2. Treat this as the developer golden path, not the fifteen-checkpoint system
golden path. The same workload intent will move from the in-process model to
resident serving, durable txLog, object publication, and distributed cell
profiles as those surfaces become runnable.

D3. Keep the application delta log distinct from the recovery txLog. The
current candidate writes raw deltas directly to volatile `okv-log`; a later
transactional path must durably append the application record through the
kernel without making the kernel execute a game reducer.

## Command

```bash
./experiments/run-okv-playground-golden-path.sh
```

The command builds under `/private/tmp`, emits materialized and delta receipts
for both workloads, requires differential fingerprint equality, and deletes the
build tree at exit.

## Verified baseline

Run date: 2026-08-25. Build: Rust debug profile. Machine identity, CPU
isolation, and release-build controls are not frozen, so throughput values are
diagnostic.

| Workload | Hard correctness result | Retained commits | txLog payload | Payload per commit |
| --- | --- | ---: | ---: | ---: |
| Tetris, 2,000 actions | Snapshot round-trip and replay exact | 2,001 | 2,535,510 B | 1,267 B |
| Chess divergent line | Snapshot, divergence, both switches, replay exact | 4 | 3,156 B | 789 B |

The application-delta candidates reconstructed the same exact fingerprints:

| Workload | Raw delta | Checkpoints | Logical history | Materialized / delta ratio |
| --- | ---: | ---: | ---: | ---: |
| Tetris, 2,000 actions | 4,000 B | 1,640 B | 5,640 B | 449.6x |
| Chess divergent line | 12 B | 81 B | 93 B | 33.9x |

The Tetris delta is two bytes per action and checkpoints a 205-byte state every
256 actions. The Chess delta is four bytes per move and checkpoints an 81-byte
state every 64 moves. These figures are logical payload, not framed,
checksummed, replicated, or object-indexed durable bytes.

Tetris model throughput fell as retained history grew:

| Actions | Actions per second | txLog payload per commit |
| ---: | ---: | ---: |
| 250 | 5,884 | 1,252 B |
| 500 | 5,178 | 1,261 B |
| 1,000 | 4,159 | 1,264 B |
| 2,000 | 2,936 | 1,267 B |
| 4,000 | 1,882 | 1,267 B |

The throughput decline is expected from this implementation. `LogState` is a
pure specification state machine whose atomic `apply_all` clones its complete
retained state before each command sequence. `okv-model` retains every MVCC
version, and Tetris rewrites its materialized view on every action. This curve
is not evidence about a production resident engine. It is a verified signal
that the model harness cannot be used as its performance control.

The previously observed live Tetris process used 27,536 KiB RSS while its UI
reported 3.66 MB of active-branch txLog payload. Those values measure different
things. The payload figure excludes map, vector, MVCC, branch-copy, allocator,
and executable overhead.

## Mutation shapes

```text
Tetris materialized action
  full state + range clear + metadata + every visible cell + event

Tetris delta action
  schema version + action tag

Chess materialized move
  full logical state + source clear + destination set + metadata + event

Chess delta move
  schema version + source + destination + promotion
```

The delta modes hold current state as a disposable in-memory cache and recover
from the latest checkpoint plus the ordered action tail. The materialized modes
remain the differential controls.

## Forward use

The paired workloads become gates at each implementation rung:

| Rung | Tetris question | Chess question |
| --- | --- | --- |
| Model | Do materialized and two-byte delta histories finish at the same fingerprint? | Do materialized and four-byte delta branches finish at the same fingerprint? |
| Resident engine | What are p50, p99, throughput, RSS, and bytes per commit? | Is branch switching bounded and exact from the resident image? |
| Durable txLog | What acknowledged action rate survives restart and reply loss? | Does every named line recover to the same fingerprint? |
| Object base | Does objectification debt remain bounded under sustained play? | Does a fork reuse immutable closure bytes rather than copy history? |
| Distributed cell | Does throughput rise with independent ranges? | Do multi-key application records retain one branch-consistent version? |

The next implementation slice is to append the delta through a transactionally
aligned application log, then recover it through `okv-wal`. Later admission
adds resident serving, frozen release builds, machine profiles, latency
histograms, peak RSS, recovery duration, object request counts, and negative
controls. The stable specification is
[`../PLAYGROUND-GOLDEN-PATH.md`](../PLAYGROUND-GOLDEN-PATH.md).
