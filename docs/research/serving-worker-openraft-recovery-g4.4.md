# G4.4 authority-owned txLog recovery

Status: `[EVALUATING]`

## Outcome

G4.4 removes the G4.3 worker dependency on `LocalReplicatedWal`. A replacement
worker now reads committed transaction commands from a linearizable, paginated
stream owned by three real OpenRaft data-authority processes. It recovers an
immutable object base through `O`, catches up through `C0`, observes four commits
while it is paused, catches up again through `C1`, and serves exact reads at
`C1` without opening a physical Raft journal path.

This is a local process-composition result. It is not independent-machine
durability, safe log reclamation, a production activation lease, GCS latency,
or a throughput and economics result.

## Implemented seam

```text
three generation/publication authority processes
  -> generation 7
  -> publication root
  -> logical txLog identity wal-g7

three data-authority processes
  -> strict-serializable TransactionCommand apply
  -> accepted commands retained in state-machine snapshots
  -> linearizable retained-transaction-read-v1 RPC

immutable object row base through O = 9
  -> 3 row segments
  -> 39,288 data bytes
  -> 1,374 index bytes

replacement worker
  -> page (O, C0 = 12]
  -> controller commits through C1 = 16
  -> page (C0, C1]
  -> exact point reads at C1
```

The stream cursor is a commit version, not a dense record offset. Raft control
and membership entries may leave numeric gaps. The first page freezes its
target after a linearizability barrier; later pages cannot drift into newer
commits. A request below the retention floor or above the current high
watermark fails closed.

Only accepted transactions enter the retained stream. Deduplicated retries,
conflicts, and rejected commands do not create records. The current
implementation retains commands in state-machine snapshots. This is correct
for the gate but duplicates bytes and has no safe-pop implementation yet.

## Frozen history

The fixed history uses eight base transactions followed by:

```text
initial suffix to C0
  Set existing key
  Clear existing key
  Set tail-only key

concurrent suffix to C1
  Set existing key
  Clear existing key
  Set tail-only key
  ClearRange over four base keys
```

The first worker reaches `initial_catchup_complete` and is killed before any
read. A distinct replacement process starts with an empty scratch directory,
repeats the same catch-up, waits while the controller commits the concurrent
suffix, then performs its second frozen catch-up.

## Fixed-seed diagnostic

Build: debug. Backend:
`object-store-local-fs+authority-openraft+data-openraft`. Seeds: 1103, 2207,
3301. Source: `a56442ad800deedd72a404a0886e88831eb308a0+dirty`.

| Path | Verdict | p99 process-entry to exact reads | Median object bytes | Anomalies |
| --- | --- | ---: | ---: | ---: |
| Lazy object plus OpenRaft tail | inconclusive | 120.183 ms | 6,177 | 0 |
| Full-hydration control | inconclusive | 197.674 ms | 42,305 | 0 |
| Skip concurrent catch-up poison | discard | 176.150 ms | 19,779 | 12 |

The candidate was 1.64x faster at p99 and moved 6.85x fewer object bytes than
full hydration. It transferred 15.19 percent of the indexed object closure for
the held-out reads. Every candidate seed used one manifest GET, one selected
index GET, one data range GET, zero complete data GETs, and zero LIST calls.

Each candidate used four retained-stream pages and returned about 3,854 bytes
of serialized response payload. It applied all three initial records and all
four concurrent records. The poison observed the same four concurrent records
but applied none, producing four wrong outcomes per seed: stale update, missed
deletion, missed insertion, and missed range clear.

The timer starts at the worker operation after CLI configuration parsing. It
includes authoritative root reads, initial catch-up, the controlled concurrent
commit interval, second catch-up, and object reads. It excludes executable
launch and CLI parsing.

## What this establishes

- `[CODE-COMPLETE]` A worker-facing txLog seam can be independent of OpenRaft's
  physical journal and entry encoding.
- `[CODE-COMPLETE]` Linearizable frozen-target pagination is exact across commit
  version gaps and commits that arrive after the first target.
- `[CODE-COMPLETE]` A populated state-machine snapshot preserves retained
  records and retry outcomes through build and install while the frozen empty
  snapshot bytes remain unchanged.
- `[CODE-COMPLETE]` A killed worker can be replaced from empty scratch by
  composing object state with two retained-stream catch-up rounds.
- `[CODE-COMPLETE]` The overlay orders point sets, point clears, tail-only
  inserts, and range clears against the immutable base.
- `[EVALUATING]` Lazy object access can retain its bounded named-read shape when
  the tail source is the real replicated transaction authority.

## What remains unproved

- The retained vector is duplicated into state-machine snapshots and never
  popped. Retained-byte curves and frontier-coupled safe pop are open.
- The data voters run as separate processes on one laptop. Host, disk, zone,
  and regional durability are open.
- The two-round protocol does not prove convergence when writes outrun
  recovery, cancellation, admission, or an atomic serving-lease handoff.
- The 39 KiB object closure is a correctness fixture, not a scale curve.
- The debug build and barrier polling dominate the latency result. It is not a
  product latency target.
- OTel export is disabled and the tree is dirty. G4.4 remains
  `[EVALUATING]`, not `[VERIFIED]`.
- Range scans, historical reads within the suffix, and multi-range scheduling
  are open.

## Next falsification gate

Measure retained-stream growth, state-machine snapshot growth, catch-up time,
and convergence as `C - O`, mutation bytes, page size, and concurrent write rate
increase. Add safe-pop certificates bound to the object-durable frontier. The
gate must reject unbounded retained bytes, missed records, or recovery that
cannot reach an activation threshold under an admitted write rate.

Only after that local curve passes should the same contract move to release
builds, OTel, GCS, and three independent data machines.

## Immutable receipts

- Candidate: `docs/artifacts/eval-receipts/serving-recovery-g4.4-v2/candidate.json`
  (`4bc41772860d30fef896def76df05d5e7c8206851231451bd0d03b3a427bc7e5`)
- Full-hydration control:
  `docs/artifacts/eval-receipts/serving-recovery-g4.4-v2/control.json`
  (`c7857115d0537946c1d67c5b55fa0835ad6f71baacc738423f6971c9b15b3354`)
- Skip-concurrent-catch-up poison:
  `docs/artifacts/eval-receipts/serving-recovery-g4.4-v2/poison.json`
  (`5dd162fa70dabf99247cf9f11a27b2c7b769f1e997c1125dd4df914651549a54`)

The superseded v1 receipts remain under
`docs/artifacts/eval-receipts/serving-recovery-g4.4-v1/` because their timer
started after catch-up and did not satisfy the metric contract.
