# RFC-0047: Resumable chunked tLog repair with live tail

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0040, RFC-0044, RFC-0045, RFC-0046

## Decision

`[EXISTS]` Repair one failed tagged-log member through a bounded, resumable
base transfer followed by an ordered tail transfer while the active policy
continues to commit. Every transfer is bound to one active-policy quorum
certificate, one immutable transfer descriptor, and one learner incarnation.
The learner remains non-voting until the active quorum certifies the final
combined root and retained position.

The first contract retains transactions `11..14` above object frontier `10`,
fails node `1`, and creates learner `4`. The base snapshot is split into fixed
chunks. The learner persists at least one chunk, restarts, accepts an exact
retry idempotently, and completes the base. Active nodes `2` and `3` commit and
append transactions `15` and `16` during repair. A second transfer carries
only the ordered tail records, not another copy of the base. After another
restart, the learner reconstructs the exact combined root through `16` and
receives a current readiness certificate. It still contributes to no active
quorum.

## Transfer invariant

For active policy `P`, learner `L`, base certificate `B`, target readiness
certificate `R`, base chunks `C`, and tail chunks `D`:

```text
accept_chunk only if:
    certificate is valid under P
    transfer identity binds cell, generation, log set, learner incarnation,
        phase, base root, payload root, payload length, and chunk count
    chunk index is in range
    exact duplicate bytes are idempotent
    conflicting duplicate bytes are rejected

finalize_base only if:
    every chunk in C is durable
    concat(C) matches the payload root and length
    decoded records are consecutive
    combined root equals B.snapshot_sha256

finalize_tail only if:
    every chunk in D is durable
    the installed base root equals the descriptor base root
    decoded tail begins at installed_position + 1
    no record is skipped, duplicated, or reordered
    encode(installed_base + D) equals R.snapshot_sha256
    final position equals R.last_position

ready only if:
    R is valid under P at the current active retained frontier
```

Chunk persistence is synchronized before acknowledgement. Restart discovers
the exact transfer descriptor and durable chunks from the learner root. A
different descriptor or conflicting retry fails closed. Finalization persists
the installed root and position before reporting success.

## Correct bounded history

1. Objectification remains at `O=10`; active nodes `2` and `3` retain positions
   `1..4` for transactions `11..14`; node `1` is unavailable.
2. The active quorum signs base certificate `B` for learner `4`.
3. Learner `4` persists the first base chunk, restarts, replays that chunk
   exactly, receives the remaining chunks, and finalizes exact position `4`.
4. While the base transfer is incomplete, active nodes append transaction `15`.
   They append transaction `16` before readiness.
5. The active quorum signs target certificate `R` for the exact combined root
   and position `6`. Tail payload bytes contain only positions `5` and `6`.
6. Learner `4` persists one tail chunk, restarts, resumes, and finalizes the
   exact combined state without rewriting the base transfer.
7. A second active quorum confirms current readiness through transaction `16`.
   Capacity, durability, pop, and serving still count only nodes `2` and `3`.
8. A fresh worker reconstructs exact transaction `16` from object frontier
   `10` and the active quorum.

## Negative subjects

The frozen suite independently attempts to:

1. lose an acknowledged chunk across learner restart;
2. finalize with one missing chunk;
3. reuse one chunk index with different bytes;
4. install a tail with a position gap or reordering;
5. declare readiness at position `4` after the active frontier reaches `6`;
6. count the learner before it reaches the current certified root;
7. recopy the full base during tail catch-up instead of transferring only the
   two new records.

Every subject must replay exactly, produce a correctness, durability,
membership, or transfer-efficiency anomaly, export OTel, and discard.

## Eval plan

Freeze `cell-tagged-log-chunked-live-repair-v0` with seeds `1103`, `2207`, and
`3301`. Each seed starts three transaction-authority processes, two
three-process tagged-log sets, one empty learner, and one fresh serving worker.
The event budget is 300 checks.

The primary metric is correctness anomalies. Secondary receipts include base
and tail payload bytes, durable chunks, exact retries, learner restarts,
installed records, repair and readiness attestations, active appends, worker
frontier, and transfer amplification. OTel carries `wal.retained_bytes`,
`availability.success_ratio`, `operation.duration`, and correctness anomalies
with exact candidate, suite, profile, run, workload, and backend labels.

## Evaluation outcome

Candidate `254cf421` kept OTel-enabled run `28dfe9f4` at 51 of 300 allowed
events with zero anomalies and exact replay across seeds `1103`, `2207`, and
`3301`. The three subjects acknowledged nine base chunks and six tail chunks,
survived six learner restarts, retried six chunks exactly, installed 18 records,
and advanced both learner and fresh-worker frontiers to a total of 48, version
16 per seed. The 5,751 tail bytes were smaller than the 11,499 base bytes.

Seven clean controls replayed exactly and discarded:

| Subject | Run | Anomalies per seed |
|---|---|---:|
| lose acknowledged chunk across restart | `97893c13` | 1 |
| finalize with one missing chunk | `30ae3394` | 1 |
| overwrite one durable chunk on retry | `1198e1c0` | 1 |
| install a gapped tail | `d5f85770` | 1 |
| certify stale learner readiness | `25ee028b` | 4 |
| count an uncaught-up learner | `528f1eec` | 1 |
| recopy the base during tail catch-up | `0190688e` | 1 |

Prometheus observed availability `1`, correctness anomalies `0`, and the
tail-only retained-byte gauge under exact candidate, suite, profile, run,
workload, and backend labels. The frozen source suite hash is `a7206d45`; the
evaluated suite hash is `3a20363c`; the profile hash is `a5555c7d`.

This admits one same-host repair with one active append during incomplete base
transfer and another append before readiness. It does not admit remote transfer,
multiple concurrent repairs, unbounded append, transfer lease expiry, orphan
chunk collection, zone failure, production key custody, or simultaneous policy
movement.

## Alternatives

### Pause writes for one complete snapshot copy

This reuses RFC-0045 but makes repair time part of write unavailability and
cannot resume a large transfer cheaply.

### Stream new appends directly into an incomplete base

This reduces catch-up latency but creates two independently advancing inputs.
The first contract completes one certified base before applying an ordered
certified tail.

### Recopy the complete snapshot after every frontier change

This is semantically simple but makes catch-up bandwidth proportional to base
size rather than new work. The gate records separate base and tail bytes and
rejects full recopy as the bounded negative subject.

## Unresolved questions

1. What production chunk size minimizes restart work without excessive object
   or RPC overhead?
2. How much concurrent append can be admitted before ratekeeping pauses writes
   or abandons one repair attempt?
3. Should chunks live in object storage, direct peer streams, or both?
4. How are transfer leases expired and orphan chunks reclaimed?
5. How does one repair span independent hosts and zones?
6. How do multiple simultaneous repairs share bandwidth and policy limits?

## Tradeoff

This contract optimizes for bounded restart work and tail-only catch-up while
keeping the learner outside every active quorum. It gives up continuous
streaming into an incomplete base and does not yet eliminate a short readiness
barrier before RFC-0046 policy activation.
