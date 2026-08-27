# G0.4 replicated strict-serializability process gate

Status: `[EVALUATING]` diagnostic on 2026-08-26.

## Clarity

Question: Can one Cell v0 OpenRaft data group enforce the frozen transaction
contract across real process failure, lost replies, exact retry, and replica
replay?

Punchline: Yes at the same-machine process boundary. The correct subject passed
the independent history oracle and every failover check across three seeds,
while both transaction poisons were rejected.

Counter: The source tree was dirty, all three processes used one machine and
filesystem, elections were controller-driven, and each seed contains only six
transactions. This is not clean admission, independent-disk evidence, a
throughput result, or a distributed multi-range transaction proof.

Next: produce a clean receipt from the frozen suite, then run the same history
contract on three independent machines and disks before measuring transaction
latency or throughput.

## Executed boundary

```text
point and range reads at R
  -> TransactionCommand(R, conflicts, mutations)
      -> one three-node OpenRaft group
          -> deterministic TransactionAuthority
              -> committed | conflict | rejected
                  -> process history
                      -> independent okv-history-oracle
```

For each seed, the controller starts three ordinary OS processes, commits
multi-range state, exercises point and empty-range conflicts, drops one
successful reply, kills the accepting leader, elects a successor, resolves and
retries the original request identity, restarts the killed replica, and compares
all reconstructed transaction state.

## Diagnostic result

- Suite: `strict-serializability-process-v1`.
- Suite hash:
  `e19c90ff544c544eb222c1b642ac673b66306e9534111f2cd3cf6ca1ce7ffefc`.
- Candidate: `a56442ad800deedd72a404a0886e88831eb308a0+dirty`.
- Profile: `local-fs` on `arm64`, Rust `1.88.0`.
- Backend: `process-openraft+transaction-authority`.
- Seeds: `1103`, `2207`, `3301`.
- Correct run ID: `5943479a-9c57-4c6b-9ca7-74a86497924b`.
- Correct receipt SHA-256:
  `1c3eacb4f4e6b1d5c2ecc8648c102ba542a26a2c37ae3724e7d58c8f606bb756`.
- Verdict: `inconclusive`, because the source tree was dirty.
- Correct-subject anomalies: zero in every seed.
- Transactions: 18 total, 12 committed and 6 conflict-aborted.
- Committed multi-range transactions: 9.
- Reads: 18 point and 6 range.
- Process contract: 12 starts, 3 kills, 6 elections, 3 dropped replies,
  3 recovered outcomes, and equal final state in every replica.
- Event budget: 30 observed of 30 allowed.
- Exact replay: passed. Each seed produced the same report and semantic digest
  on its second execution.

The reported `5.80 s` median operation duration includes process startup,
controller-driven elections, failure injection, restart, and exact replay. It is
diagnostic harness time and must not be cited as a transaction latency result.

## Poison sensitivity

| Poison | Result | Anomalies per seed | First detected class |
| --- | ---: | ---: | --- |
| Accept intervening point and range conflicts | Discard | 3 | `ReadWriteConflict` |
| Apply only the first mutation of a committed transaction | Discard | 3 | `Atomicity` |

The conflict poison run ID is
`8511ddf4-3d16-4a10-a0da-8221c36a143a`; its receipt SHA-256 is
`17ca1fdf3970c38e9bc73c681a441d9df186bad78e0eb6d48601583fed40f651`.
The partial-apply run ID is
`7fcb04d4-c8e9-4cb2-bc79-2d835f779a32`; its receipt SHA-256 is
`ec2255202cb7f8ac86d78734bd75dddc20d5275aa18459c9f82ff9d398abb2da`.

## Failure found while constructing the gate

The first real-process trace failed immediately after its seed transaction.
`TransactionAuthorityView` exposed a `BTreeMap<Vec<u8>, VersionedValue>` through
JSON status RPC. Empty maps encoded during startup, but the first non-empty map
could not become JSON object members, so the server closed the response and the
controller observed `unexpected end of file`.

The authority and status contract now encode binary-key values as a canonical
key-ordered entry array. The decoder retains compatibility with the initial
empty-object snapshot. A frozen non-empty authority fixture and round-trip test
cover the process RPC and future state-machine snapshots.

## What this changes

`[CODE-COMPLETE]` Cell v0 now has one executable single-group composition of
stable request identity, strict-serializable OCC, atomic multi-key mutation,
quorum-ordered application, lost-reply recovery, exact retry, and restarted
replica replay.

`[EVALUATING]` G0.4 remains unadmitted until a clean immutable receipt exists.
The gate does not cover automatic election, concurrent external clients,
bounded conflict retention, transaction size limits, snapshot installation,
independent disks or machines, range-partitioned resolvers, multi-group commit,
object publication, or performance curves.
