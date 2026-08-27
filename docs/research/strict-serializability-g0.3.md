# G0.3 strict-serializability semantic gate

Status: `[EVALUATING]` diagnostic on 2026-08-26.

## Clarity

Question: Can the proposed centralized Cell v0 OCC contract express strict
serializability for product-shaped point and range transactions?

Punchline: Yes at the model boundary; an independent checker accepted every
correct seeded history and rejected all six poisoned subjects.

Counter: The resolver subject is not connected to the real OpenRaft process
path, and the dirty worktree makes this run non-comparable.

Next: Feed the same frozen history schema from the real process client and make
the replicated commit path satisfy the oracle without sharing checker logic.

## Boundary

```text
Cell v0 resolver subject
  -> transaction-history-v1 JSON
      -> independent okv-history-oracle
          -> anomaly classes and exact trace digest
```

The subject lives in `okv-sim`. The checker lives in
`okv-history-oracle` and imports no resolver, txLog, MVCC, serving, or object
code. The frozen JSON shape is
`evals/schema/transaction-history-v1.schema.json`.

The checker requires exact snapshot visibility, real-time order, complete point
and range conflict coverage, rejection of intervening conflicting writes, and
atomic effects. This is an RFC-0008 OCC checker, not a general-purpose Elle
replacement.

## Diagnostic result

- Suite: `strict-serializability-v1`.
- Suite hash: `f9422786902f8d6f55de6fbe549e51d5d06d4e5fbf06704a8e806c548bc45879`.
- Candidate: `a56442ad800deedd72a404a0886e88831eb308a0+dirty`.
- Seeds: `1103`, `2207`, `3301`, `4409`, `5519`.
- Transactions: 5,000 total, 1,000 per seed.
- Correct subject: 3,414 committed, 1,586 conflict-aborted.
- Committed multi-range transactions: 2,789.
- Reads: 7,500 point and 1,250 range.
- Correct-subject anomalies: zero.
- Correct run ID: `9664d42e-f7eb-4bcc-927f-6152d6958733`.
- Receipt SHA-256:
  `312f3003a05b054a8cd744de4f85e55acc36bfb3d023bc2b8ee5b2cbecc19514`.
- Verdict: `inconclusive`, because the source tree was dirty.

## Poison sensitivity

| Poison subject | Result | Median anomalies per seed |
|---|---:|---:|
| Accept point conflict | Discard | 2 |
| Accept range phantom | Discard | 1 |
| Apply partial commit | Discard | 2 |
| Omit read conflict | Discard | 2 |
| Omit write conflict | Discard | 2 |
| Use stale read version | Discard | 6 |

All histories and oracle reports replayed exactly for their seed. Unit coverage
also validates generated history against the frozen JSON Schema.

## What this changes

`[CODE-COMPLETE]` The semantic center is no longer only prose or opaque bytes.
The repository now has an executable, poison-sensitive Cell v0 resolver
contract and an independent oracle.

`[EVALUATING]` Strict serializability remains unproved for the actual process,
txLog, retry, and serving composition. G0.3 must not become `[VERIFIED]` until a
clean immutable receipt exists. The broader G6 multi-range cell remains
`[PROPOSED]` until independent range groups and coordinator faults feed the same
oracle.
