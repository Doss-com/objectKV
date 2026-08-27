# G4.5 bounded transaction-authority state

Status: `[EVALUATING]` local dirty-source diagnostic, 2026-08-26.

## Question

If objectification is perfect and the object-durable frontier equals the commit
frontier, does the current three-process transaction-authority snapshot remain
bounded at fixed live key cardinality?

## Answer

No. The complete projected snapshot grew 9.172x from 256 to 4,096 lifetime
commits while the workload held 256 live keys and 128-byte values. RFC-0028's
2.0x ceiling discarded the current monolithic authority-state shape.

This result does not discard objectKV or prove that an incumbent transaction
authority is required. It requires the next native prototype to separate state
by reclamation frontier before implementing object-frontier safe pop.

## Frozen experiment

- Suite: `transaction-authority-state-scale-v1`
- Suite hash: `5b456689111ca650513079c3557ba675e0b8180f8dba5d5075100c9bfd1e4279`
- Profile hash: `88db504c95031579d0e4269919f0b85cd8d29cc8fc5e61539e23d1821bbd256d`
- Revision: `a56442ad800deedd72a404a0886e88831eb308a0+dirty`
- Backend: `data-openraft-local-process`
- Seeds: 1103, 2207, 3301
- Checkpoints: 256, 1,024, and 4,096 commits
- Process boundary: three fresh OpenRaft data processes per seed plus an exact
  fresh-process replay of seed 1103
- Live state: 256 rotating keys with 128-byte values
- Telemetry: disabled for this dirty local diagnostic

## Results

| Subject | 256 commits | 1,024 commits | 4,096 commits | Maximum growth | Verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| Complete state after ideal txLog pop | 280,547 B | 734,753 B | 2,573,288 B | 9.172x | discard |
| Complete state without pop | 478,321 B | 1,526,216 B | 5,742,479 B | 12.005x | discard on time budget |
| Retained-stream-only poison | 0 B | 0 B | 0 B | 1.000x | discard, incomplete accounting |

The candidate growth samples were 9.1724x, 9.1654x, and 9.1690x. The no-pop
samples were 12.0055x, 12.0003x, and 12.0034x. The poison reported a favorable
flat curve but produced nine complete-accounting anomalies.

All three workloads exceeded the frozen 480-second debug budget, completing in
548.587, 544.308, and 544.508 seconds respectively. That is an additional
diagnostic signal, but it is not used to explain the size failure.

## Component diagnosis

At the final checkpoint, ideal txLog pop removes 3,169,201 serialized bytes but
leaves 2,573,288 bytes in the snapshot:

```text
current rejected StateMachineData
|
+-- transaction authority: values + OCC history   992,491 B   5.434x
+-- durable request outcomes                    1,055,339 B  16.364x
+-- request fingerprints                          524,430 B  16.186x
+-- retained recovery commands after ideal pop          0 B
```

The residual is lifetime-commit-sized because three independent retention
domains remain coupled to the replicated authority snapshot.

## Required redesign

```text
serving coverage frontier S  -> current and historical user values
minimum admitted read R      -> OCC conflict history
retry retention floor Q      -> request outcomes and fingerprints
object-durable frontier O    -> retained recovery commands
```

The transaction authority may coordinate these frontiers, but it cannot retain
all four histories indefinitely in one state-machine snapshot. The next gate
must reproduce the same curve after separating ownership, then implement a
generation-fenced object-frontier certificate and physical txLog pop.

## Receipts

- Candidate run: `93989e1c-c260-4365-875d-0bb6c184970d`
- No-pop control: `e0eb9535-b854-4acb-a17d-e10600d242c0`
- Accounting poison: `3f64dcd0-690c-4915-8260-18d6ad131225`
- Immutable files: `docs/artifacts/eval-receipts/authority-state-g4.5-v1/`

The control receipt's human-readable reason says the dirty tree made it
incomparable even though its `budget_must_hold` gate also failed. The evaluator
now prioritizes budget failure in future reason strings. The recorded hard gate
is the authoritative representation of this receipt.
