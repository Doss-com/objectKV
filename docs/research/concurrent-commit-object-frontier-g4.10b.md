# G4.10b concurrent commit and object-frontier readout

Status: `[CODE-COMPLETE]` mechanism, local release receipts `[EVALUATING]`

Date: 2026-08-26

## Question

Can the retained 32-item commit proxy continue resolving strict-serializable
transactions while an authenticated immutable closure advances frozen object
frontier `O`, then reconstruct exact final state from:

```text
ObjectState(O) + txLog(O, C] = Database(C)
```

The same-durability one-entry path is the control. The candidate is retained
only if it preserves the equation, conflict outcomes, exact retry, failover,
and restart while clearing the frozen throughput, latency, append-density,
frontier-duration, and paired-performance gates.

## Result

The local composition passes every frozen G4.10b gate. It remains
`[EVALUATING]` because the source tree is dirty, all six processes and stable
journals share one host, the object backend is local filesystem, and OTel was
disabled.

| Subject | Median resolved/s | Maximum p99 | Minimum outcomes/append | Maximum frontier time | Result |
|---|---:|---:|---:|---:|---|
| 25% conflict candidate | 1,075.343 | 104.274 ms | 31.030 | 95.673 ms | all frozen gates pass |
| no-conflict control | 1,093.306 | 105.834 ms | 31.030 | 99.705 ms | 850/s control passes |
| 75% conflict control | 1,081.588 | 105.418 ms | 31.030 | 135.389 ms | correct and bounded |
| one-entry same-durability control | 37.369 | 2,169.209 ms | 0.999 | 128.763 ms | paired baseline |
| moving-frontier poison | 1,100.135 | 100.936 ms | 32.000 | 107.633 ms | rejected before pop |
| premature-pop poison | 1,124.409 | 79.990 ms | 32.000 | 82.702 ms | rejected before pop |

The candidate is 28.776x the one-entry control. Candidate throughput samples
were 1,091.550, 1,056.455, and 1,075.343 resolved durable outcomes per second
for seeds 5101, 5102, and 5103.

## Implemented composition

```text
three publication voters                 three data voters
          │                                      │
publish immutable row closure through O          │
          │                                      │
prepare pending(O)                               │
          ├──────── release barrier ─────────────┤
          │                                      ↓
validate complete closure              64 callers -> commit proxy
          │                                      ↓
          ├─────────────── overlap ───── 32-item quorum entries
          ↓                                      │
physical pop through O                           │
          ↓                                      │
data-voter certificate                           │
          ↓                                      │
activate frontier(O)                             │
          └──────────────────┬───────────────────┘
                             ↓
                ObjectState(O) + txLog(O,C]
                             ↓
                    exact authority state C
```

Each run commits a 512-transaction prefix, publishes its exact row closure,
and releases 1,024 suffix attempts with the frontier controller. The candidate
marks 256 suffix attempts as conflicts over 64 hot keys. Exactly 64 hot-key
writes commit and 192 later attempts conflict. The remaining 768 unique-key
attempts commit. An independent ordered conflict oracle reproduces every
outcome from applied log position, batch order, declared read version, and
conflict ranges.

The foreground transaction path owns no object backend. Object reads occur in
the frontier controller for complete closure validation, never in transaction
acknowledgement. The controller waits for observed suffix progress, not merely
a shared barrier, before applying the physical pop.

## Recovery checks

Every positive subject proves:

- three real publication processes and three real data processes;
- synchronized stable journals and release execution;
- one durable committed or conflicted outcome for every admitted identity;
- unique `(applied_log_index, batch_order)` values and contiguous order within
  each entry;
- exact object coverage at frozen `O` while final `C` continues moving;
- persisted retention floor equal to `O` and every retained record newer than
  `O`;
- exact state from immutable objects plus retained suffix;
- exact retry of one committed and one conflicted request;
- data-leader failover, publication-leader failover, killed data-voter restart,
  and reconstruction by fresh clients.

The moving-frontier poison substitutes a newer `C` for frozen `O`. Manifest
coverage validation rejects it before data mutation. The premature-pop poison
skips pending publication protection. Data-frontier apply rejects it, leaves
the floor at zero, and retains every prefix record through `O` while concurrent
suffix commits continue.

## Decision

Retain the native transaction-authority composition for the next gate. This
result removes concurrent objectification as an immediate local falsifier. It
does not establish a production cell because the current receipt cannot prove
independent failure domains, host-loss durability, remote-object behavior,
OTel causality, repeated-frontier convergence, internal OpenRaft journal
reclamation, tenant fairness, or economics.

The next gate must run the same frozen transaction and frontier semantics from
one clean revision across independent stable media and a remote object backend.
It must also measure repeated frontier cycles so application-stream pop is not
mistaken for bounded physical journal bytes.

## Receipts

Checksummed receipts:
`docs/artifacts/eval-receipts/commit-proxy-object-frontier-g4.10b-v1/`.

Suite: `evals/suites/commit-proxy-object-frontier.toml`.

Contract: `rfcs/0035-concurrent-commit-object-frontier.md`.
