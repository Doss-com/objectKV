# RFC-0026: ServingWorker process recovery from objects plus txLog

- Status: `[EVALUATING]`
- Authors: DOSS
- Created: 2026-08-26
- Scope: one-range replacement-worker recovery

## Decision

`[CODE-COMPLETE]` A replacement `ServingWorker` starts with empty disposable state
and reconstructs one readable range from two authoritative sources:

```text
linearizable cell authority
  -> active generation and logical txLog root
  -> published row-manifest root

immutable object store
  -> row base through O

durable txLog
  -> committed mutations in (O, C]

first exact read at C
  -> recent tail overlay
  -> one selected row-object index
  -> at most one selected data-block range GET
```

The worker does not receive a manifest reference or physical txLog directory as
trusted recovery state. It receives coordinator endpoints, a publication-root
name, object-store configuration, a durability-provider namespace, an empty
scratch directory, and a target read version. The coordinator selects the
active generation, logical txLog root, and published manifest.

This RFC freezes the first real-process composition gate. It does not promote
the local three-file txLog adapter to a production `DurableLog` implementation.

## Load-bearing invariant

For one active generation:

```text
O <= C
Database(C) = ObjectState(O) + quorum-durable txLog mutations in (O, C]
```

An empty worker may serve a read at `C` only after it has proved all of the
following:

1. Two linearizable generation reads around the publication-root read are
   equal and report one active generation.
2. The row manifest is the exact length and SHA-256 identity installed in the
   replicated publication authority.
3. The manifest and every selected row-object reference name the active
   generation and cover state only through `O`.
4. The durability provider resolved the logical txLog root selected by the
   generation authority and quorum-recovered a contiguous log through `C`.
5. Every replayed transaction command is structurally valid, ordered at its
   committed log position, and newer mutations are applied after the object
   base.
6. Local absence is authoritative only after the tail and selected base both
   establish absence at the target version.

A generation transition between either side of the publication read causes the
open to restart. A missing or conflicting txLog quorum, malformed command,
manifest mismatch, unsupported generation, or unavailable required object
fails the range closed.

## Recovery protocol

The first implementation executes this bounded sequence:

1. Verify that the worker scratch directory exists and is empty.
2. Read the active generation and logical txLog root through a linearizable
   coordinator read.
3. Read the named publication root through the same coordinator quorum.
4. Read the generation again and reject any phase, generation, transaction
   system, txLog root, or control-root change.
5. Fetch the exact manifest name from object storage. Verify its authoritative
   length and digest before decoding `OKVM`.
6. Open the selected durability-provider root and reconstruct its contiguous
   quorum history.
7. Validate committed transaction commands in log order. Build a versioned
   in-memory point-mutation overlay for records in `(O, C]`.
8. Signal `recovered_before_read` to the process controller. The failure gate
   kills this process at that boundary.
9. Start a distinct replacement process with a distinct empty scratch
   directory and repeat steps 1 through 7 using only authoritative inputs.
10. Serve the first base-backed read and the held-out tail reads at `C`.

The first executable gate composes the existing `Set` and `Clear` transaction
subset. `ClearRange`, historical reads inside the retained suffix, range scans,
and concurrent commits during open remain required follow-up gates. The
restriction limits this receipt, not the final objectKV contract.

## Point-read order

```text
get(key, C)
  -> newest point mutation for key with O < version <= C
       Set   -> value
       Clear -> tombstone
  -> manifest segment whose bounds may contain key
       none  -> absent
  -> exact selected index GET and validation
  -> at most one checksummed data-block range GET
  -> version selection inside the block
```

The tail lookup must happen before manifest-bound selection. A key first
created after `O` may lie outside every base-segment bound and still exist at
`C`.

## Physical work contract

For the lazy candidate, the first correct base-backed read after replacement is
bounded by:

```text
one linearizable generation/publication/generation read sequence
one exact manifest GET
one quorum txLog recovery
one selected index GET
at most one selected data-block range GET
zero object LIST operations
zero complete-range hydration
```

The receipt records authority process count, worker process starts and kills,
empty-scratch replacements, txLog records and physical bytes, object requests
and response bytes, closure bytes, first-read latency, exact logical outcomes,
and a semantic replay digest.

## Frozen controls

### Full-hydration control

The replacement opens the same authority root and txLog suffix, then reads and
verifies every index and data object before its first read. It must return exact
values. It is the same-correctness control for latency and transferred bytes.

### Skip-tail poison

The replacement quorum-recovers the txLog but deliberately omits `(O, C]` from
the read overlay. The held-out history contains an update, deletion, and
tail-only insertion. The poison must return stale or missing state and receive a
`discard` verdict.

## Current adapter and excluded claims

`[CODE-COMPLETE]` The first physical gate uses:

- three real OpenRaft authority processes with independent local journals;
- an Apache `object_store` filesystem backend;
- `LocalReplicatedWal`, which synchronizes matching `OKVW` frames to two of
  three files on one machine;
- two real serving-worker processes separated by an operating-system kill.

This proves process replacement, authoritative root selection, quorum-frame
reconstruction, tail overlay ordering, and bounded object reads on one machine.
It does not prove independent txLog machines, network replication of data-log
entries, machine-loss durability, concurrent recovery with live writes,
regional RPO, or cloud object latency. Those claims require the existing
independent-machine and real-infrastructure gates.

## Evaluation plan

`evals/suites/serving-recovery-process.toml` owns the frozen process boundary.
The candidate, full-hydration control, and skip-tail poison use the same dataset,
authority topology, object closure, txLog contents, read oracle, seeds, and
binary.

The candidate passes only with zero correctness anomalies, one real worker
kill, a distinct empty-scratch replacement, a non-empty replayed suffix, no
LIST, one manifest GET, one selected index GET, at most one data range GET, no
full data GET, and byte-exact semantic replay for the fixed seed. The suite's
primary metric is p99 `recovery.first_correct_read_duration`.

## Tradeoffs

D1. Read the txLog from its authority-selected root instead of worker-local
metadata. This protects correctness after process loss. It adds coordinator and
durability-provider work to cold open.

D2. Build only the recent point overlay before the first read instead of a
complete local image. This minimizes time to first read. It gives up resident
latency until blocks or a full serving image are admitted later.

D3. Keep the local quorum-file adapter explicit instead of presenting it as
production replication. This produces a useful composition result now. It
leaves the same contract to be repeated on the real OpenRaft data log and three
independent machines.

## Unresolved questions

- The production `DurableLog` streaming and safe-pop API used by a worker.
- Bounded concurrent catch-up when `C` advances during recovery.
- Range-clear representation in the serving overlay.
- Recovery scheduling, admission, cancellation, and memory budgets for many
  ranges opening concurrently.
- Whether authority generation and publication state should expose one combined
  linearizable recovery descriptor after the bootstrap proof.

RFC-0027 and G4.4 now provide a first `[CODE-COMPLETE]` answer for bounded
two-round catch-up and range-clear ordering. Safe pop, overload convergence,
activation leases, and independent-machine execution remain unresolved.
