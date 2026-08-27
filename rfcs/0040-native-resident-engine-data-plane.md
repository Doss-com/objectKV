# RFC-0040: Native resident-engine data plane

- Status: `[EVALUATING]`, native candidate measured and rejected by the frozen
  p99 gate
- Authors: DOSS
- Created: 2026-08-27
- Scope: GP3.1 resident point reads after RFC-0039 reached its stop condition

## Decision to test

Materialize one range's authoritative object base plus visible txLog suffix into
the resident engine. Verify generation, range coverage, object closure, and
applied version when the engine activates or advances. Give reads a bound
engine snapshot handle and remove manifest location plus the external MVCC
overlay from the steady-state point path.

```text
regional quorum txLog                 immutable object base
          |                                      |
          +---------------+----------------------+
                          v
                 ResidentRangeBuilder
                   verify closure at O
                   apply txLog (O, C]
                   publish applied C
                          |
                          v
                 ResidentRangeHandle(T)
                   generation fixed
                   coverage fixed
                   engine snapshot fixed
                          |
                          v
                native point/range lookup
```

This replaces neither the permanent object format nor the transaction
authority. It changes where resident-read correctness is enforced.

## Why RFC-0039 is insufficient

RFC-0039 keeps the recent MVCC tail outside the `ServingImage`. A complete-image
read still traverses coverage checks, the external overlay, dynamic dispatch,
and owned-value transfer around RocksDB. One focused optimization removed
manifest location and object-reference cloning after complete activation.

Two frozen GCP process orders then measured:

| Order | Candidate/control throughput | Candidate/control p99 | Result |
|---|---:|---:|---|
| AB | 0.8068x | 1.3525x | throughput pass, p99 fail |
| BA | 0.8002x | 1.2999x | throughput pass, p99 fail |

The candidate improved 11.18 percent over the prior clean throughput result.
Mean p99 improved less than 1 percent. The predeclared stop condition therefore
ends incremental optimization of that per-read composition.

## Required invariants

For cell generation `G`, assigned range `R`, object-durable version `O`, local
applied version `A`, and requested read version `T`:

```text
ObjectState(G, R, O) + txLog(G, R, O, A] = ResidentState(G, R, A)

read is allowed only when:
  handle.G = current assignment.G
  handle.R is complete
  O <= T <= handle.A
  engine snapshot represents a state at or after T
```

The implementation must not publish `A` before every mutation at or below `A`
is atomically visible in the engine. Range clears, same-version transaction
order, tombstones, and retries retain the RFC-0038 ordering rules.

The current `SingleRange` kernel owns the complete binary keyspace, so its
assigned range has unbounded start and end. Manifest `first_key` and `last_key`
values describe only the object closure at `O`. They are not serving bounds,
because txLog mutations after `O` may insert keys outside that object span.
Future range routing must supply authority-owned half-open bounds separately.

## Engine state

The first RocksDB implementation uses explicit column families or equivalent
namespaces:

```text
head
  user key -> latest value or tombstone at the engine snapshot

history
  user key + descending commit identity -> value or tombstone

metadata
  generation | assigned range bounds | object span | object root | O | applied A | schema version
```

`head` makes the common snapshot lookup an engine-native point operation.
`history` serves an older requested version and retains exact reconstruction
when a transaction's read version predates the latest applied commit. One
atomic engine write batch updates `head`, `history`, indexes required by range
clear, and the applied frontier for each admitted txLog batch.

The first GP3.1 performance subject may use the latest bound engine snapshot,
but a separate correctness gate must interleave engine advancement and reads at
older `T`. A latest-only implementation cannot satisfy the objectKV API.

## Activation and advancement

### Empty activation

1. Read the fenced range assignment and active object root.
2. Fetch and verify the complete named object closure through `O`.
3. Build `head` and `history` in an unpublished engine directory.
4. Read the retained transaction stream after `O` with the batch-aware cursor.
5. Apply complete txLog batches atomically through selected `C`.
6. Flush required engine state and verify byte and generation bounds.
7. Atomically publish the resident directory and metadata at applied `C`.
8. Create a read handle only after all prior steps pass.

### Live advancement

1. Read the next complete retained txLog batch after `A`.
2. Reject another generation, a cursor gap, or an unsupported mutation.
3. Apply data and history mutations in one engine write batch.
4. Advance engine metadata to the batch's final commit identity in that batch.
5. Expose a new snapshot handle. Existing handles remain pinned until released.

The local engine remains disposable. Its WAL may be disabled only when a crash
causes reconstruction from the authoritative object base and retained txLog,
and retention cannot pass the object frontier needed by any rebuild.

## Public boundary

The first internal contract is intentionally narrow:

```rust
trait ResidentRangeEngine {
    fn activate(request: ActivationRequest) -> Result<ActivationReceipt>;
    fn advance(batch: RetainedTransactionBatch) -> Result<AppliedReceipt>;
    fn snapshot(read_version: Version) -> Result<Box<dyn ResidentSnapshot>>;
}

trait ResidentSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<ResidentValue>>;
    fn scan(&self, begin: &[u8], end: &[u8], limit: usize)
        -> Result<Vec<ResidentRecord>>;
}
```

The final Rust types may differ. The semantic boundary may not silently fall
back to object storage after a complete resident handle is returned. Cold reads
remain a separate path with separate latency and request budgets.

## Frozen admission gate

The first performance rerun keeps the GP3.1 profile unchanged:

```text
working set:                  4 MiB, 4,096 keys, 1,024-byte values
seeds:                        1103, 2207, 3301
repeats:                      5 per seed
warmup reads:                 100,000 per sample
measured reads:               200,000 per sample
orders:                       candidate/control and control/candidate
candidate local-byte ceiling: 128 MiB
object operations in window:  0
throughput floor:             0.80x direct RocksDB
p99 ceiling:                  1.20x matched RocksDB snapshot control
```

Candidate and control must use equivalent RocksDB snapshot and value-ownership
semantics. The control may not use a pinned slice if the public candidate must
return an owned 1 KiB value. The receipt records both modes if the zero-copy API
is evaluated as a separate product contract.

## Correctness and failure subjects

The implementation is not admitted without these poisons:

1. metadata claims applied `A` before the engine batch is visible;
2. a same-version second transaction is skipped at a page boundary;
3. point set follows a range clear in the same ordered batch;
4. an old generation opens a handle after reassignment;
5. an incomplete object closure becomes readable;
6. an old snapshot changes after live advancement;
7. process death during activation publishes a partial engine;
8. process death during advancement loses a committed mutation after rebuild;
9. the measured resident window performs an object request;
10. local bytes exceed the profile cap.

## Tradeoff

This optimizes for a direct resident data plane, snapshot isolation inside the
local engine, and one materialized state instead of an external base plus tail
lookup on every read.

It gives up a fully provider-neutral hot path. RocksDB, a RAM engine, TiKV, and
FoundationDB need different implementations of activation, snapshot, MVCC, and
tail advancement. The object and txLog contracts remain provider-neutral above
them.

## Stop and pivot rule

If both reversed-order receipts still report p99 above 1.20x the matched
control, objectKV stops owning the resident and transaction data plane. TiKV or
FoundationDB becomes that plane. `okv-log`, immutable publication, branching,
empty-worker reconstruction, exact historical views, and DataFusion projection
remain the object-native layer above it.

RAM admission, multi-range consensus, PostgreSQL integration, and HTAP
performance do not advance before this decision.

The rule fired on 2026-08-27. The final owned-value AB and BA comparisons
retained 84.11 and 82.68 percent of direct RocksDB throughput, which passed the
0.80x floor. P99 was 1.210x and 1.272x control, which failed the 1.20x ceiling
in both process orders. The native resident engine is retained as a correctness
prototype and evidence artifact, not promoted as objectKV's production data
plane.

## Evidence

- Prior gate:
  `docs/artifacts/eval-receipts/single-range-ssd-gcp-r0-2026-08-27/`
- Focused optimization:
  `docs/artifacts/eval-receipts/single-range-ssd-gcp-r1-2026-08-27/`
- Native engine and pivot receipt:
  `docs/artifacts/eval-receipts/single-range-native-resident-gcp-r2-2026-08-27/`
- Durable comparison objects:
  `gs://doss-objectkv-dev-okv-evals/results/gp31native1-r2/`
- Decisions: D51 and D52 in `docs/DECISIONS.md`

`[CODE-COMPLETE]` The prototype has explicit `head`, `history`, and `metadata`
column families, atomic data plus frontier advancement, generation and range
binding, stable old snapshots, and empty-worker reconstruction from object
base plus retained txLog.

`[VERIFIED]` The correction separating assigned range bounds from immutable
object span removed thirty deterministic tail-insert anomalies. Final AB and BA
runs each completed fifteen candidate and fifteen control samples, three
million measured reads per subject, exact replay, zero measured object
operations, bounded local bytes, and OTel logs, metrics, and traces.

`[EVALUATING]` TiKV versus FoundationDB selection and the adapter contract above
that incumbent plane. The custom native plane does not advance to GP3.2.
