# RFC-0038: First integrated single-range kernel API

- Status: `[PROPOSED]`, implementation `[CODE-COMPLETE]`, local receipt `[EVALUATING]`
- Authors: DOSS
- Created: 2026-08-27
- Scope: one range, one cell generation, row-object base, and retained txLog tail

## Decision

Expose the first user-facing objectKV composition as an unpublished Rust crate
named `okv`. Its initial executable boundary is `SingleRange`, a one-range
kernel that commits through the replicated transaction authority and serves
exact point reads from:

```text
immutable row-object base through O
               +
retained transaction records in (O, C]
               =
        one readable range at C
```

`SingleRange` is an integration boundary, not the final multi-range client API.
It composes existing authority and object primitives without importing SQL,
PostgreSQL, analytical schemas, or application behavior into the kernel.

## Context and invariant

The G4.4 evaluator already reconstructs a range from a replicated publication
root, a verified immutable row-object closure, and a linearizable retained
transaction stream. That implementation is private to `okv-eval`, assumes a
filesystem backend, and advances recovery with a scalar commit-version cursor.
RFC-0032 later allowed several retained records to share one commit version and
ordered them by `batch_order`. A scalar cursor can therefore skip records when
a bounded page ends inside a transaction batch.

The owning invariant is:

```text
Database(C) = ManifestedObjectState(O) + txLog((O, max), C]

where max is the final batch order covered by O
and every later record is applied exactly once in versionstamp order.
```

An object frontier covers a complete scalar commit version. Opening at `O`
therefore starts after every batch item at `O`.

## Proposed contract

### Types

```rust
pub struct SingleRangeConfig {
    pub authority_endpoints: Vec<String>,
    pub transaction_endpoints: Vec<String>,
    pub publication_root: String,
    pub object_backend: Arc<dyn okv_object::Backend>,
    pub max_page_records: u32,
}

pub struct StreamCursor {
    pub commit_version: u64,
    pub batch_order: Option<u16>,
}

pub enum ReadOutcome {
    Value(Vec<u8>),
    Tombstone,
    Absent,
}

pub struct SingleRange { /* authority clients, verified base, tail */ }
```

`StreamCursor { commit_version: O, batch_order: None }` means resume strictly
after the complete objectified version `O`. A cursor returned for an incomplete
page carries both `next_after_version` and `next_after_batch_order`. The next
request repeats both fields exactly.

### Open

`SingleRange::open`:

1. linearly reads generation state;
2. linearly reads the named publication root;
3. linearly reads generation state again;
4. rejects a generation change or non-active generation;
5. fetches the exact manifest by its authoritative key;
6. verifies manifest length, digest, envelope, generation, and closure shape;
7. freezes the initial txLog target and applies every record after `O` through
   that target in strict versionstamp order.

Opening an exact object base with an empty retained suffix is valid.

### Commit

`SingleRange::commit` submits one deterministic transaction through the
existing quorum-durable `TransactionLogClient`. When the response is committed
at version `C`, the local range catches up through the complete version `C`
before returning. A conflict or rejection never creates tail state.

This first composition supports one caller-owned request identity. Durable
retry identity and commit-unknown resolution remain owned by the transaction
authority. The range kernel does not mint identities.

### Read

`SingleRange::get(key, version)` accepts only `O <= version <= recovered_C`.
It resolves the newest applicable tail point mutation or range clear first. If
the tail has no answer, it locates one base segment, verifies its sparse index,
and performs at most one checksummed data-block range GET. No object LIST is an
authority input.

Tail mutation order is:

```text
(commit_version, batch_order, mutation_ordinal)
```

This order applies to `Set`, `Clear`, and `ClearRange` precedence.

### Observable state

The API exposes immutable open and catch-up receipts plus bounded counters for:

- generation, object version, recovered version, and txLog root;
- txLog pages, records, and response bytes;
- manifest, index, data-range, full-data, and LIST requests;
- object response bytes;
- tail point actions and range clears.

These values feed `okv-eval`. They are evidence for an exact run, not global
performance claims.

## Failure model

- Authority endpoint unavailable or unable to serve a linearizable read.
- Generation changes around the publication-root read.
- Named publication root absent or not a manifest.
- Manifest object missing, corrupt, truncated, or from another generation.
- Retained cursor below the recovery floor or target above the high watermark.
- Retained page out of versionstamp order, outside its frozen target, or with a
  cursor inconsistent with its final record.
- Object index or selected data block missing, corrupt, or identity-mismatched.
- Commit response unavailable or semantically rejected.
- Process death at any point. A replacement must be able to repeat `open` from
  authoritative inputs and empty local state.

The first API has no serving lease or routing epoch. A caller must not present
it as an independently routable production range.

## Alternatives

### Keep the composition private to `okv-eval`

This minimizes API work. It gives up an application-callable primitive and
allows evaluator assumptions to drift from the intended product boundary.

### Expose the object reader and txLog client separately

This offers maximum composition freedom. It gives up one fail-closed recovery
equation and makes every caller reimplement generation fencing, cursor
pagination, mutation ordering, and coverage checks.

### Start with the final multi-range transaction client

This presents a larger product surface. It gives up the ability to verify the
smallest continuous path before range routing, movement, activation leases,
and cross-range reads exist.

## Eval plan

Add `single-range-kernel-v1` as a new suite. Do not modify the frozen G4.4
suite. Its first candidate:

1. starts three real publication-authority and three real transaction-authority
   processes;
2. publishes one row-object base through `O`;
3. commits a batch whose retained page boundary falls inside one shared commit
   version;
4. opens `SingleRange` with `max_page_records = 1`;
5. verifies every batch item, point clear, and range clear at exact `C`;
6. kills the range process and repeats from a distinct empty scratch root;
7. requires byte-identical semantic output and zero object LIST requests.

The shared-version page boundary is a hard gate: the candidate must report at
least one batch-order resume and reconstruct every record. Correctness is a
hard gate. The primary metric is `recovery.first_correct_read_duration`;
secondary curves are txLog page count, object request and response bytes, and
tail resident bytes. A separate negative-control workload remains future work.

`[VERIFIED]` requires a clean-source receipt and a named suite hash. Initial
single-host process evidence remains `[EVALUATING]`.

## Compatibility and migration

The crate is unpublished and the API is explicitly experimental. It consumes
the existing `OKVM`, row-object, publication-authority, and retained-stream
formats without changing their bytes. Scalar retained-stream cursors remain
valid and mean resume after the complete scalar version. New callers must
preserve the optional batch-order component returned by the server.

The G4.4 suite file and frozen inputs remain unchanged. Its private worker keeps
the same externally measured behavior, with the batch-order cursor correction
required by RFC-0032. The new suite owns admission of the public API.

## Unresolved questions

- The serving activation lease and routing epoch that authorize external reads.
- Bounded catch-up convergence while write rate exceeds one range's apply rate.
- Ordered range-scan behavior over base plus tail.
- The RAM and SSD serving-image implementations behind this same contract.
- Objectification scheduling and safe frontier advancement owned by a long-lived
  cell service rather than an eval controller.
- The final public multi-range transaction and read-session API.
