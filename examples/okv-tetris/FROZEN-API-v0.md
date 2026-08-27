# objectKV Tetris boundary v0

Status: `[CODE-COMPLETE]` example boundary. Frozen on 2026-08-25.

This contract freezes the smallest client boundary needed to run Tetris on the
real `okv-model` and `okv-log` crates. It is an implementation probe, not a
claim that the networked objectKV service exists.

## Boundary

```text
Tetris action
    ↓
TransactRequest
    ├── tenant
    ├── read_version
    ├── request_id
    ├── ordered mutations
    └── application_record
    ↓
CommittedEnvelope → okv-log partition
    ↓
CommitBatch → okv-model MVCC state
    ↓
CommitReceipt
```

The stable identifier is `objectkv-boundary-v0`. The Rust types in
`src/api.rs` are the executable source of truth.

## Local HTTP adapter

The browser playground maps HTTP directly onto the frozen Rust boundary. This
is a development adapter, not the proposed production RPC protocol.

| Method and route | Boundary operation |
| --- | --- |
| `GET /api/spec` | Describe the frozen version and current substrate. |
| `GET /api/state?version=N` | Read the game state and ordered ranges at snapshot `N`. |
| `POST /api/action` | Commit `left`, `right`, `rotate`, `tick`, `drop`, or `reset`. |
| `POST /api/recover` | Discard serving state and replay the current `okv-log`. |
| `POST /api/branch` | Clone the current log and switch to the new branch. |

The adapter serves one minimal playfield and kernel-proof view from a
self-contained HTML document. The board and proof panel use these routes, so no
mock storage path exists in the browser.

## objectKV operations

### Point read

```rust
PointReadRequest {
    tenant: String,
    key: Vec<u8>,
    read_version: Version,
}
```

Returns the newest value visible at `read_version`, or absence. The example
uses this operation to load the encoded game state for rendering and for every
new action.

### Ordered range read

```rust
RangeReadRequest {
    tenant: String,
    start: Vec<u8>,
    end: Vec<u8>,
    read_version: Version,
}
```

Returns visible key/value rows in the half-open interval `[start, end)`. The
example scans materialized board cells and the retained application-event
range.

### Transaction

```rust
TransactRequest {
    tenant: String,
    read_version: Version,
    request_id: u64,
    mutations: Vec<KvMutation>,
    application_record: Vec<u8>,
}
```

Supported mutations are `Set`, `Clear`, and `ClearRange`. One successful call
returns:

```rust
CommitReceipt {
    api_version: "objectkv-boundary-v0",
    commit_version: Version,
    request_id: u64,
    replayed: bool,
    mutation_count: usize,
    txlog_index: u64,
}
```

The game rewrites its materialized board view with one range clear plus point
sets. The point mutations at the same version take precedence over the range
clear, directly exercising the current MVCC rule.

## okv-log operations

The example uses the real `LogState` contract without wrapping away its names:

| Operation | Primitive | Tetris use |
| --- | --- | --- |
| append | `plan_suffix_append` plus `apply_all` | Append one committed envelope per action. |
| replay | `entries_clamped` | Rebuild all MVCC serving state after simulated process loss. |
| fork | `LogState::clone` | Create an independent example branch at the current frontier. |
| suffix replacement | `plan_suffix_append` | Available to later failover examples; not exposed as a game command yet. |
| exact cursor read | `entries_exact` | Reserved for the application-log cursor iteration. |
| retention | `PurgePrefix` | Not exposed until an object-base checkpoint can recover the purged prefix. |

## Committed-envelope encoding

`CommittedEnvelope` is encoded deterministically:

```text
8 bytes   magic "OKVTXV00"
16 bytes  generation and sequence, big endian
8 bytes   request ID, big endian
4 + N     application-record length and bytes
4 bytes   mutation count
repeated  mutation tag plus length-prefixed keys and values
```

Mutation tags are `1 = Set`, `2 = Clear`, and `3 = ClearRange`. Decoding rejects
unknown tags, truncation, and trailing bytes.

## What this freezes

- ordered byte keys and opaque values;
- snapshot point and range reads;
- one atomic mutation batch with stable request identity;
- externally visible commit version and receipt;
- a transactionally associated application record;
- an opaque ordered committed envelope suitable for replay;
- branch-local version history.

## What remains simulated

- one process assigns versions and owns the model;
- `okv-log` is volatile memory, not `okv-wal` or replicated stable media;
- transaction conflict ranges are not yet part of this v0 example;
- application-record emission and KV mutation are composed in-process, not by a
  crash-safe transaction service;
- branch creation clones an in-memory log rather than publishing an object
  manifest root;
- there is no production RPC, range routing, object publication, garbage
  collection, or OTel receipt. The local HTTP adapter is synchronous and owns
  one in-process kernel.

Any later implementation may replace the adapter, but the game should keep
running against this request and response contract or explicitly version it.
