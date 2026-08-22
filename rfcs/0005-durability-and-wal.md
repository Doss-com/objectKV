# RFC-0005: Durability and WAL protocol

- Status: draft
- Created: 2026-08-22

## Invariant

For committed version `C` and object durable version `O`, every acknowledged
mutation in `(O, C]` remains reconstructable from the replicated WAL. WAL through
`X` is reclaimable only after object state is authoritative through `X`.

## Questions to resolve

- `raft-rs` persistence, transport, state machine, and snapshot boundaries.
- Commit-version assignment relative to log append and generation recovery.
- Quorum fsync and `commit_unknown` behavior.
- Conservative global watermark and later tagged pop positions.
- Exact recovery proof after leader loss and partial objectification.
