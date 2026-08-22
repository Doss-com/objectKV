# RFC-0010: OLTP and OLAP snapshot semantics

- Status: draft
- Created: 2026-08-22

## Proposed contract

OLTP and analytical representations share commit versions, schema versions,
table/record identity, and one authoritative history. They may use different
physical objects.

## Questions to resolve

- Columnar snapshot metadata and covered-through version.
- Query behavior for base snapshot plus OLTP delta.
- Schema changes inside a materialization interval.
- Retention roots shared by MVCC, CDC, snapshots, and analytical objects.
- When Parquet evidence justifies a later Vortex experiment.
