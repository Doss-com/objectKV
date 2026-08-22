# RFC-0001: Project principles

- Status: proposed
- Authors: DOSS
- Created: 2026-08-22

## Decision

objectKV uses these load-bearing principles:

1. Object storage is the permanent tier. The retained WAL suffix is
   authoritative for committed versions not yet objectified.
2. Serving storage is disposable.
3. Transactions are independent of transactional segment encoding.
4. Published object bytes are immutable; transactional references are mutable.
5. Object storage is not the coordination system.
6. Correctness gates performance and distribution.
7. OLTP and OLAP share one logical version history, not necessarily one physical
   file format.
8. objectKV is a general kernel; ZebraDB is a consumer.
9. Redis, inverted search, PostgreSQL, and DataFusion semantics stay above the
   kernel transaction contract.
10. Transactional row segments own the kernel's MVCC algebra. Schema-aware
    Parquet and Vortex artifacts are separate version-aligned materializations.
11. S3 compatibility describes the object API contract, not the transaction or
    file-format contract.

## Tradeoff

This maximizes durable-storage elasticity and a reusable API. It gives up designs
that depend on one node's local disk as permanent truth, even when they would be
simpler or faster in an early benchmark.

## Acceptance evidence

Accept after the initial maintainers agree these principles are hard constraints
and the Phase 0 eval has negative controls for the claims it can currently test.
