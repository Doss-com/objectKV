# RFC-0001: Project principles

- Status: proposed
- Authors: DOSS
- Created: 2026-08-22

## Decision

objectKV uses these load-bearing principles:

1. Object storage is authoritative.
2. Serving storage is disposable.
3. Transactions are independent of physical segment representation.
4. Published object bytes are immutable; transactional references are mutable.
5. Object storage is not the coordination system.
6. Correctness gates performance and distribution.
7. OLTP and OLAP share one logical version history, not necessarily one physical
   file format.
8. objectKV is a general kernel; ZebraDB is a consumer.

## Tradeoff

This maximizes durable-storage elasticity and a reusable API. It gives up designs
that depend on one node's local disk as permanent truth, even when they would be
simpler or faster in an early benchmark.

## Acceptance evidence

Accept after the initial maintainers agree these principles are hard constraints
and the Phase 0 eval has negative controls for the claims it can currently test.
