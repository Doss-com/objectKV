# RFC-0035: Publication GC root graph contract

- Status: accepted for bounded local evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0003, RFC-0004, RFC-0007, RFC-0014

## Decision

`[ACTIVE-WORK]` Every live object closure must be reachable from one durable
publication-authority root. The first root vocabulary is checkpoint, clone,
backup, analytical lease, and tenant move. Mark uses one complete authority
snapshot, and sweep must revalidate the root-intent epoch before reserving a
named object deletion.

## Question

Can the physical local-filesystem adapter preserve all five root classes through
authority reopen and mark/sweep, reclaim only a selectively unpinned closure,
preserve shared objects, and reject a stale delete plan when a new analytical
lease is pinned after mark?

## Frozen history

Each seed writes one shared immutable data object and one unique data object for
each root type. Every root manifest references the shared object and its unique
object.

1. Pin all five manifests and reopen the durable authority.
2. Take one complete mark and sweep every unreachable named object.
3. Verify that all five closures remain readable.
4. Unpin only the clone root, mark again, and reclaim its manifest and unique
   data object.
5. Verify that the other four closures and shared object remain readable.
6. Write a new analytical-lease closure and take a mark before pinning it.
7. Pin the lease, then attempt deletion using the stale mark epoch.
8. Require the authority to reject the reservation and preserve the lease.

The negative control omits the first analytical-lease pin. Its otherwise live
manifest and unique object become unreachable and are deleted. The independent
root-preservation check must discard it.

## Hard gates

- all five root types are durably registered and survive authority reopen;
- the first complete mark preserves every declared root closure;
- selectively unpinning clone reclaims exactly its unique object and manifest;
- objects shared with remaining roots survive clone reclamation;
- the other four root closures remain readable;
- pinning a root after mark changes the root-intent epoch;
- a delete reservation made from the stale mark is rejected;
- the racing analytical lease remains readable;
- two fresh executions produce the same canonical report;
- the omitted analytical-lease control exposes a bounded anomaly.

## Interpretation

A pass admits the explicit root vocabulary and stale-mark revalidation rule
through real filesystem objects and the local durable publication authority. It
does not prove cloud object-store behavior, delayed or inconsistent inventory,
lease expiration, abandoned tenant moves, cross-tenant scale, independent host
failure, or a distributed sweeper.

## Admitted evidence

Candidate `d1ce1ec` kept run `885dfdb4` with zero anomalies across seeds
`1103`, `2207`, and `3301`. It executed 27 checks over 15 durable root
registrations, three authority reopens, nine complete marks, three stale-plan
deferrals, 291 object requests, and exact fresh execution replay. Selectively
unpinning clone reclaimed six objects in total, one manifest and one unique data
object per seed, while all shared and remaining closures stayed readable.

Omitted analytical-lease control `6e8ce843` registered only 12 of the expected
15 roots. It reclaimed the missing lease closure and clone closure, produced 12
anomalies, and discarded. OTel records the correct and control request,
availability, duration, and anomaly series under suite hash `7963b684`.

## Tradeoff

This optimizes for preventing silent deletion caused by an incomplete root graph
before adding policy automation. It gives up lease expiry and fleet-scale
collection until every root owner can first prove durable pin and unpin
lifecycle behavior.
