# RFC-0009: Failure and recovery generations

- Status: draft
- Created: 2026-08-22

## Questions to resolve

- Generation creation, persistence, activation, and rollback rules.
- How old sequencers, WAL leaders, resolvers, workers, and compactors are fenced.
- Which roles may be stateless and which state is reconstructable.
- Recovery ordering between control metadata, WAL, object manifests, and routing.
- Deterministic fault scenarios that prove stale work cannot become authoritative.
