# RFC-0002: Version and MVCC model

- Status: draft
- Created: 2026-08-22

## Proposed contract

- Commit versions are externally assigned, unique, and totally ordered.
- Commit versions are not wall-clock timestamps.
- Reads at version `R` return the newest visible mutation at `version <= R`.
- An exact mutation-batch replay at one version is idempotent.
- A different batch replayed at an existing version is corruption.
- A read newer than a worker's applied version is unavailable, not silently
  served from an older snapshot.

## Open decisions

- Whether gaps are legal and how recovery distinguishes a gap from missing log.
- Physical encoding and generation/epoch representation.
- Range tombstone visibility and compaction.
- Oldest readable version across transactions, snapshots, CDC, backups, and
  analytical snapshots.
- Large-value references and atomic visibility.

## Eval plan

Extend `okv-model` only after examples settle. Generated histories must minimize
any divergence between model and candidate implementation.
