# RFC-0003: Immutable segment contract

- Status: draft
- Created: 2026-08-22

## Questions to resolve

- Point and range-read interface at an explicit version.
- Key/version physical ordering without exposing SST internals to transactions.
- Required min/max key, min/max version, checksum, format version, and statistics.
- Content-addressed versus generated object identity.
- Forward/backward reader compatibility and corruption behavior.
- Whether SlateDB can satisfy the contract through public APIs or a narrow fork.

## Failure gate

A segment published as live must be immutable, checksum-valid, and readable by
the declared compatible reader versions.
