# RFC-0006: Logical range model

- Status: draft
- Created: 2026-08-22

## Questions to resolve

- Ordered range identifiers, boundaries, routing cache, and assignment records.
- Split/merge publication while historical objects remain shared.
- Ownership generation fencing for read serving, objectification, compaction,
  and administrative work.
- Readiness and routing cutover during movement.
- Metrics proving that movement does not copy durable database bytes.
