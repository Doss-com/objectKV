# Incumbent transaction-plane probes

Status: `[CODE-COMPLETE]` source-pinned probes, `[EVALUATING]` live receipts.

These probes implement the first semantic eliminator in RFC-0041. They do not
implement either provider adapter and do not make an HA or production
durability claim.

## FoundationDB

`foundationdb_semantic_preflight.py` runs against FoundationDB 7.4.6 and checks:

- strict-serializable write-skew rejection;
- one commit versionstamp across user data, retained change, and outcome;
- exact retry after a deliberately discarded successful reply;
- ordered reads and range clear;
- compare-and-advance object frontier conflict.

All transaction reads occur before versionstamped atomic writes. FoundationDB
marks affected versionstamped ranges unreadable inside the writing transaction.

## TiKV

`tikv-write-skew` pins the Rust client revision named by RFC-0041 and runs two
optimistic transactions that read the same two absent keys and write disjoint
keys. Both commits are expected under documented snapshot isolation. That is a
valid TiKV behavior and a knockout failure for the objectKV provider contract.

## Boundary

A provider passing this preflight may enter the R0 lifecycle suite. It is not
selected until objectification, empty-generation restore, unknown outcomes,
and matched hot-path overhead also pass.

## FoundationDB logical lifecycle R0

`foundationdb_lifecycle_r0.py` is the next bounded probe. It:

- commits current state, retained changes, and request outcomes in one active
  FoundationDB generation;
- writes a content-addressed logical closure and manifest to GCS;
- verifies both immutable objects by named generation and SHA-256;
- advances a transactional object frontier with a stale-CAS negative control;
- reconstructs the closure into an empty logical generation in deterministic,
  idempotent chunks;
- atomically activates the destination and rejects a transaction that began in
  the previous generation.

The source FoundationDB media remains present during this run. A passing result
is evidence for the lifecycle seam, not provider media-loss recovery, HA, or
production durability. Those remain separate golden-path gates.
