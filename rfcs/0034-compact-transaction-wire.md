# RFC-0034: Compact transaction and batch wire

- Status: `[CODE-COMPLETE]`, local compatibility and byte receipts `[EVALUATING]`
- Authors: DOSS
- Created: 2026-08-26
- Scope: transaction command and Raft application-entry encoding

## Decision

Replace new `OKVT1`, `OKVQ1`, and `OKVB1` writes with backward-readable v2
encodings that represent opaque byte fields as unpadded base64 strings rather
than JSON integer arrays.

```text
logical key and value bytes
          |
          v
OKVT2 transaction JSON with base64 byte fields
          |
          v
OKVB2 batch JSON with base64 command payloads
          |
          v
Raft application entry
```

The v2 format remains an inspectable bootstrap format. It is not the final
zero-copy production codec.

## Why now

The first G4.10a byte-bound control encoded an 8 KiB logical value as an 89,097
byte `OKVB1` application entry. A 128 KiB entry cap therefore admitted only one
transaction per batch. The cap behaved correctly, but the prototype JSON byte
arrays erased the batching mechanism before storage or consensus became the
limiting factor.

This result falsifies performance conclusions drawn from large `OKVT1` values.
The wire must be corrected before rerunning the byte curve.

## Compatibility

Decoders accept both versions:

```text
OKVT1 -> legacy transaction JSON with integer-array bytes
OKVT2 -> transaction JSON with base64 bytes

OKVQ1 -> legacy client envelope with integer-array payload
OKVQ2 -> client envelope with base64 payload

OKVB1 -> legacy batch envelope with integer-array payloads
OKVB2 -> batch envelope with base64 payloads
```

Encoders emit v2 only after this RFC's fixtures pass. Existing journal entries,
snapshots, retained commands, and retry fingerprints remain readable. An exact
retry must use the original retained outcome identity; it does not require the
caller to reproduce old encoded bytes after an upgrade.

## Frozen gates

1. every frozen v1 command and envelope fixture still decodes exactly;
2. v2 fixtures encode and decode byte-exactly;
3. malformed base64 fails closed;
4. transaction, batch, conflict, retry, pagination, failover, and restart tests
   remain unchanged semantically;
5. one 8 KiB-value transaction encodes below 20 KiB as an `OKVB2` entry;
6. the 128 KiB G4.10a byte control forms batches of at least four transactions;
7. no observed entry crosses its configured byte bound.

## Tradeoff

Base64 adds roughly one third to opaque bytes, plus JSON field overhead. It is
materially smaller than integer arrays and preserves inspectability, but it is
still less compact and slower than a length-delimited binary codec. If v2 wire
cost remains material after the G4.10a rerun, a separately versioned binary
codec is required.

## Result

New writes now emit `OKVT2`, `OKVQ2`, and `OKVB2`. Decoders retain v1 reads,
and exact retry accepts the same semantic transaction across a v1-to-v2
upgrade. Frozen v1 and v2 fixtures, malformed-base64 rejection, transaction
semantics, batch semantics, recovery, retry, failover, and restart tests pass.

The G4.10a byte control changed from one 8 KiB-value transaction in an 89,097
byte `OKVB1` entry to eight transactions in a 119,731 byte `OKVB2` entry under
the same 128 KiB cap. The one-transaction compatibility test also holds the v2
entry below 20 KiB. This keeps JSON as an inspectable bootstrap format while
removing integer-array amplification from the current performance curve.

The format mechanism is `[CODE-COMPLETE]`. Its performance evidence remains
`[EVALUATING]` because the receipt used dirty source and one local host.

## Not claimed

- final production wire efficiency;
- zero-copy decode;
- transport compression;
- a stable public client protocol;
- large-value support beyond the bounded transaction and Raft-entry limits.
