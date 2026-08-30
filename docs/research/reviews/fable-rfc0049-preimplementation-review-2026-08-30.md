# Fable RFC-0049 pre-implementation review

Status: `[VERIFIED]` adversarial review returned SHIP for implementation on
2026-08-30. This is a design-review result, not a C5v2 code or performance
claim.

## Scope

Fable reviewed RFC-0049, the independent physical-format generator, the full
T28 physical oracle, positive and corrupted compatibility fixtures, and the
frozen GCS evaluation plan. The review tested:

- exact grouping, wire, and Merkle construction;
- metadata and response-byte feasibility against the reused C0 closure;
- MVCC key-chain and tombstone encoding;
- fair exact-generation reuse of the RFC-0048 C0 child;
- eight logical tasks, eight C0 provider calls, and sixteen C5v2 provider
  calls;
- transport-owned proof that the two candidate calls overlap;
- preservation of the projection-only DataFusion scan path;
- separation between independent expected media and candidate code.

## Findings and resolution

### B1. Physical format was not independently frozen

The first draft used an ambiguous target group size and did not specify frame
encoding, Merkle tree construction, proof order, or odd-leaf handling. It also
lacked compatibility fixtures.

Resolution:

- froze `ordered-key-chain-greedy-v1` with an exact 32-record bound;
- specified the 24-byte index entry, 57-byte projection record, and 28-byte
  frame header;
- specified domain-separated SHA-256 leaves, parent construction, proof order,
  direction, and duplicate-self odd leaves;
- added a standalone generator that imports no objectKV package;
- added exact full-media expectations and positive/corrupted binary fixtures.

### B2. Provider concurrency inherited the wrong cap

Eight logical C5v2 operations can own sixteen simultaneous SDK attempts.
Inheriting RFC-0048's provider cap of eight would serialize work or invalidate
the intended comparison.

Resolution: the plan now binds logical concurrency at eight, C0 provider fanout
at eight, C5v2 provider fanout at sixteen, and overlap evidence to the shared
transport wrapper's actual attempt lifecycle.

### B3. The gate permitted an algorithm shortcut

The first draft required at most two requests, which allowed candidate code to
skip payload reads for tombstone or absent outcomes even though the proposed
algorithm fetches both ranges before decoding the projection.

Resolution: every indexed C5v2 point must issue exactly one projection and one
payload attempt for every outcome kind. Every pair must overlap. The sequential
pair remains a required poison.

### B4. Tombstone compatibility encoding violated the RFC

The second review decoded the generated positive fixture and found tombstone
`key=1, version=2` with `payload_offset=8`, while the format requires zero
offset and length.

Resolution: the independent generator now forces both values to zero,
regenerated every artifact, and freezes a nonzero-tombstone decoder poison.
The final fixture decodes the tombstone as `operation=0, payload_offset=0,
payload_length=0`.

## Final identity

| Artifact | SHA-256 |
|---|---|
| generator | `be32d0ac4374f2f39e8ef7873d396ffef4e95eeeb17dfb5b8a21bfe87273e980` |
| full physical oracle | `f2c2417eea48aa9c30e0c15554e5edb14aaff078e00cd2133066be3a21853b65` |
| positive fixture | `83e97b71674ad93c2359bbdb54628b5ab09ed64fc4021efa230f50a33862304d` |
| corrupted fixture | `40223d127ef436d9453ee05558a43e71332804d95966e4e8c43bbb7058fc5da0` |
| frozen plan | `5b6f2ee2ceaeabae78ff689f33c42fc2bc2022070970e6bb66a1ea410be17d61` |

All three generator outputs byte-match the checked-in artifacts. Fable's final
verdict is SHIP for implementation.
