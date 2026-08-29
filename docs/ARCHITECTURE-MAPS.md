# objectKV compact architecture maps

Status: `[EVALUATING]` working visual index.

Canonical layered review set:
[`docs/architecture/README.md`](architecture/README.md). It maps every layer
from `okv-fabric` through GCS/S3, separates decision posture from proof status,
and links each claim to the performance matrix. The RangeEngine tier model is
in [`docs/architecture/RANGE-ENGINE.md`](architecture/RANGE-ENGINE.md).

Canonical living visual:
[`docs/artifacts/objectkv-architecture/objectkv-architecture.html`](artifacts/objectkv-architecture/objectkv-architecture.html).
These text maps remain the diff-friendly source for focused subsystem reviews.

These diagrams are intentionally small. Each answers one operational question
and is kept as text so architecture reviews can edit it directly in a diff.

## Ownership hierarchy

```text
objectKV fleet
├── metacluster                         [FUTURE]
│   └── tenant placement + cell epoch
└── cell                                [PROPOSED]
    ├── transaction system
    │   ├── read-version service
    │   ├── commit proxies
    │   └── resolver partitions
    ├── range groups
    │   ├── replicated txLog
    │   └── disposable serving workers
    └── object state
        ├── authenticated manifest frontier
        ├── immutable row or typed runs
        └── snapshot and query leases
```

A tenant transaction may span ranges inside one cell. It never spans cells.

## Commit path

```text
client
  → read version + routed reads
  → commit envelope
  → conflict validation
  → cell commit version C
  → required txLog quorums sync
  → durable outcome
  → client response

object PUT: absent from normal commit path
```

## Resident and cold point read

```text
key + snapshot T
  → recent DRAM overlay
  → selected ServingImage
      ├── ssd_resident: RocksDB block cache → bounded NVMe
      └── ram_resident: admitted DRAM record, block, or range
  → bounded manifest lookup
  → object range GET
```

Resident service ends in the selected serving image and issues zero object
requests after admission. The remaining steps are the explicit elastic miss
path.

## Serving and durability are separate

```text
ServingProfile                  DurabilityProfile
├── ssd_resident          ×     ├── regional_quorum [DEFAULT]
└── ram_resident                ├── external_journal [PROPOSED]
                                ├── object_ack [PROPOSED]
                                └── volatile_buffered [PROPOSED]

range chooses left             tenant generation chooses right
volatile_buffered reports BUFFERED, never COMMITTED
```

```text
ssd_standard = ssd_resident + regional_quorum
ram_durable  = ram_resident + regional_quorum
ram_turbo    = ram_resident + volatile_buffered
ram_object   = ram_resident + object_ack
```

## Serving-profile handoff

```text
source profile serves generation G
  → destination hydrates through H
  → destination replays durable tail (H, C]
  → destination proves complete coverage
  → assignment flips to generation G+1
  → source is fenced
```

## Small writes to object packing

```text
many committed mutations
          ↓
range-local materialization buffer
          ↓
indexed immutable segment builder
          ↓
data objects + complete manifest
          ↓
fenced root publication
```

There is not one object per transaction, page, or key. Target object size is a
measured policy that balances PUT cost, read locality, parallel recovery, and
compaction debt.

## Recovery frontier

```text
O = object-durable version
C = latest committed version

object state         retained txLog suffix
≤ O                  (O, C]
───────────────┬──────────────────────────
               └── together reconstruct C
```

```text
Database(C) = ObjectState(O) + txLog(O, C]
```

## Manifested storage-layout fork

```text
one ordered MVCC history
          |
          v
manifested object LSM
├── L0 row deltas
├── opaque compacted range -> indexed row run
└── typed compacted range  -> split manifested run if admission gates pass
          ├── indexed row sidecar -> exact value and MVCC point path
          └── narrow columnar projection -> analytical scan path
          |
          ├── PointRunReader -> exact get(key, T)
          └── TypedProjectionReader -> DataFusion + live tail
```

The source of truth is the active immutable closure at `O` plus the retained
txLog suffix `(O, C]`. Row, Parquet, Vortex, or a hybrid are encodings declared
by each run, not independent databases and not source-of-truth labels.

`[EVALUATING]` The split subject is the first locally admitted typed-run shape.
It stores opaque value bytes once in the row sidecar and duplicates only key,
version, operation, and declared analytical fields. The active manifest owns
both access representations as one immutable closure.

## Exact HTAP read

```text
one query snapshot T
        ├── partition p: base Wp + tail (Wp, T]
        ├── partition q: base Wq + tail (Wq, T]
        └── partition r: base Wr + tail (Wr, T]
                              ↓
                  sorted identity overlay
                              ↓
                    Arrow batches at T
```

Materialization lag changes tail cost, not the requested logical version.

## Golden-path evidence graph

```text
semantic history
  → durable tail
  → object base
      ├→ SSD resident → RAM resident → profile handoff
      ├→ empty-worker recovery
      └→ branch root
  → multi-range cell
      ├→ application log → Redis / search
      └→ PostgreSQL page history → exact HTAP
                                      ↓
                               product economics
```

Every arrow names an artifact handoff in
`evals/scenarios/objectkv-golden-path-v1.toml`. Green component receipts do not
verify the path unless they share its scenario identity and artifact digests.

## Proof ladder

```text
1 model                 [VERIFIED]
2 local stable files    [VERIFIED]
3 local MinIO           [VERIFIED]
4 local OS processes    [VERIFIED]
5 independent machines  [PROPOSED]
6 zones + GCS           [EVALUATING]
7 sustained curves      [PROPOSED]
```

Verification at one rung never implies a claim at a higher rung.
