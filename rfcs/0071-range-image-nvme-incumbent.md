# RFC 0071: Physical NVMe range-image and RocksDB incumbent curve

- Status: frozen before implementation
- Authors: objectKV contributors
- Created: 2026-08-25
- Supersedes: none

## Decision

Measure the RFC 0070 sparse range image against raw GCP Local SSD capability
and a pinned RocksDB read-only incumbent on one ephemeral host. Both engines
must use the same deterministic 1 GiB logical dataset, exact point traces,
application-cache budget, direct-I/O device, concurrency, checksums, and
ordered-scan oracle. Keep a range-image geometry only if it is exact, remains
within memory and byte bounds, reaches at least half of RocksDB point
throughput, has no more than twice RocksDB point p99, and reaches at least half
of RocksDB scan throughput.

This is a local serving-copy comparison. It does not compare transaction
semantics, replication, durability, objectification, recovery, remote object
latency, or complete system cost.

## Context and invariant

RFC 0070 admitted the portable local-file mechanism:

```text
33,704,472-byte image
4,142,150 audited reader bytes
one 57,530-byte explicit file read at point p99
124.25 us local-file point p99 median
65,714 ordered rows/s
zero provider work after readiness
```

That host's OS page-cache state was uncontrolled. The result proves bounded
application structures and file calls, not physical media latency or
throughput. Under its uniform trace, the 64 KiB-class format averaged about
49.6 KiB of explicit file traffic per 8 KiB logical read. This projects to
about 484 MiB/s at 10,000 reads/s and 4.73 GiB/s at 100,000 reads/s.

The invariant remains:

> A hot assigned range is served from disposable RAM and local NVMe after a
> root-specific placed-ready receipt. Object storage authorizes rebuild state
> and is absent from the admitted point path.

The question is whether the local representation is efficient enough to be a
credible storage substrate, not merely whether it is faster than object
storage.

## Frozen environment

The first physical profile uses:

| Field | Frozen value |
| --- | --- |
| project | `doss-objectkv-dev` |
| zone | `us-central1-a` |
| machine | `n2-standard-8`, 8 vCPU, 32 GiB RAM |
| data device | one 375 GiB GCP Local SSD |
| interface | NVMe |
| guest image | Debian 12 family, exact image identity recorded |
| filesystem | ext4, 4 KiB blocks, `noatime` |
| Rust | 1.88.0 |
| RocksDB | tag `v11.1.2`, commit `3b446089141659fad25328c5ea3e7ed283df46e4` |
| fio | installed package version recorded |
| telemetry | OTLP metrics, traces, and logs required |

The runner must record instance ID, CPU model, kernel, image self-link, Local
SSD by-id path, resolved NVMe device, logical and physical sector sizes,
filesystem UUID and mount options, executable hashes, RocksDB commit and build
flags, fio version, start and stop times, and provisioned resource seconds.

The Local SSD is disposable. Before any destructive device action, the runner
must prove all of the following:

1. the configured path is exactly
   `/dev/disk/by-id/google-local-nvme-ssd-0`;
2. it resolves to an NVMe namespace distinct from every mounted boot device;
3. its capacity is the expected Local SSD capacity;
4. the VM metadata declares exactly one Local SSD;
5. the experiment owns a unique ephemeral runner identity.

Failure of any guard aborts without `blkdiscard`, formatting, or mounting.
Google's published Local SSD benchmark also uses a full write pass followed by
direct fio reads. The experiment follows that sequence on the guarded device.

## Frozen dataset and trace

The logical dataset contains 131,072 ordered binary keys and deterministic
high-entropy values of 8,192 bytes each, exactly 1 GiB of values. Five fixed
seeds are `724851`, `724877`, `724901`, `724921`, and `724939`.

Both engines receive one canonical fixture digest and the same serialized
point trace. The measured trace contains 16,384 warmup points followed by
131,072 measured uniform points per seed. Concurrency points are 1, 8, and 32.
Every requested value is verified against a deterministic per-key oracle. The
reader process may synthesize the expected value for one requested key but may
not retain a complete expected dataset.

Each engine also performs one complete ordered scan through bounded output
batches. The scan receipt includes row count, rolling key/value digest, logical
bytes, physical bytes, duration, and peak worker RSS.

## Compared representations

### objectKV range image

The candidate remains a derived, disposable, root-bound assigned-range image.
The experiment may create incompatible local format version 3. It must retain:

- a fixed root-bound header;
- a deterministic sparse index;
- independently checksummed data extents;
- a checksummed index and footer;
- exact half-open key bounds;
- bounded application cache and in-flight buffers;
- zero object-provider work after readiness.

The block payload classes are 8, 16, 32, and 64 KiB. A class is an unpadded
payload target. Actual extents must be padded to the device's direct-I/O
alignment, and actual bytes are the measured quantity. An 8 KiB value plus
record metadata may therefore require a 12 KiB physical extent.

Every direct read must prove aligned file offset, length, and destination
buffer. Linux must accept the file descriptor with `O_DIRECT`; falling back to
buffered I/O is a hard failure.

### RocksDB incumbent

The incumbent is a small disposable probe linked against the pinned RocksDB
source. It uses the same generator and serialized traces as objectKV. Its
read-only database is populated outside the timed read phase, flushed,
compacted, closed, and reopened before measurement.

Frozen options include:

- compression disabled;
- block size equal to the candidate payload class;
- 64 MiB block cache;
- direct reads enabled;
- mmap reads disabled;
- checksums verified;
- no writes or background compaction during the read phase;
- existing-key point reads only;
- one read-only database and the same concurrency as objectKV.

Database bytes, index and filter bytes, block-cache use, open files, worker RSS,
CPU time, physical reads, and scan throughput are mandatory. The RocksDB probe
must verify every returned value and the complete ordered-scan digest.

## Raw device calibration

Before formatting, fio performs a full 1 MiB direct write pass to the guarded
Local SSD. It then reports 30-second direct random-read curves for 4, 8, 16,
32, and 64 KiB blocks at queue depths 1, 8, 32, and 128, plus a 1 MiB
sequential-read curve. Raw calibration is an environment receipt, not the
candidate score.

The raw receipt must contain fio JSON, direct-I/O state, IOPS, bytes/s, latency
p50/p95/p99/p99.9, CPU, errors, device identity, and complete command
arguments. Missing or malformed calibration makes the batch invalid.

## Cache states

Two states are reported separately:

1. `direct-media`: both engines use direct reads. The OS page cache is outside
   the data path by construction.
2. `buffered-warm-ceiling`: direct reads are disabled, the complete selected
   data is scanned once, and the same point traces are replayed. This is a CPU
   and engine-overhead ceiling, not a media claim.

Direct and buffered results may not be combined into one latency distribution.
The candidate decision uses only `direct-media`.

## Metrics and gates

The primary metric is objectKV direct-media point IOPS divided by matched
RocksDB direct-media point IOPS at concurrency 32, maximized independently for
each payload class.

The scalar `range_image.nvme_point_p99` gate contains only the concurrency-1
candidate distribution. The attributed `range_image.nvme_point_latency`
telemetry and relative p99 receipt contain all concurrency points. Relative
IOPS primary samples contain only concurrency 32.

Every candidate geometry must satisfy:

```text
correctness anomalies                                     = 0
direct-I/O alignment violations                           = 0
objectKV image bytes / logical bytes                      <= 1.10
objectKV audited reader, cache, and in-flight bytes       <= 64 MiB
objectKV read-worker peak RSS                             <= 256 MiB
objectKV point p99 at concurrency 1                       <= 1 ms
objectKV point physical bytes p99                         <= 72 KiB
objectKV / RocksDB point IOPS at concurrency 32           >= 0.50
objectKV / RocksDB point p99 at concurrency 1, 8, and 32  <= 2.00
objectKV / RocksDB ordered-scan bytes/s                   >= 0.50
post-ready provider requests and bytes                    = 0
deterministic semantic replay                             = exact
```

At least one payload class must keep. If all four discard, redesign the local
representation before GCS hydration or distributed throughput work. A class
that passes ratios but violates correctness, identity, memory, byte, or
provider gates is discarded.

Mandatory reported curves include point latency p50/p95/p99/p99.9, IOPS,
logical and physical bytes/s, physical bytes per point, application-cache hit
ratio, CPU seconds per million points, voluntary and involuntary context
switches, major and minor faults, read-worker RSS, image or database bytes,
open duration, scan throughput, and relative engine ratios.

## Unsafe controls

Every control must produce a schema-valid `discard`:

1. report buffered reads as direct-media reads;
2. accept an unaligned direct-I/O offset, length, or buffer, or silently retry
   it through buffered I/O;
3. skip objectKV block checksum verification after corrupting one extent;
4. give RocksDB a larger cache or a different point trace;
5. skip the RocksDB value or complete-scan oracle;
6. target the boot device or any device that fails the Local SSD ownership
   guard.

## Failure model

- A spot interruption, host error, timeout, or process crash produces no
  performance verdict. The VM and Local SSD are deleted and the complete batch
  is retried on a new host.
- An I/O error, checksum mismatch, short read, or alignment error fails closed.
- An incomplete raw calibration, engine matrix, semantic replay, or telemetry
  export invalidates the complete batch.
- Results from different hosts may not be paired into a relative ratio.
- The controller deletes the VM, boot disk, Local SSD attachment, firewall,
  SSH metadata, build scratch, and temporary source after collecting compact
  receipts. Cleanup failure is reported and retried, not hidden.

## Candidate surface

An implementation experiment may change only:

- the experimental range-image writer, reader, alignment, block geometry, and
  bounded cache under `okv-object`;
- focused range-image NVMe and corruption probes;
- the minimum `okv-eval` dispatch, receipt, telemetry, and process plumbing;
- a clearly marked disposable RocksDB probe under `experiments/`;
- one guarded ephemeral-runner script under `infra/gcp/`.

This RFC, suite, metric definitions, dataset, seeds, traces, budgets, machine,
device count, RocksDB commit and options, thresholds, and controls are frozen
during a candidate experiment. A defect requires a separate contract commit.

## Alternatives

### Compare only with fio

This isolates media efficiency but does not answer whether objectKV adds too
much engine overhead. Keep fio as the device floor, not the product incumbent.

### Compare with TiKV

TiKV adds Raft, networking, scheduling, and transactional layers that RFC 0071
does not exercise in objectKV. That comparison belongs after the cell commit
path and KV Runtime are composed. RocksDB is the closer local storage-engine
incumbent for this gate.

### Keep using buffered reads and drop caches

Dropping caches is coarse and does not guarantee that every measured point
reaches media. Direct I/O has alignment complexity but gives the experiment a
stronger boundary.

### Select RocksDB as the objectKV local format now

RocksDB is a valid future candidate, but selecting it before measuring a small
range-native image would conflate mechanism choice with benchmark outcome. The
experiment keeps the custom format disposable and lets the measured curve
decide whether to retain, replace, or embed it.

## Compatibility and migration

Range-image formats remain local, derived, and disposable. A version-3 image
may be incompatible with version 2. Existing placed-ready receipts must reject
the new format until the writer, reader, and receipt identity agree. Rollback
deletes the derived image and rehydrates version 2 from the same authoritative
object root. No public key-value API or durable object format changes in this
RFC.

## Sources

- Google Cloud, [Benchmarking Local SSD performance](https://cloud.google.com/compute/docs/disks/benchmarking-local-ssd-performance)
- Google Cloud, [`gcloud compute instances create`](https://cloud.google.com/sdk/gcloud/reference/compute/instances/create)
- RocksDB, [Benchmarking tools](https://github.com/facebook/rocksdb/wiki/Benchmarking-tools)
- RocksDB, [release v11.1.2](https://github.com/facebook/rocksdb/releases/tag/v11.1.2)

## Unresolved questions

1. Which payload class clears random-read and scan gates together?
2. Does synchronous threaded `pread` reach the device envelope, or is an
   `io_uring` implementation required after the first candidate?
3. How much of the relative gap comes from SHA-256 verification, block decode,
   cache locking, or system-call scheduling?
4. Does a surviving geometry remain viable with a certified recent-MVCC
   overlay and writes?
5. Should the permanent local representation remain custom, embed RocksDB, or
   use another engine after the first fair comparison?
