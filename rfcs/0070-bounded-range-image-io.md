# RFC 0070: Bounded-memory range-image file I/O curve

- Status: frozen before implementation
- Authors: objectKV contributors
- Created: 2026-08-25
- Supersedes: none

## Decision

Measure whether the provider-free assigned-range image selected by RFC 0069
can serve exact point reads and scans without decoding the complete image into
application RAM. The reader must open from a retained local file, keep a
bounded sparse index and block cache, and issue a bounded number of local-file
reads for every cache miss.

This contract does not call the measured path physical NVMe. The evaluator can
prove that the application used explicit file reads, but a portable process
cannot prove whether the host operating system served those reads from its
page cache or from media. Latency is therefore reported as a local-file curve.
A hardware-controlled direct-I/O profile may be added only after this semantic
and byte-I/O contract passes.

## Question

RFC 0069 produced the first viable placed-ready result:

```text
one assigned range of sixteen
logical bytes                    2,101,760
placed bytes                     2,105,087
placement amplification              1.0016x
hydration amplification              1.2094x
post-ready provider requests               0
fresh-process reopen                      pass
```

The selected experimental reader reconstructed a `BTreeMap` containing every
row at open. Its `0.417` to `0.792` microsecond point p99 is useful only as a
decoded-RAM ceiling. It does not answer the load-bearing capacity question:

> Can a range image at least eight times larger than its application-memory
> budget still open cheaply, serve exact random points with at most two file
> reads and 64 KiB read per miss, remain below 1 ms p99 on the local-file path,
> and perform zero object-provider work?

If not, the derived snapshot is only a small-range RAM representation. If it
passes, objectKV has evidence for the intended three-tier serving model:

```text
decoded hot rows and blocks in RAM
  -> indexed immutable assigned-range image on local file or NVMe
  -> immutable object authority used only for rebuild
```

## Terms

- **image bytes**: the complete retained local range-image file, excluding the
  ready receipt;
- **accounted resident bytes**: the full in-memory sparse index, application
  block cache, retained decoded keys and values, and deterministic reader
  bookkeeping;
- **reader memory budget**: the hard maximum for accounted resident bytes;
- **open read bytes**: bytes explicitly read from the image before the first
  requested key;
- **point file-read operations**: explicit positional file reads attributable
  to one point operation after open;
- **point file-read bytes**: bytes returned by those positional reads;
- **application cache hit**: a point completed without a local-file read after
  the reader is open;
- **OS page-cache state**: unmeasured host state below the explicit file-read
  boundary;
- **provider work**: any object-store request or byte after placed readiness.

The accounted memory budget excludes the process runtime, shared libraries,
the OS page cache, and the caller-owned output buffer after a point returns.
Peak RSS delta remains mandatory telemetry so large unaccounted allocations
remain visible, but it is not a portable hard gate in this first curve.

## Required image properties

The experimental image may replace `okv-derived-sorted-range-v1` with a new
incompatible local format. The format remains derived and disposable. It must
contain:

1. a fixed header with format, target version, range bounds, block geometry,
   row count, and root-bound identity digest;
2. a deterministic sparse point index that can be loaded within the open-byte
   budget;
3. independently checksummed data blocks;
4. a checksummed index and footer;
5. strictly ordered keys and unambiguous half-open scan boundaries;
6. enough metadata to reject truncation, reordering, duplicate keys, corrupt
   blocks, and a receipt copied from another image.

The implementation may use a custom block file, RocksDB, SlateDB, Vortex, or
another local representation only if the same byte and memory receipts can be
audited. The first candidate should use the smallest format that tests the
mechanism rather than selecting a permanent engine.

## Frozen dataset and memory ratios

The fixture remains 4,096 deterministic high-entropy values of 8 KiB each,
about 32 MiB of logical data. It uses the RFC 0069 seeds.

The full-range workloads receive a 4 MiB reader budget. The one-of-four
workload receives a 1 MiB budget. In both cases the image must be at least
eight times larger than the budget. A candidate may not count OS page-cache
bytes as application capacity or raise the budget after observing a miss
curve.

Each random workload uses 256 deterministic warmup points and 4,096 measured
points. Uniform and Zipfian 0.99 traces are both required. The sequential scan
must emit the complete range in key order through a bounded output batch.

## Frozen workloads

Correct workloads:

1. full 32 MiB range, 4 MiB budget, uniform random points;
2. full 32 MiB range, 4 MiB budget, Zipfian 0.99 points;
3. one range of four, 1 MiB budget, uniform random points;
4. full range reopened in a fresh process from retained local bytes;
5. full ordered scan with bounded output batches.

Unsafe controls must produce schema-valid `discard` results:

1. decode and retain the complete image despite the memory budget;
2. linearly scan the image for every point instead of using the index;
3. accept a corrupt sparse index or index checksum;
4. skip data-block checksum verification after corrupting one block.

## Metrics and hard gates

The primary metric is p99 explicit file bytes per measured point, minimized.
Every correct workload must satisfy:

```text
correctness anomalies                              = 0
image bytes / reader memory budget                 >= 8
accounted resident bytes                           <= configured budget
open file-read operations                          <= 4
open file-read bytes                               <= 524,288
point file-read operations p99                     <= 2
point file-read bytes p99                          <= 65,536
local-file point latency p99                       <= 1 ms
post-ready provider requests                       = 0
post-ready provider bytes                          = 0
outside-range reads admitted                       = 0
deterministic semantic replay                      = exact
```

Scan throughput, open duration, cache-hit ratio, index bytes, block
amplification, peak RSS delta, and OS-reported I/O are mandatory curve outputs
but are not hard gates in this first portable profile.

The 1 ms point target is a product target, not a claim about uncached media.
The result must always state whether the OS page cache was controlled. The
portable profile sets `os_page_cache_controlled = false`.

## Process and failure semantics

The fresh-process workload receives only:

- the retained image and ready-receipt paths;
- the exact expected cell, tenant, range, assignment epoch, authority root,
  provider closure, target version, and txLog chain identities;
- the fixed trace seed and reader memory budget.

It receives no object-store credentials or provider client. A missing,
truncated, corrupt, stale, or mismatched image must withdraw readiness and fail
closed. It may not consult object storage in the measured point path.

## Candidate surface

An implementation experiment may change only:

- the RFC 0069 experimental range-image writer and reader;
- focused image index, block, cache, corruption, and process-reopen tests;
- the minimum `okv-eval` dispatch and receipt plumbing for this suite.

This RFC, suite, metric definitions, result schema, dataset, traces, seeds,
memory budgets, thresholds, controls, and process boundary are frozen during
an implementation experiment. A contract defect requires a separate commit.

## Bounds and tradeoffs

This contract optimizes for predictable memory and bounded local-file I/O. It
gives up the simplicity and fastest possible latency of retaining every row in
RAM. A sparse index introduces format complexity, checksum work, and block
read amplification.

A passing 32 MiB curve does not prove multi-terabyte cells, concurrent reads,
writes, compaction, or physical NVMe behavior. It establishes the next
mechanism boundary needed before those experiments become meaningful.

## Candidate sequence

1. Freeze this portable local-file contract.
2. Measure the current decode-everything reader as the unsafe memory control.
3. Implement a deterministic sparse-index block image.
4. Run all five correct workloads and four controls at one clean candidate.
5. If kept, add a hardware-controlled NVMe profile without changing semantic
   gates.
6. Only then run remote GCS hydration into the kept local format.

## Unresolved questions

1. What block size minimizes point amplification without making the index or
   scans inefficient?
2. Should the permanent local format use a custom block file, RocksDB,
   SlateDB, or Vortex?
3. How should a production Range Engine share one memory budget across many
   active assigned ranges?
4. Which direct-I/O or page-cache-control mechanism is portable enough for
   trusted NVMe media curves?
5. How should certified recent txLog mutations overlay a large immutable image
   without violating the same memory envelope?
