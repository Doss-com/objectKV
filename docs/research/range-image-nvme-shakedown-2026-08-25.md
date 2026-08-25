# RFC 0071 physical NVMe shakedown

Status: `[ACTIVE-WORK]` one-seed pre-admission result. This is an early
falsification run, not the frozen five-seed verdict.

## Answer

The 64 KiB range image is in the same throughput class as RocksDB under
concurrent direct reads, so the local-serving thesis remains credible. The
current implementation does not pass. It has a high-concurrency tail-latency
collapse, reaches only 0.292x RocksDB ordered-scan throughput, and uses about
6.2x RocksDB CPU per million point reads.

Do not spend on the full matrix yet. Retain the 64 KiB geometry, redesign the
cache and decode path, add coalesced scan I/O, correct the two measurement
defects, then rerun this shakedown.

## Run identity

| Field | Value |
| --- | --- |
| run | `20260825T034822Z-dfc41a06ebf0` |
| candidate | `dfc41a06ebf04135eec254f3b68936abc8078151` |
| RocksDB | `v11.1.2`, `3b446089141659fad25328c5ea3e7ed283df46e4` |
| host | `n2-standard-8`, `us-central1-a` |
| device | one guarded 375 GiB Local SSD, NVMe, 4 KiB sectors |
| fixture | 131,072 keys, 8 KiB values, 1 GiB logical values |
| seed | `724851` |
| cache | 64 MiB per engine |
| payload class | 64 KiB, 60 KiB physical data extents at point p99 |
| provisioned time | 917 seconds |
| raw calibration | skipped explicitly for shakedown |
| OTel and controls | not run |

The configured Local SSD path resolved to `/dev/nvme0n1`; the boot source was
`/dev/sda1`. The device guard observed exactly 402,653,184,000 bytes and passed
before formatting. The ephemeral VM and Local SSD were deleted after upload.

## First physical curve

| Metric | objectKV | RocksDB | Ratio or gate | Read |
| --- | ---: | ---: | ---: | --- |
| image or DB amplification | 1.073x | 1.003x | objectKV <= 1.10x | pass |
| concurrency-1 IOPS | 1,946 | 4,194 | 0.464x | concern |
| concurrency-1 p99 | 0.999 ms | 0.531 ms | 1.880x, objectKV <= 1 ms and <= 2x | pass, narrowly |
| concurrency-8 IOPS | 11,160 | 11,197 | 0.997x | parity |
| concurrency-8 p99 | 1.015 ms | 1.193 ms | 0.851x | pass |
| concurrency-32 IOPS | 11,373 | 11,204 | 1.015x, >= 0.50x | pass |
| concurrency-32 p99 | 53.479 ms | 7.221 ms | 7.406x, <= 2x | fail |
| ordered scan | 62.2 MiB/s | 213.3 MiB/s | 0.292x, >= 0.50x | fail |
| objectKV physical bytes at point p99 | 60 KiB | measurement defect | <= 72 KiB | pass for objectKV |
| objectKV accounted resident bytes | 67,105,230 | not comparable | <= 67,108,864 | pass |
| reported process high-water RSS | 326.5 MiB | 192.2 MiB | objectKV <= 256 MiB | invalid worker measure |
| CPU seconds per million reads, c32 | 477.8 | 76.4 | 6.25x | fail directionally |

All point values, row counts, fixture digests, trace digests, and ordered-scan
digests matched exactly. The objectKV image identity was
`df1bb45eada65b29fedb8cdf7e981d5192717b6765bebd9e184de05e614c77b9`.

The throughput result is useful but incomplete. The concurrency-32 point path
reads about 658 MB/s of physical range-image extents and returns about 93 MB/s
of logical values. Throughput is already at RocksDB parity, but raising
concurrency from 8 to 32 adds only 1.9 percent objectKV throughput while p99
grows from 1.0 ms to 53.5 ms. That is a lock, scheduling, or allocation convoy,
not a fundamental object-storage penalty.

## What the implementation is doing

The point path currently performs:

```text
key
  -> binary search sparse index
  -> global cache mutex
  -> O_DIRECT pread of one 60 KiB extent on miss
  -> SHA-256 of the complete extent
  -> full block decode for structural validation
  -> second block decode to find one row
  -> global cache mutex and linear recency maintenance
  -> 8 KiB value
```

The cache uses a `BTreeMap` plus `VecDeque::retain`. Every hit and insertion
updates recency under one mutex, and recency work is linear in cached entries.
At concurrency 32 this is a credible cause of the 53 ms tail and high CPU.

The scan path reads and verifies one 60 KiB extent at a time, synchronously,
then decodes every extent twice. It does not yet coalesce adjacent extents into
larger direct reads. The observed 62.2 MiB/s therefore measures the first
correct implementation, not a plausible scan ceiling.

## Measurement defects

D1: the RocksDB receipt used `BYTES_READ`, which is a logical `DB::Get` ticker,
as though it were physical block-read bytes. It reported zero and is invalid.
The next probe must aggregate thread-local RocksDB `PerfContext.block_read_byte`
and `block_read_count` with `PerfLevel::kEnableCount`.

D2: objectKV image construction and all three reader configurations ran in one
process. Process-lifetime `VmHWM` therefore retained allocator and prior-reader
high water and reported 326.5 MiB. The 64 MiB reader accounting passed, but the
worker RSS gate was not measured cleanly. Each concurrency and scan must run in
a fresh child process with an interval RSS sampler.

These defects invalidate physical-byte and RSS claims. They do not invalidate
wall-clock latency, IOPS, exactness, image bytes, objectKV explicit file bytes,
or scan duration.

## Next candidate sequence

D3: retain only the 64 KiB geometry for the next point-path candidate. It
optimizes for acceptable 1.073x image amplification and bounded 60 KiB reads.
It gives up smaller blocks until their padding and index cost can clear the
1.10x image gate.

D4: replace the global linear-recency cache with a weighted concurrent cache,
and parse each block once per operation. The target is concurrency-32 p99 no
more than 2x RocksDB while retaining at least 0.5x IOPS.

D5: add a scan-specific coalesced direct-read path, initially 1 MiB extents,
while preserving per-block checksums and bounded output batches. The target is
at least 0.5x RocksDB scan bytes/s.

D6: compare SHA-256 with hardware-accelerated CRC32C for disposable local block
integrity. Retain SHA-256 for image and authority identity. The tradeoff is a
weaker local checksum primitive in exchange for lower CPU on a derived copy;
the authoritative object closure remains cryptographically bound.

Run one hypothesis per commit. Repeat this one-seed shakedown after D4 and D5.
Only a candidate that clears it proceeds to all five seeds, raw 30-second fio,
six unsafe controls, OTel metrics/traces/logs, and an admitted keep or discard.

## Evidence

Results remain in
`gs://doss-objectkv-dev-okv-evals/results/rfc0071/20260825T034822Z-dfc41a06ebf0/`.

| Object | SHA-256 |
| --- | --- |
| device guard | `92def22ddf0b9a5699907a0dd64be6625c5738d540aa9d49369ea8ab55194110` |
| objectKV receipt | `547dfb8da38ad65db2d8eb922d521ffb30927459540115df964d6bd1ea971da4` |
| RocksDB receipt | `465e717c0d6edfbd54b1568eb972baa0e4b451018f0eaf99bf5670b43b331dce` |
| paired summary | `d88169eca7f062fc1082f571c86787f781958d5700b8fdc2fba0dd9fd27bd2cd` |
| run receipt | `4bfdb198032fedb4f38884c3ed59822be45bbb9b37a9e520179a38d52784f229` |

The objectKV probe executable SHA-256 was
`501a4b51b4787b8624d0a065bf3e443685b7a69e15875ecaa74d05031f412d40`;
the RocksDB probe SHA-256 was
`cc0822ab12376651be064698bee6fdc6a14ffea6c2695588db4d9f296bf6f63b`.
