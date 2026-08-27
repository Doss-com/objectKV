# G4.2 lazy empty-worker first-read diagnostic

- Status: `[EVALUATING]`
- Date: 2026-08-26
- Source state: dirty, release-build diagnostic
- Backend: Apache `object_store` local filesystem
- Cache state: new backend and reader instance for every sample
- OTel export: disabled; schema-valid receipts are retained

## Question

Can an empty serving worker return its first exact point read by fetching only
the authoritative manifest, the selected sparse index, and one verified data
block as the assigned range grows from 1 MiB to 64 MiB?

## Physical path

```text
cell authority supplies one exact manifest key
  → named OKVM GET
  → verify manifest identity, checksum, and ordered closure
  → locate the one object whose key bounds contain the request
  → named OKVI GET for that object only
  → verify index identity against the manifest
  → one OKVB range GET for the selected block
  → verify block checksum
  → return the newest visible value or tombstone
```

The candidate does not use LIST, fetch indexes for unrelated objects, or
hydrate a complete data object before the first read. Each reported profile
contains 15 independent first reads: three fixed seeds and five new-reader
repeats per seed. Fixture publication is outside the timed interval.

The control fetches and validates the manifest, every index, and every complete
data object before decoding the same key. The poison uses that full-hydration
path while claiming to be the candidate. It returns exact values but fails the
physical work gates.

## Release diagnostic

Candidate and full-restore control passed their hard gates at all three sizes.
The configured primary statistic is p99. With 15 samples, nearest-rank p99 is
the maximum observed first-read duration.

| Logical range | Objects | Complete closure | Candidate bytes | Candidate p50 | Candidate p99 | Full restore bytes | Full p50 | Full p99 | Full/candidate p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 MiB | 1 | 1,076,586 B | 67,288 B | 310.500 us | 490.916 us | 1,076,586 B | 4.245 ms | 5.261 ms | 10.72x |
| 8 MiB | 3 | 8,608,555 B | 72,571 B | 300.375 us | 493.583 us | 8,608,555 B | 23.797 ms | 24.269 ms | 49.17x |
| 64 MiB | 17 | 68,862,678 B | 80,018 B | 329.792 us | 506.459 us | 68,862,678 B | 136.814 ms | 139.839 ms | 276.11x |

The complete closure grew 63.96x from the 1 MiB to 64 MiB profile. Candidate
response bytes grew 1.19x because the selected block stayed bounded at 65,048
bytes while the manifest grew from 638 to 9,144 bytes. Candidate p99 grew
1.03x. Full-restore bytes grew with the closure and were 860.59x the candidate
at 64 MiB.

This is the desired local physical curve: total assigned bytes affect bounded
manifest metadata, not data bytes fetched before the first point result. Range
splitting must still cap manifest size as the database grows beyond one
assigned range.

## Receipt correction

The first retained run set in `empty-worker-g4.2/` exposed an evaluation
contract defect. The lane declared `p99`, but the result artifact emitted only
the median as the primary value. Those receipts remain immutable and are
marked superseded. The corrected evaluator emits `primary_metric.statistic`
and `primary_metric.value`, then retains median, MAD, and raw samples as
separate diagnostics. All figures in this readout come from the corrected
`empty-worker-g4.2-v2/` set.

## Receipts

Suite hash:
`4f28c333e7fdfed2d6f759b698c71ab0d9d3b3695c61e77b6b5503d735a0065e`.
Candidate source: `a56442ad800deedd72a404a0886e88831eb308a0+dirty`.

| Profile | Workload | Run ID | Verdict | Receipt SHA-256 |
|---|---|---|---|---|
| 1 MiB | lazy candidate | `f33c390e-10c8-4c07-b563-cbc8ab217321` | inconclusive | `a135e95a5e33d2c84af769ddfa4974b9ac6fca889819950a6532dd53893a3dd3` |
| 1 MiB | full restore | `c95c5c22-3f23-4fb7-af93-e9bfa25c3bc5` | inconclusive | `6d2912e6de5c247ddcf2305d096c1db0d40f8505b742bad00e79f6226daaba8c` |
| 1 MiB | hydration poison | `243c235a-a1c1-4964-b2a8-4d9acfc1bf6c` | discard | `164a6d680425c36d318f0e2f867cd0510ac268ac5e20a6cb967e65187c37fc8a` |
| 8 MiB | lazy candidate | `b247645a-3ac0-4863-83f9-12c60554b365` | inconclusive | `c8569c979e39baef7f9303dd95d94300b26f9438f54c508d18e28a3fe0cb3346` |
| 8 MiB | full restore | `4a3fa57f-ec4c-46fa-8b56-eff83cc35829` | inconclusive | `334e916a7f724164da3b21bcb30661841cfd8aa0d503264cd414923e6e28a0ad` |
| 8 MiB | hydration poison | `41e534ae-619d-400c-afd4-a0390cb07d32` | discard | `a15b16a93a3ae2d02992cf7ef6220d293d3fb593b3159c97d35de7ef482fb125` |
| 64 MiB | lazy candidate | `c7c9c387-90bc-4be0-bfe0-1c98508010cb` | inconclusive | `1113f3b1b9c1c860425331aa325dfb48b5a2cd3083d81c16a45a8a3461c51d1e` |
| 64 MiB | full restore | `06df7444-9439-4d64-81d3-68410b52e371` | inconclusive | `68f7c1cb828896fea4f80d4daefb14ef2eb5e1360312a2b134ddead8fed3d301` |
| 64 MiB | hydration poison | `86cb6ff9-bf4e-45b3-8afb-cf39f161120e` | discard | `2c32685c30428ef05b0e9cb1bc3b8641f04d0a9f9e8df0413cd72754b7bd4656` |

## Decision

`[CODE-COMPLETE]` The local point-read pilot now implements the lazy
manifest-first path and a separately measured full-hydration control.

`[EVALUATING]` The local release curve supports the physical G4.2 claim through
a 64 MiB assigned range. It does not admit G4.2 because the source tree is
dirty, OTel is disabled, and the operating-system filesystem cache is not a
remote object store.

The result excludes process startup, routing, cell-root acquisition, txLog tail
replay, scans, range tombstones, concurrent readers, object-store network
latency, throttling, and missing-object recovery. It proves only that the first
point-read data path can avoid work proportional to the complete range closure.

## Next falsifiers

1. Run the frozen path against pinned MinIO and GCS with OTel and explicit cold
   and warm provider-cache profiles.
2. Kill a serving process, start a replacement with empty scratch, open the
   authoritative published root, replay a non-empty txLog tail, and verify the
   first point and range reads.
3. Repeat at 1, 4, 16, and 64 concurrent clients while recording object request
   counts, response bytes, p50, p99, CPU, RSS, and estimated request cost.
4. Add range tombstones and ordered scans before calling the pilot a complete
   transactional segment reader.
