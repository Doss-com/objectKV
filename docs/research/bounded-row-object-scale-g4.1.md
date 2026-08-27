# G4.1 bounded row-object scale diagnostic

- Status: `[EVALUATING]`
- Date: 2026-08-26
- Source state: dirty, release-build diagnostic
- Backend: Apache `object_store` local filesystem
- OTel export: disabled; schema-valid receipts are retained

## Question

Within one bounded range, can objectKV partition immutable row state into
bounded objects while keeping each exact cold point read at one data request
and one block of transferred bytes?

## Physical path

```text
metadata warmup
  one OKVM manifest GET
    -> validate complete ordered closure
    -> one OKVI index GET per bounded OKVB object
    -> charge all cached metadata to the range budget

each measured point read
  key + version
    -> in-memory manifest search
    -> in-memory sparse block-index search
    -> one named OKVB range GET
    -> verify block SHA-256
    -> select newest visible value or tombstone
```

The data-object target is 4 MiB and the block target is 64 KiB. The builder
never splits the versions of one key across objects and rejects a key-version
group that cannot fit the object bound. Object, index, and manifest names are
content addressed.

## Release diagnostic

Candidate and direct control each executed 7,500 uniform reads over three fixed
seeds. All candidate and control hard gates passed at every size.

| Logical range | Encoded data | Objects | Largest object | Cached metadata | Data GETs/read | Bytes/read | Candidate p50 | Candidate p99 | Candidate/control p50 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 MiB | 1,074,346 B | 1 | 1,074,346 B | 2,240 B | 1.000 | 63,873 B | 166.583 us | 214.417 us | 0.983x |
| 8 MiB | 8,594,748 B | 3 | 4,193,503 B | 13,807 B | 1.000 | 64,770 B | 162.708 us | 180.250 us | 0.998x |
| 64 MiB | 68,757,924 B | 17 | 4,193,503 B | 104,754 B | 1.000 | 64,890 B | 166.666 us | 182.292 us | 1.004x |

Measured request work stayed at one data range GET. Transferred bytes remained
within one 65,048-byte maximum block, and candidate latency remained within
diagnostic noise of the direct indexed reader. The 64 MiB closure uses 17 index
warmup GETs and one manifest GET. Its 104,754 bytes of cached metadata are 0.15
percent of encoded data and 0.62 percent of the 16 MiB range metadata budget.

## Whole-object poison

The poison returns exact values but fetches the complete selected bounded
object. It was discarded at every size.

| Logical range | Poison bytes/read | Byte multiple | Poison p50 | p50 multiple |
|---:|---:|---:|---:|---:|
| 1 MiB | 1,074,346 B | 16.82x | 2.222 ms | 13.34x |
| 8 MiB | 4,193,503 B | 64.74x | 7.925 ms | 48.71x |
| 64 MiB | 4,193,503 B | 64.62x | 7.944 ms | 47.66x |

Partitioning caps the poison at one object instead of the complete range. It
does not make complete-object point reads admissible.

## Receipts

Suite hash:
`2ae0eee3feef7c2149e7a1373467b78dc048c5c922783eeebde28802be5a8bfa`.
Candidate source: `a56442ad800deedd72a404a0886e88831eb308a0+dirty`.

| Profile | Workload | Run ID | Verdict | Receipt SHA-256 |
|---|---|---|---|---|
| 1 MiB | candidate | `9842bac8-673f-48b5-81d2-4a3c47159b29` | inconclusive | `933e6a38e12127cbcfe3296224ddbc342a89e14a805f409c0a35304d3726d2fe` |
| 1 MiB | direct control | `93863608-a5b8-4efb-a174-8ffa8084905b` | inconclusive | `89da6afe2d80705aa41312a9cd21203f15aa12fd5fc98ff99bd79111c7a9716a` |
| 1 MiB | scan poison | `73a5bfee-b416-4cda-a84d-3c758a422999` | discard | `ce1b21673ca8846468345ec53dd0c7d5d140686a967bc71ed26377f85f689e86` |
| 8 MiB | candidate | `8d539272-ee2b-456a-a278-3af16000862a` | inconclusive | `41d79b580787a8be7ede00da88d93e9ac5c48440646704558c80dc2d0ba7e74d` |
| 8 MiB | direct control | `43250529-1d37-4e83-a885-a96a42368fc0` | inconclusive | `f700e33a8fd7e813cf2b90cffc45ad78bafdae071897943ff594d58090d6e4c2` |
| 8 MiB | scan poison | `6276ba9f-a838-4d07-b960-8ea61fa9cd56` | discard | `2a0194b590461f361129d6c89c6d42f315c5ab52446d05ebceb693e83b0d913f` |
| 64 MiB | candidate | `c1f21998-aee3-4cbd-8cb4-90b7abd0a12f` | inconclusive | `21f9c2c4e1b8daea69c2230cd705b31e54ec68fcc144502fc863356f27f53430` |
| 64 MiB | direct control | `033ceb79-e4e1-4d68-ad69-a0ed85152228` | inconclusive | `3e9bc23fd3a80237ca6901bde85ae07f22d8d926185f07da2bc7640b716f12b4` |
| 64 MiB | scan poison | `8f73b8ad-abcd-47d4-808e-d87aa632955f` | discard | `0b3f3d9e91c10cee073f9ecc7614def005adbe50faea83450c7c6d76c7358e86` |

## Decision

`[CODE-COMPLETE]` The local pilot now has a checksummed multi-object manifest,
a hard data-object bound, one independently checksummed sparse index per
object, and exact point selection without LIST.

`[EVALUATING]` The release local-filesystem curve supports the physical read
shape through a 64 MiB range. It does not admit G4.1. The receipts are
inconclusive because the source is dirty and OTel export was disabled.

The result narrows the next risk. Point-read data work does not grow across the
measured range sizes, but metadata warmup does grow with object count. Range
splitting must bound that closure, and an empty worker must lazily fetch one
selected index instead of warming every index before its first read.

## Next falsifiers

1. Repeat the frozen format on MinIO and GCS with release binaries, OTel, and
   concurrency at 1, 4, 16, and 64 clients.
2. Measure cold metadata startup separately: one manifest GET, one selected
   index GET, and one data range GET before the first correct read.
3. Add multi-range routing so database growth changes routing metadata, not one
   range's manifest budget.
4. Measure scan, compaction, publication, and request-cost curves before
   changing the 4 MiB object target.
