# Object-store capability evidence

Status: `[EVALUATING]` local profiles are verified; GCS evidence is pending.

Support means an exact backend and version passed one named profile. A segment
row does not permit the backend to host mutable authority metadata.

| Backend | Exact implementation | Segment | Authority | Conditional primitive | End-to-end digest | Guarded delete |
|---|---|---:|---:|---|---:|---:|
| Memory | Apache `object_store 0.14.1`, in-process | pass | pass | ETag | SHA-256 pass | no, reservation plus horizon fallback |
| Filesystem | Apache `object_store 0.14.1`, local filesystem | pass | fail as expected | unsupported | SHA-256 pass | no, reservation plus horizon fallback |
| MinIO | `RELEASE.2025-09-07T16-13-09Z`, Apache `object_store 0.14.1` | pass | pass | `If-Match` ETag | SHA-256 pass | no, reservation plus horizon fallback |
| GCS dev | `[EVALUATING]` `doss-objectkv-dev-okv-evals`, `us-central1` | not run | real namespaced range-read canary passed exactness gates | generation match | not run | adapter and performance canary ran; authority conformance remains open |

## Accepted local receipts

- Candidate commit: `cf37e3bceafe324727b754902f3572ea9cb548fb`.
- Suite hash: `fceafa1f238bdf6ea147f15252346eb552d0e4d8cf046ef757c4f211ba836c2f`.
- Memory authority: run `94815c1d-5daa-4b21-9e1e-ba0968c17229`, verdict
  `keep`, 12/12 cases.
- Filesystem segment: run `5eb6b910-8f4c-42ef-af06-205f434eece3`, verdict
  `keep`, 9/9 cases. The authority failure is retained by the unit and direct
  conformance gates.
- MinIO authority: run `90b83f78-8499-46aa-988b-c74e4f3138cf`, verdict `keep`,
  12/12 cases.
- Each clean eval exported one trace span, two log records, 12 metrics, and 52
  data points to the local OTel collector.

The local authority profile currently contains 12 cases:

1. named read-after-write;
2. revision identity token;
3. immutable create idempotency and conflict rejection;
4. lost create response recovery;
5. exact range read;
6. short range rejection;
7. checksum corruption rejection;
8. LIST non-authority;
9. guarded delete or declared immutable-horizon fallback;
10. conditional root update and stale writer rejection;
11. one-winner conditional update race;
12. lost root update response recovery.

The immutable-overwrite and LIST-authority negative controls both return a
failing report and process status `2`. Provider request count, byte count,
result class, compatibility cases, anomalies, workload duration, traces, and
logs flow through the eval OTel contract. The direct report also records
aggregate request latency. Keys, values, credentials, and request identities
are excluded from metric attributes.

Run commands live in `infra/minio/README.md` and `docs/EVALS.md`.

## Physical publication receipt

Candidate `602b3174ca35f4dd1d897767e4aed71d8b111fcd` ran
`object-publication-adapter-v1` against the local filesystem object client and a
separate checksummed three-file authority prototype. Clean run
`e83eeb60-29ab-447d-950c-7b533672cc43` passed 48 checks with zero anomalies.
Seven unsafe subjects discarded. OTel run
`beaa7904-f2bd-48a8-93e4-3529cb95f98b` exported all three signals and recorded
147 object requests, 3,054 write bytes, 15,939 read bytes, and success ratio 1.

This receipt proves the reservation-based fallback on one local machine. It
does not upgrade filesystem authority support, prove native conditional delete,
or provide GCS, S3, Azure, independent-disk, or multi-process evidence.
