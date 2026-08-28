# Fable review of RFC-0044

Status: `[VERIFIED]` pre-implementation review completed. This status covers
the review procedure and resolved design blockers, not the RFC implementation.

```text
reviewer:       Claude Fable 5 through OpenCode
session:        ses_fb5b7b554ffen9V78cGXekr5w7
agent mode:     plan
review target:  RFC-0044 before implementation
result:         no semantic blocker for the local-first slice
condition:      run the fresh-authority anchor falsifier first
```

## Findings and resolutions

| Finding | Resolution in RFC-0044 |
|---|---|
| An empty committed anchor is one retained record, not zero. | Receipts require exactly one empty anchor record, zero anchor mutations, zero base-value records, and zero base mutation bytes. |
| Hashing a descriptor that embeds a path containing its own ID creates a fixed-point dependency. | Global content blobs use content-digest keys. The descriptor alone is stored under `fixture_id`. Provider placement does not enter identity. |
| Logically similar suffixes at different versions or batch orders are not the same workload. | Candidate and control bind one exact canonical `RetainedTransactionRecord` stream and `tail_sha256`. |
| Raw RocksDB files are nondeterministic semantic identity. | `resident_image_id` hashes semantic inputs. Physical checkpoint digest and local bytes are observations only. |
| Native and control did not prove the same complete state. | Both must derive from the verified closure plus canonical tail and report one tagged `resident_logical_sha256` over values, tombstones, and absence. |
| `canonical JSON` did not define stable cross-language bytes. | Identity uses the versioned fixed-field `OKVF1` encoding; JSON is inspection output only. |
| A manifest covered through `O` can still reference records older than `O`. | Every decoded base record and every segment minimum and maximum version must equal `O`. |
| Anchor version may vary across fresh authorities. | The first falsifier measures the anchor distribution before descriptor code. Any variation stops the design because `O` enters fixture bytes. |
| Retry of a lost anchor response can accidentally create a second anchor. | The same identity must return the original `O`; a different identity is rejected before establishing another anchor. |
| Existing T27 receipts are frozen evidence. | RFC-0044 gets a new versioned suite. `native-resident-cache-pressure-rerun-v2` remains byte-identical. |

## Final call

The local-first implementation may begin after the anchor-distribution and
exact-retry falsifier passes. GCS persistence and the 1 GiB performance claim
remain later slices. The first code change must not alter the frozen T27 rerun
suite, hot trace, metric aggregator, or admission thresholds.
