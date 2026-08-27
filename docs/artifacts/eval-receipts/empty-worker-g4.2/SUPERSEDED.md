# Superseded G4.2 diagnostic receipts

These nine immutable receipts are retained as experiment history but are not
the current G4.2 evidence set.

The `empty-worker-recovery` lane selected `p99`, but this result-contract
revision emitted only the sample median. That made the reported primary value
different from the statistic frozen by the lane. The evaluator was corrected
to emit both the configured statistic and its selected value, while retaining
the median and MAD as noise diagnostics. The same frozen workloads were then
rerun into `../empty-worker-g4.2-v2/`.

The superseded suite hash is
`9f37becf9dd7d3ae7e574f92dc49d2fecf5ef28b7cae68f13d5534861e378056`.
The receipts remain dirty-tree local-filesystem diagnostics and must not be
used as `[VERIFIED]` evidence.
