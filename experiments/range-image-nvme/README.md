# RFC 0071 disposable probes

Status: `[ACTIVE-WORK]` the Rust objectKV probe is locally validated in buffered
mode. The pinned RocksDB probe and Linux direct-I/O path require the guarded GCP
Local SSD runner before their results are admissible.

`range-image-nvme-probe` creates the canonical binary trace, materializes the
version-3 aligned objectKV image, and reports exact point and scan curves.
`rocksdb_probe.cc` consumes the same trace and generator using RocksDB tag
`v11.1.2`, commit `3b446089141659fad25328c5ea3e7ed283df46e4`.

Both probes keep the value oracle outside the per-operation latency timer but
inside the throughput interval. This preserves exact verification and exposes
the CPU cost equally. Database creation, flush, compaction, and image creation
are outside the read curves.

These probes are disposable measurement code, not serving-path dependencies.
The frozen contract and admission thresholds remain in RFC 0071 and
`evals/suites/range-image-nvme-incumbent.toml`.
