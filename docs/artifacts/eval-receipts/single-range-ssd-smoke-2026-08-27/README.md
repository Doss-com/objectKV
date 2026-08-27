# SingleRange SSD serving-image diagnostic

Status: `[EVALUATING]`

Run `56535944-86e4-4f31-b1a8-38cce19ea668` exercised the public
`SingleRange` API with a disposable RocksDB serving image, six OpenRaft
authority processes, an empty replacement worker, immutable local object
storage, and txLog catch-up.

Observed on the local arm64 Mac debug build. The scratch path was on APFS
volume `/dev/disk3s5`, backed by physical store `disk0s2` on an internal Apple
SSD AP1024Z NVMe device:

```text
records activated:             256
serving-image local bytes:  86,667
measured point reads:       100,000
throughput:                 824,252 reads/s
p99:                          1,583 ns
post-activation object ops:        0
correctness failures:               0
```

All 15 emitted hard gates passed. The source tree was dirty, the binary was a
debug build, and the dataset was intentionally small. This receipt proves the
NVMe-backed mechanism functions and that measurement wiring is present. It is
not a comparable or production NVMe performance result.

