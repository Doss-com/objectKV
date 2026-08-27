# GP3.1 GCP R0 SSD admission, 2026-08-27

Status: `[VERIFIED]` for the public `SingleRange` recovery and resident-read
mechanism on local NVMe. `[EVALUATING]` for GP3.1 performance because the
candidate missed both frozen 20 percent parity targets.

## Decision result

```text
machine:                    private n2-standard-8, Intel Cascade Lake
stable volume:              200 GiB pd-ssd at /var/lib/objectkv
serving scratch:            375 GiB GCP local SSD, NVMe, ext4
object base:                regional versioned GCS
source revision:            19ea46fc4431b7069496618db83af72cbaa1b882
binary sha256:              21d8358c4ec70940811b3e777770a23dbf5e9899065bae839b57235442ddf93e
machine receipt sha256:     287e809f15ce0c57d31213fb031d1ffc8209368fffdd074ab3e2fe4737fbdcc8
batch:                      gp31nvme1-abba3
samples per subject:        15
measured reads per subject: 3,000,000

public SingleRange:         516,973 reads/s median, 2,482 ns median p99
direct RocksDB:             702,142 reads/s median, 1,749 ns median p99
throughput ratio:           0.7363, candidate 26.37% worse
p99 ratio:                  1.4191, candidate 41.91% worse

public local image:         4,351,739 bytes
first correct read:         636.936 ms median
object response bytes:      4,304,004 median
txLog response bytes:       16,741 median
post-activation object ops: 0
correctness anomalies:      0
comparison verdict:         worse
lease teardown:             9 resources destroyed, 0 benchmark resources remain
```

The public candidate passed every mechanism gate: exact base plus tail reads,
three publication and three data-authority processes, killed-worker replacement,
empty-scratch recovery, complete RocksDB activation, retained-stream cursor
pagination, stable generation selection, a bounded local image, and zero object
operations during measured reads. The direct control also passed every hard
gate. The comparison is therefore valid and the performance miss is real for
this r0 profile.

The 4 MiB working set is a software-overhead baseline after cache warmup. It is
not an SSD cache-pressure or capacity curve. Direct RocksDB is a serving-engine
mechanism control, not a durability-equivalent TiKV comparison.

## Failures that shaped the run

| Attempt | Observation | Correction or next action |
|---|---|---|
| Program preflight | Four repeats were rejected because performance gates require at least five. | Profile now declares five repeats. |
| 64 MiB fixture | More than five minutes elapsed before the first object write because 65,536 fixture records traversed the authority one by one. | r0 uses 4 MiB; add reusable prebuilt fixtures before restoring the 64 MiB and 10 GiB curve. |
| Collector startup | The first Docker Hub pull timed out. | Retry succeeded; retained OTel evidence covers candidate and control. Mirror or pre-pull the collector image for repeatability. |
| Retained-stream page size | A 4,096-record page did not exercise pagination and the candidate was discarded. | Freeze two records per page for the cursor-correct run. |
| Final comparison | Throughput and p99 both missed the 20 percent envelope. | One focused cycle must decompose coverage, overlay, dispatch, lookup, and value-copy costs before the same gate is rerun. |

## Durable evidence

```text
gs://doss-objectkv-dev-okv-evals/results/gp31nvme1-r3/receipts/candidate-a.json
gs://doss-objectkv-dev-okv-evals/results/gp31nvme1-r3/receipts/control-b.json
gs://doss-objectkv-dev-okv-evals/results/gp31nvme1-r3/receipts/comparison.json
gs://doss-objectkv-dev-okv-evals/results/gp31nvme1-r3/receipts/candidate-discard-r2.json
gs://doss-objectkv-dev-okv-evals/results/gp31nvme1-r3/receipts/machine.json
gs://doss-objectkv-dev-okv-evals/results/gp31nvme1-r3/objectkv-otel-evidence/
```

The OTel archive contains three trace export lines, three metric export lines,
and seven log export lines. Both final run IDs occur in all three signals. Raw
GCS scratch objects were deleted after the receipts and telemetry were copied.

## Golden-path consequence

GP3.1 remains `[EVALUATING]`. GP3.2 RAM admission, GP3.3 profile handoff, and
higher application surfaces do not inherit a performance claim from this run.
The next admissible action is to make the declared p99 ratio executable in the
comparison engine, then run one bounded public-read optimization cycle. The
same candidate/control gate follows, plus a second reversed-order batch for
drift. If the public path remains outside the 20 percent envelope, the program
follows its existing stop rule and moves the object-native leverage above
RocksDB or TiKV rather than owning the serving wrapper.
