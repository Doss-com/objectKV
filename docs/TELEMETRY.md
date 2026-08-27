# objectKV evaluation telemetry

Status: `[VERIFIED]` the local eval runner emits OTel logs, metrics, and traces over
OTLP/HTTP when an endpoint is configured. The local collector exports metrics
for Prometheus scraping and prints bounded debug summaries. Durable telemetry
storage and a shared dashboard are `[PROPOSED]`.

## Contract

`evals/metrics.toml` is the source of truth for instruments, units, descriptions,
histogram boundaries, allowed attributes, required attributes, and the per-run
series cap. A suite selects lanes and gates by stable metric ID. Rust code looks
up instruments from the registry, so adding a metric does not require adding a
new recorder type.

Every run carries this bounded resource identity:

- service and build version;
- environment, suite, profile, comparison batch, and their hashes;
- run ID, candidate commit, and backend.

Keys, values, object paths, versions, request IDs, trace IDs, and span IDs are
forbidden as metric attributes. They may appear in sampled logs or traces when
needed, subject to redaction, but never define metric series.

## Local path

```bash
docker compose -f infra/otel/compose.yaml up
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
  cargo run -p okv-eval -- run evals/suites/smoke.toml \
  --profile dev --workload model-smoke --backend model --allow-dirty
curl http://127.0.0.1:8889/metrics

OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
  cargo run -p okv-eval -- run evals/suites/fault-recovery.toml \
  --profile sim-dev --workload overlapping-generation-failures \
  --backend turmoil --allow-dirty

OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
  cargo run -p okv-eval -- run evals/suites/object-store.toml \
  --profile memory-authority \
  --workload named-object-authority-contract \
  --backend memory --allow-dirty
```

When another local stack already owns the default collector ports, start an
isolated objectKV collector without stopping that stack:

```bash
OKV_OTEL_GRPC_PORT=34317 \
OKV_OTEL_HTTP_PORT=34318 \
OKV_OTEL_PROMETHEUS_PORT=38889 \
  docker compose -f infra/otel/compose.yaml up -d

OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:34318 \
  cargo run -p okv-eval -- run <suite> <arguments>
```

The variables change host bindings only. The collector continues listening on
4317, 4318, and 8889 inside its container.

The developer profile permits local JSON-only execution when no endpoint is
set. Cloud profiles fail closed if OTLP is absent. This prevents an expensive or
non-reproducible run from completing without its performance evidence.

`--allow-dirty` is only for local diagnostics. Comparable candidates run from a
clean commit, and the contract hash covers the suite, metric registry, and result
schema together.

The result JSON is the only stdout payload. Structured local logs use stderr and
OTLP, so shell pipelines and the autonomous research loop can parse one result
without discarding telemetry.

Real-infrastructure profiles additionally require a schema-valid machine
receipt. Its SHA-256 digest becomes the result's machine identity, and the
receipt path is retained as an artifact reference. Candidate and control runs
must use the same explicit batch ID and machine-receipt digest before the
comparison authority calculates a percentage.

The default log filter retains `okv_eval=info`, warnings from other targets, and
turns off OpenRaft and Turmoil internals. A partition workload can otherwise
emit megabytes of expected retry messages and distort both the run and its
telemetry cost. Set `RUST_LOG` explicitly to opt into protocol-level detail for
a bounded diagnostic replay.

## Signal roles

| Signal | Use | Retention posture |
|---|---|---|
| Metrics | curves, hard gates, cost and latency comparisons | complete bounded series |
| Traces | explain tail latency and request amplification | sampled outside fault runs |
| Logs | lifecycle, faults, verdict, recovery narrative | structured and rate limited |

The compact schema-valid result remains the comparison authority. OTel is the
high-resolution evidence plane, not a replacement for the frozen result
contract.

The product-thesis registry also reserves bounded instruments for CPU time,
resident memory, NVMe and network bytes, local serving bytes, hydration time and
bytes, branch creation and incremental bytes, manifest-open work, transaction
retries, complete estimated cost, and immutable-object count and size. These
instruments become evidence only when an owning workload records them under a
validated profile.

## Adding or tuning a metric

1. Add one registry entry with stable ID, OTel name, unit, kind, description,
   boundaries, and the smallest useful attribute allowlist.
2. Reference the stable ID from a suite lane or constraint.
3. Record it at the owning layer and add a contract test that rejects missing or
   extra attributes.
4. Validate the suite and inspect the series count before enabling a cloud
   profile.
5. Treat boundary or aggregation changes as a metric-contract revision. Do not
   compare results across different suite or profile hashes.

## Tradeoff

D1: keep instrumentation vendor-neutral at the runner boundary. This optimizes
for portable experiments and one local/cloud contract. It gives up backend-
specific query features until a shared telemetry store is selected.

D2: enforce cardinality before export. This optimizes for bounded telemetry cost
and comparable runs. It gives up ad hoc per-key metric diagnosis, which belongs
in sampled trace events instead.
