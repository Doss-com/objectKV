# objectKV evaluation telemetry

Status: `[ACTIVE-WORK]` the eval runner emits OTel logs, metrics, and traces over
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
- environment, suite, profile, and their hashes;
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
```

The developer profile permits local JSON-only execution when no endpoint is
set. Cloud profiles fail closed if OTLP is absent. This prevents an expensive or
non-reproducible run from completing without its performance evidence.

`--allow-dirty` is only for local diagnostics. Comparable candidates run from a
clean commit, and the contract hash covers the suite, metric registry, and result
schema together.

## Signal roles

| Signal | Use | Retention posture |
|---|---|---|
| Metrics | curves, hard gates, cost and latency comparisons | complete bounded series |
| Traces | explain tail latency and request amplification | sampled outside fault runs |
| Logs | lifecycle, faults, verdict, recovery narrative | structured and rate limited |

The compact schema-valid result remains the comparison authority. OTel is the
high-resolution evidence plane, not a replacement for the frozen result
contract.

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
