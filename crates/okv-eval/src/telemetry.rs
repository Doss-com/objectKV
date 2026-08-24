use crate::config::{InstrumentKind, MetricDefinition, MetricRegistry, TelemetryConfig};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::error::Error;
use std::fmt::{Display, Formatter};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{fmt, EnvFilter, Layer as _};

#[derive(Clone, Debug)]
pub struct RunResource {
    pub service_version: String,
    pub environment: String,
    pub run_id: String,
    pub suite_id: String,
    pub suite_hash: String,
    pub profile_id: String,
    pub profile_hash: String,
    pub candidate_commit: String,
    pub backend: String,
}

pub struct Telemetry {
    enabled: bool,
    meter_provider: SdkMeterProvider,
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
}

impl Telemetry {
    /// Initialize `OTel` logs, metrics, and traces when an endpoint is configured.
    ///
    /// The standard OTLP environment variables configure the signal-specific
    /// endpoints and headers. Without an endpoint, local profiles retain the
    /// same instrumentation through a no-export SDK provider.
    ///
    /// # Errors
    ///
    /// Returns an error when telemetry is required but not configured, an
    /// exporter cannot be built, or a global tracing subscriber already exists.
    pub fn init(
        config: &TelemetryConfig,
        profile: &str,
        identity: &RunResource,
    ) -> Result<Self, Box<dyn Error>> {
        let endpoint = env::var(&config.endpoint_env)
            .ok()
            .filter(|value| !value.trim().is_empty());
        let required = config
            .required_for_profiles
            .iter()
            .any(|required_profile| required_profile == profile);
        if required && endpoint.is_none() {
            return Err(format!(
                "profile {profile} requires telemetry; set {}",
                config.endpoint_env
            )
            .into());
        }
        let enabled = endpoint.is_some();
        let resource = resource(identity);

        let mut meter_builder = SdkMeterProvider::builder().with_resource(resource.clone());
        let mut tracer_builder = SdkTracerProvider::builder().with_resource(resource.clone());
        let mut logger_builder = SdkLoggerProvider::builder().with_resource(resource);
        if enabled {
            let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .build()?;
            meter_builder = meter_builder.with_periodic_exporter(metric_exporter);

            let span_exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .build()?;
            tracer_builder = tracer_builder.with_batch_exporter(span_exporter);

            let log_exporter = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .build()?;
            logger_builder = logger_builder.with_batch_exporter(log_exporter);
        }

        let meter_provider = meter_builder.build();
        let tracer_provider = tracer_builder.build();
        let logger_provider = logger_builder.build();
        let tracer = tracer_provider.tracer("okv-eval");

        let fmt_filter = eval_log_filter();
        let fmt_layer = fmt::layer()
            .json()
            .with_writer(std::io::stderr)
            .with_target(true)
            .with_filter(fmt_filter);

        let trace_layer = enabled.then(|| tracing_opentelemetry::layer().with_tracer(tracer));
        let log_filter = eval_log_filter()
            .add_directive("hyper=off".parse()?)
            .add_directive("opentelemetry=off".parse()?)
            .add_directive("reqwest=off".parse()?);
        let log_layer = enabled
            .then(|| OpenTelemetryTracingBridge::new(&logger_provider).with_filter(log_filter));

        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(trace_layer)
            .with(log_layer)
            .try_init()?;

        Ok(Self {
            enabled,
            meter_provider,
            tracer_provider,
            logger_provider,
        })
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// Construct a typed recorder from the validated metric registry.
    #[must_use]
    pub fn recorder(&self, registry: &MetricRegistry) -> MetricRecorder {
        MetricRecorder::new(&self.meter_provider.meter("okv-eval"), registry)
    }

    pub fn shutdown(self) {
        let _ = self.meter_provider.force_flush();
        let _ = self.tracer_provider.force_flush();
        let _ = self.logger_provider.force_flush();
        let _ = self.meter_provider.shutdown();
        let _ = self.tracer_provider.shutdown();
        let _ = self.logger_provider.shutdown();
    }
}

fn eval_log_filter() -> EnvFilter {
    if env::var_os("RUST_LOG").is_some() {
        EnvFilter::from_default_env()
    } else {
        EnvFilter::new("warn,okv_eval=info,openraft=off,turmoil=off")
    }
}

fn resource(identity: &RunResource) -> Resource {
    Resource::builder()
        .with_service_name("okv-eval")
        .with_attributes([
            KeyValue::new("service.version", identity.service_version.clone()),
            KeyValue::new("deployment.environment.name", identity.environment.clone()),
            KeyValue::new("okv.eval.run.id", identity.run_id.clone()),
            KeyValue::new("okv.eval.suite.id", identity.suite_id.clone()),
            KeyValue::new("okv.eval.suite.hash", identity.suite_hash.clone()),
            KeyValue::new("okv.eval.profile.id", identity.profile_id.clone()),
            KeyValue::new("okv.eval.profile.hash", identity.profile_hash.clone()),
            KeyValue::new(
                "okv.eval.candidate.commit",
                identity.candidate_commit.clone(),
            ),
            KeyValue::new("okv.eval.backend", identity.backend.clone()),
        ])
        .build()
}

enum RegisteredInstrument {
    Counter(Counter<f64>),
    Gauge(Gauge<f64>),
    Histogram(Histogram<f64>),
}

struct RegisteredMetric {
    definition: MetricDefinition,
    instrument: RegisteredInstrument,
}

pub struct MetricRecorder {
    metrics: HashMap<String, RegisteredMetric>,
    samples: BTreeMap<String, Vec<f64>>,
    series: BTreeSet<(String, Vec<(String, String)>)>,
    max_series: u64,
}

impl MetricRecorder {
    fn new(meter: &Meter, registry: &MetricRegistry) -> Self {
        let mut metrics = HashMap::new();
        for definition in &registry.metrics {
            let instrument = match definition.kind {
                InstrumentKind::Counter => RegisteredInstrument::Counter(
                    meter
                        .f64_counter(definition.otel_name.clone())
                        .with_description(definition.description.clone())
                        .with_unit(definition.unit.clone())
                        .build(),
                ),
                InstrumentKind::Gauge => RegisteredInstrument::Gauge(
                    meter
                        .f64_gauge(definition.otel_name.clone())
                        .with_description(definition.description.clone())
                        .with_unit(definition.unit.clone())
                        .build(),
                ),
                InstrumentKind::Histogram => RegisteredInstrument::Histogram(
                    meter
                        .f64_histogram(definition.otel_name.clone())
                        .with_description(definition.description.clone())
                        .with_unit(definition.unit.clone())
                        .with_boundaries(definition.boundaries.clone())
                        .build(),
                ),
            };
            metrics.insert(
                definition.id.clone(),
                RegisteredMetric {
                    definition: definition.clone(),
                    instrument,
                },
            );
        }
        Self {
            metrics,
            samples: BTreeMap::new(),
            series: BTreeSet::new(),
            max_series: registry.cardinality.max_series_per_run,
        }
    }

    /// Record one value after enforcing the metric's attribute contract.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown metrics, negative counters, missing or
    /// unapproved attributes, non-finite values, or cardinality overflow.
    pub fn record(
        &mut self,
        metric_id: &str,
        value: f64,
        attributes: BTreeMap<String, String>,
    ) -> Result<(), MetricError> {
        if !value.is_finite() {
            return Err(MetricError::new(metric_id, "measurement must be finite"));
        }
        let metric = self
            .metrics
            .get(metric_id)
            .ok_or_else(|| MetricError::new(metric_id, "unknown metric"))?;
        if matches!(metric.definition.kind, InstrumentKind::Counter) && value < 0.0 {
            return Err(MetricError::new(metric_id, "counter must be non-negative"));
        }

        let allowlist: BTreeSet<&str> = metric
            .definition
            .attributes
            .iter()
            .map(String::as_str)
            .collect();
        for key in attributes.keys() {
            if !allowlist.contains(key.as_str()) {
                return Err(MetricError::new(
                    metric_id,
                    &format!("attribute {key} is not allowlisted"),
                ));
            }
        }
        for required in &metric.definition.required_attributes {
            if !attributes.contains_key(required) {
                return Err(MetricError::new(
                    metric_id,
                    &format!("required attribute {required} is missing"),
                ));
            }
        }

        let series_key = (
            metric_id.to_owned(),
            attributes
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        );
        if !self.series.contains(&series_key) && self.series.len() as u64 >= self.max_series {
            return Err(MetricError::new(
                metric_id,
                "series cardinality limit reached",
            ));
        }
        self.series.insert(series_key);

        let otel_attributes: Vec<KeyValue> = attributes
            .into_iter()
            .map(|(key, value)| KeyValue::new(key, value))
            .collect();
        match &metric.instrument {
            RegisteredInstrument::Counter(counter) => counter.add(value, &otel_attributes),
            RegisteredInstrument::Gauge(gauge) => gauge.record(value, &otel_attributes),
            RegisteredInstrument::Histogram(histogram) => histogram.record(value, &otel_attributes),
        }
        self.samples
            .entry(metric_id.to_owned())
            .or_default()
            .push(value);
        Ok(())
    }

    #[must_use]
    pub fn samples(&self, metric_id: &str) -> &[f64] {
        self.samples.get(metric_id).map_or(&[], Vec::as_slice)
    }
}

#[derive(Debug)]
pub struct MetricError {
    metric_id: String,
    message: String,
}

impl MetricError {
    fn new(metric_id: &str, message: &str) -> Self {
        Self {
            metric_id: metric_id.to_owned(),
            message: message.to_owned(),
        }
    }
}

impl Display for MetricError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "metric {}: {}", self.metric_id, self.message)
    }
}

impl Error for MetricError {}

#[cfg(test)]
mod tests {
    use super::MetricRecorder;
    use crate::config::{CardinalityPolicy, InstrumentKind, MetricDefinition, MetricRegistry};
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use std::collections::BTreeMap;

    fn registry() -> MetricRegistry {
        MetricRegistry {
            schema_version: 1,
            namespace: "okv.eval".to_owned(),
            cardinality: CardinalityPolicy {
                max_series_per_run: 1,
                banned_attributes: Vec::new(),
                required_resource_attributes: vec!["service.name".to_owned()],
            },
            metrics: vec![MetricDefinition {
                id: "operation.duration".to_owned(),
                otel_name: "okv.eval.operation.duration".to_owned(),
                kind: InstrumentKind::Histogram,
                unit: "s".to_owned(),
                description: "duration".to_owned(),
                attributes: vec!["workload".to_owned()],
                required_attributes: vec!["workload".to_owned()],
                boundaries: vec![0.1, 1.0],
            }],
        }
    }

    #[test]
    fn enforces_attribute_and_cardinality_contracts() {
        let provider = SdkMeterProvider::builder().build();
        let mut recorder = MetricRecorder::new(&provider.meter("test"), &registry());
        let first = BTreeMap::from([("workload".to_owned(), "a".to_owned())]);
        recorder
            .record("operation.duration", 0.5, first)
            .expect("first series");

        let second = BTreeMap::from([("workload".to_owned(), "b".to_owned())]);
        assert!(recorder.record("operation.duration", 0.5, second).is_err());
    }
}
