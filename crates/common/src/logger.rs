use std::env;
use std::sync::Once;

use opentelemetry::global;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::Protocol;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::logs;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::{runtime, trace, Resource};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

static INIT: Once = Once::new();

fn build_env_filter() -> EnvFilter {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|e| {
        println!("failed to setup env filter: {:?}", e);
        EnvFilter::new("info")
    });

    disable_noisy_targets(filter)
}

fn disable_noisy_targets(filter: EnvFilter) -> EnvFilter {
    filter
        .add_directive("p3_keccak_air=off".parse().unwrap())
        .add_directive("p3_fri=off".parse().unwrap())
        .add_directive("p3_dft=off".parse().unwrap())
        .add_directive("p3_challenger=off".parse().unwrap())
}

fn get_otlp_endpoint() -> String {
    env::var("OTLP_ENDPOINT").unwrap_or_else(|_| "http://localhost:4317".to_string())
}

pub fn init(resource: Resource) {
    INIT.call_once(|| {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let otlp_endpoint = get_otlp_endpoint();
        let env_filter = build_env_filter();
        let tracing_enabled = std::env::var("TRACING_ENABLED")
            .map(|s| s == "true")
            .unwrap_or(false);
        let telemetry: Box<dyn Layer<Registry> + Send + Sync> = if !tracing_enabled {
            Box::new(tracing_opentelemetry::layer().with_filter(env_filter))
        } else {
            let tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(&otlp_endpoint)
                        .with_protocol(Protocol::Grpc)
                        .with_timeout(std::time::Duration::from_secs(5)),
                )
                .with_trace_config(
                    trace::config()
                        .with_resource(resource.clone())
                        .with_sampler(trace::Sampler::AlwaysOn),
                )
                .install_batch(runtime::Tokio)
                .unwrap();
            Box::new(
                tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_filter(env_filter),
            )
        };

        let env_filter = build_env_filter();
        let json_logs = std::env::var("LOG_FORMAT")
            .map(|s| s.eq_ignore_ascii_case("json"))
            .unwrap_or(false);
        let base = tracing_subscriber::fmt::layer()
            .with_target(!json_logs)
            .with_thread_names(false)
            .with_span_events(FmtSpan::CLOSE)
            .with_writer(std::io::stdout)
            .with_file(json_logs)
            .with_line_number(json_logs);
        let fmt_layer = if json_logs {
            base.json().with_filter(env_filter).boxed()
        } else {
            base.compact().with_filter(env_filter).boxed()
        };

        let logging_enabled = std::env::var("LOGGING_ENABLED")
            .map(|s| s == "true")
            .unwrap_or(false);

        let env_filter = build_env_filter();
        let log_export_layer: Option<Box<dyn Layer<_> + Send + Sync>> = if logging_enabled {
            let export_layer = opentelemetry_otlp::new_pipeline()
                .logging()
                .with_log_config(logs::config().with_resource(resource))
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(&otlp_endpoint),
                )
                .install_batch(runtime::Tokio)
                .unwrap();

            Some(Box::new(
                OpenTelemetryTracingBridge::new(&export_layer).with_filter(env_filter),
            ))
        } else {
            None
        };

        let tokio_blocked_layer = {
            #[cfg(feature = "tokio-blocked")]
            {
                if let Ok(busy_ms) = std::env::var("TOKIO_BLOCKED_BUSY_MS") {
                    let busy_ms = busy_ms.parse().unwrap();
                    Some(
                        tokio_blocked::TokioBlockedLayer::new().with_warn_busy_single_poll(Some(
                            std::time::Duration::from_micros(busy_ms),
                        )),
                    )
                } else {
                    None
                }
            }

            #[cfg(not(feature = "tokio-blocked"))]
            None::<tracing_subscriber::layer::Identity>
        };
        Registry::default()
            .with(telemetry)
            .with(log_export_layer)
            .with(fmt_layer)
            .with(tokio_blocked_layer)
            .init();

        log::info!("logging initialized");
        log::debug!("OTLP endpoint configured: {}", otlp_endpoint);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_filter_does_not_enable_debug_targets() {
        let filter = disable_noisy_targets(EnvFilter::new("info")).to_string();

        assert!(!filter.contains("=debug"), "{filter}");
    }

    #[test]
    fn targeted_debug_filter_is_preserved() {
        let filter = disable_noisy_targets(EnvFilter::new("info,sp1_prover=debug")).to_string();

        assert!(filter.contains("sp1_prover=debug"), "{filter}");
    }
}
