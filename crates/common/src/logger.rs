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

fn build_env_filter(base: Option<EnvFilter>) -> EnvFilter {
    let filter = base
        .unwrap_or(EnvFilter::try_from_default_env().unwrap_or_else(|e| {
            println!("failed to setup env filter: {:?}", e);
            EnvFilter::new("info")
        }))
        .add_directive("p3_keccak_air=off".parse().unwrap())
        .add_directive("p3_fri=off".parse().unwrap())
        .add_directive("p3_dft=off".parse().unwrap())
        .add_directive("p3_challenger=off".parse().unwrap());

    let filter = with_sp1_debug(filter);
    let filter = with_cuslop_debug(filter);
    let filter = with_slop_debug(filter);
    filter
}

fn with_sp1_debug(env_filter: EnvFilter) -> EnvFilter {
    env_filter
        .add_directive("sp1_build=debug".parse().unwrap())
        .add_directive("sp1_core_executor=debug".parse().unwrap())
        .add_directive("sp1_core_machine=debug".parse().unwrap())
        .add_directive("sp1_cuda=debug".parse().unwrap())
        .add_directive("sp1_curves=debug".parse().unwrap())
        .add_directive("sp1_derive=debug".parse().unwrap())
        .add_directive("sp1_hypercube=debug".parse().unwrap())
        .add_directive("sp1_jit=debug".parse().unwrap())
        .add_directive("sp1_lib=debug".parse().unwrap())
        .add_directive("sp1_primitives=debug".parse().unwrap())
        .add_directive("sp1_prover=debug".parse().unwrap())
        .add_directive("sp1_prover_types=debug".parse().unwrap())
        .add_directive("sp1_recursion_circuit=debug".parse().unwrap())
        .add_directive("sp1_recursion_compiler=debug".parse().unwrap())
        .add_directive("sp1_recursion_executor=debug".parse().unwrap())
        .add_directive("sp1_recursion_gnark_ffi=debug".parse().unwrap())
        .add_directive("sp1_recursion_machine=debug".parse().unwrap())
        .add_directive("sp1_sdk=debug".parse().unwrap())
        .add_directive("sp1_verifier=debug".parse().unwrap())
}

fn with_cuslop_debug(env_filter: EnvFilter) -> EnvFilter {
    env_filter
        // csl-* crates
        .add_directive("csl_air=debug".parse().unwrap())
        .add_directive("csl_basefold=debug".parse().unwrap())
        .add_directive("csl_challenger=debug".parse().unwrap())
        .add_directive("csl_cuda=debug".parse().unwrap())
        .add_directive("csl_dft=debug".parse().unwrap())
        .add_directive("csl_jagged=debug".parse().unwrap())
        .add_directive("csl_logup_gkr=debug".parse().unwrap())
        .add_directive("csl_merkle_tree=debug".parse().unwrap())
        .add_directive("csl_perf=debug".parse().unwrap())
        .add_directive("csl_prover=debug".parse().unwrap())
        .add_directive("csl_server=debug".parse().unwrap())
        .add_directive("csl_sys=debug".parse().unwrap())
        .add_directive("csl_tracegen=debug".parse().unwrap())
        .add_directive("csl_tracing=debug".parse().unwrap())
        .add_directive("csl_zerocheck=debug".parse().unwrap())
        // cslpc-* crates
        .add_directive("cslpc_basefold=debug".parse().unwrap())
        .add_directive("cslpc_commit=debug".parse().unwrap())
        .add_directive("cslpc_experimental=debug".parse().unwrap())
        .add_directive("cslpc_jagged_sumcheck=debug".parse().unwrap())
        .add_directive("cslpc_logup_gkr=debug".parse().unwrap())
        .add_directive("cslpc_merkle_tree=debug".parse().unwrap())
        .add_directive("cslpc_prover=debug".parse().unwrap())
        .add_directive("cslpc_tracegen=debug".parse().unwrap())
        .add_directive("cslpc_utils=debug".parse().unwrap())
        .add_directive("cslpc_zerocheck=debug".parse().unwrap())
}

fn with_slop_debug(env_filter: EnvFilter) -> EnvFilter {
    env_filter
        .add_directive("slop_air=debug".parse().unwrap())
        .add_directive("slop_algebra=debug".parse().unwrap())
        .add_directive("slop_alloc=debug".parse().unwrap())
        .add_directive("slop_baby_bear=debug".parse().unwrap())
        .add_directive("slop_basefold=debug".parse().unwrap())
        .add_directive("slop_basefold_prover=debug".parse().unwrap())
        .add_directive("slop_bn254=debug".parse().unwrap())
        .add_directive("slop_challenger=debug".parse().unwrap())
        .add_directive("slop_commit=debug".parse().unwrap())
        .add_directive("slop_dft=debug".parse().unwrap())
        .add_directive("slop_fri=debug".parse().unwrap())
        .add_directive("slop_futures=debug".parse().unwrap())
        .add_directive("slop_jagged=debug".parse().unwrap())
        .add_directive("slop_keccak_air=debug".parse().unwrap())
        .add_directive("slop_koala_bear=debug".parse().unwrap())
        .add_directive("slop_matrix=debug".parse().unwrap())
        .add_directive("slop_maybe_rayon=debug".parse().unwrap())
        .add_directive("slop_merkle_tree=debug".parse().unwrap())
        .add_directive("slop_multilinear=debug".parse().unwrap())
        .add_directive("slop_poseidon2=debug".parse().unwrap())
        .add_directive("slop_primitives=debug".parse().unwrap())
        .add_directive("slop_stacked=debug".parse().unwrap())
        .add_directive("slop_sumcheck=debug".parse().unwrap())
        .add_directive("slop_symmetric=debug".parse().unwrap())
        .add_directive("slop_tensor=debug".parse().unwrap())
        .add_directive("slop_uni_stark=debug".parse().unwrap())
        .add_directive("slop_utils=debug".parse().unwrap())
        .add_directive("slop_whir=debug".parse().unwrap())
}

fn get_otlp_endpoint() -> String {
    env::var("OTLP_ENDPOINT").unwrap_or_else(|_| "http://localhost:4317".to_string())
}

pub fn init(resource: Resource) {
    INIT.call_once(|| {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let otlp_endpoint = get_otlp_endpoint();
        let env_filter = build_env_filter(None);
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

        let env_filter = build_env_filter(None);
        let fmt_layer = tracing_subscriber::fmt::layer()
            .compact()
            .with_file(false)
            .with_target(false)
            .with_thread_names(false)
            .with_span_events(FmtSpan::CLOSE)
            .with_writer(std::io::stdout)
            .with_file(true)
            .with_line_number(true)
            .with_filter(env_filter);

        let logging_enabled = std::env::var("LOGGING_ENABLED")
            .map(|s| s == "true")
            .unwrap_or(false);

        let env_filter = build_env_filter(None);
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

        Registry::default()
            .with(telemetry)
            .with(log_export_layer)
            .with(fmt_layer)
            .init();

        log::info!("logging initialized");
        log::debug!("OTLP endpoint configured: {}", otlp_endpoint);
    });
}
