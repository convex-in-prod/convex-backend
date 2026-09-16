#[cfg(feature = "static-hermes-wasmtime-gate")]
use std::path::{
    Path,
    PathBuf,
};
use std::time::Duration;

#[cfg(feature = "static-hermes-wasmtime-gate")]
use anyhow::Context as _;
use clap::Parser;
use cmd_util::env::{
    config_service,
    config_tool,
};
#[cfg(feature = "static-hermes-wasmtime-gate")]
use common::knobs::{
    static_hermes_wasm_primary_directions,
    APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS,
    APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_V8_SHADOW_BPS,
    APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS,
    APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_V8_SHADOW_BPS,
};
#[cfg(not(target_os = "linux"))]
use common::knobs::{
    LOCAL_BACKEND_MALLOC_TRIM_ENABLED,
    LOCAL_BACKEND_MEMORY_PRESSURE_SHEDDING_ENABLED,
    LOCAL_BACKEND_MEMORY_RECLAMATION_ENABLED,
};
use common::{
    errors::MainError,
    http::ConvexHttpService,
    knobs::{
        HTTP_SERVER_DEPENDENCY_RESERVE,
        HTTP_SERVER_MAX_CONCURRENT_REQUESTS,
        HTTP_SERVER_TIMEOUT_DURATION,
        NODE_ACTION_USER_TIMEOUT,
    },
    runtime::Runtime,
    sentry::set_sentry_tags,
    shutdown::ShutdownSignal,
    types::{
        DeploymentId,
        MemberId,
    },
    version::SERVER_VERSION_STR,
};
use db_connection::{
    connect_persistence,
    ConnectPersistenceFlags,
};
use function_runner::in_process_function_runner::InProcessFunctionRunner;
use futures::{
    future::{
        self,
        Either,
    },
    FutureExt,
};
use keybroker::{
    DeploymentSecret,
    KeyBroker,
};
#[cfg(target_os = "linux")]
use local_backend::memory_metrics;
use local_backend::{
    config::{
        AdminKeyArgs,
        KeygenCommand,
        LocalConfig,
        Subcommand,
    },
    make_app,
    proxy::dev_site_proxy,
    router::router,
    HttpActionRouteMapper,
};
use node_executor::routed::RoutedLocalNodeExecutorConfig;
use runtime::prod::ProdRuntime;
use tokio::{
    signal::{
        self,
    },
    sync::oneshot,
};

fn main() -> Result<(), MainError> {
    let config = LocalConfig::parse();
    if let Some(subcommand) = &config.subcommand {
        // Subcommands produce machine-parseable output on stdout, so configure
        // tracing to write to stderr (at ERROR level) to avoid contaminating it.
        let _guard = config_tool();
        return run_subcommand(subcommand);
    }
    let _guard = config_service();
    let max_concurrent_requests = *HTTP_SERVER_MAX_CONCURRENT_REQUESTS;
    let dependency_reserve = *HTTP_SERVER_DEPENDENCY_RESERVE;
    assert!(
        dependency_reserve < max_concurrent_requests,
        "HTTP_SERVER_DEPENDENCY_RESERVE must be smaller than HTTP_SERVER_MAX_CONCURRENT_REQUESTS"
    );
    tracing::info!("Starting a Convex backend");
    if !config.disable_beacon {
        tracing::info!(
            "The self-host Convex backend will periodically communicate with a remote beacon \
             server. This is to help Convex understand and improve the product. You can disable \
             this telemetry by setting the --disable-beacon flag or the DISABLE_BEACON \
             environment variable."
        );
    }
    let sentry = sentry::init(sentry::ClientOptions {
        release: Some(format!("local-backend@{}", *SERVER_VERSION_STR).into()),
        ..Default::default()
    });
    if sentry.is_enabled() {
        tracing::info!(
            "Sentry is enabled. Errors will be reported to project with ID {}",
            sentry
                .dsn()
                .map(|dsn| dsn.project_id().to_string())
                .unwrap_or("unknown".to_string())
        );
        sentry::configure_scope(|scope| {
            if let Some(sentry_identifier) = config.sentry_identifier.clone() {
                scope.set_user(Some(sentry::User {
                    id: Some(sentry_identifier),
                    ..Default::default()
                }));
            }
            set_sentry_tags(scope);
        });
    } else {
        tracing::info!("Sentry is not enabled.")
    }

    let tokio = ProdRuntime::init_tokio()?;
    let runtime = ProdRuntime::new(&tokio);

    let runtime_ = runtime.clone();
    let server_future = async {
        run_server(
            runtime_,
            config,
            max_concurrent_requests,
            dependency_reserve,
        )
        .await?;
        Ok(())
    };

    runtime.block_on("main", server_future)
}

fn run_subcommand(command: &Subcommand) -> Result<(), MainError> {
    match command {
        Subcommand::Capabilities => {
            println!("{}", backend_capabilities());
            Ok(())
        },
        Subcommand::Keygen {
            kind: KeygenCommand::AdminKey(args),
        } => generate_admin_key(args),
    }
}

fn backend_capabilities() -> serde_json::Value {
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    {
        let identity = isolate::STATIC_HERMES_WASMTIME_GATE_RUNTIME_SURFACE_POLICY_IDENTITY;
        return serde_json::json!({
            "kind": "convex-local-backend-capabilities-v2",
            "staticHermesRuntimeSurfacePolicy": {
                "inventorySha256": identity.inventory_sha256,
                "kind": identity.kind,
                "runtimeSurfacePolicySha256": identity.runtime_surface_policy_sha256,
            },
            "staticHermesWasmtimeGate": true,
        });
    }

    #[cfg(not(feature = "static-hermes-wasmtime-gate"))]
    serde_json::json!({
        "kind": "convex-local-backend-capabilities-v2",
        "staticHermesRuntimeSurfacePolicy": null,
        "staticHermesWasmtimeGate": false,
    })
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
const STATIC_HERMES_WASM_GATE_ENABLED_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_ENABLED";
#[cfg(feature = "static-hermes-wasmtime-gate")]
const STATIC_HERMES_WASM_GATE_PACKAGE_DIRECTORY_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_PACKAGE_DIRECTORY";
#[cfg(feature = "static-hermes-wasmtime-gate")]
const STATIC_HERMES_WASM_GATE_UDF_PATH_ENV: &str = "CONVEX_STATIC_HERMES_WASM_GATE_UDF_PATH";
#[cfg(feature = "static-hermes-wasmtime-gate")]
const STATIC_HERMES_WASM_GATE_RUNTIME_REGISTRY_ROOT_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_RUNTIME_REGISTRY_ROOT";
#[cfg(feature = "static-hermes-wasmtime-gate")]
const STATIC_HERMES_WASM_GATE_DEPLOYMENT_MANIFEST_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_DEPLOYMENT_MANIFEST";
#[cfg(feature = "static-hermes-wasmtime-gate")]
const STATIC_HERMES_WASM_GATE_ARTIFACT_CACHE_ROOT_ENV: &str =
    "CONVEX_STATIC_HERMES_WASM_GATE_ARTIFACT_CACHE_ROOT";

#[cfg(feature = "static-hermes-wasmtime-gate")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StaticHermesWasmtimeRuntimeAdmissionConfiguration {
    normal_routing_enabled: bool,
    shadow_routing_enabled: bool,
    generated_package_configured: bool,
    deployment_registry_configured: bool,
}

#[cfg(feature = "static-hermes-wasmtime-gate")]
impl StaticHermesWasmtimeRuntimeAdmissionConfiguration {
    fn from_process_environment() -> anyhow::Result<Self> {
        let gate_enabled = match std::env::var(STATIC_HERMES_WASM_GATE_ENABLED_ENV) {
            Ok(value) if value == "0" => false,
            Ok(value) if value == "1" => true,
            Ok(_) => anyhow::bail!("{STATIC_HERMES_WASM_GATE_ENABLED_ENV} must be 0 or 1"),
            Err(std::env::VarError::NotPresent) => false,
            Err(error) => {
                return Err(error).context(format!(
                    "failed to read {STATIC_HERMES_WASM_GATE_ENABLED_ENV}"
                ));
            },
        };
        let (query_wasm_primary_enabled, mutation_wasm_primary_enabled) =
            static_hermes_wasm_primary_directions(gate_enabled)?;
        let normal_routing_enabled = query_wasm_primary_enabled || mutation_wasm_primary_enabled;
        let shadow_routing_enabled = *APPLICATION_STATIC_HERMES_QUERY_SHADOW_BPS > 0
            || *APPLICATION_STATIC_HERMES_MUTATION_SHADOW_BPS > 0
            || *APPLICATION_STATIC_HERMES_QUERY_WASM_PRIMARY_V8_SHADOW_BPS > 0
            || *APPLICATION_STATIC_HERMES_MUTATION_WASM_PRIMARY_V8_SHADOW_BPS > 0;
        let package_directory_configured =
            std::env::var_os(STATIC_HERMES_WASM_GATE_PACKAGE_DIRECTORY_ENV).is_some();
        let udf_path_configured = std::env::var_os(STATIC_HERMES_WASM_GATE_UDF_PATH_ENV).is_some();
        let generated_package_configured = package_directory_configured && udf_path_configured;
        let runtime_registry_root_configured =
            std::env::var_os(STATIC_HERMES_WASM_GATE_RUNTIME_REGISTRY_ROOT_ENV).is_some();
        let deployment_manifest_configured =
            std::env::var_os(STATIC_HERMES_WASM_GATE_DEPLOYMENT_MANIFEST_ENV).is_some();
        let artifact_cache_root_configured =
            std::env::var_os(STATIC_HERMES_WASM_GATE_ARTIFACT_CACHE_ROOT_ENV).is_some();
        let deployment_registry_configured = (runtime_registry_root_configured
            && !deployment_manifest_configured
            && !artifact_cache_root_configured)
            || (!runtime_registry_root_configured
                && deployment_manifest_configured
                && artifact_cache_root_configured);
        Ok(Self {
            normal_routing_enabled,
            shadow_routing_enabled,
            generated_package_configured,
            deployment_registry_configured,
        })
    }

    fn can_admit_wasm(self) -> bool {
        (self.normal_routing_enabled
            && (self.generated_package_configured || self.deployment_registry_configured))
            || (self.shadow_routing_enabled && self.deployment_registry_configured)
    }

    fn quarantine_policy_path(
        self,
        db_spec: &str,
        data_dir: Option<&Path>,
    ) -> anyhow::Result<Option<PathBuf>> {
        if !self.can_admit_wasm() {
            return Ok(None);
        }
        isolate::static_hermes_wasmtime_quarantine_path_for_database_spec(db_spec, data_dir)
            .map(Some)
    }
}

fn generate_admin_key(args: &AdminKeyArgs) -> Result<(), MainError> {
    let secret = DeploymentSecret::try_from(args.instance_secret.as_str())?;
    let broker = KeyBroker::new(&args.instance_name, secret)?;
    let admin_key = broker.issue_admin_key(MemberId(0));
    println!("{}", admin_key.as_str());
    Ok(())
}

async fn run_server(
    runtime: ProdRuntime,
    config: LocalConfig,
    max_concurrent_requests: usize,
    dependency_reserve: usize,
) -> anyhow::Result<()> {
    let serve_future = async move {
        run_server_inner(runtime, config, max_concurrent_requests, dependency_reserve).await
    }
    .fuse();
    futures::pin_mut!(serve_future);

    futures::select! {
        r = serve_future => {
            r?;
            tracing::info!("Done")
        },
    };

    Ok(())
}

async fn run_server_inner(
    runtime: ProdRuntime,
    config: LocalConfig,
    max_concurrent_requests: usize,
    dependency_reserve: usize,
) -> anyhow::Result<()> {
    // Used to receive fatal errors from the database, memory pressure
    // controller, or /preempt endpoint.
    let (preempt_tx, preempt_rx) = oneshot::channel();
    let preempt_signal = ShutdownSignal::new(preempt_tx);
    #[cfg(target_os = "linux")]
    let (external_request_shedding, memory_reclamation) = {
        memory_metrics::validate_startup_budget()?;
        let controller = memory_metrics::initialize_memory_pressure_controller()?;
        let external_request_shedding = controller
            .as_ref()
            .and_then(memory_metrics::CgroupMemoryPressureController::external_request_shedding);
        let memory_reclamation = controller
            .as_ref()
            .map(memory_metrics::CgroupMemoryPressureController::memory_reclamation)
            .unwrap_or_default();
        memory_metrics::start(runtime.clone(), controller, preempt_signal.clone());
        (external_request_shedding, memory_reclamation)
    };
    #[cfg(not(target_os = "linux"))]
    let (external_request_shedding, memory_reclamation) = {
        anyhow::ensure!(
            !*LOCAL_BACKEND_MEMORY_RECLAMATION_ENABLED
                && !*LOCAL_BACKEND_MALLOC_TRIM_ENABLED
                && !*LOCAL_BACKEND_MEMORY_PRESSURE_SHEDDING_ENABLED,
            "Backend memory pressure control requires Linux"
        );
        (
            None,
            common::memory_pressure::MemoryPressureSignal::default(),
        )
    };

    InProcessFunctionRunner::<ProdRuntime>::preflight_context_cache_configuration()?;
    let node_executor_config = RoutedLocalNodeExecutorConfig::preflight_configuration(
        *NODE_ACTION_USER_TIMEOUT + Duration::from_secs(5),
        memory_reclamation.clone(),
    )?;

    // Use to signal to the http service to stop.
    let (shutdown_tx, shutdown_rx) = async_broadcast::broadcast(1);
    #[cfg(feature = "static-hermes-wasmtime-gate")]
    {
        let runtime_admission =
            StaticHermesWasmtimeRuntimeAdmissionConfiguration::from_process_environment()?;
        let data_dir = std::env::var_os("DATA_DIR").map(std::path::PathBuf::from);
        if let Some(quarantine_path) =
            runtime_admission.quarantine_policy_path(&config.db_spec, data_dir.as_deref())?
        {
            // Initialize the process-global policy before constructing a
            // function runner that can admit Wasm work.
            isolate::initialize_static_hermes_wasmtime_quarantine(quarantine_path)?;
        }
    }
    let persistence = connect_persistence(
        config.db,
        &config.db_spec,
        ConnectPersistenceFlags {
            require_ssl: !config.do_not_require_ssl,
            allow_read_only: false,
            skip_index_creation: false,
        },
        &config.name(),
        // No control plane hands a self-hosted backend an ID, so derive a
        // stable one from its name.
        Some(DeploymentId::stable_from_name(&config.name())),
        runtime.clone(),
        preempt_signal.clone(),
    )
    .await?;
    let st = make_app(
        runtime.clone(),
        config.clone(),
        persistence,
        shutdown_rx.clone(),
        preempt_signal.clone(),
        memory_reclamation,
        node_executor_config,
    )
    .await?;
    let router = router(st.clone());
    let mut shutdown_rx_ = shutdown_rx.clone();
    let http_service = ConvexHttpService::new_with_dependency_reserve(
        router,
        "backend",
        SERVER_VERSION_STR.to_string(),
        max_concurrent_requests,
        dependency_reserve,
        &["/api/actions/"],
        external_request_shedding.clone(),
        *HTTP_SERVER_TIMEOUT_DURATION,
        HttpActionRouteMapper,
    );
    let serve_http_future = http_service.serve(config.http_bind_address(), async move {
        let _ = shutdown_rx_.recv().await;
    });
    let proxy_future = dev_site_proxy(
        config.site_bind_address(),
        config.site_forward_prefix(),
        max_concurrent_requests,
        external_request_shedding,
        shutdown_rx,
    );

    let serve_future = future::try_join(serve_http_future, proxy_future).fuse();
    futures::pin_mut!(serve_future);

    // Start shutdown when we get a manual shutdown signal or with the first
    // ctrl-c.
    let mut force_exit_duration = None;
    futures::select! {
        r = serve_future => {
            r?;
            panic!("Serve future stopped unexpectedly!")
        },
        _err = preempt_rx.fuse() => {
            // If we fail with a fatal error, we want to exit immediately.
            tracing::info!("Received a fatal error. Shutting down immediately");
            force_exit_duration = Some(Duration::from_secs(0));
            let _: Result<_, _> = shutdown_tx.broadcast(()).await;
        }
        r = signal::ctrl_c().fuse() => {
            tracing::info!("Received Ctrl-C signal!");
            r?;
            let _: Result<_, _> = shutdown_tx.broadcast(()).await;
        },
    }

    let shutdown = async move {
        // First, drain all in-progress requests;
        tracing::info!("Shutdown initiated, draining existing requests...");
        serve_future.await?;

        // Next, shutdown all of our asynchronous workers.
        tracing::info!("Shutting down application...");
        st.shutdown().await?;

        Ok::<_, anyhow::Error>(())
    }
    .fuse();
    futures::pin_mut!(shutdown);

    let mut force_exit_future = match force_exit_duration {
        Some(force_exit_duration) => Either::Left(runtime.wait(force_exit_duration)),
        None => Either::Right(std::future::pending()),
    }
    .fuse();

    loop {
        futures::select! {
            r = shutdown => {
                r?;
                tracing::info!("Server successfully shut down.");
                // If we are not preempted we exit as soon as the requests are
                // drained. Otherwise, we have to wait for the cool down.
                if force_exit_duration.is_none() {
                    break;
                }
            },
            // Forcibly shutdown when the cool down expires
            _ = force_exit_future => {
                tracing::info!("Cool down expired. Shutting down");
                break;
            }
            // Forcibly shutdown with second ctrl-c.
            r = signal::ctrl_c().fuse() => {
                r?;
                tracing::warn!("Forcibly shutting down!");
                break;
            },
        }
    }

    Ok(())
}

#[cfg(all(test, feature = "static-hermes-wasmtime-gate"))]
mod static_hermes_wasmtime_quarantine_startup_tests {
    use super::*;

    #[test]
    fn url_backed_zero_wasm_configuration_preserves_ordinary_v8_without_data_dir() {
        let configuration = StaticHermesWasmtimeRuntimeAdmissionConfiguration {
            normal_routing_enabled: false,
            shadow_routing_enabled: false,
            generated_package_configured: false,
            deployment_registry_configured: false,
        };
        assert_eq!(
            configuration
                .quarantine_policy_path("postgres://example.invalid/backend", None)
                .unwrap(),
            None
        );
    }

    #[test]
    fn url_backed_wasm_configuration_requires_durable_quarantine_storage() {
        let configuration = StaticHermesWasmtimeRuntimeAdmissionConfiguration {
            normal_routing_enabled: true,
            shadow_routing_enabled: false,
            generated_package_configured: true,
            deployment_registry_configured: false,
        };
        assert!(configuration
            .quarantine_policy_path("postgres://example.invalid/backend", None)
            .is_err());
    }
}
