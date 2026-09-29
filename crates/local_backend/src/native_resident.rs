//! Recover native execution from the same durable source-package authority used
//! by function deployment. HTTP response delivery never owns reconciliation.
use std::{
    sync::Arc,
    time::Duration,
};

use application::Application;
use model::source_packages::{
    native::NativeResidentDescriptor,
    SourcePackageModel,
};
use node_executor::native::NativeResidentSupervisor;
use roles::RequireDeploymentOp;
use runtime::prod::ProdRuntime;
use sync_types::Timestamp;
use value::TableNamespace;

struct CommittedSelection {
    descriptor: Option<NativeResidentDescriptor>,
    version: Timestamp,
}

async fn committed_selection(
    application: &Application<ProdRuntime>,
) -> anyhow::Result<CommittedSelection> {
    let mut tx = application.begin(keybroker::Identity::system()).await?;
    let descriptor = SourcePackageModel::new(&mut tx, TableNamespace::Global)
        .get_latest_record()
        .await?
        .and_then(|package| package.native_resident.clone());
    let version = tx.into_token()?.ts();
    Ok(CommittedSelection {
        descriptor,
        version,
    })
}

pub async fn reconcile(
    application: &Application<ProdRuntime>,
    supervisor: &NativeResidentSupervisor,
) -> anyhow::Result<()> {
    let committed = committed_selection(application).await?;
    supervisor.reconcile(committed.descriptor, committed.version)
}

pub fn start(
    application: Application<ProdRuntime>,
    supervisor: Arc<NativeResidentSupervisor>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if supervisor.is_shutting_down() || supervisor.status().phase == "stopped" {
                return Ok(());
            }
            if let Err(error) = reconcile(&application, &supervisor).await {
                if supervisor.is_shutting_down() {
                    return Ok(());
                }
                tracing::error!("Native resident durable-selection reconciliation failed");
                supervisor.request_shutdown();
                return Err(error);
            }
        }
    })
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ControlRequest {
    admin_key: String,
    command: ControlCommand,
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
enum ControlCommand {
    Inspect,
    Prepare {
        descriptor: model::source_packages::native::NativeResidentDescriptor,
    },
    ForceRetire {
        generation: String,
    },
}

/// Administrative staging and inspection do not change committed selection.
pub async fn control(
    common::http::extract::MtState(st): common::http::extract::MtState<crate::LocalAppState>,
    axum::Json(request): axum::Json<ControlRequest>,
) -> Result<axum::Json<serde_json::Value>, common::http::HttpResponseError> {
    async {
        let identity = crate::admin::must_be_admin_from_key(
            st.application.app_auth(),
            st.instance_name.clone(),
            request.admin_key,
        )
        .await?;
        identity.require_operation(keybroker::DeploymentOp::Deploy)?;
        match request.command {
            ControlCommand::Inspect => (),
            ControlCommand::Prepare { descriptor } => {
                st.native_resident.prepare(&descriptor, false).await?
            },
            ControlCommand::ForceRetire { generation } => {
                st.native_resident.force_retire(&generation)?
            },
        }
        // Process convergence can lag publication. Staging CAS and uncertain
        // publication resolution use this transaction's durable selection.
        let committed = committed_selection(&st.application).await?;
        anyhow::Ok(axum::Json(serde_json::json!({
            "lifecycleProtocol": 1,
            "enabled": st.native_resident.enabled(),
            "committedSelection": committed.descriptor,
            "committedSelectionTs": committed.version.to_string(),
            "status": st.native_resident.status(),
        })))
    }
    .await
    .map_err(Into::into)
}
