use async_trait::async_trait;
use common::log_lines::LogLine;
use errors::ErrorMetadata;
use model::source_packages::types::NodeExecutorPoolTopology;
use tokio::sync::mpsc;

use crate::executor::{
    ExecutorRequest,
    InvokeResponse,
    NodeExecutor,
};
pub struct NoopNodeExecutor {}

impl NoopNodeExecutor {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl NodeExecutor for NoopNodeExecutor {
    fn enable(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn validate_pool_topology(&self, topology: &NodeExecutorPoolTopology) -> anyhow::Result<()> {
        if !topology.is_empty() {
            anyhow::bail!(ErrorMetadata::bad_request(
                "NodeExecutorPoolsNotSupported",
                "This runtime does not support dedicated Node executor pools",
            ));
        }
        Ok(())
    }

    fn reconcile_pool_topology(
        &self,
        topology: &NodeExecutorPoolTopology,
        _version: common::types::Timestamp,
    ) -> anyhow::Result<()> {
        self.validate_pool_topology(topology)
    }

    async fn invoke(
        &self,
        _request: ExecutorRequest,
        _log_line_sender: mpsc::UnboundedSender<LogLine>,
    ) -> anyhow::Result<InvokeResponse> {
        anyhow::bail!("NoopNodeExecutor cannot be used to invoke code.");
    }

    fn shutdown(&self) {}
}
