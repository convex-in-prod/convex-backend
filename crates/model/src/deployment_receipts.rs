//! A bounded set of activation receipts, committed in the activation
//! transaction. Receipts establish historical commit, not current deployment or
//! Node readiness.

use common::{
    document::{
        ParseDocument,
        ParsedDocument,
    },
    query::{
        Order,
        Query,
    },
    runtime::Runtime,
    types::{
        Timestamp,
        WriteTimestamp,
    },
};
use database::{
    query::ResolvedQuery,
    SystemMetadataModel,
    Transaction,
};
use errors::ErrorMetadata;
use serde::{
    Deserialize,
    Serialize,
};
use value::{
    codegen_convex_serialization,
    TableName,
    TableNamespace,
};

use crate::{
    SystemIndex,
    SystemTable,
};

const TABLE: TableName = TableName::const_new("_deployment_receipts");
const MAX_RECEIPTS: usize = 16;
const MAX_RESULT_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentReceipt {
    pub operation_id: String,
    pub input_sha256: String,
    pub expires_unix_seconds: i64,
    pub result_json: String,
}

codegen_convex_serialization!(DeploymentReceipt, DeploymentReceipt);

pub struct DeploymentReceiptsTable;
impl SystemTable for DeploymentReceiptsTable {
    type Metadata = DeploymentReceipt;

    const TABLE_NAME: TableName = TABLE;

    fn indexes() -> Vec<SystemIndex<Self>> {
        vec![]
    }
}

pub struct DeploymentReceiptModel<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
}

impl<'a, RT: Runtime> DeploymentReceiptModel<'a, RT> {
    pub fn new(tx: &'a mut Transaction<RT>) -> Self {
        Self { tx }
    }

    async fn records(
        &mut self,
    ) -> anyhow::Result<Vec<(ParsedDocument<DeploymentReceipt>, Timestamp)>> {
        let mut query = ResolvedQuery::new(
            self.tx,
            TableNamespace::Global,
            Query::full_table_scan(TABLE, Order::Asc),
        )?;
        let mut records = Vec::new();
        while let Some((document, ts)) = query.next_with_ts(self.tx, Some(MAX_RECEIPTS + 1)).await?
        {
            anyhow::ensure!(
                records.len() < MAX_RECEIPTS,
                "deployment receipt count exceeded its bound"
            );
            let WriteTimestamp::Committed(ts) = ts else {
                anyhow::bail!("deployment receipt was read after insertion in its transaction");
            };
            records.push((document.parse()?, ts));
        }
        Ok(records)
    }

    pub async fn lookup(
        &mut self,
        operation_id: &str,
        input_sha256: &str,
    ) -> anyhow::Result<Option<(String, Timestamp)>> {
        for (receipt, ts) in self.records().await? {
            if receipt.operation_id == operation_id {
                anyhow::ensure!(
                    receipt.input_sha256 == input_sha256,
                    ErrorMetadata::bad_request(
                        "DeploymentOperationInputMismatch",
                        "Operation ID was already committed with different input"
                    )
                );
                return Ok(Some((receipt.result_json.clone(), ts)));
            }
        }
        Ok(None)
    }

    pub async fn record(&mut self, receipt: DeploymentReceipt) -> anyhow::Result<()> {
        anyhow::ensure!(
            receipt.result_json.len() <= MAX_RESULT_BYTES,
            ErrorMetadata::bad_request(
                "DeploymentReceiptTooLarge",
                "Activation diff exceeds the durable replay limit"
            )
        );
        let now = i64::try_from(self.tx.runtime().unix_timestamp().as_secs())?;
        anyhow::ensure!(
            receipt.expires_unix_seconds > now,
            ErrorMetadata::bad_request(
                "DeploymentOperationExpired",
                "Activation operation expired before commit"
            )
        );
        let records = self.records().await?;
        let mut live = 0;
        for (previous, _) in records {
            anyhow::ensure!(
                previous.operation_id != receipt.operation_id,
                "deployment receipt was not checked before activation"
            );
            if previous.expires_unix_seconds <= now {
                SystemMetadataModel::new_global(self.tx)
                    .delete(previous.id())
                    .await?;
            } else {
                live += 1;
            }
        }
        anyhow::ensure!(
            live < MAX_RECEIPTS,
            ErrorMetadata::overloaded(
                "DeploymentReceiptsFull",
                "Durable deployment replay capacity is full. Retry after a receipt expires."
            )
        );
        SystemMetadataModel::new_global(self.tx)
            .insert(&TABLE, receipt.try_into()?)
            .await?;
        Ok(())
    }
}
