//! A bounded set of activation receipts, committed in the activation
//! transaction. Receipts establish historical commit, not current deployment or
//! Node readiness.

use std::{
    io::{
        self,
        Write,
    },
    sync::LazyLock,
};

use anyhow::Context;
use common::{
    document::{
        ParseDocument,
        ParsedDocument,
        CREATION_TIME_FIELD,
        CREATION_TIME_FIELD_PATH,
        ID_FIELD,
    },
    query::{
        IndexRange,
        IndexRangeExpression,
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
    IndexModel,
    SystemMetadataModel,
    Transaction,
};
use errors::ErrorMetadata;
use flate2::{
    write::ZlibEncoder,
    Compression,
    Decompress,
    FlushDecompress,
    Status,
};
use serde::{
    Deserialize,
    Serialize,
};
use value::{
    codegen_convex_serialization,
    ConvexValue,
    DeveloperDocumentId,
    FieldPath,
    ResolvedDocumentId,
    Size,
    TableName,
    TableNamespace,
};

use crate::{
    SystemIndex,
    SystemTable,
};

const TABLE: TableName = TableName::const_new("_deployment_receipts");
const MAX_RECEIPTS: usize = 16;
// Bound codec work independently of stored bytes. This accommodates JSON
// escaping of the MAX_SIZE audit document plus definition paths, but is not
// an admission guarantee: removed definitions can come from a legacy push.
const MAX_RESULT_BYTES: usize = 8 * value::MAX_SIZE;
const ZLIB_JSON: &str = "zlib-json-v1";
static OPERATION_FIELD: LazyLock<FieldPath> = LazyLock::new(|| "operationId".parse().unwrap());
static BY_OPERATION: LazyLock<SystemIndex<DeploymentReceiptsTable>> = LazyLock::new(|| {
    SystemIndex::new(
        "by_operation",
        [&OPERATION_FIELD, &CREATION_TIME_FIELD_PATH],
    )
    .unwrap()
});

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentReceipt {
    pub operation_id: String,
    pub input_sha256: String,
    pub expires_unix_seconds: i64,
    // Keep a non-JSON marker in the legacy field. Older backends can still
    // prune metadata but must fail replay instead of reporting a false miss.
    result_json: String,
    // Omission means the legacy, uncompressed resultJson representation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_zlib: Option<serde_bytes::ByteBuf>,
}

codegen_convex_serialization!(DeploymentReceipt, DeploymentReceipt);

impl DeploymentReceipt {
    pub fn new(
        operation_id: String,
        input_sha256: String,
        expires_unix_seconds: i64,
        result: &impl Serialize,
    ) -> anyhow::Result<Self> {
        let mut receipt = Self {
            operation_id,
            input_sha256,
            expires_unix_seconds,
            result_json: ZLIB_JSON.to_owned(),
            result_zlib: Some(vec![0; 4].into()),
        };
        // Budget the entire document, including the length header and system
        // fields. The longest document ID keeps this independent of the table
        // number allocated to an existing deployment.
        let envelope = value::serde::to_object(&receipt)?
            .insert(ID_FIELD.into(), DeveloperDocumentId::MAX.into())?
            .insert(CREATION_TIME_FIELD.into(), 1.0.into())?;
        let compressed = LimitedWriter {
            inner: vec![0; 4],
            remaining: value::MAX_SIZE - envelope.size(),
        };
        let mut json = LimitedWriter {
            inner: ZlibEncoder::new(compressed, Compression::fast()),
            remaining: MAX_RESULT_BYTES,
        };
        if let Err(error) = serde_json::to_writer(&mut json, result) {
            return Err(
                if error.io_error_kind() == Some(io::ErrorKind::FileTooLarge) {
                    receipt_too_large().into()
                } else {
                    error.into()
                },
            );
        }
        let result_len = u32::try_from(MAX_RESULT_BYTES - json.remaining)?;
        let mut compressed = json
            .inner
            .finish()
            .map_err(|error| {
                if error.kind() == io::ErrorKind::FileTooLarge {
                    anyhow::Error::from(receipt_too_large())
                } else {
                    error.into()
                }
            })?
            .inner;
        compressed[..4].copy_from_slice(&result_len.to_le_bytes());
        receipt.result_zlib = Some(compressed.into());
        Ok(receipt)
    }

    fn decode_result(&self) -> anyhow::Result<String> {
        let Some(compressed) = &self.result_zlib else {
            anyhow::ensure!(
                self.result_json != ZLIB_JSON,
                "missing deployment receipt payload"
            );
            return Ok(self.result_json.clone());
        };
        anyhow::ensure!(
            self.result_json == ZLIB_JSON,
            "unsupported deployment receipt encoding"
        );
        let (length, compressed) = compressed
            .split_at_checked(4)
            .context("truncated deployment receipt length")?;
        let length = u32::from_le_bytes(length.try_into()?) as usize;
        anyhow::ensure!(
            length <= MAX_RESULT_BYTES,
            "deployment receipt output exceeds its bound"
        );
        // Check the header before allocating, and never allow the decoder to
        // grow this buffer. Checksum, exact length and full consumption are all
        // required: truncated or concatenated streams are not replay evidence.
        let mut json = vec![0; length];
        let mut decoder = Decompress::new(true);
        let status = decoder.decompress(compressed, &mut json, FlushDecompress::Finish)?;
        anyhow::ensure!(
            status == Status::StreamEnd
                && decoder.total_in() == compressed.len() as u64
                && decoder.total_out() == length as u64,
            "invalid deployment receipt compressed result"
        );
        Ok(String::from_utf8(json)?)
    }
}

// Write has no typed size-limit outcome. FileTooLarge is translated to the
// receipt error above; neither the JSON nor compressed output grows past its
// cap.
struct LimitedWriter<W> {
    inner: W,
    remaining: usize,
}

impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::ErrorKind::FileTooLarge.into());
        }
        let written = self.inner.write(bytes)?;
        self.remaining -= written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn receipt_too_large() -> ErrorMetadata {
    ErrorMetadata::bad_request(
        "DeploymentReceiptTooLarge",
        "Activation result exceeds the durable receipt storage limit",
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReceiptMetadata {
    operation_id: String,
    expires_unix_seconds: i64,
}

pub struct DeploymentReceiptsTable;
impl SystemTable for DeploymentReceiptsTable {
    type Metadata = DeploymentReceipt;

    const TABLE_NAME: TableName = TABLE;

    fn indexes() -> Vec<SystemIndex<Self>> {
        vec![BY_OPERATION.clone()]
    }
}

pub struct DeploymentReceiptModel<'a, RT: Runtime> {
    tx: &'a mut Transaction<RT>,
}

impl<'a, RT: Runtime> DeploymentReceiptModel<'a, RT> {
    pub fn new(tx: &'a mut Transaction<RT>) -> Self {
        Self { tx }
    }

    async fn records(&mut self) -> anyhow::Result<Vec<(ResolvedDocumentId, ReceiptMetadata)>> {
        let mut query = ResolvedQuery::new(
            self.tx,
            TableNamespace::Global,
            Query::full_table_scan(TABLE, Order::Asc),
        )?;
        let mut records = Vec::new();
        // Only retain metadata, and fetch one document at a time. Sixteen large
        // results must not become sixteen decoded payloads during reclamation.
        while let Some((document, ts)) = query.next_with_ts(self.tx, Some(1)).await? {
            anyhow::ensure!(
                matches!(ts, WriteTimestamp::Committed(_)),
                "deployment receipt was read after insertion in its transaction"
            );
            anyhow::ensure!(
                records.len() < MAX_RECEIPTS,
                "deployment receipt count exceeded its bound"
            );
            records.push((
                document.id(),
                value::serde::from_object(document.into_value().0)?,
            ));
        }
        Ok(records)
    }

    pub async fn lookup(
        &mut self,
        operation_id: &str,
        input_sha256: &str,
    ) -> anyhow::Result<Option<(String, Timestamp)>> {
        // An existing table's new index backfills after upgrade. Its temporary
        // absence cannot turn a committed receipt into permission to activate.
        let indexed = IndexModel::new(self.tx)
            .enabled_index_metadata(TableNamespace::Global, &BY_OPERATION.name())?
            .is_some();
        let operation_value = ConvexValue::try_from(operation_id)?;
        let query = if indexed {
            Query::index_range(IndexRange {
                index_name: BY_OPERATION.name(),
                range: vec![IndexRangeExpression::Eq(
                    OPERATION_FIELD.clone(),
                    operation_value.clone().into(),
                )],
                order: Order::Asc,
            })
        } else {
            Query::full_table_scan(TABLE, Order::Asc)
        };
        let mut query = ResolvedQuery::new(self.tx, TableNamespace::Global, query)?;
        let mut result = None;
        let mut count = 0;
        while let Some((document, ts)) = query.next_with_ts(self.tx, Some(1)).await? {
            count += 1;
            anyhow::ensure!(
                count <= MAX_RECEIPTS,
                "deployment receipt count exceeded its bound"
            );
            if document.value().get("operationId") != Some(&operation_value) {
                continue;
            }
            anyhow::ensure!(result.is_none(), "duplicate deployment receipt");
            let receipt: ParsedDocument<DeploymentReceipt> = document.parse()?;
            anyhow::ensure!(
                receipt.input_sha256 == input_sha256,
                ErrorMetadata::bad_request(
                    "DeploymentOperationInputMismatch",
                    "Operation ID was already committed with different input"
                )
            );
            let WriteTimestamp::Committed(ts) = ts else {
                anyhow::bail!("deployment receipt was read after insertion in its transaction");
            };
            result = Some((receipt.decode_result()?, ts));
        }
        Ok(result)
    }

    pub async fn record(&mut self, receipt: DeploymentReceipt) -> anyhow::Result<()> {
        let records = self.records().await?;
        let now = i64::try_from(self.tx.runtime().unix_timestamp().as_secs())?;
        let mut expired = None;
        for (id, previous) in &records {
            anyhow::ensure!(
                previous.operation_id != receipt.operation_id,
                "deployment receipt was not checked before activation"
            );
            if expired.is_none() && previous.expires_unix_seconds <= now {
                expired = Some(*id);
            }
        }
        // Deletion retains the previous document in the transaction write set.
        // One reclaimed slot is enough: keep at most one large expired payload
        // there instead of retaining all sixteen until commit.
        if let Some(id) = expired {
            SystemMetadataModel::new_global(self.tx).delete(id).await?;
        }
        // Reclamation may read large documents. Check expiry again after that
        // work; an admitted owner's lease remains held until this attempt ends.
        let now = i64::try_from(self.tx.runtime().unix_timestamp().as_secs())?;
        anyhow::ensure!(
            receipt.expires_unix_seconds > now,
            ErrorMetadata::bad_request(
                "DeploymentOperationExpired",
                "Activation operation expired before commit"
            )
        );
        anyhow::ensure!(
            records.len() - usize::from(expired.is_some()) < MAX_RECEIPTS,
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

#[cfg(test)]
mod tests {
    use rand::{
        Rng,
        SeedableRng,
    };
    use value::ConvexObject;

    use super::*;

    #[test]
    fn codec_preserves_json_and_rejects_damaged_or_unsupported_payloads() -> anyhow::Result<()> {
        let result = serde_json::json!({
            "schema": "Unicode α, \"quotes\", \\ and \u{0000}".repeat(8192),
            "modules": ["first.js", "second.js"],
        });
        let expected = serde_json::to_string(&result)?;
        assert!(expected.len() > 128 * 1024);
        let receipt = DeploymentReceipt::new("operation".into(), "input".into(), 600, &result)?;
        // Round trip the actual Convex document representation, including bytes.
        let receipt = DeploymentReceipt::try_from(ConvexObject::try_from(receipt)?)?;
        assert_eq!(receipt.decode_result()?, expected);
        let encoded = receipt.result_zlib.as_ref().unwrap().to_vec();
        let mut damaged = Vec::new();
        damaged.push(encoded[..3].to_vec());
        damaged.push(encoded[..encoded.len() - 1].to_vec());
        let mut checksum = encoded.clone();
        *checksum.last_mut().unwrap() ^= 1;
        damaged.push(checksum);
        let mut trailing = encoded.clone();
        trailing.push(0);
        damaged.push(trailing);
        let mut concatenated = encoded.clone();
        concatenated.extend_from_slice(&encoded[4..]);
        damaged.push(concatenated);
        for length in [expected.len() - 1, expected.len() + 1, MAX_RESULT_BYTES + 1] {
            let mut changed = encoded.clone();
            changed[..4].copy_from_slice(&u32::try_from(length)?.to_le_bytes());
            damaged.push(changed);
        }
        // A valid zlib stream is still not a valid UTF-8 result.
        let mut invalid_utf8 = ZlibEncoder::new(vec![1, 0, 0, 0], Compression::fast());
        invalid_utf8.write_all(&[0xff])?;
        damaged.push(invalid_utf8.finish()?);
        for bytes in damaged {
            let mut invalid = receipt.clone();
            invalid.result_zlib = Some(bytes.into());
            assert!(invalid.decode_result().is_err());
        }
        let mut missing = receipt.clone();
        missing.result_zlib = None;
        assert!(missing.decode_result().is_err());
        let mut unsupported = receipt.clone();
        unsupported.result_json = "future-encoding".into();
        assert!(unsupported.decode_result().is_err());

        let mut legacy = receipt.clone();
        legacy.result_json = expected.clone();
        legacy.result_zlib = None;
        let legacy = DeploymentReceipt::try_from(ConvexObject::try_from(legacy)?)?;
        assert_eq!(legacy.decode_result()?, expected);
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct LegacyReceipt {
            result_json: String,
            expires_unix_seconds: i64,
        }
        let old_reader: LegacyReceipt = value::serde::from_object(receipt.try_into()?)?;
        assert!(serde_json::from_str::<serde_json::Value>(&old_reader.result_json).is_err());
        assert_eq!(old_reader.expires_unix_seconds, 600);
        Ok(())
    }

    #[test]
    fn maximum_json_result_round_trips_and_larger_output_is_rejected() -> anyhow::Result<()> {
        use errors::ErrorMetadataAnyhowExt;
        let mut result = "x".repeat(MAX_RESULT_BYTES - 2);
        let expected = serde_json::to_string(&result)?;
        assert_eq!(expected.len(), MAX_RESULT_BYTES);
        let receipt = DeploymentReceipt::new("operation".into(), "input".into(), 600, &result)?;
        let object = ConvexObject::try_from(receipt)?;
        let persisted = value::json_deserialize_bytes(object.json_serialize()?.as_bytes())?;
        let receipt = DeploymentReceipt::try_from(persisted)?;
        assert_eq!(receipt.decode_result()?, expected);
        drop(expected);
        result.push('x');
        let error =
            DeploymentReceipt::new("operation".into(), "input".into(), 600, &result).unwrap_err();
        assert_eq!(error.short_msg(), "DeploymentReceiptTooLarge");
        Ok(())
    }

    #[test]
    fn poorly_compressible_results_cannot_exceed_system_document_storage() -> anyhow::Result<()> {
        use common::document::{
            CreationTime,
            ResolvedDocument,
        };
        use errors::ErrorMetadataAnyhowExt;
        // Printable random text is valid JSON input. Its entropy defeats the
        // schema compression case and must still respect the stored-byte cap.
        let mut rng = rand::rngs::StdRng::seed_from_u64(123);
        let mut text: String = (0..48 * 1024 * 1024)
            .map(|_| char::from(rng.random_range(b' '..=b'~')))
            .collect();
        let error =
            DeploymentReceipt::new("operation".into(), "input".into(), 600, &text).unwrap_err();
        assert_eq!(error.short_msg(), "DeploymentReceiptTooLarge");
        // Calibrate this fixture to the actual codec rather than assuming a
        // compression ratio for printable random text.
        let mut encoded = ZlibEncoder::new(Vec::new(), Compression::fast());
        serde_json::to_writer(&mut encoded, &text)?;
        let encoded_len = encoded.finish()?.len();
        text.truncate(text.len() * (value::MAX_SIZE * 95 / 100) / encoded_len);
        let receipt = DeploymentReceipt::new("operation".into(), "input".into(), 600, &text)?;
        let document = ResolvedDocument::new(
            ResolvedDocumentId::new(value::TabletId::MAX, DeveloperDocumentId::MAX),
            CreationTime::try_from(1.0)?,
            receipt.try_into()?,
        )?;
        // Exercise an admitted document near the stored-byte cap, including
        // system fields and persistence's larger JSON/base64 representation.
        assert!(document.size() > value::MAX_SIZE * 9 / 10);
        let persisted =
            value::json_deserialize_bytes(document.value().json_serialize()?.as_bytes())?;
        let receipt = DeploymentReceipt::try_from(persisted)?;
        assert_eq!(receipt.decode_result()?, serde_json::to_string(&text)?);
        Ok(())
    }
}
