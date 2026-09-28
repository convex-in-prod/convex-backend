use anyhow::Context;
use pb::outcome::{
    host_operation_error,
    nonexistent_document,
    HostOperationError as HostOperationErrorProto,
    NonexistentDocument as NonexistentDocumentProto,
};
use value::DeveloperDocumentId;

const HOST_OPERATION_ERROR_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostOperation {
    Patch,
    Replace,
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostOperationErrorV1 {
    NonexistentDocument {
        operation: HostOperation,
        document_id: DeveloperDocumentId,
    },
}

impl TryFrom<HostOperationErrorV1> for HostOperationErrorProto {
    type Error = anyhow::Error;

    fn try_from(error: HostOperationErrorV1) -> anyhow::Result<Self> {
        let HostOperationErrorV1::NonexistentDocument {
            operation,
            document_id,
        } = error;
        let operation = match operation {
            HostOperation::Patch => nonexistent_document::Operation::Patch,
            HostOperation::Replace => nonexistent_document::Operation::Replace,
            HostOperation::Delete => nonexistent_document::Operation::Delete,
        };
        Ok(Self {
            schema_version: HOST_OPERATION_ERROR_SCHEMA_VERSION,
            error: Some(host_operation_error::Error::NonexistentDocument(
                NonexistentDocumentProto {
                    operation: operation as i32,
                    document_id: Some(document_id.into()),
                },
            )),
        })
    }
}

impl TryFrom<HostOperationErrorProto> for HostOperationErrorV1 {
    type Error = anyhow::Error;

    fn try_from(error: HostOperationErrorProto) -> anyhow::Result<Self> {
        anyhow::ensure!(
            error.schema_version == HOST_OPERATION_ERROR_SCHEMA_VERSION,
            "Unsupported host operation error schema version"
        );
        let host_operation_error::Error::NonexistentDocument(error) =
            error.error.context("Missing host operation error kind")?;
        let operation = nonexistent_document::Operation::try_from(error.operation)
            .context("Invalid nonexistent-document host operation")?;
        let operation = match operation {
            nonexistent_document::Operation::Patch => HostOperation::Patch,
            nonexistent_document::Operation::Replace => HostOperation::Replace,
            nonexistent_document::Operation::Delete => HostOperation::Delete,
            nonexistent_document::Operation::Unspecified => {
                anyhow::bail!("Unspecified nonexistent-document host operation")
            },
        };
        Ok(Self::NonexistentDocument {
            operation,
            document_id: error
                .document_id
                .context("Missing nonexistent-document host operation ID")?
                .try_into()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn host_operation_error_proto_round_trip_preserves_typed_metadata() {
        let expected = HostOperationErrorV1::NonexistentDocument {
            operation: HostOperation::Replace,
            document_id: DeveloperDocumentId::MAX,
        };
        let encoded = HostOperationErrorProto::try_from(expected)
            .unwrap()
            .encode_to_vec();
        let decoded = HostOperationErrorProto::decode(encoded.as_slice()).unwrap();

        assert_eq!(HostOperationErrorV1::try_from(decoded).unwrap(), expected);
    }

    #[test]
    fn host_operation_error_proto_rejects_unknown_schema() {
        let error = HostOperationErrorV1::try_from(HostOperationErrorProto {
            schema_version: 2,
            error: None,
        });

        assert!(error.is_err());
    }
}
