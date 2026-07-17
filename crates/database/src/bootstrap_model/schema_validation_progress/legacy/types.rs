use serde::{
    Deserialize,
    Serialize,
};
use value::{
    codegen_convex_serialization,
    DeveloperDocumentId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
/// Legacy aggregate progress is written while a schema is `Pending` and may
/// remain while it is `Validated`. A terminal or pruned schema can retain a
/// row until the schema worker performs bounded cleanup.
pub struct SchemaValidationProgressMetadata {
    /// The ID of the schema being validated. Should correspond to a document in
    /// the `_schemas` table. It normally identifies a `Pending` or `Validated`
    /// schema, but can identify a terminal or pruned schema until cleanup.
    pub schema_id: DeveloperDocumentId,
    /// The number of documents that have been validated so far.
    pub num_docs_validated: u64,
    /// The number of total documents that need to be validated. Note this is
    /// approximate because there could be changes since the time we wrote this
    /// value from the table summary when the schema is submitted as pending.
    /// It's possible for num_docs_validated to exceed total_docs.
    /// This field is None if there is no table summary available.
    pub total_docs: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedSchemaValidationProgressMetadata {
    pub schema_id: String,
    pub num_docs_validated: i64,
    pub total_docs: Option<i64>,
}

impl TryFrom<SchemaValidationProgressMetadata> for SerializedSchemaValidationProgressMetadata {
    type Error = anyhow::Error;

    fn try_from(metadata: SchemaValidationProgressMetadata) -> anyhow::Result<Self> {
        Ok(SerializedSchemaValidationProgressMetadata {
            schema_id: metadata.schema_id.to_string(),
            num_docs_validated: metadata.num_docs_validated.try_into()?,
            total_docs: metadata.total_docs.map(|x| x.try_into()).transpose()?,
        })
    }
}

impl TryFrom<SerializedSchemaValidationProgressMetadata> for SchemaValidationProgressMetadata {
    type Error = anyhow::Error;

    fn try_from(serialized: SerializedSchemaValidationProgressMetadata) -> anyhow::Result<Self> {
        Ok(SchemaValidationProgressMetadata {
            schema_id: serialized.schema_id.parse()?,
            num_docs_validated: serialized.num_docs_validated.try_into()?,
            total_docs: serialized.total_docs.map(|x| x.try_into()).transpose()?,
        })
    }
}

codegen_convex_serialization!(
    SchemaValidationProgressMetadata,
    SerializedSchemaValidationProgressMetadata
);
