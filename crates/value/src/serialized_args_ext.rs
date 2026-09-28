use anyhow::Context;
use errors::ErrorMetadata;
use serde_json::Value as JsonValue;
use sync_types::types::SerializedArgs;

pub trait SerializedArgsExt {
    /// Parse the arguments while retaining their original serialized form.
    ///
    /// Database UDF outcomes retain `SerializedArgs`, while warning generation
    /// needs decoded values. Borrowing here avoids copying the complete raw
    /// argument payload before that conversion.
    fn as_args(&self) -> anyhow::Result<Vec<JsonValue>>;

    fn into_args(self) -> anyhow::Result<Vec<JsonValue>>;
}

impl SerializedArgsExt for SerializedArgs {
    fn as_args(&self) -> anyhow::Result<Vec<JsonValue>> {
        serde_json::from_str(self.get()).context(ErrorMetadata::bad_request(
            "InvalidArguments",
            "Invalid arguments provided",
        ))
    }

    fn into_args(self) -> anyhow::Result<Vec<JsonValue>> {
        self.as_args()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sync_types::types::SerializedArgs;

    use super::SerializedArgsExt;

    #[test]
    fn as_args_preserves_serialized_arguments() -> anyhow::Result<()> {
        let args = SerializedArgs::from_slice(br#"[{"value":"retained"}]"#)?;

        assert_eq!(args.as_args()?, vec![json!({ "value": "retained" })]);
        assert_eq!(args.get(), r#"[{"value":"retained"}]"#);
        Ok(())
    }
}
