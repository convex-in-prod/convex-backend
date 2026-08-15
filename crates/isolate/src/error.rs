use common::{
    errors::{
        lookup_source_map_token,
        FrameData,
        JsError,
    },
    runtime::Runtime,
};
use deno_core::{
    v8,
    ModuleSpecifier,
};
use errors::ErrorMetadataAnyhowExt;
use sourcemap::SourceMap;
use url::Url;
use value::ConvexValue;

use crate::{
    environment::V8IsolateEnvironment,
    execution_scope::ExecutionScope,
    helpers::{
        deserialize_udf_custom_error,
        format_uncaught_error,
        get_property,
        source_map_from_slice,
        to_rust_string,
    },
    is_instance_of_error::is_instance_of_error,
    metrics,
};

impl<RT: Runtime, E: V8IsolateEnvironment<RT>> ExecutionScope<'_, '_, '_, RT, E> {
    pub fn format_traceback(&mut self, exception: v8::Local<v8::Value>) -> anyhow::Result<JsError> {
        // Check if we hit a system error or timeout and can't run any JavaScript now.
        // Abort with a system error here, and we'll (in the best case) pull out
        // the original system error stashed on the `ContextState`.
        self.handle().check_terminated()?;
        let error = match self.extract_source_mapped_error(exception) {
            Ok(err) => err,
            Err(e) => {
                let message = v8::Exception::create_message(self, exception);
                let message = message.get(self);
                let message = to_rust_string(self, &message)?;
                metrics::log_source_map_failure(&message, &e);
                JsError::from_message(message)
            },
        };
        Ok(error)
    }

    fn extract_source_mapped_error(
        &mut self,
        exception: v8::Local<v8::Value>,
    ) -> anyhow::Result<JsError> {
        let (message, frame_data, custom_data) = extract_source_mapped_error(self, exception)?;
        Ok(JsError::from_frames(
            message,
            frame_data,
            custom_data,
            |s| self.lookup_source_map(s),
        ))
    }

    pub fn lookup_source_map(
        &mut self,
        specifier: &ModuleSpecifier,
    ) -> anyhow::Result<Option<SourceMap>> {
        let module_map = self.module_map();
        let Some(module_id) = module_map.get_by_name(specifier) else {
            return Ok(None);
        };
        let Some(source_map) = module_map.source_map(module_id) else {
            return Ok(None);
        };
        Ok(source_map_from_slice(source_map.as_bytes()))
    }

    pub fn nicely_show_line_number_on_error(
        &mut self,
        name: &ModuleSpecifier,
        location: v8::Location,
        e: anyhow::Error,
    ) -> anyhow::Result<!> {
        let source_map = self.lookup_source_map(name)?;
        let orig_line = location.get_line_number();
        let orig_col = location.get_column_number();
        Err(e.wrap_error_message(|m| {
            format_error_source_location(name, orig_line, orig_col, source_map.as_ref(), &m)
        }))
    }
}

fn format_error_source_location(
    name: &ModuleSpecifier,
    generated_line: i32,
    generated_column: i32,
    source_map: Option<&SourceMap>,
    message: &str,
) -> String {
    // Minified lines and source-map columns are untrusted. Bound both the copied
    // excerpt and its marker before allocating diagnostic output.
    const MAX_SOURCE_EXCERPT_BYTES: usize = 4096;
    source_map
        .and_then(|source_map| {
            let token = lookup_source_map_token(
                source_map,
                generated_line.try_into().ok()?,
                generated_column.try_into().ok()?,
            )?;
            token.get_source()?;
            let (line, column) = token.get_src();
            let ctx = token.get_source_view()?.get_line(line)?;
            if ctx.len() > MAX_SOURCE_EXCERPT_BYTES || column as usize > ctx.len() {
                return None;
            }

            // Source-map columns count UTF-16 code units, not UTF-8 bytes. Only
            // accept a character boundary, including the end of the source line.
            let mut utf16_column = 0;
            let mut byte_column = 0;
            let mut padding = 0;
            for ch in ctx.chars() {
                if utf16_column >= column {
                    break;
                }
                utf16_column += ch.len_utf16() as u32;
                byte_column += ch.len_utf8();
                padding += 1;
            }
            if utf16_column != column {
                return None;
            }
            let underline = ctx.get(byte_column..)?.chars().count().max(1);
            Some(format!(
                "{name}:{line}:{column}: {message}\n\n{ctx}\n{}{}",
                " ".repeat(padding),
                "~".repeat(underline),
            ))
        })
        .unwrap_or_else(|| format!("{name}:{generated_line}:{generated_column}: {message}"))
}

/// The source-mapped frames of a stack trace, rendered the way
/// `JsError::to_string` renders them. Both runtimes answer the `error/stack`
/// op through this, so their stacks stay formatted alike.
pub fn source_mapped_stack(
    frame_data: Vec<FrameData>,
    lookup_source_map: impl FnMut(&Url) -> anyhow::Result<Option<SourceMap>>,
) -> String {
    let js_error = JsError::from_frames(String::new(), frame_data, None, lookup_source_map);
    js_error
        .frames
        .expect("JsError::from_frames has frames=None")
        .to_string()
}

pub fn extract_source_mapped_error(
    scope: &v8::PinScope<'_, '_>,
    exception: v8::Local<'_, v8::Value>,
) -> anyhow::Result<(String, Vec<FrameData>, Option<ConvexValue>)> {
    if !is_instance_of_error(scope, exception) {
        anyhow::bail!("Exception wasn't an instance of `Error`");
    }
    let exception_obj: v8::Local<v8::Object> = exception.try_into()?;

    // Get the message by formatting error.name and error.message.
    let name = get_property(scope, exception_obj, "name")?
        .filter(|v| !v.is_undefined())
        .and_then(|m| m.to_string(scope))
        .map(|s| s.to_rust_string_lossy(scope))
        .unwrap_or_else(|| "Error".to_string());
    let message_prop = get_property(scope, exception_obj, "message")?
        .filter(|v| !v.is_undefined())
        .and_then(|m| m.to_string(scope))
        .map(|s| s.to_rust_string_lossy(scope))
        .unwrap_or_else(|| "".to_string());
    let message = format_uncaught_error(message_prop, name);

    // Access the `stack` property to ensure `prepareStackTrace` has been called.
    // NOTE if this is the first time accessing `stack`, it will call the op
    // `error/stack` which does a redundant source map lookup.
    let stack: v8::Local<v8::String> = get_property(scope, exception_obj, "stack")?
        .ok_or_else(|| anyhow::anyhow!("Exception was missing the `stack` property"))?
        .try_into()?;

    let frame_data = get_property(scope, exception_obj, "__frameData")?
        .ok_or_else(|| anyhow::anyhow!("Exception was missing the `__frameData` property"))?;

    // Sometimes the frame_data is undefined. What's that about?
    if frame_data.is_undefined() {
        anyhow::bail!(
            "Exception frame data was undefined, stack: {:?}",
            to_rust_string(scope, &stack)?
        );
    }

    let frame_data: v8::Local<v8::String> = frame_data.try_into()?;
    let frame_data = to_rust_string(scope, &frame_data)?;
    let frame_data: Vec<FrameData> = serde_json::from_str(&frame_data)?;

    // error[error.ConvexErrorSymbol] === true
    let convex_error_symbol = get_property(scope, exception_obj, "ConvexErrorSymbol")?;
    let is_convex_error = convex_error_symbol.is_some_and(|symbol| {
        exception_obj
            .get(scope, symbol)
            .is_some_and(|v| v.is_true())
    });

    let custom_data = if is_convex_error {
        let custom_data: v8::Local<v8::String> = get_property(scope, exception_obj, "data")?
            .ok_or_else(|| anyhow::anyhow!("The thrown ConvexError is missing `data` property"))?
            .try_into()?;
        Some(to_rust_string(scope, &custom_data)?)
    } else {
        None
    };
    let (message, custom_data) = deserialize_udf_custom_error(message, custom_data)?;
    Ok((message, frame_data, custom_data))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_excerpt_preserves_valid_ranges_and_generated_fallback() -> anyhow::Result<()> {
        let name = ModuleSpecifier::parse("convex:/user/root.js")?;
        let map = SourceMap::from_slice(
            br#"{"version":3,"sources":["root.ts"],"sourcesContent":["hello"],"names":[],"mappings":"oGAAA;KAAA","rangeMappings":"B;B"}"#,
        )?;
        for (line, column) in [(0, 99), (1, 0), (2, 0), (0, 106), (0, i32::MAX)] {
            assert_eq!(
                format_error_source_location(&name, line, column, Some(&map), "invalid import"),
                format!("{name}:{line}:{column}: invalid import"),
            );
        }
        assert_eq!(
            format_error_source_location(&name, 0, 102, Some(&map), "invalid import"),
            format!("{name}:0:2: invalid import\n\nhello\n  ~~~"),
        );
        assert_eq!(
            format_error_source_location(&name, 1, 10, Some(&map), "invalid import"),
            format!("{name}:0:5: invalid import\n\nhello\n     ~"),
        );
        Ok(())
    }

    #[test]
    fn error_excerpt_validates_utf16_columns_and_limits_output() -> anyhow::Result<()> {
        let name = ModuleSpecifier::parse("convex:/user/root.js")?;
        for (content, column, excerpt) in [
            ("é😀x".to_owned(), 3, Some("é😀x\n  ~")),
            ("é😀x".to_owned(), 2, None),
            ("é😀x".to_owned(), 5, None),
            ("x".repeat(4097), 0, None),
        ] {
            let map = SourceMap::from_slice(&serde_json::to_vec(&serde_json::json!({
                "version": 3,
                "sources": ["root.ts"],
                "sourcesContent": [content],
                "names": [],
                "mappings": "oGAAA",
                "rangeMappings": "B",
            }))?)?;
            let expected = match excerpt {
                Some(excerpt) => format!("{name}:0:{column}: invalid import\n\n{excerpt}"),
                None => format!("{name}:0:{}: invalid import", 100 + column),
            };
            assert_eq!(
                format_error_source_location(&name, 0, 100 + column, Some(&map), "invalid import"),
                expected,
            );
        }
        Ok(())
    }
}
