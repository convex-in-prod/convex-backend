use std::{
    collections::BTreeMap,
    ops::Deref,
    str::FromStr,
    sync::Arc,
};

use anyhow::Context;
use async_lru::async_lru::SizedValue;
use common::{
    http::RoutedHttpPath,
    json::JsonForm as _,
    types::{
        HttpActionRoute,
        RoutableMethod,
        UdfType,
    },
};
use errors::ErrorMetadata;
use packed_value::StringBuffer;
use serde::{
    Deserialize,
    Serialize,
};
use sync_types::{
    CanonicalizedModulePath,
    FunctionName,
};
use value::heap_size::{
    HeapSize,
    WithHeapSize,
};

use super::function_validators::{
    ArgsValidator,
    ReturnsValidator,
};
use crate::cron_jobs::types::{
    CronIdentifier,
    CronSpec,
    SerializedCronSpec,
};

/// User-specified JavaScript source code for a module.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ModuleSource {
    source: Arc<str>,
}

impl ModuleSource {
    pub fn new(source: &str) -> Self {
        Self {
            source: source.into(),
        }
    }
}

impl Deref for ModuleSource {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

impl HeapSize for ModuleSource {
    fn heap_size(&self) -> usize {
        self.source.len()
    }
}

impl From<&str> for ModuleSource {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Bundler-generated source map for a `ModuleSource`.
pub type SourceMap = Arc<str>;

#[derive(Debug, Clone)]
pub struct FullModuleSource {
    pub source: ModuleSource,
    pub source_map: Option<SourceMap>,
}

impl SizedValue for FullModuleSource {
    fn size(&self) -> u64 {
        (self.source.heap_size() + self.source_map.heap_size()) as u64
    }
}

/// Per-module permissions for retaining an initialized V8 context between
/// executions. The policy is deliberately separate from the cache: the same
/// cache can hold contexts for several execution kinds, but it must never
/// infer permission from cache presence or from a request's wire type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextReusePolicy {
    /// Allow query executions to retain their initialized context.
    #[serde(default)]
    pub queries: bool,
    /// Allow mutation executions to retain their initialized context.
    #[serde(default)]
    pub mutations: bool,
    /// Allow ordinary Convex-runtime action executions to retain their
    /// initialized context.
    #[serde(default)]
    pub actions: bool,
    /// Allow HTTP action executions to retain their initialized context.
    #[serde(default)]
    pub http_actions: bool,
}

impl ContextReusePolicy {
    pub const fn database() -> Self {
        Self {
            queries: true,
            mutations: true,
            actions: false,
            http_actions: false,
        }
    }

    pub const fn is_empty(&self) -> bool {
        !self.queries && !self.mutations && !self.actions && !self.http_actions
    }

    /// Project this policy onto the legacy boolean without granting either
    /// database kind more authority than the typed policy.
    pub const fn allows_legacy_database_reuse(&self) -> bool {
        self.queries && self.mutations
    }

    pub const fn allows(self, udf_type: UdfType) -> bool {
        match udf_type {
            UdfType::Query => self.queries,
            UdfType::Mutation => self.mutations,
            UdfType::Action => self.actions,
            UdfType::HttpAction => self.http_actions,
        }
    }
}

pub fn parse_context_initialization_module(path: &str) -> anyhow::Result<CanonicalizedModulePath> {
    let invalid_path = ErrorMetadata::bad_request(
        "InvalidContextInitializationModule",
        "Context initialization module must be a canonical _deps module path",
    );
    let module: CanonicalizedModulePath = path.parse().context(invalid_path.clone())?;
    // ModulePath preserves redundant separators and `.` segments, and its
    // dependency predicate also accepts the legacy actions/_deps directory.
    // This cache identity must name the canonical root-level dependency path.
    anyhow::ensure!(
        path.starts_with("_deps/")
            && module.as_str() == path
            && path.split('/').all(|part| !matches!(part, "" | ".")),
        invalid_path
    );
    Ok(module)
}

/// A legacy root applies to database UDFs and ordinary actions. Version 2
/// selects a root for each context class; a missing root means per-entry reuse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "SerializedContextInitializationModule",
    into = "SerializedContextInitializationModule"
)]
pub enum ContextInitializationModule {
    Legacy(CanonicalizedModulePath),
    Separate {
        database: Option<CanonicalizedModulePath>,
        action: Option<CanonicalizedModulePath>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum SerializedContextInitializationModule {
    Legacy(String),
    Separate(SerializedSeparateContextInitializationModules),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SerializedSeparateContextInitializationModules {
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    action: Option<String>,
}

impl TryFrom<SerializedContextInitializationModule> for ContextInitializationModule {
    type Error = anyhow::Error;

    fn try_from(value: SerializedContextInitializationModule) -> anyhow::Result<Self> {
        match value {
            SerializedContextInitializationModule::Legacy(path) => {
                Ok(Self::Legacy(parse_context_initialization_module(&path)?))
            },
            SerializedContextInitializationModule::Separate(value) => Self::separate(
                value.version,
                value.database.as_deref(),
                value.action.as_deref(),
            ),
        }
    }
}

impl From<ContextInitializationModule> for SerializedContextInitializationModule {
    fn from(value: ContextInitializationModule) -> Self {
        match value {
            ContextInitializationModule::Legacy(path) => Self::Legacy(path.as_str().to_owned()),
            ContextInitializationModule::Separate { database, action } => {
                Self::Separate(SerializedSeparateContextInitializationModules {
                    version: 2,
                    database: database.map(|path| path.as_str().to_owned()),
                    action: action.map(|path| path.as_str().to_owned()),
                })
            },
        }
    }
}

impl ContextInitializationModule {
    pub fn separate(
        version: u32,
        database: Option<&str>,
        action: Option<&str>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            version == 2,
            ErrorMetadata::bad_request(
                "InvalidContextInitializationModule",
                "Unsupported context initialization module version",
            )
        );
        let database = database
            .map(parse_context_initialization_module)
            .transpose()?;
        let action = action
            .map(parse_context_initialization_module)
            .transpose()?;
        anyhow::ensure!(
            database.is_some() || action.is_some(),
            ErrorMetadata::bad_request(
                "InvalidContextInitializationModule",
                "Version 2 context initialization requires at least one module",
            )
        );
        Ok(Self::Separate { database, action })
    }

    pub fn validate(&self, policy: ContextReusePolicy) -> anyhow::Result<()> {
        // Legacy roots were allowed independently of permissions. Preserve that
        // behavior while rejecting contradictory new metadata at ingress.
        if let Self::Separate { database, action } = self {
            anyhow::ensure!(
                (database.is_some() || action.is_some())
                    && (database.is_none() || policy.queries || policy.mutations)
                    && (action.is_none() || policy.actions),
                ErrorMetadata::bad_request(
                    "InvalidContextInitializationModule",
                    "Context initialization modules require their execution-kind reuse permission",
                )
            );
        }
        Ok(())
    }

    pub fn for_type(&self, udf_type: UdfType) -> Option<&CanonicalizedModulePath> {
        match (self, udf_type) {
            (_, UdfType::HttpAction) => None,
            (Self::Legacy(path), UdfType::Query | UdfType::Mutation | UdfType::Action) => {
                Some(path)
            },
            (Self::Separate { database, .. }, UdfType::Query | UdfType::Mutation) => {
                database.as_ref()
            },
            (Self::Separate { action, .. }, UdfType::Action) => action.as_ref(),
        }
    }
}

impl HeapSize for ContextInitializationModule {
    fn heap_size(&self) -> usize {
        match self {
            Self::Legacy(path) => path.as_str().len(),
            Self::Separate { database, action } => {
                database.as_ref().map_or(0, |path| path.as_str().len())
                    + action.as_ref().map_or(0, |path| path.as_str().len())
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AnalyzedModule {
    pub functions: WithHeapSize<Vec<AnalyzedFunction>>,
    pub http_routes: Option<AnalyzedHttpRoutes>,
    pub cron_specs: Option<WithHeapSize<BTreeMap<CronIdentifier, CronSpec>>>,
    /// Index of the module's original source in the source map.
    pub source_index: Option<u32>,
    /// Whether this module requests experimental JS context reuse for each
    /// supported execution kind. When reuse occurs, state can
    /// non-deterministically leak between executions (e.g. on the global
    /// object or module attributes).
    pub context_reuse: ContextReusePolicy,
    /// Import roots for grouped database and ordinary-action contexts.
    /// Omission keeps per-entry reuse; roots do not grant execution-kind
    /// permission.
    pub context_initialization_module: Option<ContextInitializationModule>,
}

impl HeapSize for AnalyzedModule {
    fn heap_size(&self) -> usize {
        self.functions.heap_size()
            + self.http_routes.heap_size()
            + self.cron_specs.heap_size()
            + self.source_index.heap_size()
            + self.context_initialization_module.heap_size()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedAnalyzedModule {
    functions: Vec<SerializedAnalyzedFunction>,
    http_routes: Option<Vec<SerializedAnalyzedHttpRoute>>,
    cron_specs: Option<Vec<SerializedNamedCronSpec>>,
    source_mapped: Option<SerializedMappedModule>,
    // Keep this optional so an explicitly supplied all-false policy is not
    // confused with an omitted typed field during rolling compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_reuse: Option<ContextReusePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_initialization_module: Option<ContextInitializationModule>,
    // Keep the old field for rolling protocol compatibility. Older backends
    // ignore `contextReuse`, while newer backends decode this field as the
    // legacy database policy when the typed field is absent.
    #[serde(default)]
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    reuse_context: bool,
}

impl TryFrom<AnalyzedModule> for SerializedAnalyzedModule {
    type Error = anyhow::Error;

    fn try_from(m: AnalyzedModule) -> anyhow::Result<Self> {
        if let Some(initializer) = &m.context_initialization_module {
            initializer.validate(m.context_reuse)?;
        }
        let source_mapped = m
            .source_index
            .as_ref()
            .map(|_source_mapped| SerializedMappedModule::try_from(m.clone()))
            .transpose()?;
        Ok(Self {
            functions: m
                .functions
                .into_iter()
                .map(TryFrom::try_from)
                .try_collect()?,
            http_routes: m
                .http_routes
                .map(|routes| routes.into_iter().map(TryFrom::try_from).try_collect())
                .transpose()?,
            cron_specs: m
                .cron_specs
                .map(|specs| specs.into_iter().map(TryFrom::try_from).try_collect())
                .transpose()?,
            source_mapped,
            context_reuse: Some(m.context_reuse),
            context_initialization_module: m.context_initialization_module,
            reuse_context: m.context_reuse.allows_legacy_database_reuse(),
        })
    }
}

impl TryFrom<SerializedAnalyzedModule> for AnalyzedModule {
    type Error = anyhow::Error;

    fn try_from(m: SerializedAnalyzedModule) -> anyhow::Result<Self> {
        let context_reuse = m.context_reuse.unwrap_or_else(|| {
            if m.reuse_context {
                ContextReusePolicy::database()
            } else {
                ContextReusePolicy::default()
            }
        });
        if let Some(initializer) = &m.context_initialization_module {
            initializer.validate(context_reuse)?;
        }
        Ok(Self {
            functions: m
                .functions
                .into_iter()
                .map(TryFrom::try_from)
                .try_collect()?,
            http_routes: m
                .http_routes
                .map(|routes| {
                    let routes = routes.into_iter().map(TryFrom::try_from).try_collect()?;
                    anyhow::Ok(AnalyzedHttpRoutes::new(routes))
                })
                .transpose()?,
            cron_specs: m
                .cron_specs
                .map(|specs| specs.into_iter().map(TryFrom::try_from).try_collect())
                .transpose()?,
            source_index: m
                .source_mapped
                .and_then(|mapped_module| mapped_module.source_index),
            context_reuse,
            context_initialization_module: m.context_initialization_module,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerializedNamedCronSpec {
    identifier: String,
    spec: SerializedCronSpec,
}

impl TryFrom<(CronIdentifier, CronSpec)> for SerializedNamedCronSpec {
    type Error = anyhow::Error;

    fn try_from((identifier, spec): (CronIdentifier, CronSpec)) -> anyhow::Result<Self> {
        Ok(Self {
            identifier: identifier.to_string(),
            spec: SerializedCronSpec::try_from(spec)?,
        })
    }
}

impl TryFrom<SerializedNamedCronSpec> for (CronIdentifier, CronSpec) {
    type Error = anyhow::Error;

    fn try_from(s: SerializedNamedCronSpec) -> anyhow::Result<Self> {
        Ok((s.identifier.parse()?, CronSpec::try_from(s.spec)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum Visibility {
    Public,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct AnalyzedSourcePosition {
    pub path: CanonicalizedModulePath,
    pub start_lineno: u32,
    pub start_col: u32,
    // Consider adding end_* in the future
}

impl HeapSize for AnalyzedSourcePosition {
    fn heap_size(&self) -> usize {
        self.path.as_str().heap_size() + self.start_col.heap_size() + self.start_lineno.heap_size()
    }
}

#[derive(Serialize, Deserialize)]
// NOTE: serde not renamed to camelCase.
struct SerializedAnalyzedSourcePosition {
    path: String,
    start_lineno: u32,
    start_col: u32,
}

impl TryFrom<AnalyzedSourcePosition> for SerializedAnalyzedSourcePosition {
    type Error = anyhow::Error;

    fn try_from(p: AnalyzedSourcePosition) -> anyhow::Result<Self> {
        Ok(Self {
            path: p.path.as_str().to_string(),
            start_lineno: p.start_lineno,
            start_col: p.start_col,
        })
    }
}

impl TryFrom<SerializedAnalyzedSourcePosition> for AnalyzedSourcePosition {
    type Error = anyhow::Error;

    fn try_from(p: SerializedAnalyzedSourcePosition) -> anyhow::Result<Self> {
        Ok(Self {
            path: p.path.parse()?,
            start_lineno: p.start_lineno,
            start_col: p.start_col,
        })
    }
}

pub fn invalid_function_name_error(
    path: &CanonicalizedModulePath,
    e: &anyhow::Error,
) -> ErrorMetadata {
    ErrorMetadata::bad_request(
        "InvalidFunctionName",
        format!("Invalid function name used in `{}`: {}", path.as_str(), e),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzedFunction {
    pub name: FunctionName,
    pub pos: Option<AnalyzedSourcePosition>,
    pub udf_type: UdfType,
    pub visibility: Option<Visibility>,

    // Leave args and returns unparsed to avoid performance overhead in common
    // case of reading ModuleMetadata without needing to validate the function.

    // JSON-serialized ArgsValidator
    // Note that we use `StringBuffer` so that this can be a refcounted slice
    // into the `PackedDocument` stored in the `ModulesTable` memory index
    pub args_str: Option<StringBuffer>,
    // JSON-serialized ReturnsValidator
    pub returns_str: Option<StringBuffer>,
}

impl AnalyzedFunction {
    pub fn new(
        name: FunctionName,
        pos: Option<AnalyzedSourcePosition>,
        udf_type: UdfType,
        visibility: Option<Visibility>,
        args: ArgsValidator,
        returns: ReturnsValidator,
    ) -> anyhow::Result<Self> {
        let args_json = args.json_serialize()?;
        let returns_json = returns.json_serialize()?;
        Ok(Self {
            name,
            pos,
            udf_type,
            visibility,
            args_str: Some(StringBuffer::new(args_json)),
            returns_str: Some(StringBuffer::new(returns_json)),
        })
    }

    pub fn args(&self) -> anyhow::Result<ArgsValidator> {
        match &self.args_str {
            Some(args) => ArgsValidator::json_deserialize(args),
            None => Ok(ArgsValidator::Unvalidated),
        }
    }

    pub fn returns(&self) -> anyhow::Result<ReturnsValidator> {
        match &self.returns_str {
            Some(returns) => ReturnsValidator::json_deserialize(returns),
            None => Ok(ReturnsValidator::Unvalidated),
        }
    }
}

impl HeapSize for AnalyzedFunction {
    fn heap_size(&self) -> usize {
        self.name.heap_size()
            + self.pos.heap_size()
            + self.args_str.heap_size()
            + self.returns_str.heap_size()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedAnalyzedFunction {
    name: String,
    pos: Option<SerializedAnalyzedSourcePosition>,
    udf_type: String,
    visibility: Option<Visibility>,
    args: Option<StringBuffer>,
    returns: Option<StringBuffer>,
}

impl TryFrom<AnalyzedFunction> for SerializedAnalyzedFunction {
    type Error = anyhow::Error;

    fn try_from(f: AnalyzedFunction) -> anyhow::Result<Self> {
        Ok(Self {
            name: f.name.to_string(),
            pos: f.pos.map(TryFrom::try_from).transpose()?,
            udf_type: f.udf_type.to_string(),
            visibility: f.visibility,
            args: f.args_str,
            returns: f.returns_str,
        })
    }
}

impl TryFrom<SerializedAnalyzedFunction> for AnalyzedFunction {
    type Error = anyhow::Error;

    fn try_from(f: SerializedAnalyzedFunction) -> anyhow::Result<Self> {
        Ok(Self {
            name: FunctionName::from_str(&f.name)?,
            pos: f.pos.map(AnalyzedSourcePosition::try_from).transpose()?,
            udf_type: f.udf_type.parse()?,
            visibility: f.visibility,
            args_str: f.args,
            returns_str: f.returns,
        })
    }
}

mod codegen_analyzed_function {
    use value::codegen_convex_serialization;

    use super::{
        AnalyzedFunction,
        SerializedAnalyzedFunction,
    };

    codegen_convex_serialization!(AnalyzedFunction, SerializedAnalyzedFunction);
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerializedHttpActionRoute {
    path: String,
    method: String,
}

impl TryFrom<HttpActionRoute> for SerializedHttpActionRoute {
    type Error = anyhow::Error;

    fn try_from(r: HttpActionRoute) -> anyhow::Result<Self> {
        Ok(Self {
            path: r.path,
            method: r.method.to_string(),
        })
    }
}

impl TryFrom<SerializedHttpActionRoute> for HttpActionRoute {
    type Error = anyhow::Error;

    fn try_from(r: SerializedHttpActionRoute) -> anyhow::Result<Self> {
        Ok(Self {
            path: r.path.parse()?,
            method: r.method.parse()?,
            matched: true,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzedHttpRoute {
    pub route: HttpActionRoute,
    pub pos: Option<AnalyzedSourcePosition>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerializedAnalyzedHttpRoute {
    route: SerializedHttpActionRoute,
    pos: Option<SerializedAnalyzedSourcePosition>,
}

impl HeapSize for AnalyzedHttpRoute {
    fn heap_size(&self) -> usize {
        self.route.heap_size() + self.pos.heap_size()
    }
}

impl TryFrom<AnalyzedHttpRoute> for SerializedAnalyzedHttpRoute {
    type Error = anyhow::Error;

    fn try_from(r: AnalyzedHttpRoute) -> anyhow::Result<Self> {
        Ok(Self {
            route: SerializedHttpActionRoute::try_from(r.route)?,
            pos: r.pos.map(TryFrom::try_from).transpose()?,
        })
    }
}

impl TryFrom<SerializedAnalyzedHttpRoute> for AnalyzedHttpRoute {
    type Error = anyhow::Error;

    fn try_from(r: SerializedAnalyzedHttpRoute) -> anyhow::Result<Self> {
        Ok(Self {
            route: HttpActionRoute::try_from(r.route)?,
            pos: r.pos.map(AnalyzedSourcePosition::try_from).transpose()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AnalyzedHttpRoutes {
    routes: WithHeapSize<Vec<AnalyzedHttpRoute>>,
}

pub struct HttpPrefixMatch<'a> {
    pub route: &'a HttpActionRoute,
    pub suffix: RoutedHttpPath,
}

impl AnalyzedHttpRoutes {
    pub fn new(routes: Vec<AnalyzedHttpRoute>) -> Self {
        Self {
            routes: routes.into(),
        }
    }

    pub fn route_exact(&self, path: &str, method: RoutableMethod) -> Option<&HttpActionRoute> {
        self.routes
            .iter()
            .map(|analyzed| &analyzed.route)
            .find(|route| {
                !route.path.ends_with('*') && route.method == method && route.path == path
            })
    }

    pub fn route_prefix(
        &self,
        path: &RoutedHttpPath,
        method: RoutableMethod,
    ) -> Option<HttpPrefixMatch<'_>> {
        let mut longest_match: Option<HttpPrefixMatch<'_>> = None;
        for AnalyzedHttpRoute { route, .. } in &self.routes {
            if route.method != method {
                continue;
            }
            let Some(mut prefix_path) = route.path.strip_suffix('*') else {
                continue;
            };
            if prefix_path.is_empty() {
                prefix_path = "/";
            }
            let Some(match_suffix) = path.strip_prefix(prefix_path) else {
                continue;
            };
            let new_match = RoutedHttpPath(format!("/{match_suffix}"));
            if let Some(ref existing_match) = longest_match {
                // If the existing longest match has a shorter suffix, then it
                // matches a longer prefix.
                if existing_match.suffix.len() < new_match.len() {
                    continue;
                }
            }
            longest_match = Some(HttpPrefixMatch {
                route,
                suffix: new_match,
            });
        }
        longest_match
    }
}

impl HeapSize for AnalyzedHttpRoutes {
    fn heap_size(&self) -> usize {
        self.routes.heap_size()
    }
}

impl IntoIterator for AnalyzedHttpRoutes {
    type IntoIter = Box<dyn Iterator<Item = AnalyzedHttpRoute>>;
    type Item = AnalyzedHttpRoute;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.routes.into_iter())
    }
}

impl Deref for AnalyzedHttpRoutes {
    type Target = [AnalyzedHttpRoute];

    fn deref(&self) -> &Self::Target {
        &self.routes
    }
}

// TODO: consider denormalizing SerializedMappedModule into
// SerializedAnalyzedModule and  instead just include source information. This
// requires a decent migration from Dashboard  schema.
//  See https://github.com/convex/convex/pull/14382/files#r1252372646 for further discussion.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SerializedMappedModule {
    source_index: Option<u32>,
    functions: Vec<SerializedAnalyzedFunction>,
    http_routes: Option<Vec<SerializedAnalyzedHttpRoute>>,
    cron_specs: Option<Vec<SerializedNamedCronSpec>>,
}

impl TryFrom<AnalyzedModule> for SerializedMappedModule {
    type Error = anyhow::Error;

    fn try_from(m: AnalyzedModule) -> anyhow::Result<Self> {
        anyhow::ensure!(
            m.source_index.is_some(),
            "source_index must be set to be serializing into SerializedMappedModule"
        );
        Ok(Self {
            source_index: m.source_index,
            functions: m
                .functions
                .into_iter()
                .map(TryFrom::try_from)
                .try_collect()?,
            http_routes: m
                .http_routes
                .map(|routes| routes.into_iter().map(TryFrom::try_from).try_collect())
                .transpose()?,
            cron_specs: m
                .cron_specs
                .map(|specs| specs.into_iter().map(TryFrom::try_from).try_collect())
                .transpose()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_route_matches_retain_registered_identity() {
        let routes = AnalyzedHttpRoutes::new(
            [
                (RoutableMethod::Get, "/items/latest"),
                (RoutableMethod::Post, "/items/latest"),
                (RoutableMethod::Get, "/items/*"),
                (RoutableMethod::Get, "/session/*"),
            ]
            .into_iter()
            .map(|(method, path)| AnalyzedHttpRoute {
                route: HttpActionRoute {
                    method,
                    path: path.into(),
                    matched: true,
                },
                pos: None,
            })
            .collect(),
        );
        let get = routes
            .route_exact("/items/latest", RoutableMethod::Get)
            .unwrap();
        let post = routes
            .route_exact("/items/latest", RoutableMethod::Post)
            .unwrap();
        assert_ne!(get, post);
        assert_eq!(get.path, "/items/latest");
        assert_eq!(
            routes.route_exact("/items/latest", "HEAD".parse().unwrap()),
            Some(get),
        );
        let first = routes
            .route_prefix(&RoutedHttpPath("/items/first".into()), RoutableMethod::Get)
            .unwrap();
        let second = routes
            .route_prefix(&RoutedHttpPath("/items/second".into()), RoutableMethod::Get)
            .unwrap();
        assert_eq!(first.route, second.route);
        assert_eq!(first.route.path, "/items/*");
        assert_eq!(&first.suffix[..], "/first");
        assert_eq!(&second.suffix[..], "/second");
        assert_ne!(get, first.route);
        assert!(routes
            .route_exact("/missing", RoutableMethod::Get)
            .is_none());
        assert!(routes
            .route_prefix(&RoutedHttpPath("/missing".into()), RoutableMethod::Get)
            .is_none());
        assert!(routes
            .route_prefix(&RoutedHttpPath("/items/first".into()), RoutableMethod::Post)
            .is_none());
    }

    #[test]
    fn http_route_prefix_uses_the_longest_registered_prefix_in_any_order() {
        for paths in [["*", "/a/*", "/a//*"], ["/a//*", "/a/*", "*"]] {
            let routes = AnalyzedHttpRoutes::new(
                paths
                    .into_iter()
                    .map(|path| AnalyzedHttpRoute {
                        route: HttpActionRoute {
                            method: RoutableMethod::Get,
                            path: path.into(),
                            matched: true,
                        },
                        pos: None,
                    })
                    .collect(),
            );
            let matched = routes
                .route_prefix(&RoutedHttpPath("/a//item".into()), RoutableMethod::Get)
                .unwrap();
            assert_eq!(matched.route.path, "/a//*");
            assert_eq!(&matched.suffix[..], "/item");
            let fallback = routes
                .route_prefix(&RoutedHttpPath("/other".into()), RoutableMethod::Get)
                .unwrap();
            assert_eq!(fallback.route.path, "*");
            assert_eq!(&fallback.suffix[..], "/other");
        }
    }

    fn serialized_module(json: serde_json::Value) -> SerializedAnalyzedModule {
        serde_json::from_value(json).expect("invalid serialized analyzed module")
    }

    fn empty_serialized_module_with_reuse_fields(
        context_reuse: Option<serde_json::Value>,
        reuse_context: Option<bool>,
    ) -> serde_json::Value {
        let mut module = serde_json::json!({
            "functions": [],
            "httpRoutes": null,
            "cronSpecs": null,
            "sourceMapped": null,
        });
        if let Some(context_reuse) = context_reuse {
            module["contextReuse"] = context_reuse;
        }
        if let Some(reuse_context) = reuse_context {
            module["reuseContext"] = reuse_context.into();
        }
        module
    }

    #[test]
    fn initialization_metadata_round_trips_and_selects_context_class() -> anyhow::Result<()> {
        use serde_json::json;
        for (initializer, database, action) in [
            (
                json!("_deps/legacy.js"),
                Some("_deps/legacy.js"),
                Some("_deps/legacy.js"),
            ),
            (
                json!({"version": 2, "database": "_deps/db.js", "action": "_deps/actions.js"}),
                Some("_deps/db.js"),
                Some("_deps/actions.js"),
            ),
            (
                json!({"version": 2, "database": "_deps/db.js"}),
                Some("_deps/db.js"),
                None,
            ),
            (
                json!({"version": 2, "action": "_deps/actions.js"}),
                None,
                Some("_deps/actions.js"),
            ),
        ] {
            let mut value = empty_serialized_module_with_reuse_fields(
                Some(json!({
                    "queries": true, "mutations": true, "actions": true,
                })),
                None,
            );
            value["contextInitializationModule"] = initializer.clone();
            let analyzed = AnalyzedModule::try_from(serialized_module(value))?;
            let module = analyzed.context_initialization_module.as_ref().unwrap();
            for kind in [UdfType::Query, UdfType::Mutation] {
                assert_eq!(module.for_type(kind).map(|path| path.as_str()), database);
            }
            assert_eq!(
                module.for_type(UdfType::Action).map(|path| path.as_str()),
                action
            );
            assert!(module.for_type(UdfType::HttpAction).is_none());
            let encoded =
                serde_json::to_value(SerializedAnalyzedModule::try_from(analyzed.clone())?)?;
            assert_eq!(encoded["contextInitializationModule"], initializer);
            assert_eq!(
                AnalyzedModule::try_from(serialized_module(encoded))?,
                analyzed
            );
        }
        Ok(())
    }

    #[test]
    fn initialization_metadata_rejects_malformed_or_inconsistent_split_policy() {
        use serde_json::json;
        for initializer in [
            json!({"version": 1, "database": "_deps/db.js"}),
            json!({"version": 3, "action": "_deps/actions.js"}),
            json!({"database": "_deps/db.js"}),
            json!({"version": 2}),
            json!({"version": 2, "database": "entry.js"}),
            json!({"version": 2, "action": "_deps/./actions.js"}),
            json!({"version": 2, "database": false}),
            json!({"version": 2, "database": "_deps/db.js", "actions": "_deps/actions.js"}),
        ] {
            assert!(serde_json::from_value::<ContextInitializationModule>(initializer).is_err());
        }
        for (policy, initializer) in [
            (
                json!({"actions": true}),
                json!({"version": 2, "database": "_deps/db.js"}),
            ),
            (
                json!({"queries": true}),
                json!({"version": 2, "action": "_deps/actions.js"}),
            ),
        ] {
            let mut value = empty_serialized_module_with_reuse_fields(Some(policy), None);
            value["contextInitializationModule"] = initializer;
            assert!(AnalyzedModule::try_from(serialized_module(value)).is_err());
        }
    }

    #[test]
    fn typed_empty_policy_overrides_legacy_database_policy() {
        let serialized = serialized_module(empty_serialized_module_with_reuse_fields(
            Some(serde_json::json!({
                "queries": false,
                "mutations": false,
                "actions": false,
                "httpActions": false,
            })),
            Some(true),
        ));
        let analyzed = AnalyzedModule::try_from(serialized).expect("failed to decode module");
        assert_eq!(analyzed.context_reuse, ContextReusePolicy::default());
    }

    #[test]
    fn absent_typed_policy_keeps_legacy_database_compatibility() {
        let serialized =
            serialized_module(empty_serialized_module_with_reuse_fields(None, Some(true)));
        let analyzed = AnalyzedModule::try_from(serialized).expect("failed to decode module");
        assert_eq!(analyzed.context_reuse, ContextReusePolicy::database());
    }

    #[test]
    fn explicit_empty_policy_is_serialized_for_rolling_compatibility() {
        let serialized = SerializedAnalyzedModule::try_from(AnalyzedModule::default())
            .expect("failed to encode module");
        let value = serde_json::to_value(serialized).expect("failed to serialize module");
        assert_eq!(
            value.get("contextReuse"),
            Some(&serde_json::json!({
                "queries": false,
                "mutations": false,
                "actions": false,
                "httpActions": false,
            }))
        );
        assert!(value.get("reuseContext").is_none());
    }

    #[test]
    fn typed_action_policy_round_trips_without_legacy_database_permission() {
        let analyzed = AnalyzedModule {
            context_reuse: ContextReusePolicy {
                actions: true,
                http_actions: true,
                ..ContextReusePolicy::default()
            },
            ..AnalyzedModule::default()
        };
        let serialized =
            SerializedAnalyzedModule::try_from(analyzed.clone()).expect("failed to encode module");
        let value = serde_json::to_value(&serialized).expect("failed to serialize module");
        assert!(value.get("reuseContext").is_none());
        assert_eq!(AnalyzedModule::try_from(serialized).unwrap(), analyzed);
    }

    #[test]
    fn mixed_policy_preserves_legacy_database_permission() {
        let analyzed = AnalyzedModule {
            context_reuse: ContextReusePolicy {
                queries: true,
                mutations: true,
                actions: true,
                http_actions: true,
            },
            ..AnalyzedModule::default()
        };
        let serialized =
            SerializedAnalyzedModule::try_from(analyzed).expect("failed to encode module");
        let value = serde_json::to_value(serialized).expect("failed to serialize module");
        assert_eq!(
            value.get("reuseContext"),
            Some(&serde_json::Value::Bool(true))
        );
    }
}
