use std::{
    collections::BTreeMap,
    env,
    fs::OpenOptions,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
};

use anyhow::Context;
use application::deploy_config::StartPushRequest;
use common::{
    sha256::Sha256,
    types::{
        ModuleEnvironment,
        ObjectKey,
    },
};
use model::{
    config::types::{
        ModuleConfig,
        AUTH_CONFIG_FILE_NAME,
    },
    external_packages::types::ExternalDepsPackage,
    modules::hash_module_source,
    source_packages::{
        runtime_content::{
            runtime_content_sha256,
            RUNTIME_CONTENT_IDENTITY_KIND,
        },
        types::{
            NodeVersion,
            PackageSize,
        },
        upload_download::write_package,
    },
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sync_types::CanonicalizedModulePath;

const AUTHORITY_KIND: &str = "convex-source-package-preactivation-authority-v1";

struct Arguments {
    external_deps_package: Option<PathBuf>,
    external_deps_storage_key: Option<ObjectKey>,
    source_package_output: PathBuf,
    start_push: PathBuf,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FileIdentity {
    sha256: String,
    size: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DependencyIdentity {
    package: String,
    version: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExternalDepsIdentity {
    dependencies: Vec<DependencyIdentity>,
    sha256: String,
    size: usize,
    storage_key: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SourceMapIdentity {
    sha256: String,
    size: usize,
    sources_content_count: usize,
    sources_count: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum RuntimeModuleRole {
    DeploymentConfiguration,
    Node,
    UdfIsolate,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeModuleIdentity {
    environment: &'static str,
    module_sha256: String,
    node_pool: Option<String>,
    path: String,
    role: RuntimeModuleRole,
    source_map: Option<SourceMapIdentity>,
    source_sha256: String,
    source_size: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PreactivationAuthority {
    external_deps_package: Option<ExternalDepsIdentity>,
    kind: &'static str,
    node_version: Option<&'static str>,
    package_module_count: usize,
    request: FileIdentity,
    runtime_content_algorithm: &'static str,
    runtime_content_sha256: String,
    runtime_module_count: usize,
    runtime_modules: Vec<RuntimeModuleIdentity>,
    source_package: FileIdentity,
}

fn parse_arguments() -> anyhow::Result<Arguments> {
    let mut values = BTreeMap::new();
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    anyhow::ensure!(arguments.len() % 2 == 0, usage());
    for pair in arguments.chunks_exact(2) {
        anyhow::ensure!(
            matches!(
                pair[0].as_str(),
                "--external-deps-package"
                    | "--external-deps-storage-key"
                    | "--source-package-output"
                    | "--start-push"
            ) && values.insert(pair[0].clone(), pair[1].clone()).is_none(),
            usage()
        );
    }
    let external_deps_package = values.remove("--external-deps-package").map(PathBuf::from);
    let external_deps_storage_key = values
        .remove("--external-deps-storage-key")
        .map(ObjectKey::try_from)
        .transpose()?;
    anyhow::ensure!(
        external_deps_package.is_some() == external_deps_storage_key.is_some(),
        "external dependency package path and storage key must be supplied together"
    );
    let source_package_output = values
        .remove("--source-package-output")
        .map(PathBuf::from)
        .context(usage())?;
    let start_push = values
        .remove("--start-push")
        .map(PathBuf::from)
        .context(usage())?;
    anyhow::ensure!(values.is_empty(), usage());
    Ok(Arguments {
        external_deps_package,
        external_deps_storage_key,
        source_package_output,
        start_push,
    })
}

fn usage() -> &'static str {
    "usage: source_package_preactivation_authority --start-push REQUEST.json \
     --source-package-output PACKAGE.zip [--external-deps-package PACKAGE.zip \
     --external-deps-storage-key KEY]"
}

fn file_identity(bytes: &[u8]) -> FileIdentity {
    FileIdentity {
        sha256: Sha256::hash(bytes).as_hex(),
        size: bytes.len(),
    }
}

fn source_map_identity(source_map: &str) -> anyhow::Result<SourceMapIdentity> {
    let parsed: JsonValue = serde_json::from_str(source_map)?;
    let sources_count = parsed
        .get("sources")
        .and_then(JsonValue::as_array)
        .map_or(0, Vec::len);
    let sources_content_count = parsed
        .get("sourcesContent")
        .and_then(JsonValue::as_array)
        .map_or(0, |values| {
            values.iter().filter(|value| !value.is_null()).count()
        });
    Ok(SourceMapIdentity {
        sha256: Sha256::hash(source_map.as_bytes()).as_hex(),
        size: source_map.len(),
        sources_content_count,
        sources_count,
    })
}

fn module_identity(module: &ModuleConfig) -> anyhow::Result<RuntimeModuleIdentity> {
    let path = module.path.clone().canonicalize();
    let (environment, role) = match module.environment {
        ModuleEnvironment::Isolate if path.as_str() == AUTH_CONFIG_FILE_NAME => {
            ("isolate", RuntimeModuleRole::DeploymentConfiguration)
        },
        ModuleEnvironment::Isolate => ("isolate", RuntimeModuleRole::UdfIsolate),
        ModuleEnvironment::Node => ("node", RuntimeModuleRole::Node),
        ModuleEnvironment::Invalid => anyhow::bail!("runtime module has an invalid environment"),
    };
    Ok(RuntimeModuleIdentity {
        environment,
        module_sha256: hash_module_source(&module.source, module.source_map.as_ref()).as_hex(),
        node_pool: module.node_pool.as_ref().map(ToString::to_string),
        path: path.as_str().to_owned(),
        role,
        source_map: module
            .source_map
            .as_deref()
            .map(source_map_identity)
            .transpose()?,
        source_sha256: Sha256::hash(module.source.as_bytes()).as_hex(),
        source_size: module.source.as_bytes().len(),
    })
}

fn node_version_label(node_version: Option<NodeVersion>) -> Option<&'static str> {
    match node_version {
        None => None,
        Some(NodeVersion::V18x) => Some("18"),
        Some(NodeVersion::V20x) => Some("20"),
        Some(NodeVersion::V22x) => Some("22"),
        Some(NodeVersion::V24x) => Some("24"),
    }
}

async fn run(arguments: Arguments) -> anyhow::Result<()> {
    let request_bytes = std::fs::read(&arguments.start_push)?;
    let request: StartPushRequest = serde_json::from_slice(&request_bytes)?;
    anyhow::ensure!(!request.dry_run, "start_push request must not be a dry run");
    anyhow::ensure!(
        !request.for_codegen,
        "start_push request must not be for code generation"
    );
    let config = request.into_project_config()?;
    anyhow::ensure!(
        config.component_definitions.is_empty(),
        "component deployment requests are not supported"
    );
    anyhow::ensure!(
        config
            .app_definition
            .unchanged_runtime_module_hashes
            .is_empty(),
        "start_push request must contain all runtime modules"
    );

    let mut runtime_modules = config.app_definition.changed_runtime_modules.clone();
    runtime_modules.sort_by_key(|module| module.path.clone().canonicalize());
    for pair in runtime_modules.windows(2) {
        anyhow::ensure!(
            pair[0].path.clone().canonicalize() != pair[1].path.clone().canonicalize(),
            "multiple runtime modules canonicalize to the same path"
        );
    }
    let package_modules = config
        .app_definition
        .all_modules(&runtime_modules)
        .cloned()
        .collect::<Vec<_>>();

    let external_deps_bytes = arguments
        .external_deps_package
        .as_ref()
        .map(std::fs::read)
        .transpose()?;
    anyhow::ensure!(
        config.node_dependencies.is_empty() == external_deps_bytes.is_none(),
        "external dependency archive presence must match start_push declarations"
    );
    let external_deps_package = match (
        external_deps_bytes.as_deref(),
        arguments.external_deps_storage_key.clone(),
    ) {
        (None, None) => None,
        (Some(bytes), Some(storage_key)) => Some(ExternalDepsPackage {
            storage_key,
            sha256: Sha256::hash(bytes),
            deps: config.node_dependencies.clone(),
            package_size: PackageSize {
                zipped_size_bytes: bytes.len(),
                unzipped_size_bytes: 0,
            },
        }),
        _ => unreachable!("argument validation keeps external dependency inputs paired"),
    };
    if let Some(selection) = &config.external_deps_package {
        let package = external_deps_package
            .as_ref()
            .context("Selected external dependency package requires an archive")?;
        anyhow::ensure!(
            package.sha256 == selection.sha256,
            "External dependency archive SHA-256 differs from the frozen selection"
        );
    }
    let runtime_content_sha256 = runtime_content_sha256(
        &package_modules,
        external_deps_package.as_ref(),
        config.node_version,
    )?;

    let package = package_modules
        .iter()
        .map(|module| (module.path.clone().canonicalize(), module))
        .collect::<BTreeMap<CanonicalizedModulePath, &ModuleConfig>>();
    anyhow::ensure!(
        package.len() == package_modules.len(),
        "multiple source-package modules canonicalize to the same path"
    );
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&arguments.source_package_output)?;
    write_package(
        package,
        tokio::fs::File::from_std(output),
        external_deps_package
            .as_ref()
            .map(|package| package.storage_key.clone()),
    )
    .await?;
    let source_package_bytes = std::fs::read(&arguments.source_package_output)?;

    let mut dependencies = config
        .node_dependencies
        .iter()
        .map(|dependency| DependencyIdentity {
            package: dependency.package.clone(),
            version: dependency.version.clone(),
        })
        .collect::<Vec<_>>();
    dependencies.sort_by(|left, right| {
        (&left.package, &left.version).cmp(&(&right.package, &right.version))
    });
    let external_deps_identity = match (
        external_deps_bytes.as_deref(),
        external_deps_package.as_ref(),
    ) {
        (None, None) => None,
        (Some(bytes), Some(package)) => Some(ExternalDepsIdentity {
            dependencies,
            sha256: package.sha256.as_hex(),
            size: bytes.len(),
            storage_key: package.storage_key.to_string(),
        }),
        _ => unreachable!("external dependency material is constructed atomically"),
    };
    let authority = PreactivationAuthority {
        external_deps_package: external_deps_identity,
        kind: AUTHORITY_KIND,
        node_version: node_version_label(config.node_version),
        package_module_count: package_modules.len(),
        request: file_identity(&request_bytes),
        runtime_content_algorithm: RUNTIME_CONTENT_IDENTITY_KIND,
        runtime_content_sha256: runtime_content_sha256.as_hex(),
        runtime_module_count: runtime_modules.len(),
        runtime_modules: runtime_modules
            .iter()
            .map(module_identity)
            .collect::<anyhow::Result<_>>()?,
        source_package: file_identity(&source_package_bytes),
    };
    serde_json::to_writer(std::io::stdout().lock(), &authority)?;
    println!();
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(parse_arguments()?).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn selected_external_deps_reject_different_archive_before_writing_authority(
    ) -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let archive = directory.path().join("dependencies.zip");
        let request = directory.path().join("start-push.json");
        let output = directory.path().join("source-package.zip");
        let dependency_bytes = b"exact external dependency archive bytes";
        std::fs::write(&archive, dependency_bytes)?;
        let mut request_value = serde_json::json!({
            "adminKey": "test-only",
            "functions": "convex/",
            "appDefinition": {
                "definition": null,
                "dependencies": [],
                "schema": null,
                "changedModules": [{
                    "path": "action.js",
                    "source": "export const action = 1;",
                    "environment": "node",
                }],
                "unchangedModuleHashes": [],
                "udfServerVersion": "1.0.0",
            },
            "componentDefinitions": [],
            "nodeDependencies": [{ "name": "example-package", "version": "1.0.0" }],
            "nodeVersion": "24",
            "externalDepsPackage": {
                "id": value::DeveloperDocumentId::MIN.encode(),
                "sha256": "0".repeat(64),
            },
        });
        std::fs::write(&request, serde_json::to_vec(&request_value)?)?;
        let arguments = || Arguments {
            external_deps_package: Some(archive.clone()),
            external_deps_storage_key: Some("selected-external-dependencies".try_into().unwrap()),
            source_package_output: output.clone(),
            start_push: request.clone(),
        };
        let error = run(arguments()).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("differs from the frozen selection"));
        assert!(!output.exists());
        request_value["externalDepsPackage"]["sha256"] = "invalid".into();
        std::fs::write(&request, serde_json::to_vec(&request_value)?)?;
        assert!(run(arguments()).await.is_err());
        assert!(!output.exists());

        request_value["externalDepsPackage"]["sha256"] =
            Sha256::hash(dependency_bytes).as_hex().into();
        let declarations = request_value["nodeDependencies"].take();
        request_value["nodeDependencies"] = serde_json::json!([]);
        std::fs::write(&request, serde_json::to_vec(&request_value)?)?;
        assert!(run(arguments()).await.is_err());
        assert!(!output.exists());

        request_value["nodeDependencies"] = declarations;
        std::fs::write(&request, serde_json::to_vec(&request_value)?)?;
        run(arguments()).await?;
        assert!(output.exists());

        // Disposable conformance requests intentionally omit target-local selection.
        request_value
            .as_object_mut()
            .unwrap()
            .remove("externalDepsPackage");
        std::fs::write(&request, serde_json::to_vec(&request_value)?)?;
        let mut ordinary = arguments();
        ordinary.source_package_output = directory.path().join("ordinary-source-package.zip");
        run(ordinary).await?;
        Ok(())
    }
}
