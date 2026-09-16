use std::collections::BTreeMap;

use common::{
    sha256::{
        Sha256,
        Sha256Digest,
    },
    types::ModuleEnvironment,
};

use crate::{
    config::types::ModuleConfig,
    external_packages::types::ExternalDepsPackage,
    source_packages::types::NodeVersion,
};

pub const RUNTIME_CONTENT_IDENTITY_KIND: &str = "convex-source-package-runtime-content-v1";

const RUNTIME_CONTENT_DOMAIN: &[u8] = b"convex-source-package-runtime-content-v1\0";

/// Hash the runtime-relevant content of a complete source package.
///
/// This identity deliberately excludes archive representation and durable
/// references such as storage keys and document IDs.
pub fn runtime_content_sha256(
    modules: &[ModuleConfig],
    external_deps_package: Option<&ExternalDepsPackage>,
    node_version: Option<NodeVersion>,
) -> anyhow::Result<Sha256Digest> {
    let mut modules_by_path = BTreeMap::new();
    for module in modules {
        let path = module.path.clone().canonicalize();
        anyhow::ensure!(
            modules_by_path.insert(path, module).is_none(),
            "Multiple modules canonicalize to the same name"
        );
    }

    let mut hasher = Sha256::new();
    hasher.update(RUNTIME_CONTENT_DOMAIN);
    write_count(&mut hasher, modules_by_path.len());
    for (path, module) in modules_by_path {
        write_bytes(&mut hasher, path.as_str().as_bytes());
        write_bytes(&mut hasher, module.source.as_bytes());
        write_optional_bytes(
            &mut hasher,
            module
                .source_map
                .as_ref()
                .map(|source_map| source_map.as_bytes()),
        );
        write_bytes(
            &mut hasher,
            match module.environment {
                ModuleEnvironment::Isolate => b"isolate",
                ModuleEnvironment::Node => b"node",
                ModuleEnvironment::Invalid => b"invalid",
            },
        );
        write_optional_bytes(
            &mut hasher,
            module
                .node_pool
                .as_ref()
                .map(|pool| pool.as_ref().as_bytes()),
        );
    }

    match external_deps_package {
        None => hasher.update(&[0]),
        Some(package) => {
            hasher.update(&[1]);
            write_bytes(&mut hasher, package.sha256.as_ref());
            let mut deps = package.deps.iter().collect::<Vec<_>>();
            deps.sort_by(|left, right| {
                (&left.package, &left.version).cmp(&(&right.package, &right.version))
            });
            write_count(&mut hasher, deps.len());
            for dep in deps {
                write_bytes(&mut hasher, dep.package.as_bytes());
                write_bytes(&mut hasher, dep.version.as_bytes());
            }
        },
    }

    let node_version = match node_version {
        None => None,
        Some(NodeVersion::V18x) => Some(b"18".as_slice()),
        Some(NodeVersion::V20x) => Some(b"20".as_slice()),
        Some(NodeVersion::V22x) => Some(b"22".as_slice()),
        Some(NodeVersion::V24x) => Some(b"24".as_slice()),
    };
    write_optional_bytes(&mut hasher, node_version);
    Ok(hasher.finalize())
}

fn write_count(hasher: &mut Sha256, count: usize) {
    let count = u64::try_from(count).expect("source package item count does not fit in u64");
    hasher.update(&count.to_be_bytes());
}

fn write_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    let len = u64::try_from(bytes.len()).expect("source package field length does not fit in u64");
    hasher.update(&len.to_be_bytes());
    hasher.update(bytes);
}

fn write_optional_bytes(hasher: &mut Sha256, bytes: Option<&[u8]>) {
    match bytes {
        None => hasher.update(&[0]),
        Some(bytes) => {
            hasher.update(&[1]);
            write_bytes(hasher, bytes);
        },
    }
}

#[cfg(test)]
mod tests {
    use common::types::{
        NodeDependency,
        ObjectKey,
    };

    use super::*;
    use crate::{
        config::types::NodeExecutorPoolName,
        modules::module_versions::ModuleSource,
        source_packages::types::PackageSize,
    };

    fn module(
        path: &str,
        source: &str,
        source_map: Option<&str>,
        environment: ModuleEnvironment,
        node_pool: Option<&str>,
    ) -> ModuleConfig {
        ModuleConfig {
            path: path.parse().unwrap(),
            source: ModuleSource::new(source),
            source_map: source_map.map(str::to_owned),
            environment,
            node_pool: node_pool.map(|pool| pool.parse::<NodeExecutorPoolName>().unwrap()),
        }
    }

    fn external_package(storage_key: &str) -> ExternalDepsPackage {
        ExternalDepsPackage {
            storage_key: ObjectKey::try_from(storage_key).unwrap(),
            sha256: Sha256Digest::from([7; 32]),
            deps: vec![
                NodeDependency {
                    package: "zeta".to_owned(),
                    version: "2.0.0".to_owned(),
                },
                NodeDependency {
                    package: "alpha".to_owned(),
                    version: "1.0.0".to_owned(),
                },
            ],
            package_size: PackageSize {
                zipped_size_bytes: 100,
                unzipped_size_bytes: 200,
            },
        }
    }

    fn baseline_modules() -> Vec<ModuleConfig> {
        vec![
            module(
                "shared.js",
                "export const shared = 1;",
                Some("shared-source-map"),
                ModuleEnvironment::Isolate,
                None,
            ),
            module(
                "actions/run.js",
                "\"use node\"; export default 2;",
                None,
                ModuleEnvironment::Node,
                Some("workers"),
            ),
        ]
    }

    fn digest(
        modules: &[ModuleConfig],
        external_deps_package: Option<&ExternalDepsPackage>,
        node_version: Option<NodeVersion>,
    ) -> Sha256Digest {
        runtime_content_sha256(modules, external_deps_package, node_version).unwrap()
    }

    #[test]
    fn runtime_content_sha256_has_stable_golden_vector() {
        let modules = baseline_modules();
        let external_package = external_package("external-package-a");
        assert_eq!(
            digest(&modules, Some(&external_package), Some(NodeVersion::V22x)).as_hex(),
            "178960ce955cca712a805dffdb1a08bcb27a392745f061afee8cb0e4b8e263fa"
        );
    }

    #[test]
    fn runtime_content_sha256_ignores_storage_and_packaging_representation() {
        let modules = baseline_modules();
        let mut reordered_modules = modules.clone();
        reordered_modules.reverse();
        let original_external = external_package("external-package-a");
        let mut repackaged_external = external_package("external-package-b");
        repackaged_external.deps.reverse();
        repackaged_external.package_size = PackageSize {
            zipped_size_bytes: 300,
            unzipped_size_bytes: 400,
        };

        assert_eq!(
            digest(&modules, Some(&original_external), Some(NodeVersion::V22x)),
            digest(
                &reordered_modules,
                Some(&repackaged_external),
                Some(NodeVersion::V22x)
            )
        );
    }

    #[test]
    fn runtime_content_sha256_covers_every_runtime_input() {
        let modules = baseline_modules();
        let external_package = external_package("external-package-a");
        let baseline = digest(&modules, Some(&external_package), Some(NodeVersion::V22x));

        let mut changed_path = modules.clone();
        changed_path[0].path = "renamed.js".parse().unwrap();
        assert_ne!(
            baseline,
            digest(
                &changed_path,
                Some(&external_package),
                Some(NodeVersion::V22x)
            )
        );

        let mut changed_source = modules.clone();
        changed_source[0].source = ModuleSource::new("export const shared = 2;");
        assert_ne!(
            baseline,
            digest(
                &changed_source,
                Some(&external_package),
                Some(NodeVersion::V22x)
            )
        );

        let mut changed_source_map = modules.clone();
        changed_source_map[0].source_map = Some("changed-source-map".to_owned());
        assert_ne!(
            baseline,
            digest(
                &changed_source_map,
                Some(&external_package),
                Some(NodeVersion::V22x)
            )
        );

        let mut changed_environment = modules.clone();
        changed_environment[0].environment = ModuleEnvironment::Node;
        assert_ne!(
            baseline,
            digest(
                &changed_environment,
                Some(&external_package),
                Some(NodeVersion::V22x)
            )
        );

        let mut changed_node_pool = modules.clone();
        changed_node_pool[1].node_pool = Some("other".parse().unwrap());
        assert_ne!(
            baseline,
            digest(
                &changed_node_pool,
                Some(&external_package),
                Some(NodeVersion::V22x)
            )
        );

        let mut changed_external_package = external_package.clone();
        changed_external_package.sha256 = Sha256Digest::from([8; 32]);
        assert_ne!(
            baseline,
            digest(
                &modules,
                Some(&changed_external_package),
                Some(NodeVersion::V22x)
            )
        );

        let mut changed_declared_deps = external_package.clone();
        changed_declared_deps.deps[0].version = "3.0.0".to_owned();
        assert_ne!(
            baseline,
            digest(
                &modules,
                Some(&changed_declared_deps),
                Some(NodeVersion::V22x)
            )
        );

        assert_ne!(
            baseline,
            digest(&modules, Some(&external_package), Some(NodeVersion::V24x))
        );
    }

    #[test]
    fn source_and_source_map_are_separate_length_delimited_fields() {
        let first = vec![module(
            "shared.js",
            "ab",
            Some("c"),
            ModuleEnvironment::Isolate,
            None,
        )];
        let second = vec![module(
            "shared.js",
            "a",
            Some("bc"),
            ModuleEnvironment::Isolate,
            None,
        )];
        let absent_map = vec![module(
            "shared.js",
            "abc",
            None,
            ModuleEnvironment::Isolate,
            None,
        )];

        assert_ne!(digest(&first, None, None), digest(&second, None, None));
        assert_ne!(digest(&first, None, None), digest(&absent_map, None, None));
    }
}
