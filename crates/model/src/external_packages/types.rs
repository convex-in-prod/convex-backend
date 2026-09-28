use std::io::Cursor;

use async_zip_0_0_9::read::seek::ZipFileReader;
use common::types::{
    NodeDependency,
    ObjectKey,
};
use errors::ErrorMetadata;
use serde::{
    Deserialize,
    Serialize,
};
use serde_bytes::ByteBuf;
use value::{
    codegen_convex_serialization,
    id_v6::DeveloperDocumentId,
    sha256::{
        Sha256,
        Sha256Digest,
    },
    ConvexObject,
};

use crate::source_packages::types::{
    PackageSize,
    SerializedPackageSize,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalDepsPackage {
    pub storage_key: ObjectKey,
    pub sha256: Sha256Digest,
    pub deps: Vec<NodeDependency>,
    pub package_size: PackageSize,
}

#[derive(Debug, Clone)]
pub struct ExternalDepsPackageSelection {
    pub id: ExternalDepsPackageId,
    pub sha256: Sha256Digest,
}

impl ExternalDepsPackage {
    pub async fn imported_archive_size(
        bytes: &[u8],
        expected_sha256: &Sha256Digest,
    ) -> anyhow::Result<PackageSize> {
        let mut size = PackageSize {
            zipped_size_bytes: bytes.len(),
            unzipped_size_bytes: 0,
        };
        size.verify_size()?;
        anyhow::ensure!(
            Sha256::hash(bytes) == *expected_sha256,
            ErrorMetadata::bad_request(
                "ExternalDepsPackageMismatch",
                "Imported dependency archive SHA-256 differs from the selected package"
            ),
        );
        let reader = ZipFileReader::new(Cursor::new(bytes)).await.map_err(|_| {
            anyhow::anyhow!(ErrorMetadata::bad_request(
                "InvalidExternalDepsPackage",
                "Imported dependency archive is not a valid ZIP"
            ))
        })?;
        anyhow::ensure!(
            !reader.entries().is_empty(),
            ErrorMetadata::bad_request(
                "InvalidExternalDepsPackage",
                "Imported dependency archive is empty"
            )
        );
        for entry in reader.entries() {
            size.unzipped_size_bytes = size
                .unzipped_size_bytes
                .checked_add(usize::try_from(entry.uncompressed_size())?)
                .ok_or_else(|| {
                    anyhow::anyhow!(ErrorMetadata::bad_request(
                        "ModulesTooLarge",
                        "Imported dependency archive size overflows"
                    ))
                })?;
            size.verify_size()?;
        }
        Ok(size)
    }

    pub fn validate_dependencies(&self, dependencies: &[NodeDependency]) -> anyhow::Result<()> {
        let mut expected: Vec<_> = dependencies
            .iter()
            .map(|dependency| (&dependency.package, &dependency.version))
            .collect();
        let mut actual: Vec<_> = self
            .deps
            .iter()
            .map(|dependency| (&dependency.package, &dependency.version))
            .collect();
        expected.sort();
        actual.sort();
        anyhow::ensure!(
            !expected.is_empty() && actual == expected,
            ErrorMetadata::bad_request(
                "ExternalDepsPackageMismatch",
                "Selected external dependency package does not match the requested dependencies",
            )
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalDepsPackageId(DeveloperDocumentId);

impl From<DeveloperDocumentId> for ExternalDepsPackageId {
    fn from(id: DeveloperDocumentId) -> Self {
        Self(id)
    }
}

impl From<ExternalDepsPackageId> for DeveloperDocumentId {
    fn from(value: ExternalDepsPackageId) -> Self {
        value.0
    }
}

impl From<ExternalDepsPackageId> for String {
    fn from(value: ExternalDepsPackageId) -> Self {
        value.0.into()
    }
}

impl TryFrom<String> for ExternalDepsPackageId {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let id = DeveloperDocumentId::decode(&value)?;
        Ok(Self(id))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializedExternalDepsPackage {
    storage_key: String,
    sha256: ByteBuf,
    deps: Vec<ConvexObject>,
    #[serde(default)]
    package_size: Option<SerializedPackageSize>,
}

impl TryFrom<SerializedExternalDepsPackage> for ExternalDepsPackage {
    type Error = anyhow::Error;

    fn try_from(value: SerializedExternalDepsPackage) -> Result<Self, Self::Error> {
        Ok(Self {
            storage_key: value.storage_key.try_into()?,
            sha256: value.sha256.into_vec().try_into()?,
            deps: value
                .deps
                .into_iter()
                .map(NodeDependency::try_from)
                .collect::<anyhow::Result<_>>()?,
            package_size: value
                .package_size
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}

impl TryFrom<ExternalDepsPackage> for SerializedExternalDepsPackage {
    type Error = anyhow::Error;

    fn try_from(value: ExternalDepsPackage) -> Result<Self, Self::Error> {
        Ok(Self {
            storage_key: value.storage_key.into(),
            sha256: ByteBuf::from(value.sha256.to_vec().as_slice()),
            deps: value
                .deps
                .into_iter()
                .map(ConvexObject::try_from)
                .collect::<anyhow::Result<_>>()?,
            package_size: Some(value.package_size.try_into()?),
        })
    }
}

codegen_convex_serialization!(ExternalDepsPackage, SerializedExternalDepsPackage);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_external_deps_require_exact_nonempty_declarations() {
        let dependencies = vec![
            NodeDependency {
                package: "a".to_owned(),
                version: "1.0.0".to_owned(),
            },
            NodeDependency {
                package: "b".to_owned(),
                version: "2.0.0".to_owned(),
            },
        ];
        let package = ExternalDepsPackage {
            storage_key: "selected-dependencies".try_into().unwrap(),
            sha256: Sha256Digest::from([1; 32]),
            deps: dependencies.clone(),
            package_size: PackageSize::default(),
        };
        let mut reordered = dependencies.clone();
        reordered.reverse();
        package.validate_dependencies(&reordered).unwrap();
        assert!(package.validate_dependencies(&[]).is_err());
        assert!(package.validate_dependencies(&dependencies[..1]).is_err());
        let mut changed = dependencies.clone();
        changed[0].version = "1.0.1".to_owned();
        assert!(package.validate_dependencies(&changed).is_err());
        changed = dependencies.clone();
        changed.push(dependencies[0].clone());
        assert!(package.validate_dependencies(&changed).is_err());
    }
}
