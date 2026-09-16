use bytes::Bytes;
use common::{
    runtime::Runtime,
    types::NodeDependency,
};
use model::external_packages::types::{
    ExternalDepsPackage,
    ExternalDepsPackageId,
};
use storage::Upload;
use value::sha256::Sha256Digest;

use crate::Application;

impl<RT: Runtime> Application<RT> {
    /// Import exact dependency bytes when moving a frozen deployment between
    /// targets. Storage keys and document IDs are target-owned; runtime
    /// content identity depends on the archive digest and dependency
    /// declarations instead.
    pub async fn import_external_node_deps(
        &self,
        deps: Vec<NodeDependency>,
        bytes: Bytes,
        expected_sha256: Sha256Digest,
    ) -> anyhow::Result<(ExternalDepsPackageId, ExternalDepsPackage)> {
        let package_size =
            ExternalDepsPackage::imported_archive_size(&bytes, &expected_sha256).await?;
        let mut upload = self.modules_storage().start_upload().await?;
        if let Err(error) = upload.write(bytes).await {
            // This upload has not published anything. Abort its incomplete parts.
            upload.abort().await?;
            return Err(error);
        }
        let package = ExternalDepsPackage {
            storage_key: upload.complete().await?,
            sha256: expected_sha256,
            deps,
            package_size,
        };
        let id = self._upload_external_deps_package(package.clone()).await?;
        Ok((id, package))
    }
}

#[cfg(test)]
mod tests {
    use async_zip::{
        base::write::ZipFileWriter,
        Compression,
        ZipEntryBuilder,
    };
    use value::sha256::Sha256;

    use super::*;

    #[tokio::test]
    async fn imported_external_deps_preserve_digest_and_measure_archive_size() -> anyhow::Result<()>
    {
        let content = b"export const answer = 42;";
        let mut writer = ZipFileWriter::new(Vec::new());
        writer
            .write_entry_whole(
                ZipEntryBuilder::new("node_modules/example/index.js".into(), Compression::Deflate),
                content,
            )
            .await?;
        let archive = writer.close().await?;
        let size =
            ExternalDepsPackage::imported_archive_size(&archive, &Sha256::hash(&archive)).await?;
        assert_eq!(size.zipped_size_bytes, archive.len());
        assert_eq!(size.unzipped_size_bytes, content.len());
        assert!(
            ExternalDepsPackage::imported_archive_size(&archive, &Sha256Digest::from([0; 32]))
                .await
                .is_err()
        );
        let malformed = b"not a ZIP archive";
        assert!(
            ExternalDepsPackage::imported_archive_size(malformed, &Sha256::hash(malformed))
                .await
                .is_err()
        );
        let empty = ZipFileWriter::new(Vec::new()).close().await?;
        assert!(
            ExternalDepsPackage::imported_archive_size(&empty, &Sha256::hash(&empty))
                .await
                .is_err()
        );
        Ok(())
    }
}
