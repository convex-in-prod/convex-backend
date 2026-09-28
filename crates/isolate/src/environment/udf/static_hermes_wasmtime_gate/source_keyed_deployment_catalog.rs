use super::*;

/// The authenticated, immutable source-keyed catalog. Catalog entries are
/// descriptors only; presence in the catalog never implies readiness.
pub(super) struct SourceKeyedDeploymentCatalog {
    catalog_sha256: String,
    descriptors:
        BTreeMap<SourceKeyedGenerationSelector, Arc<ValidatedRuntimeRegistryGenerationDescriptor>>,
    registry_use: RegistryUse,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct SourceKeyedGenerationSelector {
    pub(super) deployment_sha256: String,
    pub(super) generation_manifest_sha256: String,
    pub(super) generation_sha256: String,
    pub(super) source_package_runtime_content_sha256: String,
}

impl SourceKeyedGenerationSelector {
    pub(super) fn from_descriptor(
        descriptor: &ValidatedRuntimeRegistryGenerationDescriptor,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            deployment_sha256: descriptor.deployment_sha256().to_owned(),
            generation_manifest_sha256: descriptor.generation_manifest_sha256().to_owned(),
            generation_sha256: descriptor.generation_sha256().to_owned(),
            source_package_runtime_content_sha256: descriptor
                .source_package_runtime_content_sha256()
                .context("runtime generation descriptor is not source-keyable")?
                .to_owned(),
        })
    }

    pub(super) fn requested(
        runtime_content_sha256: &str,
        generation: &SourceKeyedRuntimeGenerationIdentity,
    ) -> Self {
        Self {
            deployment_sha256: generation.deployment_sha256.clone(),
            generation_manifest_sha256: generation.generation_manifest_sha256.clone(),
            generation_sha256: generation.generation_sha256.clone(),
            source_package_runtime_content_sha256: runtime_content_sha256.to_owned(),
        }
    }
}

impl SourceKeyedDeploymentCatalog {
    pub(super) fn from_descriptors(
        catalog_sha256: String,
        descriptors: impl IntoIterator<Item = ValidatedRuntimeRegistryGenerationDescriptor>,
        registry_use: RegistryUse,
    ) -> anyhow::Result<Self> {
        let mut catalog = Self {
            catalog_sha256,
            descriptors: BTreeMap::new(),
            registry_use,
        };
        for descriptor in descriptors {
            catalog.stage_descriptor(Arc::new(descriptor))?;
        }
        anyhow::ensure!(
            !catalog.descriptors.is_empty(),
            "source-keyed runtime catalog must contain at least one descriptor",
        );
        Ok(catalog)
    }

    pub(super) fn catalog_sha256(&self) -> &str {
        &self.catalog_sha256
    }

    pub(super) fn registry_use(&self) -> RegistryUse {
        self.registry_use
    }

    pub(super) fn descriptor(
        &self,
        runtime_content_sha256: &str,
        generation: &SourceKeyedRuntimeGenerationIdentity,
    ) -> Option<Arc<ValidatedRuntimeRegistryGenerationDescriptor>> {
        self.descriptors
            .get(&SourceKeyedGenerationSelector::requested(
                runtime_content_sha256,
                generation,
            ))
            .cloned()
    }

    pub(super) fn descriptor_by_selector(
        &self,
        selector: &SourceKeyedGenerationSelector,
    ) -> Option<Arc<ValidatedRuntimeRegistryGenerationDescriptor>> {
        self.descriptors.get(selector).cloned()
    }

    /// Preserve descriptor ownership for unchanged selectors while allowing
    /// retired selectors to leave the catalog. In-flight loads independently
    /// verify that their descriptor is still current before publication.
    pub(super) fn successor(
        &self,
        catalog_sha256: String,
        descriptors: impl IntoIterator<Item = ValidatedRuntimeRegistryGenerationDescriptor>,
    ) -> anyhow::Result<Self> {
        let mut successor = Self {
            catalog_sha256,
            descriptors: BTreeMap::new(),
            registry_use: self.registry_use,
        };
        for descriptor in descriptors {
            let descriptor = Arc::new(descriptor);
            let selector = SourceKeyedGenerationSelector::from_descriptor(&descriptor)?;
            if let Some(previous) = self.descriptors.get(&selector) {
                anyhow::ensure!(
                    previous
                        .as_ref()
                        .same_authenticated_identity(descriptor.as_ref()),
                    "source-keyed runtime catalog reload changed a retained exact descriptor",
                );
                successor.descriptors.insert(selector, Arc::clone(previous));
            } else {
                successor.descriptors.insert(selector, descriptor);
            }
        }
        anyhow::ensure!(
            !successor.descriptors.is_empty(),
            "source-keyed runtime catalog must contain at least one descriptor",
        );
        Ok(successor)
    }

    fn stage_descriptor(
        &mut self,
        descriptor: Arc<ValidatedRuntimeRegistryGenerationDescriptor>,
    ) -> anyhow::Result<()> {
        let selector = SourceKeyedGenerationSelector::from_descriptor(&descriptor)?;
        anyhow::ensure!(
            self.descriptors.insert(selector, descriptor).is_none(),
            "source-keyed runtime catalog contains a duplicate exact source/generation pair",
        );
        Ok(())
    }
}
