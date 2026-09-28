use super::*;
use crate::environment::udf::{
    wasm_udf_manifest::UdfKind,
    wasm_udf_package::ValidatedRuntimeRegistryGenerationDescriptor,
};

/// Immutable shadow route selection authenticated with one deployment
/// generation.
///
/// This is constructed while a generation is loaded, before it can become
/// active. Keeping both the ordered route list and the membership index here
/// lets request-time evidence authentication avoid rebuilding or scanning the
/// manifest's selected routes.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct QueryShadowRouteSelection {
    pub(super) generation_sha256: String,
    route_sha256s: BTreeMap<UdfType, Vec<String>>,
    route_sha256_index: BTreeMap<UdfType, HashSet<String>>,
}

impl QueryShadowRouteSelection {
    fn new(
        generation_sha256: String,
        query_route_sha256s: BTreeSet<String>,
        mutation_route_sha256s: BTreeSet<String>,
    ) -> Self {
        let route_sha256s: BTreeMap<UdfType, Vec<String>> = BTreeMap::from([
            (UdfType::Query, query_route_sha256s.into_iter().collect()),
            (
                UdfType::Mutation,
                mutation_route_sha256s.into_iter().collect(),
            ),
        ]);
        let route_sha256_index = route_sha256s
            .iter()
            .map(|(udf_type, route_sha256s)| (*udf_type, route_sha256s.iter().cloned().collect()))
            .collect();
        Self {
            generation_sha256,
            route_sha256s,
            route_sha256_index,
        }
    }

    pub(super) fn route_sha256s(&self, udf_type: UdfType) -> &[String] {
        self.route_sha256s
            .get(&udf_type)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub(super) fn authenticates_route(
        &self,
        udf_type: UdfType,
        generation_sha256: &str,
        route_sha256: &str,
    ) -> bool {
        self.generation_sha256 == generation_sha256
            && self
                .route_sha256_index
                .get(&udf_type)
                .is_some_and(|routes| routes.contains(route_sha256))
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum GeneratedRouteIdentity {
    Singleton {
        package_key: String,
    },
    DeploymentExport {
        deployment_sha256: String,
        generation_sha256: String,
        package_key: String,
        entry: DeploymentEntryIdentity,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct GeneratedAotModuleIdentity {
    pub(super) engine_compatibility_sha256: String,
    pub(super) serialized_module_sha256: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct GeneratedGraphPoolIdentity {
    pub(super) generation_sha256: String,
    pub(super) graph_sha256: String,
    pub(super) engine_compatibility_sha256: String,
    pub(super) base: GeneratedAotModuleIdentity,
    pub(super) shared: Vec<GeneratedAotModuleIdentity>,
    pub(super) leaf: GeneratedAotModuleIdentity,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum GeneratedPoolIdentity {
    Route(GeneratedRouteIdentity),
    Graph(GeneratedGraphPoolIdentity),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum DeploymentEntryIdentity {
    LegacyExport {
        runtime_module_path: String,
        export_name: String,
    },
    CapabilityPackage,
}

impl DeploymentEntryIdentity {
    pub(super) fn from_registry(entry: DeploymentRuntimeEntry<'_>) -> Self {
        match entry {
            DeploymentRuntimeEntry::LegacyExport {
                runtime_module_path,
                export_name,
            } => Self::LegacyExport {
                runtime_module_path: runtime_module_path.to_owned(),
                export_name: export_name.to_owned(),
            },
            DeploymentRuntimeEntry::CapabilityPackage => Self::CapabilityPackage,
        }
    }
}

pub(super) struct DeploymentGeneration {
    // Legacy deployments have no authenticated generation manifest descriptor.
    pub(super) registry_descriptor: Option<ValidatedRuntimeRegistryGenerationDescriptor>,
    pub(super) admission: RuntimeRegistryAdmission,
    pub(super) current_sha256: String,
    pub(super) generation_manifest_sha256: String,
    pub(super) generation_sha256: String,
    pub(super) query_shadow_route_selection: Arc<QueryShadowRouteSelection>,
    pub(super) packages_root: PathBuf,
    pub(super) registry: ValidatedDeploymentManifest,
    pub(super) module_graph_catalog: Option<AuthenticatedModuleGraphCatalog>,
    // Preloaded graph modules do not point back to their generation. Retaining
    // them here keeps destination-compiled AOT resident for the generation's
    // ready lifetime without creating a generation <-> routed-module cycle.
    preloaded_graph_modules: parking_lot::Mutex<Option<Vec<Arc<GeneratedSharedAotModule>>>>,
    load_verified: AtomicBool,
    pub(super) retired: AtomicBool,
}

impl DeploymentGeneration {
    pub(super) fn from_validated(generation: ValidatedRuntimeRegistryGeneration) -> Arc<Self> {
        let (
            current_sha256,
            generation_manifest_sha256,
            generation_sha256,
            packages_root,
            registry,
            module_graph_catalog,
            admission,
            descriptor,
        ) = generation.into_parts();
        let query_shadow_route_selection = Arc::new(QueryShadowRouteSelection::new(
            generation_sha256.clone(),
            registry.shadow_route_ids(UdfKind::Query),
            registry.shadow_route_ids(UdfKind::Mutation),
        ));
        Arc::new(Self {
            registry_descriptor: Some(descriptor),
            admission,
            current_sha256,
            generation_manifest_sha256,
            generation_sha256,
            query_shadow_route_selection,
            packages_root,
            registry,
            module_graph_catalog,
            preloaded_graph_modules: parking_lot::Mutex::new(None),
            load_verified: AtomicBool::new(false),
            retired: AtomicBool::new(false),
        })
    }

    pub(super) fn from_legacy(
        artifact_cache_root: PathBuf,
        registry: ValidatedDeploymentManifest,
    ) -> Arc<Self> {
        let deployment_sha256 = registry.deployment_sha256().to_owned();
        let query_shadow_route_selection = Arc::new(QueryShadowRouteSelection::new(
            deployment_sha256.clone(),
            registry.shadow_route_ids(UdfKind::Query),
            registry.shadow_route_ids(UdfKind::Mutation),
        ));
        let packages_root = registry.package_registry_root(&artifact_cache_root);
        Arc::new(Self {
            registry_descriptor: None,
            admission: RuntimeRegistryAdmission::PrimaryAdmitted,
            current_sha256: deployment_sha256.clone(),
            generation_manifest_sha256: deployment_sha256.clone(),
            generation_sha256: deployment_sha256,
            query_shadow_route_selection,
            packages_root,
            registry,
            module_graph_catalog: None,
            preloaded_graph_modules: parking_lot::Mutex::new(None),
            load_verified: AtomicBool::new(false),
            retired: AtomicBool::new(false),
        })
    }

    pub(super) fn deployment_sha256(&self) -> &str {
        self.registry.deployment_sha256()
    }

    pub(super) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    pub(super) fn publish_load_verification(&self) -> anyhow::Result<()> {
        self.load_verified
            .compare_exchange(false, true, Ordering::Release, Ordering::Relaxed)
            .map(|_| ())
            .map_err(|_| {
                anyhow::anyhow!("runtime generation load verification was published twice")
            })
    }

    pub(super) fn publish_preloaded_load_verification(
        &self,
        modules: Vec<Arc<GeneratedSharedAotModule>>,
    ) -> anyhow::Result<()> {
        let mut retained = self.preloaded_graph_modules.lock();
        anyhow::ensure!(
            retained.is_none(),
            "runtime generation graph modules were published twice"
        );
        *retained = Some(modules);
        if let Err(error) = self.publish_load_verification() {
            retained.take();
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn release_preloaded_graph_modules(&self) {
        let modules = self.preloaded_graph_modules.lock().take();
        drop(modules);
    }

    pub(super) fn is_load_verified(&self) -> bool {
        self.load_verified.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AuthenticatedGraphAotResolution {
    PreloadedOnly,
    ReconstructBeforeReadiness,
}

pub(super) struct GeneratedRoutedModule {
    pub(super) emscripten_graph: Option<GeneratedEmscriptenGraphModules>,
    pub(super) engine: Arc<Engine>,
    pub(super) entry_selector: Option<u64>,
    pub(super) fixed_module_bytes: usize,
    pub(super) generation: Option<Arc<DeploymentGeneration>>,
    pub(super) module_charge: OnceLock<GeneratedModuleMemoryCharge>,
    pub(super) module: Arc<Module>,
    pub(super) manifest: Arc<WasmUdfExecutionPolicy>,
    pub(super) package_identity: Arc<ValidatedWasmUdfPackageIdentity>,
    pub(super) permitted_conditional_convex_imports: BTreeSet<&'static str>,
    pub(super) graph: Option<GeneratedGraphModules>,
    pub(super) pool_identity: GeneratedPoolIdentity,
    pub(super) route_identity: GeneratedRouteIdentity,
}

impl GeneratedRoutedModule {
    fn has_generation_incarnation(&self, generation: Option<&DeploymentGeneration>) -> bool {
        match (self.generation.as_deref(), generation) {
            (Some(left), Some(right)) => std::ptr::eq(left, right),
            (None, None) => true,
            (Some(_), None) | (None, Some(_)) => false,
        }
    }

    pub(super) fn has_same_generation_incarnation(&self, other: &Self) -> bool {
        self.has_generation_incarnation(other.generation.as_deref())
    }

    pub(super) fn store_instance_limit(&self) -> usize {
        match (&self.emscripten_graph, &self.graph) {
            (Some(graph), None) => graph.modules.len(),
            (None, Some(graph)) => graph.dependencies.len() + 1,
            (None, None) => 1,
            (Some(_), Some(_)) => {
                panic!("generated route retained two module graph representations")
            },
        }
    }
}

pub(super) fn cached_routed_module_matches_generation(
    routed: &GeneratedRoutedModule,
    generation: Option<&DeploymentGeneration>,
) -> anyhow::Result<bool> {
    if routed.has_generation_incarnation(generation) {
        return Ok(true);
    }
    // Retirement may occur after a caller checks the flag but before it takes
    // the cache lock. A later same-digest incarnation can then own the semantic
    // cache key. Live owners of the retired pointer must build without caching
    // instead of reusing that later incarnation.
    anyhow::ensure!(
        generation.is_some_and(DeploymentGeneration::is_retired),
        "cached generated Wasm module belongs to another generation incarnation"
    );
    Ok(false)
}

pub(super) struct GeneratedGraphModules {
    pub(super) dependencies: Vec<GeneratedGraphDependency>,
    pub(super) layout: CapabilityGraphLayout,
}

pub(super) struct GeneratedEmscriptenGraphModules {
    pub(super) base: Arc<GeneratedSharedAotModule>,
    pub(super) shared: Vec<Arc<GeneratedSharedAotModule>>,
    // Keep the shared compiled-code charge alive with the routed leaf Module.
    pub(super) leaf: Arc<GeneratedSharedAotModule>,
    pub(super) initialization: GraphInitialization,
    pub(super) modules: Vec<GeneratedEmscriptenGraphModuleContract>,
}

pub(super) struct GeneratedEmscriptenGraphModuleContract {
    pub(super) contract: GraphModuleContract,
    pub(super) layout: GraphModuleLayout,
    pub(super) providers: Vec<GraphModuleProvider>,
}

pub(super) struct GeneratedGraphDependency {
    pub(super) module_id: String,
    pub(super) provider_namespace: String,
    pub(super) module: Arc<GeneratedSharedAotModule>,
}

pub(super) struct GeneratedSharedAotModule {
    pub(super) identity: GeneratedAotModuleIdentity,
    pub(super) module: Arc<Module>,
    pub(super) module_charge: parking_lot::Mutex<Option<GeneratedModuleMemoryCharge>>,
}

pub(super) struct GeneratedSharedAotModuleCacheEntry {
    pub(super) last_used: u64,
    pub(super) module: Arc<GeneratedSharedAotModule>,
}

pub(super) struct GeneratedRoutedModuleCacheEntry {
    pub(super) last_used: u64,
    pub(super) routed: Arc<GeneratedRoutedModule>,
}

#[derive(Default)]
pub(super) struct GeneratedRoutedModuleCache {
    pub(super) clock: u64,
    pub(super) entries: BTreeMap<GeneratedRouteIdentity, GeneratedRoutedModuleCacheEntry>,
    pub(super) shared_aot_modules:
        BTreeMap<GeneratedAotModuleIdentity, GeneratedSharedAotModuleCacheEntry>,
}

impl GeneratedRoutedModuleCache {
    fn advance_clock(&mut self) -> u64 {
        self.clock = self
            .clock
            .checked_add(1)
            .expect("generated Wasm module cache clock overflow");
        self.clock
    }

    pub(super) fn get(
        &mut self,
        route_identity: &GeneratedRouteIdentity,
    ) -> Option<Arc<GeneratedRoutedModule>> {
        let clock = self.advance_clock();
        let entry = self.entries.get_mut(route_identity)?;
        entry.last_used = clock;
        record_module_cache_event("module_cache_hit");
        Some(Arc::clone(&entry.routed))
    }

    pub(super) fn singleton(&mut self) -> Option<Arc<GeneratedRoutedModule>> {
        let route_identity = self
            .entries
            .keys()
            .find(|identity| matches!(identity, GeneratedRouteIdentity::Singleton { .. }))?
            .clone();
        self.get(&route_identity)
    }

    pub(super) fn get_shared_aot_module(
        &mut self,
        identity: &GeneratedAotModuleIdentity,
    ) -> Option<Arc<GeneratedSharedAotModule>> {
        let clock = self.advance_clock();
        let entry = self.shared_aot_modules.get_mut(identity)?;
        entry.last_used = clock;
        record_module_cache_event("shared_aot_cache_hit");
        Some(Arc::clone(&entry.module))
    }

    pub(super) fn reserve(
        &mut self,
        controller: &Arc<GeneratedMemoryController>,
        fixed_bytes: usize,
    ) -> Result<GeneratedModuleMemoryCharge, ModuleMemoryAdmissionError> {
        loop {
            match controller.try_charge_module(fixed_bytes) {
                Ok(charge) => return Ok(charge),
                Err(error) => {
                    if error == ModuleMemoryAdmissionError::FixedBytesExceedHardBudget
                        || !self.evict_lru_idle(error)
                    {
                        return Err(error);
                    }
                },
            }
        }
    }

    fn resize(
        &mut self,
        charge: &mut GeneratedModuleMemoryCharge,
        fixed_bytes: usize,
    ) -> Result<(), ModuleMemoryAdmissionError> {
        loop {
            match charge.try_resize(fixed_bytes) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if error == ModuleMemoryAdmissionError::FixedBytesExceedHardBudget
                        || !self.evict_lru_idle(error)
                    {
                        return Err(error);
                    }
                },
            }
        }
    }

    fn evict_lru_idle(&mut self, error: ModuleMemoryAdmissionError) -> bool {
        let route_candidate = self
            .entries
            .iter()
            .filter(|(_, entry)| Arc::strong_count(&entry.routed) == 1)
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(identity, entry)| (entry.last_used, identity.clone()));
        let shared_candidate = self
            .shared_aot_modules
            .iter()
            .filter(|(_, entry)| Arc::strong_count(&entry.module) == 1)
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(identity, entry)| (entry.last_used, identity.clone()));
        let candidate = match (route_candidate, shared_candidate) {
            (Some(route), Some(shared)) if route.0 <= shared.0 => {
                Some(GeneratedModuleCacheEviction::Route(route.1))
            },
            (Some(_), Some(shared)) => Some(GeneratedModuleCacheEviction::SharedAot(shared.1)),
            (Some(route), None) => Some(GeneratedModuleCacheEviction::Route(route.1)),
            (None, Some(shared)) => Some(GeneratedModuleCacheEviction::SharedAot(shared.1)),
            (None, None) => None,
        };
        let Some(candidate) = candidate else {
            return false;
        };
        let event = match error {
            ModuleMemoryAdmissionError::Pressure => "module_cache_evicted_pressure",
            ModuleMemoryAdmissionError::SoftBudget | ModuleMemoryAdmissionError::HardBudget => {
                "module_cache_evicted_budget"
            },
            ModuleMemoryAdmissionError::FixedBytesExceedHardBudget => {
                unreachable!("permanent module charge rejection cannot be fixed by eviction")
            },
        };
        match candidate {
            GeneratedModuleCacheEviction::Route(candidate) => {
                self.remove(&candidate, event);
            },
            GeneratedModuleCacheEviction::SharedAot(candidate) => {
                self.remove_shared_aot_module(&candidate, event);
            },
        }
        true
    }

    pub(super) fn evict_idle_for_pressure(&mut self) {
        while self.evict_lru_idle(ModuleMemoryAdmissionError::Pressure) {}
    }

    pub(super) fn insert(
        &mut self,
        routed: Arc<GeneratedRoutedModule>,
        charge: GeneratedModuleMemoryCharge,
    ) -> Arc<GeneratedRoutedModule> {
        if matches!(
            &routed.route_identity,
            GeneratedRouteIdentity::Singleton { .. }
        ) {
            let replaced = self
                .entries
                .keys()
                .filter(|identity| {
                    matches!(*identity, GeneratedRouteIdentity::Singleton { .. })
                        && *identity != &routed.route_identity
                })
                .cloned()
                .collect::<Vec<_>>();
            for route_identity in replaced {
                self.remove(&route_identity, "module_cache_evicted_configuration");
            }
        }
        assert_eq!(
            charge.fixed_bytes(),
            routed.fixed_module_bytes,
            "generated Wasm module cache charge differs from its fixed-byte estimate"
        );
        assert!(
            routed.module_charge.set(charge).is_ok(),
            "generated Wasm routed module received more than one memory charge"
        );
        let last_used = self.advance_clock();
        assert!(
            self.entries
                .insert(
                    routed.route_identity.clone(),
                    GeneratedRoutedModuleCacheEntry {
                        last_used,
                        routed: Arc::clone(&routed),
                    },
                )
                .is_none(),
            "generated Wasm module cache inserted a duplicate route"
        );
        record_module_cache_event("module_cache_inserted");
        routed
    }

    pub(super) fn insert_shared_aot_module(
        &mut self,
        module: Arc<GeneratedSharedAotModule>,
        charge: GeneratedModuleMemoryCharge,
    ) -> Arc<GeneratedSharedAotModule> {
        assert!(
            module.module_charge.lock().replace(charge).is_none(),
            "generated Wasm shared AOT module received more than one memory charge"
        );
        let last_used = self.advance_clock();
        assert!(
            self.shared_aot_modules
                .insert(
                    module.identity.clone(),
                    GeneratedSharedAotModuleCacheEntry {
                        last_used,
                        module: Arc::clone(&module),
                    },
                )
                .is_none(),
            "generated Wasm shared AOT cache inserted a duplicate identity"
        );
        record_module_cache_event("shared_aot_cache_inserted");
        module
    }

    fn retain_charge_without_caching(
        &mut self,
        routed: &Arc<GeneratedRoutedModule>,
        charge: GeneratedModuleMemoryCharge,
    ) {
        assert_eq!(
            charge.fixed_bytes(),
            routed.fixed_module_bytes,
            "generated Wasm module charge differs from its fixed-byte estimate"
        );
        assert!(
            routed.module_charge.set(charge).is_ok(),
            "generated Wasm routed module received more than one memory charge"
        );
        record_module_cache_event("module_cache_bypassed_retired_generation");
    }

    pub(super) fn retire_generation(&mut self, generation: &Arc<DeploymentGeneration>) {
        let retired = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry
                    .routed
                    .generation
                    .as_ref()
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, generation))
            })
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        for route_identity in retired {
            self.remove(&route_identity, "module_cache_evicted_retired_generation");
        }
    }

    pub(super) fn remove(
        &mut self,
        route_identity: &GeneratedRouteIdentity,
        event: &'static str,
    ) -> Option<Arc<GeneratedRoutedModule>> {
        let entry = self.entries.remove(route_identity)?;
        record_module_cache_event(event);
        Some(entry.routed)
    }

    fn remove_shared_aot_module(
        &mut self,
        identity: &GeneratedAotModuleIdentity,
        event: &'static str,
    ) -> Option<Arc<GeneratedSharedAotModule>> {
        let entry = self.shared_aot_modules.remove(identity)?;
        record_module_cache_event(event);
        Some(entry.module)
    }

    pub(super) fn evict_all_idle_shared_aot_modules(&mut self) {
        let identities = self
            .shared_aot_modules
            .iter()
            .filter(|(_, entry)| Arc::strong_count(&entry.module) == 1)
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        for identity in identities {
            self.remove_shared_aot_module(&identity, "shared_aot_cache_evicted_without_consumers");
        }
    }

    pub(super) fn contains_key(&self, route_identity: &GeneratedRouteIdentity) -> bool {
        self.entries.contains_key(route_identity)
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod deployment_generation_lifetime_tests {
    use std::collections::BTreeSet;

    use anyhow::Context as _;

    use super::*;

    const SELECTOR_COUNT: u64 = 1_421;

    fn capability_route_identity(
        generation: &DeploymentGeneration,
        package_key: &str,
    ) -> GeneratedRouteIdentity {
        GeneratedRouteIdentity::DeploymentExport {
            deployment_sha256: generation.deployment_sha256().to_owned(),
            generation_sha256: generation.generation_sha256.clone(),
            package_key: package_key.to_owned(),
            entry: DeploymentEntryIdentity::CapabilityPackage,
        }
    }

    fn capability_routed_module(
        generation: Arc<DeploymentGeneration>,
        route_identity: GeneratedRouteIdentity,
    ) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
        let manifest = generated_test_manifest_with_limits(UdfKind::Query, Vec::new(), 1 << 20)?;
        generated_memory_test_routed_module_with_generation(
            manifest,
            GeneratedMemoryTestOperation {
                additional_pages: 0,
                destroy_infinite_loop: false,
                loop_iterations: Some(0),
                maximum_pages: 1,
            },
            route_identity,
            Some(generation),
        )
    }

    #[test]
    fn load_verification_keeps_cache_only_module_idle_and_generation_reclaimable(
    ) -> anyhow::Result<()> {
        let deployment_sha256 = "a".repeat(64);
        let generation = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/load-verification-lifetime"),
            ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
        );
        let weak_generation = Arc::downgrade(&generation);
        let route_identity = capability_route_identity(&generation, "capability-package");
        let routed = capability_routed_module(Arc::clone(&generation), route_identity.clone())?;
        let mut cache = GeneratedRoutedModuleCache::default();
        cache.entries.insert(
            route_identity,
            GeneratedRoutedModuleCacheEntry {
                last_used: 0,
                routed: Arc::clone(&routed),
            },
        );

        generation.publish_load_verification()?;
        assert!(generation.is_load_verified());
        assert!(generation.publish_load_verification().is_err());
        drop(routed);
        drop(generation);

        assert!(weak_generation.upgrade().is_some());
        cache.evict_idle_for_pressure();
        assert_eq!(cache.len(), 0);
        assert!(weak_generation.upgrade().is_none());
        Ok(())
    }

    #[test]
    fn ready_generation_keeps_preloaded_graph_module_out_of_idle_eviction() -> anyhow::Result<()> {
        let deployment_sha256 = "d".repeat(64);
        let generation = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/preloaded-graph-module-lifetime"),
            ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
        );
        let identity = GeneratedAotModuleIdentity {
            engine_compatibility_sha256: "e".repeat(64),
            serialized_module_sha256: "f".repeat(64),
        };
        let module = Arc::new(GeneratedSharedAotModule {
            identity: identity.clone(),
            module: Arc::new(Module::new(
                &Engine::default(),
                EncodedModule::new().finish(),
            )?),
            module_charge: parking_lot::Mutex::new(None),
        });
        let mut cache = GeneratedRoutedModuleCache::default();
        cache.shared_aot_modules.insert(
            identity.clone(),
            GeneratedSharedAotModuleCacheEntry {
                last_used: 0,
                module: Arc::clone(&module),
            },
        );

        generation.publish_preloaded_load_verification(vec![Arc::clone(&module)])?;
        drop(module);
        cache.evict_all_idle_shared_aot_modules();
        assert!(cache.shared_aot_modules.contains_key(&identity));

        // Retirement releases readiness ownership even while an admitted
        // invocation still retains the generation itself.
        generation.release_preloaded_graph_modules();
        cache.evict_all_idle_shared_aot_modules();
        assert!(!cache.shared_aot_modules.contains_key(&identity));
        Ok(())
    }

    #[test]
    fn capability_package_cache_stays_bounded_for_1421_distinct_selectors() -> anyhow::Result<()> {
        let deployment_sha256 = "b".repeat(64);
        let generation = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/capability-selector-scale"),
            ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
        );
        let route_identity = capability_route_identity(&generation, "shared-capability-package");
        let routed = capability_routed_module(Arc::clone(&generation), route_identity.clone())?;
        let mut cache = GeneratedRoutedModuleCache::default();
        cache.entries.insert(
            route_identity.clone(),
            GeneratedRoutedModuleCacheEntry {
                last_used: 0,
                routed: Arc::clone(&routed),
            },
        );
        generation.publish_load_verification()?;
        assert_eq!(Arc::strong_count(&routed), 2);

        let selectors = (0..SELECTOR_COUNT).collect::<BTreeSet<_>>();
        // The invocation route lease carries the selector. Capability-package
        // selectors must not multiply the module-cache identity.
        let route_identities = selectors
            .iter()
            .map(|_| capability_route_identity(&generation, "shared-capability-package"))
            .collect::<BTreeSet<_>>();
        assert_eq!(selectors.len(), usize::try_from(SELECTOR_COUNT)?);
        assert_eq!(route_identities.len(), 1);
        for route_identity in route_identities.iter().cycle().take(selectors.len()) {
            let selected = cache
                .get(route_identity)
                .context("capability-package cache entry disappeared")?;
            assert!(Arc::ptr_eq(&selected, &routed));
        }
        assert_eq!(cache.len(), 1);
        assert_eq!(Arc::strong_count(&routed), 2);
        Ok(())
    }

    #[test]
    fn retired_live_generation_ignores_a_later_same_digest_cache_hit() -> anyhow::Result<()> {
        let deployment_sha256 = "c".repeat(64);
        let old_generation = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/old-live-generation"),
            ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
        );
        let later_generation = DeploymentGeneration::from_legacy(
            PathBuf::from("/unused/later-resident-generation"),
            ValidatedDeploymentManifest::empty_for_test(&deployment_sha256),
        );
        let route_identity = capability_route_identity(&later_generation, "same-digest-package");
        let cached = capability_routed_module(Arc::clone(&later_generation), route_identity)?;

        assert!(
            cached_routed_module_matches_generation(&cached, Some(old_generation.as_ref()))
                .is_err()
        );
        old_generation.retired.store(true, Ordering::Release);
        assert!(!cached_routed_module_matches_generation(
            &cached,
            Some(old_generation.as_ref()),
        )?);
        assert!(cached_routed_module_matches_generation(
            &cached,
            Some(later_generation.as_ref()),
        )?);
        Ok(())
    }
}

enum GeneratedModuleCacheEviction {
    Route(GeneratedRouteIdentity),
    SharedAot(GeneratedAotModuleIdentity),
}

pub(super) static GENERATED_ROUTED_MODULES: LazyLock<
    parking_lot::Mutex<GeneratedRoutedModuleCache>,
> = LazyLock::new(|| parking_lot::Mutex::new(GeneratedRoutedModuleCache::default()));
static SHARED_GENERATED_ENGINE: LazyLock<parking_lot::Mutex<Option<Arc<Engine>>>> =
    LazyLock::new(|| parking_lot::Mutex::new(None));
static GENERATED_SHARED_AOT_LOAD_LOCKS: LazyLock<
    parking_lot::Mutex<BTreeMap<GeneratedAotModuleIdentity, Weak<parking_lot::Mutex<()>>>>,
> = LazyLock::new(|| parking_lot::Mutex::new(BTreeMap::new()));

pub(super) fn generated_shared_aot_load_lock(
    identity: &GeneratedAotModuleIdentity,
) -> Arc<parking_lot::Mutex<()>> {
    let mut locks = GENERATED_SHARED_AOT_LOAD_LOCKS.lock();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(identity).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(parking_lot::Mutex::new(()));
    locks.insert(identity.clone(), Arc::downgrade(&lock));
    lock
}

pub(super) fn new_generated_engine() -> anyhow::Result<Arc<Engine>> {
    let mut config = Config::new();
    config
        .target(GENERATED_TARGET_TRIPLE)
        .map_err(wasmtime_anyhow)?;
    config
        .consume_fuel(true)
        .epoch_interruption(true)
        // Profiling is an operator diagnostic, not part of AOT compatibility. Enabling PerfMap
        // here appends every loaded routed module to one process-wide file and can consume
        // unbounded disk and page-cache memory. A dedicated diagnostic process may opt into a
        // profiler without changing the authenticated precompiled artifact.
        .profiler(ProfilingStrategy::None)
        .wasm_exceptions(true);
    Ok(Arc::new(Engine::new(&config).map_err(wasmtime_anyhow)?))
}

pub(super) fn shared_generated_engine() -> anyhow::Result<Arc<Engine>> {
    let mut shared = SHARED_GENERATED_ENGINE.lock();
    if let Some(engine) = &*shared {
        return Ok(Arc::clone(engine));
    }
    let engine = new_generated_engine()?;
    let ticker_engine = Arc::clone(&engine);
    std::thread::Builder::new()
        .name("generated-wasm-epoch".to_owned())
        .spawn(move || {
            loop {
                std::thread::sleep(GENERATED_EPOCH_TICK_INTERVAL);
                // The shared epoch is only a clock. Each Store decides whether
                // its current invocation is eligible for interruption.
                ticker_engine.increment_epoch();
            }
        })
        .context("failed to start the generated Wasm epoch ticker")?;
    *shared = Some(Arc::clone(&engine));
    Ok(engine)
}

fn read_authenticated_graph_artifact(
    path: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> anyhow::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).with_context(|| {
        format!(
            "open authenticated module graph artifact {}",
            path.display()
        )
    })?;
    let metadata = file.metadata().with_context(|| {
        format!(
            "inspect authenticated module graph artifact {}",
            path.display()
        )
    })?;
    anyhow::ensure!(
        metadata.file_type().is_file() && metadata.len() == expected_size,
        "module graph artifact size changed after registry authentication"
    );
    let mut bytes = Vec::with_capacity(
        usize::try_from(expected_size).context("module graph artifact exceeds address space")?,
    );
    file.read_to_end(&mut bytes).with_context(|| {
        format!(
            "read authenticated module graph artifact {}",
            path.display()
        )
    })?;
    anyhow::ensure!(
        u64::try_from(bytes.len())? == expected_size
            && format!("{:x}", Sha256::digest(&bytes)) == expected_sha256,
        "module graph artifact changed after registry authentication"
    );
    Ok(bytes)
}

fn authenticate_graph_artifact(
    path: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> anyhow::Result<()> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).with_context(|| {
        format!(
            "open authenticated module graph artifact {}",
            path.display()
        )
    })?;
    let metadata = file.metadata().with_context(|| {
        format!(
            "inspect authenticated module graph artifact {}",
            path.display()
        )
    })?;
    anyhow::ensure!(
        metadata.file_type().is_file() && metadata.len() == expected_size,
        "module graph artifact size changed after registry authentication"
    );
    let mut digest = Sha256::new();
    let bytes_read =
        std::io::copy(&mut std::io::BufReader::new(file), &mut digest).with_context(|| {
            format!(
                "read authenticated module graph artifact {}",
                path.display()
            )
        })?;
    anyhow::ensure!(
        bytes_read == expected_size && format!("{:x}", digest.finalize()) == expected_sha256,
        "module graph artifact changed after registry authentication"
    );
    Ok(())
}

fn canonical_graph_value_type(ty: &ValType) -> anyhow::Result<&'static str> {
    match ty {
        ValType::I32 => Ok("i32"),
        ValType::I64 => Ok("i64"),
        ValType::F32 => Ok("f32"),
        ValType::F64 => Ok("f64"),
        ValType::V128 => Ok("v128"),
        ValType::Ref(reference) => match (reference.is_nullable(), reference.heap_type()) {
            // Match the Core Wasm contract aliases semantically. Wasmtime displays
            // these as `(ref null ...)`, which is equivalent but not contract text.
            (true, HeapType::Func) => Ok("funcref"),
            (true, HeapType::Extern) => Ok("externref"),
            (true, HeapType::Exn) => Ok("exnref"),
            _ => anyhow::bail!(
                "module graph AOT uses a reference type outside the Core Wasm contract"
            ),
        },
    }
}

fn canonical_graph_extern_type(ty: &ExternType) -> anyhow::Result<String> {
    Ok(match ty {
        ExternType::Func(ty) => canonical_graph_function_type(ty)?,
        ExternType::Global(ty) => format!(
            "global({},{})",
            canonical_graph_value_type(ty.content())?,
            if ty.mutability() == Mutability::Var {
                "var"
            } else {
                "const"
            }
        ),
        ExternType::Memory(ty) => format!(
            "memory{}(min={},max={},shared={},page={})",
            if ty.is_64() { "64" } else { "32" },
            ty.minimum(),
            ty.maximum()
                .map_or_else(|| "none".to_owned(), |value| value.to_string()),
            ty.is_shared(),
            ty.page_size_log2()
        ),
        ExternType::Table(ty) => format!(
            "table{}({},min={},max={})",
            if ty.is_64() { "64" } else { "32" },
            canonical_graph_value_type(&ValType::Ref(ty.element().clone()))?,
            ty.minimum(),
            ty.maximum()
                .map_or_else(|| "none".to_owned(), |value| value.to_string())
        ),
        ExternType::Tag(ty) => format!("tag({})", canonical_graph_function_type(ty.ty())?),
    })
}

fn canonical_graph_function_type(ty: &wasmtime::FuncType) -> anyhow::Result<String> {
    let parameters = ty
        .params()
        .map(|value| canonical_graph_value_type(&value))
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(",");
    let results = ty
        .results()
        .map(|value| canonical_graph_value_type(&value))
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(",");
    Ok(format!("func({parameters})->({results})"))
}

#[cfg(test)]
#[test]
fn graph_contract_uses_core_wasm_reference_aliases() -> anyhow::Result<()> {
    // Wasmtime renders the standard function reference as its expanded form.
    // The authenticated contract must use the stable Core Wasm alias instead.
    assert_eq!(ValType::FUNCREF.to_string(), "(ref null func)");
    assert_eq!(canonical_graph_value_type(&ValType::FUNCREF)?, "funcref");
    assert_eq!(
        canonical_graph_value_type(&ValType::EXTERNREF)?,
        "externref"
    );
    assert_eq!(canonical_graph_value_type(&ValType::EXNREF)?, "exnref");

    let mut tables = EncodedTableSection::new();
    tables.table(EncodedTableType {
        element_type: EncodedRefType::FUNCREF,
        table64: false,
        minimum: 4020,
        maximum: None,
        shared: false,
    });
    let mut exports = EncodedExportSection::new();
    exports.export("__indirect_function_table", EncodedExportKind::Table, 0);
    let mut encoded = EncodedModule::new();
    encoded.section(&tables).section(&exports);
    let engine = Engine::default();
    let module = Module::new(&engine, encoded.finish()).map_err(wasmtime_anyhow)?;
    let exported = module
        .exports()
        .next()
        .context("table fixture does not export its table")?;
    assert_eq!(
        canonical_graph_extern_type(&exported.ty())?,
        "table32(funcref,min=4020,max=none)"
    );
    Ok(())
}

#[cfg(test)]
#[test]
fn external_graph_rejects_incompatible_host_provider_type() -> anyhow::Result<()> {
    let mut types = EncodedTypeSection::new();
    types.ty().function(
        [EncodedValType::I32, EncodedValType::I32],
        std::iter::empty::<EncodedValType>(),
    );
    let mut imports = EncodedImportSection::new();
    imports.import(
        "convex",
        "convex_developer_error",
        EncodedEntityType::Function(0),
    );
    let mut encoded = EncodedModule::new();
    encoded.section(&types).section(&imports);
    let wasm = encoded.finish();
    let engine = new_generated_engine()?;
    let module = Module::new(&engine, &wasm).map_err(wasmtime_anyhow)?;
    let aot = engine.precompile_module(&wasm).map_err(wasmtime_anyhow)?;
    let imported = module
        .imports()
        .next()
        .context("host ABI fixture does not import convex_developer_error")?;
    let canonical = canonical_graph_extern_type(&imported.ty())?;
    let type_sha256 = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    let contract = GraphModuleContract {
        authority: super::super::module_graph_registry::GraphModuleContractAuthority {
            engine_compatibility_sha256: "test".into(),
            inspection_sha256: "test".into(),
            kind: "test".into(),
        },
        contract_sha256: "test".into(),
        dylink: super::super::module_graph_registry::GraphDylinkContract {
            first: true,
            sha256: "test".into(),
            weak_imports: vec![],
        },
        exports: vec![],
        imports: vec![super::super::module_graph_registry::GraphModuleImport {
            index: 0,
            module: "convex".into(),
            name: "convex_developer_error".into(),
            r#type: super::super::module_graph_registry::GraphExternalType {
                canonical,
                kind: "func".into(),
                sha256: type_sha256.clone(),
            },
        }],
    };
    let providers = vec![GraphModuleProvider {
        consumer: "base".into(),
        import_index: 0,
        imported_module: "convex".into(),
        imported_name: "convex_developer_error".into(),
        provider: "host".into(),
        provider_export: None,
        type_sha256,
        weak: false,
    }];

    let error = deserialize_authenticated_graph_module(&engine, &aot, &contract, &providers)
        .expect_err("stale host ABI must fail before graph instantiation");
    let report = format!("{error:#}");
    assert!(report.contains(
        "authenticated module graph host ABI differs from the active runtime for \
         convex::convex_developer_error"
    ));
    assert!(
        report.contains("authenticated module graph host import has an incompatible function type")
    );
    Ok(())
}

fn validate_external_graph_module_contract(
    module: &Module,
    contract: &GraphModuleContract,
    providers: &[GraphModuleProvider],
) -> anyhow::Result<()> {
    let actual_imports = module.imports().collect::<Vec<_>>();
    anyhow::ensure!(
        actual_imports.len() == contract.imports.len(),
        "module graph AOT import count differs from its authenticated contract"
    );
    for (index, (actual, expected)) in actual_imports.iter().zip(&contract.imports).enumerate() {
        let canonical = canonical_graph_extern_type(&actual.ty())?;
        anyhow::ensure!(
            expected.index == index
                && actual.module() == expected.module
                && actual.name() == expected.name
                && canonical == expected.r#type.canonical
                && format!("{:x}", Sha256::digest(canonical.as_bytes())) == expected.r#type.sha256,
            "module graph AOT import differs from its authenticated ordered contract"
        );
    }
    for provider in providers
        .iter()
        .filter(|provider| provider.provider == "host")
    {
        let expected = contract.imports.get(provider.import_index).context(
            "authenticated module graph host provider refers to an unavailable ordered import",
        )?;
        anyhow::ensure!(
            provider.imported_module == expected.module
                && provider.imported_name == expected.name
                && provider.type_sha256 == expected.r#type.sha256,
            "authenticated module graph host provider differs from its ordered import"
        );
        let actual = actual_imports.get(provider.import_index).context(
            "authenticated module graph host provider refers to an unavailable AOT import",
        )?;
        let (parameters, results) = generated_host_import_signature(
            provider.imported_module.as_str(),
            provider.imported_name.as_str(),
        )
        .with_context(|| {
            format!(
                "authenticated module graph host ABI is unsupported by the active runtime for \
                 {}::{}",
                provider.imported_module, provider.imported_name
            )
        })?;
        super::module_contract::validate_function_type(
            &actual.ty(),
            parameters,
            results,
            "authenticated module graph host import",
        )
        .with_context(|| {
            format!(
                "authenticated module graph host ABI differs from the active runtime for {}::{}",
                provider.imported_module, provider.imported_name
            )
        })?;
    }
    let actual_exports = module.exports().collect::<Vec<_>>();
    anyhow::ensure!(
        actual_exports.len() == contract.exports.len(),
        "module graph AOT export count differs from its authenticated contract"
    );
    for (index, (actual, expected)) in actual_exports.iter().zip(&contract.exports).enumerate() {
        let canonical = canonical_graph_extern_type(&actual.ty())?;
        anyhow::ensure!(
            expected.index == index
                && actual.name() == expected.name
                && canonical == expected.r#type.canonical
                && format!("{:x}", Sha256::digest(canonical.as_bytes())) == expected.r#type.sha256,
            "module graph AOT export {index} differs from its authenticated ordered contract: \
             actual name={:?} type={canonical:?}, expected name={:?} type={:?}",
            actual.name(),
            expected.name,
            expected.r#type.canonical,
        );
    }
    Ok(())
}

pub(super) fn deserialize_authenticated_graph_module(
    engine: &Engine,
    aot: &[u8],
    contract: &GraphModuleContract,
    providers: &[GraphModuleProvider],
) -> anyhow::Result<Module> {
    anyhow::ensure!(
        Engine::detect_precompiled(aot) == Some(Precompiled::Module),
        "authenticated module graph AOT artifact is not a Wasmtime core module"
    );
    let module = unsafe {
        // The registry authenticates the graph, engine, Core Wasm, AOT, and exact
        // ordered import/export contract. This read rechecks the artifact
        // identity immediately before unsafe Wasmtime deserialization.
        Module::deserialize(engine, aot)
    }
    .map_err(wasmtime_anyhow)?;
    validate_external_graph_module_contract(&module, contract, providers)?;
    Ok(module)
}

fn precompile_authenticated_graph_module(
    engine: &Engine,
    serialized_module_snapshot_directory: &Path,
    module: &AuthenticatedModuleGraphExecutionModule<'_>,
) -> anyhow::Result<Module> {
    let core_wasm = read_authenticated_graph_artifact(
        module.core_wasm_path(),
        module.core_wasm_size(),
        module.core_wasm_sha256(),
    )?;
    let aot = engine
        .precompile_module(&core_wasm)
        .map_err(wasmtime_anyhow)?;
    anyhow::ensure!(
        u64::try_from(aot.len())? == module.aot_size()
            && format!("{:x}", Sha256::digest(&aot)) == module.aot_sha256(),
        "destination-compiled module graph AOT differs from its authenticated identity"
    );
    anyhow::ensure!(
        Engine::detect_precompiled(&aot) == Some(Precompiled::Module),
        "destination-compiled module graph AOT is not a Wasmtime core module"
    );
    let snapshot = snapshot_serialized_module_bytes(&aot, serialized_module_snapshot_directory)?;
    drop(aot);
    let compiled =
        unsafe { Module::deserialize_open_file(engine, snapshot) }.map_err(wasmtime_anyhow)?;
    validate_external_graph_module_contract(&compiled, module.contract(), module.providers())?;
    Ok(compiled)
}

fn cache_authenticated_graph_shared_module(
    controller: &Arc<GeneratedMemoryController>,
    engine: &Arc<Engine>,
    engine_compatibility_sha256: &str,
    serialized_module_snapshot_directory: &Path,
    module: &AuthenticatedModuleGraphExecutionModule<'_>,
    aot_resolution: AuthenticatedGraphAotResolution,
) -> anyhow::Result<Arc<GeneratedSharedAotModule>> {
    let identity = GeneratedAotModuleIdentity {
        engine_compatibility_sha256: engine_compatibility_sha256.to_owned(),
        serialized_module_sha256: module.aot_sha256().to_owned(),
    };
    {
        let mut cache = GENERATED_ROUTED_MODULES.lock();
        if let Some(cached) = cache.get_shared_aot_module(&identity) {
            resize_shared_aot_charge(
                &mut cache,
                &cached,
                module.core_wasm_size(),
                module.aot_size(),
            )?;
            validate_external_graph_module_contract(
                &cached.module,
                module.contract(),
                module.providers(),
            )
            .with_context(|| {
                format!(
                    "validate cached {} module graph AOT contract",
                    module.role()
                )
            })?;
            return Ok(cached);
        }
    }
    if !module.aot_payload_available() {
        match aot_resolution {
            AuthenticatedGraphAotResolution::PreloadedOnly => {
                anyhow::bail!(
                    "missing {} module graph AOT was not reconstructed before generation readiness",
                    module.role()
                );
            },
            AuthenticatedGraphAotResolution::ReconstructBeforeReadiness => {},
        }
    }
    // Different graph routes can reach the same immutable shared member at
    // once. Serialize only that identity's cold load so one admitted
    // Core-plus-two-AOT peak produces one deserialization and one cache entry.
    let load_lock = generated_shared_aot_load_lock(&identity);
    let _load_guard = load_lock.lock();
    let initial_fixed_bytes =
        generated_in_memory_module_initial_fixed_bytes(module.core_wasm_size(), module.aot_size())?;
    let mut charge = {
        let mut cache = GENERATED_ROUTED_MODULES.lock();
        if let Some(cached) = cache.get_shared_aot_module(&identity) {
            resize_shared_aot_charge(
                &mut cache,
                &cached,
                module.core_wasm_size(),
                module.aot_size(),
            )?;
            validate_external_graph_module_contract(
                &cached.module,
                module.contract(),
                module.providers(),
            )
            .with_context(|| {
                format!(
                    "validate cached {} module graph AOT contract",
                    module.role()
                )
            })?;
            return Ok(cached);
        }
        // Module::deserialize copies the authenticated Vec into Wasmtime's code
        // mapping. Reserve both AOT copies before the read so the complete cold
        // load remains inside aggregate admission.
        cache
            .reserve(controller, initial_fixed_bytes)
            .map_err(module_memory_admission_error)?
    };
    let (deserialized, retained_aot) = if module.aot_payload_available() {
        authenticate_graph_artifact(
            module.core_wasm_path(),
            module.core_wasm_size(),
            module.core_wasm_sha256(),
        )?;
        let aot = read_authenticated_graph_artifact(
            module.aot_path(),
            module.aot_size(),
            module.aot_sha256(),
        )?;
        let deserialized = deserialize_authenticated_graph_module(
            engine,
            &aot,
            module.contract(),
            module.providers(),
        )
        .with_context(|| format!("deserialize {} module graph AOT", module.role()))?;
        (deserialized, Some(aot))
    } else {
        let deserialized = precompile_authenticated_graph_module(
            engine,
            serialized_module_snapshot_directory,
            module,
        )
        .with_context(|| format!("precompile {} module graph AOT", module.role()))?;
        (deserialized, None)
    };
    let fixed_module_bytes =
        generated_module_fixed_bytes(module.core_wasm_size(), module.aot_size(), &deserialized)?;
    drop(retained_aot);
    let mut cache = GENERATED_ROUTED_MODULES.lock();
    if let Some(cached) = cache.get_shared_aot_module(&identity) {
        resize_shared_aot_charge(
            &mut cache,
            &cached,
            module.core_wasm_size(),
            module.aot_size(),
        )?;
        validate_external_graph_module_contract(
            &cached.module,
            module.contract(),
            module.providers(),
        )
        .with_context(|| {
            format!(
                "validate cached {} module graph AOT contract",
                module.role()
            )
        })?;
        return Ok(cached);
    }
    cache
        .resize(&mut charge, fixed_module_bytes)
        .map_err(module_memory_admission_error)?;
    let shared = Arc::new(GeneratedSharedAotModule {
        identity,
        module: Arc::new(deserialized),
        module_charge: parking_lot::Mutex::new(None),
    });
    Ok(cache.insert_shared_aot_module(shared, charge))
}

#[cfg(test)]
#[test]
fn graph_leaf_cache_hit_reuses_authenticated_module_and_checks_contract() -> anyhow::Result<()> {
    let engine = Arc::new(new_generated_engine()?);
    super::super::module_graph_registry::tests::with_leaf_module_cache_test_records(
        &engine,
        |module, changed_contract, reconstructible| {
            let snapshots = tempfile::tempdir()?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;

                std::fs::set_permissions(snapshots.path(), std::fs::Permissions::from_mode(0o700))?;
            }
            let controller = generated_module_cache_test_controller(8 << 20, 12 << 20)?;
            let first = cache_authenticated_graph_shared_module(
                &controller,
                &engine,
                GENERATED_ENGINE_COMPATIBILITY_SHA256,
                snapshots.path(),
                &module,
                AuthenticatedGraphAotResolution::PreloadedOnly,
            )?;
            let charged_bytes = first
                .module_charge
                .lock()
                .as_ref()
                .context("cached leaf lost its charge")?
                .fixed_bytes();
            assert!(charged_bytes > 0);
            assert_eq!(Arc::strong_count(&controller), 2);
            for (path, withheld) in [
                (module.core_wasm_path(), "withheld-core.wasm"),
                (module.aot_path(), "withheld-aot.cwasm"),
            ] {
                std::fs::rename(path, path.with_file_name(withheld))?;
            }
            let second = cache_authenticated_graph_shared_module(
                &controller,
                &engine,
                GENERATED_ENGINE_COMPATIBILITY_SHA256,
                snapshots.path(),
                &module,
                AuthenticatedGraphAotResolution::PreloadedOnly,
            )?;
            assert!(Arc::ptr_eq(&first, &second));
            assert!(Arc::ptr_eq(&first.module, &second.module));
            let error = cache_authenticated_graph_shared_module(
                &controller,
                &engine,
                GENERATED_ENGINE_COMPATIBILITY_SHA256,
                snapshots.path(),
                &changed_contract,
                AuthenticatedGraphAotResolution::PreloadedOnly,
            )
            .err()
            .context("cached leaf accepted a changed export contract")?;
            assert!(format!("{error:#}").contains("export count differs"));
            assert_eq!(
                second
                    .module_charge
                    .lock()
                    .as_ref()
                    .context("reused leaf lost its charge")?
                    .fixed_bytes(),
                charged_bytes,
            );
            assert_eq!(Arc::strong_count(&controller), 2);
            let identity = first.identity.clone();
            let weak = Arc::downgrade(&first);
            drop(first);
            drop(second);
            let removed = GENERATED_ROUTED_MODULES
                .lock()
                .remove_shared_aot_module(&identity, "shared_aot_cache_evicted_leaf_reuse_test")
                .context("cached leaf disappeared before cleanup")?;
            assert_eq!(Arc::strong_count(&removed), 1);
            drop(removed);
            assert!(weak.upgrade().is_none());
            assert_eq!(Arc::strong_count(&controller), 1);
            // Once evicted, the same retained metadata must authenticate both
            // physical payloads again before admitting a deserialized module.
            for (path, withheld) in [
                (module.core_wasm_path(), "withheld-core.wasm"),
                (module.aot_path(), "withheld-aot.cwasm"),
            ] {
                let missing = cache_authenticated_graph_shared_module(
                    &controller,
                    &engine,
                    GENERATED_ENGINE_COMPATIBILITY_SHA256,
                    snapshots.path(),
                    &module,
                    AuthenticatedGraphAotResolution::PreloadedOnly,
                )
                .err()
                .context("cold module load accepted a missing artifact")?;
                assert!(format!("{missing:#}").contains("open authenticated module graph artifact"));
                std::fs::rename(path.with_file_name(withheld), path)?;
                let original = std::fs::read(path)?;
                let mut corrupted = original.clone();
                *corrupted.first_mut().context("empty artifact fixture")? ^= 1;
                std::fs::write(path, corrupted)?;
                let changed = cache_authenticated_graph_shared_module(
                    &controller,
                    &engine,
                    GENERATED_ENGINE_COMPATIBILITY_SHA256,
                    snapshots.path(),
                    &module,
                    AuthenticatedGraphAotResolution::PreloadedOnly,
                )
                .err()
                .context("cold module load accepted artifact corruption")?;
                assert!(format!("{changed:#}")
                    .contains("module graph artifact changed after registry authentication"));
                std::fs::write(path, original)?;
                assert!(GENERATED_ROUTED_MODULES
                    .lock()
                    .get_shared_aot_module(&identity)
                    .is_none());
                assert_eq!(Arc::strong_count(&controller), 1);
            }
            assert!(!reconstructible.aot_path().exists());
            let missing = cache_authenticated_graph_shared_module(
                &controller,
                &engine,
                GENERATED_ENGINE_COMPATIBILITY_SHA256,
                snapshots.path(),
                &reconstructible,
                AuthenticatedGraphAotResolution::PreloadedOnly,
            )
            .err()
            .context("request-time load reconstructed a missing AOT")?;
            assert!(format!("{missing:#}")
                .contains("was not reconstructed before generation readiness"));
            let reconstructed = cache_authenticated_graph_shared_module(
                &controller,
                &engine,
                GENERATED_ENGINE_COMPATIBILITY_SHA256,
                snapshots.path(),
                &reconstructible,
                AuthenticatedGraphAotResolution::ReconstructBeforeReadiness,
            )?;
            assert_eq!(reconstructed.identity, identity);
            let retained = cache_authenticated_graph_shared_module(
                &controller,
                &engine,
                GENERATED_ENGINE_COMPATIBILITY_SHA256,
                snapshots.path(),
                &reconstructible,
                AuthenticatedGraphAotResolution::PreloadedOnly,
            )?;
            assert!(Arc::ptr_eq(&reconstructed, &retained));
            let removed = GENERATED_ROUTED_MODULES
                .lock()
                .remove_shared_aot_module(&identity, "shared_aot_reconstructed_test_cleanup")
                .context("reconstructed module disappeared before cleanup")?;
            drop(reconstructed);
            drop(retained);
            drop(removed);
            assert_eq!(Arc::strong_count(&controller), 1);
            Ok(())
        },
    )
}

struct PreparedAuthenticatedGraphSharedModules {
    base: Arc<GeneratedSharedAotModule>,
    shared: Vec<Arc<GeneratedSharedAotModule>>,
    host_convex_imports: BTreeSet<String>,
}

fn prepare_authenticated_graph_shared_modules(
    controller: &Arc<GeneratedMemoryController>,
    engine: &Arc<Engine>,
    serialized_module_snapshot_directory: &Path,
    graph: &AuthenticatedModuleGraphExecution<'_>,
    aot_resolution: AuthenticatedGraphAotResolution,
) -> anyhow::Result<PreparedAuthenticatedGraphSharedModules> {
    let _timer = GatePhaseTimer::new(GATE_PHASE_AUTHENTICATED_GRAPH_SHARED_AOT_RESOLUTION);
    let (base_record, modules) = graph
        .modules()
        .split_first()
        .context("authenticated module graph lost its base module")?;
    let (_, shared_records) = modules
        .split_last()
        .context("authenticated module graph lost its leaf module")?;
    let base = cache_authenticated_graph_shared_module(
        controller,
        engine,
        graph.engine_compatibility_sha256(),
        serialized_module_snapshot_directory,
        base_record,
        aot_resolution,
    )?;
    let shared = shared_records
        .iter()
        .map(|module| {
            cache_authenticated_graph_shared_module(
                controller,
                engine,
                graph.engine_compatibility_sha256(),
                serialized_module_snapshot_directory,
                module,
                aot_resolution,
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let host_convex_imports = base_record
        .providers()
        .iter()
        .filter(|provider| provider.provider == "host" && provider.imported_module == "convex")
        .map(|provider| provider.imported_name.clone())
        .collect::<BTreeSet<_>>();
    let base_host_contract = base_record
        .providers()
        .iter()
        .filter(|provider| provider.provider == "host")
        .map(|provider| {
            (
                provider.imported_module.as_str(),
                provider.imported_name.as_str(),
                provider.type_sha256.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    let host_abi_contract = graph
        .host_abi()
        .imports
        .iter()
        .map(|imported| {
            (
                imported.module.as_str(),
                imported.name.as_str(),
                imported.type_sha256.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        base_host_contract == host_abi_contract,
        "base module host imports differ from the authenticated closed host ABI"
    );
    Ok(PreparedAuthenticatedGraphSharedModules {
        base,
        shared,
        host_convex_imports,
    })
}

fn build_authenticated_graph_routed_module(
    engine: Arc<Engine>,
    generation: Arc<DeploymentGeneration>,
    graph: AuthenticatedModuleGraphExecution<'_>,
    route_material: AuthenticatedModuleGraphRouteMaterial,
    route_identity: GeneratedRouteIdentity,
    shared: PreparedAuthenticatedGraphSharedModules,
    leaf: Arc<GeneratedSharedAotModule>,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    anyhow::ensure!(
        graph.engine_compatibility_sha256() == GENERATED_ENGINE_COMPATIBILITY_SHA256,
        "module graph AOT engine identity differs from the active runtime"
    );
    anyhow::ensure!(
        route_material.package_key.as_str()
            == match &route_identity {
                GeneratedRouteIdentity::DeploymentExport { package_key, .. } => {
                    package_key.as_str()
                },
                GeneratedRouteIdentity::Singleton { .. } => {
                    anyhow::bail!("authenticated module graph used a singleton route identity")
                },
            },
        "module graph route material differs from its deployment identity"
    );
    let (base_record, modules) = graph
        .modules()
        .split_first()
        .context("authenticated module graph lost its base module")?;
    let (leaf_record, shared_records) = modules
        .split_last()
        .context("authenticated module graph lost its leaf module")?;
    validate_generated_execution_contract(
        &shared.base.module,
        &route_material.execution,
        &route_material.permitted_conditional_convex_imports,
        route_material.identity.requires_entry_selector(),
        &shared.host_convex_imports,
        "convex_wasm_graph_select_entry",
    )?;
    let pool_identity = GeneratedPoolIdentity::Graph(GeneratedGraphPoolIdentity {
        generation_sha256: generation.generation_sha256.clone(),
        graph_sha256: graph.graph_sha256().to_owned(),
        engine_compatibility_sha256: graph.engine_compatibility_sha256().to_owned(),
        base: GeneratedAotModuleIdentity {
            engine_compatibility_sha256: graph.engine_compatibility_sha256().to_owned(),
            serialized_module_sha256: base_record.aot_sha256().to_owned(),
        },
        shared: shared_records
            .iter()
            .map(|module| GeneratedAotModuleIdentity {
                engine_compatibility_sha256: graph.engine_compatibility_sha256().to_owned(),
                serialized_module_sha256: module.aot_sha256().to_owned(),
            })
            .collect(),
        leaf: GeneratedAotModuleIdentity {
            engine_compatibility_sha256: graph.engine_compatibility_sha256().to_owned(),
            serialized_module_sha256: leaf_record.aot_sha256().to_owned(),
        },
    });
    let module_contract = |module: &AuthenticatedModuleGraphExecutionModule<'_>| {
        GeneratedEmscriptenGraphModuleContract {
            contract: module.contract().clone(),
            layout: module.layout().clone(),
            providers: module.providers().to_vec(),
        }
    };
    let module = Arc::clone(&leaf.module);
    Ok(Arc::new(GeneratedRoutedModule {
        emscripten_graph: Some(GeneratedEmscriptenGraphModules {
            base: shared.base,
            shared: shared.shared,
            leaf,
            initialization: graph.initialization().clone(),
            modules: graph.modules().iter().map(module_contract).collect(),
        }),
        engine,
        entry_selector: Some(route_material.entry_selector),
        // Compiled members own their shared charges; this charge covers the
        // generation-specific route record without charging the leaf again.
        fixed_module_bytes: std::mem::size_of::<GeneratedRoutedModule>(),
        generation: Some(generation),
        module_charge: OnceLock::new(),
        module,
        manifest: Arc::new(route_material.execution),
        package_identity: Arc::new(route_material.identity),
        permitted_conditional_convex_imports: route_material.permitted_conditional_convex_imports,
        graph: None,
        pool_identity,
        route_identity,
    }))
}

pub(super) fn cache_authenticated_graph_routed_module(
    controller: &Arc<GeneratedMemoryController>,
    engine: Arc<Engine>,
    serialized_module_snapshot_directory: &Path,
    generation: Arc<DeploymentGeneration>,
    graph: AuthenticatedModuleGraphExecution<'_>,
    route_material: AuthenticatedModuleGraphRouteMaterial,
    route_identity: GeneratedRouteIdentity,
    aot_resolution: AuthenticatedGraphAotResolution,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    let leaf = graph
        .modules()
        .last()
        .context("authenticated module graph lost its leaf module")?;
    if !generation.is_retired() {
        if let Some(cached) = GENERATED_ROUTED_MODULES.lock().get(&route_identity) {
            if cached_routed_module_matches_generation(&cached, Some(generation.as_ref()))? {
                return Ok(cached);
            }
        }
    }
    // Resolve all compiled members before route admission takes the cache lock.
    // Their artifact identities can be shared across generation-specific routes.
    let shared = prepare_authenticated_graph_shared_modules(
        controller,
        &engine,
        serialized_module_snapshot_directory,
        &graph,
        aot_resolution,
    )?;
    let leaf = {
        let _timer = GatePhaseTimer::new(GATE_PHASE_ROUTE_LEAF_AOT_RESOLUTION);
        cache_authenticated_graph_shared_module(
            controller,
            &engine,
            graph.engine_compatibility_sha256(),
            serialized_module_snapshot_directory,
            leaf,
            aot_resolution,
        )?
    };
    #[cfg(any(test, feature = "testing"))]
    record_generated_route_preflight_stage(
        StaticHermesGeneratedRoutePreflightStage::RoutedSharedAotPrepared,
    );
    let build_route_identity = route_identity.clone();
    let build_generation = Arc::clone(&generation);
    cache_generated_routed_module_with_generation(
        &GENERATED_ROUTED_MODULES,
        controller,
        route_identity,
        std::mem::size_of::<GeneratedRoutedModule>(),
        Some(&generation),
        move || {
            build_authenticated_graph_routed_module(
                engine,
                build_generation,
                graph,
                route_material,
                build_route_identity,
                shared,
                leaf,
            )
        },
    )
}

struct PreparedGeneratedGraph {
    dependencies: Vec<GeneratedGraphDependency>,
    host_convex_imports: BTreeSet<String>,
    leaf: ValidatedCapabilityGraphModule,
    layout: CapabilityGraphLayout,
    pool_identity: GeneratedGraphPoolIdentity,
}

fn prepare_generated_graph(
    controller: &Arc<GeneratedMemoryController>,
    engine: &Arc<Engine>,
    generation: Option<&DeploymentGeneration>,
    graph: ValidatedCapabilityGraph,
) -> anyhow::Result<PreparedGeneratedGraph> {
    let generation = generation.context(
        "generated graph package must be loaded from an authenticated registry generation",
    )?;
    let base_identity =
        generated_aot_module_identity(&graph.engine_compatibility_sha256, &graph.base.module);
    let shared_identities = graph
        .shared
        .iter()
        .map(|dependency| {
            generated_aot_module_identity(&graph.engine_compatibility_sha256, &dependency.module)
        })
        .collect::<Vec<_>>();
    let leaf_identity =
        generated_aot_module_identity(&graph.engine_compatibility_sha256, &graph.leaf.module);
    let pool_identity = GeneratedGraphPoolIdentity {
        generation_sha256: generation.generation_sha256.clone(),
        graph_sha256: graph.graph_sha256,
        engine_compatibility_sha256: graph.engine_compatibility_sha256.clone(),
        base: base_identity,
        shared: shared_identities,
        leaf: leaf_identity,
    };
    let mut dependencies = Vec::with_capacity(graph.shared.len() + 1);
    let mut host_convex_imports = BTreeSet::new();
    for dependency in std::iter::once(graph.base).chain(graph.shared) {
        let module_id = dependency.module.module_id.clone();
        let provider_namespace = dependency.module.provider_namespace.clone();
        let (module, imports) = cache_generated_shared_aot_module(
            controller,
            engine,
            &graph.engine_compatibility_sha256,
            dependency,
        )?;
        host_convex_imports.extend(imports);
        dependencies.push(GeneratedGraphDependency {
            module_id,
            provider_namespace,
            module,
        });
    }
    Ok(PreparedGeneratedGraph {
        dependencies,
        host_convex_imports,
        leaf: graph.leaf.module,
        layout: graph.layout,
        pool_identity,
    })
}

fn generated_aot_module_identity(
    engine_compatibility_sha256: &str,
    module: &ValidatedCapabilityGraphModule,
) -> GeneratedAotModuleIdentity {
    GeneratedAotModuleIdentity {
        engine_compatibility_sha256: engine_compatibility_sha256.to_owned(),
        serialized_module_sha256: module.serialized_module_sha256.clone(),
    }
}

fn cache_generated_shared_aot_module(
    controller: &Arc<GeneratedMemoryController>,
    engine: &Arc<Engine>,
    engine_compatibility_sha256: &str,
    dependency: ValidatedCapabilityGraphDependency,
) -> anyhow::Result<(Arc<GeneratedSharedAotModule>, BTreeSet<String>)> {
    let identity = generated_aot_module_identity(engine_compatibility_sha256, &dependency.module);
    {
        let mut cache = GENERATED_ROUTED_MODULES.lock();
        if let Some(module) = cache.get_shared_aot_module(&identity) {
            resize_generated_shared_aot_charge(&mut cache, &module, &dependency.module)?;
            let imports = validate_generated_graph_module_contract(
                &module.module,
                &dependency.module.contract,
            )?;
            return Ok((module, imports));
        }
    }
    anyhow::ensure!(
        detect_precompiled_snapshot_file(&dependency.serialized_module_snapshot)?
            == Some(Precompiled::Module),
        "generated graph dependency AOT artifact is not a Wasmtime core module"
    );
    let load_lock = generated_shared_aot_load_lock(&identity);
    let _load_guard = load_lock.lock();
    let initial_fixed_bytes = generated_module_initial_fixed_bytes(
        dependency.module.core_wasm_bytes,
        dependency.module.serialized_module_bytes,
    )?;
    let mut cache = GENERATED_ROUTED_MODULES.lock();
    if let Some(module) = cache.get_shared_aot_module(&identity) {
        resize_generated_shared_aot_charge(&mut cache, &module, &dependency.module)?;
        let imports =
            validate_generated_graph_module_contract(&module.module, &dependency.module.contract)?;
        return Ok((module, imports));
    }
    let mut charge = cache
        .reserve(controller, initial_fixed_bytes)
        .map_err(module_memory_admission_error)?;
    let module =
        unsafe { Module::deserialize_open_file(engine, dependency.serialized_module_snapshot) }
            .map_err(wasmtime_anyhow)?;
    let imports = validate_generated_graph_module_contract(&module, &dependency.module.contract)?;
    let fixed_module_bytes = generated_module_fixed_bytes(
        dependency.module.core_wasm_bytes,
        dependency.module.serialized_module_bytes,
        &module,
    )?;
    cache
        .resize(&mut charge, fixed_module_bytes)
        .map_err(module_memory_admission_error)?;
    let module = Arc::new(GeneratedSharedAotModule {
        identity,
        module: Arc::new(module),
        module_charge: parking_lot::Mutex::new(None),
    });
    Ok((cache.insert_shared_aot_module(module, charge), imports))
}

fn resize_generated_shared_aot_charge(
    cache: &mut GeneratedRoutedModuleCache,
    cached: &GeneratedSharedAotModule,
    requested: &ValidatedCapabilityGraphModule,
) -> anyhow::Result<()> {
    resize_shared_aot_charge(
        cache,
        cached,
        requested.core_wasm_bytes,
        requested.serialized_module_bytes,
    )
}

pub(super) fn resize_shared_aot_charge(
    cache: &mut GeneratedRoutedModuleCache,
    cached: &GeneratedSharedAotModule,
    core_wasm_bytes: u64,
    serialized_module_bytes: u64,
) -> anyhow::Result<()> {
    let required =
        generated_module_fixed_bytes(core_wasm_bytes, serialized_module_bytes, &cached.module)?;
    let mut charge = cached.module_charge.lock();
    let charge = charge
        .as_mut()
        .context("generated shared AOT cache entry has no memory charge")?;
    if charge.fixed_bytes() < required {
        cache
            .resize(charge, required)
            .map_err(module_memory_admission_error)?;
    }
    Ok(())
}

fn build_generated_routed_module(
    package: ValidatedWasmUdfPackage,
    prepared_graph: Option<PreparedGeneratedGraph>,
    route_identity: GeneratedRouteIdentity,
    engine: Arc<Engine>,
    generation: Option<Arc<DeploymentGeneration>>,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    anyhow::ensure!(
        package
            .execution
            .imported_operations()
            .iter()
            .all(|operation| {
                matches!(
                    operation.operation(),
                    ImportedOperationDescriptor::Sha256
                        | ImportedOperationDescriptor::AuthenticationGetUserIdentity {}
                        | ImportedOperationDescriptor::HostSecretVerify { .. }
                        | ImportedOperationDescriptor::FunctionHandleCreate {}
                        | ImportedOperationDescriptor::DatabaseNormalizeId { .. }
                        | ImportedOperationDescriptor::DatabaseGet { .. }
                        | ImportedOperationDescriptor::DatabaseIndexQuery { .. }
                        | ImportedOperationDescriptor::DatabaseInsert { .. }
                        | ImportedOperationDescriptor::DatabasePatch { .. }
                        | ImportedOperationDescriptor::DatabaseReplace { .. }
                        | ImportedOperationDescriptor::DatabaseDelete { .. }
                        | ImportedOperationDescriptor::SchedulerRunAfter { .. }
                        | ImportedOperationDescriptor::SchedulerRunAt { .. }
                )
            }),
        "generated package declares an operation unsupported by this runtime"
    );
    anyhow::ensure!(
        detect_precompiled_snapshot_file(&package.serialized_module_snapshot)?
            == Some(Precompiled::Module),
        "generated package AOT artifact is not a Wasmtime core module"
    );
    // Package validation streams the authenticated module into an anonymous
    // snapshot before handing its only descriptor to Wasmtime.
    let module =
        unsafe { Module::deserialize_open_file(&engine, package.serialized_module_snapshot) }
            .map_err(wasmtime_anyhow)?;
    let (graph, pool_identity) = match prepared_graph {
        Some(mut graph) => {
            let leaf_imports =
                validate_generated_graph_module_contract(&module, &graph.leaf.contract)?;
            graph.host_convex_imports.extend(leaf_imports);
            validate_generated_execution_contract(
                &module,
                &package.execution,
                &package.permitted_conditional_convex_imports,
                package.identity.requires_entry_selector(),
                &graph.host_convex_imports,
                "convex_wasm_select_entry",
            )?;
            let pool_identity = GeneratedPoolIdentity::Graph(graph.pool_identity);
            (
                Some(GeneratedGraphModules {
                    dependencies: graph.dependencies,
                    layout: graph.layout,
                }),
                pool_identity,
            )
        },
        None => {
            validate_generated_module_contract_with_imports(
                &module,
                &package.execution,
                &package.permitted_conditional_convex_imports,
                package.identity.requires_entry_selector(),
            )?;
            (None, GeneratedPoolIdentity::Route(route_identity.clone()))
        },
    };
    let fixed_module_bytes = generated_module_fixed_bytes(
        package.core_wasm_bytes,
        package.serialized_module_bytes,
        &module,
    )?;
    let module = Arc::new(GeneratedRoutedModule {
        emscripten_graph: None,
        engine,
        entry_selector: package.entry_selector,
        fixed_module_bytes,
        generation,
        module_charge: OnceLock::new(),
        module: Arc::new(module),
        manifest: Arc::new(package.execution),
        package_identity: Arc::new(package.identity),
        permitted_conditional_convex_imports: package.permitted_conditional_convex_imports,
        graph,
        pool_identity,
        route_identity,
    });
    Ok(module)
}

fn generated_module_artifact_bytes(
    core_wasm_bytes: u64,
    serialized_module_bytes: u64,
) -> anyhow::Result<(usize, usize)> {
    Ok((
        usize::try_from(core_wasm_bytes)
            .context("generated Wasm Core-Wasm size exceeds the host address space")?,
        usize::try_from(serialized_module_bytes)
            .context("generated Wasm AOT size exceeds the host address space")?,
    ))
}

pub(super) fn generated_module_initial_fixed_bytes(
    core_wasm_bytes: u64,
    serialized_module_bytes: u64,
) -> anyhow::Result<usize> {
    let (core_wasm_bytes, serialized_module_bytes) =
        generated_module_artifact_bytes(core_wasm_bytes, serialized_module_bytes)?;
    // deserialize_open_file maps one authenticated AOT image and package
    // validation retains only its file descriptor.
    core_wasm_bytes
        .checked_add(serialized_module_bytes)
        .context("generated Wasm fixed module-byte estimate overflow")
}

pub(super) fn generated_in_memory_module_initial_fixed_bytes(
    core_wasm_bytes: u64,
    serialized_module_bytes: u64,
) -> anyhow::Result<usize> {
    let (core_wasm_bytes, serialized_module_bytes) =
        generated_module_artifact_bytes(core_wasm_bytes, serialized_module_bytes)?;
    core_wasm_bytes
        .checked_add(
            serialized_module_bytes
                .checked_mul(2)
                .context("generated Wasm in-memory AOT peak estimate overflow")?,
        )
        .context("generated Wasm in-memory module-byte estimate overflow")
}

pub(super) fn generated_module_fixed_bytes(
    core_wasm_bytes: u64,
    serialized_module_bytes: u64,
    module: &Module,
) -> anyhow::Result<usize> {
    let (core_wasm_bytes, serialized_module_bytes) =
        generated_module_artifact_bytes(core_wasm_bytes, serialized_module_bytes)?;
    let image_range = module.image_range();
    let module_image_bytes = (image_range.end as usize)
        .checked_sub(image_range.start as usize)
        .context("generated Wasm module image range is invalid")?;
    // image_range is Wasmtime's complete compilation-image mapping. Wasmtime
    // does not expose allocator RSS or the heap retained by parsed module/type
    // metadata, so the authenticated Core-Wasm size is a conservative metadata
    // allowance. The authenticated AOT size remains a floor for the image term.
    module_image_bytes
        .max(serialized_module_bytes)
        .checked_add(core_wasm_bytes)
        .context("generated Wasm fixed module-byte estimate overflow")
}

pub(super) fn module_memory_admission_error(error: ModuleMemoryAdmissionError) -> anyhow::Error {
    error.into()
}

pub(super) fn cache_generated_routed_module_with(
    cache: &parking_lot::Mutex<GeneratedRoutedModuleCache>,
    controller: &Arc<GeneratedMemoryController>,
    route_identity: GeneratedRouteIdentity,
    initial_fixed_bytes: usize,
    build: impl FnOnce() -> anyhow::Result<Arc<GeneratedRoutedModule>>,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    cache_generated_routed_module_with_generation(
        cache,
        controller,
        route_identity,
        initial_fixed_bytes,
        None,
        build,
    )
}

pub(super) fn cache_generated_routed_module_with_generation(
    cache: &parking_lot::Mutex<GeneratedRoutedModuleCache>,
    controller: &Arc<GeneratedMemoryController>,
    route_identity: GeneratedRouteIdentity,
    initial_fixed_bytes: usize,
    generation: Option<&DeploymentGeneration>,
    build: impl FnOnce() -> anyhow::Result<Arc<GeneratedRoutedModule>>,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    let mut cache = cache.lock();
    if !generation.is_some_and(DeploymentGeneration::is_retired) {
        if let Some(routed) = cache.get(&route_identity) {
            if cached_routed_module_matches_generation(&routed, generation)? {
                return Ok(routed);
            }
        }
    }
    let mut charge = cache
        .reserve(controller, initial_fixed_bytes)
        .map_err(module_memory_admission_error)?;
    let routed = build()?;
    anyhow::ensure!(
        routed.route_identity == route_identity,
        "generated Wasm module cache load returned a different route identity"
    );
    cache
        .resize(&mut charge, routed.fixed_module_bytes)
        .map_err(module_memory_admission_error)?;
    if generation.is_some_and(DeploymentGeneration::is_retired) {
        cache.retain_charge_without_caching(&routed, charge);
        Ok(routed)
    } else {
        Ok(cache.insert(routed, charge))
    }
}

pub(super) fn cache_generated_routed_module(
    controller: &Arc<GeneratedMemoryController>,
    mut package: ValidatedWasmUdfPackage,
    route_identity: GeneratedRouteIdentity,
    engine: Arc<Engine>,
    generation: Option<Arc<DeploymentGeneration>>,
) -> anyhow::Result<Arc<GeneratedRoutedModule>> {
    if !generation
        .as_deref()
        .is_some_and(DeploymentGeneration::is_retired)
    {
        if let Some(routed) = GENERATED_ROUTED_MODULES.lock().get(&route_identity) {
            if cached_routed_module_matches_generation(&routed, generation.as_deref())? {
                return Ok(routed);
            }
        }
    }
    let prepared_graph = package
        .graph
        .take()
        .map(|graph| prepare_generated_graph(controller, &engine, generation.as_deref(), graph))
        .transpose()?;
    let initial_fixed_bytes = generated_module_initial_fixed_bytes(
        package.core_wasm_bytes,
        package.serialized_module_bytes,
    )?;
    let build_route_identity = route_identity.clone();
    let build_generation = generation.clone();
    cache_generated_routed_module_with_generation(
        &GENERATED_ROUTED_MODULES,
        controller,
        route_identity,
        initial_fixed_bytes,
        generation.as_deref(),
        move || {
            build_generated_routed_module(
                package,
                prepared_graph,
                build_route_identity,
                engine,
                build_generation,
            )
        },
    )
}
