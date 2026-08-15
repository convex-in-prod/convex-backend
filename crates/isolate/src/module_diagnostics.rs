use std::{
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use parking_lot::Mutex;
use sync_types::CanonicalizedModulePath;

use crate::{
    execution_observation,
    metrics,
};

#[derive(Clone, Copy)]
pub enum ModuleRequestKind {
    Analysis,
    Runtime,
    Configuration,
}

impl ModuleRequestKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Analysis => "analysis",
            Self::Runtime => "runtime",
            Self::Configuration => "configuration",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ModulePhase {
    Registration,
    Compilation,
    Serialization,
    Instantiation,
    Evaluation,
    ExportInspection,
    Cleanup,
}

impl ModulePhase {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Registration => "registration",
            Self::Compilation => "compilation",
            Self::Serialization => "serialization",
            Self::Instantiation => "instantiation",
            Self::Evaluation => "evaluation",
            Self::ExportInspection => "export_inspection",
            Self::Cleanup => "cleanup",
        }
    }
}

#[derive(Clone)]
pub struct AnalysisDiagnostic(Arc<Mutex<AnalysisDiagnosticState>>);

struct AnalysisDiagnosticState {
    root: CanonicalizedModulePath,
    phase: ModulePhase,
    module: Option<String>,
    registration: Duration,
    compilation: Duration,
    evaluation: Duration,
    measured_cpu: Option<Duration>,
}

impl AnalysisDiagnostic {
    pub(crate) fn new(root: CanonicalizedModulePath) -> Self {
        Self(Arc::new(Mutex::new(AnalysisDiagnosticState {
            root,
            phase: ModulePhase::Registration,
            module: None,
            registration: Duration::ZERO,
            compilation: Duration::ZERO,
            evaluation: Duration::ZERO,
            measured_cpu: execution_observation::thread_cpu().map(|_| Duration::ZERO),
        })))
    }

    pub(crate) fn begin(&self, phase: ModulePhase, module: Option<&str>) {
        let mut state = self.0.lock();
        state.phase = phase;
        state.module = module.map(str::to_owned);
    }

    pub(crate) fn registration_finished(&self, elapsed: Duration) {
        self.0.lock().registration = elapsed;
    }

    pub(crate) fn describe(&self) -> String {
        let state = self.0.lock();
        let module = state
            .module
            .as_ref()
            .map(|module| format!(", module {module}"))
            .unwrap_or_default();
        format!(
            "Analysis of {} during {}{} (wall: registration {:?}, compilation {:?}, evaluation \
             {:?}; measured synchronous CPU {})",
            state.root.as_str(),
            state.phase.label(),
            module,
            state.registration,
            state.compilation,
            state.evaluation,
            state
                .measured_cpu
                .map(|cpu| format!("{cpu:?}"))
                .unwrap_or_else(|| "unavailable".to_owned()),
        )
    }
}

/// Synchronous phases only: this guard never crosses an await, so its thread
/// CPU delta cannot accidentally include work on a different executor thread.
pub(crate) struct ModulePhaseGuard {
    kind: ModuleRequestKind,
    phase: ModulePhase,
    diagnostic: Option<AnalysisDiagnostic>,
    start: Instant,
    cpu: Option<u64>,
}

impl ModulePhaseGuard {
    pub(crate) fn new(
        kind: ModuleRequestKind,
        phase: ModulePhase,
        diagnostic: Option<AnalysisDiagnostic>,
        module: Option<&str>,
    ) -> Self {
        if let Some(diagnostic) = &diagnostic {
            diagnostic.begin(phase, module);
        }
        let cpu = (diagnostic.is_some() || execution_observation::is_observed())
            .then(execution_observation::thread_cpu)
            .flatten();
        Self {
            kind,
            phase,
            diagnostic,
            start: Instant::now(),
            cpu,
        }
    }
}

impl Drop for ModulePhaseGuard {
    fn drop(&mut self) {
        let wall = self.start.elapsed();
        let cpu = self
            .cpu
            .and_then(|start| execution_observation::thread_cpu()?.checked_sub(start));
        metrics::log_module_phase(self.kind, self.phase, wall, cpu);
        execution_observation::record_module_phase(self.phase, wall, cpu);
        if let Some(diagnostic) = &self.diagnostic {
            let mut state = diagnostic.0.lock();
            state.measured_cpu = state
                .measured_cpu
                .zip(cpu)
                .map(|(total, cpu)| total + Duration::from_nanos(cpu));
            match self.phase {
                ModulePhase::Compilation => state.compilation += wall,
                ModulePhase::Evaluation => state.evaluation += wall,
                ModulePhase::Registration
                | ModulePhase::Serialization
                | ModulePhase::Instantiation
                | ModulePhase::ExportInspection
                | ModulePhase::Cleanup => (),
            }
        }
    }
}
