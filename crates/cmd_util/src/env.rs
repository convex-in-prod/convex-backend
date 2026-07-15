use std::{
    env,
    fmt::Debug,
    fs::File,
    io,
    str::FromStr,
    sync::{
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
        mpsc::{
            self,
            Receiver,
            SyncSender,
        },
        Arc,
        LazyLock,
    },
    time::Duration,
};

use sentry_tracing::EventFilter;
use tracing::Level;
use tracing_subscriber::{
    fmt::{
        format::format,
        MakeWriter,
    },
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
    Layer,
};

pub fn env_config<T>(name: &str, default: T) -> T
where
    T: Debug + FromStr + PartialEq,
    <T as FromStr>::Err: Debug,
{
    let var_s = match env::var(name) {
        Ok(s) => s,
        Err(env::VarError::NotPresent) => return default,
        Err(env::VarError::NotUnicode(..)) => {
            tracing::warn!("Invalid value for {name}, falling back to {default:?}.");
            return default;
        },
    };
    match T::from_str(&var_s) {
        Ok(v) => {
            if v != default {
                tracing::info!("Overriding {name} to {v:?} from environment");
            }
            v
        },
        Err(e) => {
            tracing::warn!("Invalid value {var_s} for {name}, falling back to {default:?}: {e:?}");
            default
        },
    }
}

pub static CONVEX_TRACE_FILE: LazyLock<Option<File>> = LazyLock::new(|| {
    if env::var("CONVEX_TRACE_FILE").is_err() {
        return None;
    }

    let exe_path = std::env::current_exe().expect("Couldn't find exe name");
    let exe_name = exe_path
        .file_name()
        .expect("Path was empty")
        .to_str()
        .expect("Not valid unicode");
    // e.g. `backend.log`
    let filename = format!("{exe_name}.log");

    let file =
        File::create(&filename).unwrap_or_else(|_| panic!("Could not create file {filename}"));
    Some(file)
});

enum OutputRecord {
    Bytes { bytes: Vec<u8>, report_loss: bool },
    Finish,
}

struct OutputState {
    sender: SyncSender<OutputRecord>,
    closing: AtomicBool,
    dropped: AtomicU64,
}

#[derive(Clone)]
struct BufferedWriter {
    state: Arc<OutputState>,
    report_loss: bool,
}

impl<'a> MakeWriter<'a> for BufferedWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self {
        self.clone()
    }

    fn make_writer_for(&'a self, metadata: &tracing::Metadata<'_>) -> Self {
        Self {
            state: self.state.clone(),
            // Both stdout and the optional file receive this event. Losing a
            // summary must not create further summaries in either output.
            report_loss: metadata.target() != "cmd_util::tracing_output",
        }
    }
}

impl io::Write for BufferedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.state.closing.load(Ordering::Acquire)
            && self
                .state
                .sender
                .try_send(OutputRecord::Bytes {
                    bytes: bytes.to_vec(),
                    report_loss: self.report_loss,
                })
                .is_err()
            && self.report_loss
        {
            self.state.dropped.fetch_add(1, Ordering::Relaxed);
        }
        // The formatter can synchronously report writer errors on stderr.
        // Diagnostic loss must not move output back onto the caller's thread.
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct OutputGuard {
    state: Arc<OutputState>,
    finished: Receiver<()>,
}

impl Drop for OutputGuard {
    fn drop(&mut self) {
        self.state.closing.store(true, Ordering::Release);
        // A full queue already wakes the worker. A blocked output can outlive this
        // guard; never log, join, or flush synchronously on this cleanup path.
        let _ = self.state.sender.try_send(OutputRecord::Finish);
        let _ = self.finished.recv_timeout(Duration::from_secs(1));
    }
}

fn buffered_writer(
    mut output: impl io::Write + Send + 'static,
    name: &'static str,
    capacity: usize,
) -> (BufferedWriter, OutputGuard) {
    let (sender, receiver) = mpsc::sync_channel(capacity);
    let state = Arc::new(OutputState {
        sender,
        closing: AtomicBool::new(false),
        dropped: AtomicU64::new(0),
    });
    let (finished_tx, finished) = mpsc::sync_channel(1);
    let worker_state = state.clone();
    std::thread::Builder::new()
        .name(format!("tracing-{name}"))
        .spawn(move || {
            loop {
                let record = if worker_state.closing.load(Ordering::Acquire) {
                    receiver.try_recv().ok()
                } else {
                    receiver.recv().ok()
                };
                match record {
                    Some(OutputRecord::Bytes { bytes, report_loss }) => {
                        if output.write_all(&bytes).and_then(|()| output.flush()).is_err() {
                            if report_loss {
                                worker_state.dropped.fetch_add(1, Ordering::Relaxed);
                            }
                            continue;
                        }
                        let dropped = worker_state.dropped.swap(0, Ordering::Relaxed);
                        if dropped > 0 && !worker_state.closing.load(Ordering::Acquire) {
                            // Report only after output resumes. This event uses the same
                            // bounded queues; a failed output never recursively reports itself.
                            tracing::warn!(target: "cmd_util::tracing_output", output = name, dropped, "Tracing output dropped records");
                        }
                    },
                    Some(OutputRecord::Finish) | None => break,
                }
            }
            let _ = finished_tx.send(());
        })
        // Logging is initialized before service work starts. Fail visibly at
        // startup instead of caching an unavailable diagnostic worker forever.
        .expect("Failed to start tracing output writer");
    (
        BufferedWriter {
            state: state.clone(),
            report_loss: true,
        },
        OutputGuard { state, finished },
    )
}

/// Hold for the service lifetime. Shutdown attempts to drain each output for at
/// most one second; pending records may be lost when an output remains blocked.
pub struct TracingGuard {
    _stdout_guard: Option<OutputGuard>,
    _file_guard: Option<OutputGuard>,
}

/// Call this from scripts at startup.
#[must_use]
pub fn config_tool() -> TracingGuard {
    config_tracing(io::stderr, Level::ERROR)
}

/// Call this from services at startup.
#[must_use]
pub fn config_service() -> TracingGuard {
    let (writer, stdout_guard) = buffered_writer(io::stdout(), "stdout", 256);
    let mut guard = config_tracing(writer, Level::INFO);
    guard._stdout_guard = Some(stdout_guard);
    guard
}

fn config_tracing<W>(writer: W, level: Level) -> TracingGuard
where
    W: Send + Sync + for<'writer> MakeWriter<'writer> + 'static,
{
    let mut layers = Vec::new();
    let color_disabled = std::env::var("NO_COLOR").is_ok();
    let format_layer = tracing_subscriber::fmt::layer()
        .with_ansi(!color_disabled)
        .with_writer(writer);
    let format_layer = match std::env::var("LOG_FORMAT") {
        Ok(s) if s == "json" => format_layer.event_format(format().json()).boxed(),
        Ok(s) if s == "compact" => format_layer.event_format(format().compact()).boxed(),
        Ok(s) if s == "pretty" => format_layer.event_format(format().pretty()).boxed(),
        _ => format_layer.event_format(format().compact()).boxed(),
    };
    let format_layer = format_layer
        .with_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or(EnvFilter::new(level.as_str())),
        )
        .boxed();
    layers.push(format_layer);
    let sentry_layer = sentry_tracing::layer()
        .event_filter(|md| match md.level() {
            &tracing::Level::DEBUG | &tracing::Level::TRACE => EventFilter::Ignore,
            _ => EventFilter::Breadcrumb,
        })
        .span_filter(|_md| false);
    layers.push(sentry_layer.boxed());

    let guard = if let Some(ref file) = *CONVEX_TRACE_FILE {
        // Preserve the existing optional file buffer, but use bounded cleanup:
        // the appender's guard prints to stdout if its shutdown queue is full.
        let (file_writer, guard) = buffered_writer(
            file,
            "file",
            tracing_appender::non_blocking::DEFAULT_BUFFERED_LINES_LIMIT,
        );
        let file_writer_layer = tracing_subscriber::fmt::layer()
            .with_writer(file_writer)
            .with_filter(
                EnvFilter::from_default_env()
                    .add_directive(Level::INFO.into())
                    .add_directive("common::errors=debug".parse().unwrap()),
            )
            .boxed();
        layers.push(file_writer_layer);
        Some(guard)
    } else {
        None
    };
    tracing_subscriber::registry().with(layers).init();

    TracingGuard {
        _stdout_guard: None,
        _file_guard: guard,
    }
}

pub fn config_test() {
    // Try to initialize tracing_subcriber. Ok if it fails - probably
    // means it was initialized already. Ok to be non-rigorous here, because
    // it's very hard to run initialization of logging in tests, so we tend to
    // toss it in common helper methods all over.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .compact()
        .try_init();
}
