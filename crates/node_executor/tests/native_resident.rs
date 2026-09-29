#![cfg(all(feature = "testing", target_os = "linux"))]

use std::{
    panic::AssertUnwindSafe,
    path::Path,
    time::Duration,
};

use anyhow::Context;
use common::{
    memory_pressure::MemoryPressureSignal,
    types::Timestamp,
};
use futures::FutureExt;
use model::source_packages::native::NativeResidentDescriptor;
use node_executor::native::{
    test_supervisor,
    test_supervisor_with_limits,
    NativeResidentSupervisor,
};
use serde_json::{
    json,
    Value,
};
use value::sha256::Sha256;

async fn wait_for_active(supervisor: &NativeResidentSupervisor) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while supervisor.status().phase != "active" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native resident did not activate");
}

#[tokio::test]
async fn verified_elf_descriptor_executes_and_selection_survives_function_only_changes() {
    let root = tempfile::tempdir().unwrap();
    let artifacts = root.path().join("artifacts");
    let configurations = root.path().join("configurations");
    std::fs::create_dir_all(&configurations).unwrap();
    let bytes = std::fs::read(env!("CARGO_BIN_EXE_native_resident_fixture")).unwrap();
    let artifact_sha256 = Sha256::hash(&bytes).as_hex();
    let directory = artifacts.join(&artifact_sha256);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::copy(
        env!("CARGO_BIN_EXE_native_resident_fixture"),
        directory.join("resident"),
    )
    .unwrap();
    let event_file = root.path().join("events.jsonl");
    let configuration = serde_json::to_vec(&json!({"eventFile":event_file})).unwrap();
    let configuration_sha256 = Sha256::hash(&configuration).as_hex();
    std::fs::write(
        configurations.join(format!("{configuration_sha256}.json")),
        configuration,
    )
    .unwrap();
    let descriptor = NativeResidentDescriptor {
        artifact_sha256,
        configuration_sha256,
        lifecycle_protocol: 1,
        application_contract: "example-v1".into(),
    };
    let supervisor = test_supervisor(artifacts, configurations.clone()).unwrap();
    supervisor.prepare(&descriptor, false).await.unwrap();
    let staged = std::fs::read_to_string(&event_file).unwrap();
    assert!(!staged.contains("activate"));
    supervisor
        .reconcile(Some(descriptor.clone()), Timestamp::try_from(1u64).unwrap())
        .unwrap();
    assert!(supervisor
        .validate_publication_contract(Some("example-v2"))
        .is_err());
    wait_for_active(&supervisor).await;
    let generation = supervisor.status().active_generation.unwrap();
    supervisor
        .reconcile(Some(descriptor.clone()), Timestamp::try_from(2u64).unwrap())
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        supervisor.status().active_generation.as_ref(),
        Some(&generation)
    );
    let replacement_configuration =
        serde_json::to_vec(&json!({"eventFile":event_file, "revision":2})).unwrap();
    let replacement_configuration_sha256 = Sha256::hash(&replacement_configuration).as_hex();
    std::fs::write(
        configurations.join(format!("{replacement_configuration_sha256}.json")),
        replacement_configuration,
    )
    .unwrap();
    let replacement = NativeResidentDescriptor {
        configuration_sha256: replacement_configuration_sha256,
        ..descriptor
    };
    supervisor
        .reconcile(
            Some(replacement.clone()),
            Timestamp::try_from(3u64).unwrap(),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while supervisor.status().active_descriptor.as_ref() != Some(&replacement)
            || supervisor.status().phase != "active"
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let predecessor = generation;
    let generation = supervisor.status().active_generation.unwrap();
    assert_ne!(generation, predecessor);
    assert!(supervisor.force_retire(&predecessor).is_err());
    assert!(supervisor.force_retire("stale-generation").is_err());
    supervisor.force_retire(&generation).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = supervisor.status();
            if status.phase == "active" && status.active_generation.as_ref() != Some(&generation) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(supervisor.force_retire(&generation).is_err());
    supervisor
        .reconcile(None, Timestamp::try_from(4u64).unwrap())
        .unwrap();
    assert!(supervisor.validate_publication_contract(None).is_err());
    tokio::time::timeout(Duration::from_secs(10), async {
        while supervisor.status().phase != "inactive" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    supervisor.validate_publication_contract(None).unwrap();
    supervisor.shutdown().await.unwrap();
    let events = std::fs::read_to_string(event_file)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        events
            .iter()
            .filter(|event| event["type"] == "activate")
            .count(),
        3
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event["type"] == "retire")
            .count(),
        2
    );
    let retired = events
        .iter()
        .position(|event| event["type"] == "retire" && event["generation"] == predecessor)
        .unwrap();
    let promoted = events
        .iter()
        .position(|event| event["type"] == "activate" && event["generation"] == generation)
        .unwrap();
    assert!(retired < promoted);
    for event in events {
        let pid = event["pid"].as_u64().unwrap();
        assert!(
            !Path::new(&format!("/proc/{pid}")).exists(),
            "native child was not reaped"
        );
    }
}

struct Fixture {
    root: tempfile::TempDir,
    artifacts: std::path::PathBuf,
    configurations: std::path::PathBuf,
    descriptor: NativeResidentDescriptor,
}

impl Fixture {
    fn new(mode: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let artifacts = root.path().join("artifacts");
        let configurations = root.path().join("configurations");
        std::fs::create_dir_all(&configurations).unwrap();
        let bytes = std::fs::read(env!("CARGO_BIN_EXE_native_resident_fixture")).unwrap();
        let artifact_sha256 = Sha256::hash(&bytes).as_hex();
        let directory = artifacts.join(&artifact_sha256);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::copy(
            env!("CARGO_BIN_EXE_native_resident_fixture"),
            directory.join("resident"),
        )
        .unwrap();
        let configuration =
            serde_json::to_vec(&json!({"eventFile":root.path().join("events.jsonl"), "mode":mode}))
                .unwrap();
        let configuration_sha256 = Sha256::hash(&configuration).as_hex();
        std::fs::write(
            configurations.join(format!("{configuration_sha256}.json")),
            configuration,
        )
        .unwrap();
        Self {
            root,
            artifacts,
            configurations,
            descriptor: NativeResidentDescriptor {
                artifact_sha256,
                configuration_sha256,
                lifecycle_protocol: 1,
                application_contract: "example-v1".into(),
            },
        }
    }

    fn supervisor(
        &self,
        pressure: MemoryPressureSignal,
    ) -> std::sync::Arc<NativeResidentSupervisor> {
        test_supervisor_with_limits(
            self.artifacts.clone(),
            self.configurations.clone(),
            64 * 1024 * 1024,
            pressure,
        )
        .unwrap()
    }

    fn events(&self) -> Vec<Value> {
        match std::fs::read_to_string(self.root.path().join("events.jsonl")) {
            Ok(events) => events
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("fixture events failed: {error}"),
        }
    }

    fn publish_configuration(&self, mode: &str) -> NativeResidentDescriptor {
        let configuration = serde_json::to_vec(&json!({
            "eventFile": self.root.path().join("events.jsonl"), "mode": mode,
        }))
        .unwrap();
        let configuration_sha256 = Sha256::hash(&configuration).as_hex();
        std::fs::write(
            self.configurations
                .join(format!("{configuration_sha256}.json")),
            configuration,
        )
        .unwrap();
        NativeResidentDescriptor {
            configuration_sha256,
            ..self.descriptor.clone()
        }
    }

    async fn event(&self, kind: &str, ordinal: usize) -> Value {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(event) = self
                    .events()
                    .into_iter()
                    .filter(|event| event["type"] == kind)
                    .nth(ordinal)
                {
                    return event;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("native fixture did not observe lifecycle event")
    }

    fn assert_reaped(&self) {
        for event in self.events() {
            assert!(
                !Path::new(&format!("/proc/{}", event["pid"].as_u64().unwrap())).exists(),
                "native child retained after confirmed shutdown"
            );
        }
    }

    fn parent_command(&self, mode: &str) -> tokio::process::Command {
        // Subreaping is process-wide. Keep it enabled for this test binary so
        // parallel parent tests cannot restore it while another owns orphans.
        static SUBREAPER: std::sync::Once = std::sync::Once::new();
        SUBREAPER.call_once(|| {
            assert_eq!(unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1) }, 0);
        });
        let descriptor_path = self.root.path().join("descriptor.json");
        std::fs::write(
            &descriptor_path,
            serde_json::to_vec(&self.descriptor).unwrap(),
        )
        .unwrap();
        let mut command =
            tokio::process::Command::new(env!("CARGO_BIN_EXE_native_resident_fixture"));
        command
            .arg(mode)
            .arg(&self.artifacts)
            .arg(&self.configurations)
            .arg(descriptor_path)
            .env("LOG_FORMAT", "json")
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "info")
            .env_remove("CONVEX_TRACE_FILE")
            .process_group(0)
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        command
    }
}

async fn reap_parent_group(group: u32) {
    let group = libc::pid_t::try_from(group).unwrap();
    // The direct parent has already been reaped. This group selects only its
    // adopted residents, including children that failed before writing events.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let waited = unsafe { libc::waitpid(-group, std::ptr::null_mut(), libc::WNOHANG) };
            if waited == -1 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                assert_eq!(error.raw_os_error(), Some(libc::ECHILD));
                break;
            }
            if waited == 0 {
                // Cleanup must also handle a regression in parent-death signaling.
                assert_eq!(unsafe { libc::kill(-group, libc::SIGKILL) }, 0);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await
    .expect("fixture process group did not reap");
}

#[tokio::test]
async fn incumbent_remains_supervised_while_candidate_readiness_stalls() {
    let fixture = Fixture::new("healthy");
    let supervisor = fixture.supervisor(MemoryPressureSignal::default());
    supervisor
        .reconcile(
            Some(fixture.descriptor.clone()),
            Timestamp::try_from(1u64).unwrap(),
        )
        .unwrap();
    let incumbent = fixture.event("activate", 0).await;
    let replacement = fixture.publish_configuration("stall_ready");
    supervisor
        .reconcile(Some(replacement), Timestamp::try_from(2u64).unwrap())
        .unwrap();
    fixture.event("initialize", 1).await;
    let earlier_probes = fixture
        .events()
        .iter()
        .filter(|event| event["type"] == "probe" && event["pid"] == incumbent["pid"])
        .count();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if fixture
                .events()
                .iter()
                .filter(|event| event["type"] == "probe" && event["pid"] == incumbent["pid"])
                .count()
                > earlier_probes
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Incumbent supervision stopped during readiness");
    supervisor
        .force_retire(incumbent["generation"].as_str().unwrap())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while Path::new(&format!("/proc/{}", incumbent["pid"].as_u64().unwrap())).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Force retirement waited for candidate readiness");
    supervisor
        .reconcile(
            Some(fixture.descriptor.clone()),
            Timestamp::try_from(3u64).unwrap(),
        )
        .unwrap();
    let successor = fixture.event("activate", 1).await;
    assert_ne!(successor["generation"], incumbent["generation"]);
    supervisor.shutdown().await.unwrap();
    fixture.assert_reaped();
}

#[tokio::test]
async fn failed_inert_candidate_is_reaped_even_if_a_valid_reply_follows() {
    let fixture = Fixture::new("drain_hold");
    let supervisor = fixture.supervisor(MemoryPressureSignal::default());
    supervisor
        .reconcile(
            Some(fixture.descriptor.clone()),
            Timestamp::try_from(1u64).unwrap(),
        )
        .unwrap();
    fixture.event("activate", 0).await;
    let replacement = fixture.publish_configuration("invalid_then_progress");
    supervisor
        .reconcile(Some(replacement), Timestamp::try_from(2u64).unwrap())
        .unwrap();
    let candidate = fixture.event("initialize", 1).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while Path::new(&format!("/proc/{}", candidate["pid"].as_u64().unwrap())).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("A failed candidate survived its monitor failure");
    assert!(
        !fixture
            .events()
            .iter()
            .any(|event| event["type"] == "activate"
                && event["generation"] == candidate["generation"])
    );
    let healthy = fixture.publish_configuration("healthy");
    supervisor
        .reconcile(Some(healthy), Timestamp::try_from(3u64).unwrap())
        .unwrap();
    fixture.event("activate", 1).await;
    supervisor.shutdown().await.unwrap();
    fixture.assert_reaped();
}

#[tokio::test]
async fn memory_pressure_interrupts_stalled_progress_and_allows_recovery() {
    let fixture = Fixture::new("stall_active");
    let pressure = MemoryPressureSignal::new(true);
    let supervisor = fixture.supervisor(pressure.clone());
    assert!(supervisor
        .prepare(&fixture.descriptor, false)
        .await
        .is_err());
    assert!(fixture.events().is_empty());
    supervisor
        .reconcile(
            Some(fixture.descriptor.clone()),
            Timestamp::try_from(1u64).unwrap(),
        )
        .unwrap();
    pressure.set_active(false);
    let active = fixture.event("activate", 0).await;
    fixture.event("probe", 1).await;
    pressure.set_active(true);
    tokio::time::timeout(Duration::from_secs(2), async {
        while supervisor.status().phase != "pressure" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pressure waited for stalled progress timeout");
    assert!(!Path::new(&format!("/proc/{}", active["pid"].as_u64().unwrap())).exists());
    pressure.set_active(false);
    let successor = fixture.event("activate", 1).await;
    assert_ne!(active["generation"], successor["generation"]);
    supervisor.shutdown().await.unwrap();
    fixture.assert_reaped();
}

#[tokio::test]
async fn pressure_and_shutdown_reap_both_children_during_retirement() {
    for interrupt_with_pressure in [true, false] {
        let fixture = Fixture::new("drain_hold");
        let pressure = MemoryPressureSignal::default();
        let supervisor = fixture.supervisor(pressure.clone());
        supervisor
            .reconcile(
                Some(fixture.descriptor.clone()),
                Timestamp::try_from(1u64).unwrap(),
            )
            .unwrap();
        fixture.event("activate", 0).await;
        let configuration = serde_json::to_vec(&json!({
            "eventFile": fixture.root.path().join("events.jsonl"),
            "mode": "healthy",
        }))
        .unwrap();
        let configuration_sha256 = Sha256::hash(&configuration).as_hex();
        std::fs::write(
            fixture
                .configurations
                .join(format!("{configuration_sha256}.json")),
            configuration,
        )
        .unwrap();
        supervisor
            .reconcile(
                Some(NativeResidentDescriptor {
                    configuration_sha256,
                    ..fixture.descriptor.clone()
                }),
                Timestamp::try_from(2u64).unwrap(),
            )
            .unwrap();
        fixture.event("retire", 0).await;
        assert_eq!(supervisor.status().phase, "draining");
        assert_eq!(
            fixture
                .events()
                .iter()
                .filter(|event| event["type"] == "initialize")
                .count(),
            2,
        );
        if interrupt_with_pressure {
            pressure.set_active(true);
            tokio::time::timeout(Duration::from_secs(2), async {
                while supervisor.status().phase != "pressure" {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("pressure did not interrupt retirement");
            fixture.assert_reaped();
        }
        tokio::time::timeout(Duration::from_secs(2), supervisor.shutdown())
            .await
            .expect("shutdown did not interrupt retirement")
            .unwrap();
        fixture.assert_reaped();
        assert_eq!(
            fixture
                .events()
                .iter()
                .filter(|event| event["type"] == "activate")
                .count(),
            1,
            "inert successor activated during interruption",
        );
    }
}

#[tokio::test]
async fn rss_is_enforced_before_readiness_and_after_activation() {
    let fixture = Fixture::new("rss_before_ready");
    let supervisor = fixture.supervisor(MemoryPressureSignal::default());
    let started = tokio::time::Instant::now();
    assert!(supervisor
        .prepare(&fixture.descriptor, false)
        .await
        .is_err());
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "RSS enforcement waited for readiness timeout"
    );
    supervisor.shutdown().await.unwrap();
    fixture.assert_reaped();
    assert!(fixture
        .events()
        .iter()
        .all(|event| event["type"] != "activate"));

    let fixture = Fixture::new("rss_active");
    let supervisor = fixture.supervisor(MemoryPressureSignal::default());
    supervisor
        .reconcile(
            Some(fixture.descriptor.clone()),
            Timestamp::try_from(1u64).unwrap(),
        )
        .unwrap();
    let active = fixture.event("activate", 0).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while Path::new(&format!("/proc/{}", active["pid"].as_u64().unwrap())).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("active RSS breach did not terminate child");
    supervisor.shutdown().await.unwrap();
    fixture.assert_reaped();
}

#[tokio::test]
async fn cancelled_preparation_and_shutdown_keep_cleanup_owned() {
    use futures::{
        poll,
        FutureExt,
    };
    let fixture = Fixture::new("stall_ready");
    let supervisor = fixture.supervisor(MemoryPressureSignal::default());
    let prepare_supervisor = supervisor.clone();
    let descriptor = fixture.descriptor.clone();
    let preparation =
        tokio::spawn(async move { prepare_supervisor.prepare(&descriptor, false).await });
    fixture.event("initialize", 0).await;
    preparation.abort();
    assert!(preparation.await.unwrap_err().is_cancelled());
    let mut cancelled_shutdown = Box::pin(supervisor.shutdown());
    let result = poll!(cancelled_shutdown.as_mut());
    if let std::task::Poll::Ready(result) = result {
        result.unwrap();
    }
    drop(cancelled_shutdown);
    supervisor.shutdown().await.unwrap();
    assert_eq!(supervisor.status().phase, "stopped");
    fixture.assert_reaped();
    assert!(supervisor
        .prepare(&fixture.descriptor, false)
        .now_or_never()
        .unwrap()
        .is_err());
}

#[tokio::test]
async fn real_coordinator_stall_reaps_before_replacement() {
    let fixture = Fixture::new("stall_active");
    let supervisor = fixture.supervisor(MemoryPressureSignal::default());
    supervisor
        .reconcile(
            Some(fixture.descriptor.clone()),
            Timestamp::try_from(1u64).unwrap(),
        )
        .unwrap();
    let first = fixture.event("activate", 0).await;
    let second = fixture.event("activate", 1).await;
    assert_ne!(first["generation"], second["generation"]);
    assert!(!Path::new(&format!("/proc/{}", first["pid"].as_u64().unwrap())).exists());
    supervisor.shutdown().await.unwrap();
    fixture.assert_reaped();
}

#[tokio::test]
async fn parent_death_kills_a_resident_that_ignores_pipe_closure() {
    let fixture = Fixture::new("ignore_eof");
    let mut parent = fixture
        .parent_command("parent")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let group = parent.id().unwrap();
    let observed = AssertUnwindSafe(async {
        let active = fixture.event("activate", 0).await;
        // Capture the actual backend formatter output, including the supervised child's
        // stderr. The operator's container log collector receives this same record.
        use tokio::io::AsyncBufReadExt;
        let mut logs = tokio::io::BufReader::new(parent.stdout.take().unwrap()).lines();
        let logging = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let line = logs.next_line().await?.context("parent log pipe closed")?;
                let record: Value = serde_json::from_str(&line)?;
                if let Some(message) = record["fields"]["message"].as_str()
                    && let Some(message) = message.strip_prefix("native_resident_log ")
                {
                    anyhow::ensure!(record["fields"]["native_generation"] == active["generation"]);
                    anyhow::ensure!(
                        serde_json::from_str::<Value>(message)?
                            == json!({"event":"fixture_activated"})
                    );
                    return anyhow::Ok(());
                }
            }
        })
        .await;
        // Reap the parent and adopted resident even when the diagnostic assertion
        // fails.
        parent.kill().await.unwrap();
        let pid = libc::pid_t::try_from(active["pid"].as_u64().unwrap()).unwrap();
        let status = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let mut status = 0;
                let waited = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
                if waited == pid {
                    break status;
                }
                assert_eq!(waited, 0, "orphaned resident was not adopted for reaping");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("parent loss left an active resident");
        assert!(libc::WIFSIGNALED(status));
        assert_eq!(libc::WTERMSIG(status), libc::SIGKILL);
        fixture.assert_reaped();
        logging
            .expect("resident stderr did not reach backend logs")
            .unwrap();
    })
    .catch_unwind()
    .await;
    parent.kill().await.unwrap();
    reap_parent_group(group).await;
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn blocked_service_logs_allow_forced_reaping_and_bounded_shutdown() {
    use std::os::fd::AsRawFd;

    use tokio::io::AsyncWriteExt;

    let fixture = Fixture::new("log_backpressure");
    let reaped_path = fixture.root.path().join("reaped");
    // Size the empty pipe before spawn; resizing after the child writes can fail.
    let (mut logs, log_writer) = std::io::pipe().unwrap();
    assert!(unsafe { libc::fcntl(logs.as_raw_fd(), libc::F_SETPIPE_SZ, 4096) } > 0);
    let backpressure_probe = log_writer.try_clone().unwrap();
    let mut parent = fixture
        .parent_command("parent_blocked_logs")
        .arg(&reaped_path)
        .stdin(std::process::Stdio::piped())
        .stdout(log_writer)
        .spawn()
        .unwrap();
    let group = parent.id().unwrap();
    let mut commands = parent.stdin.take().unwrap();

    // This deadline belongs to a different process from the current-thread
    // supervisor and its output guard, so a blocked writer cannot disable it.
    let observed = AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if fixture
                .events()
                .iter()
                .any(|event| event["type"] == "logs_written")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut available = 0;
        anyhow::ensure!(
            unsafe { libc::ioctl(logs.as_raw_fd(), libc::FIONREAD, &mut available) } == 0
        );
        anyhow::ensure!(
            available > 0,
            "native diagnostics did not reach backend stdout"
        );
        let mut writable = libc::pollfd {
            fd: backpressure_probe.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        };
        anyhow::ensure!(
            unsafe { libc::poll(&mut writable, 1, 0) } == 0,
            "backend stdout was not backpressured"
        );
        commands.write_all(b"force\n").await?;
        while !reaped_path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for event in fixture.events() {
            let pid = event["pid"].as_u64().context("fixture pid missing")?;
            anyhow::ensure!(
                !Path::new(&format!("/proc/{pid}")).exists(),
                "resident was not reaped"
            );
        }
        commands.write_all(b"shutdown\n").await?;
        let status = tokio::time::timeout(Duration::from_secs(3), parent.wait())
            .await
            .context("logging guard delayed parent shutdown")??;
        anyhow::ensure!(status.success(), "fixture parent failed");
        anyhow::Ok(())
    }))
    .catch_unwind()
    .await;

    // Release finite backpressure before cleanup on a failed assertion. No output
    // is read on the success path until both child reap and parent exit are
    // observed.
    drop(backpressure_probe);
    let drained =
        tokio::task::spawn_blocking(move || std::io::copy(&mut logs, &mut std::io::sink()));
    drop(commands);
    match tokio::time::timeout(Duration::from_secs(7), parent.wait()).await {
        Ok(status) => {
            status.unwrap();
        },
        Err(_) => parent.kill().await.unwrap(),
    }
    reap_parent_group(group).await;
    drained.await.unwrap().unwrap();
    let observed = observed.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    observed
        .expect("blocked logging stalled supervision or guard shutdown")
        .unwrap();
    fixture.assert_reaped();
}
