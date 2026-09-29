//! Small native lifecycle peer used only by integration tests.
use std::io::{
    BufRead,
    Write,
};

use anyhow::Context;
use serde_json::{
    json,
    Value,
};

fn main() -> anyhow::Result<()> {
    let mode_argument = std::env::args().nth(1);
    if matches!(
        mode_argument.as_deref(),
        Some("parent" | "parent_blocked_logs")
    ) {
        let _logging = cmd_util::env::config_service();
        let arguments = std::env::args().collect::<Vec<_>>();
        let descriptor = serde_json::from_slice(&std::fs::read(&arguments[4])?)?;
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let supervisor = node_executor::native::test_supervisor(
                    arguments[2].clone().into(),
                    arguments[3].clone().into(),
                )?;
                supervisor
                    .reconcile(Some(descriptor), common::types::Timestamp::try_from(1u64)?)?;
                if mode_argument.as_deref() == Some("parent") {
                    return std::future::pending::<anyhow::Result<()>>().await;
                }
                let commands_result = async {
                    use tokio::io::AsyncBufReadExt;
                    let mut commands = tokio::io::BufReader::new(tokio::io::stdin()).lines();
                    anyhow::ensure!(commands.next_line().await?.as_deref() == Some("force"));
                    let generation = supervisor
                        .status()
                        .active_generation
                        .context("active missing")?;
                    supervisor.force_retire(&generation)?;
                    supervisor.reconcile(None, common::types::Timestamp::try_from(2u64)?)?;
                    while supervisor.status().phase != "inactive" {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    // The external observer checks /proc after this confirmed-reap boundary.
                    std::fs::write(&arguments[5], b"reaped")?;
                    anyhow::ensure!(commands.next_line().await?.as_deref() == Some("shutdown"));
                    anyhow::Ok(())
                }
                .await;
                // A failed observer closes stdin after releasing output backpressure.
                // Keep cleanup owned before returning that protocol error.
                let shutdown = supervisor.shutdown().await;
                commands_result?;
                shutdown
            });
    }
    let mut events = None;
    let mut mode = String::new();
    let mut active = false;
    let mut retained = Vec::new();
    for line in std::io::stdin().lock().lines() {
        let input: Value = serde_json::from_str(&line?)?;
        let kind = input["type"].as_str().context("type missing")?;
        if kind == "initialize" {
            mode = input["configuration"]["mode"]
                .as_str()
                .unwrap_or("healthy")
                .to_owned();
            events = Some(
                std::fs::OpenOptions::new().append(true).create(true).open(
                    input["configuration"]["eventFile"]
                        .as_str()
                        .context("event file missing")?,
                )?,
            );
        }
        let events = events.as_mut().context("initialize missing")?;
        write_event(events, kind, &input["generation"])?;
        if (kind == "initialize" && mode == "rss_before_ready")
            || (kind == "activate" && mode == "rss_active")
        {
            retained.resize(96 * 1024 * 1024, 1u8);
            std::hint::black_box(&retained);
        }
        let reply = match kind {
            "initialize" if mode == "stall_ready" || mode == "rss_before_ready" => None,
            "initialize" => {
                let mut descriptor = input["descriptor"].clone();
                descriptor["type"] = "ready".into();
                descriptor["generation"] = input["generation"].clone();
                Some(descriptor)
            },
            "activate" => {
                eprintln!("{}", json!({"event":"fixture_activated"}));
                if mode == "log_backpressure" {
                    let diagnostic =
                        json!({"event":"fixture_diagnostic", "padding":"x".repeat(8000)});
                    // Four finite bursts fit the per-child rate and probe budgets,
                    // but exceed the service output queue while its pipe is blocked.
                    for burst in 0..4 {
                        if burst > 0 {
                            std::thread::sleep(std::time::Duration::from_millis(1100));
                        }
                        for _ in 0..80 {
                            eprintln!("{diagnostic}");
                        }
                    }
                    write_event(events, "logs_written", &input["generation"])?;
                }
                active = true;
                None
            },
            "probe" if active && mode == "stall_active" => None,
            "probe" => Some(
                json!({"type":"progress", "generation":input["generation"], "nonce":input["nonce"]}),
            ),
            "retire" if mode == "drain_hold" => None,
            "retire" => Some(json!({"type":"drained", "generation":input["generation"]})),
            _ => anyhow::bail!("unknown control request"),
        };
        if let Some(reply) = reply {
            if kind == "probe" && !active && mode == "invalid_then_progress" {
                // A malformed reply followed by a valid one must not revive
                // this inert generation after its monitor fails.
                println!(
                    "{}",
                    json!({"type":"progress", "generation":input["generation"], "nonce":input["nonce"].as_u64().unwrap() + 1})
                );
            }
            println!("{reply}");
            std::io::stdout().flush()?;
        }
    }
    if mode == "ignore_eof" {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
    Ok(())
}

fn write_event(events: &mut std::fs::File, kind: &str, generation: &Value) -> anyhow::Result<()> {
    // Candidate and predecessor append concurrently. Serialize first so a
    // complete small record is emitted by one append write.
    let mut event = serde_json::to_vec(
        &json!({"type":kind, "generation":generation, "pid":std::process::id()}),
    )?;
    event.push(b'\n');
    events.write_all(&event)?;
    events.flush()?;
    Ok(())
}
