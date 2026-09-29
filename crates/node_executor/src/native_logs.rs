//! Bounded diagnostic stderr forwarding, independent of resident lifecycle
//! progress.
use std::time::Duration;

use tokio::{
    io::{
        AsyncRead,
        AsyncReadExt,
    },
    time::Instant,
};

const MAX_LINE_BYTES: usize = 8 * 1024;
const LINES_PER_SECOND: usize = 100;

pub(super) enum Record {
    Line(String),
    Dropped(u64),
    ReadFailed,
}

pub(super) fn emit(generation: &str, record: Record) {
    // Service tracing owns the bounded output queue, including lifecycle warnings.
    match record {
        Record::Line(line) => {
            tracing::info!(native_generation = %generation, "native_resident_log {line}")
        },
        Record::Dropped(count) => {
            tracing::warn!(native_generation = %generation, dropped = count, "Native resident logs dropped")
        },
        Record::ReadFailed => {
            tracing::warn!(native_generation = %generation, "Native resident log pipe failed")
        },
    }
}

pub(super) async fn forward(mut reader: impl AsyncRead + Unpin, mut emit: impl FnMut(Record)) {
    let mut bytes = [0; 4096];
    let mut line = Vec::with_capacity(MAX_LINE_BYTES);
    let mut oversized = false;
    let mut window = Instant::now();
    let mut emitted = 0;
    let mut dropped = 0u64;
    loop {
        let read = tokio::select! {
            // Report completed losses even if a healthy resident leaves stderr
            // open without writing again. No timer is polled without pending loss.
            _ = tokio::time::sleep_until(window + Duration::from_secs(1)), if dropped > 0 => {
                emit(Record::Dropped(dropped));
                dropped = 0;
                window = Instant::now();
                emitted = 0;
                continue;
            },
            read = reader.read(&mut bytes) => read,
        };
        let count = match read {
            Ok(0) => break,
            Ok(count) => count,
            Err(_) => {
                // A diagnostic pipe failure supplies no process-liveness or retirement
                // authority.
                emit(Record::ReadFailed);
                break;
            },
        };
        if window.elapsed() >= Duration::from_secs(1) {
            if dropped > 0 {
                emit(Record::Dropped(dropped));
                dropped = 0;
            }
            window = Instant::now();
            emitted = 0;
        }
        for &byte in &bytes[..count] {
            if byte == b'\n' {
                if oversized || emitted == LINES_PER_SECOND {
                    dropped = dropped.saturating_add(1);
                } else if !line.is_empty() {
                    match std::str::from_utf8(&line) {
                        Ok(text) if !text.chars().any(char::is_control) => {
                            emit(Record::Line(text.to_owned()));
                            emitted += 1;
                        },
                        _ => dropped = dropped.saturating_add(1),
                    }
                }
                line.clear();
                oversized = false;
            } else if !oversized {
                if line.len() == MAX_LINE_BYTES {
                    oversized = true;
                    line.clear();
                } else {
                    line.push(byte);
                }
            }
        }
        // A continuously writable child cannot monopolize a runtime worker while logs
        // are dropped.
        tokio::task::yield_now().await;
    }
    if oversized || !line.is_empty() {
        dropped = dropped.saturating_add(1);
    }
    if dropped > 0 {
        emit(Record::Dropped(dropped));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn framing_preserves_split_utf8_and_rejects_unsafe_or_incomplete_lines() {
        let exact = format!("{}é", "x".repeat(MAX_LINE_BYTES - 2));
        let split = exact.len() - 1;
        let mut rest = exact.as_bytes()[split..].to_vec();
        rest.push(b'\n');
        rest.extend(vec![b'x'; MAX_LINE_BYTES + 1]);
        rest.extend_from_slice(b"\n\xff\n\r\n\t\n\x7f\n\xc2\x85\n\nsafe\nfragment");
        // The reader ends a read inside the final UTF-8 scalar of an exact-limit
        // line. Recovery must also discard an oversized line across many reads.
        let reader = (&exact.as_bytes()[..split]).chain(rest.as_slice());
        let mut lines = Vec::new();
        let mut dropped = 0;
        forward(reader, |record| match record {
            Record::Line(line) => lines.push(line),
            Record::Dropped(count) => dropped += count,
            Record::ReadFailed => panic!("fixture pipe failed"),
        })
        .await;
        assert_eq!(lines, [exact, "safe".to_owned()]);
        assert_eq!(dropped, 7);
    }

    #[tokio::test]
    async fn real_child_logs_are_bounded_and_recover_after_oversized_lines() {
        let mut child = tokio::process::Command::new("python3")
            .args([
                "-c",
                "import sys; \
                 sys.stderr.write('x'*1000000+'\\n'+chr(27)+'unsafe\\n'+''.join('{\"event\":\"\
                 fixture\"}\\n' for _ in range(110)))",
            ])
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = Vec::new();
        let mut dropped = 0;
        tokio::time::timeout(
            Duration::from_secs(5),
            forward(child.stderr.take().unwrap(), |record| match record {
                Record::Line(line) => lines.push(line),
                Record::Dropped(count) => dropped += count,
                Record::ReadFailed => panic!("fixture pipe failed"),
            }),
        )
        .await
        .unwrap();
        assert!(child.wait().await.unwrap().success());
        assert!((LINES_PER_SECOND..=110).contains(&lines.len()));
        assert!(lines.iter().all(|line| line == "{\"event\":\"fixture\"}"));
        assert_eq!(lines.len() as u64 + dropped, 112);
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limit_resets_and_incomplete_output_has_no_authority() {
        use tokio::io::AsyncWriteExt;
        let (mut writer, reader) = tokio::io::duplex(4096);
        let (records_tx, mut records) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            forward(reader, |record| records_tx.send(record).unwrap()).await;
        });
        writer
            .write_all("line\n".repeat(101).as_bytes())
            .await
            .unwrap();
        // Observe the entire accepted burst before advancing time. The reader
        // finishes the same read (including the excess line) before yielding.
        for _ in 0..LINES_PER_SECOND {
            assert!(matches!(records.recv().await.unwrap(), Record::Line(line) if line == "line"));
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(1), records.recv())
                .await
                .unwrap(),
            Some(Record::Dropped(1))
        ));
        writer.write_all(b"recovered\nincomplete").await.unwrap();
        assert!(matches!(records.recv().await.unwrap(), Record::Line(line) if line == "recovered"));
        drop(writer);
        assert!(matches!(records.recv().await.unwrap(), Record::Dropped(1)));
        task.await.unwrap();
        assert!(records.recv().await.is_none());
    }
}
