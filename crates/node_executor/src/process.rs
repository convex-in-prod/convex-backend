//! Direct-child termination shared by local executor adapters.
use std::process::ExitStatus;

use tokio::process::Child;

pub(crate) async fn kill_and_reap(child: &mut Child) -> std::io::Result<(bool, ExitStatus)> {
    let requested = match child.start_kill() {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => false,
        Err(error) => return Err(error),
    };
    // A successful kill request is not evidence of reaping. Capacity stays owned
    // until wait succeeds, including when another supervisor already killed it.
    Ok((requested, child.wait().await?))
}
