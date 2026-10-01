//! What both suites bound a wait with.

use std::future::Future;
use std::time::Duration;

use tokio::runtime::Runtime;

/// How long anything that is on its way may take. Paid in full only by a
/// regression, which then fails rather than hangs.
pub const PATIENCE: Duration = Duration::from_secs(60);

/// `f`'s output. A verb or a driver left pending is the failure most of these
/// tests look for.
pub fn settled<F: Future>(rt: &Runtime, f: F) -> F::Output {
    rt.block_on(async { tokio::time::timeout(PATIENCE, f).await })
        .expect("resolves rather than hangs")
}
