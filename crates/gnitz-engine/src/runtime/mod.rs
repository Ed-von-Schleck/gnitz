//! Runtime coordination subsystem: IPC channels, master/worker/executor/committer/bootstrap.

// Runtime is a CLOSED subsystem: only `server_main` + `MAX_WORKERS` escape the
// crate-wide surface (W8). The submodules are private `mod`, so their `pub`
// internals are reachable within runtime (cross-submodule refs via
// `crate::runtime::X::…` / `super::X::…` still resolve — descendants can name a
// private sibling module) but not from any other subsystem.
//
// `orchestration` (master/worker/executor/committer) and `protocol`
// (wire/sal/w2m/w2m_ring) are internal layer groupings, not facades: their
// submodules are aliased below so the historical `crate::runtime::<mod>` paths
// keep resolving for siblings (`bootstrap`, `reactor`) and the test dir.
mod bootstrap;
mod orchestration;
mod protocol;
mod reactor;

use orchestration::{committer, executor, master, peer, worker};
use protocol::{sal, w2m, w2m_ring, wire};

pub use bootstrap::server_main;
pub(crate) use protocol::sal::MAX_WORKERS;

#[cfg(test)]
mod tests;
