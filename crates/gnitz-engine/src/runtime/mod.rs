//! Runtime coordination subsystem: IPC channels, master/worker/executor/committer/bootstrap.

// Runtime is a CLOSED subsystem: only `server_main` + `MAX_WORKERS` escape the
// crate-wide surface (W8). The submodules are private `mod`, so their `pub`
// internals are reachable within runtime (cross-submodule refs via
// `crate::runtime::X::…` / `super::X::…` still resolve — descendants can name a
// private sibling module) but not from any other subsystem.
//
// `orchestration` (master/worker/executor/committer) and `protocol`
// (wire/sal/w2m/w2m_ring) group the submodules by layer. The aliases below are
// the convention, not a compatibility shim: every reference names
// `crate::runtime::<mod>`, and nothing in the crate spells out the grouping
// directory — so the aliases are what those paths resolve through.
mod bootstrap;
mod orchestration;
mod protocol;
mod reactor;
mod tls;

use orchestration::{committer, executor, lsn, master, peer, worker};
use protocol::{sal, w2m, w2m_ring, wire};

pub use bootstrap::{server_main, TlsCli};
pub(crate) use protocol::sal::MAX_WORKERS;

#[cfg(test)]
mod tests;
