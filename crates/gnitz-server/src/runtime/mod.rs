//! Runtime coordination subsystem: IPC channels, master/worker/executor/committer/bootstrap.

// This module is the `runtime` rung, and this crate is nothing else: `main.rs`
// parses argv and calls `server_main`. Everything below is therefore reachable
// only from within this binary — `pub` on an item here publishes nothing, and
// the visibility that carries meaning is the one on the engine side of the
// seam, where `gnitz-engine` is a library.
//
// The submodules are private `mod`, so cross-submodule refs resolve through
// `crate::runtime::X::…` / `super::X::…` — descendants can name a private
// sibling module.
//
// `orchestration` (master/worker/executor/committer) and `protocol`
// (wire/sal/w2m/w2m_ring) group the submodules by layer; `posix` holds the
// syscall tier that serves them — sockets, eventfd/futex/memfd and the
// `MAP_SHARED` rings — which is why it is here and not in the engine's
// `foundation::posix_io`. `affinity` is a peer rather than part of `posix`
// because it carries the CPU placement policy, not just the syscalls that
// apply it. The aliases below are the convention, not a
// compatibility shim: every reference names `crate::runtime::<mod>`, and
// nothing in the crate spells out the grouping directory — so the aliases are
// what those paths resolve through.
mod affinity;
mod bootstrap;
mod orchestration;
mod posix;
mod protocol;
mod reactor;
mod tls;

use orchestration::{committer, executor, lsn, master, peer, worker};
use protocol::{sal, w2m, w2m_ring, wire};

pub(crate) use bootstrap::server_main;
pub(crate) use protocol::sal::MAX_WORKERS;
pub(crate) use tls::TlsCli;

#[cfg(test)]
mod tests;
