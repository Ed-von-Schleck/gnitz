//! Runtime coordination subsystem: IPC channels, master/worker/executor/committer/bootstrap.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.
//!
//! Tests no single module owns live in `suites/`, a declared `mod suites;` child
//! of this module: they reach this subsystem's surface, not any one module's
//! private items.

// This module is the `runtime` rung, the topmost of the crate's three: `main.rs`
// parses argv and calls `server_main`. Nothing links this crate, so `pub` on an
// item here publishes nothing outside the binary; the visibility that carries
// meaning is `pub(in crate::catalog)` / `pub(in crate::query)` on the two rungs
// below, which is what keeps a rung's internals out of the rung above it.
//
// The submodules are private `mod`, so cross-submodule refs resolve through
// `crate::runtime::X::…` / `super::X::…` — descendants can name a private
// sibling module.
//
// `orchestration` and `protocol` group the submodules by layer, and the aliases
// below are what `crate::runtime::<mod>` resolves through — no path names the
// grouping directory. There is no syscall tier: every syscall lives in the
// module holding the protocol or policy it serves.
mod affinity;
mod bootstrap;
mod orchestration;
mod protocol;
mod reactor;
mod tls;

use orchestration::{committer, executor, lsn, master, peer, worker};
use protocol::{m2w, sal, w2m, wire};

pub(crate) use bootstrap::server_main;
pub(crate) use tls::TlsCli;

#[cfg(test)]
mod suites;
#[cfg(test)]
mod test_support;
