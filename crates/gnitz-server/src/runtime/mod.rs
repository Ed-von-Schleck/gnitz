//! The server's processes and what they speak to each other.
//!
//! - Processes: `bootstrap` boots the server and forks the workers; `executor`
//!   serves clients on the master's `reactor`, through the `master` dispatcher;
//!   `worker` is a worker's event loop. `signals` and `affinity` set up each
//!   process; `listen` and `tls` are the client listeners.
//! - Within the master: `sync` holds the single-threaded primitives (oneshot,
//!   chan, the rwlock, select2, the wake queue) its tasks and its reactor share.
//! - Inter-process protocol: the master→worker `sal` (shared append-only log),
//!   the worker→master `w2m` rings, the worker↔worker exchange `mesh`, the
//!   futex `park` all three sleep through, and the `frame` encoder they write.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

// This module is the `runtime` rung, the topmost of the crate's three: `main.rs`
// parses argv and calls `server_main`. Nothing links this crate, so `pub` on an
// item here publishes nothing outside the binary; the visibility that carries
// meaning is `pub(in crate::catalog)` / `pub(in crate::query)` on the two rungs
// below, which is what keeps a rung's internals out of the rung above it.

// First: `#[macro_use]` reaches only the submodules declared after it.
#[macro_use]
mod fatal;

mod affinity;
mod bootstrap;
mod executor;
mod frame;
mod listen;
mod master;
mod mesh;
mod park;
mod reactor;
mod sal;
mod signals;
mod sync;
mod tls;
mod w2m;
mod worker;

pub(crate) use bootstrap::{ServerArgs, server_main};
pub(crate) use fatal::ServerExit;
pub(crate) use tls::{CertSource, TlsArgs, mint_token};

#[cfg(test)]
mod test_support;
