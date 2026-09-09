//! The process and the OS under it — everything a gnitz crate needs from
//! `libc` or the environment, and nothing else. Sibling leaves, NOT a unifying
//! facade: each keeps its own narrow surface.
//!   - `log`        — level/tag state. The four `gnitz_*!` macros it backs are
//!     `#[macro_export]`ed, so they land at this crate's root, not in `log`
//!   - `env`        — `GNITZ_*` environment-variable overrides
//!   - `fault`      — debug-only `GNITZ_INJECT_*` fault-injection seams
//!   - `host`       — what the machine or container will give us (RAM budget)
//!   - `posix_io`   — file-I/O, mmap and the syscall idioms around them,
//!     including the anonymous and reserved mappings the server builds its SAL
//!     and W2M rings on. The socket tier is not here; it lives beside the
//!     reactor that owns the fds, in `gnitz-server`
//!
//! This crate names no other gnitz crate, which is what lets any of them name
//! it. Hashing is deliberately not here: it is one owner in `gnitz-wire`,
//! because a client computes some of the same digests.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub mod env;
pub mod fault;
pub mod host;
pub mod log;
pub mod posix_io;
