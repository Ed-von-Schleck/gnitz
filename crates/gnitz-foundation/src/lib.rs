//! The process and the OS under it. Sibling leaves, NOT a unifying facade: each
//! keeps its own narrow surface.
//!   - `log`        — level/tag state. The `gnitz_*!` macros it backs are
//!     `#[macro_export]`ed, so they land at this crate's root, not in `log`
//!   - `env`        — `GNITZ_*` environment-variable overrides
//!   - `fault`      — debug-only `GNITZ_INJECT_*` fault-injection seams
//!   - `host`       — what the machine or container will give us (RAM budget, CPU quota)
//!   - `posix_io`   — the syscall idioms `std` lacks: an EINTR retry and an integer
//!     socket option
//!   - `perf`       — cost probes: instructions retired, context switches and
//!     resident-set bytes, for benchmarks and cost-claim tests. Linux only
//!
//! The server is Linux-only; a client is not. What a client links of this crate
//! builds on any Unix, so `perf` is the Linux-only part.
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
#[cfg(target_os = "linux")]
pub mod perf;
pub mod posix_io;
