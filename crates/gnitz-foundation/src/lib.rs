//! The process and the OS under it, and the probes and PRNG every crate's tests
//! and benchmarks share. Sibling leaves, NOT a unifying facade: each keeps its
//! own narrow surface.
//!   - `log`        — level/tag state. The `gnitz_*!` macros it backs are
//!     `#[macro_export]`ed, so they land at this crate's root, not in `log`
//!   - `env`        — `GNITZ_*` environment-variable overrides
//!   - `fault`      — debug-only `GNITZ_INJECT_*` fault-injection seams
//!   - `host`       — what the machine or container will give us (RAM budget,
//!     CPU quota), and why it refuses io_uring
//!   - `posix_io`   — the syscall idioms `std` lacks
//!   - `perf`       — cost probes for benchmarks: hardware events, context
//!     switches and resident-set bytes. Linux only
//!   - `process`    — a child process: tied to the thread that spawns it, awaited on its log
//!   - `local_server` — a `gnitz-server` this process runs for its own use: the
//!     shell's session server and Python's `gnitz.local_server`
//!   - `rng`        — the deterministic PRNG of tests and benchmarks, and `u64` bijections
//!
//! The server is Linux-only; a client is not. What a client links of this crate
//! builds on any Unix: `perf`, and the `posix_io` and `process` items only the
//! server calls, are gated to Linux.
//!
//! This crate names no other gnitz crate, which is what lets any of them name
//! it. Hashing is deliberately not here: it is one owner in `gnitz-wire`,
//! because a client computes some of the same digests.

pub mod env;
pub mod fault;
pub mod host;
pub mod local_server;
pub mod log;
#[cfg(target_os = "linux")]
pub mod perf;
pub mod posix_io;
pub mod process;
pub mod rng;
