//! L0 foundation — an umbrella of sibling leaves, NOT a unifying facade. Each
//! leaf keeps its own narrow surface; this module only groups them so the
//! layering reads cleanly. The leaves share nothing but being leaves:
//!   - `log`        — the `gnitz_*` logging macros + level/tag state
//!   - `env`        — `GNITZ_*` environment-variable overrides
//!   - `fault`      — debug-only `GNITZ_INJECT_*` fault-injection seams
//!   - `host`       — what the machine or container will give us (RAM budget)
//!   - `xxh`        — XXH3 hashing
//!   - `posix_io`   — POSIX file-I/O and mmap wrappers. The server's IPC and
//!     socket tier is NOT here: it has no caller in this crate and lives beside
//!     the rings it serves, in `gnitz-server`
//!   - `worker_ctx` — per-process worker rank / count and fork role

#[macro_use]
pub mod log;
pub mod env;
pub mod fault;
pub mod host;
pub mod posix_io;
pub mod worker_ctx;
pub mod xxh;
