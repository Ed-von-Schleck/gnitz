//! L0 foundation — an umbrella of sibling leaves, NOT a unifying facade. Each
//! leaf keeps its own narrow surface; this module only groups them so the
//! layering reads cleanly. The leaves share nothing but being leaves:
//!   - `log`        — the `gnitz_*` logging macros + level/tag state
//!   - `env`        — numeric `GNITZ_*` environment-variable overrides
//!   - `fault`      — debug-only `GNITZ_INJECT_*` fault-injection seams
//!   - `xxh`        — XXH3 hashing
//!   - `posix_io`   — POSIX I/O and Linux syscall wrappers (file I/O, sockets,
//!     mmap + its unaligned `*_raw` accessors, eventfd/futex/memfd IPC)
//!   - `worker_ctx` — per-process worker rank / count

#[macro_use]
pub(crate) mod log;
pub(crate) mod env;
pub(crate) mod fault;
pub(crate) mod posix_io;
pub(crate) mod worker_ctx;
pub(crate) mod xxh;
