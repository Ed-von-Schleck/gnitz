//! L0 foundation — an umbrella of sibling leaves, NOT a unifying facade. Each
//! leaf keeps its own narrow surface; this module only groups them so the
//! layering reads cleanly. The leaves share nothing but being leaves:
//!   - `log`        — the `gnitz_*` logging macros + level/tag state
//!   - `codec`      — little-endian byte pack/unpack
//!   - `xxh`        — XXH3 hashing
//!   - `posix_io`   — fd I/O, fsync, fallocate, madvise, server socket, panic guard
//!   - `syscall`    — non-blocking IPC syscalls (eventfd, futex, memfd, mmap_shared)
//!   - `worker_ctx` — per-process worker rank / count

#[macro_use]
pub(crate) mod log;
pub(crate) mod codec;
pub(crate) mod posix_io;
pub(crate) mod syscall;
pub(crate) mod worker_ctx;
pub(crate) mod xxh;
