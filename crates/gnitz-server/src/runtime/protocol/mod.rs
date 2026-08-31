//! L7 protocol — the IPC wire format, the shared append-only log (SAL), the
//! lock-free worker→master ring (`w2m`) and the master→worker wake channel
//! (`m2w`).
//!
//! A layer grouping, not a namespace callers name: `runtime/mod.rs` aliases these
//! submodules, and `crate::runtime::<mod>` is how the whole subsystem reaches
//! them.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub(super) mod m2w;
pub(super) mod sal;
pub(super) mod w2m;
pub(super) mod wire;
