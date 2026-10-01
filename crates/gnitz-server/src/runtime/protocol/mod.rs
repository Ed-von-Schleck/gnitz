//! The protocol — the IPC wire format, the shared append-only log (SAL), the
//! lock-free worker→master ring (`w2m`), which also carries the worker's park on
//! the SAL, and the worker↔worker exchange `mesh`.
//!
//! A layer grouping, not a namespace callers name: `runtime/mod.rs` aliases these
//! submodules, and `crate::runtime::<mod>` is how the whole subsystem reaches
//! them.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub(super) mod mesh;
pub(super) mod sal;
pub(super) mod w2m;
pub(super) mod wire;
