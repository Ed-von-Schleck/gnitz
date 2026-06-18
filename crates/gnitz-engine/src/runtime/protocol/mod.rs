//! L7 protocol — the IPC wire format, the shared append-only log (SAL), and the
//! lock-free worker→master ring (`w2m` + its `w2m_ring` backing store).
//!
//! Internal grouping, not a facade: `runtime/mod.rs` aliases these submodules so
//! the historical `crate::runtime::<mod>` paths keep resolving across the
//! subsystem.

pub(super) mod sal;
pub(super) mod w2m;
pub(super) mod w2m_ring;
pub(super) mod wire;
