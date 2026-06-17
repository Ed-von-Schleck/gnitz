//! L2 storage representation — the pure in-memory batch repr and the operations
//! that work directly on it: region layout (`batch`), wire/shard serialization
//! (`batch_wire`), TLS buffer recycling (`batch_pool`), the columnar comparators
//! (`columnar`), sort-merge consolidation (`merge`), exchange repartition
//! (`scatter`), the fused k-way merge kernel (`heap`), range-key helpers
//! (`range_key`), and the PK-probe filters (`bloom`, `xor8`).
//!
//! `repr/` has **no outward facade of its own** — `storage/mod.rs` curates the
//! single combined storage surface and reaches into these submodules, re-exporting
//! each leaf's items and aliasing the submodules so the L3/LSM siblings keep their
//! `super::<mod>` / `crate::storage::<mod>` paths. The only edge that points *up*
//! out of this layer is `batch_wire → {wal, shard_file, error}` (the documented
//! repr→lsm serialization exception); it reaches those siblings via `super::super::`.

pub(super) mod batch;
pub(super) mod batch_wire;
pub(crate) mod batch_pool;
pub(super) mod bloom;
pub(super) mod columnar;
pub(super) mod heap;
pub(super) mod merge;
pub(super) mod scatter;
pub(super) mod range_key;
pub(super) mod xor8;
