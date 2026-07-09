//! L2 storage representation — the pure in-memory batch repr and the operations
//! that work directly on it: region layout (`batch`), wire/shard serialization
//! (`batch_wire`), TLS buffer recycling (`batch_pool`), the columnar comparators
//! (`columnar`), sort-merge consolidation (`merge`), exchange repartition
//! (`scatter`), the fused k-way merge kernel (`heap`), range-key helpers
//! (`range_key`), the PK-probe filters (`bloom`, `xor8`), and the pure byte
//! codecs of the on-disk formats: the WAL block (`wal`), the shard image
//! (`shard_file`), and the shard-format constants (`layout`).
//!
//! `repr/` has **no outward facade of its own** — `storage/mod.rs` curates the
//! single combined storage surface and reaches into these submodules, re-exporting
//! each leaf's items and aliasing the submodules so the L3/LSM siblings keep their
//! `super::<mod>` / `crate::storage::<mod>` paths. Every edge points downward
//! (schema/foundation) or sideways within this layer.

pub(super) mod batch;
pub(crate) mod batch_pool;
pub(super) mod batch_wire;
pub(super) mod bloom;
pub(super) mod columnar;
pub(super) mod heap;
pub(super) mod layout;
pub(super) mod merge;
pub(super) mod range_key;
pub(super) mod scatter;
pub(super) mod shard_file;
pub(super) mod wal;
pub(super) mod xor8;
