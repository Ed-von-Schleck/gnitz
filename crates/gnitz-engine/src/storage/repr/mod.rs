//! L2 storage representation — the pure in-memory batch repr and the operations
//! that work directly on it: region layout (`batch`), wire/shard serialization
//! (`batch_wire`), TLS buffer recycling (`batch_pool`), the columnar comparators
//! (`columnar`), sort-merge consolidation (`merge`), exchange repartition
//! (`scatter`), the fused k-way merge kernel (`heap`), the PK-probe filters
//! (`bloom`, `shard_filter`), the shard-image encoder and its atomic writer
//! (`shard_file`), and the shard-format constants (`layout`). The low-level
//! WAL-block framer lives in `gnitz_wire::wal` (the one definition client and
//! engine share); `batch_wire` and the SAL scatter writer call it.
//!
//! The shard *image* has one owner at this layer: `shard_file` encodes it,
//! `shard_reader` mmaps and validates it, and `layout` holds the format rules
//! both call. L3 reaches down for `MappedShard`; nothing here reaches up.
//!
//! `repr/` has **no outward facade of its own** — `storage/mod.rs` curates the
//! single combined storage surface and reaches into these submodules, re-exporting
//! each leaf's items and aliasing the submodules so the L3/LSM siblings keep their
//! `super::<mod>` / `crate::storage::<mod>` paths. Every production edge points
//! downward (schema/foundation) or sideways within this layer.

pub(super) mod batch;
pub mod batch_pool;
pub(super) mod batch_wire;
pub(super) mod bloom;
pub(super) mod columnar;
pub(super) mod heap;
pub(super) mod layout;
pub(super) mod merge;
pub(super) mod scatter;
pub(super) mod shard_file;
pub(super) mod shard_filter;
pub(in crate::storage) mod shard_reader;

// The one storage-level helper the shard reader names (`StorageError`), aliased
// so its files keep their `super::super::<mod>` paths.
use super::error;
