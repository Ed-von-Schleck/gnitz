//! L2 storage representation — the pure in-memory batch repr and the operations
//! that work directly on it: region layout (`batch`), wire/shard serialization
//! (`batch_wire`), TLS buffer recycling (`batch_pool`), the OPK lower-bound
//! search (`seek`), sort-merge consolidation (`merge`), the row-selecting and
//! row-copying passes — PK routing and the column-first scatter
//! (`scatter`) —, the N-way min-merge tournament (`heap`), the shard PK-probe
//! filter (`shard_filter`), the shard-image encoder and writer (`shard_file`),
//! and the shard-format constants (`layout`). The low-level
//! WAL-block framer lives in `gnitz_wire::wal` (the one definition client and
//! engine share); `batch_wire` and the SAL scatter writer call it. The
//! row-at-a-time system-table writer over a batch is `batch_builder`.
//!
//! The shard *image* has one owner at this layer: `shard_file` encodes it,
//! `shard_reader` mmaps and validates it, and `layout` holds the format rules
//! both call. L3 reaches down for `MappedShard`; nothing here reaches up.
//!
//! `repr/` has **no outward facade of its own** — `storage/mod.rs` curates the
//! single combined storage surface and re-exports each leaf's items. Every
//! production edge points downward (schema) or sideways within this layer.

pub(super) mod batch;
pub(super) mod batch_builder;
pub(super) mod batch_pool;
pub(super) mod batch_wire;
pub(super) mod heap;
pub(super) mod layout;
pub(super) mod merge;
pub(super) mod scatter;
pub(super) mod seek;
pub(super) mod shard_file;
pub(super) mod shard_filter;
pub(in crate::storage) mod shard_reader;
