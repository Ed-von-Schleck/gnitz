//! Source accessors — `CursorSource`, the unified per-row read interface over an
//! in-memory `Batch` or an mmap'd shard that the parent merge engine reads every
//! row through.
//!
//! Both variants own their backing data via `Rc`, so a `CursorSource` (and the
//! enclosing `ReadCursor`) is a self-contained owning value with no borrow
//! lifetime — what lets `CursorHandle` cross DAG/VM boundaries without a
//! `'static` transmute. The accessors are `pub(super)`: the parent merge engine
//! and the `output` drain submodule read through them.

use std::ptr;
use std::rc::Rc;

use super::super::batch::Batch;
use super::super::columnar::ColumnarSource;
use super::super::merge::UnifiedSource;
use super::super::shard_reader::MappedShard;
use crate::schema::SchemaDescriptor;

pub(super) enum CursorSource {
    /// Rc-owned in-memory batch.  The Rc keeps the data alive for the
    /// cursor's lifetime; multiple cursors can share a snapshot.
    Batch(Rc<Batch>),
    /// Rc-owned reference to a MappedShard.  The Rc keeps the mmap alive.
    Shard(Rc<MappedShard>),
}

impl CursorSource {
    #[inline]
    pub(super) fn get_weight(&self, row: usize) -> i64 {
        match self {
            CursorSource::Batch(b) => b.get_weight(row),
            CursorSource::Shard(s) => s.get_weight(row),
        }
    }

    #[inline]
    pub(super) fn get_null_word(&self, row: usize) -> u64 {
        match self {
            CursorSource::Batch(b) => b.get_null_word(row),
            CursorSource::Shard(s) => s.get_null_word(row),
        }
    }

    /// Column data as a slice, indexed by PAYLOAD column position.
    #[inline]
    pub(super) fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        match self {
            CursorSource::Batch(b) => b.get_col_ptr(row, payload_col, col_size),
            CursorSource::Shard(s) => s.get_col_ptr(row, payload_col, col_size),
        }
    }

    #[inline]
    pub(super) fn blob_ptr(&self) -> *const u8 {
        match self {
            CursorSource::Batch(b) => {
                if b.blob.is_empty() {
                    ptr::null()
                } else {
                    b.blob.as_ptr()
                }
            }
            CursorSource::Shard(s) => s.blob_ptr(),
        }
    }

    pub(super) fn blob_slice(&self) -> &[u8] {
        match self {
            CursorSource::Batch(b) => &b.blob,
            CursorSource::Shard(s) => s.blob_slice(),
        }
    }

    /// First row whose OPK bytes are `>= key`. A raw `memcmp` binary search
    /// over the order-preserving PK regions — correct at every PK width with no
    /// schema dependency. `key` must be exactly `pk_stride` OPK bytes.
    pub(super) fn find_lower_bound_bytes(&self, key: &[u8]) -> usize {
        match self {
            CursorSource::Batch(b) => b.find_lower_bound_bytes(key),
            CursorSource::Shard(s) => s.find_lower_bound_bytes(key),
        }
    }

    /// Galloping forward lower bound seeded at `hint` (this source's live
    /// position). Forwards to the per-source `advance_to`.
    pub(super) fn advance_to(&self, key: &[u8], hint: usize) -> usize {
        match self {
            CursorSource::Batch(b) => b.advance_to(key, hint),
            CursorSource::Shard(s) => s.advance_to(key, hint),
        }
    }

    #[inline]
    pub(super) fn get_pk_bytes(&self, row: usize) -> &[u8] {
        match self {
            CursorSource::Batch(b) => b.get_pk_bytes(row),
            CursorSource::Shard(s) => s.get_pk_bytes(row),
        }
    }

    /// Build a `UnifiedSource` view backed by either a `MemBatch`'s flat data
    /// buffer (always Raw regions) or a `MappedShard`'s mmap (Raw or Constant
    /// regions, indexed by payload position).
    ///
    /// Infallible: `MappedShard::open` validates all encoding constraints and
    /// region sizes at open time, so no arm here can fail.
    pub(super) fn to_unified(&self, schema: &SchemaDescriptor) -> UnifiedSource {
        match self {
            CursorSource::Batch(b) => super::super::merge::mem_batch_to_unified(&b.as_mem_batch(), schema),
            // `s` is `&Rc<MappedShard>`; the method call auto-derefs to `&MappedShard`.
            CursorSource::Shard(s) => s.to_unified(schema),
        }
    }
}

impl ColumnarSource for CursorSource {
    #[inline]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        CursorSource::get_pk_bytes(self, row)
    }
    #[inline]
    fn get_weight(&self, row: usize) -> i64 {
        CursorSource::get_weight(self, row)
    }
    #[inline]
    fn get_null_word(&self, row: usize) -> u64 {
        CursorSource::get_null_word(self, row)
    }
    #[inline]
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        CursorSource::get_col_ptr(self, row, payload_col, col_size)
    }
    #[inline]
    fn blob_slice(&self) -> &[u8] {
        CursorSource::blob_slice(self)
    }
}
