//! Public cursor handle — `CursorHandle`, the owning wrapper the catalog/DAG hold.

use std::rc::Rc;

use super::{create_read_cursor, ReadCursor};
use super::super::batch::Batch;
use super::super::shard_reader::MappedShard;
use crate::schema::SchemaDescriptor;

/// Owns a `ReadCursor`.  Since the cursor now owns its data via `Rc`s in
/// each `CursorSource`, this is a thin newtype — the separate "owned
/// snapshots" vector that previously kept `Rc<Batch>` alive alongside
/// transmuted-to-`'static` `MemBatch` views is gone.
pub struct CursorHandle {
    pub(crate) cursor: ReadCursor,
}

impl CursorHandle {
    /// Build a CursorHandle from owned in-memory batches (no shards).
    ///
    /// Convenience wrapper used by test code.
    #[cfg(test)]
    pub(crate) fn from_owned(
        snapshots: &[Rc<Batch>],
        schema: crate::schema::SchemaDescriptor,
    ) -> CursorHandle {
        create_cursor_from_snapshots(snapshots, &[], schema)
    }

    /// Mutable access to the inner ReadCursor.
    pub(crate) fn cursor_mut(&mut self) -> &mut ReadCursor {
        &mut self.cursor
    }
}

/// Build a CursorHandle from Rust-owned snapshots + shard Rcs.
///
/// Each snapshot's `Rc<Batch>` and shard's `Rc<MappedShard>` is cloned into
/// the corresponding `CursorSource` entry, keeping the data alive for the
/// cursor's entire lifetime without any unsafe lifetime extension.
pub(crate) fn create_cursor_from_snapshots(
    snapshots: &[Rc<Batch>],
    shard_arcs: &[Rc<MappedShard>],
    schema: SchemaDescriptor,
) -> CursorHandle {
    let cursor = create_read_cursor(snapshots, shard_arcs, schema);
    CursorHandle { cursor }
}
