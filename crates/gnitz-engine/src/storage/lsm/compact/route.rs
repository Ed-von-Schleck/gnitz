//! Guard routing for shard compaction: the per-bucket guard-key lookup and the
//! `compact_shards` orchestration that merges N inputs into a single output shard.

use std::ffi::CStr;

use super::super::batch::Batch;
use super::super::error::StorageError;
use super::super::merge::BlobCacheGuard;
use super::super::shard_file::PkUniqueChecker;
use super::merge::open_and_merge;
use crate::schema::SchemaDescriptor;

pub(super) fn find_guard_for_key(guard_keys: &[u128], key: u128) -> usize {
    guard_keys.partition_point(|&g| g <= key).saturating_sub(1)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn compact_shards(
    input_files: &[&CStr],
    output_file: &CStr,
    schema: &SchemaDescriptor,
    table_id: u32,
    can_tag_pk_unique: bool,
) -> Result<(), StorageError> {
    let mut batch = Batch::with_schema(*schema, 1024);
    let mut blob_cache = BlobCacheGuard::acquire(schema, 1024);
    let mut checker = PkUniqueChecker::new();
    // `_key` is the order-preserving sort key (no longer the raw PK); copy the
    // PK from the source bytes so wide PKs are not truncated and narrow PKs are
    // not written as their OPK encoding.
    open_and_merge(input_files, schema, |_key, weight, shard, row| {
        let pk_bytes = shard.get_pk_bytes(row);
        if can_tag_pk_unique {
            checker.observe(pk_bytes, weight);
        }
        batch.append_row_from_source_bytes(pk_bytes, weight, shard, row, blob_cache.get_mut());
    })?;
    batch.write_as_shard_with_flags(output_file, table_id, checker.flags_if(can_tag_pk_unique))
}
