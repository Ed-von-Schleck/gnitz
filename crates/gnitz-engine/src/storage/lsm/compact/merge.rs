//! N-way (PK, payload) merge kernel for shard compaction.
//!
//! Opens the input shards, builds per-shard cursors + the keyless loser tree,
//! and runs the fused merge + inline consolidation loop through `drive_merge`
//! (§8 cluster-1 — the sole pending-group drain owner). The
//! `with_payload_cmp!` / `with_pk_ord!` dispatch selects the payload comparator
//! then the stride once each, never per comparison, so the inner loop is fully
//! monomorphised. `merge_and_route` is the routing-aware merge that splits the
//! consolidated stream across the guard buckets.

use std::ffi::CStr;

use super::super::batch::Batch;
use super::super::columnar::PkOrd;
use super::super::error::StorageError;
use super::super::heap::{drive_merge, HeapNode, LoserTree};
use super::super::merge::{pack_pk_be, BlobCacheGuard};
use super::super::shard_file::PkUniqueChecker;
use super::super::shard_reader::MappedShard;
use super::super::{with_payload_cmp, with_pk_ord};
use super::route::{find_guard_for_key, GuardResult};
use crate::schema::SchemaDescriptor;

// ---------------------------------------------------------------------------
// Shard cursor (position + ghost skip)
// ---------------------------------------------------------------------------

struct ShardCursor {
    position: usize,
    count: usize,
}

impl ShardCursor {
    fn new(shard: &MappedShard) -> Self {
        let mut c = ShardCursor {
            position: 0,
            count: shard.count,
        };
        c.skip_ghosts(shard);
        c
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.position < self.count
    }

    #[inline]
    fn advance(&mut self, shard: &MappedShard) {
        if self.is_valid() {
            self.position += 1;
            self.skip_ghosts(shard);
        }
    }

    #[inline]
    fn skip_ghosts(&mut self, shard: &MappedShard) {
        self.position = shard.next_non_ghost(self.position);
    }
}

// ---------------------------------------------------------------------------
// Shared merge infrastructure
// ---------------------------------------------------------------------------

/// Open input shards, build cursors and heap, run N-way merge loop.
/// Calls `emit(key, net_weight, shard, row)` for each non-ghost consolidated row,
/// where `key` is the row's `pack_pk_be` guard-routing key — re-derived from its
/// OPK bytes in the emit, since the keyless heap carries no cached key.
///
/// The payload-aware heap ordering ensures equal (PK, payload) entries appear
/// consecutively at the heap minimum, so the pending-group drain accumulates
/// their weights in O(1) per step.
///
/// File I/O lives here (not in `open_and_merge_inner`) so the cursor + heap loop
/// is the only piece monomorphised across the payload×stride specialisations —
/// no duplicated open/error code in the binary.
pub(super) fn open_and_merge(
    input_files: &[&CStr],
    schema: &SchemaDescriptor,
    emit: impl FnMut(u128, i64, &MappedShard, usize),
) -> Result<(), StorageError> {
    let mut shards: Vec<MappedShard> = Vec::with_capacity(input_files.len());
    for f in input_files {
        match MappedShard::open(f, schema, true) {
            Ok(s) => shards.push(s),
            Err(e) => return Err(e),
        }
    }

    let cursors: Vec<ShardCursor> = (0..shards.len()).map(|i| ShardCursor::new(&shards[i])).collect();

    // Dispatch the payload comparator (outer) then the stride comparator (inner),
    // each once — never a per-comparison branch. The old narrow/wide fork is gone:
    // `pk_ord` compares the full OPK bytes at every width.
    with_payload_cmp!(schema, open_and_merge_with_payload, &shards, cursors, schema, emit);
    Ok(())
}

/// Payload-dispatch layer; defers to the stride dispatch in `open_and_merge_pk`.
#[inline]
fn open_and_merge_with_payload<RowCmp>(
    shards: &[MappedShard],
    cursors: Vec<ShardCursor>,
    schema: &SchemaDescriptor,
    emit: impl FnMut(u128, i64, &MappedShard, usize),
    row_cmp: RowCmp,
) where
    RowCmp: Fn(&SchemaDescriptor, &MappedShard, usize, &MappedShard, usize) -> std::cmp::Ordering + Copy,
{
    with_pk_ord!(schema, open_and_merge_pk, shards, cursors, schema, emit, row_cmp)
}

/// N-way merge closure builder. The keyless heap reads each player's OPK bytes
/// through `(source_idx, row)`: the stride-dispatched `pk_ord` settles the PK
/// axis, then the payload `row_cmp`. `same_pk` is the width-agnostic OPK byte
/// equality (so two distinct wide PKs sharing a 16-byte prefix never fold);
/// `eq_payload` is the payload term.
#[inline]
fn open_and_merge_pk<RowCmp, PK>(
    shards: &[MappedShard],
    cursors: Vec<ShardCursor>,
    schema: &SchemaDescriptor,
    emit: impl FnMut(u128, i64, &MappedShard, usize),
    row_cmp: RowCmp,
    pk_ord: PK,
) where
    RowCmp: Fn(&SchemaDescriptor, &MappedShard, usize, &MappedShard, usize) -> std::cmp::Ordering + Copy,
    PK: PkOrd,
{
    // `less` reads `a.row` / `b.row` from the heap node directly — never
    // touches `cursors` — so it coexists with the `&mut cursors` borrow held
    // by `advance`. `source_idx` doubles as the shard index here.
    let less = |a: &HeapNode, b: &HeapNode| -> bool {
        let (a_src, a_row) = (a.source_idx as usize, a.row as usize);
        let (b_src, b_row) = (b.source_idx as usize, b.row as usize);
        match pk_ord.cmp(shards[a_src].get_pk_bytes(a_row), shards[b_src].get_pk_bytes(b_row)) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => {
                row_cmp(schema, &shards[a_src], a_row, &shards[b_src], b_row) == std::cmp::Ordering::Less
            }
        }
    };
    let same_pk = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        shards[a_src].get_pk_bytes(a_row) == shards[b_src].get_pk_bytes(b_row)
    };
    let eq_payload = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        row_cmp(schema, &shards[a_src], a_row, &shards[b_src], b_row) == std::cmp::Ordering::Equal
    };
    open_and_merge_inner(shards, cursors, emit, less, same_pk, eq_payload);
}

/// Inner cursor + tree merge body, monomorphised on the `less` / `same_pk` /
/// `eq_payload` closures its caller selects per stride and payload so LLVM
/// inlines a branch-free hot loop across `LoserTree::build` and the per-row
/// advance loop alike. The keyless node carries only the row; the guard-routing
/// `u128` passed to `emit` is re-derived from the group row's OPK bytes via
/// `pack_pk_be` (= the old cached `group_key`).
#[inline]
fn open_and_merge_inner<L, SP, EQ>(
    shards: &[MappedShard],
    mut cursors: Vec<ShardCursor>,
    mut emit: impl FnMut(u128, i64, &MappedShard, usize),
    less: L,
    same_pk: SP,
    eq_payload: EQ,
) where
    L: Fn(&HeapNode, &HeapNode) -> bool + Copy,
    SP: Fn(usize, usize, usize, usize) -> bool,
    EQ: Fn(usize, usize, usize, usize) -> bool,
{
    let mut tree = LoserTree::build(
        cursors.len(),
        |i| cursors[i].is_valid().then(|| cursors[i].position as u32),
        less,
    );
    drive_merge(
        &mut tree,
        less,
        |src| {
            cursors[src].advance(&shards[src]);
            cursors[src].is_valid().then(|| cursors[src].position as u32)
        },
        same_pk,
        eq_payload,
        |src, row| shards[src].get_weight(row),
        |group_src, group_row, w| {
            emit(
                pack_pk_be(shards[group_src].get_pk_bytes(group_row)),
                w,
                &shards[group_src],
                group_row,
            );
            std::ops::ControlFlow::Continue(())
        },
    );
}

// ---------------------------------------------------------------------------
// Routing-aware merge entry point
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn merge_and_route(
    input_files: &[&CStr],
    output_dir: &CStr,
    guard_keys: &[u128],
    schema: &SchemaDescriptor,
    table_id: u32,
    level_num: u32,
    lsn_tag: u64,
    out_results: &mut [GuardResult],
    can_tag_pk_unique: bool,
) -> Result<usize, StorageError> {
    // Empty guard_keys would make find_guard_for_key return 0 (via
    // saturating_sub(1)) and then index batches[0] out of bounds. Callers
    // always pass a non-empty list, but don't rely on that silently.
    if guard_keys.is_empty() {
        return Err(StorageError::InvalidPath);
    }
    let num_guards = guard_keys.len();
    let out_dir_str = output_dir.to_str().unwrap_or("");

    let mut batches: Vec<Batch> = (0..num_guards).map(|_| Batch::with_schema(*schema, 256)).collect();
    let mut blob_caches: Vec<BlobCacheGuard> = (0..num_guards).map(|_| BlobCacheGuard::acquire(schema, 256)).collect();
    let mut checkers: Vec<PkUniqueChecker> = (0..num_guards).map(|_| PkUniqueChecker::new()).collect();
    let out_filenames: Vec<String> = (0..num_guards)
        .map(|i| format!("{out_dir_str}/shard_{table_id}_{lsn_tag}_L{level_num}_G{i}.db"))
        .collect();

    // `key` is the order-preserving sort key — the guard-routing key (matching
    // `l1_guard_keys`, now also OPK). The PK itself is copied from the source
    // bytes so wide PKs are not truncated.
    open_and_merge(input_files, schema, |key, weight, shard, row| {
        let gi = find_guard_for_key(guard_keys, key);
        let pk_bytes = shard.get_pk_bytes(row);
        if can_tag_pk_unique {
            checkers[gi].observe(pk_bytes, weight);
        }
        batches[gi].append_row_from_source_bytes(pk_bytes, weight, shard, row, blob_caches[gi].get_mut());
    })?;

    // Validate all output paths fit in GuardResult.filename before writing anything.
    for i in 0..num_guards {
        if batches[i].count > 0 && out_filenames[i].len() >= 256 {
            return Err(StorageError::InvalidPath);
        }
    }

    let mut result_count: usize = 0;
    for i in 0..num_guards {
        if batches[i].count == 0 {
            continue;
        }
        let cpath = std::ffi::CString::new(out_filenames[i].as_str()).unwrap();
        let flags = checkers[i].flags_if(can_tag_pk_unique);
        if let Err(e) = batches[i].write_as_shard_with_flags(&cpath, table_id, flags) {
            // Only guards with rows were written (the loop `continue`s on empty
            // shards), so only those filenames exist on disk. Removing a path for
            // an empty guard could `unlink` an unrelated file sharing the name.
            for (j, fname) in out_filenames.iter().enumerate().take(i) {
                if batches[j].count > 0 {
                    let _ = std::fs::remove_file(fname);
                }
            }
            return Err(e);
        }
        let ri = result_count;
        assert!(
            ri < out_results.len(),
            "merge_and_route: out_results buffer too small ({} slots for {} guards)",
            out_results.len(),
            num_guards,
        );
        out_results[ri].guard_key = guard_keys[i];
        let name_bytes = out_filenames[i].as_bytes();
        let len = name_bytes.len().min(255);
        out_results[ri].filename[..len].copy_from_slice(&name_bytes[..len]);
        out_results[ri].filename[len] = 0;
        result_count += 1;
    }

    Ok(result_count)
}
