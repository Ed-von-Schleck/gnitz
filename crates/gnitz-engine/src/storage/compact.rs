//! Self-contained shard compaction: N-way merge of sorted shard files.

use std::ffi::CStr;

use super::columnar;
use super::error::StorageError;
use super::heap::{drive_merge, HeapNode, LoserTree};
use super::batch::Batch;
use super::merge::BlobCacheGuard;
use crate::schema::SchemaDescriptor;
use super::shard_reader::MappedShard;
#[cfg(test)]
use crate::util::{read_i64_le, read_u32_le};

#[cfg(test)]
use crate::schema::type_code;
#[cfg(test)]
use type_code::{I64 as TYPE_I64, U64 as TYPE_U64, STRING as TYPE_STRING};

// ---------------------------------------------------------------------------
// Guard output result (returned from merge_and_route)
// ---------------------------------------------------------------------------

pub struct GuardResult {
    pub guard_key: u128,
    pub filename: [u8; 256], // null-terminated
}

impl GuardResult {
    pub fn zeroed() -> Self {
        GuardResult {
            guard_key: 0,
            filename: [0u8; 256],
        }
    }

    pub fn filename_str(&self) -> &str {
        crate::util::cstr_from_buf(&self.filename)
    }
}

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

    /// Order-preserving sort key for the row at the cursor — the same key the
    /// flush/merge/read paths use (`pk_sort_key`). Exact for narrow PKs; the
    /// OPK 16-byte prefix for wide. Returning the raw `get_pk()` (as this did
    /// before) mis-ordered narrow compound and signed PKs, whose raw-LE order
    /// diverges from `compare_pk_bytes` — the order the input shards are
    /// physically written in.
    #[inline]
    fn peek_key(&self, shard: &MappedShard, schema: &SchemaDescriptor) -> u128 {
        debug_assert!(self.is_valid());
        super::merge::pk_sort_key(schema, shard.get_pk_bytes(self.position))
    }
}

#[cfg(test)]
fn is_null(shard: &MappedShard, row: usize, col_idx: usize, schema: &SchemaDescriptor) -> bool {
    let null_word = shard.get_null_word(row);
    (null_word >> schema.payload_idx(col_idx)) & 1 != 0
}

fn find_guard_for_key(guard_keys: &[u128], key: u128) -> usize {
    guard_keys.partition_point(|&g| g <= key).saturating_sub(1)
}

// ---------------------------------------------------------------------------
// Shared merge infrastructure
// ---------------------------------------------------------------------------

/// Open input shards, build cursors and heap, run N-way merge loop.
/// Calls `emit(key, net_weight, shard, row)` for each non-ghost consolidated row.
///
/// The payload-aware heap ordering ensures equal (PK, payload) entries appear
/// consecutively at the heap minimum, so the pending-group drain accumulates
/// their weights in O(1) per step.
///
/// File I/O lives here (not in `open_and_merge_inner`) so the cursor + heap
/// loop is the only piece monomorphised across the two comparator
/// specialisations — no duplicated open/error code in the binary.
fn open_and_merge(
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

    let cursors: Vec<ShardCursor> = (0..shards.len())
        .map(|i| ShardCursor::new(&shards[i]))
        .collect();

    // Select the comparator closure set once — never a per-comparison branch
    // (mirrors `merge.rs`). Wide (`pk_stride > 16`) adds a `compare_pk_bytes`
    // tiebreak on an OPK-prefix collision; the `schema_is_int_nonnull` split
    // is the orthogonal payload-comparator fast path.
    //
    // Wrap as a non-capturing closure: a direct fn-item reference would fix
    // the source-ref lifetime, conflicting with `_inner`'s HRTB Fn bound.
    #[allow(clippy::redundant_closure)]
    if schema.pk_is_wide() {
        if columnar::schema_is_int_nonnull(schema) {
            open_and_merge_wide_with(&shards, cursors, schema, emit,
                |s, a, ai, b, bi| columnar::compare_rows_int_nonnull(s, a, ai, b, bi));
        } else {
            open_and_merge_wide_with(&shards, cursors, schema, emit,
                |s, a, ai, b, bi| columnar::compare_rows(s, a, ai, b, bi));
        }
    } else if columnar::schema_is_int_nonnull(schema) {
        open_and_merge_narrow_with(&shards, cursors, schema, emit,
            |s, a, ai, b, bi| columnar::compare_rows_int_nonnull(s, a, ai, b, bi));
    } else {
        open_and_merge_narrow_with(&shards, cursors, schema, emit,
            |s, a, ai, b, bi| columnar::compare_rows(s, a, ai, b, bi));
    }
    Ok(())
}

/// Narrow-region (`pk_stride <= 16`) closure builder. The heap key is the
/// order-preserving `pk_sort_key`, which is injective at narrow stride, so
/// `less` orders the PK axis with a raw `a.key.cmp(&b.key)` and tie-breaks on
/// payload; `eq_payload` is payload-only (equal keys ⇒ equal PKs).
#[inline]
fn open_and_merge_narrow_with<RowCmp>(
    shards: &[MappedShard],
    cursors: Vec<ShardCursor>,
    schema: &SchemaDescriptor,
    emit: impl FnMut(u128, i64, &MappedShard, usize),
    row_cmp: RowCmp,
) where RowCmp: Fn(&SchemaDescriptor, &MappedShard, usize, &MappedShard, usize) -> std::cmp::Ordering + Copy
{
    // `less` reads `a.row` / `b.row` from the heap node directly — never
    // touches `cursors` — so it coexists with the `&mut cursors` borrow held
    // by `advance`. `source_idx` doubles as the shard index here.
    let less = |a: &HeapNode, b: &HeapNode| -> bool {
        match a.key.cmp(&b.key) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => row_cmp(schema, &shards[a.source_idx], a.row,
                                                         &shards[b.source_idx], b.row)
                == std::cmp::Ordering::Less,
        }
    };
    let eq_payload = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        row_cmp(schema, &shards[a_src], a_row,
                        &shards[b_src], b_row) == std::cmp::Ordering::Equal
    };
    open_and_merge_inner(shards, cursors, schema, emit, less, eq_payload);
}

/// Wide-region (`pk_stride > 16`) closure builder. `less` settles off the
/// order-preserving 16-byte OPK prefix (`node.key`) and only pays a
/// `compare_pk_bytes` column walk on a prefix collision; `eq_payload` adds the
/// `compare_pk_bytes` term so distinct wide PKs sharing a prefix + payload are
/// not folded into one summed group. Mirrors `merge.rs::merge_run_wide_with`.
#[inline]
fn open_and_merge_wide_with<RowCmp>(
    shards: &[MappedShard],
    cursors: Vec<ShardCursor>,
    schema: &SchemaDescriptor,
    emit: impl FnMut(u128, i64, &MappedShard, usize),
    row_cmp: RowCmp,
) where RowCmp: Fn(&SchemaDescriptor, &MappedShard, usize, &MappedShard, usize) -> std::cmp::Ordering + Copy
{
    let less = |a: &HeapNode, b: &HeapNode| -> bool {
        match a.key.cmp(&b.key) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => {
                match columnar::compare_pk_bytes(schema, shards[a.source_idx].get_pk_bytes(a.row),
                                                         shards[b.source_idx].get_pk_bytes(b.row)) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Equal => row_cmp(schema, &shards[a.source_idx], a.row,
                                                                 &shards[b.source_idx], b.row)
                        == std::cmp::Ordering::Less,
                }
            }
        }
    };
    let eq_payload = |a_src: usize, a_row: usize, b_src: usize, b_row: usize| {
        columnar::compare_pk_bytes(schema, shards[a_src].get_pk_bytes(a_row),
                                           shards[b_src].get_pk_bytes(b_row))
            == std::cmp::Ordering::Equal
        && row_cmp(schema, &shards[a_src], a_row,
                           &shards[b_src], b_row) == std::cmp::Ordering::Equal
    };
    open_and_merge_inner(shards, cursors, schema, emit, less, eq_payload);
}

/// Inner cursor + tree merge body, monomorphised on the `less` / `eq_payload`
/// closure set its caller selects per PK width so LLVM inlines a branch-free
/// hot loop across `LoserTree::build` and the per-row advance loop alike.
#[inline]
fn open_and_merge_inner<L, EQ>(
    shards: &[MappedShard],
    mut cursors: Vec<ShardCursor>,
    schema: &SchemaDescriptor,
    mut emit: impl FnMut(u128, i64, &MappedShard, usize),
    less: L,
    eq_payload: EQ,
) where
    L: Fn(&HeapNode, &HeapNode) -> bool + Copy,
    EQ: Fn(usize, usize, usize, usize) -> bool,
{
    let mut tree = LoserTree::build(
        cursors.len(),
        |i| {
            if cursors[i].is_valid() {
                Some((cursors[i].peek_key(&shards[i], schema), cursors[i].position))
            } else {
                None
            }
        },
        less,
    );
    drive_merge(
        &mut tree,
        less,
        |src| {
            cursors[src].advance(&shards[src]);
            if cursors[src].is_valid() {
                Some((cursors[src].peek_key(&shards[src], schema), cursors[src].position))
            } else {
                None
            }
        },
        eq_payload,
        |src, row| shards[src].get_weight(row),
        |group_src, group_row, group_key, w| {
            emit(group_key, w, &shards[group_src], group_row);
            std::ops::ControlFlow::Continue(())
        },
    );
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

pub fn compact_shards(
    input_files: &[&CStr],
    output_file: &CStr,
    schema: &SchemaDescriptor,
    table_id: u32,
) -> Result<(), StorageError> {
    let mut batch = Batch::with_schema(*schema, 1024);
    let mut blob_cache = BlobCacheGuard::acquire(schema, 1024);
    // `_key` is the order-preserving sort key (no longer the raw PK); copy the
    // PK from the source bytes so wide PKs are not truncated and narrow PKs are
    // not written as their OPK encoding.
    open_and_merge(input_files, schema, |_key, weight, shard, row| {
        batch.append_row_from_source_bytes(
            shard.get_pk_bytes(row), weight, shard, row, blob_cache.get_mut(),
        );
    })?;
    batch.write_as_shard(output_file, table_id)
}

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
) -> Result<usize, StorageError> {
    let num_guards = guard_keys.len();
    let out_dir_str = output_dir.to_str().unwrap_or("");

    let mut batches: Vec<Batch> = (0..num_guards)
        .map(|_| Batch::with_schema(*schema, 256))
        .collect();
    let mut blob_caches: Vec<BlobCacheGuard> = (0..num_guards)
        .map(|_| BlobCacheGuard::acquire(schema, 256))
        .collect();
    let out_filenames: Vec<String> = (0..num_guards)
        .map(|i| format!(
            "{}/shard_{}_{}_L{}_G{}.db",
            out_dir_str, table_id, lsn_tag, level_num, i
        ))
        .collect();

    // `key` is the order-preserving sort key — the guard-routing key (matching
    // `l1_guard_keys`, now also OPK). The PK itself is copied from the source
    // bytes so wide PKs are not truncated.
    open_and_merge(input_files, schema, |key, weight, shard, row| {
        let gi = find_guard_for_key(guard_keys, key);
        batches[gi].append_row_from_source_bytes(
            shard.get_pk_bytes(row), weight, shard, row, blob_caches[gi].get_mut(),
        );
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
        if let Err(e) = batches[i].write_as_shard(&cpath, table_id) {
            for fname in out_filenames.iter().take(i) {
                let _ = std::fs::remove_file(fname);
            }
            return Err(e);
        }
        let ri = result_count;
        assert!(
            ri < out_results.len(),
            "merge_and_route: out_results buffer too small ({} slots for {} guards)",
            out_results.len(), num_guards,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor};
    use std::fs;

    // Helper: build a minimal shard file in memory and write to disk
    fn write_test_shard(path: &str, pks: &[u64], weights: &[i64], schema: &SchemaDescriptor) {
        let mut batch = Batch::with_schema(*schema, pks.len());

        for i in 0..pks.len() {
            batch.extend_pk(pks[i] as u128);
            batch.extend_weight(&weights[i].to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());

            for (pi, _ci, col) in schema.payload_columns() {
                let cs = col.size() as usize;
                let mut val_bytes = vec![0u8; cs];
                let copy_len = cs.min(8);
                val_bytes[..copy_len].copy_from_slice(&pks[i].to_le_bytes()[..copy_len]);
                batch.extend_col(pi, &val_bytes);
            }
            batch.count += 1;
        }

        let cpath = std::ffi::CString::new(path).unwrap();
        batch.write_as_shard(&cpath, 0).unwrap();
    }

    fn make_test_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(TYPE_U64, 0),
                SchemaColumn::new(TYPE_I64, 0),
            ],
            &[0],
        )
    }

    #[test]
    fn test_compact_basic() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tmp/compact_test_basic");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard 1: keys 1, 3, 5
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 3, 5], &[1, 1, 1], &schema);

        // Shard 2: keys 2, 4, 6
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &[2, 4, 6], &[1, 1, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        // Read back merged shard
        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 6);

        // Verify sorted order
        let mut prev = 0u128;
        for i in 0..merged.count {
            let pk = merged.get_pk(i);
            assert!(pk > prev, "not sorted at row {}: {} <= {}", i, pk, prev);
            prev = pk;
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_weight_elimination() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tmp/compact_test_weight");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard 1: insert keys 1, 2, 3
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 2, 3], &[1, 1, 1], &schema);

        // Shard 2: delete key 2 (weight = -1)
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &[2], &[-1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        // Key 2 should be eliminated (net weight = 0)
        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 2);
        assert_eq!(merged.get_pk(0), 1);
        assert_eq!(merged.get_pk(1), 3);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_single_shard() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tmp/compact_test_single");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[10, 20, 30], &[1, 1, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 3);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_ghost_rows() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tmp/compact_test_ghost");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();
        // Shard with ghost rows (weight=0)
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 2, 3, 4, 5], &[1, 0, 1, 0, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 3); // only keys 1, 3, 5

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_guard_routing() {
        assert_eq!(find_guard_for_key(&[0, 100, 200], 50), 0);
        assert_eq!(find_guard_for_key(&[0, 100, 200], 100), 1);
        assert_eq!(find_guard_for_key(&[0, 100, 200], 150), 1);
        assert_eq!(find_guard_for_key(&[0, 100, 200], 200), 2);
        assert_eq!(find_guard_for_key(&[0, 100, 200], 999), 2);
    }

    #[test]
    fn test_guard_routing_empty() {
        assert_eq!(find_guard_for_key(&[] as &[u128], 42), 0);
    }

    #[test]
    fn test_guard_routing_single() {
        assert_eq!(find_guard_for_key(&[0u128], 0), 0);
        assert_eq!(find_guard_for_key(&[0u128], 999), 0);
    }

    #[test]
    fn test_compact_empty_input() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_empty");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();
        let output = dir.join("merged.db");
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs: [&CStr; 0] = [];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        // Output shard should exist with 0 rows
        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_all_cancel() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_cancel");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard 1: insert keys 1, 2, 3
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 2, 3], &[1, 1, 1], &schema);

        // Shard 2: delete all
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &[1, 2, 3], &[-1, -1, -1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_and_route_basic() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_route");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard with keys 10, 50, 150, 250
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[10, 50, 150, 250], &[1, 1, 1, 1], &schema);

        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cdir = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
        let inputs = [cs1.as_c_str()];

        // Two guards: [0, 100)  and [100, ∞)
        let guards: [u128; 2] = [0, 100];
        let mut results = [
            GuardResult::zeroed(),
            GuardResult::zeroed(),
        ];

        let n = merge_and_route(
            &inputs, &cdir, &guards, &schema,
            0, 1, 99, &mut results,
        ).unwrap();
        assert_eq!(n, 2); // both guards should have rows

        // Guard 0 should have keys 10, 50
        let fn0_end = results[0].filename.iter().position(|&b| b == 0).unwrap_or(256);
        let fn0 = std::str::from_utf8(&results[0].filename[..fn0_end]).unwrap();
        let cfn0 = std::ffi::CString::new(fn0).unwrap();
        let g0 = MappedShard::open(&cfn0, &schema, false).unwrap();
        assert_eq!(g0.count, 2);
        assert_eq!(g0.get_pk(0), 10);
        assert_eq!(g0.get_pk(1), 50);

        // Guard 1 should have keys 150, 250
        let fn1_end = results[1].filename.iter().position(|&b| b == 0).unwrap_or(256);
        let fn1 = std::str::from_utf8(&results[1].filename[..fn1_end]).unwrap();
        let cfn1 = std::ffi::CString::new(fn1).unwrap();
        let g1 = MappedShard::open(&cfn1, &schema, false).unwrap();
        assert_eq!(g1.count, 2);
        assert_eq!(g1.get_pk(0), 150);
        assert_eq!(g1.get_pk(1), 250);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_and_route_cleanup_on_partial_finalize_failure() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_route_cleanup");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        let s1 = dir.join("in1.db");
        let s2 = dir.join("in2.db");
        write_test_shard(s1.to_str().unwrap(), &[10, 50], &[1, 1], &schema);
        write_test_shard(s2.to_str().unwrap(), &[150, 250], &[1, 1], &schema);

        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        let guards: [u128; 2] = [0, 100];

        // table_id=0, level_num=1, lsn_tag=99 → second output is shard_0_99_L1_G1.db
        // Block it with a directory so finalize fails for guard 1.
        let blocker = dir.join("shard_0_99_L1_G1.db");
        fs::create_dir_all(&blocker).unwrap();

        let cdir = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
        let mut results = [GuardResult::zeroed(), GuardResult::zeroed()];
        let rc = merge_and_route(&inputs, &cdir, &guards, &schema, 0, 1, 99, &mut results);

        assert!(rc.is_err(), "expected failure, got {:?}", rc);
        let guard0_file = dir.join("shard_0_99_L1_G0.db");
        assert!(!guard0_file.exists(), "guard 0 output should have been cleaned up");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_string_column() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_string");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // Schema: u64 PK + STRING payload
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TYPE_U64, 0),
                SchemaColumn::new(TYPE_STRING, 0),
            ],
            &[0],
        );

        // Build shard with short strings
        let mut batch = Batch::with_schema(schema, 3);
        for pk in [1u64, 2, 3] {
            batch.extend_pk(pk as u128);
            batch.extend_weight(&1i64.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());

            // Write a short string: "hi" (2 bytes, inline)
            let mut str_struct = [0u8; 16];
            str_struct[0..4].copy_from_slice(&2u32.to_le_bytes()); // length=2
            str_struct[4] = b'h'; str_struct[5] = b'i'; // prefix
            batch.extend_col(0, &str_struct);
            batch.count += 1;
        }
        let s1 = dir.join("s1.db");
        let cpath = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        batch.write_as_shard(&cpath, 0).unwrap();

        // Compact it (single shard, should roundtrip)
        let output = dir.join("merged.db");
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();
        let inputs = [cpath.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 3);

        // Verify string data survived
        for row in 0..3 {
            let col_data = merged.get_col_ptr(row, 0, 16);
            let str_len = read_u32_le(col_data, 0);
            assert_eq!(str_len, 2);
            assert_eq!(col_data[4], b'h');
            assert_eq!(col_data[5], b'i');
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_nullable_column() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_nullable");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // Schema: u64 PK + nullable i64 payload
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TYPE_U64, 0),
                SchemaColumn::new(TYPE_I64, 1),
            ],
            &[0],
        );

        // Build shard: key 1 = non-null (42), key 2 = null
        let mut batch = Batch::with_schema(schema, 2);
        // Row 1: non-null
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes()); // no nulls
        batch.extend_col(0, &42i64.to_le_bytes());
        batch.count += 1;

        // Row 2: null column
        batch.extend_pk(2u128);
        batch.extend_weight(&1i64.to_le_bytes());
        // null bit for col_idx=1, pk_index=0 → payload_idx = 0 → bit 0
        batch.extend_null_bmp(&1u64.to_le_bytes());
        batch.extend_col(0, &0i64.to_le_bytes());
        batch.count += 1;

        let s1 = dir.join("s1.db");
        let cpath = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        batch.write_as_shard(&cpath, 0).unwrap();

        // Compact
        let output = dir.join("merged.db");
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();
        let inputs = [cpath.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 2);

        // Row 0: not null
        assert!(!is_null(&merged, 0, 1, &schema));
        let val = read_i64_le(merged.get_col_ptr(0, 0, 8), 0);
        assert_eq!(val, 42);

        // Row 1: null
        assert!(is_null(&merged, 1, 1, &schema));

        let _ = fs::remove_dir_all(&dir);
    }

    // -- 3-column helpers for reduce-output-pattern tests --------------------

    fn make_3col_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(TYPE_U64, 0),
                SchemaColumn::new(TYPE_I64, 0),
                SchemaColumn::new(TYPE_I64, 0),
            ],
            &[0],
        )
    }

    /// Write a shard with 3-column rows: (pk, weight, col1_val, col2_val).
    fn write_3col_shard(
        path: &str,
        rows: &[(u64, i64, i64, i64)],
        schema: &SchemaDescriptor,
    ) {
        let mut batch = Batch::with_schema(*schema, rows.len());
        for &(pk, w, c1, c2) in rows {
            batch.extend_pk(pk as u128);
            batch.extend_weight(&w.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            batch.extend_col(0, &c1.to_le_bytes());
            batch.extend_col(1, &c2.to_le_bytes());
            batch.count += 1;
        }
        let cpath = std::ffi::CString::new(path).unwrap();
        batch.write_as_shard(&cpath, 0).unwrap();
    }

    /// Read all rows from a 3-col shard as (pk, weight, col1, col2).
    fn read_3col_shard(path: &str, schema: &SchemaDescriptor) -> Vec<(u64, i64, i64, i64)> {
        let cpath = std::ffi::CString::new(path).unwrap();
        let shard = MappedShard::open(&cpath, schema, false).unwrap();
        let mut rows = Vec::new();
        for i in 0..shard.count {
            let pk = shard.get_pk(i) as u64;
            let w = shard.get_weight(i);
            let c1 = read_i64_le(shard.get_col_ptr(i, 0, 8), 0);
            let c2 = read_i64_le(shard.get_col_ptr(i, 1, 8), 0);
            rows.push((pk, w, c1, c2));
        }
        rows
    }

    /// The exact pattern that triggered the bug: same PK, different payload
    /// (different agg_val column) across shards. Retractions must cancel
    /// with matching insertions from earlier shards.
    #[test]
    fn test_compact_same_pk_different_payload_cancels() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_3col_cancel");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_3col_schema();

        // Shard 1 (tick 1): insert (pk=1, group=0, sum=5000)
        let s1 = dir.join("s1.db");
        write_3col_shard(s1.to_str().unwrap(), &[(1, 1, 0, 5000)], &schema);

        // Shard 2 (tick 2): retract sum=5000, insert sum=10000
        let s2 = dir.join("s2.db");
        write_3col_shard(s2.to_str().unwrap(), &[
            (1, -1, 0, 5000),
            (1,  1, 0, 10000),
        ], &schema);

        // Shard 3 (tick 3): retract sum=10000, insert sum=15000
        let s3 = dir.join("s3.db");
        write_3col_shard(s3.to_str().unwrap(), &[
            (1, -1, 0, 10000),
            (1,  1, 0, 15000),
        ], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cs3 = std::ffi::CString::new(s3.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str(), cs3.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        let rows = read_3col_shard(output.to_str().unwrap(), &schema);
        assert_eq!(rows.len(), 1, "expected 1 surviving row, got {:?}", rows);
        assert_eq!(rows[0], (1, 1, 0, 15000));

        let _ = fs::remove_dir_all(&dir);
    }

    /// Multiple groups with interleaved shards: ensures the pending-group
    /// algorithm handles group boundaries correctly across PKs.
    #[test]
    fn test_compact_multi_group_reduce_pattern() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_3col_multi");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_3col_schema();

        // Group A (pk=1) and Group B (pk=2), 3 ticks each
        let s1 = dir.join("s1.db");
        write_3col_shard(s1.to_str().unwrap(), &[
            (1, 1, 0, 100),
            (2, 1, 1, 200),
        ], &schema);

        let s2 = dir.join("s2.db");
        write_3col_shard(s2.to_str().unwrap(), &[
            (1, -1, 0, 100), (1, 1, 0, 300),
            (2, -1, 1, 200), (2, 1, 1, 400),
        ], &schema);

        let s3 = dir.join("s3.db");
        write_3col_shard(s3.to_str().unwrap(), &[
            (1, -1, 0, 300), (1, 1, 0, 600),
            (2, -1, 1, 400), (2, 1, 1, 800),
        ], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cs3 = std::ffi::CString::new(s3.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str(), cs3.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        let rows = read_3col_shard(output.to_str().unwrap(), &schema);
        assert_eq!(rows.len(), 2, "expected 2 surviving rows, got {:?}", rows);
        assert_eq!(rows[0], (1, 1, 0, 600));
        assert_eq!(rows[1], (2, 1, 1, 800));

        let _ = fs::remove_dir_all(&dir);
    }

    /// 10 shards simulating 10 reduce ticks for 1 group — the exact scenario
    /// from the test_heavy_agg_500k failure.
    #[test]
    fn test_compact_10_tick_reduce_single_group() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_3col_10tick");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_3col_schema();
        let mut shard_paths = Vec::new();

        // Tick 1: insert sum=5000
        let p = dir.join("t1.db");
        write_3col_shard(p.to_str().unwrap(), &[(1, 1, 0, 5000)], &schema);
        shard_paths.push(p);

        // Ticks 2-10: retract old, insert new
        for tick in 2..=10u64 {
            let old_sum = (tick - 1) * 5000;
            let new_sum = tick * 5000;
            let p = dir.join(format!("t{}.db", tick));
            write_3col_shard(p.to_str().unwrap(), &[
                (1, -1, 0, old_sum as i64),
                (1,  1, 0, new_sum as i64),
            ], &schema);
            shard_paths.push(p);
        }

        let output = dir.join("merged.db");
        let cstrs: Vec<_> = shard_paths.iter()
            .map(|p| std::ffi::CString::new(p.to_str().unwrap()).unwrap())
            .collect();
        let inputs: Vec<_> = cstrs.iter().map(|c| c.as_c_str()).collect();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        let rows = read_3col_shard(output.to_str().unwrap(), &schema);
        assert_eq!(rows.len(), 1, "expected 1 row after 10-tick consolidation, got {}", rows.len());
        assert_eq!(rows[0], (1, 1, 0, 50000), "expected final sum=50000");

        let _ = fs::remove_dir_all(&dir);
    }

    /// merge_and_route with same-PK-different-payload entries: verifies the
    /// fix applies to the guard-routed path too (shares open_and_merge).
    #[test]
    fn test_merge_and_route_same_pk_different_payload() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_3col_route");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_3col_schema();

        let s1 = dir.join("s1.db");
        write_3col_shard(s1.to_str().unwrap(), &[
            (10, 1, 0, 100),
            (20, 1, 1, 200),
        ], &schema);

        let s2 = dir.join("s2.db");
        write_3col_shard(s2.to_str().unwrap(), &[
            (10, -1, 0, 100), (10, 1, 0, 300),
            (20, -1, 1, 200), (20, 1, 1, 400),
        ], &schema);

        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cdir = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
        let inputs = [cs1.as_c_str(), cs2.as_c_str()];

        let guard_keys: Vec<u128> = vec![0]; // single guard
        let mut results = vec![GuardResult::zeroed()];

        let n = merge_and_route(&inputs, &cdir, &guard_keys, &schema, 99, 1, 1, &mut results).unwrap();
        assert!(n > 0, "merge_and_route should produce output");

        let fn0 = crate::util::cstr_from_buf(&results[0].filename);
        let rows = read_3col_shard(fn0, &schema);
        assert_eq!(rows.len(), 2, "expected 2 rows, got {:?}", rows);
        assert_eq!(rows[0], (10, 1, 0, 300));
        assert_eq!(rows[1], (20, 1, 1, 400));

        let _ = fs::remove_dir_all(&dir);
    }

    /// Bug 5: compact with checksums enabled (validate_checksums = true).
    /// Regression test confirming valid data passes checksum validation.
    #[test]
    fn test_compact_with_checksums_enabled() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_checksums");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 3, 5], &[1, 1, 1], &schema);
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &[2, 4, 6], &[1, 1, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).expect("compact with checksums enabled must succeed for valid data");

        let merged = MappedShard::open(&cout, &schema, true).unwrap();
        assert_eq!(merged.count, 6);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Bug 3: compact 10K+ rows across two shards. Validates the streaming
    /// write path (write_shard_streaming) under compaction.
    #[test]
    fn test_compact_large_dataset() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_large");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard 1: odd keys 1..9999
        let pks1: Vec<u64> = (0..5000).map(|i| i * 2 + 1).collect();
        let weights1 = vec![1i64; 5000];
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &pks1, &weights1, &schema);

        // Shard 2: even keys 2..10000
        let pks2: Vec<u64> = (1..=5000).map(|i| i * 2).collect();
        let weights2 = vec![1i64; 5000];
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &pks2, &weights2, &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0).unwrap();

        let merged = MappedShard::open(&cout, &schema, true).unwrap();
        assert_eq!(merged.count, 10000);

        // Verify sorted order
        let mut prev = 0u128;
        for i in 0..merged.count {
            let pk = merged.get_pk(i);
            assert!(pk > prev, "not sorted at row {}: {} <= {}", i, pk, prev);
            prev = pk;
        }

        let _ = fs::remove_dir_all(&dir);
    }

    /// Bug 1: merge_and_route with guard_keys=[(200,0)] and input data with
    /// keys below 200. All keys must be routed to the single guard (index 0)
    /// and be readable in the output.
    #[test]
    fn test_merge_and_route_keys_below_first_guard() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_below_guard");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard with keys 50, 100, 150, 250 — two are below guard key 200
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[50, 100, 150, 250], &[1, 1, 1, 1], &schema);

        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cdir = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
        let inputs = [cs1.as_c_str()];

        let guard_keys: Vec<u128> = vec![200]; // single guard at key 200
        let mut results = vec![GuardResult::zeroed()];

        let n = merge_and_route(&inputs, &cdir, &guard_keys, &schema, 42, 2, 1, &mut results).unwrap();
        assert!(n > 0, "merge_and_route should produce output");

        let fn0 = crate::util::cstr_from_buf(&results[0].filename);
        let cpath = std::ffi::CString::new(fn0).unwrap();
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 4, "all 4 keys must be present in output");

        // Verify all keys readable
        for &pk in &[50u64, 100, 150, 250] {
            assert!(shard.find_row_index(pk as u128).is_some(), "key {} not found in output shard", pk);
        }

        let _ = fs::remove_dir_all(&dir);
    }

    // -- Wide / compound / signed PK compaction (OPK ordering) ---------------

    /// Write a shard from explicit raw-PK-byte rows. `rows` is
    /// `(pk_bytes, weight, payload_i64_vals)`; rows must already be in
    /// `compare_pk_bytes` order (compaction assumes sorted inputs).
    fn write_bytes_pk_shard(
        path: &str,
        schema: &SchemaDescriptor,
        rows: &[(Vec<u8>, i64, Vec<i64>)],
    ) {
        let mut batch = Batch::with_schema(*schema, rows.len().max(1));
        for (pk, w, vals) in rows {
            batch.extend_pk_bytes(pk);
            batch.extend_weight(&w.to_le_bytes());
            batch.extend_null_bmp(&0u64.to_le_bytes());
            for (pi, v) in vals.iter().enumerate() {
                batch.extend_col(pi, &v.to_le_bytes());
            }
            batch.count += 1;
        }
        let cpath = std::ffi::CString::new(path).unwrap();
        batch.write_as_shard(&cpath, 0).unwrap();
    }

    fn assert_compare_pk_bytes_sorted(shard: &MappedShard, schema: &SchemaDescriptor) {
        for i in 1..shard.count {
            assert_ne!(
                columnar::compare_pk_bytes(schema, shard.get_pk_bytes(i - 1), shard.get_pk_bytes(i)),
                std::cmp::Ordering::Greater,
                "merged shard not compare_pk_bytes-sorted at row {i}",
            );
        }
    }

    fn pk2(a: u64, b: u64) -> Vec<u8> {
        let mut v = a.to_le_bytes().to_vec();
        v.extend_from_slice(&b.to_le_bytes());
        v
    }

    /// Regression: a narrow compound `(U64, U64)` PK whose raw-LE u128
    /// order diverges from `compare_pk_bytes`. The input shards are physically
    /// written in compound order; before the `peek_key → pk_sort_key` fix the
    /// N-way merge compared raw-LE and produced mis-ordered output plus missed
    /// cross-shard consolidation.
    #[test]
    fn test_compact_narrow_compound_opk_order() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_compound_opk");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // (U64, U64) PK + I64 payload.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TYPE_U64, 0),
                SchemaColumn::new(TYPE_U64, 0),
                SchemaColumn::new(TYPE_I64, 0),
            ],
            &[0, 1],
        );
        // Raw-LE u128 of these concatenations is scrambled vs compound order:
        // (3,0)=3 < (2,3) < (1,5) < (1,9) — yet compound order is
        // (1,5) < (1,9) < (2,3) < (3,0).
        assert!(pk2(1, 5) < pk2(1, 9)); // byte-lex sanity for col0==1

        // Shard 1 (compound-sorted): (1,5) v10, (2,3) v30 weight +1.
        let s1 = dir.join("s1.db");
        write_bytes_pk_shard(s1.to_str().unwrap(), &schema, &[
            (pk2(1, 5), 1, vec![10]),
            (pk2(2, 3), 1, vec![30]),
        ]);
        // Shard 2 (compound-sorted): (1,9) v20, (2,3) v30 weight -1, (3,0) v40.
        let s2 = dir.join("s2.db");
        write_bytes_pk_shard(s2.to_str().unwrap(), &schema, &[
            (pk2(1, 9), 1, vec![20]),
            (pk2(2, 3), -1, vec![30]),
            (pk2(3, 0), 1, vec![40]),
        ]);

        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(dir.join("merged.db").to_str().unwrap()).unwrap();
        compact_shards(&[cs1.as_c_str(), cs2.as_c_str()], &cout, &schema, 0).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        // (2,3) cancels (+1 -1 = 0). The cross-shard duplicate must fold, which
        // requires the two (2,3) entries to be adjacent at the heap root.
        assert_eq!(merged.count, 3, "expected 3 surviving rows (the (2,3) pair cancels)");
        assert_compare_pk_bytes_sorted(&merged, &schema);

        let present: Vec<Vec<u8>> = (0..merged.count).map(|i| merged.get_pk_bytes(i).to_vec()).collect();
        assert_eq!(present, vec![pk2(1, 5), pk2(1, 9), pk2(3, 0)]);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Regression for a single narrow signed (`I64`) PK: negatives sort
    /// last under raw-LE (zero-extended) but first under `compare_pk_bytes`.
    #[test]
    fn test_compact_narrow_signed_opk_order() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_signed_opk");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // I64 PK + I64 payload. Single signed column → not pk_is_fast.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TYPE_I64, 0),
                SchemaColumn::new(TYPE_I64, 0),
            ],
            &[0],
        );
        let k = |v: i64| v.to_le_bytes().to_vec();

        // Shard 1 (signed order): -5, 3.
        let s1 = dir.join("s1.db");
        write_bytes_pk_shard(s1.to_str().unwrap(), &schema, &[
            (k(-5), 1, vec![1]),
            (k(3), 1, vec![3]),
        ]);
        // Shard 2 (signed order): -2, 10.
        let s2 = dir.join("s2.db");
        write_bytes_pk_shard(s2.to_str().unwrap(), &schema, &[
            (k(-2), 1, vec![2]),
            (k(10), 1, vec![4]),
        ]);

        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(dir.join("merged.db").to_str().unwrap()).unwrap();
        compact_shards(&[cs1.as_c_str(), cs2.as_c_str()], &cout, &schema, 0).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 4);
        assert_compare_pk_bytes_sorted(&merged, &schema);
        let present: Vec<i64> = (0..merged.count)
            .map(|i| i64::from_le_bytes(merged.get_pk_bytes(i).try_into().unwrap()))
            .collect();
        assert_eq!(present, vec![-5, -2, 3, 10], "must be signed-sorted, not raw-LE");

        let _ = fs::remove_dir_all(&dir);
    }

    /// Wide (`pk_stride = 24`) prefix collision: two PKs share their
    /// order-preserving 16-byte prefix (col0, col1) but differ in the trailing
    /// column, with identical payloads. The `compare_pk_bytes` tiebreak in the
    /// wide comparator must keep them as two distinct rows (no fold).
    #[test]
    fn test_compact_wide_prefix_collision_no_fold() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_wide_prefix");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // (U64, U64, U64) PK (stride 24) + U64 payload.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TYPE_U64, 0),
                SchemaColumn::new(TYPE_U64, 0),
                SchemaColumn::new(TYPE_U64, 0),
                SchemaColumn::new(TYPE_U64, 0),
            ],
            &[0, 1, 2],
        );
        assert_eq!(schema.pk_stride(), 24);
        let pk3 = |a: u64, b: u64, c: u64| {
            let mut v = a.to_le_bytes().to_vec();
            v.extend_from_slice(&b.to_le_bytes());
            v.extend_from_slice(&c.to_le_bytes());
            v
        };

        // (1,1,100) and (1,1,200) share their first 16 bytes (col0,col1), differ
        // in col2 — identical payloads. Separate shards, both weight +1.
        let s1 = dir.join("s1.db");
        write_bytes_pk_shard(s1.to_str().unwrap(), &schema, &[(pk3(1, 1, 100), 1, vec![7])]);
        let s2 = dir.join("s2.db");
        write_bytes_pk_shard(s2.to_str().unwrap(), &schema, &[(pk3(1, 1, 200), 1, vec![7])]);

        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(dir.join("merged.db").to_str().unwrap()).unwrap();
        compact_shards(&[cs1.as_c_str(), cs2.as_c_str()], &cout, &schema, 0).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 2, "prefix-colliding distinct wide PKs must not fold");
        assert_compare_pk_bytes_sorted(&merged, &schema);
        let present: Vec<Vec<u8>> = (0..merged.count).map(|i| merged.get_pk_bytes(i).to_vec()).collect();
        assert_eq!(present, vec![pk3(1, 1, 100), pk3(1, 1, 200)]);

        let _ = fs::remove_dir_all(&dir);
    }
}
