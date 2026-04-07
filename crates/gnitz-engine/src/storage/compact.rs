//! Self-contained shard compaction: N-way merge of sorted shard files.

use std::collections::HashMap;
use std::ffi::CStr;

use super::columnar;
use super::heap::MergeHeap;
use super::memtable::OwnedBatch;
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

#[repr(C)]
pub struct GuardResult {
    pub guard_key_lo: u64,
    pub guard_key_hi: u64,
    pub filename: [u8; 256], // null-terminated
}

impl GuardResult {
    pub fn zeroed() -> Self {
        GuardResult {
            guard_key_lo: 0,
            guard_key_hi: 0,
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
    shard_idx: usize,
    position: usize,
    count: usize,
}

impl ShardCursor {
    fn new(shard_idx: usize, shard: &MappedShard) -> Self {
        let mut c = ShardCursor {
            shard_idx,
            position: 0,
            count: shard.count,
        };
        c.skip_ghosts(shard);
        c
    }

    fn is_valid(&self) -> bool {
        self.position < self.count
    }

    fn advance(&mut self, shard: &MappedShard) {
        if self.is_valid() {
            self.position += 1;
            self.skip_ghosts(shard);
        }
    }

    fn skip_ghosts(&mut self, shard: &MappedShard) {
        if !shard.has_ghosts { return; }
        while self.position < self.count {
            if shard.get_weight(self.position) != 0 {
                return;
            }
            self.position += 1;
        }
    }

    fn peek_key(&self, shard: &MappedShard) -> u128 {
        if self.is_valid() {
            shard.get_pk(self.position)
        } else {
            u128::MAX
        }
    }

    #[allow(dead_code)]
    fn weight(&self, shard: &MappedShard) -> i64 {
        if self.is_valid() {
            shard.get_weight(self.position)
        } else {
            0
        }
    }
}

#[cfg(test)]
fn is_null(shard: &MappedShard, row: usize, col_idx: usize, pk_index: usize) -> bool {
    let null_word = shard.get_null_word(row);
    let payload_idx = if col_idx < pk_index { col_idx } else { col_idx - 1 };
    (null_word >> payload_idx) & 1 != 0
}

// ---------------------------------------------------------------------------
// Guard key routing (binary search)
// ---------------------------------------------------------------------------

fn find_guard_for_key(guard_keys: &[(u64, u64)], key: u128) -> usize {
    let n = guard_keys.len();
    if n == 0 {
        return 0;
    }
    let mut lo = 0usize;
    let mut hi = n - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let (gk_lo, gk_hi) = guard_keys[mid];
        if crate::util::make_pk(gk_lo, gk_hi) <= key {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo
}

// ---------------------------------------------------------------------------
// Shared merge infrastructure
// ---------------------------------------------------------------------------

/// Open input shards, build cursors and heap, run N-way merge loop.
/// Calls `emit(key, net_weight, shard, row)` for each non-ghost consolidated row.
fn open_and_merge(
    input_files: &[&CStr],
    schema: &SchemaDescriptor,
    mut emit: impl FnMut(u128, i64, &MappedShard, usize),
) -> Result<(), i32> {
    let mut shards: Vec<MappedShard> = Vec::with_capacity(input_files.len());
    for f in input_files {
        match MappedShard::open(f, schema, true) {
            Ok(s) => shards.push(s),
            Err(e) => return Err(e),
        }
    }

    let mut cursors: Vec<ShardCursor> = Vec::with_capacity(shards.len());
    for i in 0..shards.len() {
        cursors.push(ShardCursor::new(i, &shards[i]));
    }

    // Payload-aware heap ordering ensures matching (PK, payload) entries
    // are adjacent in the tournament tree, which is required for correct
    // weight accumulation in the pending-group drain below.
    fn make_shard_less<'a>(
        cursors: &'a [ShardCursor],
        shards: &'a [MappedShard],
        schema: &'a SchemaDescriptor,
    ) -> impl Fn(&super::heap::HeapNode, &super::heap::HeapNode) -> bool + 'a {
        move |a, b| {
            if a.key != b.key {
                return a.key < b.key;
            }
            columnar::compare_rows(
                schema,
                &shards[cursors[a.idx].shard_idx], cursors[a.idx].position,
                &shards[cursors[b.idx].shard_idx], cursors[b.idx].position,
            ) == std::cmp::Ordering::Less
        }
    }

    let mut tree = MergeHeap::build(
        cursors.len(),
        |i| {
            if cursors[i].is_valid() {
                Some(cursors[i].peek_key(&shards[cursors[i].shard_idx]))
            } else {
                None
            }
        },
        &make_shard_less(&cursors, &shards, schema),
    );

    // Pending-group drain: pop one entry at a time, accumulate weight while
    // (PK, payload) matches the pending group, flush on change.  The payload-
    // aware heap ensures matching entries are delivered consecutively.
    let mut has_pending = false;
    let mut pending_shard_idx: usize = 0;
    let mut pending_row: usize = 0;
    let mut pending_key: u128 = 0;
    let mut pending_weight: i64 = 0;

    while !tree.is_empty() {
        let ci = tree.min_idx();
        let si = cursors[ci].shard_idx;
        let row = cursors[ci].position;
        let cur_key = shards[si].get_pk(row);
        let cur_weight = shards[si].get_weight(row);

        let same_group = has_pending
            && cur_key == pending_key
            && columnar::compare_rows(
                schema,
                &shards[pending_shard_idx], pending_row,
                &shards[si], row,
            ) == std::cmp::Ordering::Equal;

        if same_group {
            pending_weight += cur_weight;
        } else {
            if has_pending && pending_weight != 0 {
                emit(pending_key, pending_weight, &shards[pending_shard_idx], pending_row);
            }
            pending_shard_idx = si;
            pending_row = row;
            pending_key = cur_key;
            pending_weight = cur_weight;
            has_pending = true;
        }

        cursors[ci].advance(&shards[si]);
        let new_key = if cursors[ci].is_valid() {
            Some(cursors[ci].peek_key(&shards[cursors[ci].shard_idx]))
        } else {
            None
        };
        tree.advance(ci, new_key, &make_shard_less(&cursors, &shards, schema));
    }

    // Flush last pending group
    if has_pending && pending_weight != 0 {
        emit(pending_key, pending_weight, &shards[pending_shard_idx], pending_row);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

pub fn compact_shards(
    input_files: &[&CStr],
    output_file: &CStr,
    schema: &SchemaDescriptor,
    table_id: u32,
) -> i32 {
    let mut batch = OwnedBatch::with_schema(*schema, 1024);
    let mut blob_cache: HashMap<(u64, usize), usize> = HashMap::new();
    if let Err(e) = open_and_merge(input_files, schema, |key, weight, shard, row| {
        batch.append_row_from_source(key, weight, shard, row, &mut blob_cache);
    }) {
        return e;
    }
    batch.write_as_shard(output_file, table_id)
}

pub fn merge_and_route(
    input_files: &[&CStr],
    output_dir: &CStr,
    guard_keys: &[(u64, u64)],
    schema: &SchemaDescriptor,
    table_id: u32,
    level_num: u32,
    lsn_tag: u64,
    out_results: &mut [GuardResult],
) -> i32 {
    let num_guards = guard_keys.len();
    let out_dir_str = output_dir.to_str().unwrap_or("");

    let mut batches: Vec<OwnedBatch> = (0..num_guards)
        .map(|_| OwnedBatch::with_schema(*schema, 256))
        .collect();
    let mut blob_caches: Vec<HashMap<(u64, usize), usize>> = (0..num_guards)
        .map(|_| HashMap::new())
        .collect();
    let out_filenames: Vec<String> = (0..num_guards)
        .map(|i| format!(
            "{}/shard_{}_{}_L{}_G{}.db",
            out_dir_str, table_id, lsn_tag, level_num, i
        ))
        .collect();

    if let Err(e) = open_and_merge(input_files, schema, |key, weight, shard, row| {
        let gi = find_guard_for_key(guard_keys, key);
        batches[gi].append_row_from_source(key, weight, shard, row, &mut blob_caches[gi]);
    }) {
        return e;
    }

    // Validate all output paths fit in GuardResult.filename before writing anything.
    for i in 0..num_guards {
        if batches[i].count > 0 && out_filenames[i].len() >= 256 {
            return -(libc::ENAMETOOLONG as i32);
        }
    }

    let mut result_count: i32 = 0;
    for i in 0..num_guards {
        if batches[i].count == 0 {
            continue;
        }
        let cpath = std::ffi::CString::new(out_filenames[i].as_str()).unwrap();
        let frc = batches[i].write_as_shard(&cpath, table_id);
        if frc != 0 {
            for j in 0..i {
                let _ = std::fs::remove_file(&out_filenames[j]);
            }
            return frc;
        }
        let ri = result_count as usize;
        assert!(
            ri < out_results.len(),
            "merge_and_route: out_results buffer too small ({} slots for {} guards)",
            out_results.len(), num_guards,
        );
        out_results[ri].guard_key_lo = guard_keys[i].0;
        out_results[ri].guard_key_hi = guard_keys[i].1;
        let name_bytes = out_filenames[i].as_bytes();
        let len = name_bytes.len().min(255);
        out_results[ri].filename[..len].copy_from_slice(&name_bytes[..len]);
        out_results[ri].filename[len] = 0;
        result_count += 1;
    }

    result_count
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, SHORT_STRING_THRESHOLD, compare_german_strings};
    use std::fs;

    // Helper: build a minimal shard file in memory and write to disk
    fn write_test_shard(path: &str, pks: &[u64], weights: &[i64], schema: &SchemaDescriptor) {
        let pk_index = schema.pk_index as usize;
        let mut batch = OwnedBatch::with_schema(*schema, pks.len());

        for i in 0..pks.len() {
            batch.pk_lo.extend_from_slice(&pks[i].to_le_bytes());
            batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            batch.weight.extend_from_slice(&weights[i].to_le_bytes());
            batch.null_bmp.extend_from_slice(&0u64.to_le_bytes());

            let mut pi = 0;
            for ci in 0..schema.num_columns as usize {
                if ci == pk_index { continue; }
                let cs = schema.columns[ci].size as usize;
                let mut val_bytes = vec![0u8; cs];
                let copy_len = cs.min(8);
                val_bytes[..copy_len].copy_from_slice(&pks[i].to_le_bytes()[..copy_len]);
                batch.col_data[pi].extend_from_slice(&val_bytes);
                pi += 1;
            }
            batch.count += 1;
        }

        let cpath = std::ffi::CString::new(path).unwrap();
        assert_eq!(batch.write_as_shard(&cpath, 0), 0);
    }

    fn make_test_schema() -> SchemaDescriptor {
        let mut s = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        s.columns[0] = SchemaColumn { type_code: TYPE_U64, size: 8, nullable: 0, _pad: 0 };
        s.columns[1] = SchemaColumn { type_code: TYPE_I64, size: 8, nullable: 0, _pad: 0 };
        s
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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 3); // only keys 1, 3, 5

        let _ = fs::remove_dir_all(&dir);
    }

    // Build a long German string struct (len > 12) and backing blob.
    // The full `length` bytes are stored at blob[heap_offset..heap_offset+length].
    // Bytes [0..4] of the string → prefix field at struct[4..8].
    // Bytes [4..length] → blob[heap_offset+4..heap_offset+length].
    fn make_long_string(data: &[u8]) -> ([u8; 16], Vec<u8>) {
        assert!(data.len() > SHORT_STRING_THRESHOLD);
        let mut s = [0u8; 16];
        s[0..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
        s[4..8].copy_from_slice(&data[0..4]);
        // heap_offset = 0
        s[8..16].copy_from_slice(&0u64.to_le_bytes());
        let blob = data.to_vec();
        (s, blob)
    }

    #[test]
    fn test_string_comparison() {
        // Short strings
        let mut a = [0u8; 16];
        let mut b = [0u8; 16];
        // len=3, prefix="abc"
        a[0..4].copy_from_slice(&3u32.to_le_bytes());
        a[4] = b'a'; a[5] = b'b'; a[6] = b'c';
        b[0..4].copy_from_slice(&3u32.to_le_bytes());
        b[4] = b'a'; b[5] = b'b'; b[6] = b'd';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), std::cmp::Ordering::Less);

        // Equal strings
        b[6] = b'c';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), std::cmp::Ordering::Equal);

        // Different length, same prefix
        b[0..4].copy_from_slice(&4u32.to_le_bytes());
        b[7] = b'z';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), std::cmp::Ordering::Less);

        // Long strings — equal except last byte (regression: off-by-4 in blob offset)
        let data_a: Vec<u8> = b"hello_world_long_A".to_vec(); // len=18
        let data_b_lt: Vec<u8> = b"hello_world_long_B".to_vec();
        let (sa, blob_a) = make_long_string(&data_a);
        let (sb_lt, blob_b_lt) = make_long_string(&data_b_lt);
        assert_eq!(
            compare_german_strings(&sa, &blob_a, &sb_lt, &blob_b_lt),
            std::cmp::Ordering::Less,
        );

        // Long strings — equal (common same_group path)
        let (sb_eq, blob_b_eq) = make_long_string(&data_a);
        assert_eq!(
            compare_german_strings(&sa, &blob_a, &sb_eq, &blob_b_eq),
            std::cmp::Ordering::Equal,
        );

        // Long strings — prefix difference (early exit)
        let data_b_prefix: Vec<u8> = b"aello_world_long_A".to_vec();
        let (sb_prefix, blob_b_prefix) = make_long_string(&data_b_prefix);
        assert_eq!(
            compare_german_strings(&sb_prefix, &blob_b_prefix, &sa, &blob_a),
            std::cmp::Ordering::Less,
        );

        // Mixed short (len=10) vs long (len=20) with same prefix
        let short_data = b"0123456789"; // len=10, ≤ 12 → short
        let mut s_short = [0u8; 16];
        s_short[0..4].copy_from_slice(&10u32.to_le_bytes());
        s_short[4..8].copy_from_slice(&short_data[0..4]);
        s_short[8..14].copy_from_slice(&short_data[4..]);
        let long_data: Vec<u8> = b"01234567890123456789".to_vec(); // len=20
        let (s_long, blob_long) = make_long_string(&long_data);
        // short ("0123456789") < long ("01234567890123456789") — same prefix, shorter len
        assert_eq!(
            compare_german_strings(&s_short, &[], &s_long, &blob_long),
            std::cmp::Ordering::Less,
        );
    }

    #[test]
    fn test_guard_routing() {
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 50), 0);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 100), 1);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 150), 1);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 200), 2);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 999), 2);
    }

    #[test]
    fn test_guard_routing_empty() {
        assert_eq!(find_guard_for_key(&[], 42), 0);
    }

    #[test]
    fn test_guard_routing_single() {
        assert_eq!(find_guard_for_key(&[(0, 0)], 0), 0);
        assert_eq!(find_guard_for_key(&[(0, 0)], 999), 0);
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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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
        let guards = [(0u64, 0u64), (100u64, 0u64)];
        let mut results = [
            GuardResult { guard_key_lo: 0, guard_key_hi: 0, filename: [0u8; 256] },
            GuardResult { guard_key_lo: 0, guard_key_hi: 0, filename: [0u8; 256] },
        ];

        let rc = merge_and_route(
            &inputs, &cdir, &guards, &schema,
            0, 1, 99, &mut results,
        );
        assert_eq!(rc, 2); // both guards should have rows

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
        let guards = [(0u64, 0u64), (100u64, 0u64)];

        // table_id=0, level_num=1, lsn_tag=99 → second output is shard_0_99_L1_G1.db
        // Block it with a directory so finalize fails for guard 1.
        let blocker = dir.join("shard_0_99_L1_G1.db");
        fs::create_dir_all(&blocker).unwrap();

        let cdir = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
        let mut results = [GuardResult::zeroed(), GuardResult::zeroed()];
        let rc = merge_and_route(&inputs, &cdir, &guards, &schema, 0, 1, 99, &mut results);

        assert!(rc < 0, "expected failure, got {}", rc);
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
        let mut schema = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        schema.columns[0] = SchemaColumn { type_code: TYPE_U64, size: 8, nullable: 0, _pad: 0 };
        schema.columns[1] = SchemaColumn { type_code: TYPE_STRING, size: 16, nullable: 0, _pad: 0 };

        // Build shard with short strings
        let mut batch = OwnedBatch::with_schema(schema, 3);
        for pk in [1u64, 2, 3] {
            batch.pk_lo.extend_from_slice(&pk.to_le_bytes());
            batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            batch.weight.extend_from_slice(&1i64.to_le_bytes());
            batch.null_bmp.extend_from_slice(&0u64.to_le_bytes());

            // Write a short string: "hi" (2 bytes, inline)
            let mut str_struct = [0u8; 16];
            str_struct[0..4].copy_from_slice(&2u32.to_le_bytes()); // length=2
            str_struct[4] = b'h'; str_struct[5] = b'i'; // prefix
            batch.col_data[0].extend_from_slice(&str_struct);
            batch.count += 1;
        }
        let s1 = dir.join("s1.db");
        let cpath = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        assert_eq!(batch.write_as_shard(&cpath, 0), 0);

        // Compact it (single shard, should roundtrip)
        let output = dir.join("merged.db");
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();
        let inputs = [cpath.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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
        let mut schema = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        schema.columns[0] = SchemaColumn { type_code: TYPE_U64, size: 8, nullable: 0, _pad: 0 };
        schema.columns[1] = SchemaColumn { type_code: TYPE_I64, size: 8, nullable: 1, _pad: 0 };

        // Build shard: key 1 = non-null (42), key 2 = null
        let mut batch = OwnedBatch::with_schema(schema, 2);
        // Row 1: non-null
        batch.pk_lo.extend_from_slice(&1u64.to_le_bytes());
        batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        batch.weight.extend_from_slice(&1i64.to_le_bytes());
        batch.null_bmp.extend_from_slice(&0u64.to_le_bytes()); // no nulls
        batch.col_data[0].extend_from_slice(&42i64.to_le_bytes());
        batch.count += 1;

        // Row 2: null column
        batch.pk_lo.extend_from_slice(&2u64.to_le_bytes());
        batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
        batch.weight.extend_from_slice(&1i64.to_le_bytes());
        // null bit for col_idx=1, pk_index=0 → payload_idx = 0 → bit 0
        batch.null_bmp.extend_from_slice(&1u64.to_le_bytes());
        batch.col_data[0].extend_from_slice(&0i64.to_le_bytes());
        batch.count += 1;

        let s1 = dir.join("s1.db");
        let cpath = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        assert_eq!(batch.write_as_shard(&cpath, 0), 0);

        // Compact
        let output = dir.join("merged.db");
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();
        let inputs = [cpath.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 2);

        // Row 0: not null
        assert!(!is_null(&merged, 0, 1, 0));
        let val = read_i64_le(merged.get_col_ptr(0, 0, 8), 0);
        assert_eq!(val, 42);

        // Row 1: null
        assert!(is_null(&merged, 1, 1, 0));

        let _ = fs::remove_dir_all(&dir);
    }

    // -- 3-column helpers for reduce-output-pattern tests --------------------

    fn make_3col_schema() -> SchemaDescriptor {
        let mut s = SchemaDescriptor {
            num_columns: 3,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        s.columns[0] = SchemaColumn { type_code: TYPE_U64, size: 8, nullable: 0, _pad: 0 };
        s.columns[1] = SchemaColumn { type_code: TYPE_I64, size: 8, nullable: 0, _pad: 0 };
        s.columns[2] = SchemaColumn { type_code: TYPE_I64, size: 8, nullable: 0, _pad: 0 };
        s
    }

    /// Write a shard with 3-column rows: (pk, weight, col1_val, col2_val).
    fn write_3col_shard(
        path: &str,
        rows: &[(u64, i64, i64, i64)],
        schema: &SchemaDescriptor,
    ) {
        let mut batch = OwnedBatch::with_schema(*schema, rows.len());
        for &(pk, w, c1, c2) in rows {
            batch.pk_lo.extend_from_slice(&pk.to_le_bytes());
            batch.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            batch.weight.extend_from_slice(&w.to_le_bytes());
            batch.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            batch.col_data[0].extend_from_slice(&c1.to_le_bytes());
            batch.col_data[1].extend_from_slice(&c2.to_le_bytes());
            batch.count += 1;
        }
        let cpath = std::ffi::CString::new(path).unwrap();
        assert_eq!(batch.write_as_shard(&cpath, 0), 0);
    }

    /// Read all rows from a 3-col shard as (pk, weight, col1, col2).
    fn read_3col_shard(path: &str, schema: &SchemaDescriptor) -> Vec<(u64, i64, i64, i64)> {
        let cpath = std::ffi::CString::new(path).unwrap();
        let shard = MappedShard::open(&cpath, schema, false).unwrap();
        let mut rows = Vec::new();
        for i in 0..shard.count {
            let pk = shard.get_pk_lo(i);
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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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

        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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

        let guard_keys = vec![(0u64, 0u64)]; // single guard
        let mut results = vec![GuardResult::zeroed()];

        let n = merge_and_route(&inputs, &cdir, &guard_keys, &schema, 99, 1, 1, &mut results);
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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0, "compact with checksums enabled must succeed for valid data");

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
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

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

        let guard_keys = vec![(200u64, 0u64)]; // single guard at key 200
        let mut results = vec![GuardResult::zeroed()];

        let n = merge_and_route(&inputs, &cdir, &guard_keys, &schema, 42, 2, 1, &mut results);
        assert!(n > 0, "merge_and_route should produce output");

        let fn0 = crate::util::cstr_from_buf(&results[0].filename);
        let cpath = std::ffi::CString::new(fn0).unwrap();
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 4, "all 4 keys must be present in output");

        // Verify all keys readable
        for &pk in &[50u64, 100, 150, 250] {
            let idx = shard.find_row_index(pk, 0);
            assert!(idx >= 0, "key {} not found in output shard", pk);
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
