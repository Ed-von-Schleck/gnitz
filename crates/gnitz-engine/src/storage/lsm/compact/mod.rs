//! Self-contained shard compaction: N-way merge of sorted shard files.
//!
//! Carved along the merge/route seam: the N-way (PK, payload) merge kernel and
//! the routing-aware `merge_and_route` live in [`merge`]; the guard lookup,
//! [`GuardResult`], and the `compact_shards` orchestration live in [`route`].
//! This module re-exports the public entry points and hosts the shared tests,
//! which exercise both halves through the public surface.

mod merge;
mod route;

pub use merge::merge_and_route;
pub use route::{compact_shards, GuardResult};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use super::route::find_guard_for_key;
    use super::super::columnar;
    use super::super::batch::Batch;
    use super::super::shard_reader::MappedShard;
    use std::ffi::CStr;
    use std::fs;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use type_code::{I64 as TYPE_I64, U64 as TYPE_U64, STRING as TYPE_STRING};
    use crate::foundation::codec::{read_i64_le, read_u32_le};

    fn is_null(shard: &MappedShard, row: usize, col_idx: usize, schema: &SchemaDescriptor) -> bool {
        let null_word = shard.get_null_word(row);
        let pi = schema.try_payload_idx(col_idx).expect("is_null test helper: col_idx is a payload column");
        (null_word >> pi) & 1 != 0
    }

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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

        // Read back merged shard
        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 6);

        // Verify sorted order
        let mut prev = 0u128;
        for i in 0..merged.count {
            let pk = merged.get_pk(i);
            assert!(pk > prev, "not sorted at row {i}: {pk} <= {prev}");
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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_and_route_rejects_empty_guards() {
        // Empty guard_keys would index batches[0] out of bounds inside the
        // merge loop; the function must reject it up front.
        let schema = make_test_schema();
        let cdir = std::ffi::CString::new("/tmp").unwrap();
        let guards: [u128; 0] = [];
        let mut results: [GuardResult; 0] = [];
        let r = merge_and_route(&[], &cdir, &guards, &schema, 0, 1, 0, &mut results, false);
        assert!(r.is_err(), "empty guard_keys must return Err, not panic");
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

        // Two guards: [0, 100)  and [100, ∞). Guard keys live in the same
        // order-preserving pack_pk_be space as the router's sort key, so derive
        // them from the OPK bytes of the boundary values (not native u128s).
        let guards: [u128; 2] = [
            crate::storage::merge::pk_sort_key(&0u64.to_be_bytes()),
            crate::storage::merge::pk_sort_key(&100u64.to_be_bytes()),
        ];
        let mut results = [
            GuardResult::zeroed(),
            GuardResult::zeroed(),
        ];

        let n = merge_and_route(
            &inputs, &cdir, &guards, &schema,
            0, 1, 99, &mut results, false,
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
        let rc = merge_and_route(&inputs, &cdir, &guards, &schema, 0, 1, 99, &mut results, false);

        assert!(rc.is_err(), "expected failure, got {rc:?}");
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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

        let rows = read_3col_shard(output.to_str().unwrap(), &schema);
        assert_eq!(rows.len(), 1, "expected 1 surviving row, got {rows:?}");
        assert_eq!(rows[0], (1, 1, 0, 15000));

        let _ = fs::remove_dir_all(&dir);
    }

    /// PIN — root adjacency of equal-(PK, payload) rows across shards.
    /// Three single-row shards, all PK=1, two of which share the *exact*
    /// payload (0,100) with opposite weights; the third carries a different
    /// payload (0,200). Each shard is internally (PK, payload)-sorted (one
    /// row), but across shards the matching payload-100 rows are NOT adjacent
    /// in shard order — they bracket the payload-200 row.
    ///
    /// The k-way heap MUST order by (PK, payload) so the two payload-100 rows
    /// reach the fold root consecutively and cancel; the payload-200 row
    /// survives. A PK-only heap `less` (dropping the payload tiebreak) leaves
    /// the three same-PK rows unordered among themselves, so `drive_merge`'s
    /// fold breaks on the first payload mismatch and the +1/-1 payload-100
    /// pair never sums — leaking a spurious row.
    #[test]
    fn test_compact_same_pk_nonadjacent_payload_interleave() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tmp/compact_test_3col_nonadjacent");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_3col_schema();

        // s1: (pk=1, +1, payload (0,100))
        let s1 = dir.join("s1.db");
        write_3col_shard(s1.to_str().unwrap(), &[(1, 1, 0, 100)], &schema);

        // s2: (pk=1, +1, payload (0,200)) — different payload, between the
        // two payload-100 rows in shard order.
        let s2 = dir.join("s2.db");
        write_3col_shard(s2.to_str().unwrap(), &[(1, 1, 0, 200)], &schema);

        // s3: (pk=1, -1, payload (0,100)) — retracts the s1 row.
        let s3 = dir.join("s3.db");
        write_3col_shard(s3.to_str().unwrap(), &[(1, -1, 0, 100)], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cs3 = std::ffi::CString::new(s3.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str(), cs3.as_c_str()];
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

        let rows = read_3col_shard(output.to_str().unwrap(), &schema);
        assert_eq!(
            rows.len(),
            1,
            "payload-100 +1/-1 pair must cancel; only payload-200 survives, got {rows:?}"
        );
        assert_eq!(rows[0], (1, 1, 0, 200));

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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

        let rows = read_3col_shard(output.to_str().unwrap(), &schema);
        assert_eq!(rows.len(), 2, "expected 2 surviving rows, got {rows:?}");
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
            let p = dir.join(format!("t{tick}.db"));
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

        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

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

        let n = merge_and_route(&inputs, &cdir, &guard_keys, &schema, 99, 1, 1, &mut results, false).unwrap();
        assert!(n > 0, "merge_and_route should produce output");

        let fn0 = crate::foundation::codec::cstr_from_buf(&results[0].filename);
        let rows = read_3col_shard(fn0, &schema);
        assert_eq!(rows.len(), 2, "expected 2 rows, got {rows:?}");
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
        compact_shards(&inputs, &cout, &schema, 0, false).expect("compact with checksums enabled must succeed for valid data");

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
        compact_shards(&inputs, &cout, &schema, 0, false).unwrap();

        let merged = MappedShard::open(&cout, &schema, true).unwrap();
        assert_eq!(merged.count, 10000);

        // Verify sorted order
        let mut prev = 0u128;
        for i in 0..merged.count {
            let pk = merged.get_pk(i);
            assert!(pk > prev, "not sorted at row {i}: {pk} <= {prev}");
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

        let n = merge_and_route(&inputs, &cdir, &guard_keys, &schema, 42, 2, 1, &mut results, false).unwrap();
        assert!(n > 0, "merge_and_route should produce output");

        let fn0 = crate::foundation::codec::cstr_from_buf(&results[0].filename);
        let cpath = std::ffi::CString::new(fn0).unwrap();
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 4, "all 4 keys must be present in output");

        // Verify all keys readable
        for &pk in &[50u64, 100, 150, 250] {
            assert!(shard.find_row_index(pk as u128).is_some(), "key {pk} not found in output shard");
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

    fn assert_compare_pk_bytes_sorted(shard: &MappedShard) {
        for i in 1..shard.count {
            assert_ne!(
                columnar::compare_pk_bytes(shard.get_pk_bytes(i - 1), shard.get_pk_bytes(i)),
                std::cmp::Ordering::Greater,
                "merged shard not compare_pk_bytes-sorted at row {i}",
            );
        }
    }

    /// OPK bytes for a `(U64, U64)` compound PK: each column big-endian.
    fn pk2(a: u64, b: u64) -> Vec<u8> {
        let mut v = a.to_be_bytes().to_vec();
        v.extend_from_slice(&b.to_be_bytes());
        v
    }

    /// Regression: a narrow compound `(U64, U64)` PK whose raw-LE u128
    /// order diverges from `compare_pk_bytes`. The input shards are physically
    /// written in compound (OPK) order; an N-way merge that compared raw-LE u128
    /// instead would produce mis-ordered output plus missed cross-shard
    /// consolidation.
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
        compact_shards(&[cs1.as_c_str(), cs2.as_c_str()], &cout, &schema, 0, false).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        // (2,3) cancels (+1 -1 = 0). The cross-shard duplicate must fold, which
        // requires the two (2,3) entries to be adjacent at the heap root.
        assert_eq!(merged.count, 3, "expected 3 surviving rows (the (2,3) pair cancels)");
        assert_compare_pk_bytes_sorted(&merged);

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
        // OPK bytes for a single I64 PK column (big-endian, sign bit flipped).
        let k = |v: i64| {
            let mut out = [0u8; 8];
            gnitz_wire::encode_pk_column(&v.to_le_bytes(), type_code::I64, &mut out);
            out.to_vec()
        };

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
        compact_shards(&[cs1.as_c_str(), cs2.as_c_str()], &cout, &schema, 0, false).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 4);
        assert_compare_pk_bytes_sorted(&merged);
        // PK region is OPK; decode each column back to its native I64 value.
        let present: Vec<i64> = (0..merged.count)
            .map(|i| {
                let mut le = [0u8; 8];
                gnitz_wire::decode_pk_column(merged.get_pk_bytes(i), type_code::I64, &mut le);
                i64::from_le_bytes(le)
            })
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
        // OPK bytes for a 3×U64 compound PK: each column big-endian.
        let pk3 = |a: u64, b: u64, c: u64| {
            let mut v = a.to_be_bytes().to_vec();
            v.extend_from_slice(&b.to_be_bytes());
            v.extend_from_slice(&c.to_be_bytes());
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
        compact_shards(&[cs1.as_c_str(), cs2.as_c_str()], &cout, &schema, 0, false).unwrap();

        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_eq!(merged.count, 2, "prefix-colliding distinct wide PKs must not fold");
        assert_compare_pk_bytes_sorted(&merged);
        let present: Vec<Vec<u8>> = (0..merged.count).map(|i| merged.get_pk_bytes(i).to_vec()).collect();
        assert_eq!(present, vec![pk3(1, 1, 100), pk3(1, 1, 200)]);

        let _ = fs::remove_dir_all(&dir);
    }
}
