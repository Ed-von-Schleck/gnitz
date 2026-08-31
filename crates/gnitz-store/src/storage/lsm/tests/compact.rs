use super::super::batch::{Batch, REG_PAYLOAD_START};
use super::super::layout::{ENCODING_FOR, ENCODING_RAW};
use super::super::merge::{run_merge, BlobCacheGuard};
use super::super::naming;
use super::super::shard_file::{self, region_dir, ShardWriteOpts};
use super::super::shard_index::guard_slot;
use super::super::shard_reader::MappedShard;
use super::*;
use crate::schema::key::PkBuf;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{make_schema_u64_i64, opk_pk, pk_payload_schema};
use gnitz_wire::read_i64_le;
use std::ffi::CStr;
use std::fs;
use type_code::{I64 as TYPE_I64, STRING as TYPE_STRING, U64 as TYPE_U64};

/// A probed store's `Output` — the shape every test here compacts into.
fn out(dir: &str, table_id: u32, level_num: usize, compact_seq: u64) -> Output<'_> {
    Output {
        dir,
        table_id,
        level_num,
        compact_seq,
        skip_pk_filter: false,
    }
}

/// A one-payload-column shard of `pks` at `weights` (payload = pk), written to
/// `dir/name`. Returns the path, so a caller can hand it straight to
/// `compact_one` without restating it as a `CString`.
fn write_shard(
    dir: &std::path::Path,
    name: &str,
    pks: &[u64],
    weights: &[i64],
    schema: &SchemaDescriptor,
) -> std::ffi::CString {
    let rows: Vec<(Vec<u8>, i64, i64)> = pks
        .iter()
        .zip(weights)
        .map(|(&p, &w)| (opk_pk(schema, &[p as u128]), w, p as i64))
        .collect();
    shard_file::write_test_shard(&dir.join(name), schema, &rows, ShardWriteOpts::default())
}

/// Single-guard compaction of `inputs` into `dir`. Returns the output
/// shard's path, or `None` when every row cancelled (no shard is written).
fn compact_one_opt(
    dir: &std::path::Path,
    inputs: &[&CStr],
    schema: &SchemaDescriptor,
    seq: u64,
) -> Option<std::ffi::CString> {
    let anchor = PkBuf::zeroed(schema.pk_stride() as usize);
    let outs = merge_and_route(
        inputs,
        &[(anchor, false)],
        schema,
        out(dir.to_str().unwrap(), 0, 1, seq),
    )
    .unwrap();
    outs.first().map(|(_, p)| std::ffi::CString::new(p.as_str()).unwrap())
}

fn compact_one(dir: &std::path::Path, inputs: &[&CStr], schema: &SchemaDescriptor, seq: u64) -> std::ffi::CString {
    compact_one_opt(dir, inputs, schema, seq).expect("compaction produced no output shard")
}

/// Encoding byte of a shard's payload directory entry.
fn payload_encoding(path: &str) -> u8 {
    region_dir(&fs::read(path).unwrap(), REG_PAYLOAD_START).1
}

/// Compaction packs eligible integer payload columns while the raw L0-style
/// inputs stay plain; the packed output is content-identical (weight +
/// payload) to the inputs, and re-compacting a packed input repacks it.
#[test]
fn compaction_packs_eligible_int_payload() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let schema = make_schema_u64_i64();

    // Two raw L0-style inputs (write_test_shard → pack_ints = false). Payload
    // == PK, a narrow range that FoR packs once merged.
    let pks1: Vec<u64> = (0..300).map(|i| i * 2).collect(); // evens
    let pks2: Vec<u64> = (0..300).map(|i| i * 2 + 1).collect(); // odds
    let cs1 = write_shard(&dir, "s1.db", &pks1, &vec![1i64; 300], &schema);
    let cs2 = write_shard(&dir, "s2.db", &pks2, &vec![1i64; 300], &schema);
    // L0 spill inputs stay Raw (policy: only compaction packs).
    assert_eq!(
        payload_encoding(cs1.to_str().unwrap()),
        ENCODING_RAW,
        "L0 input stays raw"
    );
    assert_eq!(payload_encoding(cs2.to_str().unwrap()), ENCODING_RAW);

    let cout = compact_one(&dir, &[cs1.as_c_str(), cs2.as_c_str()], &schema, 1);

    // Compaction output packs the eligible payload.
    assert_eq!(
        payload_encoding(cout.to_str().unwrap()),
        ENCODING_FOR,
        "compaction packs payload"
    );

    // Content: every merged row's decoded payload == its PK, weight 1.
    let merged = MappedShard::open(&cout, &schema, false).unwrap();
    assert_eq!(merged.count, 600);
    for r in 0..merged.count {
        let pk = merged.get_pk(r);
        let pay = read_i64_le(merged.get_col_ptr(r, 0, 8), 0);
        assert_eq!(pay as u128, pk, "row {r} payload == pk");
        assert_eq!(merged.get_weight(r), 1);
    }

    // Re-compaction of a packed input (decode → merge → re-encode).
    let cout2 = compact_one(&dir, &[cout.as_c_str()], &schema, 2);
    assert_eq!(
        payload_encoding(cout2.to_str().unwrap()),
        ENCODING_FOR,
        "re-compaction repacks"
    );
    let merged2 = MappedShard::open(&cout2, &schema, false).unwrap();
    assert_eq!(merged2.count, merged.count);
    for r in 0..merged2.count {
        assert_eq!(merged2.get_pk(r), merged.get_pk(r));
        assert_eq!(
            read_i64_le(merged2.get_col_ptr(r, 0, 8), 0),
            read_i64_le(merged.get_col_ptr(r, 0, 8), 0),
            "re-compaction preserves payload at row {r}",
        );
        assert_eq!(merged2.get_weight(r), merged.get_weight(r));
    }
}

/// Compaction over a whole input set: the survivors are the net-nonzero rows in
/// ascending PK order, and an input set that cancels completely writes no shard
/// at all rather than an empty one the caller would have to register. Every
/// case reads its output back with checksum validation on.
#[test]
fn compaction_merges_inputs_and_drops_cancelled_rows() {
    /// One input shard: its pks and their weights.
    type Shard = (Vec<u64>, Vec<i64>);
    /// A name, the input shards, and the surviving pks — `None` when the
    /// compaction writes no shard at all.
    type Case = (&'static str, Vec<Shard>, Option<Vec<u64>>);
    let cases: Vec<Case> = vec![
        ("no inputs", vec![], None),
        (
            "single shard",
            vec![(vec![10, 20, 30], vec![1; 3])],
            Some(vec![10, 20, 30]),
        ),
        (
            "two interleaved shards",
            vec![(vec![1, 3, 5], vec![1; 3]), (vec![2, 4, 6], vec![1; 3])],
            Some(vec![1, 2, 3, 4, 5, 6]),
        ),
        (
            "one key cancels",
            vec![(vec![1, 2, 3], vec![1; 3]), (vec![2], vec![-1])],
            Some(vec![1, 3]),
        ),
        (
            "every key cancels",
            vec![(vec![1, 2, 3], vec![1; 3]), (vec![1, 2, 3], vec![-1; 3])],
            None,
        ),
        (
            "a thousand rows over two shards",
            vec![
                ((0..500).map(|i| i * 2 + 1).collect(), vec![1; 500]),
                ((1..=500).map(|i| i * 2).collect(), vec![1; 500]),
            ],
            Some((1..=1000).collect()),
        ),
    ];

    let schema = make_schema_u64_i64();
    for (name, shards, want) in cases {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let paths: Vec<std::ffi::CString> = shards
            .iter()
            .enumerate()
            .map(|(i, (pks, ws))| write_shard(&dir, &format!("s{i}.db"), pks, ws, &schema))
            .collect();
        let inputs: Vec<&CStr> = paths.iter().map(|p| p.as_c_str()).collect();

        match (compact_one_opt(&dir, &inputs, &schema, 1), want) {
            (None, None) => {}
            (Some(cout), Some(want)) => {
                let merged = MappedShard::open(&cout, &schema, true).unwrap();
                let got: Vec<u64> = (0..merged.count).map(|i| merged.get_pk(i) as u64).collect();
                assert_eq!(got, want, "{name}");
            }
            (got, want) => panic!("{name}: wrote a shard = {}, wanted {}", got.is_some(), want.is_some()),
        }
    }
}

/// Guard routing is a saturating slot lookup: slot 0 owns every key below the
/// first guard key, and the last guard owns everything above it.
#[test]
fn guard_slot_saturates_at_both_ends() {
    let cases: &[(&[u64], u64, usize)] = &[
        (&[], 42, 0),
        (&[0], 0, 0),
        (&[0], 999, 0),
        (&[0, 100, 200], 50, 0),
        (&[0, 100, 200], 100, 1),
        (&[0, 100, 200], 150, 1),
        (&[0, 100, 200], 200, 2),
        (&[0, 100, 200], 999, 2),
        // Slot 0 owns keys below the first guard key, which is not always 0.
        (&[200, 400], 50, 0),
    ];
    for &(guards, key, want) in cases {
        let keys: Vec<[u8; 8]> = guards.iter().map(|g| g.to_be_bytes()).collect();
        assert_eq!(
            guard_slot(&keys, &key.to_be_bytes(), |g| &g[..]),
            want,
            "guards={guards:?} key={key}"
        );
    }
}

#[test]
#[should_panic(expected = "at least one guard")]
fn test_merge_and_route_rejects_empty_guards() {
    // Empty guard_keys would index batches[0] out of bounds inside the
    // merge loop; a caller passing one has violated the contract — fail
    // loudly up front.
    let schema = make_schema_u64_i64();
    let guards: [(PkBuf, bool); 0] = [];
    let _ = merge_and_route(&[], &guards, &schema, out("/tmp", 0, 1, 0));
}

#[test]
fn test_merge_and_route_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let schema = make_schema_u64_i64();

    // Shard with keys 10, 50, 150, 250
    let cs1 = write_shard(&dir, "s1.db", &[10, 50, 150, 250], &[1, 1, 1, 1], &schema);
    let inputs = [cs1.as_c_str()];

    // Two guards: [0, 100)  and [100, ∞). A guard key is a whole OPK key, the
    // same bytes the router compares, so derive them from the OPK image of the
    // boundary values (not their native little-endian form).
    let guards = [
        (PkBuf::from_bytes(&0u64.to_be_bytes()), false),
        (PkBuf::from_bytes(&100u64.to_be_bytes()), false),
    ];
    let guard_outputs = merge_and_route(&inputs, &guards, &schema, out(dir.to_str().unwrap(), 0, 1, 99)).unwrap();
    assert_eq!(guard_outputs.len(), 2); // both guards should have rows

    // Guard 0 should have keys 10, 50
    let cfn0 = std::ffi::CString::new(guard_outputs[0].1.as_str()).unwrap();
    let g0 = MappedShard::open(&cfn0, &schema, false).unwrap();
    assert_eq!(g0.count, 2);
    assert_eq!(g0.get_pk(0), 10);
    assert_eq!(g0.get_pk(1), 50);

    // Guard 1 should have keys 150, 250
    let cfn1 = std::ffi::CString::new(guard_outputs[1].1.as_str()).unwrap();
    let g1 = MappedShard::open(&cfn1, &schema, false).unwrap();
    assert_eq!(g1.count, 2);
    assert_eq!(g1.get_pk(0), 150);
    assert_eq!(g1.get_pk(1), 250);
}

#[test]
fn test_merge_and_route_cleanup_on_partial_finalize_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let schema = make_schema_u64_i64();

    let cs1 = write_shard(&dir, "in1.db", &[10, 50], &[1, 1], &schema);
    let cs2 = write_shard(&dir, "in2.db", &[150, 250], &[1, 1], &schema);
    let inputs = [cs1.as_c_str(), cs2.as_c_str()];
    let guards: [(PkBuf, bool); 2] = [
        (PkBuf::from_bytes(&0u64.to_be_bytes()), false),
        (PkBuf::from_bytes(&100u64.to_be_bytes()), false),
    ];

    // table_id=0, level_num=1, compact_seq=99 → the second output is
    // shard_0_99_L1_P1.db, named by its part index within the compaction.
    // Block it with a directory so finalize fails for that guard.
    let blocker = dir.join(naming::compact_shard_name(0, 99, 1, 1));
    fs::create_dir_all(&blocker).unwrap();

    let rc = merge_and_route(&inputs, &guards, &schema, out(dir.to_str().unwrap(), 0, 1, 99));

    assert!(rc.is_err(), "expected failure, got {rc:?}");
    let guard0_file = dir.join(naming::compact_shard_name(0, 99, 1, 0));
    assert!(!guard0_file.exists(), "guard 0 output should have been cleaned up");
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
fn write_3col_shard(path: &str, rows: &[(u64, i64, i64, i64)], schema: &SchemaDescriptor) {
    let mut batch = Batch::with_capacity(*schema, rows.len());
    for &(pk, w, c1, c2) in rows {
        batch.extend_pk(pk as u128);
        batch.extend_weight(&w.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &c1.to_le_bytes());
        batch.extend_col(1, &c2.to_le_bytes());
        batch.count += 1;
    }
    let cpath = std::ffi::CString::new(path).unwrap();
    batch.write_as_shard(&cpath, schema, ShardWriteOpts::default()).unwrap();
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
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let schema = make_3col_schema();

    // Shard 1 (tick 1): insert (pk=1, group=0, sum=5000)
    let s1 = dir.join("s1.db");
    write_3col_shard(s1.to_str().unwrap(), &[(1, 1, 0, 5000)], &schema);

    // Shard 2 (tick 2): retract sum=5000, insert sum=10000
    let s2 = dir.join("s2.db");
    write_3col_shard(s2.to_str().unwrap(), &[(1, -1, 0, 5000), (1, 1, 0, 10000)], &schema);

    // Shard 3 (tick 3): retract sum=10000, insert sum=15000
    let s3 = dir.join("s3.db");
    write_3col_shard(s3.to_str().unwrap(), &[(1, -1, 0, 10000), (1, 1, 0, 15000)], &schema);

    let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
    let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
    let cs3 = std::ffi::CString::new(s3.to_str().unwrap()).unwrap();
    let inputs = [cs1.as_c_str(), cs2.as_c_str(), cs3.as_c_str()];
    let cout = compact_one(&dir, &inputs, &schema, 1);

    let rows = read_3col_shard(cout.to_str().unwrap(), &schema);
    assert_eq!(rows.len(), 1, "expected 1 surviving row, got {rows:?}");
    assert_eq!(rows[0], (1, 1, 0, 15000));
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
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

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

    let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
    let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
    let cs3 = std::ffi::CString::new(s3.to_str().unwrap()).unwrap();
    let inputs = [cs1.as_c_str(), cs2.as_c_str(), cs3.as_c_str()];
    let cout = compact_one(&dir, &inputs, &schema, 1);

    let rows = read_3col_shard(cout.to_str().unwrap(), &schema);
    assert_eq!(
        rows.len(),
        1,
        "payload-100 +1/-1 pair must cancel; only payload-200 survives, got {rows:?}"
    );
    assert_eq!(rows[0], (1, 1, 0, 200));
}

/// Multiple groups with interleaved shards: ensures the pending-group
/// algorithm handles group boundaries correctly across PKs.
#[test]
fn test_compact_multi_group_reduce_pattern() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let schema = make_3col_schema();

    // Group A (pk=1) and Group B (pk=2), 3 ticks each
    let s1 = dir.join("s1.db");
    write_3col_shard(s1.to_str().unwrap(), &[(1, 1, 0, 100), (2, 1, 1, 200)], &schema);

    let s2 = dir.join("s2.db");
    write_3col_shard(
        s2.to_str().unwrap(),
        &[(1, -1, 0, 100), (1, 1, 0, 300), (2, -1, 1, 200), (2, 1, 1, 400)],
        &schema,
    );

    let s3 = dir.join("s3.db");
    write_3col_shard(
        s3.to_str().unwrap(),
        &[(1, -1, 0, 300), (1, 1, 0, 600), (2, -1, 1, 400), (2, 1, 1, 800)],
        &schema,
    );

    let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
    let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
    let cs3 = std::ffi::CString::new(s3.to_str().unwrap()).unwrap();
    let inputs = [cs1.as_c_str(), cs2.as_c_str(), cs3.as_c_str()];
    let cout = compact_one(&dir, &inputs, &schema, 1);

    let rows = read_3col_shard(cout.to_str().unwrap(), &schema);
    assert_eq!(rows.len(), 2, "expected 2 surviving rows, got {rows:?}");
    assert_eq!(rows[0], (1, 1, 0, 600));
    assert_eq!(rows[1], (2, 1, 1, 800));
}

/// 10 shards simulating 10 reduce ticks for 1 group — the exact scenario
/// from the test_heavy_agg_500k failure.
#[test]
fn test_compact_10_tick_reduce_single_group() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

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
        write_3col_shard(
            p.to_str().unwrap(),
            &[(1, -1, 0, old_sum as i64), (1, 1, 0, new_sum as i64)],
            &schema,
        );
        shard_paths.push(p);
    }

    let cstrs: Vec<_> = shard_paths
        .iter()
        .map(|p| std::ffi::CString::new(p.to_str().unwrap()).unwrap())
        .collect();
    let inputs: Vec<_> = cstrs.iter().map(|c| c.as_c_str()).collect();
    let cout = compact_one(&dir, &inputs, &schema, 1);

    let rows = read_3col_shard(cout.to_str().unwrap(), &schema);
    assert_eq!(
        rows.len(),
        1,
        "expected 1 row after 10-tick consolidation, got {}",
        rows.len()
    );
    assert_eq!(rows[0], (1, 1, 0, 50000), "expected final sum=50000");
}

/// Keys below the only guard key must still route to it and stay readable.
#[test]
fn test_merge_and_route_keys_below_first_guard() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let schema = make_schema_u64_i64();

    // Shard with keys 50, 100, 150, 250 — two are below guard key 200
    let cs1 = write_shard(&dir, "s1.db", &[50, 100, 150, 250], &[1, 1, 1, 1], &schema);
    let inputs = [cs1.as_c_str()];

    // A single guard keyed at 200: everything below it is guard 0's tail.
    let guards = [(PkBuf::from_bytes(&200u64.to_be_bytes()), false)];
    let guard_outputs = merge_and_route(&inputs, &guards, &schema, out(dir.to_str().unwrap(), 42, 2, 1)).unwrap();
    assert!(!guard_outputs.is_empty(), "merge_and_route should produce output");

    let cpath = std::ffi::CString::new(guard_outputs[0].1.as_str()).unwrap();
    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
    assert_eq!(shard.count, 4, "all 4 keys must be present in output");

    // Verify all keys readable
    for &pk in &[50u64, 100, 150, 250] {
        let at = shard.find_lower_bound_bytes(&pk.to_be_bytes());
        assert!(
            at < shard.count && shard.get_pk(at) == pk as u128,
            "key {pk} not found in output shard"
        );
    }
}

// -- Wide / compound / signed PK compaction (OPK ordering) ---------------

fn assert_compare_pk_bytes_sorted(shard: &MappedShard) {
    for i in 1..shard.count {
        assert_ne!(
            crate::schema::key::compare_pk_bytes(shard.get_pk_bytes(i - 1), shard.get_pk_bytes(i)),
            std::cmp::Ordering::Greater,
            "merged shard not compare_pk_bytes-sorted at row {i}",
        );
    }
}

/// On the PK axis compaction is type-blind: it orders and folds on OPK bytes at
/// whatever stride the schema gives it. Each case is a PK shape whose OPK order
/// diverges from a raw little-endian `u128` compare of the same region — a
/// compound key the second column reorders, a signed key whose negatives sort
/// first, a wide key that collides on its leading 16 bytes, and narrow and
/// mixed-width keys whose strides are not 8 or 16. A merge comparing raw LE
/// would mis-order the output and miss the cross-shard fold.
#[test]
fn compaction_orders_and_folds_on_opk_bytes_at_every_pk_shape() {
    // Native PK column values per row, with the row's weight and payload.
    type Row = (Vec<u128>, i64, i64);
    struct Case {
        name: &'static str,
        pk_types: &'static [u8],
        stride: u8,
        shards: Vec<Vec<Row>>,
        /// Surviving rows' PK column values, in the order they must come back.
        want: Vec<Vec<u128>>,
    }

    let neg = |v: i64| v as u128;
    let cases = vec![
        Case {
            // Raw-LE order of these concatenations is (3,0) < (2,3) < (1,5) < (1,9);
            // compound order is (1,5) < (1,9) < (2,3) < (3,0). The (2,3) pair
            // cancels, which needs both entries adjacent at the heap root.
            name: "compound (U64, U64)",
            pk_types: &[TYPE_U64, TYPE_U64],
            stride: 16,
            shards: vec![
                vec![(vec![1, 5], 1, 10), (vec![2, 3], 1, 30)],
                vec![(vec![1, 9], 1, 20), (vec![2, 3], -1, 30), (vec![3, 0], 1, 40)],
            ],
            want: vec![vec![1, 5], vec![1, 9], vec![3, 0]],
        },
        Case {
            // Negatives sort last under raw LE (zero-extended), first under OPK.
            name: "signed I64",
            pk_types: &[TYPE_I64],
            stride: 8,
            shards: vec![
                vec![(vec![neg(-5)], 1, 1), (vec![3], 1, 3)],
                vec![(vec![neg(-2)], 1, 2), (vec![10], 1, 4)],
            ],
            want: vec![vec![neg(-5)], vec![neg(-2)], vec![3], vec![10]],
        },
        Case {
            // (1,1,100) and (1,1,200) share their leading 16 bytes and carry
            // identical payloads: the wide comparator's tiebreak must keep both.
            name: "wide 3xU64, leading-16-byte collision",
            pk_types: &[TYPE_U64, TYPE_U64, TYPE_U64],
            stride: 24,
            shards: vec![vec![(vec![1, 1, 100], 1, 7)], vec![(vec![1, 1, 200], 1, 7)]],
            want: vec![vec![1, 1, 100], vec![1, 1, 200]],
        },
        Case {
            name: "narrow U8",
            pk_types: &[type_code::U8],
            stride: 1,
            shards: vec![
                vec![(vec![200], 1, 1), (vec![255], 1, 2)],
                vec![(vec![1], 1, 3), (vec![200], -1, 1)],
            ],
            want: vec![vec![1], vec![255]],
        },
        Case {
            // A stride that is neither 8 nor 16, so no fast width arm applies.
            name: "mixed-width (U64, U16, U8)",
            pk_types: &[TYPE_U64, type_code::U16, type_code::U8],
            stride: 11,
            shards: vec![
                vec![(vec![1, 2, 3], 1, 10), (vec![1, 2, 9], 1, 20)],
                vec![(vec![1, 2, 9], -1, 20), (vec![1, 3, 0], 1, 30)],
            ],
            want: vec![vec![1, 2, 3], vec![1, 3, 0]],
        },
    ];

    for case in cases {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let schema = pk_payload_schema(case.pk_types);
        assert_eq!(schema.pk_stride(), case.stride, "{}: fixture stride", case.name);

        let paths: Vec<std::ffi::CString> = case
            .shards
            .iter()
            .enumerate()
            .map(|(i, rows)| {
                let rows: Vec<(Vec<u8>, i64, i64)> =
                    rows.iter().map(|(pk, w, v)| (opk_pk(&schema, pk), *w, *v)).collect();
                shard_file::write_test_shard(&dir.join(format!("s{i}.db")), &schema, &rows, ShardWriteOpts::default())
            })
            .collect();
        let inputs: Vec<&CStr> = paths.iter().map(|p| p.as_c_str()).collect();

        let cout = compact_one(&dir, &inputs, &schema, 1);
        let merged = MappedShard::open(&cout, &schema, false).unwrap();
        assert_compare_pk_bytes_sorted(&merged);

        let got: Vec<Vec<u8>> = (0..merged.count).map(|i| merged.get_pk_bytes(i).to_vec()).collect();
        let want: Vec<Vec<u8>> = case.want.iter().map(|pk| opk_pk(&schema, pk)).collect();
        assert_eq!(got, want, "{}", case.name);
    }
}

// -- Columnar vs row-at-a-time materialization (differential) ------------
//
// Compaction materializes its merge survivors through the shared column-first
// scatter. These tests pin it against a row-at-a-time oracle over the shard
// arm — the one element the batch-only `columnar_materialize_differential`
// (in `repr::merge`) does not cover.

/// Row spec for the string-schema differential shards: pk, weight, two
/// German-string cells (`None` = NULL), one nullable I64 cell (`None` = NULL).
type DiffRow = (u64, i64, Option<&'static str>, Option<&'static str>, Option<i64>);

/// Decoded payload cell, compared by *content* so a differing blob byte
/// layout (column-major vs row-major append order) is tolerated.
#[derive(Debug, PartialEq)]
enum DiffCell {
    Null,
    Str(Vec<u8>),
    Int(i64),
}

/// One decoded compacted row: `(pk_bytes, weight, null_word, payload cells)`.
type DecodedRow = (Vec<u8>, i64, u64, Vec<DiffCell>);

/// U64 PK + STRING + STRING + nullable I64 — two spilling string columns, so
/// the column-major blob-offset relocation is exercised.
fn diff_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TYPE_U64, 0),
            SchemaColumn::new(TYPE_STRING, 0),
            SchemaColumn::new(TYPE_STRING, 0),
            SchemaColumn::new(TYPE_I64, 1),
        ],
        &[0],
    )
}

/// Build a `(PK, payload)`-sorted shard file from `rows`. Long strings (> 12
/// bytes) spill to the blob heap; `None` cells set the null bit.
fn write_diff_shard(path: &str, schema: &SchemaDescriptor, rows: &[DiffRow]) {
    let mut batch = Batch::with_capacity(*schema, rows.len().max(1));
    for &(pk, w, c0, c1, c2) in rows {
        let mut nw = 0u64;
        let st0 = match c0 {
            Some(s) => gnitz_wire::encode_german_string(s.as_bytes(), &mut batch.blob),
            None => {
                nw |= 1 << 0;
                [0u8; 16]
            }
        };
        let st1 = match c1 {
            Some(s) => gnitz_wire::encode_german_string(s.as_bytes(), &mut batch.blob),
            None => {
                nw |= 1 << 1;
                [0u8; 16]
            }
        };
        let ic = match c2 {
            Some(v) => v.to_le_bytes(),
            None => {
                nw |= 1 << 2;
                [0u8; 8]
            }
        };
        batch.extend_pk(pk as u128);
        batch.extend_weight(&w.to_le_bytes());
        batch.extend_null_bmp(&nw.to_le_bytes());
        batch.extend_col(0, &st0);
        batch.extend_col(1, &st1);
        batch.extend_col(2, &ic);
        batch.count += 1;
    }
    let cpath = std::ffi::CString::new(path).unwrap();
    batch.write_as_shard(&cpath, schema, ShardWriteOpts::default()).unwrap();
}

/// Read a compacted shard back to its rows, decoding strings to their
/// content against the shard's own blob (never the raw struct bytes).
fn decode_diff_shard(path: &str, schema: &SchemaDescriptor) -> Vec<DecodedRow> {
    let cpath = std::ffi::CString::new(path).unwrap();
    let shard = MappedShard::open(&cpath, schema, false).unwrap();
    let blob = shard.blob_slice();
    (0..shard.count)
        .map(|i| {
            let pk = shard.get_pk_bytes(i).to_vec();
            let w = shard.get_weight(i);
            let nw = shard.get_null_word(i);
            let cells = schema
                .payload_columns()
                .map(|(pi, col)| {
                    let cs = col.size() as usize;
                    if gnitz_wire::null_word_get(nw, pi) {
                        DiffCell::Null
                    } else if gnitz_wire::is_german_string(col.type_code) {
                        let st: [u8; 16] = shard.get_col_ptr(i, pi, 16).try_into().unwrap();
                        DiffCell::Str(gnitz_wire::try_decode_german_string(&st, blob).expect("valid string"))
                    } else {
                        DiffCell::Int(i64::from_le_bytes(shard.get_col_ptr(i, pi, cs).try_into().unwrap()))
                    }
                })
                .collect();
            (pk, w, nw, cells)
        })
        .collect()
}

/// Row-at-a-time oracle: merge survivors materialized one
/// `(row, column)` at a time via `append_row_from_source_bytes`. The oracle
/// the columnar path is checked against.
fn oracle_compact_row_at_a_time(input_files: &[&CStr], output_file: &CStr, schema: &SchemaDescriptor) {
    let shards = open_shards(input_files, schema).unwrap();
    let mut batch = Batch::with_capacity(*schema, 1024);
    let mut blob_cache = BlobCacheGuard::acquire(schema, 1024);
    run_merge(&shards, schema, |src, row, w| {
        let pk_bytes = shards[src].get_pk_bytes(row);
        batch.append_row_from_source_bytes(pk_bytes, w, &shards[src], row, blob_cache.get_mut());
    });
    batch
        .write_as_shard(output_file, schema, ShardWriteOpts::COMPACTION)
        .unwrap();
}

/// Compact `shard_rows` both ways (production and the oracle) and assert
/// value-identity of the readback. Returns the production rows for the
/// caller's concrete pins.
fn assert_compact_paths_agree(
    dir: &std::path::Path,
    schema: &SchemaDescriptor,
    shard_rows: &[Vec<DiffRow>],
) -> Vec<DecodedRow> {
    let in_paths: Vec<std::path::PathBuf> = (0..shard_rows.len()).map(|i| dir.join(format!("in{i}.db"))).collect();
    for (rows, p) in shard_rows.iter().zip(&in_paths) {
        write_diff_shard(p.to_str().unwrap(), schema, rows);
    }
    let in_cstrs: Vec<std::ffi::CString> = in_paths
        .iter()
        .map(|p| std::ffi::CString::new(p.to_str().unwrap()).unwrap())
        .collect();
    let inputs: Vec<&CStr> = in_cstrs.iter().map(|c| c.as_c_str()).collect();

    let out_old = dir.join("out_old.db");
    let cold = std::ffi::CString::new(out_old.to_str().unwrap()).unwrap();

    let cnew = compact_one(dir, &inputs, schema, 1);
    oracle_compact_row_at_a_time(&inputs, &cold, schema);

    let rows_new = decode_diff_shard(cnew.to_str().unwrap(), schema);
    let rows_old = decode_diff_shard(out_old.to_str().unwrap(), schema);

    assert_eq!(rows_new, rows_old, "columnar vs row-at-a-time materialization diverged");
    rows_new
}

const LONG_A: &str = "long_string_value_A_padpadpadpad"; // > 12 → spills
const LONG_DUP: &str = "duplicated_long_string_payload_zz"; // > 12, reused across cells

/// Differential over cross-shard duplicate PKs (a fold to weight 4), a
/// cancellation (pk=40 dropped), nulls in distinct columns, two spilling
/// STRING columns, and a same-PK/different-payload pair. Columnar output
/// must equal the row-at-a-time oracle.
#[test]
fn test_compact_columnar_matches_row_at_a_time() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let schema = diff_schema();

    let shard_rows = vec![
        vec![
            (10, 1, Some("aaa"), Some(LONG_A), Some(100)),
            (20, 1, Some("short"), Some(LONG_DUP), None), // c2 NULL
            (30, 1, None, Some("mid"), Some(-5)),         // c0 NULL
            (40, 1, Some("ghost0"), Some("ghost1"), Some(7)),
        ],
        vec![
            (10, 3, Some("aaa"), Some(LONG_A), Some(100)), // folds with s1 pk=10 → w=4
            (25, 1, Some(LONG_DUP), Some("x"), Some(9)),
            (40, -1, Some("ghost0"), Some("ghost1"), Some(7)), // cancels s1 pk=40 → dropped
            (50, 1, Some("last"), None, Some(0)),              // c1 NULL
        ],
        vec![
            (10, 1, Some("aaa"), Some("bbb"), Some(200)), // pk=10 distinct payload → second survivor
            (60, 1, Some("tail0"), Some("tail1"), None),  // c2 NULL
        ],
    ];

    let rows = assert_compact_paths_agree(&dir, &schema, &shard_rows);

    // Concrete pins beyond agreement.
    assert_eq!(rows.len(), 7, "expected 7 survivors (pk=40 cancels), got {rows:?}");
    let pk10 = 10u64.to_be_bytes().to_vec();
    let folded = rows
        .iter()
        .find(|r| r.0 == pk10 && r.3[2] == DiffCell::Int(100))
        .expect("folded pk=10 row present");
    assert_eq!(folded.1, 4, "pk=10 folds across shards to weight 4");
    assert_eq!(
        folded.3[1],
        DiffCell::Str(LONG_A.as_bytes().to_vec()),
        "spilled long string"
    );
    assert!(
        !rows.iter().any(|r| r.0 == 40u64.to_be_bytes().to_vec()),
        "pk=40 ghost must be dropped",
    );
}

// -- Multi-guard routed differential -------------------------------------
//
// The single-guard differential above already pins per-row materialization
// value-identity over the column-first scatter; this test pins only the
// *routed* split it cannot reach — survivors spanning multiple guard ranges
// land in the right per-guard shard (rows, weights, null words), and an empty
// guard and a fully-cancelled guard each write no shard — by checking every
// routed guard shard against a row-at-a-time oracle that routes the same
// `run_merge` survivor stream per guard.

/// Row-at-a-time multi-guard compaction, the routed oracle (one growable
/// `Batch` + `BlobCacheGuard` per guard). Returns `Some(path)` per non-empty
/// guard, `None` for an empty one.
fn oracle_merge_and_route_row_at_a_time(
    input_files: &[&CStr],
    out_dir: &std::path::Path,
    guard_keys: &[PkBuf],
    schema: &SchemaDescriptor,
) -> Vec<Option<String>> {
    let shards = open_shards(input_files, schema).unwrap();
    let n = guard_keys.len();
    let mut batches: Vec<Batch> = (0..n).map(|_| Batch::with_capacity(*schema, 256)).collect();
    let mut blob_caches: Vec<BlobCacheGuard> = (0..n).map(|_| BlobCacheGuard::acquire(schema, 256)).collect();
    run_merge(&shards, schema, |src, row, w| {
        let pk = shards[src].get_pk_bytes(row);
        let g = guard_slot(guard_keys, pk, PkBuf::pk_bytes);
        batches[g].append_row_from_source_bytes(pk, w, &shards[src], row, blob_caches[g].get_mut());
    });
    (0..n)
        .map(|g| {
            if batches[g].count == 0 {
                return None;
            }
            let path = out_dir.join(format!("oracle_G{g}.db"));
            let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
            batches[g]
                .write_as_shard(&cpath, schema, ShardWriteOpts::COMPACTION)
                .unwrap();
            Some(path.to_str().unwrap().to_string())
        })
        .collect()
}

/// Survivors spanning four guard ranges with cross-shard duplicate PKs: a
/// guard holding a fold to weight 4 plus a same-PK/different-payload pair, a
/// fully-cancelled guard (all its rows net to zero), an empty guard (no rows
/// route to it), and a distinct-PK guard. Each routed output shard must
/// equal the per-guard row-at-a-time oracle.
#[test]
fn test_merge_and_route_multi_guard_matches_row_at_a_time() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let schema = diff_schema();

    // Two input shards, each internally (PK, payload)-sorted. Guard ranges
    // (U64 PK): G0=[0,100), G1=[100,200), G2=[200,300), G3=[300,∞).
    let s0: &[DiffRow] = &[
        (10, 1, Some("aaa"), Some(LONG_A), Some(100)), // G0 — folds with s1 pk=10 → w=4
        (50, 1, Some("g0b"), Some("g0c"), None),       // G0 — c2 NULL
        (120, 1, Some("cancel"), Some("x"), Some(7)),  // G1 — cancelled by s1
        (310, 1, Some("t0"), Some("t1"), Some(1)),     // G3
        (320, 1, None, Some("u"), Some(2)),            // G3 — c0 NULL
    ];
    let s1: &[DiffRow] = &[
        (10, 1, Some("aaa"), Some("bbb"), Some(200)), // G0 — distinct payload (bbb < LONG_A)
        (10, 3, Some("aaa"), Some(LONG_A), Some(100)), // G0 — folds with s0 pk=10
        (120, -1, Some("cancel"), Some("x"), Some(7)), // G1 — cancels s0 pk=120 → net 0
        (330, 1, Some("t2"), Some(LONG_DUP), None),   // G3 — c2 NULL
    ];

    let p0 = dir.join("in0.db");
    let p1 = dir.join("in1.db");
    write_diff_shard(p0.to_str().unwrap(), &schema, s0);
    write_diff_shard(p1.to_str().unwrap(), &schema, s1);
    let c0 = std::ffi::CString::new(p0.to_str().unwrap()).unwrap();
    let c1 = std::ffi::CString::new(p1.to_str().unwrap()).unwrap();
    let inputs = [c0.as_c_str(), c1.as_c_str()];

    // A guard key is a whole OPK key, so derive them from the boundary values'
    // OPK bytes.
    let guard_keys: Vec<PkBuf> = [0u64, 100, 200, 300]
        .iter()
        .map(|&b| PkBuf::from_bytes(&b.to_be_bytes()))
        .collect();

    // table_id=7, level_num=1, compact_seq=42 → routed shards are named by their
    // part index within the compaction: shard_7_42_L1_P{g}.db.
    let dests: Vec<(PkBuf, bool)> = guard_keys.iter().map(|&k| (k, false)).collect();
    let routed = merge_and_route(&inputs, &dests, &schema, out(dir.to_str().unwrap(), 7, 1, 42)).unwrap();
    let oracle = oracle_merge_and_route_row_at_a_time(&inputs, &dir, &guard_keys, &schema);

    // Only the populated guards (0 and 3) produce output, in increasing-g order.
    assert_eq!(routed.len(), 2, "only guards 0 and 3 have survivors, got {routed:?}");
    assert_eq!(routed[0].0, guard_keys[0]);
    assert_eq!(routed[1].0, guard_keys[3]);

    for (g, oracle_entry) in oracle.iter().enumerate() {
        let routed_path = dir.join(super::super::naming::compact_shard_name(7, 42, 1, g));
        match oracle_entry {
            None => assert!(
                !routed_path.exists(),
                "guard {g} has no survivors — routed must write no shard"
            ),
            Some(oracle_path) => {
                assert!(routed_path.exists(), "guard {g} has survivors — routed shard missing");
                let rows_new = decode_diff_shard(routed_path.to_str().unwrap(), &schema);
                let rows_old = decode_diff_shard(oracle_path, &schema);
                assert_eq!(rows_new, rows_old, "guard {g} routed vs row-at-a-time rows diverged");
            }
        }
    }

    // Concrete per-guard pins beyond oracle agreement.
    let g0_name = super::super::naming::compact_shard_name(7, 42, 1, 0);
    let g0_rows = decode_diff_shard(dir.join(&g0_name).to_str().unwrap(), &schema);
    let pk10 = 10u64.to_be_bytes().to_vec();
    let folded = g0_rows
        .iter()
        .find(|r| r.0 == pk10 && r.3[2] == DiffCell::Int(100))
        .expect("folded pk=10 row present in guard 0");
    assert_eq!(folded.1, 4, "pk=10 folds across shards to weight 4");
    assert_eq!(
        g0_rows.len(),
        3,
        "guard 0: pk=10 (×2 payloads) + pk=50, got {g0_rows:?}"
    );

    let g3_name = super::super::naming::compact_shard_name(7, 42, 1, 3);
    let g3_rows = decode_diff_shard(dir.join(&g3_name).to_str().unwrap(), &schema);
    assert_eq!(g3_rows.len(), 3, "guard 3: pk=310,320,330, got {g3_rows:?}");
}

/// `merge_and_route`'s survivor split against `guard_slot` — independent
/// derivations of one rule, so a boundary slip (`>` where the rule says `>=`)
/// shows up as a disagreement. Swept across strides because at 24 and 32 the
/// keys differ only past byte 16, which a stride ≤ 16 case cannot reach.
#[test]
fn the_routed_split_agrees_with_guard_slot_at_every_stride() {
    for pk_cols in [1usize, 3, 4] {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let schema = pk_payload_schema(&vec![TYPE_U64; pk_cols]);
        assert_eq!(schema.pk_stride() as usize, pk_cols * 8);

        // Keys ascending in the LAST PK column, so every one of them shares its
        // leading `(pk_cols - 1) * 8` bytes with every other.
        let key = |i: u64| {
            let mut pk = vec![0u8; (pk_cols - 1) * 8];
            pk.extend_from_slice(&i.to_be_bytes());
            pk
        };
        let rows: Vec<(Vec<u8>, i64, i64)> = (0..400u64).map(|i| (key(i), 1, i as i64)).collect();
        let path = shard_file::write_test_shard(&dir.join("in.db"), &schema, &rows, ShardWriteOpts::default());

        // Guard keys straddling the data, including one below every row (so
        // guard 0's tail is exercised) and one above every row (an empty bucket).
        let guard_keys: Vec<PkBuf> = [0u64, 1, 100, 250, 399, 1000]
            .iter()
            .map(|&b| PkBuf::from_bytes(&key(b)))
            .collect();
        let dests: Vec<(PkBuf, bool)> = guard_keys.iter().map(|&k| (k, false)).collect();
        let routed = merge_and_route(&[path.as_c_str()], &dests, &schema, out(dir.to_str().unwrap(), 5, 1, 7)).unwrap();

        // Every written shard's rows must be exactly the ones `guard_slot` sends
        // to that part, and an unwritten part must be one `guard_slot` sends
        // nothing to.
        for (g, &gkey) in guard_keys.iter().enumerate() {
            let want: Vec<u64> = (0..400u64)
                .filter(|&i| guard_slot(&guard_keys, &key(i), PkBuf::pk_bytes) == g)
                .collect();
            let hit = routed.iter().find(|(k, _)| *k == gkey);
            match (hit, want.is_empty()) {
                (None, true) => {}
                (None, false) => panic!("stride {}: part {g} wrote no shard but owns {want:?}", pk_cols * 8),
                (Some((_, path)), _) => {
                    let shard =
                        MappedShard::open(&std::ffi::CString::new(path.as_str()).unwrap(), &schema, true).unwrap();
                    let got: Vec<Vec<u8>> = (0..shard.count).map(|r| shard.get_pk_bytes(r).to_vec()).collect();
                    assert_eq!(
                        got,
                        want.iter().map(|&i| key(i)).collect::<Vec<_>>(),
                        "stride {}: part {g} rows diverge from the guard_slot oracle",
                        pk_cols * 8,
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Skeleton (dehydrated) destination guards
// ---------------------------------------------------------------------------

mod skeleton_tests {
    use super::super::*;
    use crate::storage::repr::batch::{Batch, REG_NULL_BMP};
    use crate::storage::repr::layout::{ENCODING_CONSTANT, OFF_FILE_NPC, SHARD_FLAG_SKELETON};
    use crate::storage::repr::shard_file::{region_dir, ShardWriteOpts};
    use crate::storage::repr::shard_reader::MappedShard;
    use crate::test_support::make_schema_u64_i64;
    use gnitz_wire::{read_i64_le, read_u64_le};

    /// A `(PK, weight, payload)` shard, so one PK can carry several payloads.
    fn write_shard(path: &std::path::Path, rows: &[(u64, i64, i64)], schema: &SchemaDescriptor) -> std::ffi::CString {
        let mut b = Batch::with_capacity(*schema, rows.len().max(1));
        for &(pk, w, v) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &v.to_le_bytes());
            b.count += 1;
        }
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        b.into_consolidated(schema)
            .write_as_shard(&cpath, schema, ShardWriteOpts::default())
            .unwrap();
        cpath
    }

    /// `(pk, weight)` of every row of an output shard, read under the view schema.
    fn read_rows(path: &str, schema: &SchemaDescriptor) -> Vec<(u128, i64)> {
        let s = MappedShard::open(&std::ffi::CString::new(path).unwrap(), schema, false).unwrap();
        (0..s.count).map(|i| (s.get_pk(i), s.get_weight(i))).collect()
    }

    /// A guard's *first* dehydration: survivors still hold several payloads per
    /// PK, and the output is exactly one `(PK, Σweight)` row per key. Its null
    /// region collapses to `ENCODING_CONSTANT` (8 bytes for the whole file), and
    /// the file declares itself skeleton with a zero payload arity.
    #[test]
    fn first_dehydration_folds_a_guard_to_one_row_per_pk() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let schema = make_schema_u64_i64();

        // PK 1: three payloads summing to 6. PK 2: one row. PK 3: cancels.
        let a = write_shard(
            &dir.join("a.db"),
            &[(1, 1, 10), (1, 2, 20), (2, 5, 30), (3, 4, 40)],
            &schema,
        );
        let b = write_shard(&dir.join("b.db"), &[(1, 3, 50), (3, -4, 40)], &schema);

        let outs = merge_and_route(
            &[a.as_c_str(), b.as_c_str()],
            &[(crate::schema::key::PkBuf::zeroed(schema.pk_stride() as usize), true)],
            &schema,
            super::out(dir.to_str().unwrap(), 0, 2, 1),
        )
        .unwrap();
        assert_eq!(outs.len(), 1);
        let path = &outs[0].1;

        assert_eq!(read_rows(path, &schema), vec![(1, 6), (2, 5)], "PK 3 cancelled exactly");

        let raw = std::fs::read(path).unwrap();
        assert_eq!(
            read_u64_le(&raw, OFF_FILE_NPC),
            SHARD_FLAG_SKELETON,
            "declared skeleton, zero payload arity"
        );
        assert_eq!(region_dir(&raw, REG_NULL_BMP).1, ENCODING_CONSTANT);
        assert_eq!(region_dir(&raw, REG_NULL_BMP).0, 8, "one element for the whole file");
    }

    /// A fold routing into a mixed destination set writes each guard in its own
    /// representation, and the hydrated guard's output is byte-identical to the
    /// same fold with no dehydrated sibling.
    #[test]
    fn a_mixed_destination_set_writes_each_guard_in_its_own_form() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let schema = make_schema_u64_i64();
        let src = write_shard(&dir.join("src.db"), &[(1, 1, 10), (1, 2, 11), (100, 3, 30)], &schema);
        // Guard 0 owns [.., 100), guard 1 owns [100, ..).
        let g0 = crate::schema::key::PkBuf::zeroed(schema.pk_stride() as usize);
        let g1 = crate::schema::key::PkBuf::from_bytes(&100u64.to_be_bytes());

        let mixed = merge_and_route(
            &[src.as_c_str()],
            &[(g0, true), (g1, false)],
            &schema,
            super::out(dir.to_str().unwrap(), 0, 2, 1),
        )
        .unwrap();
        let all_hydrated = merge_and_route(
            &[src.as_c_str()],
            &[(g0, false), (g1, false)],
            &schema,
            super::out(dir.to_str().unwrap(), 0, 2, 2),
        )
        .unwrap();

        assert_eq!(mixed.len(), 2);
        // Guard 0 dehydrated: its two payloads for PK 1 fold to one coarse row.
        assert_eq!(read_rows(&mixed[0].1, &schema), vec![(1, 3)]);
        assert!(
            MappedShard::open(&crate::storage::cstr(mixed[0].1.as_str()).unwrap(), &schema, false)
                .unwrap()
                .is_skeleton()
        );
        // Guard 1 hydrated: identical bytes to the all-hydrated run, modulo the
        // compaction sequence in the filename.
        let hy = std::fs::read(&mixed[1].1).unwrap();
        let ref_ = std::fs::read(&all_hydrated[1].1).unwrap();
        assert_eq!(hy.len(), ref_.len());
        assert_eq!(
            read_i64_le(
                MappedShard::open(&crate::storage::cstr(mixed[1].1.as_str()).unwrap(), &schema, false)
                    .unwrap()
                    .get_col_ptr(0, 0, 8),
                0
            ),
            30,
            "the hydrated guard keeps its payload"
        );
    }
}
