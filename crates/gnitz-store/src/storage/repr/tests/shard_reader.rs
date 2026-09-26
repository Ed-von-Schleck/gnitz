use super::super::batch::{Batch, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::super::error::StorageError;
use super::super::layout::*;
use super::super::merge::ColumnarSource;
use super::super::shard_file::{region_dir, write_i64_shard, ShardWriteOpts};
use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64, read_german_string};
use gnitz_expr::RowSource;
use gnitz_wire::num_regions;
use gnitz_wire::{read_i64_le, write_u64_le};

/// Build a shard through `write_as_shard` (uses encoding selection).
fn build_test_shard(dir: &std::path::Path, rows: &[(u64, i64)]) -> String {
    let pks: Vec<u64> = rows.iter().map(|&(pk, _)| pk).collect();
    let weights: Vec<i64> = rows.iter().map(|_| 1i64).collect();
    let vals: Vec<i64> = rows.iter().map(|&(_, v)| v).collect();
    build_test_shard_weights(dir, "test.db", &pks, &weights, &vals, false)
}

/// Build a `(U64 PK | I64 payload)` shard with custom weights; `pack`
/// toggles FoR on the payload region.
fn build_test_shard_weights(
    dir: &std::path::Path,
    name: &str,
    pks: &[u64],
    wts: &[i64],
    vals: &[i64],
    pack: bool,
) -> String {
    let rows: Vec<(Vec<u8>, i64, i64)> = (0..pks.len())
        .map(|i| (pks[i].to_be_bytes().to_vec(), wts[i], vals[i]))
        .collect();
    let path = dir.join(name);
    super::super::shard_file::write_test_shard(
        &path,
        &make_schema_u64_i64(),
        &rows,
        ShardWriteOpts { pack_ints: pack, ..Default::default() },
    );
    path.to_str().unwrap().to_string()
}

/// Patch a copy of `base` — the image already written at `path` — write it
/// back to `path`, and open it. The descriptive digest is left stale, so a
/// patch inside the prefix is rejected by the digest. Writing back to the
/// same path keeps the basename, and with it the digest's seed, unchanged;
/// under a fresh name every case would fail on the name alone.
fn open_patched(
    path: &str,
    schema: &SchemaDescriptor,
    base: &[u8],
    patch: impl FnOnce(&mut Vec<u8>),
) -> Result<MappedShard, StorageError> {
    let mut data = base.to_vec();
    patch(&mut data);
    std::fs::write(path, &data).unwrap();
    MappedShard::open(path, schema)
}

/// As [`open_patched`], with the descriptive digest re-stamped over `schema`'s
/// prefix, so the forgery reaches the check under test.
fn open_patched_restamped(
    path: &str,
    schema: &SchemaDescriptor,
    base: &[u8],
    patch: impl FnOnce(&mut Vec<u8>),
) -> Result<MappedShard, StorageError> {
    open_patched(path, schema, base, |data| {
        patch(data);
        let cs = desc_digest(path, &data[..desc_len(schema.num_payload_cols())]);
        write_u64_le(data, OFF_DESC_CHECKSUM, cs);
    })
}

/// Rewrite directory entry `i` of `data` through `patch`.
fn patch_entry(data: &mut [u8], i: usize, patch: impl FnOnce(&mut DirEntry)) {
    let mut e = DirEntry::read(data, i);
    patch(&mut e);
    e.write(data, i);
}

/// The offset region `i` of `image` starts at.
fn region_offset(image: &[u8], i: usize) -> usize {
    region_spans(image, ShardHeader::read(image).unwrap().file_npc).unwrap()[i].off
}

#[test]
fn open_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    assert_eq!(shard.count, 10);
    assert_eq!(shard.get_pk(0), 1);
    assert_eq!(shard.get_pk(9), 10);
    assert_eq!(shard.get_weight(0), 1);
}

#[test]
fn binary_search() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=100).map(|i| (i * 2, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();

    // Present keys, absent keys, and both ends, against a linear scan.
    for probe in [0u64, 1, 3, 10, 100, 200, 201, u64::MAX] {
        let key = probe.to_be_bytes();
        let want = (0..shard.count)
            .find(|&i| crate::schema::key::compare_pk_bytes(shard.get_pk_bytes(i), &key) != std::cmp::Ordering::Less)
            .unwrap_or(shard.count);
        assert_eq!(shard.find_lower_bound_bytes(&key), want, "probe={probe}");
        for hint in [0, shard.count / 2, shard.count] {
            assert_eq!(shard.advance_to(&key, hint), want, "probe={probe} hint={hint}");
        }
    }
}

#[test]
fn pk_and_payload_addressing() {
    let dir = tempfile::tempdir().unwrap();
    let rows = vec![(1u64, 42i64), (2, 84)];
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();

    // PK column holds OPK (big-endian) bytes at rest.
    assert_eq!(u64::from_be_bytes(shard.get_pk_bytes(0).try_into().unwrap()), 1);
    assert_eq!(read_i64_le(shard.get_col_ptr(0, 0, 8), 0), 42);
    assert_eq!(read_i64_le(shard.get_col_ptr(1, 0, 8), 0), 84);
}

// --- ALTER TABLE ADD COLUMN: reading pre-ALTER bytes -------------------

/// `(U64 PK, I64, <tail>)` where `tail` is nullable — the shape an
/// `ADD COLUMN` leaves behind. `make_schema_u64_i64` is its narrow twin.
fn schema_with_appended(tail: TypeCode) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(tail, true),
        ],
        &[0],
    )
}

/// Open a narrow shard (one I64 payload column) under a schema that has
/// since grown a trailing nullable column.
fn open_widened(dir: &std::path::Path, name: &str, rows: &[(u64, i64)], tail: TypeCode) -> MappedShard {
    let pks: Vec<u64> = rows.iter().map(|&(pk, _)| pk).collect();
    let wts: Vec<i64> = rows.iter().map(|_| 1i64).collect();
    let vals: Vec<i64> = rows.iter().map(|&(_, v)| v).collect();
    let path = build_test_shard_weights(dir, name, &pks, &wts, &vals, false);
    MappedShard::open(&path, &schema_with_appended(tail)).unwrap()
}

#[test]
fn file_npc_header_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = build_test_shard(dir.path(), &[(1u64, 10i64)]);
    let data = std::fs::read(&path).unwrap();
    // The writer stamps its own descriptor's payload arity.
    assert_eq!(ShardHeader::read(&data).unwrap().file_npc, 1);

    // A shard read back at the width it was written pays nothing.
    let schema = make_schema_u64_i64();
    let shard = MappedShard::open(&path, &schema).unwrap();
    assert_eq!(shard.null_pad_mask, 0);
    assert_eq!(shard.col_regions.len(), 1);
}

#[test]
fn padded_shard_pads_the_appended_column() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=5).map(|i| (i, i as i64 * 100)).collect();
    let shard = open_widened(dir.path(), "pad.db", &rows, TypeCode::I64);

    // The walk is driven by the file's own arity: one mapped column, and the
    // appended one reads `ZERO_CELL` rather than a mis-read directory entry (a
    // mis-read Constant entry would also be stride 0, so the address decides).
    assert_eq!(shard.col_regions.len(), 2);
    assert!(matches!(shard.col_regions[1], PayloadRegion::Mapped(cp) if std::ptr::eq(cp.base, ZERO_CELL.as_ptr())));
    assert_eq!(shard.null_pad_mask, 1 << 1);
    assert_eq!(shard.count, 5);

    for row in 0..5 {
        // Reader 1: the per-row null word. Column 0 stays non-null, the
        // appended column 1 reads NULL.
        let w = shard.get_null_word(row);
        assert!(!gnitz_wire::null_word_get(w, 0), "row {row} col 0");
        assert!(gnitz_wire::null_word_get(w, 1), "row {row} col 1");
        // The file's own column still reads its value…
        assert_eq!(read_i64_le(shard.get_col_ptr(row, 0, 8), 0), (row as i64 + 1) * 100);
        // …and the absent one hands back zeros, not mmap bytes.
        assert_eq!(shard.get_col_ptr(row, 1, 8), [0u8; 8].as_slice());
    }
}

#[test]
fn padded_shard_slice_to_owned_batch_pads() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=4).map(|i| (i, i as i64)).collect();
    let shard = open_widened(dir.path(), "pad_slice.db", &rows, TypeCode::I64);
    let schema = schema_with_appended(TypeCode::I64);

    // Reader 2: the bulk materializer.
    let batch = shard.slice_to_owned_batch(0, 4, &schema);
    assert_eq!(batch.count, 4);
    for row in 0..4 {
        let w = batch.get_null_word(row);
        assert!(!gnitz_wire::null_word_get(w, 0));
        assert!(gnitz_wire::null_word_get(w, 1), "row {row} appended col not NULL");
    }
    // The appended column's cells are written (the arena is uninitialised),
    // and written as zero.
    assert_eq!(batch.col_data(1), [0u8; 32].as_slice());
}

#[test]
fn padded_shard_to_unified_pads() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=3).map(|i| (i, i as i64)).collect();
    let shard = open_widened(dir.path(), "pad_unified.db", &rows, TypeCode::I64);

    // Reader 3: the shared column-first scatter reads `null_pad_mask` off the
    // view rather than the shard, so the view must carry it.
    let mut cols = Vec::new();
    let unified = shard.to_unified(&schema_with_appended(TypeCode::I64), &mut cols);
    assert_eq!(unified.null_pad_mask, 1 << 1);
    // The absent column reads one shared `'static` zero cell for every row —
    // the same `stride == 0` shape a Constant region uses, so the gather has
    // no per-row branch and never dereferences a null base.
    let absent = cols[unified.cols_off + 1];
    assert_eq!(absent.stride, 0);
    assert!(!absent.base.is_null());
    assert_eq!(unsafe { absent.row(2, 8) }, [0u8; 8].as_slice());
}

#[test]
fn padded_shard_with_appended_string_column() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=4).map(|i| (i, i as i64)).collect();
    let shard = open_widened(dir.path(), "pad_str.db", &rows, TypeCode::String);
    let schema = schema_with_appended(TypeCode::String);

    // Both blob arms: the relocating one walks *schema* payload columns and
    // reaches the appended STRING with the zero cell, which decodes as length
    // 0 and never touches the heap; the whole-region arm copies a heap the
    // file does have (empty here).
    for relocate in [true, false] {
        let batch = shard.slice_to_owned_batch_with(0, 4, &schema, relocate);
        assert_eq!(batch.count, 4, "relocate={relocate}");
        for row in 0..4 {
            assert!(
                gnitz_wire::null_word_get(batch.get_null_word(row), 1),
                "relocate={relocate} row={row}"
            );
        }
    }
}

#[test]
fn shard_wider_than_reader_schema_opens() {
    // Reachable from a correct crash: a checkpoint publishes base manifests
    // before it makes the catalog durable, so a boot can read the catalog
    // back at width N and find a shard written at N+1. Rejecting it would
    // make the database unbootable, so the reader narrows instead.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wide.db");
    let wide = schema_with_appended(TypeCode::I64);
    let rows: Vec<_> = (1u64..=3)
        .map(|p| (p.to_be_bytes().to_vec(), 1, 0, vec![p as i64 * 10, p as i64 * 11]))
        .collect();
    let shard_path = path.to_str().unwrap().to_owned();
    write_i64_shard(&shard_path, &wide, &rows, &[], ShardWriteOpts::default());

    let narrow = make_schema_u64_i64();
    let shard = MappedShard::open(&shard_path, &narrow).unwrap();
    // Every column the reader's schema names is served; the surplus directory
    // entry is left unmapped and needs no pad.
    assert_eq!(shard.col_regions.len(), 1);
    assert_eq!(shard.null_pad_mask, 0);
    for row in 0..3 {
        assert_eq!(read_i64_le(shard.get_col_ptr(row, 0, 8), 0), (row as i64 + 1) * 10);
    }
    // The blob region came from the *file's* index, not the reader's.
    assert_eq!(shard.blob_len, 0);
}

#[test]
fn forged_file_npc_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = build_test_shard(dir.path(), &[(1u64, 10i64)]);
    let schema = make_schema_u64_i64();
    let base = std::fs::read(&path).unwrap();

    for forged in [u64::MAX, gnitz_wire::MAX_COLUMNS as u64 + 1] {
        assert_eq!(
            open_patched(&path, &schema, &base, |d| write_u64_le(d, OFF_FILE_NPC, forged)).err(),
            Some(StorageError::Corrupt("payload arity")),
            "file_npc {forged}",
        );
    }

    // In range but wrong: the digest rejects it.
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| write_u64_le(d, OFF_FILE_NPC, 2)).err(),
        Some(StorageError::Corrupt("descriptor digest"))
    );
}

/// An all-PK table widened by `ADD COLUMN` has a skeleton's region shape, and
/// its NULL columns are real: only the flag makes a skeleton.
#[test]
fn a_widened_all_pk_shard_is_not_a_skeleton() {
    let dir = tempfile::tempdir().unwrap();
    let all_pk = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false)], &[0]);
    let widened = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let shard_path = dir.path().join("allpk.db").to_str().unwrap().to_owned();
    write_i64_shard(
        &shard_path,
        &all_pk,
        &[(1u64.to_be_bytes().to_vec(), 1, 0, vec![])],
        &[],
        ShardWriteOpts::default(),
    );
    let shard = MappedShard::open(&shard_path, &widened).unwrap();
    assert!(gnitz_wire::null_word_get(shard.get_null_word(0), 0));
    assert!(!shard.is_skeleton());
}

#[test]
fn skeleton_flag_is_covered_by_the_digest() {
    let dir = tempfile::tempdir().unwrap();
    let path = build_test_shard(dir.path(), &[(1u64, 42i64)]);
    let schema = make_schema_u64_i64();
    let base = std::fs::read(&path).unwrap();
    assert!(!MappedShard::open(&path, &schema).unwrap().is_skeleton());

    let forge = |d: &mut Vec<u8>| write_u64_le(d, OFF_FLAGS, SHARD_FLAG_SKELETON);
    assert_eq!(
        open_patched(&path, &schema, &base, forge).err(),
        Some(StorageError::Corrupt("descriptor digest")),
    );
    assert!(open_patched_restamped(&path, &schema, &base, forge)
        .unwrap()
        .is_skeleton());
}

#[test]
fn rebind_matches_a_fresh_open() {
    let dir = tempfile::tempdir().unwrap();
    let n = 200usize;
    let pks: Vec<u64> = (0..n as u64).collect();
    let vals: Vec<i64> = (0..n as i64).map(|i| 90_000 + (i % 150)).collect();
    let path = build_i64_shard(dir.path(), "rebind.db", &pks, &vals, true);
    let narrow = MappedShard::open(&path, &make_schema_u64_i64()).unwrap();
    // Decode the packed column on the narrow handle first.
    assert_eq!(read_i64_le(narrow.get_col_ptr(0, 0, 8), 0), vals[0]);

    let wide = schema_with_appended(TypeCode::I64);
    let rebound = narrow.rebind(&wide).unwrap();
    let fresh = MappedShard::open(&path, &wide).unwrap();
    assert!(std::rc::Rc::ptr_eq(&narrow.mmap, &rebound.mmap));
    assert!(matches!(&rebound.col_regions[0], PayloadRegion::Packed(p) if p.decoded.get().is_none()));
    assert!(matches!(rebound.col_regions[1], PayloadRegion::Mapped(cp) if std::ptr::eq(cp.base, ZERO_CELL.as_ptr())));
    assert_eq!(rebound.null_pad_mask, fresh.null_pad_mask);
    assert_eq!(rebound.count, fresh.count);
    for r in 0..n {
        assert_eq!(rebound.get_pk_bytes(r), fresh.get_pk_bytes(r), "pk row {r}");
        assert_eq!(rebound.get_weight(r), fresh.get_weight(r), "weight row {r}");
        assert_eq!(rebound.get_null_word(r), fresh.get_null_word(r), "null row {r}");
        for pi in 0..2 {
            assert_eq!(
                rebound.get_col_ptr(r, pi, 8),
                fresh.get_col_ptr(r, pi, 8),
                "col {pi} row {r}"
            );
        }
    }
    assert_eq!(read_i64_le(rebound.get_col_ptr(n - 1, 0, 8), 0), vals[n - 1]);
}

#[test]
fn previous_format_version_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = build_test_shard(dir.path(), &[(1u64, 10i64)]);
    let schema = make_schema_u64_i64();
    let base = std::fs::read(&path).unwrap();
    // A v11 file carries a zero `file_npc`; the version word is what rejects
    // it, ahead of every structural read.
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| {
            write_u64_le(d, OFF_VERSION, SHARD_VERSION - 1);
            write_u64_le(d, OFF_FILE_NPC, 0);
        })
        .err(),
        Some(StorageError::Corrupt("version"))
    );
}

#[test]
fn checksum_validation() {
    let dir = tempfile::tempdir().unwrap();
    let rows = vec![(1u64, 10i64)];
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    assert_eq!(MappedShard::open(&path, &schema).unwrap().verify_body(), Ok(()));

    // Open leaves the body unchecked.
    let path_str = path.as_str();
    let mut data = std::fs::read(path_str).unwrap();
    let pk_off = region_offset(&data, REG_PK);
    data[pk_off] ^= 0xFF;
    std::fs::write(path_str, &data).unwrap();

    let shard = MappedShard::open(&path, &schema).unwrap();
    assert_eq!(shard.verify_body(), Err(StorageError::Corrupt("body checksum")));

    // The alignment padding between the directory and the first region is
    // covered too.
    let pad_off = desc_len(schema.num_payload_cols());
    let mut data = std::fs::read(path_str).unwrap();
    data[pk_off] ^= 0xFF;
    assert!(pad_off < pk_off, "premise: the first region is preceded by padding");
    data[pad_off] ^= 0xFF;
    std::fs::write(path_str, &data).unwrap();
    let shard = MappedShard::open(&path, &schema).unwrap();
    assert_eq!(shard.verify_body(), Err(StorageError::Corrupt("body checksum")));
}

#[test]
fn a_zero_row_count_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = build_test_shard(dir.path(), &[(1u64, 10i64)]);
    let schema = make_schema_u64_i64();
    let base = std::fs::read(&path).unwrap();
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| write_u64_le(d, OFF_ROW_COUNT, 0)).err(),
        Some(StorageError::Corrupt("no rows")),
    );
}

// --- v4 encoding tests ---

#[test]
fn constant_weight_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let n = 100;
    let pks: Vec<u64> = (1..=n).collect();
    let wts: Vec<i64> = vec![1; n as usize];
    let vals: Vec<i64> = (1..=n).map(|i| i as i64 * 10).collect();
    let path = build_test_shard_weights(dir.path(), "const_w.db", &pks, &wts, &vals, false);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    assert_eq!(shard.count, n as usize);
    for i in 0..n as usize {
        assert_eq!(shard.get_weight(i), 1);
    }

    // One weight value across every row must be stored as a single 8-byte
    // Constant region rather than n*8 raw bytes.
    let image = std::fs::read(&path).unwrap();
    assert_eq!(region_dir(&image, REG_WEIGHT), (8, Encoding::Constant));
}

#[test]
fn two_value_weight_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let n = 64usize;
    let pks: Vec<u64> = (1..=n as u64).collect();
    let wts: Vec<i64> = (0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
    let vals: Vec<i64> = (0..n).map(|i| i as i64 * 10).collect();
    let path = build_test_shard_weights(dir.path(), "twoval_w.db", &pks, &wts, &vals, false);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    assert_eq!(shard.count, n);
    for i in 0..n {
        let expected = if i % 2 == 0 { 1i64 } else { -1i64 };
        assert_eq!(shard.get_weight(i), expected, "row {i}");
    }
}

#[test]
fn three_value_weight_raw() {
    let dir = tempfile::tempdir().unwrap();
    let pks: Vec<u64> = vec![1, 2, 3];
    let wts: Vec<i64> = vec![1, -1, 2];
    let vals: Vec<i64> = vec![10, 20, 30];
    let path = build_test_shard_weights(dir.path(), "raw_w.db", &pks, &wts, &vals, false);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    assert_eq!(shard.get_weight(0), 1);
    assert_eq!(shard.get_weight(1), -1);
    assert_eq!(shard.get_weight(2), 2);
}

#[test]
fn constant_null_bmp() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    for i in 0..10 {
        assert_eq!(shard.get_null_word(i), 0);
    }
}

#[test]
fn constant_payload_column() {
    let dir = tempfile::tempdir().unwrap();
    let n = 10;
    let pks: Vec<u64> = (1..=n).collect();
    let wts: Vec<i64> = vec![1; n as usize];
    let vals: Vec<i64> = vec![42; n as usize]; // all same
    let path = build_test_shard_weights(dir.path(), "const_col.db", &pks, &wts, &vals, false);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    for i in 0..n as usize {
        // Every row reads the region's single element (stride 0).
        assert_eq!(read_i64_le(shard.get_col_ptr(i, 0, 8), 0), 42);
    }
}

/// Each region role admits only certain encodings. Forging one that a role may
/// not carry must be refused at open, whatever the byte means elsewhere. The
/// digest is re-stamped so the verdict is the decode site's, not the digest's.
#[test]
fn an_encoding_a_role_may_not_carry_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=8).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();
    let base = std::fs::read(&path).unwrap();
    let blob = num_regions(schema.num_payload_cols()) - 1;
    let filter = blob + 1;

    // (region, forged encoding byte)
    let cases: &[(usize, u8)] = &[
        (REG_PK, 0x10), // not an encoding at all
        (REG_PK, Encoding::TwoValue as u8),
        (REG_PK, Encoding::For as u8),
        (REG_WEIGHT, Encoding::For as u8),
        (REG_NULL_BMP, Encoding::For as u8),
        (blob, Encoding::For as u8),
        (REG_PAYLOAD_START, Encoding::TwoValue as u8),
        (blob, Encoding::Constant as u8),
        (filter, Encoding::Constant as u8),
    ];
    for &(region, enc) in cases {
        let opened = open_patched_restamped(&path, &schema, &base, |data| {
            patch_entry(data, region, |e| e.encoding = enc);
        });
        assert_eq!(
            opened.err(),
            Some(StorageError::Corrupt("encoding")),
            "encoding {enc:#x} on region {region} must be rejected",
        );
    }
}

#[test]
fn single_row_shard() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = vec![(42, 999)];
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    assert_eq!(shard.count, 1);
    assert_eq!(shard.get_pk(0), 42);
    assert_eq!(shard.get_weight(0), 1);
    assert_eq!(shard.get_null_word(0), 0);
    assert_eq!(shard.find_lower_bound_bytes(&42u64.to_be_bytes()), 0);
    assert_eq!(shard.find_lower_bound_bytes(&43u64.to_be_bytes()), 1);
}

#[test]
fn whole_shard_slice_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    let batch = shard.slice_to_owned_batch(0, shard.count, &schema);

    assert_eq!(batch.count, 10);
    for i in 0..10 {
        assert_eq!(batch.get_pk(i), (i + 1) as u128);
        let w = read_i64_le(batch.weight_data(), i * 8);
        assert_eq!(w, 1);
        let v = read_i64_le(batch.col_data(0), i * 8);
        assert_eq!(v, (i as i64 + 1) * 100);
    }
}

#[test]
fn whole_shard_slice_constant_regions() {
    // All weights = 1 (Constant), all null = 0 (Constant), all vals = 42 (Constant)
    let dir = tempfile::tempdir().unwrap();
    let n = 20;
    let pks: Vec<u64> = (1..=n).collect();
    let wts: Vec<i64> = vec![1; n as usize];
    let vals: Vec<i64> = vec![42; n as usize]; // constant payload
    let path = build_test_shard_weights(dir.path(), "const_batch.db", &pks, &wts, &vals, false);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    let batch = shard.slice_to_owned_batch(0, shard.count, &schema);

    assert_eq!(batch.count, n as usize);
    for i in 0..n as usize {
        assert_eq!(batch.get_pk(i), (i + 1) as u128);
        assert_eq!(read_i64_le(batch.weight_data(), i * 8), 1);
        assert_eq!(read_i64_le(batch.col_data(0), i * 8), 42);
    }
}

#[test]
fn whole_shard_slice_two_value_weight() {
    let dir = tempfile::tempdir().unwrap();
    let n = 16usize;
    let pks: Vec<u64> = (1..=n as u64).collect();
    let wts: Vec<i64> = (0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
    let vals: Vec<i64> = (0..n).map(|i| i as i64 * 10).collect();
    let path = build_test_shard_weights(dir.path(), "twoval_batch.db", &pks, &wts, &vals, false);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    let batch = shard.slice_to_owned_batch(0, shard.count, &schema);

    assert_eq!(batch.count, n);
    for i in 0..n {
        let expected_w = if i % 2 == 0 { 1i64 } else { -1i64 };
        assert_eq!(read_i64_le(batch.weight_data(), i * 8), expected_w, "row {i}");
        assert_eq!(read_i64_le(batch.col_data(0), i * 8), i as i64 * 10);
    }
}

#[test]
fn slice_to_owned_batch_with_offset() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();

    // Slice rows 3..7 (0-indexed: PKs 4,5,6,7)
    let batch = shard.slice_to_owned_batch(3, 4, &schema);
    assert_eq!(batch.count, 4);
    assert_eq!(batch.get_pk(0), 4);
    assert_eq!(batch.get_pk(3), 7);
    assert_eq!(read_i64_le(batch.col_data(0), 0), 400);
    assert_eq!(read_i64_le(batch.col_data(0), 3 * 8), 700);
}

#[test]
fn slice_to_owned_batch_empty() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=5).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    let batch = shard.slice_to_owned_batch(0, 0, &schema);
    assert_eq!(batch.count, 0);
}

#[test]
fn u64_pk_open_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = vec![(10, 100), (20, 200), (30, 300)];
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    assert_eq!(shard.pk_stride, 8, "pk_stride must be 8 for U64 schema");
    assert_eq!(shard.count, 3);
    assert_eq!(shard.get_pk(0), 10u128);
    assert_eq!(shard.get_pk(1), 20u128);
    assert_eq!(shard.get_pk(2), 30u128);
    assert_eq!(shard.find_lower_bound_bytes(&15u64.to_be_bytes()), 1);
    assert_eq!(shard.find_lower_bound_bytes(&10u64.to_be_bytes()), 0);
    assert_eq!(shard.find_lower_bound_bytes(&31u64.to_be_bytes()), 3);
}

#[test]
fn u64_pk_slice_to_owned_batch() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1u64..=8).map(|i| (i * 10, i as i64 * 100)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();

    let shard = MappedShard::open(&path, &schema).unwrap();
    let batch = shard.slice_to_owned_batch(0, 8, &schema);

    assert_eq!(batch.count, 8);
    // pk_data() length = count * pk_stride = 8 * 8
    assert_eq!(batch.pk_data().len(), 8 * 8, "pk region must be 8B/row for U64 schema");
    for i in 0..8usize {
        assert_eq!(batch.get_pk(i), (i as u128 + 1) * 10, "PK row {i}");
    }
}

fn u128_pk_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    )
}

fn build_test_shard_u128(dir: &std::path::Path, name: &str, pks: &[u128], vals: &[i64]) -> String {
    let path = dir.join(name);
    // PK region holds OPK (order-preserving big-endian) bytes at rest.
    let rows: Vec<_> = pks
        .iter()
        .zip(vals)
        .map(|(&p, &v)| (p.to_be_bytes().to_vec(), 1, 0, vec![v]))
        .collect();
    let shard_path = path.to_str().unwrap().to_owned();
    write_i64_shard(&shard_path, &u128_pk_schema(), &rows, &[], ShardWriteOpts::default());
    path.to_str().unwrap().to_string()
}

/// `get_pk_bytes` returns the OPK image for every PK region shape: per-row at
/// both strides, and the Constant region that stores one key for the shard.
#[test]
fn get_pk_bytes_returns_the_opk_image_for_every_region_shape() {
    let dir = tempfile::tempdir().unwrap();
    let wide = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210u128;
    // (label, PKs, per-row region?) — identical PKs make the writer pick Constant.
    let cases: [(&str, Vec<u128>, bool); 3] = [
        ("u64 per-row", (1..=8u128).map(|i| i * 3).collect(), true),
        (
            "u128 per-row",
            vec![1, (u64::MAX as u128) + 1, (u64::MAX as u128) * 2 + 3, u128::MAX],
            true,
        ),
        ("u128 constant", vec![wide; 32], false),
    ];

    for (what, pks, per_row) in cases {
        // A u64-ranged key list still round-trips through the u128 writer; what
        // varies here is the region encoding, not the schema width.
        let vals: Vec<i64> = (0..pks.len() as i64).collect();
        let path = build_test_shard_u128(dir.path(), &format!("{what}.db"), &pks, &vals);
        let schema = u128_pk_schema();
        let shard = MappedShard::open(&path, &schema).unwrap();

        assert_eq!(shard.pk.stride != 0, per_row, "{what}: region shape");
        assert_eq!(shard.pk_stride, 16, "{what}: stride");
        for (i, &pk) in pks.iter().enumerate() {
            assert_eq!(shard.get_pk_bytes(i), &pk.to_be_bytes(), "{what} row {i}: opk bytes");
            assert_eq!(shard.get_pk(i), pk, "{what} row {i}: value");
        }
    }
}

#[test]
fn find_lower_bound_bytes_wide_pk_distinct() {
    // Wide PK (3xU64 all-PK, stride 24). Distinct PKs keep the PK region
    // per-row.
    //
    // Schema is all-PK (num_payload = 0), so the region count is the
    // writer↔reader contract 3 + num_payload_cols + 1 = 4:
    // [pk, weight, null_bmp, blob]. open() reads exactly that many directory
    // entries.
    let dir = tempfile::tempdir().unwrap();
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0, 1, 2],
    );
    assert_eq!(schema.pk_stride(), 24);

    // OPK for a 3xU64 compound PK: each column big-endian, concatenated in
    // pk-list order. memcmp of the bytes then equals (col0, col1, col2)
    // lexicographic order.
    let opk3 = |a: u64, b: u64, c: u64| -> [u8; 24] {
        let mut out = [0u8; 24];
        out[0..8].copy_from_slice(&a.to_be_bytes());
        out[8..16].copy_from_slice(&b.to_be_bytes());
        out[16..24].copy_from_slice(&c.to_be_bytes());
        out
    };

    // Five rows, sorted in compare_pk_bytes order (col0, then col1, col2).
    let pks: [[u8; 24]; 5] = [
        opk3(0, 0, 0),
        opk3(1, 0, 0),
        opk3(1, 5, 0),
        opk3(1, 5, 9),
        opk3(2, 0, 0),
    ];
    let rows: Vec<_> = pks.iter().map(|r| (r.to_vec(), 1, 0, vec![])).collect();
    let path = dir.path().join("wide_pk.db");
    let shard_path = path.to_str().unwrap().to_owned();
    write_i64_shard(&shard_path, &schema, &rows, &[], ShardWriteOpts::default());
    let shard = MappedShard::open(&shard_path, &schema).unwrap();
    assert_eq!(shard.pk_stride, 24);
    assert!(shard.pk.stride != 0, "distinct PKs must keep the PK region per-row");

    // Probe keys covering before, between, and after each row.
    let probes: [[u8; 24]; 5] = [
        opk3(0, 0, 0),
        opk3(0, 0, 1),
        opk3(1, 5, 9),
        opk3(1, 5, 10),
        opk3(3, 0, 0),
    ];
    for key in &probes {
        let expected = (0..shard.count)
            .find(|&i| crate::schema::key::compare_pk_bytes(shard.get_pk_bytes(i), key) != std::cmp::Ordering::Less)
            .unwrap_or(shard.count);
        let got = shard.find_lower_bound_bytes(key);
        assert_eq!(got, expected, "probe={key:?}");
    }
}

// -----------------------------------------------------------------------
// FoR (Encoding::For) packed-payload reader tests
// -----------------------------------------------------------------------

/// Build a `(U64 PK | I64 payload)` shard with all-1 weights;
/// `pack` toggles FoR on the payload region.
fn build_i64_shard(dir: &std::path::Path, name: &str, pks: &[u64], vals: &[i64], pack: bool) -> String {
    build_test_shard_weights(dir, name, pks, &vec![1i64; pks.len()], vals, pack)
}

#[test]
fn packed_roundtrip_all_surfaces() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    // Small, narrow-range payload → FoR-eligible; distinct so not Constant.
    let n = 500usize;
    let pks: Vec<u64> = (0..n as u64).collect();
    let vals: Vec<i64> = (0..n as i64).map(|i| 1_000_000 + (i % 300)).collect();

    let packed_path = build_i64_shard(dir.path(), "packed.db", &pks, &vals, true);
    let raw_path = build_i64_shard(dir.path(), "raw.db", &pks, &vals, false);

    // Writer verdict: packed shard carries Encoding::For on the payload,
    // control stays Raw.
    assert_eq!(
        region_dir(&std::fs::read(&packed_path).unwrap(), REG_PAYLOAD_START).1,
        Encoding::For
    );
    assert_eq!(
        region_dir(&std::fs::read(&raw_path).unwrap(), REG_PAYLOAD_START).1,
        Encoding::Raw
    );

    let packed = MappedShard::open(&packed_path, &schema).unwrap();
    let raw = MappedShard::open(&raw_path, &schema).unwrap();
    assert!(matches!(packed.col_regions[0], PayloadRegion::Packed(_)));
    assert!(matches!(raw.col_regions[0], PayloadRegion::Mapped(_)));

    // Surface 1 (get_col_ptr): per-row equality vs the Raw control and vs
    // the source values.
    for (r, &want) in vals.iter().enumerate() {
        assert_eq!(
            packed.get_col_ptr(r, 0, 8),
            raw.get_col_ptr(r, 0, 8),
            "get_col_ptr row {r}"
        );
        let pv = read_i64_le(packed.get_col_ptr(r, 0, 8), 0);
        assert_eq!(pv, want, "decoded value row {r}");
    }

    // Surface 3 (slice_to_owned_batch / to_owned_batch): byte-identical
    // payload region against the control.
    let pb = packed.slice_to_owned_batch(0, packed.count, &schema);
    let rb = raw.slice_to_owned_batch(0, raw.count, &schema);
    let pbytes = pb.region_at(REG_PAYLOAD_START);
    let rbytes = rb.region_at(REG_PAYLOAD_START);
    assert_eq!(pbytes.len(), rbytes.len());
    assert_eq!(pbytes, rbytes, "whole-shard slice payload region byte-identical");
    // A mid-shard slice reads the decoded image from `start`, not from row 0.
    let (start, len) = (137, 211);
    assert_slices_match(&packed, &raw, start, len, &schema);

    // Surface 4 (to_unified): read the payload ColPtr per row.
    let mut cols = Vec::new();
    let pu = packed.to_unified(&schema, &mut cols);
    for (r, &want) in vals.iter().enumerate() {
        let cp = cols[pu.cols_off];
        let v = read_i64_le(unsafe { cp.row(r, 8) }, 0);
        assert_eq!(v, want, "to_unified row {r}");
    }
}

/// `a.slice(start, len)` and `b.slice(start, len)` hold the same PK, weight,
/// null and payload bytes.
fn assert_slices_match(a: &MappedShard, b: &MappedShard, start: usize, len: usize, schema: &SchemaDescriptor) {
    let (sa, sb) = (
        a.slice_to_owned_batch(start, len, schema),
        b.slice_to_owned_batch(start, len, schema),
    );
    assert_eq!(sa.count, len);
    for region in [REG_PK, REG_WEIGHT, REG_NULL_BMP, REG_PAYLOAD_START] {
        assert_eq!(
            sa.region_at(region),
            sb.region_at(region),
            "slice [{start}, +{len}) region {region}"
        );
    }
}

#[test]
fn packed_i32_mid_shard_slice() {
    // The 4-byte decode arm, read through a mid-shard slice.
    let dir = tempfile::tempdir().unwrap();
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
        ],
        &[0],
    );
    let n = 400usize;
    let write = |name: &str, pack: bool| {
        let mut b = Batch::with_capacity(&schema, n);
        for i in 0..n {
            b.begin_row(&(i as u64).to_be_bytes(), 1);
            b.extend_col(0, &(-70_000i32 + (i % 250) as i32).to_le_bytes());
            b.commit_row(0);
        }
        let shard_path = dir.path().join(name).to_str().unwrap().to_owned();
        b.write_as_shard(&shard_path, ShardWriteOpts { pack_ints: pack, ..Default::default() })
            .unwrap();
        MappedShard::open(&shard_path, &schema).unwrap()
    };
    let packed = write("packed32.db", true);
    let raw = write("raw32.db", false);
    let PayloadRegion::Packed(region) = &packed.col_regions[0] else {
        panic!("the payload must pack")
    };
    assert!(matches!(raw.col_regions[0], PayloadRegion::Mapped(_)));
    let slices_match = || {
        assert_slices_match(&packed, &raw, 0, n, &schema);
        assert_slices_match(&packed, &raw, 91, 233, &schema);
    };
    slices_match();
    assert!(region.decoded.get().is_none(), "a slice decodes only its window");
    for r in 0..n {
        assert_eq!(packed.get_col_ptr(r, 0, 4), raw.get_col_ptr(r, 0, 4), "row {r}");
    }
    assert!(region.decoded.get().is_some());
    slices_match();
}

/// A mid-shard, odd-length slice over constant weight and null regions and a
/// column the file predates: the doubling fill's last step is partial, and
/// every cell must still match the per-row accessors.
#[test]
fn slice_constant_regions_mid_shard_odd_length() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=40).map(|i| (i, i as i64 * 7)).collect();
    let shard = open_widened(dir.path(), "const_slice.db", &rows, TypeCode::I64);
    let schema = schema_with_appended(TypeCode::I64);
    assert!(
        matches!(shard.weight, WeightRegion::Mapped(cp) if cp.stride == 0),
        "weight is Constant"
    );
    assert_eq!(shard.null_bmp.stride, 0, "null bitmap is Constant");

    let (start, len) = (5, 23);
    let batch = shard.slice_to_owned_batch(start, len, &schema);
    assert_eq!(batch.count, len);
    for i in 0..len {
        let r = start + i;
        assert_eq!(batch.get_pk_bytes(i), shard.get_pk_bytes(r), "pk row {i}");
        assert_eq!(batch.get_weight(i), shard.get_weight(r), "weight row {i}");
        assert_eq!(batch.get_null_word(i), shard.get_null_word(r), "null row {i}");
        for pi in 0..2 {
            assert_eq!(
                batch.get_col_ptr(i, pi, 8),
                shard.get_col_ptr(r, pi, 8),
                "col {pi} row {i}"
            );
        }
    }
}

#[test]
fn packed_bytes_stable() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let pks: Vec<u64> = (0..300).collect();
    let vals: Vec<i64> = (0..300).map(|i| 500 + (i % 100)).collect();
    let path = build_i64_shard(dir.path(), "stable.db", &pks, &vals, true);
    let shard = MappedShard::open(&path, &schema).unwrap();
    assert!(matches!(shard.col_regions[0], PayloadRegion::Packed(_)));

    let p1 = shard.get_col_ptr(0, 0, 8).as_ptr();
    let p2 = shard.get_col_ptr(0, 0, 8).as_ptr();
    assert_eq!(p1, p2, "packed_bytes address stable across calls");
}

#[test]
fn forged_for_payload_bad_size_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let pks: Vec<u64> = (0..16).collect();
    let vals: Vec<i64> = (0..16).map(|i| 2000 + (i % 5)).collect();
    let path = build_i64_shard(dir.path(), "packed16.db", &pks, &vals, true);
    // Confirm it packed.
    assert_eq!(
        region_dir(&std::fs::read(&path).unwrap(), REG_PAYLOAD_START).1,
        Encoding::For
    );
    let base = std::fs::read(&path).unwrap();
    let schema = make_schema_u64_i64();
    let (sz, _) = region_dir(&base, REG_PAYLOAD_START);

    // (a) size < 8: patch the payload entry size to 4.
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |data| patch_entry(
            data,
            REG_PAYLOAD_START,
            |e| e.size = 4
        ))
        .err(),
        Some(StorageError::Corrupt("FoR width")),
        "size < 8 must be rejected",
    );
    // (b) size not an exact 8 + count·bw: bump by one non-multiple byte.
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |data| {
            patch_entry(data, REG_PAYLOAD_START, |e| e.size = sz + 1)
        })
        .err(),
        Some(StorageError::Corrupt("FoR width")),
        "inexact 8 + count·bw size must be rejected",
    );
}

#[test]
fn checksum_catches_corrupted_packed_region() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let pks: Vec<u64> = (0..200).collect();
    let vals: Vec<i64> = (0..200).map(|i| 7000 + (i % 120)).collect();
    let path = build_i64_shard(dir.path(), "corrupt.db", &pks, &vals, true);
    let (_sz, enc) = region_dir(&std::fs::read(&path).unwrap(), REG_PAYLOAD_START);
    assert_eq!(enc, Encoding::For);

    // Flip a byte in the packed payload region's on-disk offset bytes.
    let mut data = std::fs::read(&path).unwrap();
    let roff = region_offset(&data, REG_PAYLOAD_START);
    data[roff + 16] ^= 0xFF; // past the 8-byte ref, into the offset bytes
    std::fs::write(&path, &data).unwrap();
    assert_eq!(
        MappedShard::open(&path, &schema).unwrap().verify_body(),
        Err(StorageError::Corrupt("body checksum")),
        "corrupted packed region caught by verify_body",
    );
}

// --- slice blob relocation ---

/// Build a `(U64 PK | STRING payload)` shard from `(pk, cell)` rows, where
/// each cell is an already-encoded German string over the shared `blob`.
/// Passing the cells in lets a caller give two rows the *same* heap span,
/// which a per-row `encode_german_string` never does (it appends
/// unconditionally). Rows must be PK-ascending.
fn write_string_shard(dir: &std::path::Path, name: &str, rows: &[(u64, [u8; 16])], blob: &[u8]) -> String {
    use crate::storage::Batch;
    let schema = make_schema_pk_u64_payload_string();
    let mut batch = Batch::with_capacity(&schema, rows.len().max(1));
    batch.blob.extend_from_slice(blob);
    for &(pk, cell) in rows {
        batch.extend_pk(pk as u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &cell);
        batch.count += 1;
    }
    let path = dir.join(name).to_str().unwrap().to_string();
    batch.write_as_shard(&path, ShardWriteOpts::default()).unwrap();
    path
}

/// Open a `(U64 PK | STRING payload)` shard written by [`write_string_shard`].
fn open_string_shard(path: &str) -> MappedShard {
    let schema = make_schema_pk_u64_payload_string();
    MappedShard::open(path, &schema).unwrap()
}

/// One distinct heap string per row — the blob-region shape the fixed-size
/// relations cannot reach, since a blob length is genuinely non-derivable.
fn build_string_shard(dir: &std::path::Path, name: &str, n: usize, width: usize) -> String {
    let mut blob = Vec::new();
    let rows: Vec<(u64, [u8; 16])> = (0..n)
        .map(|i| {
            (
                i as u64 + 1,
                gnitz_wire::encode_german_string(&wide_string(i, width), &mut blob),
            )
        })
        .collect();
    write_string_shard(dir, name, &rows, &blob)
}

/// A `width`-byte string unique to row `i`, long enough to spill to the heap.
fn wide_string(i: usize, width: usize) -> Vec<u8> {
    let mut s = format!("row-{i:08}-").into_bytes();
    s.resize(width, b'x');
    s
}

/// The blob arm, both ways on one shard. 128 rows of 64-byte strings, of which
/// rows 0 and 1 share a span, is a 8128-byte heap at 63 bytes/row, so
/// `should_relocate_blob` cuts over at 15 sliced rows. Below it the slice
/// carries only its own rows' bytes — once per *distinct* span, since the
/// relocation dedup cache keys on `(src_blob, offset, length)`. At and past it,
/// and for the whole shard, it copies the region verbatim. Every arm decodes
/// back to the original strings.
#[test]
fn slice_relocates_only_its_own_strings() {
    let dir = tempfile::tempdir().unwrap();
    const N: usize = 128;
    const W: usize = 64;
    const CUT: usize = 14;
    let mut blob = Vec::new();
    // Row 1 reuses row 0's cell verbatim, so the two share one heap span — which
    // a per-row `encode_german_string` never produces (it always appends).
    let shared = gnitz_wire::encode_german_string(&wide_string(0, W), &mut blob);
    let mut rows: Vec<(u64, [u8; 16])> = vec![(1, shared), (2, shared)];
    rows.extend((2..N).map(|i| {
        (
            i as u64 + 1,
            gnitz_wire::encode_german_string(&wide_string(i, W), &mut blob),
        )
    }));
    let schema = make_schema_pk_u64_payload_string();
    let shard = open_string_shard(&write_string_shard(dir.path(), "reloc.db", &rows, &blob));
    assert_eq!(shard.blob_len, (N - 1) * W, "row 1 added no bytes");

    let one = shard.slice_to_owned_batch(37, 1, &schema);
    assert_eq!(one.blob.len(), W, "a one-row slice carries one string");
    assert_eq!(read_german_string(&one, 0, 0), wide_string(37, W));

    let under = shard.slice_to_owned_batch(0, CUT, &schema);
    assert_eq!(
        under.blob.len(),
        (CUT - 1) * W,
        "relocates, and rows 0/1 share one span"
    );
    // Rows 0 and 1 both resolve to row 0's string, through the one copied span.
    assert_eq!(read_german_string(&under, 0, 0), wide_string(0, W));
    assert_eq!(read_german_string(&under, 0, 1), wide_string(0, W));

    let at = shard.slice_to_owned_batch(0, CUT + 1, &schema);
    assert_eq!(at.blob.len(), (N - 1) * W, "at the cut the whole region is copied");
    let full = shard.slice_to_owned_batch(0, N, &schema);
    assert_eq!(full.blob.as_slice(), shard.blob(), "whole shard: verbatim");

    for i in 2..CUT {
        assert_eq!(read_german_string(&under, 0, i), wide_string(i, W), "relocated row {i}");
        assert_eq!(read_german_string(&at, 0, i), wide_string(i, W), "whole-region row {i}");
    }
}

/// The measurement `RELOCATE_CELL_COST_BYTES` is set from: whole-region memcpy
/// against per-cell relocation on the *same* slice, swept over slice fraction ×
/// string width.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn slice_blob_relocate_bench() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    const N: usize = 20_000;
    const ITERS: usize = 50;

    let time_arm = |shard: &MappedShard, rc: usize, relocate: bool| -> f64 {
        // One untimed pass faults in the cold mmap pages.
        std::hint::black_box(shard.slice_to_owned_batch_with(0, rc, &schema, relocate));
        let t = Instant::now();
        for _ in 0..ITERS {
            std::hint::black_box(shard.slice_to_owned_batch_with(0, rc, &schema, relocate));
        }
        t.elapsed().as_secs_f64() * 1e9 / ITERS as f64
    };

    for &w in &[16usize, 40, 256, 1024] {
        let mut blob = Vec::new();
        let rows: Vec<(u64, [u8; 16])> = (0..N)
            .map(|i| {
                (
                    i as u64 + 1,
                    gnitz_wire::encode_german_string(&wide_string(i, w), &mut blob),
                )
            })
            .collect();
        let shard = open_string_shard(&write_string_shard(dir.path(), &format!("bench_{w}.db"), &rows, &blob));
        for &pct in &[
            1usize, 2, 3, 4, 6, 8, 12, 16, 20, 25, 33, 40, 50, 60, 68, 75, 85, 90, 99,
        ] {
            let rc = (N * pct / 100).max(1);
            let reloc = time_arm(&shard, rc, true);
            let copy = time_arm(&shard, rc, false);
            let picks = if super::super::merge::should_relocate_blob(shard.blob_len, shard.count, rc) {
                "relocate"
            } else {
                "memcpy  "
            };
            println!(
                "width={w:>5} slice={pct:>3}% picks {picks}: \
                 relocate {reloc:9.0} ns  memcpy {copy:9.0} ns  speedup {:5.2}x",
                copy / reloc,
            );
        }
    }
}

// -----------------------------------------------------------------------
// Descriptive-prefix and filter integrity
// -----------------------------------------------------------------------

#[test]
fn directory_sizes_that_do_not_tile_the_file_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let base = std::fs::read(&path).unwrap();
    let filter = num_regions(schema.num_payload_cols());
    let (fsz, _) = region_dir(&base, filter);
    assert!(fsz > 0);

    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| patch_entry(d, filter, |e| e.size = fsz - 1)).err(),
        Some(StorageError::Corrupt("directory does not span the file")),
        "last entry ends short of the file",
    );
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| patch_entry(d, filter, |e| e.size = fsz + 1)).err(),
        Some(StorageError::Corrupt("region past the end")),
        "last entry ends past the file",
    );
    // An intact prefix over a file cut short of its body.
    assert_eq!(
        open_patched(&path, &schema, &base, |d| d.truncate(d.len() - 1)).err(),
        Some(StorageError::Corrupt("region past the end")),
        "truncated body",
    );
}

#[test]
fn truncated_directory_is_reported_as_such() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=4).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let base = std::fs::read(&path).unwrap();
    assert_eq!(
        open_patched(&path, &schema, &base, |d| d.truncate(dir_entry_off(2))).err(),
        Some(StorageError::Corrupt("shorter than its directory")),
    );
}

#[test]
fn a_corrupt_filter_opens_and_fails_the_body_check() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let base = std::fs::read(&path).unwrap();
    let filter = num_regions(schema.num_payload_cols());
    let xoff = region_offset(&base, filter);
    let (xsz, _) = region_dir(&base, filter);
    assert!(xsz > xorf::Descriptor::DMA_LEN);

    for (name, at) in [("seed", xoff + 4), ("fingerprint", xoff + xsz - 1)] {
        let shard = open_patched(&path, &schema, &base, |d| d[at] ^= 0x01).unwrap();
        assert!(shard.has_shard_filter(), "{name} flip");
        assert_eq!(
            shard.verify_body(),
            Err(StorageError::Corrupt("body checksum")),
            "{name} flip",
        );
    }
}

/// The patch is in the body, so no digest stands in front of the parse.
#[test]
fn a_structurally_invalid_filter_fails_the_open() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let base = std::fs::read(&path).unwrap();
    let xoff = region_offset(&base, num_regions(schema.num_payload_cols()));

    assert_eq!(
        open_patched(&path, &schema, &base, |d| {
            // `segment_length_mask` (descriptor bytes 12..16) no longer
            // agrees with `segment_length`.
            d[xoff + 12] ^= 0x01;
        })
        .err(),
        Some(StorageError::Corrupt("filter descriptor")),
    );
}

#[test]
fn a_filterless_shard_has_an_empty_trailing_entry() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = dir.path().join("nofilter.db");
    let rows = vec![(1u64.to_be_bytes().to_vec(), 1, 7)];
    let shard_path = super::super::shard_file::write_test_shard(
        &path,
        &schema,
        &rows,
        ShardWriteOpts {
            skip_pk_filter: true,
            ..Default::default()
        },
    );
    let image = std::fs::read(&path).unwrap();
    assert_eq!(
        region_dir(&image, num_regions(schema.num_payload_cols())),
        (0, Encoding::Raw)
    );
    let shard = MappedShard::open(&shard_path, &schema).unwrap();
    assert!(!shard.has_shard_filter());
    assert_eq!(shard.verify_body(), Ok(()));
}

/// The digest is seeded with the basename and nothing else: a shard renamed
/// out from under the manifest fails to open, while one hard-linked into
/// another directory under the same name still opens. The second half is
/// what the replicated relayout relies on when it links a sibling child.
#[test]
fn the_digest_seed_separates_names_not_directories() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=4).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let open = |p: &std::path::Path| MappedShard::open(p.to_str().unwrap(), &schema);

    let sibling = dir.path().join("child");
    std::fs::create_dir(&sibling).unwrap();
    let linked = sibling.join("test.db");
    std::fs::hard_link(&path, &linked).unwrap();
    assert_eq!(open(&linked).unwrap().count, 4, "same name, another directory");

    let moved = dir.path().join("renamed.db");
    std::fs::rename(&path, &moved).unwrap();
    assert_eq!(
        open(&moved).err(),
        Some(StorageError::Corrupt("descriptor digest")),
        "new name"
    );
}

/// The shard shapes the sweep below runs over: one per *region count*, since
/// that is what sets the digest's span, plus the encodings that vary within
/// one. `(label, path, schema)`.
fn sweep_shapes(dir: &std::path::Path) -> Vec<(&'static str, String, SchemaDescriptor)> {
    let n = 32usize;
    let seq: Vec<u64> = (1..=n as u64).collect();
    // All-PK (no payload column), so 4 regions rather than 5 — the arity the
    // reader derives from the schema and checks the prefix length against.
    let all_pk = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false)], &[0]);
    let pk_only: Vec<_> = seq.iter().map(|&p| (p.to_be_bytes().to_vec(), 1, 0, vec![])).collect();
    let shard_path = dir.join("sw_pkonly.db").to_str().unwrap().to_owned();
    write_i64_shard(&shard_path, &all_pk, &pk_only, &[], ShardWriteOpts::default());

    vec![
        // One row: a repeated PK is not a consolidated run, so that is the only
        // shape whose every region is Constant-encoded.
        (
            "all-constant",
            build_test_shard_weights(dir, "sw_const.db", &[1u64], &[1i64], &[7i64], false),
            make_schema_u64_i64(),
        ),
        // TwoValue weight, and a FoR-packed payload.
        (
            "two-value weight, for-packed payload",
            build_test_shard_weights(
                dir,
                "sw_twoval_for.db",
                &seq,
                &(0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect::<Vec<_>>(),
                &(0..n as i64).map(|i| 5_000_000 + i).collect::<Vec<_>>(),
                true,
            ),
            make_schema_u64_i64(),
        ),
        // A blob region with real content.
        (
            "string payload",
            build_string_shard(dir, "sw_string.db", 24, 48),
            make_schema_pk_u64_payload_string(),
        ),
        ("all-pk (4 regions)", shard_path, all_pk),
    ]
}

/// The verdicts a corruption at `off` inside the prefix may produce: a header
/// field `ShardHeader::read` checks can fail its own check before the digest.
fn prefix_verdicts(off: usize) -> &'static [StorageError] {
    match off {
        o if o < OFF_VERSION => &[StorageError::Corrupt("magic")],
        o if o < OFF_ROW_COUNT => &[StorageError::Corrupt("version")],
        OFF_ROW_COUNT..OFF_DESC_CHECKSUM => &[
            StorageError::Corrupt("no rows"),
            StorageError::Corrupt("descriptor digest"),
        ],
        o if (OFF_FILE_NPC..OFF_FILE_NPC + 8).contains(&o) => &[
            StorageError::Corrupt("payload arity"),
            StorageError::Corrupt("shorter than its directory"),
            StorageError::Corrupt("descriptor digest"),
        ],
        _ => &[StorageError::Corrupt("descriptor digest")],
    }
}

/// Every bit of the descriptive prefix, including the digest field itself,
/// is inside the digest: no single-bit change to it opens.
#[test]
fn every_single_bit_flip_in_the_prefix_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    for (label, path, schema) in sweep_shapes(dir.path()) {
        let base = std::fs::read(&path).unwrap();
        let n_desc = desc_len(schema.num_payload_cols());
        assert!(n_desc <= base.len());
        for off in 0..n_desc {
            for bit in 0..8u32 {
                let got = open_patched(&path, &schema, &base, |d| d[off] ^= 1 << bit).err();
                let want = prefix_verdicts(off);
                assert!(
                    got.is_some_and(|e| want.contains(&e)),
                    "{label}: byte {off} bit {bit}: got {got:?}, want one of {want:?}",
                );
            }
        }
    }
}

/// Every fixed-width region carries exactly what the schema implies — one
/// byte either way is rejected. Re-stamped, so the verdict is the size
/// check's rather than the digest's.
/// A region's size is fully determined by the row count and its encoding, so a
/// directory size that is off by a byte in either direction must be refused —
/// for the Direct roles and for the TwoValue weight bitvec alike.
#[test]
fn a_region_size_that_disagrees_with_the_row_count_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let n = 64usize;

    let direct = build_test_shard(dir.path(), &(1..=10u64).map(|i| (i, i as i64 * 3)).collect::<Vec<_>>());
    // Alternating weights make the writer pick TwoValue for the weight region.
    let two_value = build_test_shard_weights(
        dir.path(),
        "twoval_size.db",
        &(1..=n as u64).collect::<Vec<_>>(),
        &(0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect::<Vec<_>>(),
        &(0..n as i64).collect::<Vec<_>>(),
        false,
    );

    for (what, path, regions) in [
        (
            "direct",
            direct,
            &[REG_PK, REG_WEIGHT, REG_NULL_BMP, REG_PAYLOAD_START][..],
        ),
        ("two-value weight", two_value, &[REG_WEIGHT][..]),
    ] {
        let base = std::fs::read(&path).unwrap();
        for &region in regions {
            let (sz, _) = region_dir(&base, region);
            for delta in [-1isize, 1] {
                let forged = sz.checked_add_signed(delta).unwrap();
                assert_eq!(
                    open_patched_restamped(&path, &schema, &base, |d| patch_entry(d, region, |e| e.size = forged))
                        .err(),
                    Some(StorageError::Corrupt("region size")),
                    "{what}: region {region} size {sz}{delta:+}",
                );
            }
        }
    }
}

/// Per-pass cost of slicing a FoR shard window by window.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_slice_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    const WINDOW: usize = 1024;
    const HANDLES: usize = 20;
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(Vec<u8>, i64, i64)> = (0..N)
        .map(|i| ((i as u64).to_be_bytes().to_vec(), 1, 3_000_000_000 + (i % 4000) as i64))
        .collect();
    let path = super::super::shard_file::write_test_shard(
        &dir.path().join("for_slice.db"),
        &schema,
        &rows,
        ShardWriteOpts::COMPACTION,
    );
    let open = || MappedShard::open(&path, &schema).unwrap();
    let decoded_bytes = |shard: &MappedShard| -> usize {
        match &shard.col_regions[0] {
            PayloadRegion::Packed(p) => p.decoded.get().map_or(0, |d| d.len()),
            PayloadRegion::Mapped(_) => panic!("the payload must pack"),
        }
    };
    let slice_all = |shard: &MappedShard| {
        for start in (0..N).step_by(WINDOW) {
            black_box(shard.slice_to_owned_batch(start, WINDOW.min(N - start), &schema));
        }
    };
    let (cycles, instructions) = (Counter::cycles().unwrap(), Counter::instructions().unwrap());
    let report = |label: &str, handles: &[MappedShard]| {
        let (((), i), c) = cycles.measure(|| instructions.measure(|| handles.iter().for_each(slice_all)));
        let decoded: usize = handles.iter().map(decoded_bytes).sum();
        println!(
            "{label}: {} cycles, {} instructions per pass; {decoded} decoded bytes retained",
            c / handles.len() as u64,
            i / handles.len() as u64,
        );
    };

    let fresh: Vec<MappedShard> = (0..HANDLES).map(|_| open()).collect();
    report("undecoded", &fresh);
    report("undecoded, second pass", &fresh);
    let cached: Vec<MappedShard> = (0..HANDLES)
        .map(|_| {
            let shard = open();
            black_box(shard.to_unified(&schema, &mut Vec::new()));
            shard
        })
        .collect();
    report("decoded", &cached);
}
