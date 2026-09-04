use super::super::batch::{strides_from_schema, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::super::error::StorageError;
use super::super::layout::*;
use super::super::shard_file::{region_dir, ShardWriteOpts};
use super::*;
use crate::foundation::xxh;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{make_schema_pk_u64_payload_string, make_schema_u64_i64, read_german_string};
use gnitz_wire::as_le_bytes;
use gnitz_wire::{read_i64_le, read_u64_le, write_u64_le};

/// Build a shard via write_shard_streaming (uses encoding detection).
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

/// Region count under `schema`: the fixed roles plus the trailing blob.
fn num_regions(schema: &SchemaDescriptor) -> usize {
    strides_from_schema(schema).1 as usize + 1
}

/// Patch a copy of `base` — the image already written at `path` — write it
/// back to `path`, and open it. The descriptive digest is left stale, so a
/// patch inside the prefix is rejected by the digest. Writing back to the
/// same path keeps the basename, and with it the digest's seed, unchanged;
/// under a fresh name every case would fail on the name alone.
///
/// Opens with checksum validation off. Both unconditional digests run ahead
/// of that gate, so it cannot change any verdict here — only mask one, by
/// tripping on a per-region checksum left stale by the patch.
fn open_patched(
    path: &str,
    schema: &SchemaDescriptor,
    base: &[u8],
    patch: impl FnOnce(&mut Vec<u8>),
) -> Result<MappedShard, StorageError> {
    let mut data = base.to_vec();
    patch(&mut data);
    std::fs::write(path, &data).unwrap();
    MappedShard::open(&std::ffi::CString::new(path).unwrap(), schema, false)
}

/// As [`open_patched`], but re-stamp the descriptive digest after the patch,
/// so the forgery reaches the structural check under test.
fn open_patched_restamped(
    path: &str,
    schema: &SchemaDescriptor,
    base: &[u8],
    patch: impl FnOnce(&mut Vec<u8>),
) -> Result<MappedShard, StorageError> {
    open_patched(path, schema, base, |data| {
        patch(data);
        let cpath = std::ffi::CString::new(path).unwrap();
        let cs = desc_digest(shard_basename(cpath.to_bytes()), data, num_regions(schema));
        write_u64_le(data, OFF_DESC_CHECKSUM, cs);
    })
}

#[test]
fn open_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();

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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();

    // PK column holds OPK (big-endian) bytes at rest.
    assert_eq!(u64::from_be_bytes(shard.get_pk_bytes(0).try_into().unwrap()), 1);
    assert_eq!(read_i64_le(shard.get_col_ptr(0, 0, 8), 0), 42);
    assert_eq!(read_i64_le(shard.get_col_ptr(1, 0, 8), 0), 84);
}

// --- ALTER TABLE ADD COLUMN: reading pre-ALTER bytes -------------------

/// `(U64 PK, I64, <tail>)` where `tail` is nullable — the shape an
/// `ADD COLUMN` leaves behind. `make_schema_u64_i64` is its narrow twin.
fn schema_with_appended(tail: u8) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
            SchemaColumn::new(tail, 1),
        ],
        &[0],
    )
}

/// Open a narrow shard (one I64 payload column) under a schema that has
/// since grown a trailing nullable column.
fn open_widened(dir: &std::path::Path, name: &str, rows: &[(u64, i64)], tail: u8) -> MappedShard {
    let pks: Vec<u64> = rows.iter().map(|&(pk, _)| pk).collect();
    let wts: Vec<i64> = rows.iter().map(|_| 1i64).collect();
    let vals: Vec<i64> = rows.iter().map(|&(_, v)| v).collect();
    let path = build_test_shard_weights(dir, name, &pks, &wts, &vals, false);
    let cpath = std::ffi::CString::new(path).unwrap();
    MappedShard::open(&cpath, &schema_with_appended(tail), false).unwrap()
}

#[test]
fn file_npc_header_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = build_test_shard(dir.path(), &[(1u64, 10i64)]);
    let data = std::fs::read(&path).unwrap();
    // The writer stamps its own descriptor's payload arity.
    assert_eq!(read_u64_le(&data, OFF_FILE_NPC), 1);

    // A shard read back at the width it was written pays nothing.
    let schema = make_schema_u64_i64();
    let shard = MappedShard::open(&std::ffi::CString::new(path).unwrap(), &schema, false).unwrap();
    assert_eq!(shard.null_pad_mask, 0);
    assert_eq!(shard.col_regions.len(), 1);
}

#[test]
fn padded_shard_pads_the_appended_column() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = (1..=5).map(|i| (i, i as i64 * 100)).collect();
    let shard = open_widened(dir.path(), "pad.db", &rows, type_code::I64);

    // The walk is driven by the file's own arity: one mapped column, and the
    // appended one is `Absent` rather than a mis-read directory entry.
    assert_eq!(shard.col_regions.len(), 2);
    assert!(matches!(shard.col_regions[1], PayloadRegion::Absent));
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
    let shard = open_widened(dir.path(), "pad_slice.db", &rows, type_code::I64);
    let schema = schema_with_appended(type_code::I64);

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
    let shard = open_widened(dir.path(), "pad_unified.db", &rows, type_code::I64);
    let schema = schema_with_appended(type_code::I64);

    // Reader 3: the shared column-first scatter reads `null_pad_mask` off the
    // view rather than the shard, so the view must carry it.
    let mut cols = Vec::new();
    let unified = shard.to_unified(&schema, &mut cols);
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
    let shard = open_widened(dir.path(), "pad_str.db", &rows, type_code::STRING);
    let schema = schema_with_appended(type_code::STRING);

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
    let wide = schema_with_appended(type_code::I64);
    let pk_bytes: Vec<u8> = (1u64..=3).flat_map(|p| p.to_be_bytes()).collect();
    let wts: Vec<i64> = vec![1; 3];
    let null_bm: Vec<u64> = vec![0; 3];
    let col0: Vec<i64> = vec![10, 20, 30];
    let col1: Vec<i64> = vec![11, 22, 33];
    let blob: Vec<u8> = Vec::new();
    let regions: Vec<&[u8]> = vec![
        &pk_bytes,
        as_le_bytes(&wts),
        as_le_bytes(&null_bm),
        as_le_bytes(&col0),
        as_le_bytes(&col1),
        &blob,
    ];
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    super::super::shard_file::write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        3,
        &regions,
        &wide,
        ShardWriteOpts::default(),
    )
    .unwrap();

    let narrow = make_schema_u64_i64();
    let shard = MappedShard::open(&cpath, &narrow, false).unwrap();
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

    // Out of range: rejected before any arithmetic on it, because `desc_len`
    // would overflow on a forged u64.
    assert_eq!(
        open_patched(&path, &schema, &base, |d| write_u64_le(d, OFF_FILE_NPC, u64::MAX)).err(),
        Some(StorageError::InvalidShard)
    );
    assert_eq!(
        open_patched(&path, &schema, &base, |d| write_u64_le(d, OFF_FILE_NPC, 66)).err(),
        Some(StorageError::InvalidShard)
    );

    // In range but wrong: it moves the digest span *and* sits inside it, so it
    // fails the digest like any other forged descriptive byte. (The restamp
    // helper re-stamps over the reader schema's span, which is what the file
    // truthfully has.)
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| write_u64_le(d, OFF_FILE_NPC, 2)).err(),
        Some(StorageError::ChecksumMismatch)
    );
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
        Some(StorageError::InvalidVersion)
    );
}

#[test]
fn checksum_validation() {
    let dir = tempfile::tempdir().unwrap();
    let rows = vec![(1u64, 10i64)];
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true);
    assert!(shard.is_ok());

    let path_str = cpath.to_str().unwrap();
    let mut data = std::fs::read(path_str).unwrap();
    let pk_lo_off = read_u64_le(&data, HEADER_SIZE) as usize;
    data[pk_lo_off] ^= 0xFF;
    std::fs::write(path_str, &data).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true);
    assert_eq!(shard.err(), Some(StorageError::ChecksumMismatch));
}

#[test]
fn empty_shard() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = vec![];
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();
    assert_eq!(shard.count, 0);
    assert_eq!(shard.find_lower_bound_bytes(&1u64.to_be_bytes()), 0);
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
    let cpath = std::ffi::CString::new(path.clone()).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
    assert_eq!(shard.count, n as usize);
    for i in 0..n as usize {
        assert_eq!(shard.get_weight(i), 1);
    }

    // One weight value across every row must be stored as a single 8-byte
    // Constant region rather than n*8 raw bytes.
    let image = std::fs::read(&path).unwrap();
    assert_eq!(region_dir(&image, REG_WEIGHT), (8, ENCODING_CONSTANT));
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
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
    let blob = num_regions(&schema) - 1;

    // (region, forged encoding byte)
    let cases: &[(usize, u8)] = &[
        (REG_PK, 0x10), // not an encoding at all
        (REG_PK, ENCODING_TWO_VALUE),
        (REG_PK, ENCODING_FOR),
        (REG_WEIGHT, ENCODING_FOR),
        (REG_NULL_BMP, ENCODING_FOR),
        (blob, ENCODING_FOR),
        (REG_PAYLOAD_START, ENCODING_TWO_VALUE),
        (blob, ENCODING_CONSTANT),
    ];
    for &(region, enc) in cases {
        let opened = open_patched_restamped(&path, &schema, &base, |data| {
            data[dir_entry_off(region) + 24] = enc;
        });
        assert_eq!(
            opened.err(),
            Some(StorageError::InvalidShard),
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();

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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();
    let batch = shard.slice_to_owned_batch(0, 0, &schema);
    assert_eq!(batch.count, 0);
}

#[test]
fn u64_pk_open_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(u64, i64)> = vec![(10, 100), (20, 200), (30, 300)];
    let path = build_test_shard(dir.path(), &rows);
    let schema = make_schema_u64_i64();
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
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
    let cpath = std::ffi::CString::new(path).unwrap();

    let shard = MappedShard::open(&cpath, &schema, false).unwrap();
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
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

fn build_test_shard_u128(dir: &std::path::Path, name: &str, pks: &[u128], vals: &[i64]) -> String {
    let path = dir.join(name);
    let count = pks.len() as u32;
    // PK region holds OPK (order-preserving big-endian) bytes at rest.
    let pk_bytes: Vec<u8> = pks.iter().flat_map(|&p| p.to_be_bytes()).collect();
    let weights: Vec<i64> = vec![1; pks.len()];
    let null_bm: Vec<u64> = vec![0; pks.len()];
    let blob: Vec<u8> = Vec::new();

    let regions: Vec<&[u8]> = vec![
        &pk_bytes,
        as_le_bytes(&weights),
        as_le_bytes(&null_bm),
        as_le_bytes(vals),
        &blob,
    ];

    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    super::super::shard_file::write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        count,
        &regions,
        &u128_pk_schema(),
        ShardWriteOpts::default(),
    )
    .unwrap();
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
        let cpath = std::ffi::CString::new(path).unwrap();
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();

        assert_eq!(shard.pk.is_per_row(), per_row, "{what}: region shape");
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
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
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
    let count = pks.len() as u32;
    let pk_bytes: Vec<u8> = pks.iter().flat_map(|r| r.iter().copied()).collect();
    let weights: Vec<i64> = vec![1; count as usize];
    let null_bm: Vec<u64> = vec![0; count as usize];
    let empty: Vec<u8> = Vec::new();

    // 4 regions: pk, weight, null_bmp, blob (num_payload_cols = 0).
    let regions: Vec<&[u8]> = vec![&pk_bytes, as_le_bytes(&weights), as_le_bytes(&null_bm), &empty];
    let path = dir.path().join("wide_pk.db");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    super::super::shard_file::write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        count,
        &regions,
        &schema,
        ShardWriteOpts::default(),
    )
    .unwrap();
    let shard = MappedShard::open(&cpath, &schema, false).unwrap();
    assert_eq!(shard.pk_stride, 24);
    assert!(shard.pk.is_per_row(), "distinct PKs must keep the PK region per-row");

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
// FoR (ENCODING_FOR) packed-payload reader tests
// -----------------------------------------------------------------------

/// Build a `(U64 PK | I64 payload)` shard with all-1 weights;
/// `pack` toggles FoR on the payload region.
fn build_i64_shard(dir: &std::path::Path, name: &str, pks: &[u64], vals: &[i64], pack: bool) -> String {
    build_test_shard_weights(dir, name, pks, &vec![1i64; pks.len()], vals, pack)
}

/// The directory entry's `(size, encoding)` for an on-disk shard.
fn payload_dir_entry(path: &str, region_idx: usize) -> (usize, u8) {
    region_dir(&std::fs::read(path).unwrap(), region_idx)
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

    // Writer verdict: packed shard carries ENCODING_FOR on the payload,
    // control stays Raw.
    assert_eq!(payload_dir_entry(&packed_path, REG_PAYLOAD_START).1, ENCODING_FOR);
    assert_eq!(payload_dir_entry(&raw_path, REG_PAYLOAD_START).1, ENCODING_RAW);

    let pc = std::ffi::CString::new(packed_path).unwrap();
    let rc = std::ffi::CString::new(raw_path).unwrap();
    let packed = MappedShard::open(&pc, &schema, true).unwrap();
    let raw = MappedShard::open(&rc, &schema, true).unwrap();
    assert!(matches!(packed.col_regions[0], PayloadRegion::Packed(_)));
    assert!(matches!(raw.col_regions[0], PayloadRegion::Direct(_)));

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
    let pbytes = pb.region_or_blob(REG_PAYLOAD_START);
    let rbytes = rb.region_or_blob(REG_PAYLOAD_START);
    assert_eq!(pbytes.len(), rbytes.len());
    assert_eq!(pbytes, rbytes, "whole-shard slice payload region byte-identical");

    // Surface 4 (to_unified): read the payload ColPtr per row.
    let mut cols = Vec::new();
    let pu = packed.to_unified(&schema, &mut cols);
    for (r, &want) in vals.iter().enumerate() {
        let cp = cols[pu.cols_off];
        let v = unsafe { *(cp.base.add(r * cp.stride) as *const i64) };
        assert_eq!(v, want, "to_unified row {r}");
    }
}

#[test]
fn packed_bytes_stable_and_aligned() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let pks: Vec<u64> = (0..300).collect();
    let vals: Vec<i64> = (0..300).map(|i| 500 + (i % 100)).collect();
    let path = build_i64_shard(dir.path(), "stable.db", &pks, &vals, true);
    let cpath = std::ffi::CString::new(path).unwrap();
    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
    assert!(matches!(shard.col_regions[0], PayloadRegion::Packed(_)));

    let p1 = shard.get_col_ptr(0, 0, 8).as_ptr();
    let p2 = shard.get_col_ptr(0, 0, 8).as_ptr();
    assert_eq!(p1, p2, "packed_bytes address stable across calls");
    assert_eq!(p1 as usize % 8, 0, "packed_bytes 8-aligned");
}

#[test]
fn forged_for_payload_bad_size_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let pks: Vec<u64> = (0..16).collect();
    let vals: Vec<i64> = (0..16).map(|i| 2000 + (i % 5)).collect();
    let path = build_i64_shard(dir.path(), "packed16.db", &pks, &vals, true);
    // Confirm it packed.
    assert_eq!(payload_dir_entry(&path, REG_PAYLOAD_START).1, ENCODING_FOR);
    let base = std::fs::read(&path).unwrap();
    let schema = make_schema_u64_i64();
    let d = dir_entry_off(REG_PAYLOAD_START);
    let sz = read_u64_le(&base, d + 8);

    // (a) count == 0: patch the header row count to 0. The fixed regions'
    // sizes go to 0 with it, or the exact-size check rejects the PK region
    // (128 != 0) before the divisor guard this case names is reached.
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |data| {
            write_u64_le(data, OFF_ROW_COUNT, 0);
            for r in [REG_PK, REG_WEIGHT, REG_NULL_BMP] {
                write_u64_le(data, dir_entry_off(r) + 8, 0);
            }
        })
        .err(),
        Some(StorageError::InvalidShard),
        "count == 0 must be rejected",
    );
    // (b) size < 8: patch the payload entry size to 4.
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |data| write_u64_le(data, d + 8, 4)).err(),
        Some(StorageError::InvalidShard),
        "size < 8 must be rejected",
    );
    // (c) size not an exact 8 + count·bw: bump by one non-multiple byte.
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |data| write_u64_le(data, d + 8, sz + 1)).err(),
        Some(StorageError::InvalidShard),
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
    let (_sz, enc) = payload_dir_entry(&path, REG_PAYLOAD_START);
    assert_eq!(enc, ENCODING_FOR);

    // Flip a byte in the packed payload region's on-disk offset bytes.
    let mut data = std::fs::read(&path).unwrap();
    let d = dir_entry_off(REG_PAYLOAD_START);
    let roff = read_u64_le(&data, d) as usize;
    data[roff + 16] ^= 0xFF; // past the 8-byte ref, into the offset bytes
    std::fs::write(&path, &data).unwrap();
    let cpath = std::ffi::CString::new(path).unwrap();
    assert_eq!(
        MappedShard::open(&cpath, &schema, true).err(),
        Some(StorageError::ChecksumMismatch),
        "corrupted packed region caught by validate_checksums",
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
    let mut batch = Batch::with_capacity(schema, rows.len().max(1));
    batch.blob.extend_from_slice(blob);
    for &(pk, cell) in rows {
        batch.extend_pk(pk as u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &cell);
        batch.count += 1;
    }
    let path = dir.join(name).to_str().unwrap().to_string();
    batch
        .write_as_shard(
            &std::ffi::CString::new(path.as_str()).unwrap(),
            &schema,
            ShardWriteOpts::default(),
        )
        .unwrap();
    path
}

/// Open a `(U64 PK | STRING payload)` shard written by [`write_string_shard`].
fn open_string_shard(path: &str) -> MappedShard {
    let schema = make_schema_pk_u64_payload_string();
    MappedShard::open(&std::ffi::CString::new(path).unwrap(), &schema, false).unwrap()
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
    assert_eq!(full.blob.as_slice(), shard.blob_slice(), "whole shard: verbatim");

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

/// The forgeries a byte-wise sweep cannot model: each one *permutes* or
/// *relocates* bytes that are individually unchanged, so it is rejected only
/// because the digest is order- and position-sensitive. Every one of them
/// leaves a self-consistent file that passes every structural check.
#[test]
fn permuted_directory_entries_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    // Distinct PKs (Raw), all-1 weights (Constant), all-0 nulls (Constant),
    // varying payload (Raw).
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let base = std::fs::read(&path).unwrap();
    assert_eq!(region_dir(&base, REG_NULL_BMP), (8, ENCODING_CONSTANT));
    assert_eq!(region_dir(&base, REG_PAYLOAD_START), (80, ENCODING_RAW));

    let pk_e = dir_entry_off(REG_PK);
    let w_e = dir_entry_off(REG_WEIGHT);
    let nb_e = dir_entry_off(REG_NULL_BMP);
    let pay_e = dir_entry_off(REG_PAYLOAD_START);
    let swap = |d: &mut Vec<u8>, a: usize, b: usize| {
        for k in 0..DIR_ENTRY_SIZE {
            d.swap(a + k, b + k);
        }
    };

    // Each entry keeps the size its encoding demands, so every per-role
    // check passes and the payload column collapses to the null bitmap's
    // constant. A per-region checksum cannot catch this: it travels inside
    // the entry and moves with it.
    assert_eq!(
        open_patched(&path, &schema, &base, |d| swap(d, nb_e, pay_e)).err(),
        Some(StorageError::ChecksumMismatch),
        "null_bmp <-> payload entries swapped",
    );
    // weight and null_bmp are both Constant at 8 bytes, so the swapped
    // entries are byte-identical in every field a relation could check.
    assert_eq!(
        open_patched(&path, &schema, &base, |d| swap(d, w_e, nb_e)).err(),
        Some(StorageError::ChecksumMismatch),
        "identical (size, encoding) entries swapped",
    );
    // A region offset pointed at another region: every byte it names is
    // real, and the size still matches the schema.
    assert_eq!(
        open_patched(&path, &schema, &base, |d| {
            let w_off = read_u64_le(d, w_e);
            write_u64_le(d, pk_e, w_off);
        })
        .err(),
        Some(StorageError::ChecksumMismatch),
        "pk offset redirected to the weight region",
    );
}

/// A file truncated inside its directory reports `Truncated`, not a digest
/// mismatch — the prefix-length check runs ahead of the digest.
#[test]
fn truncated_directory_reports_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=4).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let base = std::fs::read(&path).unwrap();
    assert_eq!(
        open_patched(&path, &schema, &base, |d| d.truncate(dir_entry_off(2))).err(),
        Some(StorageError::Truncated),
    );
}

/// The filter's own digest, which lives outside the descriptive prefix. A
/// corrupt filter's descriptor drives `contains`'s fingerprint indexing, so
/// probing one panics; rejecting it is not optional.
#[test]
fn forged_shard_filter_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let base = std::fs::read(&path).unwrap();
    let xoff = read_u64_le(&base, OFF_SHARD_FILTER_OFFSET) as usize;
    let xsz = read_u64_le(&base, OFF_SHARD_FILTER_SIZE) as usize;
    assert!(xoff > 0 && xsz > xorf::Descriptor::DMA_LEN);

    // The patches land outside the prefix, so the stale descriptive digest is
    // irrelevant and the verdict is the filter digest's.
    for (name, at) in [("seed", xoff + 4), ("fingerprint", xoff + xsz - 1)] {
        assert_eq!(
            open_patched(&path, &schema, &base, |d| d[at] ^= 0x01).err(),
            Some(StorageError::ChecksumMismatch),
            "{name} flip",
        );
    }

    // Re-stamping the filter's checksum to match a forged filter is rejected
    // by the descriptive digest — which is why the field lives in the header.
    assert_eq!(
        open_patched(&path, &schema, &base, |d| {
            d[xoff + 4] ^= 0x01;
            let cs = xxh::checksum(&d[xoff..xoff + xsz]);
            write_u64_le(d, OFF_SHARD_FILTER_CHECKSUM, cs);
        })
        .err(),
        Some(StorageError::ChecksumMismatch),
        "a re-stamped filter checksum is inside the descriptive digest",
    );
}

/// A structurally invalid filter region fails the open rather than degrading
/// to filterless — its descriptor drives `contains`'s indexing, and the
/// checksum runs ahead of the parse, so reaching this means a writer bug.
/// Both digests are re-stamped, the filter's own living inside the
/// descriptive one, or the verdict would be `ChecksumMismatch` and the rule
/// under test never reached.
#[test]
fn a_structurally_invalid_filter_fails_the_open() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let base = std::fs::read(&path).unwrap();
    let xoff = read_u64_le(&base, OFF_SHARD_FILTER_OFFSET) as usize;
    let xsz = read_u64_le(&base, OFF_SHARD_FILTER_SIZE) as usize;

    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| {
            // `segment_length_mask` (descriptor bytes 12..16) no longer
            // agrees with `segment_length`.
            d[xoff + 12] ^= 0x01;
            let cs = xxh::checksum(&d[xoff..xoff + xsz]);
            write_u64_le(d, OFF_SHARD_FILTER_CHECKSUM, cs);
        })
        .err(),
        Some(StorageError::InvalidShard),
    );
}

#[test]
fn empty_shard_carries_no_filter_and_a_zero_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = build_test_shard(dir.path(), &[]);
    let image = std::fs::read(&path).unwrap();
    assert_eq!(read_u64_le(&image, OFF_SHARD_FILTER_OFFSET), 0);
    assert_eq!(read_u64_le(&image, OFF_SHARD_FILTER_CHECKSUM), 0);
    let shard = MappedShard::open(&std::ffi::CString::new(path).unwrap(), &schema, true).unwrap();
    assert!(!shard.has_shard_filter());
}

/// The digest is seeded with the basename and nothing else: a shard renamed
/// out from under the manifest fails to open, while one hard-linked into
/// another directory under the same name still opens. The second half is
/// what `link_child` relies on when it seeds a sibling child.
#[test]
fn the_digest_seed_separates_names_not_directories() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64)> = (1..=4).map(|i| (i, i as i64)).collect();
    let path = build_test_shard(dir.path(), &rows);
    let open =
        |p: &std::path::Path| MappedShard::open(&std::ffi::CString::new(p.to_str().unwrap()).unwrap(), &schema, false);

    let sibling = dir.path().join("child");
    std::fs::create_dir(&sibling).unwrap();
    let linked = sibling.join("test.db");
    std::fs::hard_link(&path, &linked).unwrap();
    assert_eq!(open(&linked).unwrap().count, 4, "same name, another directory");

    let moved = dir.path().join("renamed.db");
    std::fs::rename(&path, &moved).unwrap();
    assert_eq!(open(&moved).err(), Some(StorageError::ChecksumMismatch), "new name");
}

/// The shard shapes the sweep below runs over: one per *region count*, since
/// that is what sets the digest's span, plus the encodings that vary within
/// one. `(label, path, schema)`.
fn sweep_shapes(dir: &std::path::Path) -> Vec<(&'static str, String, SchemaDescriptor)> {
    let n = 32usize;
    let seq: Vec<u64> = (1..=n as u64).collect();
    // All-PK (no payload column), so 4 regions rather than 5 — the arity the
    // reader derives from the schema and checks the prefix length against.
    let all_pk = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
    let pk_only: Vec<u8> = seq.iter().flat_map(|&p| p.to_be_bytes()).collect();
    let cpath = std::ffi::CString::new(dir.join("sw_pkonly.db").to_str().unwrap()).unwrap();
    super::super::shard_file::write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        n as u32,
        &[&pk_only, as_le_bytes(&vec![1i64; n]), as_le_bytes(&vec![0u64; n]), &[]],
        &all_pk,
        ShardWriteOpts::default(),
    )
    .unwrap();

    vec![
        // Constant PK / Constant weight / Constant null / Constant payload.
        (
            "all-constant",
            build_test_shard_weights(
                dir,
                "sw_const.db",
                &vec![1u64; n],
                &vec![1i64; n],
                &vec![7i64; n],
                false,
            ),
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
        ("all-pk (4 regions)", cpath.to_str().unwrap().to_string(), all_pk),
    ]
}

/// The verdicts a corruption at `off` inside the prefix may produce. Magic
/// and version are checked ahead of the digest so a wrong-format or
/// wrong-build file names its actual defect. `file_npc` is read ahead of it
/// too — it sizes the digest span itself — so a flip that raises it is
/// caught by the bound or the length check first. Every other byte is the
/// digest's alone.
fn prefix_verdicts(off: usize) -> &'static [StorageError] {
    match off {
        o if o < OFF_VERSION => &[StorageError::InvalidMagic],
        o if o < OFF_ROW_COUNT => &[StorageError::InvalidVersion],
        o if (OFF_FILE_NPC..OFF_FILE_NPC + 8).contains(&o) => &[
            StorageError::InvalidShard,
            StorageError::Truncated,
            StorageError::ChecksumMismatch,
        ],
        _ => &[StorageError::ChecksumMismatch],
    }
}

/// Every bit of the descriptive prefix, including the digest field itself,
/// is inside the digest: no single-bit change to it opens.
#[test]
fn every_single_bit_flip_in_the_prefix_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    for (label, path, schema) in sweep_shapes(dir.path()) {
        let base = std::fs::read(&path).unwrap();
        let n_desc = desc_len(num_regions(&schema));
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
            let e = dir_entry_off(region);
            let sz = read_u64_le(&base, e + 8);
            for delta in [-1i64, 1] {
                let forged = (sz as i64 + delta) as u64;
                assert_eq!(
                    open_patched_restamped(&path, &schema, &base, |d| write_u64_le(d, e + 8, forged)).err(),
                    Some(StorageError::InvalidShard),
                    "{what}: region {region} size {sz}{delta:+}",
                );
            }
        }
    }
}
