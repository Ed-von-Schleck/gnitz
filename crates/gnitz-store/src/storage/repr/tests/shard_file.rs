use super::super::merge::ColumnarSource;
use super::super::shard_reader::MappedShard;
use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::make_schema_u64_i64;
use gnitz_expr::RowSource;
use gnitz_wire::read_i64_le;
use xorf::Filter;

/// `(opk, weight, null_word, payload)` rows over a U64 PK.
type Rows = Vec<(Vec<u8>, i64, u64, Vec<i64>)>;

fn u64_rows(pks: &[u64], weights: &[i64], nulls: &[u64], cols: &[Vec<i64>]) -> Rows {
    (0..pks.len())
        .map(|i| {
            (
                pks[i].to_be_bytes().to_vec(),
                weights[i],
                nulls[i],
                cols.iter().map(|c| c[i]).collect(),
            )
        })
        .collect()
}

/// One shard, written and read back: header fields, every row through the
/// reader, and a PK filter that contains each key.
#[test]
fn write_open_roundtrip() {
    let pks: Vec<u64> = vec![100, 200, 300, 400, 500];
    let vals: Vec<i64> = vec![10, 20, 30, 40, 50];
    let n = pks.len();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("roundtrip.db");
    let shard_path = path.to_str().unwrap().to_owned();
    let schema = make_schema_u64_i64();
    let rows = u64_rows(&pks, &vec![1; n], &vec![0; n], std::slice::from_ref(&vals));
    write_i64_shard(&shard_path, &schema, &rows, &[], ShardWriteOpts::default());

    let image = std::fs::read(&path).unwrap();
    assert_eq!(ShardHeader::read(&image).unwrap().row_count, n);
    // The filter is the trailing directory entry.
    let (filter_size, filter_encoding) = region_dir(&image, gnitz_wire::num_regions(1));
    assert!(filter_size > 0);
    assert_eq!(filter_encoding, Encoding::Raw);

    // `open` itself rejects a bad magic or version, so a successful open is
    // what pins those; the rest is the row data.
    let shard = MappedShard::open(&shard_path, &schema).unwrap();
    assert_eq!(shard.row_count(), n);
    assert!(shard.has_shard_filter());
    for (i, (&pk, &val)) in pks.iter().zip(&vals).enumerate() {
        assert_eq!(shard.get_pk(i), pk as u128, "row {i} pk");
        assert_eq!(shard.get_weight(i), 1, "row {i} weight");
        assert_eq!(read_i64_le(shard.get_col_ptr(i, 0, 8), 0), val, "row {i} payload");
        assert!(
            shard.shard_filter_may_contain(probe_key(&pk.to_be_bytes())),
            "the PK filter must contain PK {pk}"
        );
    }
}

/// No false negatives through the real write → `open` → probe path, at a
/// key count where the construction picks a segment geometry the handful of
/// rows the other shard tests write never reach. The in-module
/// `build_and_query_no_false_negatives` covers the same property in memory;
/// this one is what a descriptor that survives the round trip but is
/// reassembled wrong would fail.
#[test]
fn no_false_negatives_through_write_open_probe() {
    const N: usize = 200_000;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fn.db");
    let shard_path = path.to_str().unwrap().to_owned();

    // Members and non-members from one stream, split by parity of the draw,
    // so neither set is a range the other can be confused with.
    let mut rng = crate::test_rng::Rng::new(0xF11E_5EED);
    let mut members: Vec<u64> = (0..N).map(|_| rng.next_u64()).collect();
    let absent: Vec<u64> = (0..N).map(|_| rng.next_u64()).collect();
    // The PK region is sorted by (PK, payload) — the writer's contract.
    members.sort_unstable();
    members.dedup();
    let n = members.len();

    let vals: Vec<i64> = members.iter().map(|&p| p as i64).collect();
    let schema = make_schema_u64_i64();
    let rows = u64_rows(&members, &vec![1; n], &vec![0; n], &[vals]);
    write_i64_shard(&shard_path, &schema, &rows, &[], ShardWriteOpts::default());
    let shard = MappedShard::open(&shard_path, &schema).unwrap();
    assert!(shard.has_shard_filter());

    for &pk in &members {
        assert!(
            shard.shard_filter_may_contain(probe_key(&pk.to_be_bytes())),
            "false negative for PK {pk}",
        );
    }
    // The filter still discriminates: at ~0.4% nominal, a filter that
    // admitted everything (or was rebuilt against the wrong fingerprints)
    // would blow this bound rather than fail above.
    let fp = absent
        .iter()
        .filter(|&&pk| shard.shard_filter_may_contain(probe_key(&pk.to_be_bytes())))
        .count();
    assert!(fp * 100 < N, "false-positive rate above 1%: {fp}/{N}");
}

#[test]
fn encoding_selection_pins_all_roles() {
    let dir = tempfile::tempdir().unwrap();
    let write_and_read = |schema: &SchemaDescriptor, rows: &Rows, blob: &[u8], name: &str| -> Vec<u8> {
        let path = dir.path().join(name);
        let shard_path = path.to_str().unwrap().to_owned();
        write_i64_shard(&shard_path, schema, rows, blob, ShardWriteOpts::default());
        std::fs::read(&path).unwrap()
    };

    // --- Shard A (Constant-heavy): constant PK, all-1 weight, all-0 nulls,
    // one constant + one varying payload column, non-empty blob. ---
    let schema_a = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false), // PK
            SchemaColumn::new(TypeCode::I64, false), // constant payload
            SchemaColumn::new(TypeCode::I64, false), // varying payload
        ],
        &[0],
    );
    let n_a = 4usize;
    let blob_a: Vec<u8> = vec![0xAA, 0xBB, 0xCC];
    let rows_a = u64_rows(
        &vec![7; n_a],
        &vec![1; n_a],
        &vec![0; n_a],
        &[vec![42; n_a], (0..n_a as i64).collect()],
    );
    let img_a = write_and_read(&schema_a, &rows_a, &blob_a, "pin_a.db");
    assert_eq!(region_dir(&img_a, 0), (8, Encoding::Constant), "A pk constant");
    assert_eq!(region_dir(&img_a, 1), (8, Encoding::Constant), "A weight constant");
    assert_eq!(region_dir(&img_a, 2), (8, Encoding::Constant), "A null constant");
    assert_eq!(region_dir(&img_a, 3), (8, Encoding::Constant), "A payload constant");
    assert_eq!(
        region_dir(&img_a, 4),
        (n_a * 8, Encoding::Raw),
        "A payload varying → raw"
    );
    assert_eq!(region_dir(&img_a, 5), (blob_a.len(), Encoding::Raw), "A blob raw");

    // --- Shard B (TwoValue weight, Raw nulls): distinct PKs, alternating
    // 1/-1 weights, a nullable column NULL on a subset so the null_bmp holds
    // ≥2 distinct null-words. ---
    let schema_b = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true), // nullable
        ],
        &[0],
    );
    let n_b = 4usize;
    let rows_b = u64_rows(
        &(1u64..=n_b as u64).collect::<Vec<_>>(),
        &[1, -1, 1, -1],
        &[1, 0, 1, 0], // col-0 null bit set on rows 0,2
        &[vec![10, 20, 30, 40]],
    );
    let img_b = write_and_read(&schema_b, &rows_b, &[], "pin_b.db");
    assert_eq!(region_dir(&img_b, 0), (n_b * 8, Encoding::Raw), "B pk distinct → raw");
    assert_eq!(
        region_dir(&img_b, 1),
        (two_value_image_len(n_b), Encoding::TwoValue),
        "B weight two-value"
    );
    assert_eq!(region_dir(&img_b, 2), (n_b * 8, Encoding::Raw), "B null mixed → raw");

    // --- Shard C (Raw weight): ≥3 distinct weight values. ---
    let schema_c = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let n_c = 3usize;
    let rows_c = u64_rows(&[1, 2, 3], &[1, -1, 2], &vec![0; n_c], &[vec![5, 6, 7]]);
    let img_c = write_and_read(&schema_c, &rows_c, &[], "pin_c.db");
    assert_eq!(
        region_dir(&img_c, 1),
        (n_c * 8, Encoding::Raw),
        "C weight ≥3 distinct → raw"
    );
}

/// Any change to the written bytes, the writer's own or a dependency's, needs a
/// `SHARD_EPOCH` bump.
#[test]
fn shard_bytes_are_pinned() {
    const PINNED: (u64, u64) = (21, 14026387432709276183);
    let n = 64usize;
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false), // narrow range → FoR
            SchemaColumn::new(TypeCode::I64, false), // full range → Raw
        ],
        &[0],
    );
    let rows = u64_rows(
        &(0..n as u64).map(|i| i * 3 + 1).collect::<Vec<_>>(),
        &(0..n).map(|i| 1 + (i % 2) as i64).collect::<Vec<_>>(),
        &vec![0; n],
        &[
            (0..n as i64).map(|i| 1_000_000 + i * 3).collect(),
            (0..n as i64)
                .map(|i| i.wrapping_mul(0x0123_4567_89AB_CDEF) ^ (i << 60))
                .collect(),
        ],
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("golden.db");
    write_i64_shard(
        path.to_str().unwrap(),
        &schema,
        &rows,
        b"heap",
        ShardWriteOpts::COMPACTION,
    );
    let mut bytes = std::fs::read(&path).unwrap();

    let encodings: Vec<Encoding> = (0..=gnitz_wire::num_regions(2))
        .map(|i| region_dir(&bytes, i).1)
        .collect();
    use Encoding::*;
    assert_eq!(
        encodings,
        [Raw, TwoValue, Constant, For, Raw, Raw, Raw],
        "the batch covers every encoding"
    );
    assert!(region_dir(&bytes, gnitz_wire::num_regions(2)).0 > 0, "and a PK filter");

    // Both vary with things other than the writer: the system schema, the path.
    write_u64_le(&mut bytes, OFF_VERSION, 0);
    write_u64_le(&mut bytes, OFF_DESC_CHECKSUM, 0);
    assert_eq!(
        (SHARD_EPOCH, gnitz_wire::checksum(&bytes)),
        PINNED,
        "shard bytes changed: bump SHARD_EPOCH and re-pin"
    );
}

/// The filter builder walks the PK region in `stride`-wide chunks, so every
/// stride must chunk the region the same way the probe does. (It cannot test the
/// fingerprint derivation: builder and probe both call `probe_key`, so that half
/// is the same function by construction.)
#[test]
fn a_filter_built_from_a_pk_region_has_no_false_negatives_at_any_stride() {
    for stride in [8usize, 12, 16, 24] {
        let rows: Vec<Vec<u8>> = (0u8..5)
            .map(|r| (0..stride).map(|b| r.wrapping_mul(7).wrapping_add(b as u8)).collect())
            .collect();
        let pk_bytes: Vec<u8> = rows.iter().flatten().copied().collect();
        let f = build_shard_filter_from_pk_region(&pk_bytes, stride)
            .unwrap_or_else(|| panic!("stride {stride} must build a filter"));
        for row in &rows {
            assert!(f.contains(&probe_key(row)), "stride {stride}: false negative");
        }
    }
}

/// A replicated relayout hard-links one shard inode into several stores, so a
/// write at an existing name must leave that file untouched.
#[test]
fn write_refuses_an_existing_path_and_leaves_it_intact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("taken.db");
    let path = path.to_str().unwrap();
    let schema = make_schema_u64_i64();
    let rows = u64_rows(&[1], &[1], &[0], &[vec![10]]);
    write_i64_shard(path, &schema, &rows, &[], ShardWriteOpts::default());
    let before = std::fs::read(path).unwrap();

    let other = crate::test_support::make_batch_opk(&schema, &[(&2u64.to_be_bytes()[..], 1, 20)]);
    assert_eq!(
        other.write_as_shard(path, ShardWriteOpts::default()),
        Err(StorageError::Io(libc::EEXIST))
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
}
