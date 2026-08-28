use super::super::shard_reader::MappedShard;
use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{make_schema_u64_i64, pk_only_schema};
use gnitz_wire::as_le_bytes;
use xorf::Filter;

#[test]
fn build_image_roundtrip() {
    let row_count = 3u32;
    let pks: Vec<u64> = vec![10, 20, 30];
    let weights: Vec<i64> = vec![1, 1, 1];
    let nulls: Vec<u64> = vec![0, 0, 0];
    let vals: Vec<i64> = vec![100, 200, 300];

    // PK region is OPK (big-endian) at rest; U64 OPK == BE.
    let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
    let weight_bytes: Vec<u8> = weights.iter().flat_map(|w| w.to_le_bytes()).collect();
    let null_bytes: Vec<u8> = nulls.iter().flat_map(|n| n.to_le_bytes()).collect();
    let val_bytes: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
    let blob: Vec<u8> = vec![];

    let regions: Vec<&[u8]> = vec![&pk_bytes, &weight_bytes, &null_bytes, &val_bytes, &blob];

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("build_image.db");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        row_count,
        &regions,
        &make_schema_u64_i64(),
        ShardWriteOpts::default(),
    )
    .unwrap();
    let image = std::fs::read(&path).unwrap();

    assert_eq!(read_u64_le(&image, OFF_MAGIC), SHARD_MAGIC);
    assert_eq!(read_u64_le(&image, OFF_VERSION), SHARD_VERSION);
    assert_eq!(read_u64_le(&image, OFF_ROW_COUNT), 3);
    assert!(read_u64_le(&image, OFF_SHARD_FILTER_OFFSET) > 0);
    assert!(read_u64_le(&image, OFF_SHARD_FILTER_SIZE) > 0);
}

#[test]
fn empty_shard() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.db");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    let regions: Vec<&[u8]> = vec![&[], &[], &[], &[]];

    // All-PK single-column schema (num_payload_cols = 0) → 4 regions.
    let schema = pk_only_schema(&[type_code::U64]);
    write_shard_streaming(libc::AT_FDCWD, &cpath, 0, &regions, &schema, ShardWriteOpts::default()).unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
    assert_eq!(shard.count, 0);
    assert!(!shard.has_shard_filter());
}

/// write_shard_streaming roundtrip — PK + weight + null_bmp + i64 value regions.
#[test]
fn test_write_shard_streaming_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("streaming.db");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    let row_count = 3u32;
    let pks: Vec<u64> = vec![10, 20, 30];
    let weights: Vec<i64> = vec![1, 1, 1];
    let nulls: Vec<u64> = vec![0, 0, 0];
    let vals: Vec<i64> = vec![100, 200, 300];
    // PK region is OPK (big-endian) at rest; U64 OPK == BE.
    let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
    let blob: Vec<u8> = vec![];

    let regions: Vec<&[u8]> = vec![
        &pk_bytes,
        as_le_bytes(&weights),
        as_le_bytes(&nulls),
        as_le_bytes(&vals),
        &blob,
    ];

    let schema = make_schema_u64_i64();
    write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        row_count,
        &regions,
        &schema,
        ShardWriteOpts::default(),
    )
    .unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
    assert_eq!(shard.count, 3);
    assert_eq!(shard.get_pk(0), 10);
    assert_eq!(shard.get_pk(1), 20);
    assert_eq!(shard.get_pk(2), 30);
    assert_eq!(shard.get_weight(0), 1);
    assert_eq!(shard.get_weight(1), 1);
    assert_eq!(shard.get_weight(2), 1);
    assert!(shard.has_shard_filter());
    assert!(shard.shard_filter_may_contain(probe_key(&10u64.to_be_bytes())));
    assert!(shard.shard_filter_may_contain(probe_key(&20u64.to_be_bytes())));
    assert!(shard.shard_filter_may_contain(probe_key(&30u64.to_be_bytes())));
}

#[test]
fn u64_pk_shard_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("u64pk.db");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    let n = 5u32;
    let pks: Vec<u64> = vec![100, 200, 300, 400, 500];
    let weights: Vec<i64> = vec![1; n as usize];
    let nulls: Vec<u64> = vec![0; n as usize];
    let vals: Vec<i64> = vec![10, 20, 30, 40, 50];
    // PK region is OPK (big-endian) at rest; U64 OPK == BE.
    let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
    assert_eq!(pk_bytes.len(), n as usize * 8, "U64 PK region must be 8B/row");
    let blob: Vec<u8> = vec![];

    let regions: Vec<&[u8]> = vec![
        &pk_bytes,
        as_le_bytes(&weights),
        as_le_bytes(&nulls),
        as_le_bytes(&vals),
        &blob,
    ];

    let schema = make_schema_u64_i64();
    write_shard_streaming(libc::AT_FDCWD, &cpath, n, &regions, &schema, ShardWriteOpts::default()).unwrap();
    let image = std::fs::read(&path).unwrap();
    assert_eq!(
        read_u64_le(&image, OFF_VERSION),
        SHARD_VERSION,
        "must write current shard version"
    );

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
    assert_eq!(shard.pk_stride, 8, "pk_stride must be 8 for U64 schema");
    assert_eq!(shard.count, n as usize);
    for (i, &expected_pk) in pks.iter().enumerate() {
        assert_eq!(shard.get_pk(i), expected_pk as u128, "get_pk row {i}");
    }
    assert!(shard.has_shard_filter());
    for &pk in &pks {
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
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    // Members and non-members from one stream, split by parity of the draw,
    // so neither set is a range the other can be confused with.
    let mut rng = crate::test_rng::Rng::new(0xF11E_5EED);
    let mut members: Vec<u64> = (0..N).map(|_| rng.next_u64()).collect();
    let absent: Vec<u64> = (0..N).map(|_| rng.next_u64()).collect();
    // The PK region is sorted by (PK, payload) — the writer's contract.
    members.sort_unstable();
    members.dedup();
    let n = members.len() as u32;

    let weights: Vec<i64> = vec![1; members.len()];
    let nulls: Vec<u64> = vec![0; members.len()];
    let vals: Vec<i64> = members.iter().map(|&p| p as i64).collect();
    let pk_bytes: Vec<u8> = members.iter().flat_map(|p| p.to_be_bytes()).collect();
    let blob: Vec<u8> = vec![];
    let regions: Vec<&[u8]> = vec![
        &pk_bytes,
        as_le_bytes(&weights),
        as_le_bytes(&nulls),
        as_le_bytes(&vals),
        &blob,
    ];

    let schema = make_schema_u64_i64();
    write_shard_streaming(libc::AT_FDCWD, &cpath, n, &regions, &schema, ShardWriteOpts::default()).unwrap();
    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
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
fn u64_pk_constant_shard() {
    // All rows share the same PK → Constant encoding for the PK region.
    let n = 6u32;
    let pk_val: u64 = 42;
    let pks: Vec<u64> = vec![pk_val; n as usize];
    let weights: Vec<i64> = vec![1; n as usize];
    let nulls: Vec<u64> = vec![0; n as usize];
    let vals: Vec<i64> = (1..=n as i64).collect();
    // PK region is OPK (big-endian) at rest; U64 OPK == BE.
    let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
    let blob: Vec<u8> = vec![];

    let regions: Vec<&[u8]> = vec![
        &pk_bytes,
        as_le_bytes(&weights),
        as_le_bytes(&nulls),
        as_le_bytes(&vals),
        &blob,
    ];

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("u64_const.db");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        n,
        &regions,
        &make_schema_u64_i64(),
        ShardWriteOpts::default(),
    )
    .unwrap();
    let image = std::fs::read(&path).unwrap();

    // Constant-encoded PK region → directory entry size == 8 (one elem).
    assert_eq!(
        region_dir(&image, 0),
        (8, ENCODING_CONSTANT),
        "PK region must be Constant-encoded, storing a single 8B value"
    );
}

/// Streaming write with regions that trigger Constant encoding.
#[test]
fn test_write_shard_streaming_encodings() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("streaming_enc.db");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    let row_count = 4u32;
    let pks: Vec<u64> = vec![1, 2, 3, 4];
    let weights: Vec<i64> = vec![1, 1, 1, 1]; // all-same → Constant encoding
    let nulls: Vec<u64> = vec![0, 0, 0, 0]; // all-zero → Constant encoding
    let vals: Vec<i64> = vec![42, 42, 42, 42]; // all-same → Constant encoding
                                               // PK region is OPK (big-endian) at rest; U64 OPK == BE.
    let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
    let blob: Vec<u8> = vec![];

    let regions: Vec<&[u8]> = vec![
        &pk_bytes,
        as_le_bytes(&weights),
        as_le_bytes(&nulls),
        as_le_bytes(&vals),
        &blob,
    ];

    let schema = make_schema_u64_i64();
    write_shard_streaming(
        libc::AT_FDCWD,
        &cpath,
        row_count,
        &regions,
        &schema,
        ShardWriteOpts::default(),
    )
    .unwrap();

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
    assert_eq!(shard.count, 4);
    for i in 0..4 {
        assert_eq!(shard.get_pk(i), (i + 1) as u128);
        assert_eq!(shard.get_weight(i), 1);
    }
}

/// Pins every `(region role → encoding)` pair the writer can emit to today's
/// behavior. A shard has one weight and one null_bmp region, so one shard can
/// pin at most one weight and one null encoding; three shards choreograph all
/// ten pairs — PK{Constant,Raw}, Weight{Constant,TwoValue,Raw},
/// NullBmp{Constant,Raw}, Payload{Constant,Raw}, Blob{Raw}.
#[test]
fn encoding_selection_pins_all_roles() {
    let dir = tempfile::tempdir().unwrap();
    let write_and_read = |schema: &SchemaDescriptor, n: u32, regions: &[&[u8]], name: &str| -> Vec<u8> {
        let path = dir.path().join(name);
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        write_shard_streaming(libc::AT_FDCWD, &cpath, n, regions, schema, ShardWriteOpts::default()).unwrap();
        std::fs::read(&path).unwrap()
    };

    // --- Shard A (Constant-heavy): constant PK, all-1 weight, all-0 nulls,
    // one constant + one varying payload column, non-empty blob. ---
    let schema_a = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0), // PK
            SchemaColumn::new(type_code::I64, 0), // constant payload
            SchemaColumn::new(type_code::I64, 0), // varying payload
        ],
        &[0],
    );
    let n_a = 4usize;
    let pk_a: Vec<u8> = vec![7u64; n_a].iter().flat_map(|p| p.to_be_bytes()).collect();
    let w_a: Vec<i64> = vec![1; n_a];
    let null_a: Vec<u64> = vec![0; n_a];
    let const_col: Vec<i64> = vec![42; n_a];
    let vary_col: Vec<i64> = (0..n_a as i64).collect();
    let blob_a: Vec<u8> = vec![0xAA, 0xBB, 0xCC];
    let regions_a: Vec<&[u8]> = vec![
        &pk_a,
        as_le_bytes(&w_a),
        as_le_bytes(&null_a),
        as_le_bytes(&const_col),
        as_le_bytes(&vary_col),
        &blob_a,
    ];
    let img_a = write_and_read(&schema_a, n_a as u32, &regions_a, "pin_a.db");
    assert_eq!(region_dir(&img_a, 0), (8, ENCODING_CONSTANT), "A pk constant");
    assert_eq!(region_dir(&img_a, 1), (8, ENCODING_CONSTANT), "A weight constant");
    assert_eq!(region_dir(&img_a, 2), (8, ENCODING_CONSTANT), "A null constant");
    assert_eq!(region_dir(&img_a, 3), (8, ENCODING_CONSTANT), "A payload constant");
    assert_eq!(
        region_dir(&img_a, 4),
        (n_a * 8, ENCODING_RAW),
        "A payload varying → raw"
    );
    assert_eq!(region_dir(&img_a, 5), (blob_a.len(), ENCODING_RAW), "A blob raw");

    // --- Shard B (TwoValue weight, Raw nulls): distinct PKs, alternating
    // 1/-1 weights, a nullable column NULL on a subset so the null_bmp holds
    // ≥2 distinct null-words. ---
    let schema_b = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 1), // nullable
        ],
        &[0],
    );
    let n_b = 4usize;
    let pk_b: Vec<u8> = (1u64..=n_b as u64).flat_map(|p| p.to_be_bytes()).collect();
    let w_b: Vec<i64> = vec![1, -1, 1, -1];
    let null_b: Vec<u64> = vec![1, 0, 1, 0]; // col-0 null bit set on rows 0,2
    let pay_b: Vec<i64> = vec![10, 20, 30, 40];
    let blob_b: Vec<u8> = vec![];
    let regions_b: Vec<&[u8]> = vec![
        &pk_b,
        as_le_bytes(&w_b),
        as_le_bytes(&null_b),
        as_le_bytes(&pay_b),
        &blob_b,
    ];
    let img_b = write_and_read(&schema_b, n_b as u32, &regions_b, "pin_b.db");
    assert_eq!(region_dir(&img_b, 0), (n_b * 8, ENCODING_RAW), "B pk distinct → raw");
    assert_eq!(
        region_dir(&img_b, 1),
        (two_value_image_len(n_b), ENCODING_TWO_VALUE),
        "B weight two-value"
    );
    assert_eq!(region_dir(&img_b, 2), (n_b * 8, ENCODING_RAW), "B null mixed → raw");

    // --- Shard C (Raw weight): ≥3 distinct weight values. ---
    let schema_c = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    );
    let n_c = 3usize;
    let pk_c: Vec<u8> = (1u64..=n_c as u64).flat_map(|p| p.to_be_bytes()).collect();
    let w_c: Vec<i64> = vec![1, -1, 2];
    let null_c: Vec<u64> = vec![0; n_c];
    let pay_c: Vec<i64> = vec![5, 6, 7];
    let blob_c: Vec<u8> = vec![];
    let regions_c: Vec<&[u8]> = vec![
        &pk_c,
        as_le_bytes(&w_c),
        as_le_bytes(&null_c),
        as_le_bytes(&pay_c),
        &blob_c,
    ];
    let img_c = write_and_read(&schema_c, n_c as u32, &regions_c, "pin_c.db");
    assert_eq!(
        region_dir(&img_c, 1),
        (n_c * 8, ENCODING_RAW),
        "C weight ≥3 distinct → raw"
    );
}

/// `check_region_shape`, both arms. It runs ahead of every syscall, so a
/// rejected write leaves neither the shard nor its staging file behind.
#[test]
fn regions_disagreeing_with_the_schema_fail_the_write_and_leave_no_file() {
    let dir = tempfile::tempdir().unwrap();
    let n = 4u32;
    let pk_bytes: Vec<u8> = (1u64..=4).flat_map(|p| p.to_be_bytes()).collect();
    let weights: Vec<i64> = vec![1; 4];
    let nulls: Vec<u64> = vec![0; 4];
    let vals: Vec<i64> = vec![10, 20, 30, 40];
    let blob: Vec<u8> = vec![];
    let full: Vec<&[u8]> = vec![
        &pk_bytes,
        as_le_bytes(&weights),
        as_le_bytes(&nulls),
        as_le_bytes(&vals),
        &blob,
    ];
    // Three payload values where the schema's stride implies four, and a
    // region list one short of the schema's arity.
    let mut short_payload = full.clone();
    short_payload[REG_PAYLOAD_START] = &as_le_bytes(&vals)[..24];

    for (name, regions) in [
        ("short payload region", &short_payload),
        ("missing region", &full[..4].to_vec()),
    ] {
        let path = dir.path().join(format!("{name}.db"));
        assert_eq!(
            write_shard_streaming(
                libc::AT_FDCWD,
                &std::ffi::CString::new(path.to_str().unwrap()).unwrap(),
                n,
                regions,
                &make_schema_u64_i64(),
                ShardWriteOpts::default(),
            ),
            Err(StorageError::InvalidShard),
            "{name}",
        );
        assert!(!path.exists(), "{name}: no shard file");
        assert!(
            !dir.path().join(format!("{name}.db.tmp")).exists(),
            "{name}: no staging file either",
        );
    }
}

#[test]
fn build_filter_wide_compound_region() {
    let stride = 24usize;
    let rows: Vec<Vec<u8>> = (0u8..5)
        .map(|r| (0..stride).map(|b| r.wrapping_add(b as u8)).collect())
        .collect();
    let pk_bytes: Vec<u8> = rows.iter().flatten().copied().collect();
    let f = build_shard_filter_from_pk_region(&pk_bytes, stride).expect("wide compound region must build a filter");
    for row in &rows {
        assert!(f.contains(&probe_key(row)), "no false negative for wide-region row");
    }
}

#[test]
fn build_filter_narrow_compound_region() {
    let stride = 12usize;
    let rows: Vec<Vec<u8>> = (0u8..5)
        .map(|r| (0..stride).map(|b| r.wrapping_mul(7).wrapping_add(b as u8)).collect())
        .collect();
    let pk_bytes: Vec<u8> = rows.iter().flatten().copied().collect();
    let f = build_shard_filter_from_pk_region(&pk_bytes, stride).expect("narrow compound region must build a filter");
    for row in &rows {
        // Builder fingerprint for stride <= 16 is widen_pk_be(OPK bytes);
        // the probe must derive the same value.
        assert!(f.contains(&probe_key(row)), "no false negative for narrow-compound row");
    }
}

#[test]
fn build_filter_single_pk_regression() {
    let pks64: Vec<u64> = vec![10, 20, 30, 40];
    // OPK at rest: U64 region is big-endian.
    let b64: Vec<u8> = pks64.iter().flat_map(|p| p.to_be_bytes()).collect();
    let f64 = build_shard_filter_from_pk_region(&b64, 8).expect("8-byte region must build a filter");
    for p in &pks64 {
        assert!(f64.contains(&probe_key(&p.to_be_bytes())));
    }

    let pks128: Vec<u128> = vec![1, 1 << 64, u128::MAX, 12345];
    // OPK at rest: U128 region is big-endian.
    let b128: Vec<u8> = pks128.iter().flat_map(|p| p.to_be_bytes()).collect();
    let f128 = build_shard_filter_from_pk_region(&b128, 16).expect("16-byte region must build a filter");
    for p in &pks128 {
        assert!(f128.contains(&probe_key(&p.to_be_bytes())));
    }
}

mod for_codec_tests {
    use super::super::{align64, decode_for_region, encode_for_region, for_encoded_size, for_params};
    use crate::test_rng::Rng;

    /// Pack signed/unsigned values into a raw `stride`-byte-per-cell LE region,
    /// exactly as the payload region would appear on the batch.
    fn pack(vals: &[i128], stride: usize) -> Vec<u8> {
        let mut b = Vec::with_capacity(vals.len() * stride);
        for &v in vals {
            let le = (v as i64 as u64).to_le_bytes();
            b.extend_from_slice(&le[..stride]);
        }
        b
    }

    /// Encode → assert size → decode → assert byte-exact reproduction of the
    /// input region. Returns the chosen `bw` (None ⇒ region stays Raw).
    fn roundtrip(vals: &[i128], stride: usize, signed: bool) -> Option<usize> {
        let raw = pack(vals, stride);
        let n = vals.len();
        let image = encode_for_region(&raw, stride, signed, n)?;
        // The width the writer chose, recovered exactly as `decode_for_region` does.
        let bw = (image.len() - 8) / n;
        assert_eq!(image.len(), for_encoded_size(n, bw), "emitted size == for_encoded_size");
        assert!(bw >= 1 && bw < stride, "bw {bw} in [1, {stride})");
        let decoded = decode_for_region(&image, n, stride);
        let bytes = decoded.as_bytes();
        assert_eq!(bytes.as_ptr() as usize % 8, 0, "decoded buffer 8-aligned");
        assert_eq!(bytes, &raw[..], "byte-exact roundtrip (bw={bw}, stride={stride})");
        Some(bw)
    }

    #[test]
    fn stride1_never_packs() {
        // U8/I8 (stride 1) can never pack: bw >= 1 == stride.
        assert_eq!(roundtrip(&[0, 1, 2, 3, 200], 1, false), None);
        assert_eq!(roundtrip(&[-5, 0, 5, 100], 1, true), None);
    }

    #[test]
    fn small_unsigned_bw1_bw2() {
        // Small values → bw 1.
        let small: Vec<i128> = (0..300).map(|i| (i % 200) as i128).collect();
        assert_eq!(roundtrip(&small, 4, false), Some(1));
        assert_eq!(roundtrip(&small, 8, false), Some(1));
        // Span needing 2 bytes.
        let mid: Vec<i128> = (0..300).map(|i| ((i * 211) % 60000) as i128).collect();
        assert_eq!(roundtrip(&mid, 4, false), Some(2));
    }

    #[test]
    fn high_floor_unsigned_drops_bytes() {
        // A tight range far from zero frames on its min and drops the high bytes.
        let hi: Vec<i128> = (0..256).map(|i| 1_000_000 + (i % 50) as i128).collect();
        assert_eq!(roundtrip(&hi, 4, false), Some(1));
        let hi64: Vec<i128> = (0..256).map(|i| 5_000_000_000i128 + (i % 40) as i128).collect();
        // Span < 256 → bw 1 even though the values need 5 bytes raw.
        assert_eq!(roundtrip(&hi64, 8, false), Some(1));
    }

    #[test]
    fn signed_negative_min_frames_on_min() {
        // Range spanning zero frames on the signed min, not on 0.
        let s: Vec<i128> = (0..256).map(|i| -100 + (i % 150) as i128).collect();
        assert_eq!(roundtrip(&s, 4, true), Some(1));
        assert_eq!(roundtrip(&s, 8, true), Some(1));
        // Larger signed span, still one frame.
        let s2: Vec<i128> = (0..500).map(|i| -30_000 + (i % 60000) as i128).collect();
        assert_eq!(roundtrip(&s2, 4, true), Some(2));
    }

    #[test]
    fn all_equal_declines_for() {
        // Constant is claimed before FoR; for_params/encode return None.
        let eq = vec![42i128; 100];
        assert_eq!(for_params(&pack(&eq, 4), 4, false, 100), None);
        assert_eq!(encode_for_region(&pack(&eq, 4), 4, false, 100), None);
    }

    #[test]
    fn extremes_fall_back_to_raw() {
        // MIN/MAX span needs the full stride → bw >= stride → Raw.
        let ext_u: Vec<i128> = vec![0, u32::MAX as i128];
        assert_eq!(roundtrip(&ext_u, 4, false), None);
        let ext_s: Vec<i128> = vec![i32::MIN as i128, i32::MAX as i128];
        assert_eq!(roundtrip(&ext_s, 4, true), None);
        let ext_u64: Vec<i128> = vec![0, u64::MAX as i128];
        assert_eq!(roundtrip(&ext_u64, 8, false), None);
        let ext_i64: Vec<i128> = vec![i64::MIN as i128, i64::MAX as i128];
        assert_eq!(roundtrip(&ext_i64, 8, true), None);
    }

    #[test]
    fn null_zero_cells_roundtrip_bit_exact() {
        // NULL cells carry a zeroed value that joins the region's [min, max].
        // Packing frames on 0 and the zeros must round-trip bit-exact.
        let mut v: Vec<i128> = Vec::new();
        for i in 0..200 {
            v.push(if i % 3 == 0 { 0 } else { 1_000_000 + (i % 500) as i128 });
        }
        // min 0, max ~1_000_500 → 3-byte span.
        assert_eq!(roundtrip(&v, 4, false), Some(3));
        // Signed variant: NULL zeros among negative values → min is negative.
        let mut vs: Vec<i128> = Vec::new();
        for i in 0..200 {
            vs.push(if i % 4 == 0 { 0 } else { -500_000 - (i % 300) as i128 });
        }
        assert_eq!(roundtrip(&vs, 4, true), Some(3));
    }

    #[test]
    fn seeded_random_every_eligible_type() {
        let mut rng = Rng::new(0x9E3779B97F4A7C15);
        // (stride, signed, value mask) for every eligible fixed-int type code.
        let cases: &[(usize, bool)] = &[(2, false), (2, true), (4, false), (4, true), (8, false), (8, true)];
        for &(stride, signed) in cases {
            for &n in &[1usize, 1023, 1024, 10000] {
                // Draw a random tight base and a random small span so most
                // regions pack; a few will naturally decline.
                let base = rng.next_u64();
                let span = 1 + (rng.next_u64() % 4000);
                let vals: Vec<i128> = (0..n)
                    .map(|_| {
                        let off = (rng.next_u64() % span) as i128;
                        if signed {
                            // Center the window around a signed base.
                            (base as i64).wrapping_add(off as i64) as i128
                        } else {
                            // Mask into the type width to stay in range.
                            let masked = if stride == 2 {
                                (base as u16 as u64).wrapping_add(off as u64) as u16 as u64
                            } else if stride == 4 {
                                (base as u32 as u64).wrapping_add(off as u64) as u32 as u64
                            } else {
                                base.wrapping_add(off as u64)
                            };
                            masked as i128
                        }
                    })
                    .collect();
                // Whatever the verdict, the roundtrip helper asserts byte-exact
                // reproduction whenever it does pack; a None means Raw, also fine.
                roundtrip(&vals, stride, signed);
            }
        }
    }

    #[test]
    fn aligned_footprint_tie_declines() {
        // 10 U32 rows: raw 40 B, packed 8 + 10·2 = 28 B — both align64 to 64.
        // No block dropped → decline (stay Raw) despite a raw-byte win.
        let vals: Vec<i128> = (0..10).map(|i| (i * 1000) as i128).collect();
        assert_eq!(align64(40), align64(28));
        assert_eq!(roundtrip(&vals, 4, false), None, "aligned footprints tie → Raw");
        // 100 U32 rows with the same per-row span pack (align64 drops blocks).
        let many: Vec<i128> = (0..100).map(|i| (i % 60000) as i128).collect();
        assert!(roundtrip(&many, 4, false).is_some(), "100 rows pack");
    }

    /// FoR decode-throughput + compression-ratio microbench. Run in release:
    /// `cargo test -p gnitz-engine --release for_decode_bench --
    ///   --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore]
    fn for_decode_bench() {
        use std::hint::black_box;
        use std::time::Instant;

        // A representative re-keyed ex-PK column: 1M I64 rows over a narrow
        // range far from zero (the `_int_`/`_hist_`/`_reduce_` shape).
        let n = 1_000_000usize;
        let vals: Vec<i128> = (0..n).map(|i| 3_000_000_000i128 + (i % 4000) as i128).collect();
        let raw = pack(&vals, 8);
        let image = encode_for_region(&raw, 8, true, n).expect("region must pack");
        let bw = (image.len() - 8) / n;

        let raw_bytes = align64(n * 8);
        let packed_bytes = align64(for_encoded_size(n, bw));
        println!(
            "FoR ratio: {n} I64 rows, bw={bw}, raw(aligned)={raw_bytes}B packed(aligned)={packed_bytes}B \
             ({:.2}x)",
            raw_bytes as f64 / packed_bytes as f64
        );

        let iters = 200;
        let start = Instant::now();
        for _ in 0..iters {
            let decoded = decode_for_region(black_box(&image), n, 8);
            black_box(&decoded);
        }
        let elapsed = start.elapsed();
        let vals_per_s = (n as f64 * iters as f64) / elapsed.as_secs_f64();
        println!(
            "FoR decode: {:.1} M values/s ({:.2} ms per {n}-row region)",
            vals_per_s / 1e6,
            elapsed.as_secs_f64() * 1e3 / iters as f64
        );
    }
}
