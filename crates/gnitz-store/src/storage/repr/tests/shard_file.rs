use super::super::shard_reader::MappedShard;
use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::test_support::{make_schema_u64_i64, pk_only_schema};
use gnitz_wire::read_u64_le;
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
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    let schema = make_schema_u64_i64();
    let rows = u64_rows(&pks, &vec![1; n], &vec![0; n], std::slice::from_ref(&vals));
    write_i64_shard(&cpath, &schema, &rows, &[], ShardWriteOpts::default());

    let image = std::fs::read(&path).unwrap();
    assert_eq!(read_u64_le(&image, OFF_ROW_COUNT), n as u64);
    assert!(read_u64_le(&image, OFF_SHARD_FILTER_OFFSET) > 0);
    assert!(read_u64_le(&image, OFF_SHARD_FILTER_SIZE) > 0);

    // `open` itself rejects a bad magic or version, so a successful open is
    // what pins those; the rest is the row data.
    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
    assert_eq!(shard.count, n);
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

#[test]
fn empty_shard() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.db");
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

    // All-PK single-column schema (num_payload_cols = 0) → 4 regions.
    let schema = pk_only_schema(&[type_code::U64]);
    write_i64_shard(&cpath, &schema, &[], &[], ShardWriteOpts::default());

    let shard = MappedShard::open(&cpath, &schema, true).unwrap();
    assert_eq!(shard.count, 0);
    assert!(!shard.has_shard_filter());
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
    let n = members.len();

    let vals: Vec<i64> = members.iter().map(|&p| p as i64).collect();
    let schema = make_schema_u64_i64();
    let rows = u64_rows(&members, &vec![1; n], &vec![0; n], &[vals]);
    write_i64_shard(&cpath, &schema, &rows, &[], ShardWriteOpts::default());
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
fn encoding_selection_pins_all_roles() {
    let dir = tempfile::tempdir().unwrap();
    let write_and_read = |schema: &SchemaDescriptor, rows: &Rows, blob: &[u8], name: &str| -> Vec<u8> {
        let path = dir.path().join(name);
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        write_i64_shard(&cpath, schema, rows, blob, ShardWriteOpts::default());
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
    let blob_a: Vec<u8> = vec![0xAA, 0xBB, 0xCC];
    let rows_a = u64_rows(
        &vec![7; n_a],
        &vec![1; n_a],
        &vec![0; n_a],
        &[vec![42; n_a], (0..n_a as i64).collect()],
    );
    let img_a = write_and_read(&schema_a, &rows_a, &blob_a, "pin_a.db");
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
    let rows_b = u64_rows(
        &(1u64..=n_b as u64).collect::<Vec<_>>(),
        &[1, -1, 1, -1],
        &[1, 0, 1, 0], // col-0 null bit set on rows 0,2
        &[vec![10, 20, 30, 40]],
    );
    let img_b = write_and_read(&schema_b, &rows_b, &[], "pin_b.db");
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
    let rows_c = u64_rows(&[1, 2, 3], &[1, -1, 2], &vec![0; n_c], &[vec![5, 6, 7]]);
    let img_c = write_and_read(&schema_c, &rows_c, &[], "pin_c.db");
    assert_eq!(
        region_dir(&img_c, 1),
        (n_c * 8, ENCODING_RAW),
        "C weight ≥3 distinct → raw"
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

mod for_codec_tests {
    use super::super::super::shard_reader::decode_for_region;
    use super::super::{for_image, for_image_bw, for_image_len, ALIGNMENT};
    use crate::test_rng::Rng;
    use gnitz_wire::FixedInt;

    /// Pack signed/unsigned values into a raw `fi.width()`-byte-per-cell LE
    /// region, exactly as the payload region would appear on the batch.
    fn pack(vals: &[i128], fi: FixedInt) -> Vec<u8> {
        let width = fi.width();
        let mut b = Vec::with_capacity(vals.len() * width);
        for &v in vals {
            let le = (v as i64 as u64).to_le_bytes();
            b.extend_from_slice(&le[..width]);
        }
        b
    }

    /// Encode → recover `bw` from the image geometry → decode → assert
    /// byte-exact reproduction of the input region. Returns the chosen `bw`
    /// (None ⇒ region stays Raw).
    fn roundtrip(vals: &[i128], fi: FixedInt) -> Option<usize> {
        let raw = pack(vals, fi);
        let n = vals.len();
        let image = for_image(&raw, fi)?;
        let bw = for_image_bw(image.len(), n, fi.width()).expect("the encoder's image has FoR geometry");
        let decoded = decode_for_region(&image, n, bw, fi.width());
        let bytes = decoded.as_bytes();
        assert_eq!(bytes.as_ptr() as usize % 8, 0, "decoded buffer 8-aligned");
        assert_eq!(bytes, &raw[..], "byte-exact roundtrip (bw={bw}, {fi:?})");
        Some(bw)
    }

    #[test]
    fn stride1_never_packs() {
        // U8/I8 (stride 1) can never pack: bw >= 1 == stride.
        assert_eq!(roundtrip(&[0, 1, 2, 3, 200], FixedInt::U8), None);
        assert_eq!(roundtrip(&[-5, 0, 5, 100], FixedInt::I8), None);
    }

    #[test]
    fn small_unsigned_bw1_bw2() {
        // Small values → bw 1.
        let small: Vec<i128> = (0..300).map(|i| (i % 200) as i128).collect();
        assert_eq!(roundtrip(&small, FixedInt::U32), Some(1));
        assert_eq!(roundtrip(&small, FixedInt::U64), Some(1));
        // Span needing 2 bytes.
        let mid: Vec<i128> = (0..300).map(|i| ((i * 211) % 60000) as i128).collect();
        assert_eq!(roundtrip(&mid, FixedInt::U32), Some(2));
    }

    #[test]
    fn high_floor_unsigned_drops_bytes() {
        // A tight range far from zero frames on its min and drops the high bytes.
        let hi: Vec<i128> = (0..256).map(|i| 1_000_000 + (i % 50) as i128).collect();
        assert_eq!(roundtrip(&hi, FixedInt::U32), Some(1));
        let hi64: Vec<i128> = (0..256).map(|i| 5_000_000_000i128 + (i % 40) as i128).collect();
        // Span < 256 → bw 1 even though the values need 5 bytes raw.
        assert_eq!(roundtrip(&hi64, FixedInt::U64), Some(1));
    }

    #[test]
    fn signed_negative_min_frames_on_min() {
        // Range spanning zero frames on the signed min, not on 0.
        let s: Vec<i128> = (0..256).map(|i| -100 + (i % 150) as i128).collect();
        assert_eq!(roundtrip(&s, FixedInt::I32), Some(1));
        assert_eq!(roundtrip(&s, FixedInt::I64), Some(1));
        // Larger signed span, still one frame.
        let s2: Vec<i128> = (0..500).map(|i| -30_000 + (i % 60000) as i128).collect();
        assert_eq!(roundtrip(&s2, FixedInt::I32), Some(2));
    }

    #[test]
    fn all_equal_declines_for() {
        // Constant is claimed before FoR; the encoder itself declines too.
        let eq = vec![42i128; 100];
        assert_eq!(for_image(&pack(&eq, FixedInt::U32), FixedInt::U32), None);
    }

    #[test]
    fn extremes_fall_back_to_raw() {
        // MIN/MAX span needs the full stride → bw >= stride → Raw.
        assert_eq!(roundtrip(&[0, u32::MAX as i128], FixedInt::U32), None);
        assert_eq!(roundtrip(&[i32::MIN as i128, i32::MAX as i128], FixedInt::I32), None);
        assert_eq!(roundtrip(&[0, u64::MAX as i128], FixedInt::U64), None);
        assert_eq!(roundtrip(&[i64::MIN as i128, i64::MAX as i128], FixedInt::I64), None);
    }

    #[test]
    fn null_zero_cells_roundtrip_bit_exact() {
        // NULL cells carry a zeroed value that joins the region's [min, max].
        // Packing frames on 0 and the zeros must round-trip bit-exact.
        let v: Vec<i128> = (0..200)
            .map(|i| if i % 3 == 0 { 0 } else { 1_000_000 + (i % 500) as i128 })
            .collect();
        // min 0, max ~1_000_500 → 3-byte span.
        assert_eq!(roundtrip(&v, FixedInt::U32), Some(3));
        // Signed variant: NULL zeros among negative values → min is negative.
        let vs: Vec<i128> = (0..200)
            .map(|i| if i % 4 == 0 { 0 } else { -500_000 - (i % 300) as i128 })
            .collect();
        assert_eq!(roundtrip(&vs, FixedInt::I32), Some(3));
    }

    #[test]
    fn seeded_random_every_eligible_type() {
        use FixedInt::*;
        let mut rng = Rng::new(0x9E3779B97F4A7C15);
        for fi in [U16, I16, U32, I32, U64, I64] {
            let signed = matches!(fi, I16 | I32 | I64);
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
                            let masked = match fi.width() {
                                2 => (base as u16 as u64).wrapping_add(off as u64) as u16 as u64,
                                4 => (base as u32 as u64).wrapping_add(off as u64) as u32 as u64,
                                _ => base.wrapping_add(off as u64),
                            };
                            masked as i128
                        }
                    })
                    .collect();
                // Whatever the verdict, the roundtrip helper asserts byte-exact
                // reproduction whenever it does pack; a None means Raw, also fine.
                roundtrip(&vals, fi);
            }
        }
    }

    #[test]
    fn aligned_footprint_tie_declines() {
        // 10 U32 rows: raw 40 B, packed 8 + 10·2 = 28 B — both align to 64.
        // No block dropped → decline (stay Raw) despite a raw-byte win.
        let vals: Vec<i128> = (0..10).map(|i| (i * 1000) as i128).collect();
        assert_eq!(roundtrip(&vals, FixedInt::U32), None, "aligned footprints tie → Raw");
        // 100 U32 rows with the same per-row span pack (alignment drops blocks).
        let many: Vec<i128> = (0..100).map(|i| (i % 60000) as i128).collect();
        assert!(roundtrip(&many, FixedInt::U32).is_some(), "100 rows pack");
    }

    /// FoR decode-throughput + compression-ratio microbench. Run in release:
    /// `cargo test -p gnitz-store --release for_decode_bench --
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
        let raw = pack(&vals, FixedInt::I64);
        let image = for_image(&raw, FixedInt::I64).expect("region must pack");
        let bw = for_image_bw(image.len(), n, 8).expect("FoR geometry");

        let raw_bytes = (n * 8).next_multiple_of(ALIGNMENT);
        let packed_bytes = for_image_len(n, bw).next_multiple_of(ALIGNMENT);
        println!(
            "FoR ratio: {n} I64 rows, bw={bw}, raw(aligned)={raw_bytes}B packed(aligned)={packed_bytes}B \
             ({:.2}x)",
            raw_bytes as f64 / packed_bytes as f64
        );

        let iters = 200;
        let start = Instant::now();
        for _ in 0..iters {
            let decoded = decode_for_region(black_box(&image), n, bw, 8);
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
