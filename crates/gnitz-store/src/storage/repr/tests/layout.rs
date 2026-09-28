use super::*;
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
    let image = for_encode(&raw, fi)?;
    let bw = for_image_bw(image.len(), n, fi.width()).expect("the encoder's image has FoR geometry");
    let mut decoded = vec![0u8; n * fi.width()];
    for_decode(&image, bw, fi.width(), 0, &mut decoded);
    assert_eq!(decoded, raw, "byte-exact roundtrip (bw={bw}, {fi:?})");
    if n >= 10 {
        let w = fi.width();
        let mut window = vec![0u8; 7 * w];
        for_decode(&image, bw, w, 3, &mut window);
        assert_eq!(
            window,
            raw[3 * w..10 * w],
            "rows [3, 10) decode alone (bw={bw}, {fi:?})"
        );
    }
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
    assert_eq!(for_encode(&pack(&eq, FixedInt::U32), FixedInt::U32), None);
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
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_decode_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    // A representative re-keyed ex-PK column: 1M I64 rows over a narrow
    // range far from zero (the `_int_`/`_hist_`/`_reduce_` shape).
    let n = 1_000_000usize;
    let vals: Vec<i128> = (0..n).map(|i| 3_000_000_000i128 + (i % 4000) as i128).collect();
    let raw = pack(&vals, FixedInt::I64);
    let image = for_encode(&raw, FixedInt::I64).expect("region must pack");
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
    let mut decoded = vec![0u8; n * 8];
    for _ in 0..iters {
        for_decode(black_box(&image), bw, 8, 0, &mut decoded);
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
