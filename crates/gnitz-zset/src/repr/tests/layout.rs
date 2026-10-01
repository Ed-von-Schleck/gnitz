use super::*;
use crate::test_support::Rng;
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

/// The offset width `for_encode` must choose: the bytes `vals`' span needs, if
/// packing at that width drops an aligned block of the raw region.
fn expected_bw(vals: &[i128], fi: FixedInt) -> Option<usize> {
    let span = vals.iter().max()? - vals.iter().min()?;
    let bw = (128 - span.leading_zeros() as usize).div_ceil(8);
    (bw > 0 && region_start(for_image_len(vals.len(), bw)) < region_start(vals.len() * fi.width())).then_some(bw)
}

#[test]
fn for_packs_at_the_width_the_span_needs() {
    use FixedInt::*;
    let series = |n: i128, f: fn(i128) -> i128| (0..n).map(f).collect::<Vec<_>>();
    let cases: Vec<(&str, Vec<i128>, FixedInt, Option<usize>)> = vec![
        // bw >= 1 == stride.
        ("stride 1", vec![0, 1, 2, 3, 200], U8, None),
        ("stride 1 signed", vec![-5, 0, 5, 100], I8, None),
        ("small", series(300, |i| i % 200), U32, Some(1)),
        ("small u64", series(300, |i| i % 200), U64, Some(1)),
        ("two-byte span", series(300, |i| (i * 211) % 60000), U32, Some(2)),
        // A tight range far from zero frames on its min.
        ("high floor", series(256, |i| 1_000_000 + i % 50), U32, Some(1)),
        ("high floor u64", series(256, |i| 5_000_000_000 + i % 40), U64, Some(1)),
        // A range spanning zero frames on the signed min, not on 0.
        ("across zero", series(256, |i| -100 + i % 150), I32, Some(1)),
        ("across zero i64", series(256, |i| -100 + i % 150), I64, Some(1)),
        ("wide across zero", series(500, |i| -30_000 + i % 60000), I32, Some(2)),
        // Constant is claimed before FoR; the encoder declines too.
        ("all equal", vec![42; 100], U32, None),
        ("extremes", vec![0, u32::MAX as i128], U32, None),
        ("extremes signed", vec![i32::MIN as i128, i32::MAX as i128], I32, None),
        ("extremes u64", vec![0, u64::MAX as i128], U64, None),
        ("extremes i64", vec![i64::MIN as i128, i64::MAX as i128], I64, None),
        // NULL cells carry a zero that joins the frame.
        (
            "null zeros",
            series(200, |i| if i % 3 == 0 { 0 } else { 1_000_000 + i % 500 }),
            U32,
            Some(3),
        ),
        (
            "null zeros signed",
            series(200, |i| if i % 4 == 0 { 0 } else { -500_000 - i % 300 }),
            I32,
            Some(3),
        ),
        // Raw 40 B, packed 8 + 10·2 + 7 B: both align to 64, so nothing is saved.
        ("aligned footprints tie", series(10, |i| i * 1000), U32, None),
    ];
    for (what, vals, fi, want) in cases {
        assert_eq!(roundtrip(&vals, fi), want, "{what}");
        assert_eq!(expected_bw(&vals, fi), want, "{what}: the oracle");
    }
}

/// Random regions of every eligible type, at spans from one value to the
/// type's whole range, pack exactly when and as tightly as [`expected_bw`] says.
#[test]
fn random_regions_pack_at_the_expected_width() {
    use FixedInt::*;
    let mut rng = Rng::new(0x9E3779B97F4A7C15);
    let (mut packed, mut declined) = (0, 0);
    for fi in [U8, I8, U16, I16, U32, I32, U64, I64] {
        let bits = 8 * fi.width() as u32;
        let signed = matches!(fi, I8 | I16 | I32 | I64);
        let (lo, size) = (if signed { -(1i128 << (bits - 1)) } else { 0 }, 1u128 << bits);
        for &n in &[1usize, 2, 10, 1023, 1024, 5000] {
            for _ in 0..8 {
                let span = 1u128 << rng.gen_range(bits as u64 + 1);
                let base = lo + (rng.gen_u128() % (size - span + 1)) as i128;
                let vals: Vec<i128> = (0..n).map(|_| base + (rng.gen_u128() % span) as i128).collect();
                let got = roundtrip(&vals, fi);
                assert_eq!(got, expected_bw(&vals, fi), "{fi:?} n={n} span={span}");
                if got.is_some() {
                    packed += 1;
                } else {
                    declined += 1;
                }
            }
        }
    }
    assert!(packed > 50 && declined > 50, "{packed} packed, {declined} declined");
}

/// FoR decode-throughput + compression-ratio microbench. Run in release:
/// `cargo test -p gnitz-zset --release for_decode_bench --
///   --ignored --nocapture --test-threads=1`
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_decode_bench() {
    use std::hint::black_box;

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
    let mut decoded = vec![0u8; n * 8];
    let elapsed = crate::test_support::bench_time(iters, || {
        for_decode(black_box(&image), bw, 8, 0, &mut decoded);
        black_box(&decoded);
    });
    let vals_per_s = (n as f64 * iters as f64) / elapsed.as_secs_f64();
    println!(
        "FoR decode: {:.1} M values/s ({:.2} ms per {n}-row region)",
        vals_per_s / 1e6,
        elapsed.as_secs_f64() * 1e3 / iters as f64
    );
}
