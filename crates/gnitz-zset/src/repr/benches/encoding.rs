use super::tests::{key_region, pack};
use super::*;
use crate::repr::batch::ColPtr;
use crate::repr::seek::seek_lower_bound;
use crate::schema::TypeCode;
use crate::test_support::Rng;
use gnitz_wire::FixedInt;

/// Instructions per value to decode a framed region in bulk, at each cell
/// width and each offset width it admits.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_decode_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    let instructions = Counter::instructions();
    for fi in [FixedInt::I16, FixedInt::I32, FixedInt::I64] {
        let w = fi.width();
        for bw in 1..w {
            // Offsets that span all of `bw` bytes, on a frame below zero.
            let span = 1i128 << (8 * bw - 1);
            let mut vals: Vec<i128> = (0..N as i128)
                .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) % span - span / 2)
                .collect();
            (vals[0], vals[1]) = (-span / 2, span / 2 - 1);
            let raw = pack(&vals, fi);
            let image = for_encode(&raw, fi).expect("region must pack");
            let frame = ForImage::parse(&image, N, w - 1).expect("the encoder's image parses");
            assert_eq!(frame.bw, bw);
            let mut decoded = vec![0u8; N * w];
            let ((), i) = instructions.measure(|| black_box(&frame).decode(0, w, black_box(&mut decoded)));
            assert_eq!(decoded, raw);
            println!(
                "{fi:?} at {bw}-byte offsets: {:.2} instructions per value, {:.2}x smaller",
                i as f64 / N as f64,
                raw.len() as f64 / image.len() as f64
            );
        }
    }
}

/// A PK region as its Key image, by key shape: the packed row's bytes, and
/// instructions per row to encode and to bulk-decode it and per seek, in the
/// image and in the region, at keys the region holds and keys one off them.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn key_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    use TypeCode::*;
    const N: usize = 1 << 20;
    const PROBES: usize = 200_000;
    type Value = fn(usize, usize) -> u128;
    let shapes: Vec<(&str, Vec<TypeCode>, Value)> = vec![
        ("a serial i64", vec![I64], |i, _| 4 * i as u128),
        ("a time-ordered i64 id", vec![I64], |i, _| {
            ((1_760_000_000_000 + i as u128 * 2471) << 22) | ((i as u128 * 977) % 4096)
        }),
        ("(i64 of 10k, serial i64)", vec![I64, I64], |i, c| {
            [(i as u128 * 7919) % 10_000, i as u128][c]
        }),
        ("(i64 of 10k, time-ordered i64 id)", vec![I64, I64], |i, c| {
            [
                (i as u128 * 7919) % 10_000,
                (1_760_000_000_000 + i as u128 * 2471) << 22,
            ][c]
        }),
        ("(i32 of 50, i32 of 1000, i64 seconds)", vec![I32, I32, I64], |i, c| {
            [
                (i % 50) as u128,
                (i as u128 * 31) % 1000,
                1_760_000_000 + (i as u128 * 7919) % 86_400,
            ][c]
        }),
        ("(i64 of 2k, i64 seconds, serial i64)", vec![I64, I64, I64], |i, c| {
            [
                (i as u128 * 7919) % 2000,
                1_760_000_000 + (i as u128 * 31) % 2_592_000,
                i as u128,
            ][c]
        }),
        ("a u128 of 80 bits", vec![U128], |i, _| {
            (i as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C) >> 8
        }),
        (
            "(i32 of 50, i32 of 1000, i32 of 7, i64 seconds)",
            vec![I32, I32, I32, I64],
            |i, c| {
                [
                    (i % 50) as u128,
                    (i as u128 * 31) % 1000,
                    (i % 7) as u128,
                    1_760_000_000 + (i as u128 * 7919) % 86_400,
                ][c]
            },
        ),
        (
            "(u128 of 50, u128 of 1000, u128 seconds)",
            vec![U128, U128, U128],
            |i, c| {
                [
                    (i % 50) as u128,
                    (i as u128 * 31) % 1000,
                    1_760_000_000 + (i as u128 * 7919) % 86_400,
                ][c]
            },
        ),
        ("five u128", vec![U128, U128, U128, U128, U128], |i, c| {
            [
                (i % 9) as u128,
                (i % 50) as u128,
                (i as u128 * 31) % 1000,
                (i % 7) as u128,
                1_760_000_000 + (i as u128 * 7919) % 86_400,
            ][c]
        }),
        (
            "(i16 of 9, i32 of 50, i32 of 1000, i32 of 7, i64 seconds)",
            vec![I16, I32, I32, I32, I64],
            |i, c| {
                [
                    (i % 9) as u128,
                    (i % 50) as u128,
                    (i as u128 * 31) % 1000,
                    (i % 7) as u128,
                    1_760_000_000 + (i as u128 * 7919) % 86_400,
                ][c]
            },
        ),
    ];
    let instructions = Counter::instructions();
    for (label, types, value) in shapes {
        let (schema, raw) = key_region(&types, N, value);
        let stride = schema.pk_stride();
        let (image, encode) = instructions.measure(|| key_encode(black_box(&raw), &schema).expect("region must pack"));
        let key = KeyImage::parse(&image, N, &schema).expect("the encoder's image parses");
        let mut decoded = vec![0u8; raw.len()];
        let ((), decode) = instructions.measure(|| black_box(&key).decode(0, black_box(&mut decoded)));
        assert!(decoded == raw);

        let mut rng = Rng::new(0x004B_4559);
        let probes: Vec<Vec<u8>> = (0..PROBES)
            .map(|i| {
                let row = rng.gen_range(N as u64) as usize;
                let mut probe = raw[row * stride..][..stride].to_vec();
                probe[stride - 1] ^= (i % 2) as u8;
                probe
            })
            .collect();
        let pk = ColPtr { base: raw.as_ptr(), stride };
        let (in_region, region) = instructions.measure(|| {
            // SAFETY: `raw` holds `N` rows of `stride` bytes.
            probes
                .iter()
                .map(|p| unsafe { seek_lower_bound(N, stride, pk, black_box(p)) })
                .sum::<usize>()
        });
        let (in_image, packed) = instructions.measure(|| {
            probes
                .iter()
                .map(|p| black_box(&key).seek(black_box(p), None))
                .sum::<usize>()
        });
        assert_eq!(in_image, in_region);
        println!(
            "key_bench {label:<58} {stride:>2} -> {:>2} bytes a row; per row: encode {:5.1}, decode {:5.1}; \
             per seek: {:6.1} in the image, {:6.1} in the region",
            key.stride,
            encode as f64 / N as f64,
            decode as f64 / N as f64,
            packed as f64 / PROBES as f64,
            region as f64 / PROBES as f64,
        );
    }
}
