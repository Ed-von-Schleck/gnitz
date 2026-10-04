use super::tests::pack;
use super::*;
use gnitz_wire::FixedInt;

/// Instructions per value to decode a framed region in bulk, at each cell
/// width and each offset width it admits.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_decode_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    let instructions = Counter::instructions().unwrap();
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
