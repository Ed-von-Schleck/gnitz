//! Instruction benchmarks for the mask kernels.

use crate::batch::MORSEL;
use crate::simd::{self, encode_f64};
use crate::test_support::{simd_levels, Meter};

/// [`crate::simd`]'s kernels alone, over one morsel that stays in cache, at
/// each level.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn mask_kernel_bench() {
    use std::hint::black_box;
    /// Passes per measurement: a morsel alone is too few instructions to
    /// outweigh the counter's own.
    const PASSES: usize = 1000;
    let meter = Meter::new();
    let a: Vec<i64> = (0..MORSEL).map(|i| (i * 7919 % 1000) as i64).collect();
    let b = vec![500i64; MORSEL];
    let floats = |v: &[i64]| -> Vec<i64> { v.iter().map(|&x| encode_f64(x as f64)).collect() };
    let (fa, fb) = (floats(&a), floats(&b));
    let nulls: Vec<u8> = (0..MORSEL as u64)
        .flat_map(|i| (i.is_multiple_of(5) as u64 * 2).to_le_bytes())
        .collect();
    let take = [0x5a5a_1234_dead_beef, 7, u64::MAX, 0x0f0f_0f0f_0f0f_0f0f];
    let set: Vec<i64> = (0..32).map(|i| i * 31 + 1).collect();
    let mut bits = [0u64; MORSEL / 64];
    let mut lanes = vec![0i64; MORSEL];

    for (level_name, level) in simd_levels() {
        let (a, b, fa, fb) = (
            black_box(&a[..]),
            black_box(&b[..]),
            black_box(&fa[..]),
            black_box(&fb[..]),
        );
        macro_rules! drive {
            ($name:expr, $kernel:expr) => {
                meter.pass(
                    "mask_kernel_bench",
                    &format!("{} @{level_name}", $name),
                    MORSEL * PASSES,
                    || {
                        for _ in 0..PASSES {
                            $kernel;
                            black_box((&bits, &lanes));
                        }
                    },
                )
            };
        }
        drive!("gt", simd::pred_bits::<simd::GtSigned>(level, a, b, &mut bits));
        drive!(
            "gt_unsigned",
            simd::pred_bits::<simd::GtUnsigned>(level, a, b, &mut bits)
        );
        drive!("gt_float", simd::pred_bits::<simd::GtFloat>(level, fa, fb, &mut bits));
        drive!("ne", simd::pred_bits::<simd::Not<simd::Eq>>(level, a, b, &mut bits));
        drive!("gt_total", simd::pred_bits::<simd::GtTotal>(level, fa, fb, &mut bits));
        drive!("truthy", simd::truthy_bits(level, a, &mut bits));
        for k in [2, 4, 8, 32] {
            drive!(
                format!("in{k}"),
                simd::in_set_bits(level, a, black_box(&set[..k]), &mut bits)
            );
        }
        drive!("nulls", simd::null_bits(level, black_box(&nulls), 2, &mut bits));
        drive!("blend", simd::blend(level, black_box(&take), a, b, &mut lanes));
        drive!("bit_lanes", simd::bit_lanes(level, black_box(&take), &mut lanes));
    }
}
