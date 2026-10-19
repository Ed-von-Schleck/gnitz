use super::*;
use crate::test_support::Rng;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// Row number `r` as a key of `stride` bytes, in its leading `stride`.
fn key(stride: usize, r: usize) -> [u8; 40] {
    let mut k = [0u8; 40];
    k[stride - 8..stride].copy_from_slice(&(r as u64).to_be_bytes());
    k
}

/// `rows` ascending keys of `stride` bytes, row `r` holding key `r`.
fn region(rows: usize, stride: usize) -> Vec<u8> {
    (0..rows)
        .flat_map(|r| key(stride, r).into_iter().take(stride))
        .collect()
}

/// Instructions and cycles per probe over a region of ascending keys, row `r`
/// holding key `r`, at a stride in each width arm and each key-packing band: a
/// [`seek_lower_bound`] at random keys, then a [`seek_advance_to`] by the gap
/// between probes and by where the hint stands — at the last landing of an
/// ascending or a descending sweep, at row 0, and at the boundary itself.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn seek_bench() {
    const ROWS: usize = 1 << 20;
    const PROBES: usize = 100_000;
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    /// The hint for a probe at row `r`, the last probe having landed at `prev`.
    type Hint = fn(usize, usize) -> usize;
    let drivers: [(&str, bool, Hint); 4] = [
        ("ascending, seeded", false, |prev, _| prev),
        ("ascending, from row 0", false, |_, _| 0),
        ("descending, seeded", true, |prev, _| prev),
        ("hint at the boundary", false, |_, r| r),
    ];
    for stride in [8usize, 12, 16, 24, 32, 40] {
        let key = |r| key(stride, r);
        let region = region(ROWS, stride);
        let pk = ColPtr { base: region.as_ptr(), stride };
        let mut rng = Rng::new(0xC0FFEE + stride as u64);
        let probes: Vec<usize> = (0..PROBES).map(|_| rng.gen_range(ROWS as u64) as usize).collect();
        let lower_bounds = || {
            let mut landed = 0;
            for &r in &probes {
                let k = key(r);
                // SAFETY: `region` holds `ROWS` rows of `stride` bytes.
                landed += unsafe { seek_lower_bound(ROWS, stride, pk, black_box(&k[..stride])) };
            }
            landed
        };
        assert_eq!(lower_bounds(), probes.iter().sum::<usize>(), "row r holds key r");
        let (instr, cyc) = (instructions.measure(lower_bounds).1, cycles.measure(lower_bounds).1);
        println!(
            "seek_bench stride={stride:>2} lower bound at random keys {:6.1} instr/probe, {:6.1} cycles/probe",
            instr as f64 / PROBES as f64,
            cyc as f64 / PROBES as f64
        );
        for gap in [1usize, 128, 4096] {
            for (label, descending, hint) in drivers {
                let mut probes: Vec<usize> = (gap..ROWS).step_by(gap).collect();
                if descending {
                    probes.reverse();
                }
                let sweep = || {
                    let (mut at, mut landed) = (0, 0);
                    for &r in &probes {
                        let k = key(r);
                        // SAFETY: `region` holds `ROWS` rows of `stride` bytes.
                        at = unsafe { seek_advance_to(ROWS, stride, pk, black_box(&k[..stride]), hint(at, r)) };
                        landed += at;
                    }
                    landed
                };
                assert_eq!(sweep(), probes.iter().sum::<usize>(), "row r holds key r");
                let (instr, cyc) = (instructions.measure(sweep).1, cycles.measure(sweep).1);
                println!(
                    "seek_bench stride={stride:>2} advance, gap={gap:>4} {label:<21} \
                     {:6.1} instr/probe, {:6.1} cycles/probe",
                    instr as f64 / probes.len() as f64,
                    cyc as f64 / probes.len() as f64
                );
            }
        }
    }
}
