use super::*;
use gnitz_foundation::perf::Counter;
use std::hint::black_box;

/// Instructions and cycles per [`seek_advance_to`] probe over a region of
/// ascending keys, row `r` holding key `r`: at a stride in each width arm and
/// each key-packing band, by the gap between probes, and by where the hint
/// stands — at the last landing of an ascending or a descending sweep, at row 0,
/// and at the boundary itself.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn seek_advance_to_bench() {
    const ROWS: usize = 1 << 20;
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
        let key = |r: usize| {
            let mut k = [0u8; 40];
            k[stride - 8..stride].copy_from_slice(&(r as u64).to_be_bytes());
            k
        };
        let region: Vec<u8> = (0..ROWS).flat_map(|r| key(r).into_iter().take(stride)).collect();
        let pk = ColPtr { base: region.as_ptr(), stride };
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
                    "seek_advance_to_bench stride={stride:>2} gap={gap:>4} {label:<21} \
                     {:6.1} instr/probe, {:6.1} cycles/probe",
                    instr as f64 / probes.len() as f64,
                    cyc as f64 / probes.len() as f64
                );
            }
        }
    }
}
