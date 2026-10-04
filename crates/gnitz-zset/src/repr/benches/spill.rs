use super::*;
use crate::schema::TypeCode;
use crate::test_support::{pk_only_schema, Rng};
use gnitz_foundation::perf::Counter;

/// The whole pipeline — push 128 MiB of random records a chunk at a time,
/// `finish`, drain through `fill` — at stride classes up to the widest the
/// pre-flight reaches, with the data in one RAM run, in 4 spilled runs and in 32.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn spill_sort_bench() {
    const DATA: usize = 128 << 20;
    const CHUNK_ROWS: usize = 65_536;
    let (instructions, cycles) = (Counter::instructions(), Counter::cycles());
    for stride in [8usize, 16, 24, 40, 64] {
        let n = DATA / stride;
        let mut rng = Rng::new(0x5EED_0000 + stride as u64);
        let mut flat = vec![0u8; n * stride];
        for word in flat.chunks_exact_mut(8) {
            word.copy_from_slice(&rng.next_u64().to_be_bytes());
        }
        let mut pk = vec![TypeCode::U128; stride / 16];
        pk.extend((stride % 16 == 8).then_some(TypeCode::U64));
        let schema = pk_only_schema(&pk);
        for (runs, budget) in [(1, usize::MAX), (4, DATA / 4), (32, DATA / 32)] {
            let dir = tempfile::tempdir().unwrap();
            let sort = || {
                let mut s = SpillSort::new(dir.path().to_str().unwrap(), stride, budget);
                for chunk in flat.chunks(CHUNK_ROWS * stride) {
                    s.push(chunk).unwrap();
                }
                let mut sorted = s.finish().unwrap();
                let (mut chunk, mut rows) = (Batch::empty_with_schema(&schema), 0);
                while sorted.remaining() > 0 {
                    sorted.fill(&mut chunk, CHUNK_ROWS);
                    rows += std::hint::black_box(&chunk).count;
                }
                assert_eq!(rows, n);
            };
            let (instr, cyc) = (instructions.measure(sort).1, cycles.measure(sort).1);
            println!(
                "spill_sort_bench stride={stride:<3} runs={runs:<3} {:6.1} instr/record, {:6.1} cycles/record",
                instr as f64 / n as f64,
                cyc as f64 / n as f64
            );
        }
    }
}
