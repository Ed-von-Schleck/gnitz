use super::*;
use crate::repr::BatchBuilder;
use crate::schema::TypeCode;
use crate::test_support::pk_only_schema;
use crate::test_support::Rng;

/// The indirect sort of a flat record buffer. Sweeps OPK strides at a
/// chunk-sized `n` and a large one.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn sort_indices_bench() {
    use crate::test_support::bench_time_each;

    const ITERS: usize = 3;
    for &n in &[65_536usize, 4 << 20] {
        for &stride in &[4usize, 8, 12, 16, 24, 40] {
            for seq in [true, false] {
                let mut rng = Rng::new(0x5EED_0000 + n as u64 + stride as u64);
                let mut flat = vec![0u8; n * stride];
                for (i, rec) in flat.chunks_mut(stride).enumerate() {
                    for chunk in rec.chunks_mut(8) {
                        let bytes = rng.next_u64().to_be_bytes();
                        chunk.copy_from_slice(&bytes[..chunk.len()]);
                    }
                    // Distinct ids in the leading bytes: neighbours share a prefix.
                    if seq {
                        let id = (i as u64).wrapping_mul(0x9E37_79B9) % n as u64;
                        let be = id.to_be_bytes();
                        let w = stride.min(8);
                        rec[..w].copy_from_slice(&be[8 - w..]);
                    }
                }
                let elapsed = bench_time_each(ITERS, Vec::new, |mut idx| {
                    sort_indices(&flat, stride, &mut idx);
                    std::hint::black_box(&idx);
                });
                let ns = elapsed.as_secs_f64() * 1e9 / (ITERS as f64 * n as f64);
                let shape = if seq { "id" } else { "rnd" };
                println!("  n={n:<8} stride={stride:<3} {shape:<3} {ns:7.2} ns/record");
            }
        }
    }
}

/// `compare_pk_ordering` per compare and `find_lower_bound_bytes` per probe, in
/// instructions, at the strides an index entry and a PK take.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn pk_compare_bench() {
    use std::hint::black_box;

    const ROWS: usize = 512 * 1024;
    const PROBES: usize = 100_000;
    let counter = gnitz_foundation::perf::Counter::instructions();
    println!("\npk_compare_bench — instructions per compare / per probe ({ROWS} rows):");
    for stride in [2usize, 4, 8, 9, 10, 12, 16, 24] {
        let types: Vec<TypeCode> = match stride {
            2 => vec![TypeCode::U16],
            4 => vec![TypeCode::U32],
            8 => vec![TypeCode::U64],
            9 => vec![TypeCode::U8, TypeCode::U64],
            10 => vec![TypeCode::U16, TypeCode::U64],
            12 => vec![TypeCode::U32, TypeCode::U64],
            16 => vec![TypeCode::U64, TypeCode::U64],
            _ => vec![TypeCode::U64, TypeCode::U64, TypeCode::U64],
        };
        let schema = pk_only_schema(&types);
        assert_eq!(schema.pk_stride(), stride);
        // Ascending keys: the row number in the trailing bytes, a slow counter ahead of it.
        let rows = ROWS.min(1 << (8 * stride.min(3)));
        let mut bb = BatchBuilder::new(&schema);
        for i in 0..rows as u128 {
            let natives: Vec<u128> = match types.len() {
                1 => vec![i],
                2 => vec![(i >> 16) & 0xFF, i],
                _ => vec![i >> 16, 7, i],
            };
            bb.begin_row_natives(&natives, 1);
            bb.end_row();
        }
        let mut batch = bb.finish();
        batch.certify_consolidated();
        let mb = batch.as_mem_batch();
        let mut rng = Rng::new(0xC0FFEE + stride as u64);
        let picks: Vec<(usize, usize)> = (0..PROBES)
            .map(|_| (rng.gen_range(rows as u64) as usize, rng.gen_range(rows as u64) as usize))
            .collect();

        let (acc, cmp) = counter.measure(|| {
            let mut acc = 0usize;
            for &(a, b) in &picks {
                acc += compare_pk_ordering(black_box(mb.get_pk_bytes(a)), black_box(mb.get_pk_bytes(b))) as i8 as usize;
            }
            acc
        });
        black_box(acc);
        let (acc, probe) = counter.measure(|| {
            let mut acc = 0usize;
            for &(a, _) in &picks {
                acc += batch.find_lower_bound_bytes(black_box(mb.get_pk_bytes(a)));
            }
            acc
        });
        black_box(acc);
        println!(
            "  stride={stride:<3} rows={rows:<7} compare {:6.1}  lower_bound {:7.1}",
            cmp as f64 / PROBES as f64,
            probe as f64 / PROBES as f64
        );
    }
}
