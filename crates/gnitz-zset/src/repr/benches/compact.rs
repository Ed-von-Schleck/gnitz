use super::*;
use crate::test_support::map_shard;
use gnitz_wire::PkBuf;
use std::rc::Rc;

/// Compaction over packed inputs that all hold the same keys, so the merge breaks
/// every PK tie on the payload, at several source × guard counts; then with every
/// guard a skeleton.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_compaction_bench() {
    use crate::repr::BatchBuilder;
    use crate::test_support::pk_u64_two_i64_schema;
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const TOTAL: usize = 1 << 20;
    let schema = pk_u64_two_i64_schema();
    let dir = tempfile::tempdir().unwrap();
    let (cycles, instructions) = (Counter::cycles(), Counter::instructions());
    for (sources, guards, skeleton) in [(4, 1, false), (32, 32, false), (64, 64, false), (32, 32, true)] {
        let per = TOTAL / sources;
        let inputs: Vec<Rc<MappedShard>> = (0..sources)
            .map(|s| {
                let mut b = BatchBuilder::new(&schema);
                for i in 0..per {
                    b.begin_row(i as u128, 1);
                    b.put_int(s as u128);
                    b.put_int(3 * i as u128);
                    b.end_row();
                }
                let name = format!("in_{sources}_{guards}_{skeleton}_{s}.db");
                map_shard(&dir.path().join(name), &b.finish())
            })
            .collect();
        let guard_keys: Vec<PkBuf> = (0..guards)
            .map(|g| PkBuf::from_bytes(&((g * per / guards) as u64).to_be_bytes()))
            .collect();
        let inputs: Vec<&MappedShard> = inputs.iter().map(|s| &**s).collect();
        let (((), i), c) = cycles.measure(|| {
            instructions.measure(|| {
                merge_and_route(&inputs, &guard_keys, skeleton, &schema, &mut |_, _, batch| {
                    black_box(batch);
                    Ok(())
                })
                .unwrap()
            })
        });
        println!(
            "{sources} sources x {guards} guards{}: {:.1} instr/row, {:.1} cycles/row",
            if skeleton { ", all skeleton" } else { "" },
            i as f64 / TOTAL as f64,
            c as f64 / TOTAL as f64,
        );
    }
}
