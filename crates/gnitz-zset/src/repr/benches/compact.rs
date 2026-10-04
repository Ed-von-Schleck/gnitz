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

/// Compaction over inputs that share no key, the shape a fold of scattered
/// spills has: every match is settled on the PK. By what the PK is — 8 bytes,
/// then 24 that differ in their leading or only in their trailing column — and
/// by how the rows are dealt to the sources. The merge alone is the run walk
/// without the output batch.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn distinct_compaction_bench() {
    use crate::repr::BatchBuilder;
    use crate::test_support::pk_payload_schema;
    use gnitz_foundation::perf::Counter;
    use gnitz_wire::TypeCode;
    use std::hint::black_box;
    const TOTAL: u64 = 1 << 20;
    let dir = tempfile::tempdir().unwrap();
    let instructions = Counter::instructions();
    let mix = |k: u64| k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    // Label, PK columns, the column the key is in.
    let pks = [
        ("8-byte PK", 1, 0),
        ("24-byte PK, leading column", 3, 0),
        ("24-byte PK, trailing column", 3, 2),
    ];
    // Label, source count, the source a row lands in.
    type Deal = (&'static str, u64, fn(u64) -> u64);
    let deals: [Deal; 3] = [
        ("2 equal sources", 2, |h| h % 2),
        ("5 equal sources", 5, |h| h % 5),
        ("1 large and 4 small sources", 5, |h| {
            if h % 16 < 12 {
                0
            } else {
                1 + h % 4
            }
        }),
    ];
    for (pk, (pk_label, pk_cols, key_col)) in pks.into_iter().enumerate() {
        let schema = pk_payload_schema(&vec![TypeCode::U64; pk_cols]);
        for (deal, (deal_label, sources, source_of)) in deals.into_iter().enumerate() {
            let mut builders: Vec<BatchBuilder> = (0..sources).map(|_| BatchBuilder::new(&schema)).collect();
            for k in 0..TOTAL {
                let mut key = vec![0u8; pk_cols * 8];
                key[key_col * 8..][..8].copy_from_slice(&k.to_be_bytes());
                let b = &mut builders[source_of(mix(k) >> 20) as usize];
                b.begin_row_bytes(&key, 1);
                b.put_int(mix(k) as u128);
                b.end_row();
            }
            let inputs: Vec<Rc<MappedShard>> = (0..)
                .zip(builders)
                .map(|(s, b)| map_shard(&dir.path().join(format!("{pk}_{deal}_{s}.db")), &b.finish()))
                .collect();
            let inputs: Vec<&MappedShard> = inputs.iter().map(|s| &**s).collect();
            let guard_keys = [PkBuf::zeroed(schema.pk_stride())];
            let ((), whole) = instructions.measure(|| {
                merge_and_route(&inputs, &guard_keys, false, &schema, &mut |_, _, batch| {
                    assert_eq!(batch.len() as u64, TOTAL);
                    black_box(batch);
                    Ok(())
                })
                .unwrap()
            });
            let (rows, merge) = instructions.measure(|| {
                let mut rows = 0u64;
                run_merge(&inputs, &schema, |src, row, w| {
                    rows += black_box((src, row, w)).2 as u64
                });
                rows
            });
            assert_eq!(rows, TOTAL);
            println!(
                "{pk_label}, {deal_label}: {:.1} instr/row, the merge alone {:.1}",
                whole as f64 / TOTAL as f64,
                merge as f64 / TOTAL as f64,
            );
        }
    }
}
