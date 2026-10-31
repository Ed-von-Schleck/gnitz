use super::*;
use crate::repr::merge::run_merge;
use crate::repr::BatchBuilder;
use crate::schema::SchemaColumn;
use crate::test_support::{
    map_coded_shard, map_shard, mix, pk_payload_schema, pk_u64_two_i64_schema, prose, u64_pk_schema,
};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{PkBuf, TypeCode};
use std::hint::black_box;
use std::rc::Rc;

/// Compaction over packed inputs that all hold the same keys, so the merge breaks
/// every PK tie on the payload, at several source × guard counts; then with every
/// guard a skeleton.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_compaction_bench() {
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
        let mut merge = GuardMerge::begin(inputs, skeleton).unwrap();
        let ((rows, i), c) = cycles.measure(|| {
            instructions.measure(|| {
                (0..guard_keys.len())
                    .filter_map(|g| {
                        let until = guard_keys.get(g + 1).map(PkBuf::pk_bytes);
                        merge.next(until, None)
                    })
                    .map(|run| black_box(run).row_count())
                    .sum::<usize>()
            })
        });
        // Every source holds every key, each under its own payload.
        assert_eq!(rows, if skeleton { per } else { TOTAL });
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
/// by how the rows are dealt to the sources: scattered, or in ascending blocks
/// as the spills of an ascending key are. The merge alone is the run walk
/// without the output batch.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn distinct_compaction_bench() {
    const TOTAL: u64 = 1 << 20;
    let dir = tempfile::tempdir().unwrap();
    let instructions = Counter::instructions();
    // Label, PK columns, the column the key is in.
    let pks = [
        ("8-byte PK", 1, 0),
        ("24-byte PK, leading column", 3, 0),
        ("24-byte PK, trailing column", 3, 2),
    ];
    // Label, source count, the source key `k` lands in.
    type Deal = (&'static str, u64, fn(u64) -> u64);
    let deals: [Deal; 4] = [
        ("2 equal sources", 2, |k| (mix(k) >> 20) % 2),
        ("5 equal sources", 5, |k| (mix(k) >> 20) % 5),
        ("1 large and 4 small sources", 5, |k| {
            let h = mix(k) >> 20;
            if h % 16 < 12 {
                0
            } else {
                1 + h % 4
            }
        }),
        ("5 ascending blocks", 5, |k| k * 5 / TOTAL),
    ];
    for (pk, (pk_label, pk_cols, key_col)) in pks.into_iter().enumerate() {
        let schema = pk_payload_schema(&vec![TypeCode::U64; pk_cols]);
        for (deal, (deal_label, sources, source_of)) in deals.into_iter().enumerate() {
            let mut builders: Vec<BatchBuilder> = (0..sources).map(|_| BatchBuilder::new(&schema)).collect();
            for k in 0..TOTAL {
                let mut key = vec![0u8; pk_cols * 8];
                key[key_col * 8..][..8].copy_from_slice(&k.to_be_bytes());
                let b = &mut builders[source_of(k) as usize];
                b.begin_row_bytes(&key, 1);
                b.put_int(mix(k) as u128);
                b.end_row();
            }
            let inputs: Vec<Rc<MappedShard>> = (0..)
                .zip(builders)
                .map(|(s, b)| map_shard(&dir.path().join(format!("{pk}_{deal}_{s}.db")), &b.finish()))
                .collect();
            let mut merge = GuardMerge::begin(inputs.clone(), false).unwrap();
            let ((), whole) = instructions.measure(|| {
                let run = merge.next(None, None).unwrap();
                let batch = run.plain().unwrap();
                assert_eq!(batch.len() as u64, TOTAL);
                black_box(batch);
            });
            // Over the decoded windows `GuardMerge::next` merges, so that the difference
            // is the output batch.
            let (rows, merge) = instructions.measure(|| {
                let windows: Vec<_> = inputs.iter().map(|s| s.window(0..s.row_count())).collect();
                let rows = run_merge(&windows, &schema, windows.iter().map(|w| 0..w.row_count()));
                black_box(rows).iter().map(|r| r.2 as u64).sum::<u64>()
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

/// Coded compaction of rows with three long STRING payload values each, over
/// sources coded under the merge's own codebook, so their codes are kept.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn coded_compaction_bench() {
    const TOTAL: usize = 1 << 18;
    const SOURCES: usize = 4;
    let string = SchemaColumn::new(TypeCode::String, false);
    let schema = u64_pk_schema(&[string, string, string]);
    let dir = tempfile::tempdir().unwrap();
    let instructions = Counter::instructions();
    let batches: Vec<Batch> = (0..SOURCES)
        .map(|s| {
            let mut b = BatchBuilder::new(&schema);
            for k in (s..TOTAL).step_by(SOURCES) {
                b.begin_row(k as u128, 1);
                for c in 0..3 {
                    b.put_string(&prose(k * 3 + c));
                }
                b.end_row();
            }
            b.finish()
        })
        .collect();
    let book = Codebook::for_batch(None, &batches[0]).unwrap();
    let inputs: Vec<Rc<MappedShard>> = (0..)
        .zip(&batches)
        .map(|(s, b)| map_coded_shard(&dir.path().join(format!("{s}.db")), b, &book))
        .collect();
    let mut merge = GuardMerge::begin(inputs, false).unwrap();
    let (rows, i) = instructions.measure(|| black_box(merge.next(None, Some(&book)).unwrap()).row_count());
    assert_eq!(rows, TOTAL);
    println!(
        "{SOURCES} coded sources, 3 long strings: {:.1} instr/row",
        i as f64 / TOTAL as f64
    );
}
