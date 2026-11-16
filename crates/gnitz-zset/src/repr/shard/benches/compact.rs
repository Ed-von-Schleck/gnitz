use super::*;
use crate::repr::BatchBuilder;
use crate::repr::merge::run_merge;
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
        let mut merge = GuardMerge::begin(inputs, guard_keys[1..].to_vec(), skeleton).unwrap();
        let ((rows, i), c) = cycles.measure(|| {
            instructions.measure(|| {
                let mut rows = 0;
                while !merge.done() {
                    rows += merge.next(None).map_or(0, |run| black_box(run).row_count());
                }
                rows
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
/// as the spills of an ascending key are, or mostly to one large source with a
/// scattered few elsewhere, as a level's shard folded with a small delta is.
/// The merge alone is the run walk without the output batch.
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
    let deals: [Deal; 6] = [
        ("2 equal sources", 2, |k| (mix(k) >> 20) % 2),
        ("5 equal sources", 5, |k| (mix(k) >> 20) % 5),
        ("1 large and 4 small sources", 5, |k| {
            let h = mix(k) >> 20;
            if h % 16 < 12 { 0 } else { 1 + h % 4 }
        }),
        ("5 ascending blocks", 5, |k| k * 5 / TOTAL),
        ("1 large and 1 tiny source", 2, |k| {
            u64::from((mix(k) >> 20).is_multiple_of(64))
        }),
        ("1 large and 4 tiny sources", 5, |k| {
            let h = mix(k) >> 20;
            if h.is_multiple_of(64) { 1 + (h >> 6) % 4 } else { 0 }
        }),
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
            let mut merge = GuardMerge::begin(inputs.clone(), Vec::new(), false).unwrap();
            let ((), whole) = instructions.measure(|| {
                let run = merge.next(None).unwrap();
                let batch = run.plain().unwrap();
                assert_eq!(batch.len() as u64, TOTAL);
                black_box(batch);
            });
            // Over the decoded windows `GuardMerge::next` merges, so that the difference
            // is the output batch.
            let (rows, merge) = instructions.measure(|| {
                let windows: Vec<_> = inputs.iter().map(|s| s.window(0..s.row_count())).collect();
                let rows = run_merge(&windows, &schema, windows.iter().map(|w| 0..w.row_count()), true);
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
    let mut merge = GuardMerge::begin(inputs, Vec::new(), false).unwrap();
    let (rows, i) = instructions.measure(|| black_box(merge.next(Some(&book)).unwrap()).row_count());
    assert_eq!(rows, TOTAL);
    println!(
        "{SOURCES} coded sources, 3 long strings: {:.1} instr/row",
        i as f64 / TOTAL as f64
    );
}

/// Compaction of disjoint ascending key blocks, the shape the spills of an
/// ascending key have, each window taken whole: rows of two i64 payload values
/// and three long STRING ones, one of them a value repeated across the blocks,
/// over sources coded under the merge's own codebook; then the same keys as
/// skeleton shards. With the bytes of the run's heap.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn disjoint_coded_compaction_bench() {
    const TOTAL: usize = 1 << 18;
    const SOURCES: usize = 4;
    let (int, string) = (
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::String, false),
    );
    let schema = u64_pk_schema(&[int, string, string, int, string]);
    let dir = tempfile::tempdir().unwrap();
    let instructions = Counter::instructions();
    let per = TOTAL / SOURCES;
    let batches: Vec<Batch> = (0..SOURCES)
        .map(|s| {
            let mut b = BatchBuilder::new(&schema);
            for k in s * per..(s + 1) * per {
                b.begin_row(k as u128, 1);
                b.put_int(mix(k as u64) as u128 >> 40);
                b.put_string(&prose(k * 3));
                b.put_string(&prose(k * 3 + 1));
                b.put_int(k as u128 * 7);
                b.put_string(&prose(k % 64));
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
    let mut merge = GuardMerge::begin(inputs.clone(), Vec::new(), false).unwrap();
    let (heap, i) = instructions.measure(|| {
        let run = black_box(merge.next(Some(&book)).unwrap());
        assert_eq!(run.row_count(), TOTAL);
        run.batch.blob().len()
    });
    println!(
        "{SOURCES} disjoint coded sources, 2 i64 and 3 long strings: {:.1} instr/row, {heap} heap bytes",
        i as f64 / TOTAL as f64
    );

    let skeletons: Vec<Rc<MappedShard>> = (0..)
        .zip(inputs)
        .map(|(s, input)| {
            let run = GuardMerge::begin(vec![input], Vec::new(), true)
                .unwrap()
                .next(None)
                .unwrap();
            let path = dir.path().join(format!("skeleton_{s}.db"));
            let path = path.to_str().unwrap();
            (run.batch
                .write_shard_over(&run.batch, path, ShardWriteOpts::default(), true))
            .unwrap();
            Rc::new(MappedShard::open(path, &schema).unwrap())
        })
        .collect();
    let mut merge = GuardMerge::begin(skeletons, Vec::new(), false).unwrap();
    let (rows, i) = instructions.measure(|| black_box(merge.next(Some(&book)).unwrap()).row_count());
    assert_eq!(rows, TOTAL);
    println!(
        "{SOURCES} disjoint skeleton sources: {:.1} instr/row",
        i as f64 / TOTAL as f64
    );
}

/// [`disjoint_coded_compaction_bench`]'s sources written uncoded and merged
/// with no codebook: once with each window a whole shard, then under fences
/// that cut each shard seven eighths of the way through, so every run takes
/// the large part of one shard and the small rest of the one before it. Per
/// row, the merge alone, then the merge with each run written as a shard.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn disjoint_plain_compaction_bench() {
    const TOTAL: usize = 1 << 18;
    const SOURCES: usize = 4;
    let (int, string) = (
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::String, false),
    );
    let schema = u64_pk_schema(&[int, string, string, int, string]);
    let dir = tempfile::tempdir().unwrap();
    let instructions = Counter::instructions();
    let per = TOTAL / SOURCES;
    let inputs: Vec<Rc<MappedShard>> = (0..SOURCES)
        .map(|s| {
            let mut b = BatchBuilder::new(&schema);
            for k in s * per..(s + 1) * per {
                b.begin_row(k as u128, 1);
                b.put_int(mix(k as u64) as u128 >> 40);
                b.put_string(&prose(k * 3));
                b.put_string(&prose(k * 3 + 1));
                b.put_int(k as u128 * 7);
                b.put_string(&prose(k % 64));
                b.end_row();
            }
            map_shard(&dir.path().join(format!("{s}.db")), &b.finish())
        })
        .collect();
    let cut: Vec<PkBuf> = (0..SOURCES)
        .map(|s| PkBuf::from_bytes(&((s * per + per * 7 / 8) as u64).to_be_bytes()))
        .collect();
    for (label, fences) in [("whole windows", Vec::new()), ("cut windows", cut)] {
        let mut merge = GuardMerge::begin(inputs.clone(), fences.clone(), false).unwrap();
        let ((rows, heap), merged) = instructions.measure(|| {
            let (mut rows, mut heap) = (0, 0);
            while !merge.done() {
                if let Some(run) = merge.next(None) {
                    let run = black_box(run);
                    rows += run.row_count();
                    heap += run.batch.blob().len();
                }
            }
            (rows, heap)
        });
        assert_eq!(rows, TOTAL);
        let mut merge = GuardMerge::begin(inputs.clone(), fences, false).unwrap();
        let mut written = 0;
        let (rows, with_write) = instructions.measure(|| {
            let mut rows = 0;
            while !merge.done() {
                if let Some(run) = merge.next(None) {
                    let path = dir.path().join(format!("out_{label}_{written}.db"));
                    run.write_as_shard(path.to_str().unwrap(), ShardWriteOpts::default())
                        .unwrap();
                    rows += run.row_count();
                    written += 1;
                }
            }
            rows
        });
        assert_eq!(rows, TOTAL);
        println!(
            "{SOURCES} disjoint plain sources, {label}: {:.1} instr/row, written as shards {:.1}, {heap} heap bytes",
            merged as f64 / TOTAL as f64,
            with_write as f64 / TOTAL as f64,
        );
    }
}
