use super::AdhocFold;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{mix, u64_pk_schema};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{AggDescriptor, AggFunc, GroupAggs, TypeCode};

const N: u64 = 1 << 20;
/// A store's scan chunk.
const CHUNK: u64 = 65_536;
/// A store's cap on the groups of one fold.
const GROUP_CAP: usize = 65_536;

/// Instructions retired by one whole fold of `spec` — every chunk's `ranges`, and
/// the finish — measured after one warm-up fold.
fn instructions(
    counter: &Counter,
    schema: &SchemaDescriptor,
    spec: &GroupAggs,
    chunks: &[Batch],
    ranges: &[(usize, usize)],
) -> u64 {
    let fold = || {
        let mut fold = AdhocFold::new(schema, spec, GROUP_CAP).unwrap();
        for c in chunks {
            fold.fold_ranges(c, ranges).unwrap();
        }
        fold.finish()
    };
    std::hint::black_box(fold());
    let (out, instructions) = counter.measure(fold);
    std::hint::black_box(out);
    instructions
}

/// Instructions per row of a whole fold — every chunk and the finish — over
/// `[U64 pk | I64 grp | I64 a | I32 b NULL | F64 c]`, across aggregate sets and
/// group counts: grouped by `grp` into a few groups and into nearly the cap, and
/// by no column, which is the global fold.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn adhoc_fold_bench() {
    let schema = u64_pk_schema(&[
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::I32, true),
        SchemaColumn::new(TypeCode::F64, false),
    ]);
    let agg = |col_idx: u32, agg_op: AggFunc| AggDescriptor { col_idx, agg_op };
    let agg_sets: [(&str, Vec<AggDescriptor>); 10] = [
        ("count", vec![AggDescriptor::COUNT_STAR]),
        ("sum i32n", vec![agg(3, AggFunc::Sum)]),
        ("sum f64", vec![agg(4, AggFunc::Sum)]),
        ("cnn i32n", vec![agg(3, AggFunc::CountNonNull)]),
        ("min i64", vec![agg(2, AggFunc::Min)]),
        ("max i32n", vec![agg(3, AggFunc::Max)]),
        ("count+sum", vec![AggDescriptor::COUNT_STAR, agg(2, AggFunc::Sum)]),
        (
            "count+3sum+cnn",
            vec![
                AggDescriptor::COUNT_STAR,
                agg(2, AggFunc::Sum),
                agg(3, AggFunc::Sum),
                agg(4, AggFunc::Sum),
                agg(3, AggFunc::CountNonNull),
            ],
        ),
        (
            "count+sum+min+max",
            vec![
                AggDescriptor::COUNT_STAR,
                agg(2, AggFunc::Sum),
                agg(2, AggFunc::Min),
                agg(3, AggFunc::Max),
            ],
        ),
        // The column kernel over a PK column's OPK cells.
        ("pk min+sum", vec![agg(0, AggFunc::Min), agg(0, AggFunc::Sum)]),
    ];
    let counter = Counter::instructions();
    for groups in [0u64, 16, 60_000] {
        let chunks: Vec<Batch> = (0..N / CHUNK)
            .map(|c| {
                let mut b = BatchBuilder::new(&schema);
                for i in c * CHUNK..(c + 1) * CHUNK {
                    b.begin_row(i as u128, 1);
                    b.put_int((mix(i) >> 20) as u128 % groups.max(1) as u128);
                    b.put_int(mix(i ^ 7) as i64 as u128);
                    b.put_opt_int((i % 5 != 0).then(|| mix(i ^ 9) as u32 as u128));
                    b.put_float((mix(i) >> 11) as f64 / 3.0);
                    b.end_row();
                }
                b.finish()
            })
            .collect();
        let group_cols = if groups == 0 { vec![] } else { vec![1] };
        for (name, aggs) in &agg_sets {
            let spec = GroupAggs {
                group_cols: group_cols.clone(),
                aggs: aggs.clone(),
            };
            let instructions = instructions(&counter, &schema, &spec, &chunks, &[(0, CHUNK as usize)]);
            println!(
                "adhoc_fold_bench groups={groups:<6} {name:<18} {:>6.1} instr/row",
                instructions as f64 / N as f64
            );
        }
    }
}

/// Shapes `adhoc_fold_bench` has no cell for, over
/// `[U64 k0, U32 k1, U64 k2 (PK) | I64 grp | I64 a | I32 b NULL-dense | F64 c | I16 d NULL-sparse]`.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn adhoc_fold_shapes_bench() {
    let c = SchemaColumn::new;
    let schema = SchemaDescriptor::new(
        &[
            c(TypeCode::U64, false),
            c(TypeCode::U32, false),
            c(TypeCode::U64, false),
            c(TypeCode::I64, false),
            c(TypeCode::I64, false),
            c(TypeCode::I32, true),
            c(TypeCode::F64, false),
            c(TypeCode::I16, true),
        ],
        &[0, 1, 2],
    );
    let agg = |col_idx: u32, agg_op: AggFunc| AggDescriptor { col_idx, agg_op };
    use AggFunc::{CountNonNull as Cnn, Max, Min, Sum};
    let agg_sets: Vec<(&str, Vec<AggDescriptor>)> = vec![
        ("sum pk1(u32 mid)", vec![agg(1, Sum)]),
        ("min pk1(u32 mid)", vec![agg(1, Min)]),
        ("max pk2(u64 last)", vec![agg(2, Max)]),
        ("sum pk2(u64 last)", vec![agg(2, Sum)]),
        ("sum b(null-dense)", vec![agg(5, Sum)]),
        ("min b(null-dense)", vec![agg(5, Min)]),
        ("cnn b(null-dense)", vec![agg(5, Cnn)]),
        ("min d(i16 sparse)", vec![agg(7, Min)]),
        ("min c(f64)", vec![agg(6, Min)]),
        ("min a", vec![agg(4, Min)]),
        ("max a", vec![agg(4, Max)]),
        (
            "9 aggs",
            vec![
                AggDescriptor::COUNT_STAR,
                agg(4, Sum),
                agg(5, Sum),
                agg(6, Sum),
                agg(5, Cnn),
                agg(4, Min),
                agg(5, Max),
                agg(6, Min),
                agg(4, Max),
            ],
        ),
        (
            "12 aggs",
            vec![
                AggDescriptor::COUNT_STAR,
                agg(4, Sum),
                agg(5, Sum),
                agg(6, Sum),
                agg(5, Cnn),
                agg(4, Min),
                agg(5, Max),
                agg(6, Min),
                agg(4, Max),
                agg(7, Sum),
                agg(7, Min),
                agg(1, Max),
            ],
        ),
    ];
    let counter = Counter::instructions();
    for (wname, weight) in [("w=+1", false), ("w=mixed", true)] {
        for groups in [0u64, 16, 60_000] {
            let chunks: Vec<Batch> = (0..N / CHUNK)
                .map(|ch| {
                    let mut b = BatchBuilder::new(&schema);
                    for i in ch * CHUNK..(ch + 1) * CHUNK {
                        b.begin_row_natives(
                            &[i as u128, (mix(i ^ 3) >> 40) as u32 as u128, mix(i ^ 5) as u128],
                            if weight && mix(i) >> 61 < 3 {
                                -1
                            } else if weight {
                                2
                            } else {
                                1
                            },
                        );
                        b.put_int((mix(i) >> 20) as u128 % groups.max(1) as u128);
                        b.put_int(mix(i ^ 7) as i64 as u128);
                        b.put_opt_int((mix(i ^ 11) >> 60 == 0).then(|| mix(i ^ 9) as u32 as u128));
                        b.put_float((mix(i) >> 11) as f64 / 3.0);
                        b.put_opt_int((i % 17 != 0).then(|| mix(i ^ 13) as u16 as u128));
                        b.end_row();
                    }
                    b.finish()
                })
                .collect();
            let group_cols = if groups == 0 { vec![] } else { vec![3] };
            for (name, aggs) in &agg_sets {
                let spec = GroupAggs {
                    group_cols: group_cols.clone(),
                    aggs: aggs.clone(),
                };
                let instructions = instructions(&counter, &schema, &spec, &chunks, &[(0, CHUNK as usize)]);
                println!(
                    "adhoc_fold_shapes_bench {wname:<8} groups={groups:<6} {name:<18} {:>6.1} instr/row",
                    instructions as f64 / N as f64
                );
            }
        }
    }
}

/// Instructions per surviving row of a fold grouped by a packed key — two
/// integer columns, or one string — over survivor ranges of each density.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn adhoc_fold_sparse_bench() {
    let c = SchemaColumn::new;
    let ints = SchemaDescriptor::new(
        &[
            c(TypeCode::U64, false),
            c(TypeCode::I64, false),
            c(TypeCode::I32, false),
            c(TypeCode::I64, false),
        ],
        &[0],
    );
    let strs = SchemaDescriptor::new(
        &[
            c(TypeCode::U64, false),
            c(TypeCode::String, false),
            c(TypeCode::I64, false),
        ],
        &[0],
    );
    let counter = Counter::instructions();
    for (label, schema, group_cols, sum_col) in [("i64+i32", &ints, vec![1u32, 2], 3u32), ("string", &strs, vec![1], 2)]
    {
        let chunks: Vec<Batch> = (0..N / CHUNK)
            .map(|ch| {
                let mut b = BatchBuilder::new(schema);
                for i in ch * CHUNK..(ch + 1) * CHUNK {
                    b.begin_row(i as u128, 1);
                    let g = mix(i) % 1000;
                    if group_cols.len() == 2 {
                        b.put_int(g as u128);
                        b.put_int((g * 7) as u32 as u128);
                    } else {
                        b.put_string(&format!("group-key-string-{g:08}"));
                    }
                    b.put_int(mix(i ^ 7) as i64 as u128);
                    b.end_row();
                }
                b.finish()
            })
            .collect();
        let spec = GroupAggs {
            group_cols: group_cols.clone(),
            aggs: vec![
                AggDescriptor::COUNT_STAR,
                AggDescriptor { agg_op: AggFunc::Sum, col_idx: sum_col },
            ],
        };
        let n = CHUNK as usize;
        for (density, ranges) in [
            ("whole", vec![(0, n)]),
            (
                "runs of 8 in 64",
                (0..n).step_by(64).map(|s| (s, s + 8)).collect::<Vec<_>>(),
            ),
            ("1 row in 2", (0..n).step_by(2).map(|s| (s, s + 1)).collect()),
            ("1 row in 4", (0..n).step_by(4).map(|s| (s, s + 1)).collect()),
            ("1 row in 8", (0..n).step_by(8).map(|s| (s, s + 1)).collect()),
            ("1 row in 16", (0..n).step_by(16).map(|s| (s, s + 1)).collect()),
            ("1 row in 32", (0..n).step_by(32).map(|s| (s, s + 1)).collect()),
            ("1 row in 64", (0..n).step_by(64).map(|s| (s, s + 1)).collect()),
            ("runs of 2 in 8", (0..n).step_by(8).map(|s| (s, s + 2)).collect()),
            ("runs of 4 in 8", (0..n).step_by(8).map(|s| (s, s + 4)).collect()),
            ("runs of 16 in 32", (0..n).step_by(32).map(|s| (s, s + 16)).collect()),
        ] {
            let kept: usize = ranges.iter().map(|&(s, e)| e - s).sum::<usize>() * chunks.len();
            let instructions = instructions(&counter, schema, &spec, &chunks, &ranges);
            println!(
                "adhoc_fold_sparse_bench {label:<8} {density:<16} {:>7.1} instr/surviving row",
                instructions as f64 / kept as f64
            );
        }
    }
}

/// Instructions per group of the finish alone — the fold before it untimed — at
/// nearly the group cap, over
/// `[U64 pk | I64 grp | STRING sgrp | I64 a | I32 b NULL | STRING s NULL]`,
/// grouped by an integer column and by a string one.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn adhoc_fold_finish_bench() {
    let c = SchemaColumn::new;
    let schema = u64_pk_schema(&[
        c(TypeCode::I64, false),
        c(TypeCode::String, false),
        c(TypeCode::I64, false),
        c(TypeCode::I32, true),
        c(TypeCode::String, true),
    ]);
    const GROUPS: u64 = 60_000;
    let chunks: Vec<Batch> = (0..N / CHUNK)
        .map(|ch| {
            let mut b = BatchBuilder::new(&schema);
            for i in ch * CHUNK..(ch + 1) * CHUNK {
                let g = (mix(i) >> 20) % GROUPS;
                b.begin_row(i as u128, 1);
                b.put_int(g as u128);
                b.put_string(&format!("group-key-string-{g:08}"));
                b.put_int(mix(i ^ 7) as i64 as u128);
                b.put_opt_int((i % 5 != 0).then(|| mix(i ^ 9) as u32 as u128));
                match i % 7 {
                    0 => b.put_null(),
                    _ => b.put_string(&format!("value-string-{:016x}", mix(i ^ 11))),
                }
                b.end_row();
            }
            b.finish()
        })
        .collect();
    let agg = |col_idx: u32, agg_op: AggFunc| AggDescriptor { col_idx, agg_op };
    let agg_sets: [(&str, Vec<AggDescriptor>); 3] = [
        ("count", vec![AggDescriptor::COUNT_STAR]),
        (
            "count+sum+min+max",
            vec![
                AggDescriptor::COUNT_STAR,
                agg(3, AggFunc::Sum),
                agg(3, AggFunc::Min),
                agg(4, AggFunc::Max),
            ],
        ),
        ("min+max string", vec![agg(5, AggFunc::Min), agg(5, AggFunc::Max)]),
    ];
    let counter = Counter::instructions();
    for (key, group_col) in [("grp i64", 1u32), ("grp string", 2)] {
        for (name, aggs) in &agg_sets {
            let spec = GroupAggs {
                group_cols: vec![group_col],
                aggs: aggs.clone(),
            };
            let folded = || {
                let mut fold = AdhocFold::new(&schema, &spec, GROUP_CAP).unwrap();
                for c in &chunks {
                    fold.fold_ranges(c, &[(0, CHUNK as usize)]).unwrap();
                }
                fold
            };
            std::hint::black_box(folded().finish());
            let fold = folded();
            let (out, instructions) = counter.measure(|| fold.finish());
            let groups = out.len();
            std::hint::black_box(out);
            println!(
                "adhoc_fold_finish_bench {key:<10} {name:<18} {:>6.1} instr/group ({groups} groups)",
                instructions as f64 / groups as f64
            );
        }
    }
}
