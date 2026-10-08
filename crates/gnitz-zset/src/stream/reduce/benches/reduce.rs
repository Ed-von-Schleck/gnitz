use super::plan::ReducePlan;
use super::tests::Harness;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{mix, pk_payload_schema};
use gnitz_foundation::perf::Counter;
use gnitz_wire::{AggDescriptor, AggFunc};

/// A U64 PK, the `group` columns, then one `value` column.
fn grouped_schema(group: &[SchemaColumn], value: SchemaColumn) -> SchemaDescriptor {
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend_from_slice(group);
    cols.push(value);
    SchemaDescriptor::new(&cols, &[0])
}

/// The reduce of `aggs` over `schema`'s last column, with the COUNT(*) a
/// circuit reduce carries.
fn plan(schema: &SchemaDescriptor, group: &[u32], aggs: &[AggFunc]) -> ReducePlan {
    let col_idx = schema.num_columns() as u32 - 1;
    let aggs: Vec<AggDescriptor> = aggs
        .iter()
        .map(|&agg_op| AggDescriptor { col_idx, agg_op })
        .chain([AggDescriptor::COUNT_STAR])
        .collect();
    ReducePlan::from_wire(schema, group, &aggs, false).unwrap()
}

/// Instructions per delta row of the value-index entries a MIN/MAX reduce
/// derives from its delta, per image kind: a scalar, a fixed wide value, a
/// string, a column holding NULLs, and two extremes at once.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reduce_index_batch_bench() {
    use AggFunc::{Max, Min};
    const N: u64 = 65_536;
    let counter = Counter::instructions();
    type Put = fn(&mut BatchBuilder, u64);
    let cases: [(&str, SchemaColumn, &[AggFunc], Put); 6] = [
        ("I64 MIN", SchemaColumn::new(TypeCode::I64, false), &[Min], |b, i| {
            b.put_u64(mix(i))
        }),
        ("U128 MAX", SchemaColumn::new(TypeCode::U128, false), &[Max], |b, i| {
            b.put_int((mix(i) as u128) << 64 | i as u128)
        }),
        (
            "STRING MIN",
            SchemaColumn::new(TypeCode::String, false),
            &[Min],
            |b, i| b.put_string(&format!("{:040}", mix(i))),
        ),
        // Every image escapes a `0x00`.
        (
            "STRING MIN, NULs",
            SchemaColumn::new(TypeCode::String, false),
            &[Min],
            |b, i| b.put_string(&format!("{:020}\0{:019}", mix(i), i)),
        ),
        (
            "nullable I64 MIN",
            SchemaColumn::new(TypeCode::I64, true),
            &[Min],
            |b, i| b.put_opt_int((i % 4 != 0).then_some(mix(i) as u128)),
        ),
        (
            "I64 MIN and MAX",
            SchemaColumn::new(TypeCode::I64, false),
            &[Min, Max],
            |b, i| b.put_u64(mix(i)),
        ),
    ];
    let u32_group: &[SchemaColumn] = &[SchemaColumn::new(TypeCode::U32, false)];
    let nullable_pair: &[SchemaColumn] = &[SchemaColumn::new(TypeCode::I64, true); 2];
    let grouped = cases
        .iter()
        .map(|&(label, value, aggs, put)| (label.to_string(), u32_group, value, aggs, put));
    // The first case again under a group set too wide for a packed key.
    let folded = cases[..1]
        .iter()
        .map(|&(label, value, aggs, put)| (format!("{label}, 2 groups"), nullable_pair, value, aggs, put));
    for (label, group, value, aggs, put) in grouped.chain(folded) {
        let schema = grouped_schema(group, value);
        let mut b = BatchBuilder::new(&schema);
        for i in 0..N {
            b.begin_row(i as u128, 1);
            for _ in group {
                b.put_int((i % 4096) as u128);
            }
            put(&mut b, i);
            b.end_row();
        }
        let mut delta = b.finish();
        delta.certify_consolidated();
        let group_cols: Vec<u32> = (1..=group.len() as u32).collect();
        let plan = plan(&schema, &group_cols, aggs);
        // The first pass takes the pool's first allocations.
        let [_, (entries, instructions)] = [(); 2].map(|()| counter.measure(|| plan.index_batch(&delta).unwrap()));
        println!(
            "reduce_index_batch_bench {label:<17} {:6.1} instr/row ({} entries)",
            instructions as f64 / N as f64,
            entries.count
        );
    }
}

/// One epoch over `delta`: the instructions per delta row of `op_reduce` alone,
/// the index entries the VM adds ahead of it left out.
fn epoch(counter: &Counter, h: &mut Harness, delta: &Batch) -> f64 {
    h.index(delta);
    let (out, instructions) = counter.measure(|| h.reduce(delta));
    h.trace_out.ingest(out);
    instructions as f64 / delta.count as f64
}

/// Instructions per delta row of `op_reduce` per shape, over three epochs: an
/// insert against an empty trace, a second insert into the same groups, and the
/// retraction of the first.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_bench() {
    use AggFunc::{Min, Sum};
    const N: u64 = 1 << 18;
    let counter = Counter::instructions();
    let run = |label: &str, schema: &SchemaDescriptor, group: &[u32], agg: AggFunc, make: &dyn Fn(u64) -> Batch| {
        let mut h = Harness::new(plan(schema, group, &[agg]));
        let (d1, d2) = (h.fold(make(1)), h.fold(make(2)));
        let retraction = h.fold(d1.clone().negated());
        let [empty, populated, retract] = [&d1, &d2, &retraction].map(|d| epoch(&counter, &mut h, d));
        println!(
            "op_reduce_bench {label:<24} empty trace {empty:7.1}, populated {populated:7.1}, \
             retraction {retract:7.1} instr/row"
        );
    };

    // Unsorted rows over a `grouped_schema`: every group column holds `grp` of
    // the row, a nullable one NULL on every 16th row.
    let scattered = |schema: &SchemaDescriptor, salt: u64, grp: &dyn Fn(u64) -> u64| {
        let group = &schema.columns()[1..schema.num_columns() - 1];
        let mut bb = BatchBuilder::new(schema);
        for i in 0..N {
            bb.begin_row(mix(i + salt * N) as u128, 1);
            for (c, col) in group.iter().enumerate() {
                let g = grp(i) & (u64::MAX >> (64 - 8 * col.size()));
                bb.put_opt_int((!col.nullable || i % 16 != c as u64).then_some(g as u128));
            }
            bb.put_u64(mix(i ^ salt));
            bb.end_row();
        }
        bb.finish()
    };
    // PK-sorted rows over one or two U64 PK columns and one I64 value, certified
    // consolidated: 16 rows per leading PK column value.
    let sorted = |schema: &SchemaDescriptor, salt: u64| {
        let mut bb = BatchBuilder::new(schema);
        for i in 0..N {
            match schema.pk_cols().len() {
                1 => bb.begin_row(i as u128, 1),
                _ => bb.begin_row_natives(&[(i / 16) as u128, (i % 16) as u128], 1),
            }
            bb.put_u64(i + salt);
            bb.end_row();
        }
        let mut b = bb.finish();
        b.certify_consolidated();
        b
    };

    let value = SchemaColumn::new(TypeCode::I64, false);
    let one = grouped_schema(&[SchemaColumn::new(TypeCode::I64, false)], value);
    let narrow_nullable = grouped_schema(&[SchemaColumn::new(TypeCode::I32, true)], value);
    let two_nullable = grouped_schema(&[SchemaColumn::new(TypeCode::I64, true); 2], value);
    let single_pk = pk_payload_schema(&[TypeCode::U64]);
    let compound_pk = pk_payload_schema(&[TypeCode::U64, TypeCode::U64]);

    // The delta's group runs, long enough to fold as ranges: linear, and with an
    // extreme to step.
    run("leading_pk_col_sum", &compound_pk, &[0], Sum, &|s| {
        sorted(&compound_pk, s)
    });
    run("leading_pk_col_min", &compound_pk, &[0], Min, &|s| {
        sorted(&compound_pk, s)
    });
    // One run of every row.
    run("ungrouped_sum", &one, &[], Sum, &|s| scattered(&one, s, &|_| 0));
    // Runs of one row: numbered off the delta's own order.
    run("source_pk_sum", &single_pk, &[0], Sum, &|s| sorted(&single_pk, s));
    // Unsorted groups, numbered by sorting below four rows a group and by
    // hashing from there.
    run("keyed_distinct_sum", &one, &[1], Sum, &|s| {
        scattered(&one, s, &|i| mix(i ^ 0x55))
    });
    run("keyed_4_per_group_sum", &one, &[1], Sum, &|s| {
        scattered(&one, s, &|i| mix(i % (N / 4)))
    });
    run("keyed_256_groups_sum", &one, &[1], Sum, &|s| {
        scattered(&one, s, &|i| mix(i) % 256)
    });
    run("keyed_256_groups_min", &one, &[1], Min, &|s| {
        scattered(&one, s, &|i| mix(i) % 256)
    });
    run("keyed_200_per_group_min", &one, &[1], Min, &|s| {
        scattered(&one, s, &|i| mix(i) % (N / 200))
    });
    run("keyed_8_per_group_min", &one, &[1], Min, &|s| {
        scattered(&one, s, &|i| i / 8)
    });
    run("i32_nullable_8_per_min", &narrow_nullable, &[1], Min, &|s| {
        scattered(&narrow_nullable, s, &|i| i / 8)
    });
    run("2xi64_nullable_8_per_min", &two_nullable, &[1, 2], Min, &|s| {
        scattered(&two_nullable, s, &|i| i / 8)
    });
    // The 256 groups under the other group-key forms.
    run("i32_nullable_256_sum", &narrow_nullable, &[1], Sum, &|s| {
        scattered(&narrow_nullable, s, &|i| mix(i) % 256)
    });
    run("2xi64_nullable_256_sum", &two_nullable, &[1, 2], Sum, &|s| {
        scattered(&two_nullable, s, &|i| mix(i) % 256)
    });
}

/// Instructions per delta row of `op_reduce` over a three-run output trace whose
/// two older runs lie wholly below the delta's first group: the delta touches
/// every group of the newest run and adds a new group between each pair.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_multi_run_bench() {
    const G: u64 = 1 << 14;
    let counter = Counter::instructions();
    let schema = pk_payload_schema(&[TypeCode::U64]);
    let rows = |keys: &mut dyn Iterator<Item = u64>| {
        let mut bb = BatchBuilder::new(&schema);
        for k in keys {
            bb.begin_row(k as u128, 1);
            bb.put_u64(k);
            bb.end_row();
        }
        let mut b = bb.finish();
        b.certify_consolidated();
        b
    };
    let mut h = Harness::new(plan(&schema, &[0], &[AggFunc::Sum]));
    epoch(&counter, &mut h, &rows(&mut (0..2 * G).step_by(2)));
    epoch(&counter, &mut h, &rows(&mut (1..2 * G).step_by(2)));
    epoch(&counter, &mut h, &rows(&mut (2 * G..4 * G).step_by(2)));
    let delta = rows(&mut (2 * G..4 * G));
    // The first pass takes the pool's first allocations.
    let [_, (out, instructions)] = [(); 2].map(|()| counter.measure(|| h.reduce(&delta)));
    println!(
        "op_reduce_multi_run_bench {:.1} instr/row (out {})",
        instructions as f64 / delta.count as f64,
        out.count
    );
}

/// `[U64 a, U64 b | value]`, PK `(a, b)`.
fn compound_schema(value: SchemaColumn) -> SchemaDescriptor {
    let k = SchemaColumn::new(TypeCode::U64, false);
    SchemaDescriptor::new(&[k, k, value], &[0, 1])
}

type PutValue = fn(&mut BatchBuilder, u64);
fn value_shapes() -> [(&'static str, SchemaColumn, PutValue); 6] {
    [
        ("I64", SchemaColumn::new(TypeCode::I64, false), |b, i| b.put_u64(mix(i))),
        ("U128", SchemaColumn::new(TypeCode::U128, false), |b, i| {
            b.put_int((mix(i) as u128) << 64 | i as u128)
        }),
        ("STR40", SchemaColumn::new(TypeCode::String, false), |b, i| {
            b.put_string(&format!("{:040}", mix(i)))
        }),
        ("STR8", SchemaColumn::new(TypeCode::String, false), |b, i| {
            b.put_string(&format!("{:08}", mix(i) % 100_000_000))
        }),
        ("STRVAR", SchemaColumn::new(TypeCode::String, false), |b, i| {
            let s = format!("{:060}", mix(i));
            b.put_string(&s[..13 + (mix(i) % 47) as usize])
        }),
        ("F64", SchemaColumn::new(TypeCode::F64, false), |b, i| {
            b.put_float((mix(i) >> 11) as f64 / 3.0)
        }),
    ]
}

/// The run arm: groups are runs of `run_len` rows of the leading PK column.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_runs_bench() {
    use AggFunc::{Max, Min, Sum};
    const N: u64 = 1 << 16;
    let counter = Counter::instructions();
    for run_len in [2u64, 16, 128, 1024] {
        for (label, value, put) in value_shapes() {
            for (aname, aggs) in [("min", &[Min][..]), ("min+max+sum", &[Min, Max, Sum][..])] {
                if aggs.len() > 1 && label != "I64" {
                    continue;
                }
                let schema = compound_schema(value);
                let make = |salt: u64| {
                    let mut bb = BatchBuilder::new(&schema);
                    for i in 0..N {
                        bb.begin_row_natives(&[(i / run_len) as u128, (i % run_len + salt * run_len) as u128], 1);
                        put(&mut bb, i + salt * N);
                        bb.end_row();
                    }
                    let mut b = bb.finish();
                    b.certify_consolidated();
                    b
                };
                let mut h = Harness::new(plan(&schema, &[0], aggs));
                let (d1, d2) = (h.fold(make(1)), h.fold(make(2)));
                let retraction = h.fold(d1.clone().negated());
                let [empty, populated, retract] = [&d1, &d2, &retraction].map(|d| epoch(&counter, &mut h, d));
                println!(
                    "op_reduce_runs_bench len={run_len:<5} {label:<7} {aname:<12} empty {empty:7.1}, populated {populated:7.1}, \
                     retraction {retract:7.1} instr/row"
                );
            }
        }
    }
}

/// Instructions per `op_reduce` call on a tiny delta against
/// a populated trace.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_tiny_bench() {
    use AggFunc::{Max, Min, Sum};
    const ITERS: u64 = 2_000;
    let counter = Counter::instructions();
    let i64c = SchemaColumn::new(TypeCode::I64, false);
    let strc = SchemaColumn::new(TypeCode::String, false);
    // [U64 pk | I64 grp | value]
    let keyed_i = grouped_schema(&[i64c], i64c);
    let keyed_s = grouped_schema(&[i64c], strc);
    let lead_i = compound_schema(i64c);
    let lead_s = compound_schema(strc);
    type Row = (u64, u64, u64, i64); // pk-or-a, grp-or-b, value seed, weight
    let build = |schema: &SchemaDescriptor, rows: &[Row]| {
        let compound = schema.pk_cols().len() == 2;
        let string = schema.columns()[2].type_code == TypeCode::String;
        let mut bb = BatchBuilder::new(schema);
        for &(a, b, v, w) in rows {
            if compound {
                bb.begin_row_natives(&[a as u128, b as u128], w);
            } else {
                bb.begin_row(a as u128, w);
                bb.put_int(b as u128);
            }
            if string {
                bb.put_string(&format!("{:040}", mix(v)));
            } else {
                bb.put_u64(mix(v));
            }
            bb.end_row();
        }
        bb.finish()
    };
    // The stored rows: keyed → pk i, grp i/8; leading → (i/8, i%8).
    let stored = |schema: &SchemaDescriptor| -> Vec<Row> {
        let compound = schema.pk_cols().len() == 2;
        (0..4096u64)
            .map(|i| {
                if compound {
                    (i / 8, i % 8, i, 1)
                } else {
                    (i, i / 8, i, 1)
                }
            })
            .collect()
    };
    let shapes: [(&str, &SchemaDescriptor, &[u32], &[AggFunc]); 10] = [
        ("keyed sum", &keyed_i, &[1], &[Sum]),
        ("keyed min", &keyed_i, &[1], &[Min]),
        ("keyed min+max+sum", &keyed_i, &[1], &[Min, Max, Sum]),
        ("keyed str min", &keyed_s, &[1], &[Min]),
        ("leading sum", &lead_i, &[0], &[Sum]),
        ("leading min", &lead_i, &[0], &[Min]),
        ("leading min+max+sum", &lead_i, &[0], &[Min, Max, Sum]),
        ("leading str min", &lead_s, &[0], &[Min]),
        ("global sum", &keyed_i, &[], &[Sum]),
        ("global min", &keyed_i, &[], &[Min]),
    ];
    for (label, schema, group, aggs) in shapes {
        let compound = schema.pk_cols().len() == 2;
        // Row i of the stored set, and a fresh row in group g.
        let old = |i: u64, w: i64| -> Row {
            if compound {
                (i / 8, i % 8, i, w)
            } else {
                (i, i / 8, i, w)
            }
        };
        let fresh = |g: u64, n: u64| -> Row {
            if compound {
                (g, 100 + n, 9000 + n, 1)
            } else {
                (100_000 + g * 16 + n, g, 9000 + n, 1)
            }
        };
        let deltas: [(&str, Vec<Row>); 5] = [
            ("1 insert", vec![fresh(7, 0)]),
            ("2 inserts 1 group", vec![fresh(7, 0), fresh(7, 1)]),
            ("2 inserts 2 groups", vec![fresh(7, 0), fresh(9, 0)]),
            ("update in group", vec![old(57, -1), fresh(7, 0)]),
            ("1 delete", vec![old(57, -1)]),
        ];
        for (dname, rows) in deltas {
            let mut h = Harness::new(plan(schema, group, aggs));
            let base = h.fold(build(schema, &stored(schema)));
            epoch(&counter, &mut h, &base);
            let delta = h.fold(build(schema, &rows));
            h.index(&delta);
            std::hint::black_box(h.reduce(&delta));
            let ((), instructions) = counter.measure(|| {
                for _ in 0..ITERS {
                    std::hint::black_box(h.reduce(&delta));
                }
            });
            println!(
                "op_reduce_tiny_bench {label:<20} {dname:<19} {:>6} instr/call",
                instructions / ITERS
            );
        }
    }
    // The ground row: an empty delta over an empty trace, and the delete of the
    // last row of a global aggregate.
    for (label, aggs) in [("ground sum", &[Sum][..]), ("ground min", &[Min][..])] {
        let col_idx = 2;
        let aggs: Vec<AggDescriptor> = aggs
            .iter()
            .map(|&agg_op| AggDescriptor { col_idx, agg_op })
            .chain([AggDescriptor::COUNT_STAR])
            .collect();
        let mk = || Harness::new(ReducePlan::from_wire(&keyed_i, &[], &aggs, true).unwrap());
        let h = mk();
        let empty = Batch::empty_with_schema(&keyed_i);
        let ((), instructions) = counter.measure(|| {
            for _ in 0..ITERS {
                std::hint::black_box(h.reduce(&empty));
            }
        });
        println!(
            "op_reduce_tiny_bench {label:<20} {:<19} {:>6} instr/call",
            "empty delta",
            instructions / ITERS
        );
        let mut h = mk();
        let one = h.fold(build(&keyed_i, &[(1, 1, 1, 1)]));
        epoch(&counter, &mut h, &one);
        let del = h.fold(build(&keyed_i, &[(1, 1, 1, -1)]));
        h.index(&del);
        let ((), instructions) = counter.measure(|| {
            for _ in 0..ITERS {
                std::hint::black_box(h.reduce(&del));
            }
        });
        println!(
            "op_reduce_tiny_bench {label:<20} {:<19} {:>6} instr/call",
            "delete last row",
            instructions / ITERS
        );
    }
}

/// The ordinal arm over mixed-sign weights and wide values: 8 rows a group,
/// scattered, every third row of the second delta a retraction of a stored row.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_wide_bench() {
    use AggFunc::{Max, Min, Sum};
    const N: u64 = 1 << 16;
    let counter = Counter::instructions();
    let grp = SchemaColumn::new(TypeCode::I64, false);
    for (label, value, put) in value_shapes() {
        for (aname, aggs) in [("min", &[Min][..]), ("min+max+sum", &[Min, Max, Sum][..])] {
            let aggs = if aggs.len() > 1 && !matches!(label, "I64" | "F64") {
                &[Min, Max][..]
            } else {
                aggs
            };
            let schema = grouped_schema(&[grp], value);
            let make = |lo: u64, hi: u64, w: i64| {
                let mut bb = BatchBuilder::new(&schema);
                for i in lo..hi {
                    bb.begin_row(mix(i) as u128, w);
                    bb.put_int((i % N / 8) as u128);
                    put(&mut bb, i);
                    bb.end_row();
                }
                bb.finish()
            };
            let mut h = Harness::new(plan(&schema, &[1], aggs));
            let d1 = h.fold(make(0, N, 1));
            // Inserts into every group, and a retraction of every third stored row.
            let mut mixed = make(N, 2 * N, 1);
            let mut bb = BatchBuilder::new(&schema);
            for i in (0..N).step_by(3) {
                bb.begin_row(mix(i) as u128, -1);
                bb.put_int((i % N / 8) as u128);
                put(&mut bb, i);
                bb.end_row();
            }
            mixed.append_batch(&bb.finish());
            let d2 = h.fold(mixed);
            let [empty, mixed] = [&d1, &d2].map(|d| epoch(&counter, &mut h, d));
            println!("op_reduce_wide_bench {label:<7} {aname:<12} empty {empty:7.1}, mixed-sign {mixed:7.1} instr/row");
        }
    }
}

/// Index entries per string shape `reduce_index_batch_bench` has no cell for.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reduce_index_strings_bench() {
    use AggFunc::{Max, Min};
    const N: u64 = 65_536;
    let counter = Counter::instructions();
    type Put = fn(&mut BatchBuilder, u64);
    let s = SchemaColumn::new(TypeCode::String, false);
    let cases: [(&str, &[AggFunc], Put); 8] = [
        ("STR8 MIN", &[Min], |b, i| {
            b.put_string(&format!("{:08}", mix(i) % 100_000_000))
        }),
        ("STR13 MIN", &[Min], |b, i| {
            b.put_string(&format!("{:013}", mix(i) % 10_000_000_000_000))
        }),
        ("STR40 MIN", &[Min], |b, i| b.put_string(&format!("{:040}", mix(i)))),
        ("STR40 MAX", &[Max], |b, i| b.put_string(&format!("{:040}", mix(i)))),
        ("STR40 trailing NUL", &[Min], |b, i| {
            b.put_string(&format!("{:039}\0", mix(i)))
        }),
        ("STR40 NUL every 4", &[Min], |b, i| {
            b.put_string(&format!("{:040}", mix(i)).replace(['0', '5', '7'], "\0"))
        }),
        ("STR400 MIN", &[Min], |b, i| b.put_string(&format!("{:0400}", mix(i)))),
        ("STR400 one NUL", &[Min], |b, i| {
            b.put_string(&format!("{:0200}\0{:0199}", mix(i), i))
        }),
    ];
    for (label, aggs, put) in cases {
        let schema = grouped_schema(&[SchemaColumn::new(TypeCode::U32, false)], s);
        let mut b = BatchBuilder::new(&schema);
        for i in 0..N {
            b.begin_row(i as u128, 1);
            b.put_int((i % 4096) as u128);
            put(&mut b, i);
            b.end_row();
        }
        let mut delta = b.finish();
        delta.certify_consolidated();
        let plan = plan(&schema, &[1], aggs);
        let [_, (entries, instructions)] = [(); 2].map(|()| counter.measure(|| plan.index_batch(&delta).unwrap()));
        println!(
            "reduce_index_strings_bench {label:<20} {:7.1} instr/row ({} entries)",
            instructions as f64 / N as f64,
            entries.count
        );
    }
}
