use std::rc::Rc;

use super::tests::Harness;
use super::{ReducePlan, op_reduce};
use crate::runs::{MappedShard, Run, from_runs_in_band};
use crate::test_support::{assert_folds, map_shard, pk_payload_schema, u64_pk_schema};
use gnitz_foundation::perf::Counter;
use gnitz_foundation::rng::mix;
use gnitz_wire::PkBuf;
use gnitz_wire::{AggDescriptor, AggFunc, TypeCode};
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor, group_band};

/// The reduce of `aggs` over `schema`'s last column, with the COUNT(*) a
/// circuit reduce carries; `ground` for a global one that publishes a ground
/// row.
fn plan(schema: &SchemaDescriptor, group: &[u32], aggs: &[AggFunc], ground: bool) -> ReducePlan {
    let col_idx = schema.num_columns() as u32 - 1;
    let aggs: Vec<AggDescriptor> = aggs
        .iter()
        .map(|&agg_op| AggDescriptor { col_idx, agg_op })
        .chain([AggDescriptor::COUNT_STAR])
        .collect();
    ReducePlan::from_wire(schema, group, &aggs, ground).unwrap()
}

/// Instructions per delta row of the value-index entries a MIN/MAX reduce
/// derives from its delta, per image kind: a scalar, a fixed wide value, a
/// column holding NULLs, two extremes at once, and strings of 8, 13, 40 and 400
/// bytes, with no NUL, a trailing one, one in the middle, or one in every few
/// bytes; then a scalar under hashing group keys, with one extreme and two.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn reduce_index_entries_bench() {
    use AggFunc::{Max, Min};
    const N: u64 = 65_536;
    let counter = Counter::instructions();
    type Put = fn(&mut BatchBuilder, u64);
    let string = SchemaColumn::new(TypeCode::String, false);
    let cases: [(&str, SchemaColumn, &[AggFunc], Put); 13] = [
        ("I64 MIN", SchemaColumn::new(TypeCode::I64, false), &[Min], |b, i| {
            b.put_u64(mix(i));
        }),
        ("U128 MAX", SchemaColumn::new(TypeCode::U128, false), &[Max], |b, i| {
            b.put_int((mix(i) as u128) << 64 | i as u128);
        }),
        (
            "STRING MIN",
            SchemaColumn::new(TypeCode::String, false),
            &[Min],
            |b, i| {
                b.put_string(&format!("{:040}", mix(i)));
            },
        ),
        // Every image escapes a `0x00`.
        (
            "STRING MIN, NULs",
            SchemaColumn::new(TypeCode::String, false),
            &[Min],
            |b, i| {
                b.put_string(&format!("{:020}\0{:019}", mix(i), i));
            },
        ),
        (
            "nullable I64 MIN",
            SchemaColumn::new(TypeCode::I64, true),
            &[Min],
            |b, i| {
                b.put_opt_int((i % 4 != 0).then_some(mix(i) as u128));
            },
        ),
        (
            "I64 MIN and MAX",
            SchemaColumn::new(TypeCode::I64, false),
            &[Min, Max],
            |b, i| {
                b.put_u64(mix(i));
            },
        ),
        ("STR8 MIN", string, &[Min], |b, i| {
            b.put_string(&format!("{:08}", mix(i) % 100_000_000));
        }),
        ("STR13 MIN", string, &[Min], |b, i| {
            b.put_string(&format!("{:013}", mix(i) % 10_000_000_000_000));
        }),
        ("STR40 MAX", string, &[Max], |b, i| {
            b.put_string(&format!("{:040}", mix(i)));
        }),
        ("STR40 trailing NUL", string, &[Min], |b, i| {
            b.put_string(&format!("{:039}\0", mix(i)));
        }),
        ("STR40 NUL every 4", string, &[Min], |b, i| {
            b.put_string(&format!("{:040}", mix(i)).replace(['0', '5', '7'], "\0"));
        }),
        ("STR400 MIN", string, &[Min], |b, i| {
            b.put_string(&format!("{:0400}", mix(i)));
        }),
        ("STR400 one NUL", string, &[Min], |b, i| {
            b.put_string(&format!("{:0200}\0{:0199}", mix(i), i));
        }),
    ];
    let u32_group: &[SchemaColumn] = &[SchemaColumn::new(TypeCode::U32, false)];
    let nullable_pair: &[SchemaColumn] = &[SchemaColumn::new(TypeCode::I64, true); 2];
    let string_group: &[SchemaColumn] = &[string];
    let grouped = cases
        .iter()
        .map(|&(label, value, aggs, put)| (label.to_string(), u32_group, value, aggs, put));
    let nullable_triple: &[SchemaColumn] = &[SchemaColumn::new(TypeCode::I64, true); 3];
    // The first case again under a group set too wide for a packed key.
    let folded = cases[..1]
        .iter()
        .map(|&(label, value, aggs, put)| (format!("{label}, 2 groups"), nullable_pair, value, aggs, put));
    // The scalar cases, one extreme and two, again under group keys that pack
    // by hashing: a group set whose last columns fold, and a string group.
    let hashed = [(", 3 groups", nullable_triple), (", STRING group", string_group)];
    let hashed = hashed.into_iter().flat_map(|(suffix, group)| {
        [cases[0], cases[5]]
            .into_iter()
            .map(move |(label, value, aggs, put)| (format!("{label}{suffix}"), group, value, aggs, put))
    });
    for (label, group, value, aggs, put) in grouped.chain(folded).chain(hashed) {
        let schema = u64_pk_schema(&[group, &[value]].concat());
        let mut b = BatchBuilder::new(&schema);
        for i in 0..N {
            b.begin_row(i as u128, 1);
            for col in group {
                match col.type_code {
                    TypeCode::String => {
                        b.put_string(&format!("group {:016}", i % 4096));
                    }
                    _ => {
                        b.put_int((i % 4096) as u128);
                    }
                }
            }
            put(&mut b, i);
            b.end_row();
        }
        let mut delta = b.finish();
        delta.certify_consolidated();
        let group_cols: Vec<u32> = (1..=group.len() as u32).collect();
        let plan = plan(&schema, &group_cols, aggs, false);
        // The first pass takes the first allocations.
        let [_, (entries, instructions)] = [(); 2].map(|()| counter.measure(|| plan.index_entries(&delta).unwrap()));
        println!(
            "reduce_index_entries_bench {label:<32} {:6.1} instr/row ({} entries)",
            instructions as f64 / N as f64,
            entries.len()
        );
    }
}

/// One epoch over `delta`: the instructions per delta row of `op_reduce` alone,
/// the index entries the VM adds ahead of it left out.
fn epoch(counter: &Counter, h: &mut Harness, delta: &Batch) -> f64 {
    h.index(delta);
    let (out, instructions) = counter.measure(|| h.reduce(delta));
    h.out_trace.ingest(out);
    instructions as f64 / delta.len() as f64
}

/// [`epoch`] over three deltas: an insert against an empty trace, a second
/// insert into the same groups, and the retraction of the first.
fn three_epochs(counter: &Counter, h: &mut Harness, d1: Batch, d2: Batch) -> [f64; 3] {
    let (d1, d2) = (h.fold(d1), h.fold(d2));
    let retraction = h.fold(d1.clone().negated());
    [&d1, &d2, &retraction].map(|d| epoch(counter, h, d))
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
        let mut h = Harness::new(plan(schema, group, &[agg], false));
        let [empty, populated, retract] = three_epochs(&counter, &mut h, make(1), make(2));
        println!(
            "op_reduce_bench {label:<24} empty trace {empty:7.1}, populated {populated:7.1}, \
             retraction {retract:7.1} instr/row"
        );
    };

    // Unsorted rows over a U64 PK, the group columns, then one value: every
    // group column holds `grp` of the row, a nullable one NULL on every 16th row.
    let scattered = |schema: &SchemaDescriptor, salt: u64, grp: &dyn Fn(u64) -> u64| {
        let group = &schema.columns()[1..schema.num_columns() - 1];
        let mut bb = BatchBuilder::new(schema);
        for i in 0..N {
            bb.begin_row(mix(i + salt * N) as u128, 1);
            for (c, col) in group.iter().enumerate() {
                let g = grp(i) & gnitz_wire::image_mask(col.size()) as u64;
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
                1 => {
                    bb.begin_row(i as u128, 1);
                }
                _ => {
                    bb.begin_row_natives(&[(i / 16) as u128, (i % 16) as u128], 1);
                }
            }
            bb.put_u64(i + salt);
            bb.end_row();
        }
        let mut b = bb.finish();
        b.certify_consolidated();
        b
    };

    let value = SchemaColumn::new(TypeCode::I64, false);
    let one = u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, false), value]);
    let narrow_nullable = u64_pk_schema(&[SchemaColumn::new(TypeCode::I32, true), value]);
    let nullable = SchemaColumn::new(TypeCode::I64, true);
    let two_nullable = u64_pk_schema(&[nullable, nullable, value]);
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

/// Instructions per delta row of `op_reduce` over a multi-run output trace,
/// with a new group between each pair the delta touches.
///
/// The heap arm: three in-memory runs, whose two older runs lie wholly below
/// the delta's first group; the delta touches every group of the newest run.
/// The shard arm: the same three runs and a fourth updating every group of the
/// third, each written as a shard, so the two newest shards both hold each
/// group the delta touches. Both arms list their runs newest first.
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
    let mut h = Harness::new(plan(&schema, &[0], &[AggFunc::Sum], false));
    let third = rows(&mut (2 * G..4 * G).step_by(2));
    epoch(&counter, &mut h, &rows(&mut (0..2 * G).step_by(2)));
    epoch(&counter, &mut h, &rows(&mut (1..2 * G).step_by(2)));
    epoch(&counter, &mut h, &third);
    let delta = rows(&mut (2 * G..4 * G));
    // The first pass takes the first allocations.
    let [_, (out, instructions)] = [(); 2].map(|()| counter.measure(|| h.reduce(&delta)));
    println!(
        "op_reduce_multi_run_bench heap  {:.1} instr/row (out {})",
        instructions as f64 / delta.len() as f64,
        out.len()
    );

    // A fourth tick retracts and rewrites every group of the third.
    epoch(&counter, &mut h, &third);
    let expected = h.reduce(&delta);
    let runs = h.out_trace.runs();
    assert_eq!(
        (runs[2].len(), runs[3].len()),
        (G as usize, 2 * G as usize),
        "the two newest runs both hold every group of the third tick"
    );
    let dir = tempfile::tempdir().unwrap();
    let out_schema = *runs[0].schema();
    let shards: Vec<Rc<MappedShard>> = (runs.iter().enumerate().rev())
        .map(|(i, run)| map_shard(&dir.path().join(format!("{i}.db")), run))
        .collect();
    let mut open = |first: &[u8], last: &[u8]| {
        let (start, end) = group_band(first, last, out_schema.pk_stride()).unwrap();
        let runs = shards.iter().cloned().map(Run::Shard);
        from_runs_in_band(runs, out_schema, start.pk_bytes(), end.as_ref().map(PkBuf::pk_bytes))
    };
    let mut plan = plan(&schema, &[0], &[AggFunc::Sum], false);
    let [_, (out, instructions)] = [(); 2].map(|()| counter.measure(|| op_reduce(&delta, &mut open, None, &mut plan)));
    assert_folds(&[expected], &out, "the shard arm against the heap trace");
    println!(
        "op_reduce_multi_run_bench shard {:.1} instr/row (out {})",
        instructions as f64 / delta.len() as f64,
        out.len()
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
        ("I64", SchemaColumn::new(TypeCode::I64, false), |b, i| {
            b.put_u64(mix(i));
        }),
        ("U128", SchemaColumn::new(TypeCode::U128, false), |b, i| {
            b.put_int((mix(i) as u128) << 64 | i as u128);
        }),
        ("STR40", SchemaColumn::new(TypeCode::String, false), |b, i| {
            b.put_string(&format!("{:040}", mix(i)));
        }),
        ("STR8", SchemaColumn::new(TypeCode::String, false), |b, i| {
            b.put_string(&format!("{:08}", mix(i) % 100_000_000));
        }),
        ("STRVAR", SchemaColumn::new(TypeCode::String, false), |b, i| {
            let s = format!("{:060}", mix(i));
            b.put_string(&s[..13 + (mix(i) % 47) as usize]);
        }),
        ("F64", SchemaColumn::new(TypeCode::F64, false), |b, i| {
            b.put_float((mix(i) >> 11) as f64 / 3.0);
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
    // 128 against 129 is the run step cap's boundary.
    for run_len in [2u64, 16, 128, 129, 256, 1024] {
        for (label, value, put) in value_shapes() {
            for (aname, aggs) in [("min", &[Min][..]), ("min+max+sum", &[Min, Max, Sum][..])] {
                if aggs.len() > 1 && label != "I64" {
                    continue;
                }
                let schema = compound_schema(value);
                let make = |pk_salt: u64, value_salt: u64| {
                    let mut bb = BatchBuilder::new(&schema);
                    for i in 0..N {
                        let b = i % run_len + pk_salt * run_len;
                        bb.begin_row_natives(&[(i / run_len) as u128, b as u128], 1);
                        put(&mut bb, i + value_salt * N);
                        bb.end_row();
                    }
                    let mut b = bb.finish();
                    b.certify_consolidated();
                    b
                };
                let mut h = Harness::new(plan(&schema, &[0], aggs, false));
                let [empty, populated, retract] = three_epochs(&counter, &mut h, make(1, 1), make(2, 2));
                // Every row of the second delta moved to a new PK, its value
                // kept: each group's aggregates are unchanged.
                let mut rewrite = make(2, 2).negated();
                rewrite.append_batch(&make(3, 2));
                let rewrite = h.fold(rewrite);
                let rewritten = epoch(&counter, &mut h, &rewrite);
                println!(
                    "op_reduce_runs_bench len={run_len:<5} {label:<7} {aname:<12} empty {empty:7.1}, populated {populated:7.1}, \
                     retraction {retract:7.1}, rewrite {rewritten:7.1} instr/row"
                );
            }
        }
    }
}

/// The run arm over updates: runs of `run_len` stored rows of the leading PK
/// column, then an epoch replacing each run's first row with a fresh one.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_runs_update_bench() {
    use AggFunc::{Max, Min, Sum};
    const N: u64 = 1 << 16;
    let counter = Counter::instructions();
    for run_len in [2u64, 16, 128] {
        for (label, value, put) in value_shapes() {
            for (aname, aggs) in [("min", &[Min][..]), ("min+max+sum", &[Min, Max, Sum][..])] {
                if !matches!(label, "I64" | "F64") || (aggs.len() > 1 && label != "I64") {
                    continue;
                }
                let schema = compound_schema(value);
                // Rows `(run, row)` of `rows`, each at weight `w`, valued off its seed.
                let make = |rows: &mut dyn Iterator<Item = (u64, u64, u64)>, w: i64| {
                    let mut bb = BatchBuilder::new(&schema);
                    for (run, row, seed) in rows {
                        bb.begin_row_natives(&[run as u128, row as u128], w);
                        put(&mut bb, seed);
                        bb.end_row();
                    }
                    bb.finish()
                };
                let mut h = Harness::new(plan(&schema, &[0], aggs, false));
                let stored = h.fold(make(&mut (0..N).map(|i| (i / run_len, i % run_len, i)), 1));
                epoch(&counter, &mut h, &stored);
                // Each run loses its row 0 and gains a row `run_len`.
                let runs = || 0..N / run_len;
                let mut update = make(&mut runs().map(|g| (g, 0, g * run_len)), -1);
                update.append_batch(&make(&mut runs().map(|g| (g, run_len, N + g)), 1));
                let update = h.fold(update);
                let instructions = epoch(&counter, &mut h, &update);
                println!(
                    "op_reduce_runs_update_bench len={run_len:<4} {label:<4} {aname:<12} update {instructions:7.1} instr/row"
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
    let keyed_i = u64_pk_schema(&[i64c, i64c]);
    let keyed_s = u64_pk_schema(&[i64c, strc]);
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
        // Row i of the stored set — keyed: pk i, grp i/8; leading: (i/8, i%8) —
        // and a fresh row in group g.
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
        // Stored row i's group and value under a PK no other row holds.
        let kept = |i: u64, w: i64| -> Row {
            if compound {
                (i / 8, 157, i, w)
            } else {
                (100_000 + i, i / 8, i, w)
            }
        };
        let mut deltas: Vec<(String, Vec<Row>)> = vec![
            ("1 insert".into(), vec![fresh(7, 0)]),
            ("2 inserts 1 group".into(), vec![fresh(7, 0), fresh(7, 1)]),
            ("2 inserts 2 groups".into(), vec![fresh(7, 0), fresh(9, 0)]),
            ("update in group".into(), vec![old(57, -1), fresh(7, 0)]),
            ("update kept values".into(), vec![old(57, -1), kept(57, 1)]),
            ("1 delete".into(), vec![old(57, -1)]),
        ];
        // Either side of the most groups an epoch emits a row at a time, each
        // group stored.
        if !group.is_empty() {
            for k in [4u64, 5, 8, 32] {
                deltas.push((
                    format!("{k} inserts {k} groups"),
                    (0..k).map(|g| fresh(7 * g, 0)).collect(),
                ));
            }
        }
        for (dname, rows) in deltas {
            let mut h = Harness::new(plan(schema, group, aggs, false));
            let stored: Vec<Row> = (0..4096).map(|i| old(i, 1)).collect();
            let base = h.fold(build(schema, &stored));
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
                "op_reduce_tiny_bench {label:<32} {dname:<20} {:>6} instr/call",
                instructions / ITERS
            );
        }
    }
    // The delete of the last row of a global aggregate, which emits its ground row.
    for (label, aggs) in [("ground sum", &[Sum][..]), ("ground min", &[Min][..])] {
        let mut h = Harness::new(plan(&keyed_i, &[], aggs, true));
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
            "op_reduce_tiny_bench {label:<32} {:<19} {:>6} instr/call",
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
            let schema = u64_pk_schema(&[grp, value]);
            let make = |ids: &mut dyn Iterator<Item = u64>, w: i64| {
                let mut bb = BatchBuilder::new(&schema);
                for i in ids {
                    bb.begin_row(mix(i) as u128, w);
                    bb.put_int((i % N / 8) as u128);
                    put(&mut bb, i);
                    bb.end_row();
                }
                bb.finish()
            };
            let mut h = Harness::new(plan(&schema, &[1], aggs, false));
            let d1 = h.fold(make(&mut (0..N), 1));
            // Inserts into every group, and a retraction of every third stored row.
            let mut mixed = make(&mut (N..2 * N), 1);
            mixed.append_batch(&make(&mut (0..N).step_by(3), -1));
            let d2 = h.fold(mixed);
            let [empty, mixed] = [&d1, &d2].map(|d| epoch(&counter, &mut h, d));
            println!("op_reduce_wide_bench {label:<7} {aname:<12} empty {empty:7.1}, mixed-sign {mixed:7.1} instr/row");
        }
    }
}

/// Instructions per delta row of `op_reduce` over medium deltas of fresh rows
/// spread over a populated trace's groups, the same delta again and again: from
/// sizes whose buffers the plan keeps to sizes whose buffers it drops after each
/// epoch.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_medium_bench() {
    const STORED: u64 = 1 << 16;
    const ITERS: u64 = 50;
    let counter = Counter::instructions();
    let i64c = SchemaColumn::new(TypeCode::I64, false);
    // [U64 pk | I64 grp | I64 value]
    let schema = u64_pk_schema(&[i64c, i64c]);
    let build = |ids: &mut dyn Iterator<Item = u64>| {
        let mut bb = BatchBuilder::new(&schema);
        for i in ids {
            bb.begin_row(i as u128, 1);
            bb.put_int((mix(i) % 4096) as u128);
            bb.put_u64(mix(i ^ 1));
            bb.end_row();
        }
        bb.finish()
    };
    for n in [256u64, 1024, 4096, 16384] {
        let mut h = Harness::new(plan(&schema, &[1], &[AggFunc::Min], false));
        let base = h.fold(build(&mut (0..STORED)));
        epoch(&counter, &mut h, &base);
        let delta = h.fold(build(&mut (STORED..STORED + n)));
        h.index(&delta);
        std::hint::black_box(h.reduce(&delta));
        let ((), instructions) = counter.measure(|| {
            for _ in 0..ITERS {
                std::hint::black_box(h.reduce(&delta));
            }
        });
        println!(
            "op_reduce_medium_bench {n:>6} rows {:7.1} instr/row",
            instructions as f64 / (ITERS * n) as f64
        );
    }
}

/// Instructions per delta row of `op_reduce` over a float SUM alone and as a
/// view plans one, a finite sum with its +inf and -inf counts, over finite F64
/// and F32 values: runs of 16 rows of the leading PK column, 8 scattered rows a
/// group, and one global group; three epochs each, as [`op_reduce_bench`]'s.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn op_reduce_float_sum_bench() {
    use AggFunc::{CountNegInf, CountPosInf, FiniteSum, Sum};
    const N: u64 = 1 << 16;
    let counter = Counter::instructions();
    for tc in [TypeCode::F64, TypeCode::F32] {
        let value = SchemaColumn::new(tc, false);
        let runs = compound_schema(value);
        let keyed = u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, false), value]);
        let make = |schema: &SchemaDescriptor, salt: u64| {
            let mut bb = BatchBuilder::new(schema);
            for i in 0..N {
                match schema.pk_cols().len() {
                    2 => {
                        bb.begin_row_natives(&[(i / 16) as u128, (i % 16 + salt * 16) as u128], 1);
                    }
                    _ => {
                        bb.begin_row(mix(i + salt * N) as u128, 1);
                        bb.put_int((i % (N / 8)) as u128);
                    }
                }
                bb.put_float((mix(i ^ salt) >> 11) as f64 / 3.0);
                bb.end_row();
            }
            let mut b = bb.finish();
            if schema.pk_cols().len() == 2 {
                b.certify_consolidated();
            }
            b
        };
        let shapes = [
            ("runs of 16", &runs, &[0][..]),
            ("8 a group", &keyed, &[1]),
            ("global", &keyed, &[]),
        ];
        for ((shape, schema, group), (aname, aggs)) in shapes.into_iter().flat_map(|s| {
            [
                ("sum", &[Sum][..]),
                ("sum+infs", &[FiniteSum, CountPosInf, CountNegInf]),
            ]
            .map(|a| (s, a))
        }) {
            let mut h = Harness::new(plan(schema, group, aggs, false));
            let [empty, populated, retract] = three_epochs(&counter, &mut h, make(schema, 1), make(schema, 2));
            println!(
                "op_reduce_float_sum_bench {tc:<4} {shape:<11} {aname:<9} empty {empty:7.1}, populated {populated:7.1}, \
                 retraction {retract:7.1} instr/row"
            );
        }
    }
}
