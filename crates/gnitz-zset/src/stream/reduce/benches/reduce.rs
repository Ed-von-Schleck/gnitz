use super::plan::ReducePlan;
use super::tests::Harness;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{ColumnTable, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::pk_payload_schema;
use gnitz_foundation::perf::Counter;
use gnitz_wire::{AggDescriptor, AggFunc};

fn mix(i: u64) -> u64 {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

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
    let cases: [(&str, SchemaColumn, &[AggFunc], Put); 5] = [
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
        let group = &schema.columns[1..schema.num_columns() - 1];
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
