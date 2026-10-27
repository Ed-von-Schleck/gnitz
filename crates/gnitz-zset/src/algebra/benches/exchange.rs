use super::*;
use crate::algebra::Placement;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_payload_schema, pk_u64_two_i64_schema, u64_pk_schema,
};
use gnitz_wire::TypeCode;
use std::hint::black_box;

/// `n` rows, none NULL: the last PK column `0..n` and every other column a
/// spread function of it, each cut to its column's width, a string one of
/// 50 000 values.
fn bench_stripe(schema: &SchemaDescriptor, n: usize) -> Batch {
    let mut b = BatchBuilder::new(schema);
    let pk = schema.pk_cols();
    let width = |c: u32| gnitz_wire::image_mask(schema.columns()[c as usize].size());
    for row in 0..n as u64 {
        let spread = |c: u64| row.wrapping_mul(0x9E37_79B9_7F4A_7C15 + 2 * c) as u128;
        let natives: Vec<u128> = (0..pk.len())
            .map(|i| match i + 1 == pk.len() {
                true => row as u128,
                false => spread(i as u64),
            } & width(pk[i]))
            .collect();
        b.begin_row_natives(&natives, 1);
        for (pc, col) in schema.payload_columns() {
            match col.type_code.is_german_string() {
                true => b.put_string(&format!("key-{}", row % 50_000)),
                false => b.put_int(spread(pc as u64 + 8) & gnitz_wire::image_mask(col.size())),
            }
        }
        b.end_row();
    }
    b.finish()
}

/// Instructions per [`ScatterPlan::route`] call, and per row, for each routing
/// kernel: on a one-row delta, where the per-call fixed cost a round adds shows,
/// and at 1M rows.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn exchange_route_bench() {
    use TypeCode::{I128, I64, U128, U16, U64, U8};
    let instructions = gnitz_foundation::perf::Counter::instructions();
    let (one, two) = (make_schema_u64_i64(), pk_u64_two_i64_schema());
    let wide = pk_payload_schema(&[U64; 3]);
    let mixed = pk_payload_schema(&[U8, U16, U64, U8]);
    let nullable = u64_pk_schema(&[SchemaColumn::new(I64, true)]);
    let three = u64_pk_schema(&[SchemaColumn::new(I64, false); 3]);
    let strs = make_schema_pk_u64_payload_string();
    let native = |placement| Ok(ScatterPlan::native(placement));
    for (name, schema, plan, workers) in [
        ("pk", &one, ScatterPlan::group(&one, &[0]), 4),
        ("prefix 3", &mixed, native(Placement::keyed(&mixed, 2)), 4),
        ("prefix 12", &mixed, native(Placement::full_pk(&mixed)), 4),
        ("prefix 16", &wide, native(Placement::keyed(&wide, 2)), 4),
        ("pk column in a 24-byte pk", &wide, ScatterPlan::group(&wide, &[1]), 4),
        (
            "pk range past the start",
            &wide,
            ScatterPlan::join(&wide, &[(1, U64), (2, U64)]),
            4,
        ),
        ("whole 24-byte pk", &wide, ScatterPlan::group(&wide, &[0, 1, 2]), 4),
        ("image", &one, ScatterPlan::group(&one, &[1]), 4),
        ("packed", &two, ScatterPlan::group(&two, &[1, 2]), 4),
        ("packed nullable", &nullable, ScatterPlan::group(&nullable, &[1]), 4),
        (
            "join 3 columns",
            &three,
            ScatterPlan::join(&three, &[(1, I64), (2, I64), (3, I64)]),
            4,
        ),
        ("join promoted", &one, ScatterPlan::join(&one, &[(1, I128)]), 4),
        ("join string", &strs, ScatterPlan::join(&strs, &[(1, U128)]), 4),
        ("fold", &three, ScatterPlan::group(&three, &[1, 2, 3]), 4),
        ("global", &one, ScatterPlan::group(&one, &[]), 4),
        // Reads no key, whatever the plan.
        ("one worker", &one, ScatterPlan::group(&one, &[0]), 1),
    ] {
        let plan = plan.unwrap();
        for (n, iters) in [(1, 10_000), (1_000_000, 20)] {
            let batch = bench_stripe(schema, n);
            let mut pool = Vec::new();
            let routed: usize = plan.route(&batch, &mut pool, workers).iter().map(Vec::len).sum();
            assert_eq!(routed, n, "{name}: the route dropped rows");
            let ((), i) = instructions.measure(|| {
                for _ in 0..iters {
                    black_box(plan.route(&batch, &mut pool, workers));
                }
            });
            println!(
                "exchange_route_bench {name:<26} {n:>7} rows -> {workers} workers: {:>9} instr/call, {:>7.1} instr/row",
                i / iters,
                i as f64 / (iters as usize * n) as f64
            );
        }
    }
}
