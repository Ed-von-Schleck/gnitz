use super::*;
use crate::algebra::Placement;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{make_schema_u64_i64, pk_payload_schema, pk_u64_two_i64_schema, u64_pk_schema};
use std::hint::black_box;

/// `n` rows, none NULL, over a U64-PK-columns, all-I64-payload `schema`, the last PK column
/// `0..n`, every other column a spread function of it.
fn bench_stripe(schema: &SchemaDescriptor, n: usize) -> Batch {
    let mut b = BatchBuilder::new(schema);
    let lead = schema.pk_cols().len() as u64 - 1;
    for pk in 0..n as u64 {
        let mut natives: Vec<u128> = (0..lead)
            .map(|c| pk.wrapping_mul(0x9E37_79B9_7F4A_7C15 + 2 * c) as u128)
            .collect();
        natives.push(pk as u128);
        b.begin_row_natives(&natives, 1);
        for c in 1..=schema.num_payload_cols() as i64 {
            b.put_int((pk as i64).wrapping_mul(2_654_435_761 + c) as u128);
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
    let instructions = gnitz_foundation::perf::Counter::instructions();
    let (one, two) = (make_schema_u64_i64(), pk_u64_two_i64_schema());
    let wide = pk_payload_schema(&[TypeCode::U64; 3]);
    let nullable = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let mut three = vec![SchemaColumn::new(TypeCode::I64, false); 4];
    three[0] = SchemaColumn::new(TypeCode::U64, false);
    let three = SchemaDescriptor::new(&three, &[0]);
    let prefix = |n: u8| Ok(ScatterPlan::native(Placement::Keyed { dist_stride: n }));
    for (name, schema, plan, workers) in [
        ("pk", &one, ScatterPlan::group(&one, &[0]), 4),
        ("prefix 3", &wide, prefix(3), 4),
        ("prefix 12", &wide, prefix(12), 4),
        ("prefix 16", &wide, prefix(16), 4),
        ("pk column in a 24-byte pk", &wide, ScatterPlan::group(&wide, &[1]), 4),
        (
            "pk range past the start",
            &wide,
            ScatterPlan::join(&wide, &[(1, TypeCode::U64), (2, TypeCode::U64)]),
            4,
        ),
        ("whole 24-byte pk", &wide, ScatterPlan::group(&wide, &[0, 1, 2]), 4),
        ("image", &one, ScatterPlan::group(&one, &[1]), 4),
        ("packed", &two, ScatterPlan::group(&two, &[1, 2]), 4),
        ("packed nullable", &nullable, ScatterPlan::group(&nullable, &[1]), 4),
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
