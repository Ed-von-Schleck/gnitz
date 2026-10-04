use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{Placement, SchemaColumn, TypeCode};
use crate::test_support::{make_schema_u64_i64, pk_u64_two_i64_schema};
use std::hint::black_box;

/// `n` rows, none NULL, over a U64-PK-columns, all-I64-payload `schema`, the last PK column
/// `0..n`, every other column a spread function of it.
fn bench_stripe(schema: &SchemaDescriptor, n: usize) -> Batch {
    use crate::schema::ColumnTable;
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
/// kernel: on small deltas, where the per-call fixed cost a round adds shows, and
/// at 1M rows.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn exchange_route_bench() {
    let instructions = gnitz_foundation::perf::Counter::instructions().unwrap();
    let (one, two) = (make_schema_u64_i64(), pk_u64_two_i64_schema());
    let wide = crate::test_support::wide_pk_3xu64_schema();
    let nullable = crate::test_support::u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let prefix = |n: u8| Ok(ScatterPlan::native(Placement::Keyed { dist_stride: n }));
    for (name, schema, plan) in [
        ("pk", &one, ScatterPlan::group(&one, &[0])),
        ("prefix 3", &wide, prefix(3)),
        ("prefix 5", &wide, prefix(5)),
        ("prefix 12", &wide, prefix(12)),
        ("prefix 16", &wide, prefix(16)),
        ("pk column in a 24-byte pk", &wide, ScatterPlan::group(&wide, &[1])),
        ("image", &one, ScatterPlan::group(&one, &[1])),
        (
            "packed",
            &two,
            ScatterPlan::join(&two, &[(1, TypeCode::I64), (2, TypeCode::I64)]),
        ),
        ("packed group", &two, ScatterPlan::group(&two, &[1, 2])),
        ("pk prefix group", &wide, ScatterPlan::group(&wide, &[0, 1])),
        (
            "pk range past the start",
            &wide,
            ScatterPlan::join(&wide, &[(1, TypeCode::U64), (2, TypeCode::U64)]),
        ),
        ("nullable column group", &nullable, ScatterPlan::group(&nullable, &[1])),
    ] {
        let plan = plan.unwrap();
        for (n, workers, iters) in [
            (1, 4, 10_000),
            (64, 16, 10_000),
            (1024, 16, 10_000),
            (1_000_000, 4, 20),
            (1_000_000, 1, 20),
        ] {
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
                "{name}: {n} rows -> {workers} workers: {} instructions per call, {:.1} per row",
                i / iters,
                i as f64 / (iters as usize * n) as f64
            );
        }
    }
}
