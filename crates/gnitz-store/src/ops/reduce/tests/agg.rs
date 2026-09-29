use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::{Batch, BatchBuilder};
use crate::test_support::le_cell;
use gnitz_wire::AggDescriptor;

/// Single-row batch with a U64 PK and one F64 payload column carrying `val`.
fn f64_batch(val: f64) -> Batch {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
        ],
        &[0],
    );
    let mut b = BatchBuilder::new(schema);
    b.begin_row(1u128, 1i64);
    b.put_float(val);
    b.end_row();
    b.finish()
}

fn f64_acc(agg_op: AggFunc) -> Accumulator {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, false),
        ],
        &[0],
    );
    let aggs = [AggDescriptor { col_idx: 1, agg_op }, AggDescriptor::COUNT_STAR];
    let mut accs = super::super::plan::ReducePlan::from_wire(&schema, &[0], &aggs, false)
        .unwrap()
        .shape
        .acc_template;
    accs.swap_remove(0)
}

// A NaN seen first must not poison MIN. A subsequent finite value
// is smaller under the total order and must replace it.
#[test]
fn min_nan_first_does_not_poison() {
    let nan = f64_batch(f64::NAN);
    let finite = f64_batch(5.0);
    let mut acc = f64_acc(AggFunc::Min);
    acc.step_from_batch(&nan.as_mem_batch(), 0, 1);
    acc.step_from_batch(&finite.as_mem_batch(), 0, 1);
    let got = f64::from_bits(acc.value_bits());
    assert_eq!(got, 5.0, "finite value must displace a leading NaN in MIN");
}

// MAX must use the total order consistently. Under
// total_cmp a quiet (positive) NaN is the greatest value, so once seen it
// is retained as the max regardless of arrival order.
#[test]
fn max_uses_total_order_for_nan() {
    let finite = f64_batch(5.0);
    let nan = f64_batch(f64::NAN);
    let mut acc = f64_acc(AggFunc::Max);
    acc.step_from_batch(&finite.as_mem_batch(), 0, 1);
    acc.step_from_batch(&nan.as_mem_batch(), 0, 1);
    let got = f64::from_bits(acc.value_bits());
    assert!(got.is_nan(), "MAX must adopt NaN as the greatest under total order");
}

/// `bulk_step` over a range reaches the same value as stepping each row with
/// `step_from_batch`, for every aggregate over every scalar payload width, with
/// NULLs and non-unit weights.
#[test]
fn bulk_step_matches_step_from_batch() {
    const N: usize = 97;
    let tcs = [
        TypeCode::U8,
        TypeCode::I8,
        TypeCode::U16,
        TypeCode::I16,
        TypeCode::U32,
        TypeCode::I32,
        TypeCode::U64,
        TypeCode::I64,
        TypeCode::F32,
        TypeCode::F64,
    ];
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend(tcs.iter().map(|&tc| SchemaColumn::new(tc, true)));
    let schema = SchemaDescriptor::new(&cols, &[0]);
    let mut rng = crate::test_rng::Rng::new(0x5eed);
    let mut b = BatchBuilder::new(schema);
    for row in 0..N {
        b.begin_row(row as u128, rng.gen_range(5) as i64 + 1);
        let nulls = rng.next_u64() & ((1 << tcs.len()) - 1);
        for (pi, &tc) in tcs.iter().enumerate() {
            let v = rng.next_u64();
            match tc {
                _ if nulls >> pi & 1 == 1 => b.put_null(),
                TypeCode::F32 => b.put_float(f64::from((v as i32) as f32 / 7.0)),
                TypeCode::F64 => b.put_float((v as i64) as f64 / 7.0),
                _ => b.put_int(le_cell(&v.to_le_bytes()[..schema.columns[pi + 1].size() as usize])),
            }
        }
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();
    for ci in 1..=tcs.len() as u32 {
        for agg_op in [
            AggFunc::Count,
            AggFunc::CountNonNull,
            AggFunc::Sum,
            AggFunc::Min,
            AggFunc::Max,
        ] {
            let aggs = [AggDescriptor { col_idx: ci, agg_op }, AggDescriptor::COUNT_STAR];
            let template = super::super::plan::ReducePlan::from_wire(&schema, &[0], &aggs, false)
                .unwrap()
                .shape
                .acc_template;
            let (mut bulk, mut each) = (template[0].clone(), template[0].clone());
            for (s, e) in [(0, 0), (0, 1), (3, 40), (40, 41), (41, N)] {
                (bulk.bulk_step())(&mut bulk, &mb, s..e);
                for row in s..e {
                    each.step_from_batch(&mb, row, mb.get_weight(row));
                }
            }
            let bits = |a: &Accumulator| {
                a.value().map(|v| match v {
                    AggValue::Bits(b) => b,
                    AggValue::Wide(..) => unreachable!("a scalar column"),
                })
            };
            assert_eq!(bits(&bulk), bits(&each), "{agg_op:?} over column {ci}");
        }
    }
}
