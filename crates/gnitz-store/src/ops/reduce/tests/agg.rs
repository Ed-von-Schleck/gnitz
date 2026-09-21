use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::Batch;
use gnitz_wire::AggDescriptor;

/// Single-row batch with a U64 PK and one F64 payload column carrying `val`.
fn f64_batch(val: f64) -> Batch {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::F64, 0),
        ],
        &[0],
    );
    let mut b = Batch::with_capacity(&schema, 1);
    b.extend_pk(1u128);
    b.extend_weight(&1i64.to_le_bytes());
    b.extend_null_bmp(&0u64.to_le_bytes());
    b.extend_col(0, &val.to_le_bytes());
    b.count += 1;
    b
}

fn f64_acc(agg_op: AggFunc) -> Accumulator {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::F64, 0),
        ],
        &[0],
    );
    let aggs = [AggDescriptor { col_idx: 1, agg_op }, AggDescriptor::COUNT_STAR];
    let mut accs = super::super::plan::ReducePlan::from_wire(&schema, &[0], &aggs, false, false)
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
        type_code::U8,
        type_code::I8,
        type_code::U16,
        type_code::I16,
        type_code::U32,
        type_code::I32,
        type_code::U64,
        type_code::I64,
        type_code::F32,
        type_code::F64,
    ];
    let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
    cols.extend(tcs.iter().map(|&tc| SchemaColumn::new(tc, 1)));
    let schema = SchemaDescriptor::new(&cols, &[0]);
    let mut rng = crate::test_rng::Rng::new(0x5eed);
    let mut b = Batch::with_capacity(&schema, N);
    for row in 0..N {
        b.extend_pk(row as u128);
        b.extend_weight(&(rng.gen_range(5) as i64 + 1).to_le_bytes());
        let nulls = rng.next_u64() & ((1 << tcs.len()) - 1);
        b.extend_null_bmp(&nulls.to_le_bytes());
        for (pi, &tc) in tcs.iter().enumerate() {
            let v = rng.next_u64();
            let bytes = match tc {
                type_code::F32 => ((v as i32) as f32 / 7.0).to_le_bytes().to_vec(),
                type_code::F64 => ((v as i64) as f64 / 7.0).to_le_bytes().to_vec(),
                _ => v.to_le_bytes()[..schema.columns[pi + 1].size() as usize].to_vec(),
            };
            b.extend_col(pi, &bytes);
        }
        b.count += 1;
    }
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
            let template = super::super::plan::ReducePlan::from_wire(&schema, &[0], &aggs, false, false)
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
