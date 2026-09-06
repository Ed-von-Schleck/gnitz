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
    let mut accs = super::super::plan::ReducePlan::new(
        &schema,
        &[0],
        &[AggDescriptor { col_idx: 1, agg_op }],
        schema.reduce_out_key(&[0]),
        false,
        false,
    )
    .unwrap()
    .acc_template;
    accs.pop().unwrap()
}

// Item 19: a NaN seen first must not poison MIN. A subsequent finite value
// is smaller under the total order and must replace it.
#[test]
fn min_nan_first_does_not_poison() {
    let nan = f64_batch(f64::NAN);
    let finite = f64_batch(5.0);
    let mut acc = f64_acc(AggFunc::Min);
    acc.step_from_batch(&nan.as_mem_batch(), 0, 1);
    acc.step_from_batch(&finite.as_mem_batch(), 0, 1);
    let got = f64::from_bits(acc.get_value_bits());
    assert_eq!(got, 5.0, "finite value must displace a leading NaN in MIN");
}

// Item 19 mirror: MAX must use the total order consistently. Under
// total_cmp a quiet (positive) NaN is the greatest value, so once seen it
// is retained as the max regardless of arrival order.
#[test]
fn max_uses_total_order_for_nan() {
    let finite = f64_batch(5.0);
    let nan = f64_batch(f64::NAN);
    let mut acc = f64_acc(AggFunc::Max);
    acc.step_from_batch(&finite.as_mem_batch(), 0, 1);
    acc.step_from_batch(&nan.as_mem_batch(), 0, 1);
    let got = f64::from_bits(acc.get_value_bits());
    assert!(got.is_nan(), "MAX must adopt NaN as the greatest under total order");
}

/// `Accumulator` resolves the two `AggFunc` classifications the wire owns —
/// linearity and the zero-identity empty render — off its own `StepKind` rather
/// than a stored opcode, so both must still answer exactly as `AggFunc` does for
/// every opcode the wire can name.
#[test]
fn step_kind_answers_the_wire_classifications() {
    for &op in AggFunc::ALL {
        let acc = f64_acc(op);
        assert_eq!(acc.is_linear(), op.is_linear(), "{op:?}: linearity");
        assert_eq!(
            acc.empty_renders_zero(),
            op.empty_renders_zero(),
            "{op:?}: empty render",
        );
    }
}
