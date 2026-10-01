use super::*;
use crate::test_support::{col, ncol, rejected};
use WireAggFunc as W;

/// An aggregate's physical ops: a count over a NOT NULL argument counts rows, a
/// count companion rides exactly where null-ness needs one, and only SUM and AVG
/// are refused a type — MIN, MAX and COUNT select or count a row of any.
#[test]
fn agg_ops_picks_the_value_op_and_the_count_its_nullness_needs() {
    use AggFunc::*;
    let nullable = ncol("x", TypeCode::I64);
    let not_null = col("y", TypeCode::I64);
    let (blob, uuid, string) = (
        ncol("b", TypeCode::Blob),
        col("u", TypeCode::UUID),
        col("s", TypeCode::String),
    );
    for (func, arg, ungrouped, want) in [
        (Count, None, false, (W::Count, None)),
        (Count, Some(&nullable), false, (W::CountNonNull, None)),
        (Count, Some(&not_null), false, (W::Count, None)),
        (Count, Some(&blob), false, (W::CountNonNull, None)),
        (Avg, Some(&not_null), false, (W::Sum, Some(W::Count))),
        (Avg, Some(&nullable), false, (W::Sum, Some(W::CountNonNull))),
        (Sum, Some(&nullable), false, (W::Sum, Some(W::CountNonNull))),
        (Sum, Some(&not_null), false, (W::Sum, None)),
        (Sum, Some(&not_null), true, (W::Sum, Some(W::Count))),
        (Sum, Some(&nullable), true, (W::Sum, Some(W::CountNonNull))),
        (Min, Some(&not_null), true, (W::Min, None)),
        (Min, Some(&string), false, (W::Min, None)),
        (Max, Some(&uuid), false, (W::Max, None)),
        (Max, Some(&blob), false, (W::Max, None)),
    ] {
        assert_eq!(agg_ops(func, arg, ungrouped).unwrap(), want, "{func:?}({arg:?})");
    }
    for (func, arg, want) in [
        (Sum, &blob, "SUM: not supported on BLOB column 'b'"),
        (Avg, &uuid, "AVG: not supported on UUID column 'u'"),
        (Sum, &string, "SUM: not supported on STRING column 's'"),
    ] {
        assert_eq!(rejected(agg_ops(func, Some(arg), false)), want);
    }
}

/// The raw reduce column carries a DECIMAL argument's scale.
#[test]
fn a_decimal_aggregate_keeps_its_arguments_scale() {
    let price = ColumnDef::typed("price", ColType::decimal(2), true);
    for op in [W::Sum, W::Min, W::Max] {
        assert_eq!(agg_col_def(op, Some(&price), false).ty, ColType::decimal(2), "{op:?}");
    }
}
