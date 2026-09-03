use super::*;
use crate::test_support::col_def;

// Columns: 0=pk(U64), 1=n(I64), 2=b(Blob), 3=u(UUID), 4=s(String).
fn schema() -> Schema {
    Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("n", TypeCode::I64, true),
            col_def("b", TypeCode::Blob, true),
            col_def("u", TypeCode::UUID, true),
            col_def("s", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    }
}

fn try_push(func: AggFunc, arg_col: Option<usize>) -> Result<AggShape, GnitzSqlError> {
    let mut specs = Vec::new();
    push_agg_specs(func, arg_col, &schema().columns, &mut specs).map(|t| t.shape)
}

#[test]
fn push_agg_specs_rejects_unevaluatable_arg_types() {
    for (func, ci) in [
        (AggFunc::Sum, 2), // SUM(blob)
        (AggFunc::Avg, 3), // AVG(uuid)
        (AggFunc::Min, 4), // MIN(str)
        (AggFunc::Max, 2), // MAX(blob)
    ] {
        assert!(matches!(try_push(func, Some(ci)), Err(GnitzSqlError::Unsupported(_))));
    }
}

#[test]
fn push_agg_specs_accepts_valid_arg_types() {
    assert!(try_push(AggFunc::Sum, Some(1)).is_ok()); // SUM(i64)
    assert!(try_push(AggFunc::Avg, Some(1)).is_ok()); // AVG(i64)
    assert!(try_push(AggFunc::Min, Some(1)).is_ok()); // MIN(i64)
    assert!(try_push(AggFunc::Count, None).is_ok()); // COUNT(*)
    assert!(try_push(AggFunc::CountNonNull, Some(2)).is_ok()); // COUNT(blob) — presence only
}

/// SUM over a U64 source is typed U64 (bit pattern is the correct unsigned
/// sum), so a downstream unsigned compare re-seeds; a narrow unsigned / signed
/// source widens to I64. MIN/MAX preserve the U64 source type as before.
#[test]
fn agg_result_type_sum_preserves_u64() {
    let s = Schema {
        columns: vec![
            col_def("pk", TypeCode::U64, false),
            col_def("u", TypeCode::U64, true),
            col_def("w", TypeCode::U32, true),
            col_def("i", TypeCode::I64, true),
            col_def("f", TypeCode::F64, true),
        ],
        pk_cols: vec![0],
    };
    // The raw reduce value type; only AVG (untested here) diverges from it, and
    // `HirAgg::view_type` is what renders that.
    let rt = |f, i: usize| agg_typing(f, Some(&s.columns[i])).unwrap().ops[0].1;
    assert_eq!(rt(AggFunc::Sum, 1), TypeCode::U64); // SUM(u64) → U64
    assert_eq!(rt(AggFunc::Sum, 2), TypeCode::I64); // SUM(u32) → I64
    assert_eq!(rt(AggFunc::Sum, 3), TypeCode::I64); // SUM(i64) → I64
    assert_eq!(rt(AggFunc::Sum, 4), TypeCode::F64); // SUM(f64) → F64
    assert_eq!(rt(AggFunc::Min, 1), TypeCode::U64);
    // MIN(u64) preserved
}

/// The unaliased aggregate column names are user-visible in the view schema
/// (e2e-pinned), so the derivation from the canonical name table must render
/// exactly these — in particular both COUNT shapes share `_count`.
#[test]
fn default_agg_name_renders_pinned_view_column_names() {
    assert_eq!(default_agg_name(AggFunc::Count, 0), "_count0");
    assert_eq!(default_agg_name(AggFunc::CountNonNull, 0), "_count0");
    assert_eq!(default_agg_name(AggFunc::Sum, 1), "_sum1");
    assert_eq!(default_agg_name(AggFunc::Min, 2), "_min2");
    assert_eq!(default_agg_name(AggFunc::Max, 3), "_max3");
    assert_eq!(default_agg_name(AggFunc::Avg, 4), "_avg4");
}

// The Direct-aggregate nullability decision shared by the SELECT projection
// (output-schema nullability) and the HAVING `IS [NOT] NULL` const-fold — and,
// through the same shared rule, by the engine's physical reduce output schema.
#[test]
fn raw_output_nullable_matches_emit_semantics() {
    use WireAggFunc as W;
    // COUNT / COUNT_NON_NULL / SumZero: always a concrete integer.
    for f in [W::Count, W::CountNonNull, W::SumZero] {
        for src_nullable in [false, true] {
            for ungrouped in [false, true] {
                assert!(!f.raw_output_nullable(src_nullable, ungrouped), "{f:?}");
            }
        }
    }
    // SUM / MIN / MAX: NULL over an empty group set (the ground row stands in
    // for a never-populated source), or grouped over a nullable source (an
    // all-NULL group). Grouped over a non-nullable source never renders NULL —
    // that is what keeps such a reduce on the null-blind fixed-int comparator.
    for f in [W::Sum, W::Min, W::Max] {
        assert!(!f.raw_output_nullable(false, false), "{f:?} grouped, non-nullable");
        assert!(f.raw_output_nullable(true, false), "{f:?} grouped, nullable");
        assert!(f.raw_output_nullable(false, true), "{f:?} global, non-nullable");
        assert!(f.raw_output_nullable(true, true), "{f:?} global, nullable");
    }
}
