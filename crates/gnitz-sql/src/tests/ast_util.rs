use super::*;
use crate::test_support::parse_expr_sql;

/// CEIL/FLOOR/CAST reach the binder as dedicated AST nodes rather than as
/// function calls, so [`expr_operands`] has to name their operand. Falling
/// into the wildcard would make each of these look aggregate-free and route
/// the query to the scalar path.
#[test]
fn expr_operands_reaches_through_the_dedicated_nodes() {
    for src in [
        "CAST(SUM(x) AS INT)",
        "SUM(x)::INT",
        "TRY_CAST(SUM(x) AS INT)",
        "CEIL(SUM(x))",
        "FLOOR(SUM(x))",
        "ABS(CAST(SUM(x) AS INT))",
    ] {
        assert!(expr_has_aggregate(&parse_expr_sql(src)), "{src}");
        let mut seen = 0usize;
        for_each_agg_call::<()>(&parse_expr_sql(src), &mut |_| {
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1, "{src}: the aggregate must be collected exactly once");
    }
}

/// Same walker, the subquery consumers: a subquery under a CAST must still
/// route the view to the subquery-bearing builder.
#[test]
fn expr_operands_exposes_a_subquery_under_a_cast() {
    assert!(expr_any(
        &parse_expr_sql("CAST((SELECT 1) AS INT)"),
        &is_scalar_subquery
    ));
    assert!(expr_any(
        &parse_expr_sql("CAST(x AS INT) IN (SELECT y FROM t)"),
        &is_exists_in
    ));
}
