use super::*;
use crate::test_support::parse_expr_sql;

/// An aggregate is found wherever it is written outside a subquery's body, and a
/// subquery's own aggregate is not this body's.
#[test]
fn expr_has_aggregate_searches_every_position_outside_a_subquery() {
    for (src, want) in [
        ("(SUM(x) > 0) IS TRUE", true),
        ("COUNT(*) OVER (PARTITION BY a ORDER BY SUM(x))", true),
        ("SUM(x) OVER ()", false),
        ("(SELECT SUM(x) FROM t)", false),
        ("EXISTS (SELECT SUM(x) FROM t)", false),
        ("a IN (SELECT SUM(x) FROM t)", false),
        ("a > ALL (SELECT SUM(x) FROM t)", false),
        ("(SELECT MAX(x) FROM t) + SUM(y)", true),
        ("SUM(y) + (SELECT MAX(x) FROM t)", true),
    ] {
        assert_eq!(expr_has_aggregate(&parse_expr_sql(src)), want, "{src}");
    }
}
