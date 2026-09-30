use super::*;

/// A QUALIFY over placeholder `rn` is a top-N bound only as `rn <= n`, `rn < n`
/// or `rn = 1`, either way round, selecting at least one row.
#[test]
fn row_number_bound_reads_only_a_top_n_predicate() {
    let ids = ColIdGen::new();
    let (rn, other) = (ids.next(), ids.next());
    let lit = |n| BExpr::LitInt(n);
    let rn_op = |op, n| BExpr::bin(BExpr::ColRef(rn), op, lit(n));
    for (qualify, want) in [
        (rn_op(BinOp::Le, 3), Some(3)),
        (rn_op(BinOp::Lt, 3), Some(2)),
        (rn_op(BinOp::Eq, 1), Some(1)),
        (BExpr::bin(lit(3), BinOp::Ge, BExpr::ColRef(rn)), Some(3)),
        (BExpr::bin(lit(3), BinOp::Gt, BExpr::ColRef(rn)), Some(2)),
        (rn_op(BinOp::Lt, 1), None),
        (rn_op(BinOp::Le, 0), None),
        (rn_op(BinOp::Eq, 2), None),
        (rn_op(BinOp::Ge, 3), None),
        (BExpr::bin(BExpr::ColRef(other), BinOp::Le, lit(3)), None),
    ] {
        assert_eq!(row_number_bound(&qualify, rn), want, "{qualify:?}");
    }
}
