use super::*;

/// A QUALIFY conjunct over placeholder `rn` bounds a top-N only as `rn <= n`,
/// `rn < n` or `rn = n`, either way round, keeping at least one slot.
#[test]
fn row_number_bound_reads_only_a_top_n_predicate() {
    let ids = ColIdGen::new();
    let (rn, other) = (ids.next(), ids.next());
    let lit = |n| BExpr::LitInt(n);
    let rn_op = |op, n| BExpr::bin(BExpr::ColRef(rn), op, lit(n));
    for (conjunct, want) in [
        (rn_op(BinOp::Le, 3), Some((0, 3))),
        (rn_op(BinOp::Lt, 3), Some((0, 2))),
        (rn_op(BinOp::Eq, 1), Some((0, 1))),
        (rn_op(BinOp::Eq, 3), Some((2, 1))),
        (BExpr::bin(lit(3), BinOp::Ge, BExpr::ColRef(rn)), Some((0, 3))),
        (BExpr::bin(lit(3), BinOp::Gt, BExpr::ColRef(rn)), Some((0, 2))),
        (BExpr::bin(lit(3), BinOp::Eq, BExpr::ColRef(rn)), Some((2, 1))),
        (rn_op(BinOp::Lt, 1), None),
        (rn_op(BinOp::Le, 0), None),
        (rn_op(BinOp::Eq, 0), None),
        (rn_op(BinOp::Eq, -2), None),
        (rn_op(BinOp::Gt, 3), None),
        (rn_op(BinOp::Ge, 3), None),
        (BExpr::bin(BExpr::ColRef(other), BinOp::Le, lit(3)), None),
    ] {
        assert_eq!(row_number_bound(&conjunct, rn), want, "{conjunct:?}");
    }
}
