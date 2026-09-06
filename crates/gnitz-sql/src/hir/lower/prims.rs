//! Circuit-emission primitives shared by the join and EXISTS/IN lowering: the
//! null-filter and reindex program builders and the source-PK rekey. Each builds
//! one program or emits at most one node — a multi-node construction belongs in
//! [`super::joincore`]. The pass-neutral key-pair validators live in
//! `hir::guards`.

use crate::error::GnitzSqlError;
use crate::expr_lower::compile_bound_expr_to_program;
use crate::ir::{BinOp, BoundExpr};
use gnitz_core::{CircuitBuilder, ColumnDef, NodeId, ReindexRole, ReindexSlot, Schema};

/// Multi-column NULL predicate for a Filter over a composite equijoin key,
/// reusing the WHERE-clause bound-expr → program path so each column index
/// maps to its payload byte. A composite key is NULL — and matches nothing
/// (SQL 3VL) — iff ANY component is NULL.
///   want_null == false: keep rows whose key is fully defined →
///                       `c0 IS NOT NULL AND … AND ck IS NOT NULL`.
///   want_null == true : keep rows whose key is NULL (LEFT-join bypass) →
///                       `c0 IS NULL OR … OR ck IS NULL`.
/// The two are exact De Morgan complements, so the LEFT-join match/bypass split
/// partitions the preserved side with no gap and no double-count. `cols` is
/// non-empty (k ≥ 1 is guaranteed by the join key classification). At k = 1 this emits
/// exactly the single-column IsNotNull/IsNull program, so existing single-key
/// plans are byte-identical.
pub(crate) fn multi_null_filter_prog(
    cols: &[usize],
    coldefs: &[ColumnDef],
    want_null: bool,
) -> Result<gnitz_expr::LogicalProgram, GnitzSqlError> {
    // Only a nullable column can satisfy IsNull or fail IsNotNull, so the NOT
    // NULL keys contribute no instruction. Every caller checks that at least
    // one key is nullable before building the gate.
    let cols: Vec<usize> = cols.iter().copied().filter(|&c| coldefs[c].is_nullable).collect();
    assert!(!cols.is_empty(), "a NULL-key gate over keys none of which is nullable");

    let leaf = |c: usize| BoundExpr::NullTest {
        inner: Box::new(BoundExpr::ColRef(c)),
        want_null,
    };
    let op = if want_null { BinOp::Or } else { BinOp::And };
    let mut expr = leaf(cols[0]);
    for &c in &cols[1..] {
        expr = BoundExpr::BinOp(Box::new(expr), op, Box::new(leaf(c)));
    }
    compile_bound_expr_to_program(&expr, coldefs)
}

/// NULL-key gate: when any of `cols` is nullable, filter NULL-keyed rows out of
/// `node` (SQL 3VL — a NULL key matches nothing); a NOT NULL key leaves the
/// node untouched (no filter operator, byte-identical circuit). Returns the
/// gated node AND the nullability fact — the callers reuse the fact for their
/// `a_all`/`b_all` reindex-node reuse and NULL-key-branch decisions, so the two
/// derivations cannot drift (a drift would be a silent weight bug in the
/// null-fill).
pub(crate) fn null_gate(
    cb: &mut CircuitBuilder,
    node: NodeId,
    cols: &[usize],
    coldefs: &[ColumnDef],
) -> Result<(NodeId, bool), GnitzSqlError> {
    let nullable = cols.iter().any(|&c| coldefs[c].is_nullable);
    let gated = if nullable {
        cb.filter(node, Some(multi_null_filter_prog(cols, coldefs, false)?))
    } else {
        node
    };
    Ok((gated, nullable))
}
/// A reindex key over `cols` with every slot self-derived (`MapKind::Reindex`'s
/// `None`) — the shape of every re-key that moves rows without widening them.
pub(crate) fn self_derived_key(cols: &[usize]) -> Vec<ReindexSlot> {
    cols.iter().map(|&c| (c as u32, None)).collect()
}

/// [`self_derived_key`] over a schema's own PK column list.
pub(crate) fn self_derived_pk_key(schema: &Schema) -> Vec<ReindexSlot> {
    schema.pk_cols.iter().map(|&c| (c, None)).collect()
}

/// Re-key `node` onto its own source PK, self-deriving each key slot's type and
/// keeping `keep` as payload; `keep` indexes the *source* row, as the key does.
/// One home, so two operands of a null-fill subtraction cannot drift apart.
fn rekey_on_source_pk(
    cb: &mut CircuitBuilder,
    node: NodeId,
    schema: &Schema,
    keep: &[u32],
    role: ReindexRole,
) -> NodeId {
    cb.map_reindex(node, &self_derived_pk_key(schema), keep, role)
}

/// [`rekey_on_source_pk`] for an internal operand: the `P_all` of a null-fill's
/// `positive_part(P_all − π_P(inner))`, or the NULL-key bypass.
pub(crate) fn rekey_aux_on_source_pk(cb: &mut CircuitBuilder, node: NodeId, schema: &Schema, keep: &[u32]) -> NodeId {
    rekey_on_source_pk(cb, node, schema, keep, ReindexRole::Auxiliary)
}

/// [`rekey_on_source_pk`] as a source's route key — the reindex the relay reads a
/// scan's scatter key off. A source whose every reindex is `Auxiliary` is refused,
/// so this is what a keyless join's per-side trace key must use.
pub(crate) fn rekey_scatter_on_source_pk(
    cb: &mut CircuitBuilder,
    node: NodeId,
    schema: &Schema,
    keep: &[u32],
) -> NodeId {
    rekey_on_source_pk(cb, node, schema, keep, ReindexRole::ScatterKey)
}

#[cfg(test)]
#[path = "tests/prims.rs"]
mod tests;
