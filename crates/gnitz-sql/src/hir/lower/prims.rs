//! Circuit-emission primitives shared by the join and EXISTS/IN lowering
//! (`join.rs`, `exists.rs`): the null-filter and reindex program builders, the
//! source-PK rekey, and the small schema helpers they emit against. The
//! pass-neutral key-pair validators live in `hir::guards`.

use crate::error::GnitzSqlError;
use crate::expr_lower::compile_bound_expr_to_program;
use crate::ir::{BinOp, BoundExpr};
use gnitz_core::{CircuitBuilder, ColumnDef, NodeId, ReindexRole, Schema, TypeCode};

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
    // Caller invariant: cols is non-empty (at least one join key column) AND at
    // least one entry is nullable (the outer guard in the join lowering
    // ensures both). Guard here so future callers fail loudly rather than
    // panicking at cols[0].
    if cols.is_empty() {
        return Err(GnitzSqlError::Plan(
            "multi_null_filter_prog: column list cannot be empty".into(),
        ));
    }
    // Only nullable columns can ever satisfy IsNull or fail IsNotNull, so drop the
    // NOT NULL columns to elide tautological (want_null=false) / contradictory
    // (want_null=true) filter instructions. The caller (the join lowering)
    // only reaches this with ≥ 1 nullable key column, so `nullable` is non-empty on
    // every real path; the `is_empty` fallback to the unfiltered `cols` still
    // degrades correctly should that ever change — with every key NOT NULL,
    // `c IS NOT NULL` is a tautology (keep all rows) and `c IS NULL` a contradiction
    // (drop all), exactly right when no key can be NULL.
    let nullable: Vec<usize> = cols.iter().copied().filter(|&c| coldefs[c].is_nullable).collect();
    let cols = if nullable.is_empty() { cols } else { &nullable[..] };

    let leaf = |c: usize| {
        if want_null {
            BoundExpr::IsNull(c)
        } else {
            BoundExpr::IsNotNull(c)
        }
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

/// The reindex kept-column list that prunes nothing — every source column
/// survives as payload, in order.
pub(crate) fn keep_all(n_cols: usize) -> Vec<u32> {
    (0..n_cols as u32).collect()
}

/// Re-key `node` onto its own source PK, payload verbatim — the `P_all` operand of
/// an outer null-fill's `positive_part(P_all − π_P(inner))`, and the NULL-key
/// bypass re-key. The target type codes are all-zero (self-derive): the source PK
/// columns already carry their own types, so the re-key is width- and sign-exact.
/// One home, so every null-fill's preserved side is keyed identically to the
/// `π_P(inner)` it is subtracted from — a drift there would be a silent weight bug.
///
pub(crate) fn rekey_on_source_pk(cb: &mut CircuitBuilder, node: NodeId, schema: &Schema) -> NodeId {
    cb.map_reindex(
        node,
        &schema.pk_cols,
        &[],
        &keep_all(schema.columns.len()),
        ReindexRole::Auxiliary,
    )
}

/// A schema's column type codes in order — the `null_extend` argument naming the
/// NULL columns to append for the non-preserved side of an outer-join null-fill.
pub(crate) fn schema_type_codes(coldefs: &[ColumnDef]) -> Vec<u64> {
    coldefs.iter().map(|c| c.type_code as u64).collect()
}

/// Output columns of the inline pure-range-LEFT threshold `m`: the scalar
/// (no-GROUP-BY) reduce emits a synthetic `_group_pk` (U128) plus the single
/// aggregate column. Non-float MIN/MAX carries its source type
/// (`compiler::agg_output_type`), and the compare type `Tc` is capped to a ≤8-byte
/// integer (the pure-range LEFT cap; the 8-byte accumulator emits at `Tc`'s native
/// width), so the reduce emits the value already typed `Tc`. Declaring `m` as `Tc`
/// here lets `reindex_m` self-derive the `Tc` OPK order directly off the reduce
/// output — no relabel.
pub(crate) fn pure_range_m_output_cols(tc: TypeCode) -> Vec<ColumnDef> {
    vec![
        ColumnDef::new("_group_pk", TypeCode::U128, false),
        ColumnDef::new("m", tc, false),
    ]
}

#[cfg(test)]
#[path = "tests/prims.rs"]
mod tests;
