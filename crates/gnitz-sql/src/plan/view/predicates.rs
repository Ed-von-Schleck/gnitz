//! JOIN ON-clause classification: equality-prefix key pairs, the single optional
//! range/band conjunct, and the residual filter. The shared substrate for both
//! the equi and range join builders (`join.rs`), plus the key-pair type
//! validators, the reindex/null-filter program builders, and the `JoinResidual`
//! leaf binder.

use crate::bind::{
    bind_structural, fold_null_test, resolve_qualified_column, resolve_unqualified_column, AliasMap, LeafBinder,
};
use crate::error::GnitzSqlError;
use crate::ir::{BinOp, BoundExpr};
use crate::lower::compile_bound_expr;
use crate::plan::validate::reject_float_key;
use gnitz_core::{ColumnDef, ExprBuilder, RangeRel, Schema, TypeCode};
use sqlparser::ast::{BinaryOperator, Expr};

#[cfg(test)]
use crate::bind::ResolvedRelation;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::rc::Rc;

/// Multi-column NULL predicate for a Filter over a composite equijoin key,
/// reusing the WHERE-clause bound-expr → ExprProgram path so each column index
/// maps to its payload byte. A composite key is NULL — and matches nothing
/// (SQL 3VL) — iff ANY component is NULL.
///   want_null == false: keep rows whose key is fully defined →
///                       `c0 IS NOT NULL AND … AND ck IS NOT NULL`.
///   want_null == true : keep rows whose key is NULL (LEFT-join bypass) →
///                       `c0 IS NULL OR … OR ck IS NULL`.
/// The two are exact De Morgan complements, so the LEFT-join match/bypass split
/// partitions the preserved side with no gap and no double-count. `cols` is
/// non-empty (k ≥ 1 is guaranteed by extract_join_predicates). At k = 1 this emits
/// exactly the single-column IsNotNull/IsNull program, so existing single-key
/// plans are byte-identical.
pub(crate) fn multi_null_filter_prog(
    cols: &[usize],
    schema: &Schema,
    want_null: bool,
) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
    // Caller invariant: cols is non-empty (at least one join key column) AND at
    // least one entry is nullable (the outer guard in execute_create_join_view
    // ensures both). Guard here so future callers fail loudly rather than
    // panicking at cols[0].
    if cols.is_empty() {
        return Err(GnitzSqlError::Plan(
            "multi_null_filter_prog: column list cannot be empty".into(),
        ));
    }
    // Only nullable columns can ever satisfy IsNull or fail IsNotNull, so drop the
    // NOT NULL columns to elide tautological (want_null=false) / contradictory
    // (want_null=true) filter instructions. The caller (execute_create_join_view)
    // only reaches this with ≥ 1 nullable key column, so `nullable` is non-empty on
    // every real path; the `is_empty` fallback to the unfiltered `cols` still
    // degrades correctly should that ever change — with every key NOT NULL,
    // `c IS NOT NULL` is a tautology (keep all rows) and `c IS NULL` a contradiction
    // (drop all), exactly right when no key can be NULL.
    let nullable: Vec<usize> = cols
        .iter()
        .copied()
        .filter(|&c| schema.columns[c].is_nullable)
        .collect();
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
    let mut eb = ExprBuilder::new();
    let (r, _) = compile_bound_expr(&expr, schema, &mut eb)?;
    Ok(eb.build(r))
}

/// Build a reindex ExprProgram that copies all columns as payload. Arity-
/// independent: it copies every source column to payload offsets `0..n`, and
/// `reindex_output_schema` places those payload columns at physical indices
/// `k..k+n` regardless of the key arity `k` (the `k` PK slots precede them), so
/// the payload offsets never shift with the number of key columns.
pub(crate) fn build_reindex_program(schema: &Schema) -> gnitz_core::ExprProgram {
    let mut eb = ExprBuilder::new();
    for (ci, col) in schema.columns.iter().enumerate() {
        let tc = col.type_code as u32;
        eb.copy_col(tc, ci as u32, ci as u32);
    }
    eb.build(0) // result_reg unused — COPY_COL writes directly
}

/// Validate one equijoin key pair and return the pair's common reindex output
/// type `T`. Float keys are rejected outright (IEEE-754 -0.0/+0.0 compare
/// byte-unequal and NaN has no canonical form). A German string (STRING/BLOB) may
/// only join another German string: it reindexes to a 16-byte content hash, which
/// is byte-incompatible with the native U128/UUID encoding even though both
/// collapse to the U128 output type. The remaining pairs are resolved by
/// `join_key_common_type`, which promotes integers of different widths (and
/// different sign classes, as long as the unsigned side is ≤ 8 bytes (`U64`)) to a
/// common type that faithfully holds both ranges, so the two reindex sides
/// co-partition byte-for-byte and the `_join_pk` catalog stride matches both. Only
/// a cross-sign pair whose unsigned side is 128-bit (`U128`/`UUID`) stays
/// rejected — its faithful common type is a signed-256 type that does not exist.
fn validate_join_key_pair(left: &ColumnDef, right: &ColumnDef) -> Result<u8, GnitzSqlError> {
    for col in [left, right] {
        reject_float_key(col, "JOIN ON")?;
    }
    // STRING/BLOB reindex to a 16-byte XXH3 content hash; U128/UUID reindex to the
    // 16-byte native value. Both collapse to the U128 output type, so
    // `join_key_common_type` cannot tell them apart — but a content hash never
    // equals a native integer, so the join would silently match nothing.
    if left.type_code.is_german_string() != right.type_code.is_german_string() {
        return Err(GnitzSqlError::Unsupported(format!(
            "JOIN ON: cannot equijoin string/blob column '{}' ({:?}) with non-string \
             column '{}' ({:?}); a string content hash never matches a native key",
            left.name, left.type_code, right.name, right.type_code
        )));
    }
    left.type_code
        .join_key_common_type(right.type_code)
        .map(|t| t as u8)
        .ok_or_else(|| {
            GnitzSqlError::Unsupported(format!(
                "JOIN ON: join key columns '{}' ({:?}) and '{}' ({:?}) cannot co-partition; \
             a cross-sign pair whose unsigned side is 128-bit (e.g. DECIMAL(38,0)/UUID \
             joined with a signed integer) needs a signed-256 type that does not exist",
                left.name, left.type_code, right.name, right.type_code
            ))
        })
}

/// Validate the range conjunct's key pair and return its common reindex output
/// type `T`. A range bound must be order-preserving: STRING/BLOB reindex to a
/// 16-byte content hash that is equality-correct but NOT order-preserving, so
/// they are rejected here (they remain legal in the equality prefix). Floats are
/// rejected by `validate_join_key_pair`, which then resolves the common integer
/// type via `join_key_common_type` (cross-sign promotion included).
fn validate_range_join_key_pair(left: &ColumnDef, right: &ColumnDef) -> Result<u8, GnitzSqlError> {
    for col in [left, right] {
        if col.type_code.is_german_string() {
            return Err(GnitzSqlError::Unsupported(format!(
                "range join key column '{}' ({:?}): a string/blob content hash is not \
                 order-preserving and cannot bound a range conjunct",
                col.name, col.type_code
            )));
        }
    }
    validate_join_key_pair(left, right)
}

/// Map a SQL comparison operator to its `RangeRel`, or `None` for a non-range
/// operator (`=`, `AND`, arithmetic, …).
fn sql_binop_to_range_rel(op: &BinaryOperator) -> Option<RangeRel> {
    match op {
        BinaryOperator::Lt => Some(RangeRel::Lt),
        BinaryOperator::LtEq => Some(RangeRel::Le),
        BinaryOperator::Gt => Some(RangeRel::Gt),
        BinaryOperator::GtEq => Some(RangeRel::Ge),
        _ => None,
    }
}

/// The order-reversing converse of a `RangeRel` (`x OP y` ⟺ `y converse(OP) x`).
/// Used both to canonicalize a right-table-first range conjunct to left-first and
/// to derive term AB's rel from the canonical OP (§3 table).
pub(crate) fn converse_rel(r: RangeRel) -> RangeRel {
    match r {
        RangeRel::Lt => RangeRel::Gt,
        RangeRel::Le => RangeRel::Ge,
        RangeRel::Gt => RangeRel::Lt,
        RangeRel::Ge => RangeRel::Le,
    }
}

/// Classify two global column indices `l`/`r` as a cross-table pair. Returns
/// `(left_idx, right_idx_rel, swapped)` when they reference *different* tables —
/// `left_idx` table-relative to the left schema, `right_idx_rel` relative to the
/// right — and `None` for a same-table or same-side pair. `swapped` is true when
/// the *right* table's column was the left operand (`b.y OP a.x`), which drives
/// `converse_rel` in the range arm.
fn cross_table_pair(l: usize, r: usize, left_n: usize) -> Option<(usize, usize, bool)> {
    if l < left_n && r >= left_n {
        Some((l, r - left_n, false))
    } else if r < left_n && l >= left_n {
        Some((r, l - left_n, true))
    } else {
        None
    }
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
        ColumnDef {
            name: "_group_pk".into(),
            type_code: TypeCode::U128,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
        },
        ColumnDef {
            name: "m".into(),
            type_code: tc,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
        },
    ]
}

/// The single range conjunct of a band/range join, canonicalized to
/// `left_col OP right_col` (both table-relative). `op` is the canonical OP as a
/// `RangeRel`; `tc` is the pair's common reindex output type.
#[derive(Debug)]
pub(crate) struct RangeConjunct {
    pub(crate) left_col: usize,
    pub(crate) right_col: usize,
    pub(crate) op: RangeRel,
    pub(crate) tc: u8,
}

/// The full ON-clause classification: per-position-paired equality-prefix key
/// columns (`left_cols`, `right_cols`) with their per-pair common reindex output
/// type (`target_tcs`), an optional single range conjunct, and the residual list
/// — every other ON conjunct (a second range, `<>`, a column-vs-literal test, a
/// same-table or arithmetic comparison, an OR/IS NULL group), evaluated as a
/// linear post-join `Filter` for INNER joins (§6).
pub(crate) type JoinPredicates = (Vec<usize>, Vec<usize>, Vec<u8>, Option<RangeConjunct>, Vec<Expr>);

/// Classify a JOIN ON clause into its equality-prefix pairs and an optional
/// single range conjunct. A pure superset of the old equi-only extraction: with
/// zero range conjuncts the equality pairs (and therefore the equi circuit and
/// its serialization) are byte-for-byte unchanged. Returns
/// `(left_eq, right_eq, eq_tcs, range)`, all table-relative and position-paired.
pub(crate) fn extract_join_predicates(
    on_expr: &Expr,
    left_schema: &Schema,
    right_schema: &Schema,
    alias_map: &AliasMap,
) -> Result<JoinPredicates, GnitzSqlError> {
    let mut left_cols = Vec::new();
    let mut right_cols = Vec::new();
    let mut target_tcs = Vec::new();
    let mut range: Option<RangeConjunct> = None;
    let mut residual: Vec<Expr> = Vec::new();
    collect_join_predicates(
        on_expr,
        left_schema,
        right_schema,
        alias_map,
        &mut left_cols,
        &mut right_cols,
        &mut target_tcs,
        &mut range,
        &mut residual,
    )?;
    if left_cols.is_empty() && range.is_none() {
        // A residual cannot stand alone: a residual-only ON (`ON a.r <> b.s`) would
        // be an incremental cross-join, which the engine cannot build. Residuals
        // are only ever evaluated alongside a physical equi/range anchor (§3).
        return Err(GnitzSqlError::Bind(
            "JOIN ON must have at least one equijoin or range predicate".into(),
        ));
    }
    // Reindex-slot arity cap: each equality pair plus the optional range slot
    // becomes one synthetic `_join_pk` PK-list slot, and the codec holds at most
    // PK_LIST_MAX_COLS. Reject a wider ON here as a clean planner error rather than
    // a `pack_pk_cols` panic at registration. (The output pair-PK has its own cap,
    // checked in the range circuit builder.)
    let slots = left_cols.len() + range.is_some() as usize;
    if slots > gnitz_core::PK_LIST_MAX_COLS {
        return Err(GnitzSqlError::Unsupported(if range.is_none() {
            format!(
                "JOIN ON: at most {} equijoin key columns are supported (got {})",
                gnitz_core::PK_LIST_MAX_COLS,
                left_cols.len()
            )
        } else {
            format!(
                "range JOIN ON: at most {} join key columns (equality prefix + \
                     range) are supported (got {})",
                gnitz_core::PK_LIST_MAX_COLS,
                slots
            )
        }));
    }
    Ok((left_cols, right_cols, target_tcs, range, residual))
}

#[allow(clippy::too_many_arguments)]
fn collect_join_predicates(
    expr: &Expr,
    left_schema: &Schema,
    right_schema: &Schema,
    alias_map: &AliasMap,
    left_cols: &mut Vec<usize>,
    right_cols: &mut Vec<usize>,
    target_tcs: &mut Vec<u8>,
    range: &mut Option<RangeConjunct>,
    residual: &mut Vec<Expr>,
) -> Result<(), GnitzSqlError> {
    match expr {
        // Parentheses: `ON (a.x = b.x) AND a.y = b.y`, `ON (a.x = b.x AND a.y = b.y)`.
        // sqlparser wraps a parenthesized sub-expression in Expr::Nested; unwrap it
        // so grouping never changes which predicates are extracted. This mirrors the
        // WHERE binder (`binder.rs` Expr::Nested arm) and the HAVING path, both of
        // which already unwrap Nested.
        Expr::Nested(inner) => {
            collect_join_predicates(
                inner,
                left_schema,
                right_schema,
                alias_map,
                left_cols,
                right_cols,
                target_tcs,
                range,
                residual,
            )?;
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                collect_join_predicates(
                    left,
                    left_schema,
                    right_schema,
                    alias_map,
                    left_cols,
                    right_cols,
                    target_tcs,
                    range,
                    residual,
                )?;
                collect_join_predicates(
                    right,
                    left_schema,
                    right_schema,
                    alias_map,
                    left_cols,
                    right_cols,
                    target_tcs,
                    range,
                    residual,
                )?;
            }
            _ => {
                // Classify a non-AND conjunct. It is consumed as an equi key pair or
                // the one physical range conjunct ONLY when both operands resolve to
                // columns of different tables; everything else — a literal operand, a
                // same-table pair, an arithmetic operand, a second range, `<>`, an
                // OR/IS NULL group — falls through to the residual. The residual binder
                // re-resolves and surfaces any genuine "column not found".
                let left_n = left_schema.columns.len();
                let cross = match (
                    resolve_join_col_ref(left, alias_map),
                    resolve_join_col_ref(right, alias_map),
                ) {
                    (Ok(l), Ok(r)) => cross_table_pair(l, r, left_n),
                    _ => None, // a literal/expression operand, or unresolved
                };
                match (op, cross) {
                    // Cross-table column equality → equijoin key pair, validated as
                    // today. A mixed/incompatible pair still errors via
                    // validate_join_key_pair — it is NOT demoted to a residual (§3).
                    (BinaryOperator::Eq, Some((li, ri, _swapped))) => {
                        // Drop an exact-duplicate pair (`a.x = b.x AND a.x = b.x`, or
                        // the same pair sides-swapped): byte-identical key slots,
                        // keeping them only widens the synthetic PK and can spuriously
                        // trip the arity cap. A pair sharing one column but not the
                        // other (`a.x = b.x AND a.x = b.y`) is distinct and kept.
                        if left_cols
                            .iter()
                            .zip(right_cols.iter())
                            .any(|(&pl, &pr)| pl == li && pr == ri)
                        {
                            return Ok(());
                        }
                        // Push T only on the same path that pushes (li, ri) — after
                        // the dup early-return — so the three vectors stay parallel.
                        let t = validate_join_key_pair(&left_schema.columns[li], &right_schema.columns[ri])?;
                        left_cols.push(li);
                        right_cols.push(ri);
                        target_tcs.push(t);
                    }
                    // Cross-table column ordering comparison, range slot still free →
                    // the one physical range conjunct, canonicalized to `left_col OP
                    // right_col` (converse on a right-table-first operand). A
                    // non-order-preserving pair (string/blob/float) is rejected by
                    // validate_range_join_key_pair — it never silently becomes a
                    // residual. A *second* ordering comparison (range already set)
                    // fails this guard and falls through to the residual.
                    (_, Some((li, ri, swapped))) if sql_binop_to_range_rel(op).is_some() && range.is_none() => {
                        let rel = sql_binop_to_range_rel(op).expect("range rel present");
                        let canon_op = if swapped { converse_rel(rel) } else { rel };
                        let t = validate_range_join_key_pair(&left_schema.columns[li], &right_schema.columns[ri])?;
                        *range = Some(RangeConjunct {
                            left_col: li,
                            right_col: ri,
                            op: canon_op,
                            tc: t,
                        });
                    }
                    // Anything else (2nd range, `<>`, col-vs-literal, same-table,
                    // arithmetic operand) → residual.
                    _ => residual.push(expr.clone()),
                }
            }
        },
        // A non-binary, non-Nested top-level conjunct (IS [NOT] NULL, an OR group, a
        // function, …) → residual.
        _ => residual.push(expr.clone()),
    }
    Ok(())
}

/// Resolve a column reference in a JOIN ON clause to a global column index.
fn resolve_join_col_ref(expr: &Expr, alias_map: &AliasMap) -> Result<usize, GnitzSqlError> {
    match expr {
        Expr::Nested(inner) => resolve_join_col_ref(inner, alias_map),
        Expr::Identifier(ident) => resolve_unqualified_column(&ident.value, alias_map),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            resolve_qualified_column(&parts[0].value, &parts[1].value, alias_map)
        }
        _ => Err(GnitzSqlError::Unsupported(
            "JOIN ON: only column references supported".to_string(),
        )),
    }
}

/// `LeafBinder` for a residual JOIN-ON predicate (§6.2). Columns resolve through
/// the join `alias_map` to a global A‖B index `g`, then shift into the merged
/// join-output layout `[_join_pk × k, A, B]` by `+ base` (`base = k`). Null tests
/// fold against `merged`, which carries each base column's real nullability for an
/// INNER join (the equi `out_cols` / range `union_cols` clone the source
/// `ColumnDef`s). Aggregates in ON are rejected outright.
struct JoinResidual<'a> {
    alias_map: &'a AliasMap,
    merged: &'a Schema,
    base: usize,
}

impl JoinResidual<'_> {
    fn idx(&self, e: &Expr) -> Result<usize, GnitzSqlError> {
        Ok(self.base + resolve_join_col_ref(e, self.alias_map)?) // unwraps Nested
    }
}

impl LeafBinder for JoinResidual<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        Ok(BoundExpr::ColRef(self.idx(e)?))
    }
    fn bind_function(&self, _f: &sqlparser::ast::Function) -> Result<BoundExpr, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported(
            "JOIN ON: aggregate functions are not allowed".into(),
        ))
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
        Ok(fold_null_test(self.merged, self.idx(inner)?, want_null))
    }
}

/// AND every residual conjunct (bound against the merged join-output schema via
/// the `JoinResidual` leaf) into one `ExprProgram`. `residual` is non-empty —
/// callers guard, and the residual is only spliced for INNER joins (§6.3). The
/// filter ANDs the conjuncts, so the ON left-to-right collection order is
/// immaterial to the result.
pub(crate) fn build_residual_filter_prog(
    residual: &[Expr],
    alias_map: &AliasMap,
    merged_schema: &Schema,
    payload_base: usize,
) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
    let leaf = JoinResidual {
        alias_map,
        merged: merged_schema,
        base: payload_base,
    };
    let mut acc: Option<BoundExpr> = None;
    for e in residual {
        let b = bind_structural(e, &leaf)?;
        acc = Some(match acc {
            None => b,
            Some(a) => BoundExpr::BinOp(Box::new(a), BinOp::And, Box::new(b)),
        });
    }
    let mut eb = ExprBuilder::new();
    let (r, _) = compile_bound_expr(&acc.expect("non-empty residual"), merged_schema, &mut eb)?;
    Ok(eb.build(r))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Multi-column equijoin key extraction / validation ────────────────────
    //
    // These exercise the k ≥ 2 logic directly: extract_join_predicates and
    // validate_join_key_pair return their result *before* the `k > 1` planner
    // gate in execute_create_join_view, so a composite key is fully testable
    // here even though CREATE VIEW still rejects it end-to-end.

    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn col(name: &str, tc: TypeCode, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            type_code: tc,
            is_nullable: nullable,
            fk_table_id: 0,
            fk_col_idx: 0,
        }
    }

    fn parse_on(src: &str) -> Expr {
        Parser::new(&GenericDialect {})
            .try_with_sql(src)
            .unwrap()
            .parse_expr()
            .unwrap()
    }

    /// Build the (left_schema, right_schema, alias_map) trio the way
    /// `execute_create_join_view` does, for aliases `a` (left) / `b` (right).
    fn join_ctx(left: Vec<ColumnDef>, right: Vec<ColumnDef>) -> (Schema, Schema, AliasMap) {
        let left_n = left.len();
        let left_schema = Schema {
            columns: left,
            pk_cols: vec![0],
        };
        let right_schema = Schema {
            columns: right,
            pk_cols: vec![0],
        };
        let mut am = HashMap::new();
        am.insert(
            "a".to_string(),
            ResolvedRelation {
                table_id: 1,
                schema: Rc::new(left_schema.clone()),
                col_offset: 0,
            },
        );
        am.insert(
            "b".to_string(),
            ResolvedRelation {
                table_id: 2,
                schema: Rc::new(right_schema.clone()),
                col_offset: left_n,
            },
        );
        (left_schema, right_schema, am)
    }

    fn extract(
        on: &str,
        left: Vec<ColumnDef>,
        right: Vec<ColumnDef>,
    ) -> Result<(Vec<usize>, Vec<usize>), GnitzSqlError> {
        let (ls, rs, am) = join_ctx(left, right);
        extract_join_predicates(&parse_on(on), &ls, &rs, &am).map(|(l, r, _, _, _)| (l, r))
    }

    /// The equi-join half of `JoinPredicates` (no range conjunct): paired key
    /// columns + per-pair common reindex type. Named for legible helper returns.
    type EquiKeys = (Vec<usize>, Vec<usize>, Vec<u8>);

    /// Like `extract` but also returns the per-pair common reindex output type
    /// `T`, for cross-width promotion assertions. Asserts no range conjunct.
    fn extract_with_tcs(on: &str, left: Vec<ColumnDef>, right: Vec<ColumnDef>) -> Result<EquiKeys, GnitzSqlError> {
        let (ls, rs, am) = join_ctx(left, right);
        extract_join_predicates(&parse_on(on), &ls, &rs, &am).map(|(l, r, t, range, _)| {
            assert!(range.is_none(), "expected no range conjunct");
            (l, r, t)
        })
    }

    /// Full classification including the optional range conjunct.
    fn extract_full(on: &str, left: Vec<ColumnDef>, right: Vec<ColumnDef>) -> Result<JoinPredicates, GnitzSqlError> {
        let (ls, rs, am) = join_ctx(left, right);
        extract_join_predicates(&parse_on(on), &ls, &rs, &am)
    }

    fn two_u64() -> Vec<ColumnDef> {
        vec![col("x", TypeCode::U64, false), col("y", TypeCode::U64, false)]
    }

    #[test]
    fn extract_keys_per_table_order_side_independent() {
        // Each `=` is resolved regardless of which side the left/right ref appears
        // on: `b.y = a.y` keys the same as `a.y = b.y`.
        let r = extract("a.x = b.x AND b.y = a.y", two_u64(), two_u64()).unwrap();
        assert_eq!(r, (vec![0, 1], vec![0, 1]));
    }

    #[test]
    fn extract_keys_parentheses_unwrap() {
        // Expr::Nested grouping never changes which equijoins are extracted.
        let base = extract("a.x = b.x AND a.y = b.y", two_u64(), two_u64()).unwrap();
        assert_eq!(base, (vec![0, 1], vec![0, 1]));
        assert_eq!(
            extract("(a.x = b.x) AND a.y = b.y", two_u64(), two_u64()).unwrap(),
            base
        );
        assert_eq!(
            extract("(a.x = b.x AND a.y = b.y)", two_u64(), two_u64()).unwrap(),
            base
        );
        // Parenthesized column ref on one side resolves too (single pair).
        assert_eq!(
            extract("(a.x) = b.x", two_u64(), two_u64()).unwrap(),
            (vec![0], vec![0])
        );
    }

    #[test]
    fn extract_keys_dedup_exact_duplicate() {
        // Exact duplicate and sides-swapped duplicate both collapse to one column.
        assert_eq!(
            extract("a.x = b.x AND a.x = b.x", two_u64(), two_u64()).unwrap(),
            (vec![0], vec![0])
        );
        assert_eq!(
            extract("a.x = b.x AND b.x = a.x", two_u64(), two_u64()).unwrap(),
            (vec![0], vec![0])
        );
        // Overlapping-but-distinct (shares left col, differs on right) keeps both.
        assert_eq!(
            extract("a.x = b.x AND a.x = b.y", two_u64(), two_u64()).unwrap(),
            (vec![0, 0], vec![0, 1])
        );
    }

    #[test]
    fn extract_keys_single_pair() {
        assert_eq!(extract("a.x = b.y", two_u64(), two_u64()).unwrap(), (vec![0], vec![1]));
    }

    #[test]
    fn extract_keys_rejections() {
        // Float key → Unsupported (from validate_join_key_pair).
        let fl = vec![col("x", TypeCode::F64, false), col("y", TypeCode::U64, false)];
        assert!(matches!(
            extract("a.x = b.x", fl.clone(), fl).unwrap_err(),
            GnitzSqlError::Unsupported(_)
        ));
        // Cross-sign pair U128 = I64 → Unsupported (needs a signed-256 type that
        // does not exist). (U64 = I64 now promotes to I128 — see
        // `extract_keys_cross_sign_u64_promotes`.)
        let l = vec![col("x", TypeCode::U128, false)];
        let r = vec![col("x", TypeCode::I64, false)];
        assert!(matches!(
            extract("a.x = b.x", l, r).unwrap_err(),
            GnitzSqlError::Unsupported(_)
        ));
        // One past the codec cap → Unsupported (arity cap). Built relative to
        // PK_LIST_MAX_COLS so a future bump doesn't silently slacken the test.
        let n = gnitz_core::PK_LIST_MAX_COLS + 1;
        let wide: Vec<ColumnDef> = (0..n).map(|i| col(&format!("c{i}"), TypeCode::U64, false)).collect();
        let on_wide = (0..n)
            .map(|i| format!("a.c{i}=b.c{i}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        assert!(matches!(
            extract(&on_wide, wide.clone(), wide).unwrap_err(),
            GnitzSqlError::Unsupported(_)
        ));
        // Both refs on the same table → Bind (now via the anchor guard: the
        // same-table `=` becomes a residual, leaving no equijoin/range anchor).
        assert!(matches!(
            extract("a.x = a.y", two_u64(), two_u64()).unwrap_err(),
            GnitzSqlError::Bind(_)
        ));
        // A lone `<>` is a residual-only ON (no equi/range anchor) → Bind.
        assert!(matches!(
            extract("a.x <> b.x", two_u64(), two_u64()).unwrap_err(),
            GnitzSqlError::Bind(_)
        ));
        // A lone OR group is a residual-only ON (no equi/range anchor) → Bind.
        assert!(matches!(
            extract("a.x = b.x OR a.y = b.y", two_u64(), two_u64()).unwrap_err(),
            GnitzSqlError::Bind(_)
        ));
    }

    #[test]
    fn validate_pair_accepts_compatible() {
        // Same-type pairs: T == the source reindex output type.
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U64, false), &col("b", TypeCode::U64, false)).unwrap(),
            TypeCode::U64 as u8
        );
        validate_join_key_pair(&col("a", TypeCode::String, false), &col("b", TypeCode::String, false)).unwrap();
        validate_join_key_pair(&col("a", TypeCode::String, false), &col("b", TypeCode::Blob, false)).unwrap();
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U128, false), &col("b", TypeCode::UUID, false)).unwrap(),
            TypeCode::U128 as u8
        );
        // Cross-width same-sign pairs promote to the wider type.
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::I32, false), &col("b", TypeCode::I64, false)).unwrap(),
            TypeCode::I64 as u8
        );
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U32, false), &col("b", TypeCode::U64, false)).unwrap(),
            TypeCode::U64 as u8
        );
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U8, false), &col("b", TypeCode::U64, false)).unwrap(),
            TypeCode::U64 as u8
        );
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U32, false), &col("b", TypeCode::U128, false)).unwrap(),
            TypeCode::U128 as u8
        );
        // Cross-sign pairs whose unsigned side is ≤ 8 bytes (U64) promote to the
        // narrowest signed type holding both ranges.
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U32, false), &col("b", TypeCode::I64, false)).unwrap(),
            TypeCode::I64 as u8
        );
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U8, false), &col("b", TypeCode::I16, false)).unwrap(),
            TypeCode::I16 as u8
        );
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U16, false), &col("b", TypeCode::I32, false)).unwrap(),
            TypeCode::I32 as u8
        );
        // U32 = I32 needs the wider I64 (a signed type of equal width cannot hold
        // U32's full range).
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U32, false), &col("b", TypeCode::I32, false)).unwrap(),
            TypeCode::I64 as u8
        );
        // U64 cross-sign with any signed integer ⇒ the signed-128 common type.
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U64, false), &col("b", TypeCode::I64, false)).unwrap(),
            TypeCode::I128 as u8
        );
        assert_eq!(
            validate_join_key_pair(&col("a", TypeCode::U64, false), &col("b", TypeCode::I8, false)).unwrap(),
            TypeCode::I128 as u8
        );
    }

    #[test]
    fn validate_pair_rejects_incompatible() {
        // Float column.
        assert!(validate_join_key_pair(&col("a", TypeCode::F64, false), &col("b", TypeCode::U64, false)).is_err());
        // Cross-sign pair whose unsigned side is 128-bit (would need a signed-256
        // type that does not exist). (U64 = I64 now promotes to I128.)
        assert!(validate_join_key_pair(&col("a", TypeCode::U128, false), &col("b", TypeCode::I64, false)).is_err());
        assert!(validate_join_key_pair(&col("a", TypeCode::UUID, false), &col("b", TypeCode::I64, false)).is_err());
        // Mixed string/native — STRING = U128 both reindex to U128, so this is
        // caught *only* by the german-string check, not the common-type check.
        assert!(validate_join_key_pair(&col("a", TypeCode::String, false), &col("b", TypeCode::U128, false)).is_err());
        assert!(validate_join_key_pair(&col("a", TypeCode::String, false), &col("b", TypeCode::I64, false)).is_err());
    }

    /// Cross-width promotion threads `T` into both per-side carried tcs and the
    /// `_join_pk` stamp: extract returns the promoted common type per pair.
    #[test]
    fn extract_keys_cross_width_promotes() {
        // INT (I32) = BIGINT (I64) → T = I64.
        let l = vec![col("x", TypeCode::I32, false)];
        let r = vec![col("x", TypeCode::I64, false)];
        let (lc, rc, tcs) = extract_with_tcs("a.x = b.x", l, r).unwrap();
        assert_eq!((lc, rc), (vec![0], vec![0]));
        assert_eq!(tcs, vec![TypeCode::I64 as u8]);

        // U32 = U64 → T = U64.
        let l = vec![col("x", TypeCode::U32, false)];
        let r = vec![col("x", TypeCode::U64, false)];
        let (_, _, tcs) = extract_with_tcs("a.x = b.x", l, r).unwrap();
        assert_eq!(tcs, vec![TypeCode::U64 as u8]);

        // Cross-sign: INT UNSIGNED (U32) = BIGINT (I64) → T = I64.
        let l = vec![col("x", TypeCode::U32, false)];
        let r = vec![col("x", TypeCode::I64, false)];
        let (_, _, tcs) = extract_with_tcs("a.x = b.x", l, r).unwrap();
        assert_eq!(tcs, vec![TypeCode::I64 as u8]);
    }

    /// BIGINT UNSIGNED (U64) cross-sign joined with any signed integer promotes to
    /// the signed-128 common type `I128`, the new capability of this feature.
    #[test]
    fn extract_keys_cross_sign_u64_promotes() {
        for signed in [TypeCode::I8, TypeCode::I16, TypeCode::I32, TypeCode::I64] {
            let l = vec![col("x", TypeCode::U64, false)];
            let r = vec![col("x", signed, false)];
            let (lc, rc, tcs) = extract_with_tcs("a.x = b.x", l, r).unwrap();
            assert_eq!((lc, rc), (vec![0], vec![0]));
            assert_eq!(tcs, vec![TypeCode::I128 as u8], "U64 = {signed:?} must promote to I128");
        }
    }

    #[test]
    fn multi_null_filter_builds_at_every_arity() {
        let schema = Schema {
            columns: vec![
                col("c0", TypeCode::U64, true),
                col("c1", TypeCode::U64, true),
                col("c2", TypeCode::U64, true),
            ],
            pk_cols: vec![0],
        };
        // k = 1 (byte-identical to the old single-column null_filter_prog).
        multi_null_filter_prog(&[0], &schema, false).unwrap();
        multi_null_filter_prog(&[0], &schema, true).unwrap();
        // k ≥ 2: a multi-leaf And for want_null=false, a multi-leaf Or for true.
        multi_null_filter_prog(&[0, 1, 2], &schema, false).unwrap();
        multi_null_filter_prog(&[0, 1, 2], &schema, true).unwrap();
    }

    // ── Range / band join extraction ─────────────────────────────────────────

    /// An equi-only ON yields no range conjunct (the pure superset property — the
    /// equi path is byte-identical because nothing about its inputs changed).
    #[test]
    fn extract_equi_only_has_no_range() {
        let (_, _, _, range, _) = extract_full("a.x = b.x AND a.y = b.y", two_u64(), two_u64()).unwrap();
        assert!(range.is_none());
    }

    /// The §3 rel mapping for all four operators in BOTH operand orders. The
    /// canonical form is always `left_col OP right_col`; a right-table-first
    /// conjunct flips the operator to its converse.
    #[test]
    fn range_rel_mapping_both_operand_orders() {
        // (ON clause, canonical op, left_col, right_col). two_u64 = [x=0, y=1].
        let cases: &[(&str, RangeRel)] = &[
            ("a.x < b.y", RangeRel::Lt),
            ("a.x <= b.y", RangeRel::Le),
            ("a.x > b.y", RangeRel::Gt),
            ("a.x >= b.y", RangeRel::Ge),
            // Right-table-first: `b.y > a.x` ⟺ `a.x < b.y`, etc.
            ("b.y > a.x", RangeRel::Lt),
            ("b.y >= a.x", RangeRel::Le),
            ("b.y < a.x", RangeRel::Gt),
            ("b.y <= a.x", RangeRel::Ge),
        ];
        for (on, want_op) in cases {
            let (eq_l, _, _, range, _) = extract_full(on, two_u64(), two_u64()).unwrap();
            assert!(eq_l.is_empty(), "{on}: pure range join has no eq prefix");
            let rc = range.unwrap_or_else(|| panic!("{on}: expected a range conjunct"));
            assert_eq!(rc.op, *want_op, "{on}");
            assert_eq!((rc.left_col, rc.right_col), (0, 1), "{on}: canonicalized to a.x / b.y");
        }
    }

    /// A band join (equality prefix + one range conjunct): the eq pair and the
    /// range conjunct are both extracted.
    #[test]
    fn extract_band_join_eq_prefix_plus_range() {
        // a.k = b.k AND a.lo <= b.t. cols: a=[k=0, lo=1], b=[k=0, t=1].
        let (eq_l, eq_r, eq_tcs, range, _) = extract_full("a.x = b.x AND a.y <= b.y", two_u64(), two_u64()).unwrap();
        assert_eq!((eq_l, eq_r), (vec![0], vec![0]));
        assert_eq!(eq_tcs, vec![TypeCode::U64 as u8]);
        let rc = range.expect("range conjunct");
        assert_eq!((rc.left_col, rc.right_col, rc.op), (1, 1, RangeRel::Le));
    }

    /// Rejections at extraction: a string/blob/float range pair (the content hash
    /// is not order-preserving) and a same-table range. A *second* range conjunct
    /// is no longer rejected — it lands in the residual (asserted separately).
    #[test]
    fn range_extraction_rejections() {
        // A second range conjunct is now accepted: the first is the physical range,
        // the second a residual filter.
        let (_, _, _, range, residual) = extract_full("a.x < b.x AND a.y < b.y", two_u64(), two_u64()).unwrap();
        assert!(range.is_some(), "first range conjunct is physical");
        assert_eq!(residual.len(), 1, "second range conjunct is a residual");
        // String range pair → Unsupported (not order-preserving).
        let s = vec![col("x", TypeCode::String, false), col("y", TypeCode::String, false)];
        assert!(matches!(
            extract_full("a.x < b.x", s.clone(), s).unwrap_err(),
            GnitzSqlError::Unsupported(_)
        ));
        // BLOB range pair → Unsupported.
        let bl = vec![col("x", TypeCode::Blob, false), col("y", TypeCode::Blob, false)];
        assert!(matches!(
            extract_full("a.x > b.x", bl.clone(), bl).unwrap_err(),
            GnitzSqlError::Unsupported(_)
        ));
        // Float range pair → Unsupported (via validate_join_key_pair).
        let f = vec![col("x", TypeCode::F64, false), col("y", TypeCode::F64, false)];
        assert!(matches!(
            extract_full("a.x < b.x", f.clone(), f).unwrap_err(),
            GnitzSqlError::Unsupported(_)
        ));
        // Both range operands on the same table → Bind.
        assert!(matches!(
            extract_full("a.x < a.y", two_u64(), two_u64()).unwrap_err(),
            GnitzSqlError::Bind(_)
        ));
    }

    /// Residual classification: conjuncts the physical join cannot consume are
    /// collected into the residual list alongside the equi/range anchor.
    #[test]
    fn extract_collects_residual_conjuncts() {
        // Equijoin + inequality: one equi pair, no range, the `<>` is a residual.
        let (eq_l, eq_r, _, range, residual) = extract_full("a.x = b.x AND a.y <> b.y", two_u64(), two_u64()).unwrap();
        assert_eq!((eq_l, eq_r), (vec![0], vec![0]));
        assert!(range.is_none());
        assert_eq!(residual.len(), 1);

        // Band + second range: the eq pair and first range anchor; the second range
        // conjunct falls through to the residual.
        let (eq_l, _, _, range, residual) =
            extract_full("a.x = b.x AND a.y < b.y AND a.y > b.x", two_u64(), two_u64()).unwrap();
        assert_eq!(eq_l, vec![0]);
        assert!(range.is_some());
        assert_eq!(residual.len(), 1);

        // Equijoin + column-vs-literal: the literal comparison is a residual.
        let (_, _, _, _, residual) = extract_full("a.x = b.x AND a.y = 5", two_u64(), two_u64()).unwrap();
        assert_eq!(residual.len(), 1);

        // OR-group behind an equijoin anchor: the whole OR is one residual conjunct.
        let (_, _, _, _, residual) = extract_full("a.x = b.x AND (a.y > 5 OR b.y < 3)", two_u64(), two_u64()).unwrap();
        assert_eq!(residual.len(), 1);
    }

    /// A signed range pair via cross-sign promotion resolves to the common type
    /// (U32 vs I64 → I64), the same ladder the equi path uses.
    #[test]
    fn range_pair_cross_sign_promotes() {
        let l = vec![col("id", TypeCode::U64, false), col("x", TypeCode::U32, false)];
        let r = vec![col("id", TypeCode::U64, false), col("y", TypeCode::I64, false)];
        let (_, _, _, range, _) = extract_full("a.x < b.y", l, r).unwrap();
        assert_eq!(range.unwrap().tc, TypeCode::I64 as u8);
    }
}
