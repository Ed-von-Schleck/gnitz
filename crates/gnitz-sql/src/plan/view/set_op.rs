//! Set-operation and SELECT DISTINCT view compilation: UNION (ALL), INTERSECT,
//! EXCEPT, and DISTINCT, all hashed to a synthetic content-PK and combined with
//! join-free union/negate/distinct/positive_part arithmetic per operator.

use crate::ast_util::{
    extract_table_factor_name, is_bare_wildcard_projection, wildcard_name_is_visible, WildcardRewrite,
};
use crate::bind::{bind_single_table, Binder};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::lower::compile_filter_program;
use crate::plan::validate::{
    reject_duplicate_column_names, reject_float_keys, reject_unhonored_select_clauses, HonoredClauses,
};
use crate::plan::view::EmitPieces;
use gnitz_core::{CircuitBuilder, ColumnDef, FixedInt, GnitzClient, Schema, TypeCode};
use sqlparser::ast::{SelectItem, SetExpr, SetOperator, SetQuantifier};

/// Resolve a set-op side's FROM: exactly one plain table. A derived table or a
/// JOIN input is not composed here (set-op branches are not pre-compiled by the
/// front door), so both reject via the strict extractor / the arity check.
fn resolve_side_source(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    select: &sqlparser::ast::Select,
    context: &str,
) -> Result<(u64, std::rc::Rc<Schema>), GnitzSqlError> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(format!(
            "{context}: only a single table without JOINs is supported"
        )));
    }
    let table_name = extract_table_factor_name(&select.from[0].relation, context)?;
    binder.resolve(client, &table_name)
}

/// Per-column common type for a set-op pair, or `None` to keep the exact-match
/// type-mismatch error. Same types pass through; a cross-width integer pair
/// promotes to the join-key ladder's common type (same-sign → the wider type;
/// cross-sign with the unsigned operand ≤ U32 → the narrowest strictly-wider
/// signed type, e.g. `U32` vs `I32` → `I64`) but only when the result is a
/// concrete ≤8-byte integer. `None`, the `I128` collapse (`U64` vs `I64`), and
/// every 16-byte / non-integer / string pair are rejected: the widening path
/// loads a value into one i64 register and cannot represent a 16-byte extremum.
fn set_op_common_type(l: TypeCode, r: TypeCode) -> Option<TypeCode> {
    if l == r {
        return Some(l);
    }
    let t = l.join_key_common_type(r)?;
    FixedInt::from_type_code(t).is_some().then_some(t)
}

/// First half of a set-op / DISTINCT side: clause rejection, input delta, optional
/// WHERE, and projection resolution. Returns the filtered node plus the projected
/// source column indices and their output column defs — but does NOT hash/shard,
/// because the promotion targets need both sides' output columns first.
fn resolve_set_op_side(
    select: &sqlparser::ast::Select,
    cb: &mut CircuitBuilder,
    context: &str,
    distinct_honored: bool,
    source: (u64, std::rc::Rc<Schema>),
) -> Result<(gnitz_core::NodeId, Vec<usize>, Vec<ColumnDef>, u64), GnitzSqlError> {
    // Honor FROM, WHERE, and the projection always; plain DISTINCT only when
    // `distinct_honored` — i.e. the DISTINCT-view caller, which dedups after this
    // returns. Set-op branches pass `false`: a per-branch DISTINCT is not
    // deduplicated and would leak duplicates under UNION ALL. Reject every other
    // clause so none is silently dropped (a wrong result).
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: distinct_honored,
        },
        context,
    )?;

    // `source` is pre-resolved by the caller: the side's single FROM table, or —
    // for the DISTINCT caller — a hidden view H compiled from a JOIN input, over
    // which this side's WHERE/projection resolve by column name.
    let (source_tid, source_schema) = source;

    let inp = cb.input_delta_tagged(source_tid);

    // Optional WHERE (elided when the predicate bound to a true constant).
    let filtered = if let Some(where_expr) = &select.selection {
        let bound = bind_single_table(where_expr, &source_schema)?;
        match compile_filter_program(&bound, &source_schema)? {
            Some(p) => cb.filter(inp, Some(p)),
            None => inp,
        }
    } else {
        inp
    };

    // Resolve the projection to a set of source column indices (in SELECT
    // order). Unlike `build_projection`, the source PK is NOT force-included:
    // set membership is over exactly the projected columns.
    let (proj_indices, out_cols) = resolve_set_projection(&select.projection, &source_schema, context)?;

    Ok((filtered, proj_indices, out_cols, source_tid))
}

/// Second half: hash the projected columns to a synthetic content PK — widening
/// each column whose `target_tcs` entry is non-zero into the promoted layout so
/// both set-op sides share one physical representation — then shard by that PK.
fn hash_shard_side(
    cb: &mut CircuitBuilder,
    filtered: gnitz_core::NodeId,
    proj_indices: &[usize],
    target_tcs: &[u8],
    branch_id: u8,
) -> gnitz_core::NodeId {
    // Reindex by a hash of the projected columns, so set membership
    // (EXCEPT/INTERSECT/UNION-distinct) is decided by the projected row content,
    // not by the source table's PK: two rows from different tables sharing a PK
    // but differing in payload must not match.
    let reindexed = cb.map_hash_row(filtered, proj_indices, target_tcs, branch_id);
    // Repartition by the synthetic hash PK (column 0) so that under
    // multiple workers each row lands on the worker that owns its new PK's
    // shard, co-locating matching rows for the downstream set arithmetic and
    // placing each output row on its owning worker for the sink/scan. The hash
    // is computed in-circuit, so the master cannot pre-shard the source by it;
    // this in-circuit exchange is mandatory. Single-worker mode elides the IPC.
    cb.shard(reindexed, &[0])
}

/// Resolve a set-operation side's projection to source column indices plus
/// output column definitions. Supports `SELECT *`, bare column references, and
/// aliased column references; rejects computed expressions (which have no
/// meaningful set identity here) with a clean error rather than silently
/// dropping them.
fn resolve_set_projection(
    projection: &[SelectItem],
    source_schema: &Schema,
    context: &str,
) -> Result<(Vec<usize>, Vec<ColumnDef>), GnitzSqlError> {
    // Wildcard expands to *visible* columns only: an upstream synthetic key
    // (`_join_pk`, `_set_pk`, …) must not participate in UNION/INTERSECT/
    // EXCEPT/DISTINCT row identity — hashing it into the dedup key would keep
    // otherwise-identical rows distinct. Only a *bare* `*` takes this fast path;
    // a `* EXCEPT/EXCLUDE/RENAME` (or a rejected `* REPLACE/ILIKE`) falls into
    // the single Wildcard arm below.
    if is_bare_wildcard_projection(projection) {
        let (indices, cols): (Vec<usize>, Vec<ColumnDef>) =
            source_schema.visible_columns().map(|(i, c)| (i, c.clone())).unzip();
        reject_float_keys(source_schema, &indices)?;
        return Ok((indices, cols));
    }
    let mut indices: Vec<usize> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                // Visible columns only (as the bare-`*` fast path); `EXCEPT`/
                // `EXCLUDE`/`RENAME` rewrite by name, `REPLACE`/`ILIKE` reject.
                let rw =
                    WildcardRewrite::for_item(item, |n| wildcard_name_is_visible(&source_schema.columns, n), context)?;
                for (i, col) in source_schema.visible_columns() {
                    let Some(out) = rw.rewrite_column(col) else { continue };
                    indices.push(i);
                    out_cols.push(out);
                }
            }
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                // Resolve the column once; an alias only renames the output column.
                let ci = match bind_single_table(expr, source_schema)? {
                    BoundExpr::ColRef(ci) => ci,
                    _ => {
                        return Err(GnitzSqlError::Unsupported(format!(
                            "{context}: computed expressions are not supported"
                        )))
                    }
                };
                indices.push(ci);
                let mut col = source_schema.columns[ci].clone();
                if let SelectItem::ExprWithAlias { alias, .. } = item {
                    col.name = alias.value.clone();
                }
                out_cols.push(col);
            }
            _ => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "{context}: unsupported SELECT item"
                )))
            }
        }
    }
    // Single chokepoint: every projected column lands in `indices`, so one pass
    // here rejects a float row-identity key regardless of which SELECT-item arm
    // produced it (a new arm is covered automatically).
    reject_float_keys(source_schema, &indices)?;
    Ok((indices, out_cols))
}

/// The two leaf nodes feeding an INTERSECT/EXCEPT weight-clamp arm. DISTINCT (and
/// the bare default quantifier) clamps each side to {0,1} via `distinct` so the
/// arithmetic is set-valued; ALL keeps the raw per-row bag counts. Only called
/// from the `q @ (Distinct | None | All)` arms, so `All` vs. "distinct it" is the
/// whole decision.
fn set_op_leaves(
    cb: &mut CircuitBuilder,
    quantifier: SetQuantifier,
    left: gnitz_core::NodeId,
    right: gnitz_core::NodeId,
) -> (gnitz_core::NodeId, gnitz_core::NodeId) {
    match quantifier {
        SetQuantifier::All => (left, right),
        _ => (cb.distinct(left), cb.distinct(right)),
    }
}

/// Emit a set-operation view's circuit pieces `(circuit, cols, pk)` for a
/// pre-allocated `view_id` — as a plain view or as a chain final over compiled
/// sub-plans (each side's FROM resolves to a hidden view).
pub(crate) fn emit_set_op_pieces(
    client: &mut GnitzClient,
    view_id: u64,
    op: SetOperator,
    set_quantifier: SetQuantifier,
    left: &SetExpr,
    right: &SetExpr,
    binder: &mut Binder<'_>,
) -> Result<EmitPieces, GnitzSqlError> {
    let left_select = match left {
        SetExpr::Select(s) => s,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "set operation: left side must be a SELECT".to_string(),
            ))
        }
    };
    let right_select = match right {
        SetExpr::Select(s) => s,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "set operation: right side must be a SELECT".to_string(),
            ))
        }
    };

    let mut cb = CircuitBuilder::new(view_id, 0);

    // UNION ALL keeps both copies of an identical row (weight +2), so the two
    // branches must hash to distinct PKs — give the right side branch_id = 1.
    // Deduplicating set-ops (UNION/EXCEPT/INTERSECT) intentionally coalesce
    // identical rows, so both sides use branch_id = 0.
    let right_branch_id = matches!((op, set_quantifier), (SetOperator::Union, SetQuantifier::All)) as u8;

    let left_src = resolve_side_source(client, binder, left_select, "set operation")?;
    let (left_filtered, left_proj, left_cols, left_tid) =
        resolve_set_op_side(left_select, &mut cb, "set operation", false, left_src)?;
    let right_src = resolve_side_source(client, binder, right_select, "set operation")?;
    let (right_filtered, right_proj, right_cols, right_tid) =
        resolve_set_op_side(right_select, &mut cb, "set operation", false, right_src)?;

    // INTERSECT/EXCEPT build their two distinct-ed leaves on the same content-hash
    // PK, then combine with union/negate/positive_part. When both branches resolve
    // to the same relation, one source delta would have to drive both branch
    // pipelines in a single epoch — which the single-source-per-epoch execution
    // model does not deliver — so reject it. (The join planner solves the same
    // constraint by wrapping the repeated relation in a pass-through hidden view;
    // set-op sides could adopt that wrap if this shape is ever needed.) The
    // join-free arithmetic itself has no cross-term, so this is an execution-model
    // constraint, not a correctness-of-the-math one. UNION / UNION ALL are linear
    // merges, exempt. Two *different* views over the same base table produce two
    // distinct dependency edges (two passes), so the discriminator is source-id
    // equality, not base-table overlap.
    if matches!(op, SetOperator::Intersect | SetOperator::Except) && left_tid == right_tid {
        return Err(GnitzSqlError::Unsupported(
            "INTERSECT/EXCEPT whose two inputs are the same relation is not supported".into(),
        ));
    }

    // Schema compatibility check
    if left_cols.len() != right_cols.len() {
        return Err(GnitzSqlError::Plan(format!(
            "set operation: column count mismatch ({} vs {})",
            left_cols.len(),
            right_cols.len()
        )));
    }
    // Per-column common type. The merge operators read payload bytes at one fixed
    // width per column, so both sides must share a physical layout. Same types
    // pass through; a cross-width integer pair widens to a common ≤8-byte integer
    // (value-preserving, so equal logical values co-hash). A pair with no such
    // common type — string vs int, a 16-byte pair, or the `U64` vs `I64` → I128
    // collapse the one-register widen cannot represent — keeps the exact-match
    // type-mismatch error rather than reading garbage.
    let common_tcs: Vec<TypeCode> = left_cols
        .iter()
        .zip(&right_cols)
        .enumerate()
        .map(|(i, (l, r))| {
            set_op_common_type(l.type_code, r.type_code).ok_or_else(|| {
                GnitzSqlError::Plan(format!(
                    "set operation: column {} type mismatch ({:?} vs {:?})",
                    i, l.type_code, r.type_code,
                ))
            })
        })
        .collect::<Result<_, _>>()?;

    // A side promotes column i iff its type differs from the common type (0 =
    // keep the source type). Both sides emit the promoted layout, so the content
    // hash agrees across sides and the union / positive_part merge reads one width.
    let side_targets = |cols: &[ColumnDef]| -> Vec<u8> {
        cols.iter()
            .zip(&common_tcs)
            .map(|(c, &ct)| if c.type_code == ct { 0 } else { ct as u8 })
            .collect()
    };
    let left_target_tcs = side_targets(&left_cols);
    let right_target_tcs = side_targets(&right_cols);
    let left_node = hash_shard_side(&mut cb, left_filtered, &left_proj, &left_target_tcs, 0);
    let right_node = hash_shard_side(&mut cb, right_filtered, &right_proj, &right_target_tcs, right_branch_id);

    let out_node = match (op, set_quantifier) {
        (SetOperator::Union, SetQuantifier::All) => cb.union(left_node, right_node),
        (SetOperator::Union, SetQuantifier::Distinct | SetQuantifier::None) => {
            // UNION (distinct): union → distinct
            let merged = cb.union(left_node, right_node);
            cb.distinct(merged)
        }
        // ── INTERSECT / EXCEPT: join-free weight-clamp algebra. ──
        // Both sides are content-hashed with branch_id = 0 (right_branch_id is 0 for
        // everything but UNION ALL), so L's and R's copies of a row co-hash and net
        // to per-row counts before the clamp. One algebra serves both quantifiers:
        // DISTINCT differs from ALL only by clamping each leaf to {0,1} first
        // (`set_op_leaves`). The path has no join / anti-join: `positive_part` owns
        // its own integral, so correctness is independent of which side ticks first
        // (single-source-per-epoch; no cross-delta term).
        (SetOperator::Intersect, q @ (SetQuantifier::Distinct | SetQuantifier::None | SetQuantifier::All)) => {
            // INTERSECT = min(a, b) = a − positive_part(a − b). For DISTINCT the
            // {0,1} leaves make the output {0,1}, so no outer distinct is needed.
            // `a` feeds BOTH the `positive_diff` (as its minuend) and the final union,
            // so both keep it on the non-destructive PORT_IN_B: `positive_diff` puts
            // the minuend there, and the final `union(neg_pos, a)` does too. union is
            // Z-set addition (commutative), so the reordering preserves the value.
            let (a, b) = set_op_leaves(&mut cb, q, left_node, right_node);
            let pos = cb.positive_diff(a, b); // max(0, a − b)
            let neg_pos = cb.negate(pos);
            cb.union(neg_pos, a) // a − max(0, a−b) = min(a, b)
        }
        (SetOperator::Except, q @ (SetQuantifier::Distinct | SetQuantifier::None | SetQuantifier::All)) => {
            // EXCEPT = positive_part(a − b). For DISTINCT, clamping the leaves BEFORE
            // the subtraction is load-bearing: positive_part on raw multiplicities
            // gives [cL>cR], wrong for cL=2,cR=1 (SQL needs the value absent).
            let (a, b) = set_op_leaves(&mut cb, q, left_node, right_node);
            cb.positive_diff(a, b) // max(0, a − b)
        }

        // BY NAME column alignment is a separate unimplemented feature; reject it
        // honestly rather than silently falling back to positional alignment.
        (_, SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName) => {
            return Err(GnitzSqlError::Unsupported(
                "BY NAME set operations are not supported".into(),
            ))
        }

        // SetOperator::Minus (any quantifier) and any future operator.
        _ => {
            return Err(GnitzSqlError::Unsupported(format!(
                "set operation {op:?} not supported"
            )))
        }
    };

    cb.sink(out_node);
    let circuit = cb.build();

    // Output schema: U128 PK + all payload columns at the promoted common type.
    // The column name comes from the left side (SQL takes output names from the
    // first query); the type is the shared physical layout both sides emit.
    let mut out_cols_final: Vec<ColumnDef> = Vec::new();
    // Hidden: the salted set-operation key is a physical PK column, not a
    // presentation column — `SELECT *` shows only the projected content.
    out_cols_final.push(ColumnDef::new("_set_pk", TypeCode::U128, false).hidden());
    for ((l, r), &ct) in left_cols.iter().zip(&right_cols).zip(&common_tcs) {
        let mut col = l.clone();
        col.type_code = ct;
        // Output nullability is operator-specific. EXCEPT emits only left-side
        // rows — a right-only value hits positive_part(0−1)=0 and a both-sides
        // value cancels +1/−1 — so it is nullable iff the left input is. INTERSECT emits a
        // tuple only when it is in both sides (set membership matches NULLs as
        // equal), so a column is nullable iff BOTH inputs admit NULL there.
        // UNION{,ALL} may emit from either side. Tightening only ever removes the
        // flag, and only when the operator's tuple algebra forbids NULL, so it
        // cannot mislabel a nullable column. Type equality is checked above.
        col.is_nullable = match op {
            SetOperator::Intersect => l.is_nullable && r.is_nullable,
            SetOperator::Except => l.is_nullable,
            _ => l.is_nullable || r.is_nullable,
        };
        out_cols_final.push(col);
    }
    reject_duplicate_column_names(&out_cols_final, "set operation view")?;

    // Set-op views emit a synthetic single-column content-hash PK at slot 0.
    Ok((circuit, out_cols_final, vec![0]))
}

/// Emit a `SELECT DISTINCT` view's circuit pieces `(circuit, cols, pk)` for a
/// pre-allocated `view_id` over the pre-resolved `source` — a plain table, or a
/// hidden view H compiled from a JOIN input.
pub(crate) fn emit_distinct_pieces(
    view_id: u64,
    select: &sqlparser::ast::Select,
    source: (u64, std::rc::Rc<Schema>),
) -> Result<EmitPieces, GnitzSqlError> {
    let mut cb = CircuitBuilder::new(view_id, 0);
    // Reuse the set-op side pipeline (clause rejection, input delta, optional
    // WHERE, project, hash-reindex by the projected content, and shard) under the
    // "SELECT DISTINCT" error context. `distinct` then dedups on that
    // hash-of-projection PK; keying by the source PK (unique) would make it a
    // no-op and leak the unprojected columns.
    let (filtered, proj_indices, proj_cols, _) = resolve_set_op_side(select, &mut cb, "SELECT DISTINCT", true, source)?;
    // A single source has nothing to promote against — empty `target_tcs`.
    let sharded = hash_shard_side(&mut cb, filtered, &proj_indices, &[], 0);
    let distinct_node = cb.distinct(sharded);

    cb.sink(distinct_node);
    let circuit = cb.build();

    // Output schema: synthetic U128 PK + the projected columns.
    let mut out_cols: Vec<ColumnDef> = Vec::with_capacity(proj_cols.len() + 1);
    // Hidden: the synthetic DISTINCT key is a physical PK column, not a
    // presentation column.
    out_cols.push(ColumnDef::new("_distinct_pk", TypeCode::U128, false).hidden());
    out_cols.extend(proj_cols);
    reject_duplicate_column_names(&out_cols, "SELECT DISTINCT view")?;

    // DISTINCT views emit a synthetic single-column content-hash PK at slot 0.
    Ok((circuit, out_cols, vec![0]))
}
