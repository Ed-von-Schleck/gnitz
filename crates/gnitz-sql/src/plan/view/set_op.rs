//! Set-operation and SELECT DISTINCT view compilation: UNION (ALL), INTERSECT,
//! EXCEPT, and DISTINCT, all hashed to a synthetic content-PK and combined with
//! join-free union/negate/distinct/positive_part arithmetic per operator.

use crate::ast_util::{extract_table_factor_name, is_wildcard_projection};
use crate::bind::{bind_single_table, Binder};
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use crate::lower::compile_bound_expr_to_program;
use crate::plan::validate::{
    reject_duplicate_column_names, reject_float_keys, reject_unhonored_select_clauses, HonoredClauses,
};
use crate::SqlResult;
use gnitz_core::{CircuitBuilder, ColumnDef, GnitzClient, Schema, TypeCode};
use sqlparser::ast::{Distinct, SelectItem, SetExpr, SetOperator, SetQuantifier};

fn compile_set_op_side(
    client: &mut GnitzClient,
    select: &sqlparser::ast::Select,
    binder: &mut Binder<'_>,
    cb: &mut CircuitBuilder,
    branch_id: u8,
    context: &str,
) -> Result<(gnitz_core::NodeId, Vec<ColumnDef>, u64), GnitzSqlError> {
    // Honor only FROM, WHERE, and the projection here. The DISTINCT-view caller dedups
    // plain DISTINCT after this returns; the set-op caller rejects a plain branch DISTINCT
    // before calling in. Reject every other clause so none is silently dropped (a wrong result).
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
        },
        context,
    )?;

    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(format!(
            "{context}: only a single table without JOINs is supported"
        )));
    }
    let table_name = extract_table_factor_name(&select.from[0].relation, context)?;
    let (source_tid, source_schema) = binder.resolve(client, &table_name)?;

    let inp = cb.input_delta_tagged(source_tid);

    // Optional WHERE
    let filtered = if let Some(where_expr) = &select.selection {
        let bound = bind_single_table(where_expr, &source_schema)?;
        cb.filter(inp, Some(compile_bound_expr_to_program(&bound, &source_schema)?))
    } else {
        inp
    };

    // Resolve the projection to a set of source column indices (in SELECT
    // order). Unlike `build_projection`, the source PK is NOT force-included:
    // set membership is over exactly the projected columns.
    let (proj_indices, out_cols) = resolve_set_projection(&select.projection, &source_schema, context)?;

    // Reindex by a hash of the projected columns, so set membership
    // (EXCEPT/INTERSECT/UNION-distinct) is decided by the projected row content,
    // not by the source table's PK: two rows from different tables sharing a PK
    // but differing in payload must not match.
    let reindexed = cb.map_hash_row(filtered, &proj_indices, branch_id);
    // Repartition by the synthetic hash PK (column 0) so that under
    // multiple workers each row lands on the worker that owns its new PK's
    // shard, co-locating matching rows for the downstream set arithmetic and
    // placing each output row on its owning worker for the sink/scan. The hash
    // is computed in-circuit, so the master cannot pre-shard the source by it;
    // this in-circuit exchange is mandatory. Single-worker mode elides the IPC.
    let sharded = cb.shard(reindexed, &[0]);
    Ok((sharded, out_cols, source_tid))
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
    if is_wildcard_projection(projection) {
        let indices: Vec<usize> = (0..source_schema.columns.len()).collect();
        reject_float_keys(source_schema, &indices)?;
        return Ok((indices, source_schema.columns.clone()));
    }
    let mut indices: Vec<usize> = Vec::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                for i in 0..source_schema.columns.len() {
                    indices.push(i);
                    out_cols.push(source_schema.columns[i].clone());
                }
            }
            SelectItem::UnnamedExpr(expr) => match bind_single_table(expr, source_schema)? {
                BoundExpr::ColRef(ci) => {
                    indices.push(ci);
                    out_cols.push(source_schema.columns[ci].clone());
                }
                _ => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "{context}: computed expressions are not supported"
                    )))
                }
            },
            SelectItem::ExprWithAlias { expr, alias } => match bind_single_table(expr, source_schema)? {
                BoundExpr::ColRef(ci) => {
                    indices.push(ci);
                    let mut col = source_schema.columns[ci].clone();
                    col.name = alias.value.clone();
                    out_cols.push(col);
                }
                _ => {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "{context}: computed expressions are not supported"
                    )))
                }
            },
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_create_set_op_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    op: SetOperator,
    set_quantifier: SetQuantifier,
    left: &SetExpr,
    right: &SetExpr,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
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

    // The funnel (compile_set_op_side) honors plain DISTINCT only for the DISTINCT-view caller,
    // which dedups after it. A set-op branch is not deduplicated per branch, so a branch DISTINCT
    // would be silently dropped — under UNION ALL that leaks duplicates. The funnel rejects
    // DISTINCT ON for both callers; this covers plain branch DISTINCT, the one clause whose
    // support differs by caller.
    for (branch, side) in [(left_select, "left"), (right_select, "right")] {
        if matches!(&branch.distinct, Some(Distinct::Distinct)) {
            return Err(GnitzSqlError::Unsupported(format!(
                "set operation: DISTINCT on the {side} branch is not supported"
            )));
        }
    }

    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let mut cb = CircuitBuilder::new(view_id, 0);

    // UNION ALL keeps both copies of an identical row (weight +2), so the two
    // branches must hash to distinct PKs — give the right side branch_id = 1.
    // Deduplicating set-ops (UNION/EXCEPT/INTERSECT) intentionally coalesce
    // identical rows, so both sides use branch_id = 0.
    let right_branch_id = matches!((op, set_quantifier), (SetOperator::Union, SetQuantifier::All)) as u8;

    let (left_node, left_cols, left_tid) =
        compile_set_op_side(client, left_select, binder, &mut cb, 0, "set operation")?;
    let (right_node, right_cols, right_tid) =
        compile_set_op_side(client, right_select, binder, &mut cb, right_branch_id, "set operation")?;

    // INTERSECT/EXCEPT build their two distinct-ed leaves on the same content-hash
    // PK, then combine with union/negate/positive_part. When both branches resolve
    // to the same relation, one source delta would have to drive both branch
    // pipelines in a single epoch — which the single-source-per-epoch execution
    // model does not deliver — so reject it. Mirror of the self-join guard. The
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
    // The merge operators read payload bytes using the left schema's column
    // sizes; a STRING column read as an 8-byte integer (or vice versa) produces
    // garbage, so both sides must agree on column types.
    for (i, (l, r)) in left_cols.iter().zip(&right_cols).enumerate() {
        if l.type_code != r.type_code {
            return Err(GnitzSqlError::Plan(format!(
                "set operation: column {} type mismatch ({:?} vs {:?})",
                i, l.type_code, r.type_code,
            )));
        }
    }

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

    // Output schema: U128 PK + all payload columns
    let mut out_cols_final: Vec<ColumnDef> = Vec::new();
    out_cols_final.push(ColumnDef::new("_set_pk", TypeCode::U128, false));
    for (l, r) in left_cols.iter().zip(&right_cols) {
        let mut col = l.clone();
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
    reject_duplicate_column_names(out_cols_final.iter().map(|c| c.name.as_str()), "set operation view")?;

    // Set-op views emit a synthetic single-column content-hash PK at slot 0.
    client
        .create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols_final, &[0])
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

pub(crate) fn execute_create_distinct_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    select: &sqlparser::ast::Select,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let mut cb = CircuitBuilder::new(view_id, 0);
    // Reuse the set-op side pipeline (validate FROM, resolve, input delta,
    // optional WHERE, project, hash-reindex by the projected content, and shard)
    // under the "SELECT DISTINCT" error context. `distinct` then dedups on that
    // hash-of-projection PK; keying by the source PK (unique) would make it a
    // no-op and leak the unprojected columns.
    let (sharded, proj_cols, _) = compile_set_op_side(client, select, binder, &mut cb, 0, "SELECT DISTINCT")?;
    let distinct_node = cb.distinct(sharded);

    cb.sink(distinct_node);
    let circuit = cb.build();

    // Output schema: synthetic U128 PK + the projected columns.
    let mut out_cols: Vec<ColumnDef> = Vec::with_capacity(proj_cols.len() + 1);
    out_cols.push(ColumnDef::new("_distinct_pk", TypeCode::U128, false));
    out_cols.extend(proj_cols);
    reject_duplicate_column_names(out_cols.iter().map(|c| c.name.as_str()), "SELECT DISTINCT view")?;

    // DISTINCT views emit a synthetic single-column content-hash PK at slot 0.
    client
        .create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols, &[0])
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}
