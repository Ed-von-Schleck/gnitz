//! Set-operation and SELECT DISTINCT view compilation: UNION (ALL), INTERSECT,
//! EXCEPT, and DISTINCT, all hashed to a synthetic content-PK and deduplicated /
//! semi-/anti-joined per operator.

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
    // shard, co-locating matching rows for the downstream semi/anti-join and
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

    // INTERSECT/EXCEPT inline both branches and semi/anti-join each against the
    // other's delayed trace with no same-epoch cross-correction. When both
    // branches are the same relation, one delta drives both sides in a single
    // pass and the correction term is dropped — silently wrong results. Mirror
    // of the self-join guard. UNION / UNION ALL are linear merges with no
    // correction term, exempt. Two *different* views over the same base table
    // produce two distinct dependency edges (two passes), so the discriminator
    // is source-id equality, not base-table overlap.
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
        (SetOperator::Intersect, SetQuantifier::Distinct | SetQuantifier::None) => {
            // Lift both sides through `distinct`: set membership flips once per
            // projected row regardless of how many source rows carry it, and the
            // integrated traces then hold only weight-1 entries (semi-join tests
            // existence, not weight, so storing raw multiplicities only bloats
            // the trace tables for no effect). Build each `integrate_trace`
            // before its `distinct`'s other consumer so the Kahn schedule
            // (ascending node-id tie-break) keeps the non-destructive reader
            // ahead of any destructive co-consumer; here both readers of each
            // `distinct` are semi-joins (no trace-absent destructive branch).
            let distinct_l = cb.distinct(left_node);
            let distinct_r = cb.distinct(right_node);
            let trace_l = cb.integrate_trace(distinct_l);
            let trace_r = cb.integrate_trace(distinct_r);
            let semi_lr = cb.semi_join_with_trace_node(distinct_l, trace_r);
            let semi_rl = cb.semi_join_with_trace_node(distinct_r, trace_l);
            cb.union(semi_lr, semi_rl)
        }
        (SetOperator::Except, SetQuantifier::Distinct | SetQuantifier::None) => {
            // EXCEPT DISTINCT: difference of the two projected sets. Lifting the
            // left through `distinct` before the anti-join caps a value carried
            // by multiple source rows at weight 1 (the raw `left_node` would emit
            // weight n, surviving the difference once the right side covers it);
            // the integrated traces then hold only weight-1 entries.
            let distinct_l = cb.distinct(left_node);
            let distinct_r = cb.distinct(right_node);
            // `trace_l` is created before `except_lr`: whenever `trace_r` is still
            // empty the anti-join takes its trace-absent branch and drains
            // `distinct_l`'s register, so the non-destructive `integrate_trace`
            // (lower node id) must — and does — schedule first.
            let trace_l = cb.integrate_trace(distinct_l);
            let trace_r = cb.integrate_trace(distinct_r);
            // ΔA path: left set-members not in I(B).
            let except_lr = cb.anti_join_with_trace_node(distinct_l, trace_r);
            // ΔB path: when B's set membership flips, retract/emit the matching
            // left row. Reading `distinct_r` makes a second insert of an
            // already-present projected B row a no-op.
            let semi_rl = cb.semi_join_with_trace_node(distinct_r, trace_l);
            let correction = cb.negate(semi_rl);
            cb.union(except_lr, correction)
        }

        // ── Bag semantics: clamp the net per-row count, don't deduplicate. ──
        // Both sides are content-hashed with branch_id = 0 (right_branch_id is 0 for
        // everything but UNION ALL), so L's and R's copies of a row co-hash and net to
        // the per-row counts cL, cR before the clamp. The path has no join / semi-join /
        // anti-join: `positive_part` owns its own integral, so correctness is independent
        // of which side ticks first (single-source-per-epoch; no cross-delta term).
        (SetOperator::Except, SetQuantifier::All) => {
            // EXCEPT ALL = positive_part(A − B) ⇒ max(0, cL − cR).
            let neg_r = cb.negate(right_node);
            let diff = cb.union(left_node, neg_r); // cL − cR
            cb.positive_part(diff) // max(0, cL − cR)
        }
        (SetOperator::Intersect, SetQuantifier::All) => {
            // INTERSECT ALL = A − positive_part(A − B) ⇒ min(cL, cR). `left_node` feeds
            // BOTH unions, so it must sit on the non-destructive PORT_IN_B (second
            // operand) of each: the destructive-register ordering invariant rejects a
            // register with two destructive (PORT_IN_A) consumers. union is Z-set
            // addition (commutative), so this reordering preserves the value.
            let neg_r = cb.negate(right_node);
            let diff = cb.union(neg_r, left_node); // −cR + cL = cL − cR
            let pos = cb.positive_part(diff); // max(0, cL − cR)
            let neg_pos = cb.negate(pos);
            cb.union(neg_pos, left_node) // −max(0, cL−cR) + cL = min(cL, cR)
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
        // values (right-side corrections must match a left set-pk to fire, and a
        // NULL-in-c right tuple has a set-pk a NOT NULL left column never
        // produces), so it is nullable iff the left input is. INTERSECT emits a
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
