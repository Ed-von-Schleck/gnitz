//! GROUP BY / HAVING / aggregation view compilation: the agg-spec model
//! (`AggMapping`/`AggShape`/`AggSpec`), the reduce-output schema layout, the
//! post-reduce projection, and the HAVING cluster (collection, binding, and the
//! `Having` leaf binder over the grouped relation).

use crate::ast_util::{expr_operands, extract_table_factor_name, single_relation_col_name};
use crate::bind::{
    bind_single_table, bind_structural, find_unique_column, reject_unsupported_agg_qualifiers, Binder, LeafBinder,
};
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BinOp, BoundExpr};
use crate::lower::compile_bound_expr_to_program;
use crate::plan::validate::{
    reject_duplicate_column_names, reject_float_key, reject_unhonored_select_clauses, HonoredClauses,
};
use crate::types::{is_integer_type, is_min_max_orderable, is_wide_int};
use crate::SqlResult;
use gnitz_core::{
    CircuitBuilder, ColumnDef, ExprBuilder, GnitzClient, Schema, TypeCode, AGG_COUNT, AGG_COUNT_NON_NULL, AGG_MAX,
    AGG_MIN, AGG_SUM, AGG_SUM_ZERO,
};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, SelectItem};

/// Tracks how a user-level aggregate maps to reduce agg_specs.
struct AggMapping {
    specs_start: usize, // index into agg_specs
    shape: AggShape,
    output_name: String,
    output_type: TypeCode,
    agg_func: AggFunc,
    arg_col: Option<usize>,
}

impl AggMapping {
    /// Whether the aggregate's argument column holds floats — selects the float
    /// vs integer load/divide path when finalizing AVG / nullable SUM.
    fn arg_is_float(&self, schema: &Schema) -> bool {
        self.arg_col
            .map(|c| schema.columns[c].type_code.is_float())
            .unwrap_or(false)
    }
}

/// How an aggregate's value column is finalized — which also fixes how many
/// physical specs `push_agg_specs` emits and how the SELECT projection / HAVING
/// binding read the result. `Avg` and `NullfillSum` each carry a hidden
/// COUNT_NON_NULL companion at `specs_start + 1`: their null-ness derives from
/// `companion == 0`, never the value column's saturating `has_value` bit.
/// `Direct` is a single spec copied straight through.
#[derive(Clone, Copy)]
enum AggShape {
    Direct,
    Avg,
    NullfillSum,
}

impl AggShape {
    /// True iff a hidden COUNT_NON_NULL companion sits at `specs_start + 1` and
    /// carries this aggregate's null-ness (AVG and nullable-source SUM).
    fn has_count_companion(self) -> bool {
        matches!(self, AggShape::Avg | AggShape::NullfillSum)
    }
}

/// One physical reduce spec: the wire agg op code (`AGG_*`), its source column,
/// and the output column type it produces. `out_type` is computed once at spec
/// creation by `push_agg_specs` (the spec-layout authority), so the reduce
/// schema builder reads it directly instead of reconstructing it from `op`. The
/// circuit builder consumes only `(op, col)`.
struct AggSpec {
    op: u64,
    col: usize,
    out_type: TypeCode,
}

/// Mirror the engine's `agg_output_type`: COUNT/COUNT_NON_NULL → I64, AVG → F64,
/// float SUM/MIN/MAX → F64. SUM over a non-float column widens to I64 (it can
/// overflow the source width); MIN/MAX over a non-float column *preserve the
/// source type* (the extremum is one of the input rows, so it is representable
/// in that type — incl. U64). The binder restricts MIN/MAX inputs to ≤8-byte
/// integers and floats, so the source type returned here is always ≤8 bytes —
/// the engine's I64 fallback for STRING / 16-byte MIN/MAX sources is reachable
/// only via the low-level circuit API, never from SQL. A planner/compiler
/// mismatch silently scrambles the view's output column positions, widths, and
/// types.
fn agg_result_type(func: AggFunc, src_col: Option<usize>, schema: &Schema) -> TypeCode {
    match func {
        AggFunc::Count | AggFunc::CountNonNull => TypeCode::I64,
        AggFunc::Avg => TypeCode::F64,
        AggFunc::Sum => match src_col {
            Some(c) if schema.columns[c].type_code.is_float() => TypeCode::F64,
            _ => TypeCode::I64,
        },
        AggFunc::Min | AggFunc::Max => match src_col {
            Some(c) if schema.columns[c].type_code.is_float() => TypeCode::F64,
            Some(c) => schema.columns[c].type_code, // preserve the source type (incl. U64)
            None => TypeCode::I64,
        },
    }
}

/// What each SELECT item represents in a GROUP BY query.
enum GroupBySelectItem {
    GroupCol { src_col: usize, name: String },
    Aggregate { agg_idx: usize },
}

/// Reduce-output column index for a group column `src_col`, mirroring the
/// reduce output schema and the engine's `build_reduce_output_schema`:
///
/// * `group_set_eq_pk` — the PK region holds the source PK columns in source-PK
///   order; locate `src_col` there.
/// * `single_col_natural_pk` — the lone group col is the PK at index 0.
/// * synthetic `_group_pk` — group cols follow the U128 PK, in GROUP BY order.
///
/// The branch order mirrors `build_reduce_output_schema`'s decision tree
/// (`group_set_eq_pk` → `single_col_natural_pk` → synthetic) so the helper and
/// the reduce-schema build cannot drift. When both natural-PK flags hold
/// (grouping by a single-column PK) both branches return 0, so correctness does
/// not depend on the order.
fn group_col_reduce_pos(
    src_col: usize,
    group_set_eq_pk: bool,
    single_col_natural_pk: bool,
    source_schema: &Schema,
    group_col_indices: &[usize],
) -> usize {
    if group_set_eq_pk {
        source_schema
            .pk_cols
            .iter()
            .position(|&pi| pi == src_col)
            .expect("group_set_eq_pk: every group col is a source PK col")
    } else if single_col_natural_pk {
        0
    } else {
        1 + group_col_indices.iter().position(|&gi| gi == src_col).unwrap()
    }
}

pub(crate) fn execute_create_group_by_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name: &str,
    sql_text: &str,
    select: &sqlparser::ast::Select,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    // Grouped views consume FROM, WHERE, GROUP BY, HAVING, and the projection; reject every
    // other clause (PREWHERE, TOP, QUALIFY, …) so a dropped clause is a clean error.
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: true,
            distinct: false,
        },
        "CREATE VIEW",
    )?;
    // 1. Resolve source table
    let table_name = extract_table_factor_name(&select.from[0].relation, "GROUP BY")?;
    let (source_tid, source_schema) = binder.resolve(client, &table_name)?;
    // A reduce directly over a REPLICATED source must run shard-free on every
    // worker (`reduce_multi_local`): the full copy is already on every worker, so
    // a sharded reduce would scatter W identical copies into each group owner and
    // N-fold-multiply the aggregate. The result is itself replicated (identical on
    // every worker), so the engine single-sources its read. Views resolve to
    // non-replicated (the MVP user surface is base-table dimensions; circuit-level
    // distribution propagation through nested views is a later superset).
    let source_replicated = client.table_replicated(source_tid).map_err(GnitzSqlError::Exec)?;
    // The compound-PK source guard is intentionally NOT applied here: the engine
    // reduce output already emits a full compound natural-PK region
    // (`build_reduce_output_schema` walks `pk_columns()` for `group_set_eq_pk`),
    // the helpers below map group columns through `group_col_reduce_pos`, and the
    // co-partition analyzers now compare the full PK sequence — so a reduce that
    // shards by one component of a compound PK gets the exchange it needs.

    // 2. Parse GROUP BY → group column indices
    let group_exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "GROUP BY: only expression list supported".to_string(),
            ))
        }
    };
    let mut group_col_indices: Vec<usize> = Vec::new();
    for ge in group_exprs {
        match ge {
            Expr::Identifier(id) => {
                let idx = find_unique_column(&source_schema.columns, &id.value)?
                    .ok_or_else(|| GnitzSqlError::Bind(format!("GROUP BY column '{}' not found", id.value)))?;
                reject_float_key(&source_schema.columns[idx], "GROUP BY")?;
                group_col_indices.push(idx);
            }
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "GROUP BY: only simple column references supported".to_string(),
                ))
            }
        }
    }

    // 3. Analyze SELECT items → group cols + aggregates
    let mut agg_mappings: Vec<AggMapping> = Vec::new();
    let mut select_items: Vec<GroupBySelectItem> = Vec::new();
    let mut agg_specs: Vec<AggSpec> = Vec::new();

    for (idx, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::UnnamedExpr(expr) => (expr, None),
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "GROUP BY: unsupported SELECT item".to_string(),
                ))
            }
        };

        let bound = bind_single_table(expr, &source_schema)?;
        match &bound {
            BoundExpr::ColRef(col_idx) => {
                if !group_col_indices.contains(col_idx) {
                    return Err(GnitzSqlError::Plan(format!(
                        "column '{}' must appear in GROUP BY or an aggregate function",
                        source_schema.columns[*col_idx].name
                    )));
                }
                let name = alias.unwrap_or_else(|| source_schema.columns[*col_idx].name.clone());
                select_items.push(GroupBySelectItem::GroupCol {
                    src_col: *col_idx,
                    name,
                });
            }
            BoundExpr::AggCall { func, arg } => {
                let src_col = match arg {
                    Some(a) => match a.as_ref() {
                        BoundExpr::ColRef(c) => Some(*c),
                        _ => {
                            return Err(GnitzSqlError::Unsupported(
                                "aggregate on computed expression not supported".to_string(),
                            ))
                        }
                    },
                    None => None,
                };
                let out_type = agg_result_type(*func, src_col, &source_schema);
                let agg_idx = agg_mappings.len();
                let start = agg_specs.len();
                let shape = push_agg_specs(*func, src_col, &source_schema, &mut agg_specs)?;
                let out_name = alias.unwrap_or_else(|| {
                    let prefix = match func {
                        AggFunc::Count | AggFunc::CountNonNull => "_count",
                        AggFunc::Sum => "_sum",
                        AggFunc::Min => "_min",
                        AggFunc::Max => "_max",
                        AggFunc::Avg => "_avg",
                    };
                    format!("{prefix}{idx}")
                });
                agg_mappings.push(AggMapping {
                    specs_start: start,
                    shape,
                    output_name: out_name,
                    output_type: out_type,
                    agg_func: *func,
                    arg_col: src_col,
                });
                select_items.push(GroupBySelectItem::Aggregate { agg_idx });
            }
            _ => {
                return Err(GnitzSqlError::Plan(
                    "GROUP BY SELECT: only column refs and aggregates supported".to_string(),
                ))
            }
        }
    }

    // 3b. Materialise aggregates referenced only by HAVING. HAVING is evaluated
    //     over the grouped relation, so every aggregate it references needs an
    //     agg_spec and a reduce-output column even when the SELECT list omits
    //     it. Done before reduce_schema is built so the new columns are included.
    if let Some(having_expr) = &select.having {
        collect_having_aggs(having_expr, &source_schema, &mut agg_specs, &mut agg_mappings)?;
    }

    // 3c. Every grouped or global scalar reduce gates group existence on a
    //     NULL-blind COUNT(*) cardinality (a group exists iff its net row weight
    //     > 0). Both the combined value-index path and the single-scan fallback
    //     read it — a mixed reduce folds its linear companions to a numeric value
    //     whose saturating `has_value` cannot signal an emptied group, so without
    //     this companion an emptied group would emit a phantom `(g, NULL, 0)` row.
    //     Reuse a user COUNT(*) when present; else append exactly one hidden
    //     trailing companion. Appended last — after every SELECT and HAVING-only
    //     aggregate — so it shifts no existing aggregate's specs_start; it is added
    //     to agg_specs only (never select_items / agg_mappings), so the post-reduce
    //     MAP strips it (a raw reduce column with no output column, like a
    //     HAVING-only agg). Every planner-built reduce is grouped or a global
    //     scalar aggregate, so the guard is simply "no COUNT(*) present yet" —
    //     linear or not. (`all_linear` is computed here pre-append for the
    //     two-phase decision below; the companion never changes linearity.)
    let all_linear = agg_specs
        .iter()
        .all(|s| matches!(s.op, AGG_COUNT | AGG_SUM | AGG_COUNT_NON_NULL));
    if !agg_specs.iter().any(|s| s.op == AGG_COUNT) {
        // Route through push_agg_specs — the single source of truth for spec
        // layout and out_type — rather than hand-rolling the COUNT spec. The
        // companion has no select_item / agg_mapping, so its AggShape is discarded.
        push_agg_specs(AggFunc::Count, None, &source_schema, &mut agg_specs)?;
    }

    // 4. Determine reduce output schema layout.
    //    A nullable group column cannot become the natural PK — the PK region
    //    has no null bitmap, so we fall through to the synthetic _group_pk
    //    (U128, non-nullable) path which is always safe.
    //
    //    The two natural-PK cases must mirror the compiler's
    //    `build_reduce_output_schema` decision (compiler.rs:695-697):
    //      (a) `group_set_eq_pk`: group cols are a permutation of the source
    //          PK cols, regardless of PK type — even native I64.
    //      (b) `is_single_col_natural_pk`: a single non-nullable U64/U128/UUID
    //          group col. Narrow unsigned/signed group cols still take the
    //          synthetic path because the engine helper rejects them.
    //    A divergence between planner and compiler here scrambles the view's
    //    output column positions silently.
    let group_set_eq_pk = source_schema.group_cols_eq_pk(&group_col_indices);
    let single_col_natural_pk = source_schema.is_single_col_natural_pk(&group_col_indices);

    // Build the reduce output schema (mirrors server's _build_reduce_output_schema).
    let mut reduce_schema_cols: Vec<ColumnDef> = Vec::new();
    let mut reduce_pk_cols: Vec<usize> = Vec::new();
    if group_set_eq_pk {
        for &pi in &source_schema.pk_cols {
            reduce_pk_cols.push(reduce_schema_cols.len());
            reduce_schema_cols.push(source_schema.columns[pi].clone());
        }
    } else if single_col_natural_pk {
        reduce_pk_cols.push(0);
        reduce_schema_cols.push(source_schema.columns[group_col_indices[0]].clone());
    } else {
        reduce_pk_cols.push(0);
        reduce_schema_cols.push(ColumnDef::new("_group_pk", TypeCode::U128, false));
        for &gi in &group_col_indices {
            reduce_schema_cols.push(source_schema.columns[gi].clone());
        }
    }
    // The aggregate columns are pushed next, so they start at the current width
    // of the reduce schema: the PK region plus, on the synthetic path only, the
    // group cols carried as payload.
    let agg_col_offset = reduce_schema_cols.len();
    for spec in &agg_specs {
        // Each spec carries the output column type push_agg_specs computed for it
        // (float SUM/MIN/MAX → F64, MIN/MAX preserve the source type, SUM/COUNT*
        // → I64), so the planner's virtual reduce schema matches the compiler's
        // physical reduce output (§3a) with no per-op reconstruction.
        reduce_schema_cols.push(ColumnDef::new("_agg", spec.out_type, false));
    }
    let reduce_schema = Schema {
        columns: reduce_schema_cols,
        pk_cols: reduce_pk_cols,
    };

    // 5. Build circuit
    let view_id = client.alloc_table_id().map_err(GnitzSqlError::Exec)?;
    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta();

    // Optional WHERE filter
    let filtered = if let Some(where_expr) = &select.selection {
        let bound = bind_single_table(where_expr, &source_schema)?;
        cb.filter(inp, Some(compile_bound_expr_to_program(&bound, &source_schema)?))
    } else {
        inp
    };

    // REDUCE — always use multi-agg path.
    //
    // For `group_set_eq_pk`, shard/reindex the reduce by the group columns in
    // source-PK (schema) order, not the user's GROUP BY order. The groups are
    // identical under any permutation of the PK (each group is a PK singleton),
    // and `build_reduce_output_schema` emits the output PK in source-PK order
    // regardless — so this only normalizes the shard key. Without it a permuted
    // grouping (e.g. `GROUP BY pk1, pk0`) shards by a non-PK-order key: the
    // co-partition analyzer (correctly) declines to skip the exchange, the
    // shuffle hash-routes by `[pk1, pk0]`, and the reduce output lands
    // partitioned by `hash(pk1, pk0)` rather than by the view's declared PK
    // `(pk0, pk1)` — so the multi-worker gather drops the rows that hashed to a
    // different worker. Sharding in PK order keeps the reduce co-partitioned with
    // the source (the exchange is skipped, or routes by `partition_for_pk_bytes`),
    // so the view stays partitioned by its real PK. The non-eq-pk paths keep the
    // user order: their synthetic/single-natural PK and reduce layout depend on
    // it (`group_col_reduce_pos`'s synthetic branch indexes by GROUP BY order).
    let reduce_group_cols: Vec<usize> = if group_set_eq_pk {
        source_schema.pk_cols.clone()
    } else {
        group_col_indices.clone()
    };
    // The circuit builder needs only (op, col) per spec; out_type is the
    // planner's concern and already shaped reduce_schema above.
    let circuit_specs: Vec<(u64, usize)> = agg_specs.iter().map(|s| (s.op, s.col)).collect();
    // An empty group set is the user's ungrouped (global) scalar aggregate —
    // `SELECT MIN(x) FROM t` with no GROUP BY. It compiles as a one-row reduce
    // (synthetic `_group_pk` at the constant V₀) that must emit exactly one row
    // even over an empty/fully-retracted source, so the engine seeds a ground row
    // (COUNT=0, SUM/MIN/MAX/AVG=NULL). Grouped reduces (`group_col_indices`
    // non-empty) pass `false` and are byte-for-byte unchanged.
    let global_ground = group_col_indices.is_empty();
    // Two-phase (distributable) path for an all-linear, integer, partitioned GLOBAL
    // aggregate: fold a per-worker partial locally (no exchange), then exchange only
    // the ≤ N partials to V₀'s owner and combine them. A linear aggregate satisfies
    // Agg(A+B)=Agg(A)+Agg(B), so this replaces the single-worker full-delta funnel.
    // Float SUM (and AVG over a float, whose SUM component is float) is excluded:
    // IEEE-754 addition is non-associative, so summing per-worker partials would make
    // the result depend on the worker count — those keep the deterministic funnel.
    // `two_phase ⊆ global_ground`: it is the distributable refinement of the
    // ungrouped (empty group set) case, so it reuses that predicate.
    let two_phase = global_ground
        && !source_replicated
        && all_linear
        && !agg_specs.iter().any(|s| s.op == AGG_SUM && s.out_type.is_float());
    let reduced = if two_phase {
        // Phase 1 — per-worker local partial. No ExchangeShard, global_ground = false
        // (a worker with no local rows contributes no partial, never a ground row).
        // Output: [_group_pk:U128 (col 0, PK), agg0 (col 1), agg1 (col 2), ...].
        let local = cb.reduce_multi_local(filtered, &[], &circuit_specs, false);

        // Phase 2 (exchange) + Phase 3 (combine). reduce_multi inserts the single
        // ExchangeShard(∅) routing every partial (all at PK V₀) to V₀'s owner, then
        // the combine reduce sums each partial column: a COUNT/COUNT_NON_NULL partial
        // sums with SumZero (Sum fold, 0 ground — a COUNT's empty value is 0, not
        // NULL), a SUM partial with plain Sum (NULL ground). The user aggregate
        // columns land at the same positions as the funnel reduce's, so the post-map
        // is unchanged; the trailing COUNT-of-partials is the existence gate (the
        // reduce's cardinality gate finds it via the lone AggOp::Count). global_ground
        // = true: an empty global source emits exactly one ground row here.
        let mut combine_specs: Vec<(u64, usize)> = circuit_specs
            .iter()
            .enumerate()
            .map(|(i, (op, _))| {
                let merge = match *op {
                    AGG_COUNT | AGG_COUNT_NON_NULL => AGG_SUM_ZERO,
                    AGG_SUM => AGG_SUM,
                    _ => unreachable!("two_phase implies all-linear, integer specs"),
                };
                // Local output column `1 + i` holds local agg `i` (col 0 is _group_pk).
                (merge, 1 + i)
            })
            .collect();
        combine_specs.push((AGG_COUNT, 0)); // COUNT-of-partials existence gate
        cb.reduce_multi(local, &[], &combine_specs, true)
    } else if source_replicated {
        // Shard-free: every worker reduces its full local copy to the same global
        // aggregate (no ExchangeShard ⇒ no gather barrier, no N-fold sum).
        cb.reduce_multi_local(filtered, &reduce_group_cols, &circuit_specs, global_ground)
    } else {
        cb.reduce_multi(filtered, &reduce_group_cols, &circuit_specs, global_ground)
    };

    // 6. Post-reduce MAP: project group cols + compute aggregates (AVG = SUM/COUNT)
    //    Reduce output: [pk, (group_cols...), agg0, agg1, ...]
    //    MAP inherits PK from input; ExprProgram writes payload columns only.
    //    Natural-PK group cols are part of that inherited PK region — the alias
    //    renames the PK slot in place (no payload copy). Synthetic-PK group cols
    //    are written to payload. Output: [pk(inherited, renamed), …payload…].
    let mut post_map_eb = ExprBuilder::new();
    let mut out_cols: Vec<ColumnDef> = Vec::new();
    let mut payload_idx: u32 = 0;

    // PK region in the output schema (inherited by MAP, not written by ExprProgram):
    // the full source PK in source-PK order for a compound natural PK; the lone
    // leading column for the single-natural / synthetic paths. `reduce_schema.pk_cols`
    // is dense (`0..pk_count`), so the leading columns are exactly the PK region.
    out_cols.extend(reduce_schema.columns[..reduce_schema.pk_cols.len()].iter().cloned());

    // Tracks which PK slots a natural-PK group col has already renamed in place,
    // so a group column selected twice with a different alias falls back to a
    // payload COPY_COL copy rather than silently overwriting the first alias.
    let mut pk_renamed = vec![false; reduce_schema.pk_cols.len()];

    for si in &select_items {
        match si {
            GroupBySelectItem::GroupCol { src_col, name } => {
                // Find group col position in reduce output (routed through the
                // shared helper so SELECT and HAVING cannot drift on the
                // source-PK-order mapping a permuted `group_set_eq_pk` needs).
                let reduce_col = group_col_reduce_pos(
                    *src_col,
                    group_set_eq_pk,
                    single_col_natural_pk,
                    &source_schema,
                    &group_col_indices,
                );
                let tc = reduce_schema.columns[reduce_col].type_code;
                // On both natural-PK paths `group_col_reduce_pos` returns a position
                // within the dense PK region, so on the `is_natural` branch
                // `reduce_col` always indexes a real PK slot in `out_cols` /
                // `pk_renamed` (asserted below). `&&` short-circuits, so the index
                // is only evaluated once `is_natural` holds — the synthetic path's
                // larger `reduce_col` never reaches it.
                let is_natural = group_set_eq_pk || single_col_natural_pk;
                if is_natural && !pk_renamed[reduce_col] {
                    // First projection of this group column: rename the PK slot
                    // in-place. The MAP inherits the PK region verbatim — no
                    // COPY_COL needed, and the column must not be re-pushed to
                    // out_cols (it is already there from the PK-region extend).
                    debug_assert!(
                        reduce_col < reduce_schema.pk_cols.len(),
                        "GROUP BY natural-PK rename: reduce_col {reduce_col} is not a PK slot \
                         (pk_cols.len() = {})",
                        reduce_schema.pk_cols.len(),
                    );
                    out_cols[reduce_col].name = name.clone();
                    pk_renamed[reduce_col] = true;
                } else {
                    // Synthetic-PK path, or second projection of the same group column.
                    post_map_eb.copy_col(tc as u32, reduce_col as u32, payload_idx);
                    // Natural-PK path (group_set_eq_pk / single_col_natural_pk):
                    // the source col is non-nullable. Synthetic-PK path: propagate
                    // source nullability — nothing forces NOT NULL.
                    out_cols.push(ColumnDef::new(
                        name.clone(),
                        tc,
                        source_schema.columns[*src_col].is_nullable,
                    ));
                    payload_idx += 1;
                }
            }
            GroupBySelectItem::Aggregate { agg_idx } => {
                let m = &agg_mappings[*agg_idx];
                let sum_col = agg_col_offset + m.specs_start;
                let cnt_col = agg_col_offset + m.specs_start + 1;
                match m.shape {
                    AggShape::Avg => {
                        // AVG = SUM / COUNT: two agg_specs were pushed (SUM, COUNT).
                        // For a float source, the SUM accumulator stores IEEE-754
                        // bits; loading those as an int and casting numerically would
                        // produce a wildly wrong value. Load them directly as float.
                        let sum_f = if m.arg_is_float(&source_schema) {
                            post_map_eb.load_col_float(sum_col)
                        } else {
                            let sum_reg = post_map_eb.load_col_int(sum_col);
                            post_map_eb.int_to_float(sum_reg)
                        };
                        let cnt_reg = post_map_eb.load_col_int(cnt_col);
                        let cnt_f = post_map_eb.int_to_float(cnt_reg);
                        let avg_reg = post_map_eb.float_div(sum_f, cnt_f);
                        post_map_eb.emit_col(avg_reg, payload_idx);
                        // AVG of an empty / all-NULL group is NULL (COUNT_NON_NULL=0
                        // → float_div by zero marks the result NULL), so the column
                        // must be nullable to match what the circuit can emit.
                        out_cols.push(ColumnDef::new(m.output_name.clone(), TypeCode::F64, true));
                        payload_idx += 1;
                    }
                    AggShape::NullfillSum => {
                        // Nullable-source SUM: the raw SUM column's null bit is the
                        // accumulator's `has_value`, which saturates true on the
                        // linear fold and so emits a concrete 0 (not NULL) when the
                        // last non-null contributor is retracted from a surviving
                        // group. Derive null-ness from the hidden COUNT_NON_NULL
                        // companion instead, the same way AVG does — divide the SUM by
                        // `cnt != 0`: the divisor is 1 when the count is positive (an
                        // exact identity divisor — `x / 1` for every i64, `x / 1.0`
                        // for float) and 0 when it is zero (div-by-zero marks the row
                        // NULL). This distinguishes SUM({5,-5})=0 from SUM({NULL})=NULL.
                        // Unlike AVG the gate is type-preserving: SUM keeps its own
                        // output type (I64, or F64 for a float source).
                        let cnt_reg = post_map_eb.load_col_int(cnt_col);
                        let zero = post_map_eb.load_const(0);
                        let gate = post_map_eb.cmp_ne(cnt_reg, zero);
                        let gated = if m.arg_is_float(&source_schema) {
                            let sum_f = post_map_eb.load_col_float(sum_col);
                            let gate_f = post_map_eb.int_to_float(gate);
                            post_map_eb.float_div(sum_f, gate_f)
                        } else {
                            let sum_reg = post_map_eb.load_col_int(sum_col);
                            post_map_eb.div(sum_reg, gate)
                        };
                        post_map_eb.emit_col(gated, payload_idx);
                        // A surviving group whose non-null count is zero yields
                        // NULL (div-by-zero), so the column must be nullable.
                        out_cols.push(ColumnDef::new(m.output_name.clone(), m.output_type, true));
                        payload_idx += 1;
                    }
                    AggShape::Direct => {
                        // Direct aggregate: single spec copied straight through.
                        let tc = reduce_schema.columns[sum_col].type_code;
                        post_map_eb.copy_col(tc as u32, sum_col as u32, payload_idx);
                        // SUM/MIN/MAX emit NULL for an all-NULL group (emit.rs sets
                        // the null bit when the accumulator is_untouched() and not
                        // empty_renders_zero(), since they skip NULL inputs).
                        // COUNT/COUNT_NON_NULL always return an integer (0, never
                        // NULL — empty_renders_zero()). Match emit.rs so a
                        // schema-driven decoder reads NULL instead of raw zero bytes.
                        out_cols.push(ColumnDef::new(
                            m.output_name.clone(),
                            m.output_type,
                            matches!(m.agg_func, AggFunc::Sum | AggFunc::Min | AggFunc::Max),
                        ));
                        payload_idx += 1;
                    }
                }
            }
        }
    }

    // The result_reg for a MAP program is typically 0 (true = pass through)
    let post_map_prog = post_map_eb.build(0);

    // 7. Optional HAVING filter, applied to the grouped relation *before* the
    //    SELECT projection — the relational order standard SQL specifies. Filter
    //    and map are both row-wise linear operators and commute, so this is
    //    semantically and incrementally sound. Binding against reduce_schema lets
    //    HAVING reference group columns by their source name (unaffected by
    //    SELECT aliases or omission) and aggregates that are not projected.
    let filtered_reduced = if let Some(having_expr) = &select.having {
        let bound = bind_having_expr(
            having_expr,
            &HavingCtx {
                source_schema: &source_schema,
                group_col_indices: &group_col_indices,
                group_set_eq_pk,
                single_col_natural_pk,
                agg_mappings: &agg_mappings,
                agg_col_offset,
            },
        )?;
        cb.filter(reduced, Some(compile_bound_expr_to_program(&bound, &reduce_schema)?))
    } else {
        reduced
    };

    let mapped = cb.map_expr(filtered_reduced, post_map_prog);

    // 8. Sink
    cb.sink(mapped);
    let circuit = cb.build();

    // A SELECT that names the same group column twice (e.g. `k, k AS k2` is fine,
    // but `k, k` collides) must be caught cleanly rather than registering a view
    // with duplicate column names.
    reject_duplicate_column_names(out_cols.iter().map(|c| c.name.as_str()), "GROUP BY view")?;

    // The view's physical PK is the reduce output's PK region: the full source
    // PK (source-PK order) for a compound natural PK, else the lone leading
    // column. `reduce_schema.pk_cols` is dense (`0..pk_count`).
    let view_pk: Vec<u32> = (0..reduce_schema.pk_cols.len() as u32).collect();
    client
        .create_view_with_circuit(schema_name, view_name, sql_text, circuit, &out_cols, &view_pk)
        .map_err(GnitzSqlError::Exec)?;

    Ok(SqlResult::ViewCreated { view_id })
}

/// Resolve a HAVING function call to its aggregate function selector + argument
/// column, shared by collection and binding so both agree on what an aggregate
/// reference means.
fn having_agg_func(
    func: &sqlparser::ast::Function,
    source_schema: &Schema,
) -> Result<(AggFunc, Option<usize>), GnitzSqlError> {
    reject_unsupported_agg_qualifiers(func)?;
    let name = func.name.to_string().to_lowercase();
    let arg_col = extract_func_arg_col(func, source_schema)?;
    let agg_func = match name.as_str() {
        "count" => {
            if arg_col.is_none() {
                AggFunc::Count
            } else {
                AggFunc::CountNonNull
            }
        }
        "sum" => AggFunc::Sum,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        "avg" => AggFunc::Avg,
        _ => {
            return Err(GnitzSqlError::Unsupported(format!(
                "HAVING: function '{name}' not supported"
            )))
        }
    };
    Ok((agg_func, arg_col))
}

/// True iff `m` is the reduce mapping for the aggregate `(agg_func, arg_col)`.
/// COUNT(*) ignores arg_col (it has none); every other form matches the source
/// column too, so `MAX(c2)` never binds to `SUM(c1)`.
fn agg_mapping_matches(m: &AggMapping, agg_func: AggFunc, arg_col: Option<usize>) -> bool {
    match agg_func {
        AggFunc::Avg => m.agg_func == AggFunc::Avg && m.arg_col == arg_col,
        AggFunc::Count => m.agg_func == AggFunc::Count,
        AggFunc::CountNonNull => m.agg_func == AggFunc::CountNonNull && m.arg_col == arg_col,
        AggFunc::Sum => m.agg_func == AggFunc::Sum && m.arg_col == arg_col,
        AggFunc::Min => m.agg_func == AggFunc::Min && m.arg_col == arg_col,
        AggFunc::Max => m.agg_func == AggFunc::Max && m.arg_col == arg_col,
    }
}

/// Push the engine `agg_specs` for one aggregate and return whether it is an
/// AVG (which materialises two specs — SUM then COUNT_NON_NULL). The single
/// source of truth for the spec layout — and for each spec's output column type,
/// recorded here (the one place the AVG split lives) so the reduce schema
/// builder never reconstructs it from the op code. Shared by the SELECT
/// projection and the HAVING-only materialisation so the two stay in lockstep —
/// notably the AVG-emits-two-specs invariant, on which the reduce-output column
/// positions and `AggMapping::specs_start` both depend.
fn push_agg_specs(
    agg_func: AggFunc,
    arg_col: Option<usize>,
    schema: &Schema,
    agg_specs: &mut Vec<AggSpec>,
) -> Result<AggShape, GnitzSqlError> {
    // Every aggregate except COUNT(*) needs a column argument, which the specs
    // below unwrap. Validating here — the single source of truth for spec
    // layout — covers both the SELECT-list and HAVING callers, so neither needs
    // its own wildcard guard and a future caller cannot reintroduce the panic.
    if !matches!(agg_func, AggFunc::Count) && arg_col.is_none() {
        return Err(GnitzSqlError::Plan(format!(
            "{agg_func:?} requires an argument column; only COUNT(*) accepts a wildcard"
        )));
    }
    // Reject argument column types the engine cannot evaluate. Single validated
    // gate for both the SELECT-list and HAVING-only (`append_having_agg`) callers.
    // SELECT-list MIN/MAX is also rejected earlier by the leaf binder, so only the
    // HAVING-only path reaches the MIN/MAX arm here.
    if let Some(c) = arg_col {
        let tc = schema.columns[c].type_code;
        match agg_func {
            AggFunc::Sum | AggFunc::Avg => {
                if !(is_integer_type(tc) || tc.is_float()) || is_wide_int(tc) {
                    return Err(GnitzSqlError::Bind(format!(
                        "{agg_func:?} is not supported on column type {tc:?} ('{}')",
                        schema.columns[c].name,
                    )));
                }
            }
            AggFunc::Min | AggFunc::Max => {
                if !is_min_max_orderable(tc) {
                    return Err(GnitzSqlError::Bind(format!(
                        "{agg_func:?} is not supported on column type {tc:?} ('{}')",
                        schema.columns[c].name,
                    )));
                }
            }
            AggFunc::Count | AggFunc::CountNonNull => {}
        }
    }
    let mut push = |op: u64, func: AggFunc, col: usize| {
        agg_specs.push(AggSpec {
            op,
            col,
            out_type: agg_result_type(func, Some(col), schema),
        });
    };
    Ok(match agg_func {
        AggFunc::Count => {
            push(AGG_COUNT, AggFunc::Count, 0);
            AggShape::Direct
        }
        AggFunc::CountNonNull => {
            push(AGG_COUNT_NON_NULL, AggFunc::CountNonNull, arg_col.unwrap());
            AggShape::Direct
        }
        AggFunc::Sum => {
            let c = arg_col.unwrap();
            push(AGG_SUM, AggFunc::Sum, c);
            // A nullable source means the group's non-null count can fall back to
            // zero — its last contributor retracted — while the group still
            // survives (via COUNT(*) or another aggregate), which SQL renders as
            // NULL. The raw SUM column cannot express that on the linear fold: its
            // `has_value` boolean saturates true and never returns to false, so a
            // netted-to-zero SUM emits a concrete 0 where NULL is correct. Attach a
            // hidden COUNT_NON_NULL companion and let the finalize null-gate the
            // SUM on it — distinguishing SUM({5,-5})=0 from SUM({NULL})=NULL. A
            // non-nullable source can never be NULL on a surviving group, so it
            // keeps its plain single-spec copy.
            if schema.columns[c].is_nullable {
                push(AGG_COUNT_NON_NULL, AggFunc::CountNonNull, c);
                AggShape::NullfillSum
            } else {
                AggShape::Direct
            }
        }
        AggFunc::Min => {
            push(AGG_MIN, AggFunc::Min, arg_col.unwrap());
            AggShape::Direct
        }
        AggFunc::Max => {
            push(AGG_MAX, AggFunc::Max, arg_col.unwrap());
            AggShape::Direct
        }
        AggFunc::Avg => {
            let c = arg_col.unwrap();
            push(AGG_SUM, AggFunc::Sum, c);
            push(AGG_COUNT_NON_NULL, AggFunc::CountNonNull, c);
            AggShape::Avg
        }
    })
}

/// Push the agg_specs + AggMapping for one HAVING-only aggregate (one absent
/// from the SELECT list). Reuses `push_agg_specs` so the reduce-output column
/// positions line up with the SELECT projection. The mapping is found later by
/// `agg_mapping_matches`.
fn append_having_agg(
    agg_func: AggFunc,
    arg_col: Option<usize>,
    source_schema: &Schema,
    agg_specs: &mut Vec<AggSpec>,
    agg_mappings: &mut Vec<AggMapping>,
) -> Result<(), GnitzSqlError> {
    let out_type = agg_result_type(agg_func, arg_col, source_schema);
    let agg_idx = agg_mappings.len();
    let start = agg_specs.len();
    let shape = push_agg_specs(agg_func, arg_col, source_schema, agg_specs)?;
    agg_mappings.push(AggMapping {
        specs_start: start,
        shape,
        output_name: format!("_having_agg{agg_idx}"),
        output_type: out_type,
        agg_func,
        arg_col,
    });
    Ok(())
}

/// Recursively collect aggregate calls referenced in a HAVING expression,
/// appending any not already present in `agg_mappings`. Descends through
/// `expr_operands` — the binder's node set — so every construct the binder
/// reaches (`agg IS NULL`, `agg BETWEEN lo AND hi`, `agg IN (…)`)
/// materialises its aggregates; a node bound but not walked here would bind an
/// aggregate that was never materialised and fail `resolve_having_mapping`.
fn collect_having_aggs(
    expr: &Expr,
    source_schema: &Schema,
    agg_specs: &mut Vec<AggSpec>,
    agg_mappings: &mut Vec<AggMapping>,
) -> Result<(), GnitzSqlError> {
    if let Expr::Function(func) = expr {
        let (agg_func, arg_col) = having_agg_func(func, source_schema)?;
        if !agg_mappings.iter().any(|m| agg_mapping_matches(m, agg_func, arg_col)) {
            append_having_agg(agg_func, arg_col, source_schema, agg_specs, agg_mappings)?;
        }
        return Ok(());
    }
    for operand in expr_operands(expr) {
        collect_having_aggs(operand, source_schema, agg_specs, agg_mappings)?;
    }
    Ok(())
}

/// Invariant context for `bind_having_expr`'s recursion: everything needed to
/// resolve a HAVING identifier or aggregate call against the reduce-output
/// (grouped) relation. Bundled so the recursion threads one `&self` instead of
/// re-passing five unchanging arguments at every node. (The reduce schema
/// itself is not needed for binding — the caller compiles the bound expression
/// against it separately.)
struct HavingCtx<'a> {
    source_schema: &'a Schema,
    group_col_indices: &'a [usize],
    group_set_eq_pk: bool,
    single_col_natural_pk: bool,
    agg_mappings: &'a [AggMapping],
    agg_col_offset: usize,
}

/// Resolve a HAVING aggregate function reference to its reduce `AggMapping`, or a
/// Bind error naming the unresolved aggregate. Shared by the value-position binder
/// (`bind_having_expr`) and the IS [NOT] NULL binder (`bind_having_null_test`) so
/// the lookup and its error message stay in one place.
fn resolve_having_mapping<'a>(
    func: &sqlparser::ast::Function,
    ctx: &HavingCtx<'a>,
) -> Result<&'a AggMapping, GnitzSqlError> {
    let (agg_func, arg_col) = having_agg_func(func, ctx.source_schema)?;
    ctx.agg_mappings
        .iter()
        .find(|m| agg_mapping_matches(m, agg_func, arg_col))
        .ok_or_else(|| {
            GnitzSqlError::Bind(format!(
                "HAVING: aggregate {:?}({}) could not be resolved",
                agg_func,
                arg_col.map_or("*".to_string(), |c| ctx.source_schema.columns[c].name.clone()),
            ))
        })
}

/// Bind a HAVING expression against the reduce-output (grouped) relation —
/// before the SELECT projection, as standard SQL specifies. Group-column
/// identifiers resolve by their source name (unaffected by SELECT aliases or
/// omission); aggregate calls resolve to their reduce-output column. The
/// structural recursion is shared with WHERE/residual via `bind_structural`, so
/// HAVING inherits the full operator map (incl. `Mul`/`Div`/`Mod`), `UnaryOp`,
/// and the `BETWEEN` desugar from the core — the `Having` leaf supplies only the
/// three grouped-relation decisions.
fn bind_having_expr(expr: &Expr, ctx: &HavingCtx) -> Result<BoundExpr, GnitzSqlError> {
    bind_structural(expr, &Having { ctx })
}

/// `LeafBinder` for HAVING (the grouped relation).
struct Having<'a> {
    ctx: &'a HavingCtx<'a>,
}

impl LeafBinder for Having<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        let ctx = self.ctx;
        // HAVING references the grouped relation by source column name. A qualified
        // ref (`t.g`) resolves the bare name the same way — the qualifier carries no
        // disambiguating information over a single grouped relation.
        let col_name = single_relation_col_name(e)
            .ok_or_else(|| GnitzSqlError::Unsupported(format!("HAVING: unsupported column reference {e:?}")))?;
        let src = find_unique_column(&ctx.source_schema.columns, col_name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("HAVING: column '{col_name}' not found")))?;
        // HAVING may only reference grouped columns.
        if !ctx.group_col_indices.contains(&src) {
            return Err(GnitzSqlError::Bind(format!(
                "HAVING: column '{col_name}' must appear in GROUP BY or an aggregate function"
            )));
        }
        // Map the name to its reduce-output position (natural-PK grouping puts the
        // lone group col at the PK index 0; the synthetic path lays group cols out
        // after the U128 _group_pk).
        let reduce_col = group_col_reduce_pos(
            src,
            ctx.group_set_eq_pk,
            ctx.single_col_natural_pk,
            ctx.source_schema,
            ctx.group_col_indices,
        );
        Ok(BoundExpr::ColRef(reduce_col))
    }
    fn bind_function(&self, func: &sqlparser::ast::Function) -> Result<BoundExpr, GnitzSqlError> {
        let ctx = self.ctx;
        let m = resolve_having_mapping(func, ctx)?;
        let sum_col = ctx.agg_col_offset + m.specs_start;
        let cnt_col = ctx.agg_col_offset + m.specs_start + 1;
        match m.shape {
            AggShape::Avg => {
                // AVG = SUM / COUNT, both materialised as reduce columns. Force float
                // division (an int-source SUM/COUNT would otherwise truncate) by
                // lifting SUM to float via `* 1.0`.
                let sum_f = BoundExpr::BinOp(
                    Box::new(BoundExpr::ColRef(sum_col)),
                    BinOp::Mul,
                    Box::new(BoundExpr::LitFloat(1.0)),
                );
                Ok(BoundExpr::BinOp(
                    Box::new(sum_f),
                    BinOp::Div,
                    Box::new(BoundExpr::ColRef(cnt_col)),
                ))
            }
            AggShape::NullfillSum => {
                // Same companion gate the SELECT projection applies: the raw SUM
                // column saturates to a concrete 0 once its last non-null contributor
                // is retracted, so read null-ness from the COUNT_NON_NULL companion
                // via `sum / (cnt != 0)` — an exact identity divisor (1) while the
                // count is positive, div-by-zero → NULL when it is 0.
                // `compile_bound_expr` dispatches int vs float Div on the SUM column's
                // type, so the gate is type-preserving without an explicit branch here.
                Ok(BoundExpr::BinOp(
                    Box::new(BoundExpr::ColRef(sum_col)),
                    BinOp::Div,
                    Box::new(BoundExpr::BinOp(
                        Box::new(BoundExpr::ColRef(cnt_col)),
                        BinOp::Ne,
                        Box::new(BoundExpr::LitInt(0)),
                    )),
                ))
            }
            AggShape::Direct => Ok(BoundExpr::ColRef(sum_col)),
        }
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
        // Only aggregates whose null-ness is carried by a hidden COUNT_NON_NULL
        // companion — nullable-source SUM and AVG — are supported: their value
        // column's raw null bit is unreliable (a linear-fold SUM emits 0, not NULL,
        // once its last non-null contributor is retracted, and AVG has no single raw
        // column), so the projected null-ness is `companion == 0`. Binding the test
        // to `companion {== | !=} 0` keeps HAVING in agreement with the SELECT
        // projection's gate.
        match inner {
            Expr::Nested(i) => self.bind_null_test(i, want_null),
            Expr::Function(func) => {
                let m = resolve_having_mapping(func, self.ctx)?;
                if m.shape.has_count_companion() {
                    // The companion COUNT_NON_NULL sits at specs_start + 1; the value
                    // is NULL exactly when that count is 0.
                    let cnt_col = self.ctx.agg_col_offset + m.specs_start + 1;
                    let bop = if want_null { BinOp::Eq } else { BinOp::Ne };
                    Ok(BoundExpr::BinOp(
                        Box::new(BoundExpr::ColRef(cnt_col)),
                        bop,
                        Box::new(BoundExpr::LitInt(0)),
                    ))
                } else {
                    Err(GnitzSqlError::Unsupported(format!(
                        "HAVING: IS [NOT] NULL on {:?} is not supported",
                        m.agg_func
                    )))
                }
            }
            _ => Err(GnitzSqlError::Unsupported(
                "HAVING: IS [NOT] NULL is only supported on nullable SUM/AVG aggregates".to_string(),
            )),
        }
    }
}

/// Extract the column index from a function argument (for HAVING binding).
fn extract_func_arg_col(func: &sqlparser::ast::Function, schema: &Schema) -> Result<Option<usize>, GnitzSqlError> {
    match &func.args {
        FunctionArguments::List(list) if list.args.len() == 1 => match &list.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
                let idx = find_unique_column(&schema.columns, &id.value)?
                    .ok_or_else(|| GnitzSqlError::Bind(format!("column '{}' not found", id.value)))?;
                Ok(Some(idx))
            }
            _ => Err(GnitzSqlError::Unsupported(
                "HAVING: unsupported function argument".to_string(),
            )),
        },
        _ => Err(GnitzSqlError::Unsupported(
            "HAVING: function requires one argument".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::col_def;

    // Columns: 0=pk(U64), 1=n(I64), 2=b(Blob), 3=u(UUID), 4=s(String).
    fn schema() -> Schema {
        Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("n", TypeCode::I64, true),
                col_def("b", TypeCode::Blob, true),
                col_def("u", TypeCode::UUID, true),
                col_def("s", TypeCode::String, true),
            ],
            pk_cols: vec![0],
        }
    }

    fn try_push(func: AggFunc, arg_col: Option<usize>) -> Result<AggShape, GnitzSqlError> {
        let mut specs = Vec::new();
        push_agg_specs(func, arg_col, &schema(), &mut specs)
    }

    #[test]
    fn push_agg_specs_rejects_unevaluatable_arg_types() {
        assert!(matches!(try_push(AggFunc::Sum, Some(2)), Err(GnitzSqlError::Bind(_)))); // SUM(blob)
        assert!(matches!(try_push(AggFunc::Avg, Some(3)), Err(GnitzSqlError::Bind(_)))); // AVG(uuid)
        assert!(matches!(try_push(AggFunc::Min, Some(4)), Err(GnitzSqlError::Bind(_)))); // MIN(str)
        assert!(matches!(try_push(AggFunc::Max, Some(2)), Err(GnitzSqlError::Bind(_))));
        // MAX(blob)
    }

    #[test]
    fn push_agg_specs_accepts_valid_arg_types() {
        assert!(try_push(AggFunc::Sum, Some(1)).is_ok()); // SUM(i64)
        assert!(try_push(AggFunc::Avg, Some(1)).is_ok()); // AVG(i64)
        assert!(try_push(AggFunc::Min, Some(1)).is_ok()); // MIN(i64)
        assert!(try_push(AggFunc::Count, None).is_ok()); // COUNT(*)
        assert!(try_push(AggFunc::CountNonNull, Some(2)).is_ok()); // COUNT(blob) — presence only
    }
}
