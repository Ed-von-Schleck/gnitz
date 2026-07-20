//! GROUP BY / HAVING / aggregation view compilation over the shared aggregate
//! layout model (`crate::agg`): the shared analysis (`analyze_group_by`), the
//! reduce-output schema layout, the post-reduce projection, and the HAVING
//! cluster (collection, binding, and the `Having` leaf binder over the grouped
//! relation).

use crate::agg::{
    agg_arg_col, append_agg_mapping, emit_reduce, ensure_cardinality_count, group_col_reduce_pos, reduce_output_schema,
    AggMapping, AggShape, AggSpec, GroupByLayout, GroupBySelectItem, ReduceShape,
};
use crate::ast_util::{expr_operands, single_relation_col_name};
use crate::bind::{bind_single_table, bind_structural, find_unique_column, fold_null_test, LeafBinder, SingleTable};
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BinOp, BoundExpr};
use crate::lower::compile_filter_program;
use crate::plan::validate::{
    reject_duplicate_column_names, reject_float_key, reject_unhonored_select_clauses, HonoredClauses,
};
use crate::plan::view::EmitPieces;
use gnitz_core::{CircuitBuilder, ColumnDef, ExprBuilder, GnitzClient, ReduceOutKey, Schema};
use sqlparser::ast::{Expr, GroupByExpr, SelectItem};

/// Analyze a single-relation aggregate `select` against `source_schema` into its
/// physical [`GroupByLayout`] — GROUP BY resolution, the projection-mixing rule,
/// aggregate spec/mapping construction, the SUM/AVG and MIN/MAX type gates, and
/// HAVING-only aggregate materialization. Pure analysis: no clause gate (each
/// caller invokes it), no COUNT(*) companion (the view path appends it), no
/// circuit.
pub(crate) fn analyze_group_by(
    select: &sqlparser::ast::Select,
    source_schema: &Schema,
) -> Result<GroupByLayout, GnitzSqlError> {
    // Parse GROUP BY → group column indices.
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
        // Bare or qualified (`t.g`) single-relation reference — the qualifier
        // carries no disambiguating information over the single grouped source,
        // matching HAVING and the projection (`bind_single_table`).
        let name = single_relation_col_name(ge).ok_or_else(|| {
            GnitzSqlError::Unsupported("GROUP BY: only simple column references supported".to_string())
        })?;
        let idx = find_unique_column(&source_schema.columns, name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("GROUP BY column '{name}' not found")))?;
        reject_float_key(&source_schema.columns[idx], "GROUP BY")?;
        group_col_indices.push(idx);
    }

    // An empty group set is the user's ungrouped (global) scalar aggregate —
    // `SELECT MIN(x) FROM t` with no GROUP BY. It compiles as a one-row reduce
    // (synthetic `_group_pk` at the constant V₀) that must emit exactly one row
    // even over an empty/fully-retracted source, so the engine seeds a ground row
    // (COUNT=0, SUM/MIN/MAX/AVG=NULL) — which is also why a global aggregate's
    // output is always nullable. Grouped reduces (`group_col_indices` non-empty)
    // pass `false` and are byte-for-byte unchanged.
    let global_ground = group_col_indices.is_empty();

    // Analyze SELECT items → group cols + aggregates.
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

        let bound = bind_single_table(expr, source_schema)?;
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
                let src_col = agg_arg_col(arg.as_deref())?;
                let agg_idx = agg_mappings.len();
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
                append_agg_mapping(
                    *func,
                    src_col,
                    out_name,
                    global_ground,
                    source_schema,
                    &mut agg_specs,
                    &mut agg_mappings,
                )?;
                select_items.push(GroupBySelectItem::Aggregate { agg_idx });
            }
            _ => {
                return Err(GnitzSqlError::Plan(
                    "GROUP BY SELECT: only column refs and aggregates supported".to_string(),
                ))
            }
        }
    }

    // Materialise aggregates referenced only by HAVING. HAVING is evaluated
    // over the grouped relation, so every aggregate it references needs an
    // agg_spec and a reduce-output column even when the SELECT list omits it.
    if let Some(having_expr) = &select.having {
        collect_having_aggs(
            having_expr,
            global_ground,
            source_schema,
            &mut agg_specs,
            &mut agg_mappings,
        )?;
    }

    Ok(GroupByLayout {
        group_col_indices,
        agg_specs,
        agg_mappings,
        select_items,
    })
}

/// Emit a GROUP BY view's reduce circuit from its `select`, returning
/// `(circuit, output_columns, pk_cols)`. `view_id` is pre-allocated so a chain's
/// downstream segment can reference this segment's store; the segment itself is
/// created by the caller. The view's PK is the group key — a natural PK, so a
/// grouped hidden segment needs no synthetic-PK (`vis`) offset when joined.
///
/// `source` is the pre-resolved input relation `(tid, schema)` — the FROM table,
/// or a hidden view H compiled from a JOIN input, over which the group /
/// aggregate / HAVING columns resolve by name (a qualifier on a compound ref is
/// informational in a single-source context — see `bind_single_table`).
/// `bound` narrows the initial full-source backfill scan to a secondary-index
/// range. A physical access hint only: the WHERE below is emitted verbatim either
/// way, so the bound never changes what the view contains.
pub(crate) fn emit_group_by_pieces(
    client: &mut GnitzClient,
    view_id: u64,
    select: &sqlparser::ast::Select,
    source: (u64, std::rc::Rc<Schema>),
    bound: Option<gnitz_wire::ScanBound>,
) -> Result<EmitPieces, GnitzSqlError> {
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
    let (source_tid, source_schema) = source;
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
    // (`build_reduce_output_schema` walks `pk_columns()` for `PkPermutation`),
    // the helpers below map group columns through `group_col_reduce_pos`, and the
    // co-partition analyzers now compare the full PK sequence — so a reduce that
    // shards by one component of a compound PK gets the exchange it needs.

    // GROUP BY resolution, projection-mixing rule, aggregate spec/mapping
    // construction, and HAVING-only aggregate materialization — the shared
    // analysis (`analyze_group_by`), also driving the ad-hoc path. The clause
    // gate above and the COUNT(*) companion below stay here.
    let layout = analyze_group_by(select, &source_schema)?;
    let GroupByLayout {
        group_col_indices,
        mut agg_specs,
        agg_mappings,
        select_items,
    } = layout;

    ensure_cardinality_count(&source_schema.columns, &mut agg_specs)?;

    // The reduce's physical shape. Its `out_key` is the output-key kind shipped
    // to the engine (see `ReduceOutKey`: the planner owns this decision, the
    // engine validates and obeys it) — schema layout, shard key, and column
    // positions all derive from that one value. The two empty-group reduces
    // (two-phase local / combine) always ship `SyntheticFold`: an empty group set
    // is neither natural kind.
    let shape = ReduceShape::new(&source_schema, &group_col_indices, &agg_specs, source_replicated);
    let out_key = shape.out_key;
    let (reduce_schema, agg_col_offset) = reduce_output_schema(&shape);

    // Build circuit
    let mut cb = CircuitBuilder::new(view_id, source_tid);
    let inp = cb.input_delta_bounded(bound);

    // Optional WHERE filter (elided when the predicate bound to a true constant).
    // Emitted VERBATIM — the index conjuncts a `bound` covers are NOT trimmed out
    // of it: a steady-state push never consults the bound, so a row failing the
    // index predicate but passing the residual would leak if the filter were
    // narrowed. (A wholly-constant WHERE elides the node, but cannot co-occur with
    // a bound: it has no `col OP literal` conjunct for a candidate to come from.)
    let filtered = if let Some(where_expr) = &select.selection {
        let pred = bind_single_table(where_expr, &source_schema)?;
        match compile_filter_program(&pred, &source_schema)? {
            Some(p) => cb.filter(inp, Some(p)),
            None => inp,
        }
    } else {
        inp
    };

    // REDUCE — the shared strategy selection (two-phase / replicated / sharded).
    let reduced = emit_reduce(&mut cb, filtered, &shape);

    // Post-reduce MAP: project group cols + compute aggregates (AVG = SUM/COUNT)
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
                // source-PK-order mapping a permuted `PkPermutation` grouping needs).
                let reduce_col = group_col_reduce_pos(*src_col, out_key, &source_schema, &group_col_indices);
                let tc = reduce_schema.columns[reduce_col].type_code;
                // On both natural-PK paths `group_col_reduce_pos` returns a position
                // within the dense PK region, so on the `is_natural` branch
                // `reduce_col` always indexes a real PK slot in `out_cols` /
                // `pk_renamed` (asserted below). `&&` short-circuits, so the index
                // is only evaluated once `is_natural` holds — the synthetic path's
                // larger `reduce_col` never reaches it.
                let is_natural = out_key != ReduceOutKey::SyntheticFold;
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
                    // Natural-PK path (PkPermutation / SingleNaturalCol):
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
                        // AVG of an empty / all-NULL group is NULL (COUNT_NON_NULL=0
                        // → float_div by zero marks the result NULL), hence the
                        // blanket-nullable output.
                        post_map_eb.emit_col(avg_reg, payload_idx);
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
                        // A surviving group whose non-null count is zero yields
                        // NULL (div-by-zero), hence the blanket-nullable output.
                        post_map_eb.emit_col(gated, payload_idx);
                    }
                    AggShape::Direct => {
                        // Direct aggregate: single spec copied straight through,
                        // raw null bit included (set by emit.rs only for an
                        // all-NULL MIN/MAX group or a global ground row).
                        let tc = reduce_schema.columns[sum_col].type_code;
                        post_map_eb.copy_col(tc as u32, sum_col as u32, payload_idx);
                    }
                }
                // The declared nullability was fixed at mapping construction
                // (`AggMapping::output_nullable`) to exactly what the emission
                // above can render, so a schema-driven decoder never reads raw
                // zero bytes as a live value (or, for COUNT, a forbidden NULL) —
                // and a grouped SUM/MIN/MAX over a non-nullable source is NOT
                // NULL, letting downstream comparators skip null tracking.
                out_cols.push(ColumnDef::new(m.output_name.clone(), m.output_type, m.output_nullable));
                payload_idx += 1;
            }
        }
    }

    // The result_reg for a MAP program is typically 0 (true = pass through)
    let post_map_prog = post_map_eb.build(0);

    // Optional HAVING filter, applied to the grouped relation *before* the
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
                out_key,
                agg_mappings: &agg_mappings,
                agg_col_offset,
            },
        )?;
        // A HAVING that bound to a true constant (e.g. `IS NOT NULL` on a shape
        // that can never be NULL) compiles to no filter operator at all.
        match compile_filter_program(&bound, &reduce_schema)? {
            Some(p) => cb.filter(reduced, Some(p)),
            None => reduced,
        }
    } else {
        reduced
    };

    let mapped = cb.map_expr(filtered_reduced, post_map_prog);

    // Sink
    cb.sink(mapped);
    let circuit = cb.build();

    // A SELECT that names the same group column twice (e.g. `k, k AS k2` is fine,
    // but `k, k` collides) must be caught cleanly rather than registering a view
    // with duplicate column names.
    reject_duplicate_column_names(&out_cols, "GROUP BY view")?;

    // The view's physical PK is the reduce output's PK region: the full source
    // PK (source-PK order) for a compound natural PK, else the lone leading
    // column. `reduce_schema.pk_cols` is dense (`0..pk_count`).
    let view_pk: Vec<u32> = (0..reduce_schema.pk_cols.len() as u32).collect();
    Ok((circuit, out_cols, view_pk))
}

/// Resolve a HAVING function call to its aggregate function selector + argument
/// column, shared by collection and binding so both agree on what an aggregate
/// reference means. Routed through the same leaf binder as a SELECT-list
/// aggregate (`SingleTable::bind_function`), so the two surfaces share one
/// definition of a supported aggregate call — name set, qualifier rejection,
/// COUNT(*) vs COUNT(col), qualified arguments, and MIN/MAX orderability.
fn having_agg_func(
    func: &sqlparser::ast::Function,
    source_schema: &Schema,
) -> Result<(AggFunc, Option<usize>), GnitzSqlError> {
    match (SingleTable { schema: source_schema }).bind_function(func)? {
        BoundExpr::AggCall { func, arg } => Ok((func, agg_arg_col(arg.as_deref())?)),
        other => unreachable!("SingleTable::bind_function only binds aggregate calls, got {other:?}"),
    }
}

/// True iff `m` is the reduce mapping for the aggregate `(agg_func, arg_col)`.
/// COUNT(*) ignores arg_col (it has none); every other form matches the source
/// column too, so `MAX(c2)` never binds to `SUM(c1)`.
fn agg_mapping_matches(m: &AggMapping, agg_func: AggFunc, arg_col: Option<usize>) -> bool {
    m.agg_func == agg_func && (matches!(agg_func, AggFunc::Count) || m.arg_col == arg_col)
}

/// Recursively collect aggregate calls referenced in a HAVING expression,
/// appending any not already present in `agg_mappings`. Descends through
/// `expr_operands` — the binder's node set — so every construct the binder
/// reaches (`agg IS NULL`, `agg BETWEEN lo AND hi`, `agg IN (…)`)
/// materialises its aggregates; a node bound but not walked here would bind an
/// aggregate that was never materialised and fail `resolve_having_mapping`.
fn collect_having_aggs(
    expr: &Expr,
    is_global: bool,
    source_schema: &Schema,
    agg_specs: &mut Vec<AggSpec>,
    agg_mappings: &mut Vec<AggMapping>,
) -> Result<(), GnitzSqlError> {
    if let Expr::Function(func) = expr {
        let (agg_func, arg_col) = having_agg_func(func, source_schema)?;
        if !agg_mappings.iter().any(|m| agg_mapping_matches(m, agg_func, arg_col)) {
            let name = format!("_having_agg{}", agg_mappings.len());
            append_agg_mapping(
                agg_func,
                arg_col,
                name,
                is_global,
                source_schema,
                agg_specs,
                agg_mappings,
            )?;
        }
        return Ok(());
    }
    for operand in expr_operands(expr) {
        collect_having_aggs(operand, is_global, source_schema, agg_specs, agg_mappings)?;
    }
    Ok(())
}

/// Invariant context for `bind_having_expr`'s recursion: everything needed to
/// resolve a HAVING identifier or aggregate call against the reduce-output
/// (grouped) relation. Bundled so the recursion threads one `&self` instead of
/// re-passing five unchanging arguments at every node. (The reduce schema
/// itself is not needed for binding — the caller compiles the bound expression
/// against it separately.)
pub(crate) struct HavingCtx<'a> {
    pub(crate) source_schema: &'a Schema,
    pub(crate) group_col_indices: &'a [usize],
    pub(crate) out_key: ReduceOutKey,
    pub(crate) agg_mappings: &'a [AggMapping],
    pub(crate) agg_col_offset: usize,
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
pub(crate) fn bind_having_expr(expr: &Expr, ctx: &HavingCtx) -> Result<BoundExpr, GnitzSqlError> {
    bind_structural(expr, &Having { ctx })
}

/// `LeafBinder` for HAVING (the grouped relation).
struct Having<'a> {
    ctx: &'a HavingCtx<'a>,
}

impl Having<'_> {
    /// Resolve a group-column name to `(source column, reduce-output position)`,
    /// enforcing GROUP BY membership. The one name→position pipeline for every
    /// HAVING group-column reference (value position and null test alike), so
    /// the two cannot drift on the source-PK-order mapping a permuted
    /// `PkPermutation` grouping needs (natural-PK grouping puts group cols in
    /// the PK region; the synthetic path lays them out after the U128 _group_pk).
    fn resolve_group_col(&self, col_name: &str) -> Result<(usize, usize), GnitzSqlError> {
        let ctx = self.ctx;
        let src = find_unique_column(&ctx.source_schema.columns, col_name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("HAVING: column '{col_name}' not found")))?;
        // HAVING may only reference grouped columns.
        if !ctx.group_col_indices.contains(&src) {
            return Err(GnitzSqlError::Bind(format!(
                "HAVING: column '{col_name}' must appear in GROUP BY or an aggregate function"
            )));
        }
        let reduce_col = group_col_reduce_pos(src, ctx.out_key, ctx.source_schema, ctx.group_col_indices);
        Ok((src, reduce_col))
    }
}

impl LeafBinder for Having<'_> {
    fn bind_column(&self, e: &Expr) -> Result<BoundExpr, GnitzSqlError> {
        // HAVING references the grouped relation by source column name. A qualified
        // ref (`t.g`) resolves the bare name the same way — the qualifier carries no
        // disambiguating information over a single grouped relation.
        let col_name = single_relation_col_name(e)
            .ok_or_else(|| GnitzSqlError::Unsupported(format!("HAVING: unsupported column reference {e:?}")))?;
        let (_, reduce_col) = self.resolve_group_col(col_name)?;
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
        // IS [NOT] NULL over the grouped relation. A companion-carrying aggregate
        // (nullable SUM / AVG) reads the hidden COUNT_NON_NULL companion — its raw
        // value column's saturating has_value bit is unreliable (a linear-fold SUM
        // emits 0, not NULL, once its last non-null contributor is retracted, and
        // AVG has no single raw column). Everything else — a Direct aggregate's
        // value column or a bare group column — routes through the shared
        // `fold_null_test` with its authoritative nullability, so a never-null
        // shape const-folds: the fast-path win of `fold_null_test` (keeps
        // eval_batch's no_nulls arm), and for a non-nullable group column it also
        // keeps EXPR_IS_NULL off the PK sentinel (see eval_is_null's assertion).
        match inner {
            Expr::Nested(i) => self.bind_null_test(i, want_null),
            Expr::Function(func) => {
                let m = resolve_having_mapping(func, self.ctx)?;
                let val_col = self.ctx.agg_col_offset + m.specs_start;
                if m.shape.has_count_companion() {
                    // Nullable SUM / AVG: NULL ⇔ COUNT_NON_NULL companion (at
                    // specs_start + 1) is 0 — the same gate the SELECT
                    // projection applies.
                    let bop = if want_null { BinOp::Eq } else { BinOp::Ne };
                    Ok(BoundExpr::BinOp(
                        Box::new(BoundExpr::ColRef(val_col + 1)),
                        bop,
                        Box::new(BoundExpr::LitInt(0)),
                    ))
                } else {
                    // Direct aggregate: the value column's raw null bit is
                    // authoritative, exactly as the SELECT projection reads it.
                    Ok(fold_null_test(m.output_nullable, val_col, want_null))
                }
            }
            _ => {
                // A bare group-column reference.
                let col_name = single_relation_col_name(inner).ok_or_else(|| {
                    GnitzSqlError::Unsupported(
                        "HAVING: IS [NOT] NULL is only supported on an aggregate or a group column".to_string(),
                    )
                })?;
                let (src, reduce_col) = self.resolve_group_col(col_name)?;
                Ok(fold_null_test(
                    self.ctx.source_schema.columns[src].is_nullable,
                    reduce_col,
                    want_null,
                ))
            }
        }
    }
}
