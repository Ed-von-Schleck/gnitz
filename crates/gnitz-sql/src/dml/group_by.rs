//! The ad-hoc read path's aggregate / DISTINCT front end (consumed by
//! `dml::select`): `analyze_group_by` over the shared aggregate layout model
//! (`crate::agg`), the HAVING cluster (collection, binding, and the `Having` leaf
//! binder over the grouped relation), and `resolve_set_projection` (the bare-column
//! projection resolver the `SELECT DISTINCT` fold shares). The CREATE VIEW grouped
//! / DISTINCT bodies are compiled by the HIR pipeline instead.

use crate::agg::{
    agg_arg_col, append_agg_mapping, default_agg_name, finalize_agg_bexpr, finalize_agg_null_test,
    group_col_reduce_pos, AggMapping, AggSpec, GroupByLayout, GroupBySelectItem,
};
use crate::ast_util::{
    aliased_def, expand_wildcard_item, for_each_agg_call, group_by_exprs, is_bare_wildcard_projection,
    reject_computed_grouped_item, reject_ungrouped_column, scalar_projection_item, single_relation_col_name,
};
use crate::bind::{bind_single_table, bind_structural, find_unique_column, fold_null_test, LeafBinder, SingleTable};
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BoundExpr};
use crate::validate::{reject_float_key, reject_float_keys};
use gnitz_core::{ColumnDef, ReduceOutKey, Schema};
use sqlparser::ast::{Expr, SelectItem};

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
    let mut group_col_indices: Vec<usize> = Vec::new();
    for ge in group_by_exprs(select)? {
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
        let (expr, alias) = scalar_projection_item(item, "GROUP BY")?;

        let bound = bind_single_table(expr, source_schema)?;
        match &bound {
            BoundExpr::ColRef(col_idx) => {
                if !group_col_indices.contains(col_idx) {
                    return Err(reject_ungrouped_column(&source_schema.columns[*col_idx].name));
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
                let out_name = alias.unwrap_or_else(|| default_agg_name(*func, idx));
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
            _ => return Err(reject_computed_grouped_item()),
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

/// Collect the aggregate calls a HAVING expression references, appending any not
/// already present in `agg_mappings`. HAVING is evaluated over the grouped
/// relation, so every aggregate it names needs a spec and a reduce-output column
/// even when the SELECT list omits it. Walks through the shared
/// [`for_each_agg_call`], so this and the view path's collector cover exactly the
/// same node set.
fn collect_having_aggs(
    expr: &Expr,
    is_global: bool,
    source_schema: &Schema,
    agg_specs: &mut Vec<AggSpec>,
    agg_mappings: &mut Vec<AggMapping>,
) -> Result<(), GnitzSqlError> {
    for_each_agg_call(expr, &mut |func| -> Result<(), GnitzSqlError> {
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
        Ok(())
    })?;
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
        // The shared finalize rule over reduce-output column positions: the value
        // column at `specs_start`, its COUNT_NON_NULL companion (when the shape
        // carries one) at `specs_start + 1`.
        let ctx = self.ctx;
        let m = resolve_having_mapping(func, ctx)?;
        let val_col = ctx.agg_col_offset + m.specs_start;
        Ok(finalize_agg_bexpr(
            val_col,
            m.shape.has_count_companion().then_some(val_col + 1),
            m.agg_func,
        ))
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<BoundExpr, GnitzSqlError> {
        // IS [NOT] NULL over the grouped relation: an aggregate goes through the
        // shared companion-vs-value rule, a bare group column through the plain
        // nullability fold (which for a non-nullable column also keeps
        // EXPR_IS_NULL off the PK sentinel — see eval_is_null's assertion).
        match inner {
            Expr::Nested(i) => self.bind_null_test(i, want_null),
            Expr::Function(func) => {
                let m = resolve_having_mapping(func, self.ctx)?;
                let val_col = self.ctx.agg_col_offset + m.specs_start;
                Ok(finalize_agg_null_test(
                    val_col,
                    m.shape.has_count_companion().then_some(val_col + 1),
                    m.output_nullable,
                    want_null,
                ))
            }
            _ => {
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

/// Resolve a `SELECT DISTINCT` projection to source column indices plus output
/// column definitions. Supports `SELECT *`, bare column references, and aliased
/// column references; rejects computed expressions (which have no meaningful set
/// identity) with a clean error rather than silently dropping them.
pub(crate) fn resolve_set_projection(
    projection: &[SelectItem],
    source_schema: &Schema,
    context: &str,
) -> Result<(Vec<usize>, Vec<ColumnDef>), GnitzSqlError> {
    // Wildcard expands to *visible* columns only: an upstream synthetic key
    // (`_join_pk`, `_set_pk`, …) must not participate in DISTINCT row identity —
    // hashing it into the dedup key would keep otherwise-identical rows distinct.
    // Only a *bare* `*` takes this fast path; a `* EXCEPT/EXCLUDE/RENAME` (or a
    // rejected `* REPLACE/ILIKE`) falls into the single Wildcard arm below.
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
                for (i, out) in expand_wildcard_item(item, &source_schema.columns, context)? {
                    indices.push(i);
                    out_cols.push(out);
                }
            }
            _ => {
                let (expr, alias) = scalar_projection_item(item, context)?;
                let BoundExpr::ColRef(ci) = bind_single_table(expr, source_schema)? else {
                    return Err(GnitzSqlError::Unsupported(format!(
                        "{context}: computed expressions are not supported"
                    )));
                };
                indices.push(ci);
                out_cols.push(aliased_def(&source_schema.columns[ci], alias));
            }
        }
    }
    // Single chokepoint: every projected column lands in `indices`, so one pass
    // here rejects a float row-identity key regardless of which SELECT-item arm
    // produced it (a new arm is covered automatically).
    reject_float_keys(source_schema, &indices)?;
    Ok((indices, out_cols))
}
