//! AST → HIR binding. The one entry, `bind_query`, resolves every name to an
//! opaque `ColId`, validates the honored clauses, and produces a logical
//! `RelExpr` tree — single-table linear (`Simple`) and FROM-join (`Join`)
//! bodies. CTE/derived aliases are already in the binder cache (the old
//! `inline_ctes`/`compile_derived_tables` ran first in `build_query_segments`),
//! so a `Get` resolves them by name.

use super::{col_by_id, ColId, ColIdGen, HirAgg, HirCol, HirExpr, HirRef, ProjEntry, RelExpr, SetOpKind};
use crate::agg::{agg_typing, direct_agg_nullable, AggShape};
use crate::ast_util::{
    agg_func_from_name, body_is_grouped, extract_relation_name, extract_table_name_and_alias, flatten_conjuncts,
    is_wildcard_projection, projection_item_expr, reject_unsupported_fn_qualifiers, single_fn_name,
    single_relation_col_name, wildcard_name_is_visible, WildcardRewrite,
};
use crate::bind::{bind_structural, find_unique_column, fold_null_test, Binder, LeafBinder};
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BExpr, BinOp};
use crate::plan::validate::{
    reject_duplicate_column_names, reject_float_key, reject_unhonored_select_clauses, HonoredClauses,
};
use crate::plan::view::join::join_on_and_type;
use gnitz_core::{ColumnDef, GnitzClient, TypeCode};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Query, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, TableFactor,
};
use std::ops::Range;
use std::rc::Rc;

/// Bind a CREATE VIEW body to a logical `RelExpr` tree, returning the tree and
/// the `ColIdGen` positioned past every minted id — the lowering continues it to
/// mint fresh placeholder ids for a cut segment's hidden `_join_pk` slots.
pub(crate) fn bind_query(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    query: &Query,
) -> Result<(Rc<RelExpr>, ColIdGen), GnitzSqlError> {
    let mut ids = ColIdGen::new();
    // `query.with` is ignored: the old inline_ctes/compile_derived_tables already
    // ran in build_query_segments and populated the binder cache with any
    // CTE/derived aliases, so binder.resolve picks them up as `Get`s.
    let rel = bind_body(client, binder, &mut ids, query.body.as_ref())?;
    Ok((rel, ids))
}

/// Bind one query body — a single SELECT (linear / join / grouped / DISTINCT) or
/// a set operation whose sides bind recursively.
fn bind_body(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &mut ColIdGen,
    body: &SetExpr,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    match body {
        SetExpr::Select(select) => bind_select(client, binder, ids, select),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => bind_set_op(client, binder, ids, *op, *set_quantifier, left, right),
        SetExpr::Query(q) => bind_body(client, binder, ids, q.body.as_ref()),
        _ => Err(GnitzSqlError::Unsupported(
            "CREATE VIEW only supports SELECT and set operations".to_string(),
        )),
    }
}

/// Bind one single-SELECT body — a linear (`Simple`) body or a FROM-join
/// (`Join`) body. Classify has already gated grouped/set-op/subquery bodies to
/// the old path.
fn bind_select(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &mut ColIdGen,
    select: &Select,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    if select.from.len() != 1 {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW: only single FROM item supported".to_string(),
        ));
    }
    if select.from[0].joins.is_empty() {
        bind_linear_select(client, binder, ids, select)
    } else {
        bind_join_select(client, binder, ids, select)
    }
}

/// Bind a single-table linear body to `Project(Filter?(Get))`.
fn bind_linear_select(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &mut ColIdGen,
    select: &Select,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let grouped = body_is_grouped(select);
    let distinct = select.distinct.is_some();
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            // Honor grouping only on the grouped path; the DISTINCT path passes
            // `false` so a DISTINCT + GROUP BY body keeps the pinned "GROUP BY is
            // not supported" rejection.
            grouping: grouped && !distinct,
            distinct,
        },
        "CREATE VIEW",
    )?;

    let name = extract_relation_name(&select.from[0].relation, "CREATE VIEW")?;
    let (tid, schema) = binder.resolve(client, &name)?;
    let from_catalog = binder.is_catalog_relation(&name);
    let get = RelExpr::get(ids, tid, schema, from_catalog);
    // The one relation's cols — the leaf environment for name resolution.
    let env = get.cols();
    let leaf = HirSingleTable { env: &env };

    // WHERE — flatten to conjuncts, bind each through the HIR leaf.
    let mut rel = get;
    if let Some(where_expr) = &select.selection {
        let mut conjuncts = Vec::new();
        flatten_conjuncts(where_expr, &mut conjuncts);
        let preds = conjuncts
            .iter()
            .map(|c| bind_structural(c, &leaf))
            .collect::<Result<Vec<_>, _>>()?;
        rel = RelExpr::filter(rel, preds);
    }

    // DISTINCT outranks a grouped shape (matching the old classify order); a real
    // GROUP BY is already rejected by the gate above on this path.
    if distinct {
        // The dup-name guard fires in `lower_distinct` over the full output
        // column list (the synthetic `_distinct_pk` included) — a strict superset
        // of this projection, so checking it again here would be redundant.
        let items = bind_projection(&select.projection, &env, &leaf, ids)?;
        return Ok(RelExpr::distinct(RelExpr::project(rel, items)));
    }
    if grouped {
        return bind_grouped_suffix(ids, select, rel, &leaf);
    }

    // Projection — SELECT order (place_pk_front is physical, applied at lowering).
    let items = bind_projection(&select.projection, &env, &leaf, ids)?;
    // Dup-name check for a non-wildcard projection (matching lower_linear); a pure
    // `SELECT *` carries duplicates through positionally.
    reject_dup_proj_names(&items, &select.projection, "CREATE VIEW projection")?;
    Ok(RelExpr::project(rel, items))
}

/// Reject duplicate output names in a bound projection. A pure `SELECT *`
/// carries duplicates through positionally (the wildcard expansion is the
/// source's own column list), so only an explicit projection is checked — one
/// home for the guard the four bind sites otherwise repeat verbatim.
fn reject_dup_proj_names(items: &[ProjEntry], projection: &[SelectItem], ctx: &str) -> Result<(), GnitzSqlError> {
    if is_wildcard_projection(projection) {
        return Ok(());
    }
    let out_defs: Vec<ColumnDef> = items.iter().map(|e| e.out.def.clone()).collect();
    reject_duplicate_column_names(&out_defs, ctx)
}

/// Expand a bare `*` item over `cols` (honoring `EXCEPT`/`EXCLUDE`/`RENAME`,
/// skipping hidden columns) into pass-through `ProjEntry`s — the one wildcard
/// expansion, shared by the linear and join projections.
fn expand_wildcard(
    item: &SelectItem,
    cols: &[HirCol],
    ctx: &str,
    ids: &mut ColIdGen,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    let rw = WildcardRewrite::for_item(item, |n| wildcard_name_is_visible(cols.iter().map(|c| &c.def), n), ctx)?;
    let mut items = Vec::new();
    for c in cols {
        if c.def.is_hidden {
            continue;
        }
        let Some(out_def) = rw.rewrite_column(&c.def) else {
            continue;
        };
        items.push(ProjEntry {
            expr: BExpr::ColRef(HirRef::Col(c.id)),
            out: HirCol {
                id: ids.next(),
                def: out_def,
            },
        });
    }
    Ok(items)
}

/// Resolve every SELECT item into a `ProjEntry` in SELECT order, expanding a bare
/// `*` via [`expand_wildcard`]. This is `resolve_projection_items` over the HIR
/// leaf; the pass-through-vs-computed split, `_expr{idx}` naming,
/// hardcoded-`true` computed nullability, and `infer_type` typing are the same
/// rules `resolve_proj_col_with` fixes.
fn bind_projection(
    projection: &[SelectItem],
    env: &[HirCol],
    leaf: &HirSingleTable<'_>,
    ids: &mut ColIdGen,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    let mut items = Vec::new();
    for (idx, item) in projection.iter().enumerate() {
        match item {
            // Only a *bare* `*` expands (a `tbl.*` QualifiedWildcard is not a
            // single-table projection item — it falls to the `_` reject arm, as in
            // `resolve_projection_items`).
            SelectItem::Wildcard(_) => items.extend(expand_wildcard(item, env, "CREATE VIEW", ids)?),
            SelectItem::UnnamedExpr(expr) => items.push(bind_proj_expr(expr, None, idx, env, leaf, ids)?),
            SelectItem::ExprWithAlias { expr, alias } => {
                items.push(bind_proj_expr(expr, Some(alias.value.clone()), idx, env, leaf, ids)?)
            }
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "unsupported SELECT item in CREATE VIEW projection".to_string(),
                ))
            }
        }
    }
    Ok(items)
}

/// Bind one non-wildcard SELECT expression into a `ProjEntry`. A bare (possibly
/// aliased/qualified/parenthesized) column reference binds to a pass-through
/// carrying the source column's def (alias only renames); anything else is a
/// computed column, `_expr{idx}` when unaliased, typed by `infer_type`, and
/// **always nullable** (a fixed constant, never inferred — inferring from
/// operand nullability would diverge a downstream `IS NOT NULL` const-elision).
fn bind_proj_expr(
    expr: &Expr,
    alias: Option<String>,
    idx: usize,
    env: &[HirCol],
    leaf: &HirSingleTable<'_>,
    ids: &mut ColIdGen,
) -> Result<ProjEntry, GnitzSqlError> {
    let bound = bind_structural(expr, leaf)?;
    let out_def = if let BExpr::ColRef(HirRef::Col(id)) = &bound {
        let mut def = hircol_of(env, *id).def.clone();
        if let Some(name) = alias {
            def.name = name;
        }
        def
    } else {
        let ty = bound.infer_type_with(&|r: &HirRef| type_of(env, r));
        ColumnDef::new(alias.unwrap_or_else(|| format!("_expr{idx}")), ty, true)
    };
    Ok(ProjEntry {
        expr: bound,
        out: HirCol {
            id: ids.next(),
            def: out_def,
        },
    })
}

/// The env `HirCol` a `ColId` names. A HIR `ColRef` always references an env
/// column (bind mints them there), so absence is an internal compile error.
fn hircol_of(env: &[HirCol], id: ColId) -> &HirCol {
    col_by_id(env, id).expect("HIR ColRef references an env column")
}

/// The declared type of a leaf reference — the env column's type code.
fn type_of(env: &[HirCol], r: &HirRef) -> TypeCode {
    let HirRef::Col(id) = r;
    hircol_of(env, *id).def.type_code
}

/// The leaf for a single-relation body (linear WHERE + projection). Resolves a
/// column name against the relation's env; structurally identical to
/// `SingleTable`, differing only in the reference payload (`HirRef::Col(id)` vs a
/// `usize` position).
struct HirSingleTable<'a> {
    env: &'a [HirCol],
}

impl HirSingleTable<'_> {
    /// The env column of an `Identifier` / two-part `CompoundIdentifier`.
    fn resolve(&self, e: &Expr) -> Result<&HirCol, GnitzSqlError> {
        let name = single_relation_col_name(e)
            .ok_or_else(|| GnitzSqlError::Unsupported("expected a column reference".into()))?;
        let idx = find_unique_column(self.env.iter().map(|c| &c.def), name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found")))?;
        Ok(&self.env[idx])
    }
}

impl LeafBinder<HirRef> for HirSingleTable<'_> {
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        Ok(BExpr::ColRef(HirRef::Col(self.resolve(e)?.id)))
    }
    fn bind_function(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        // A Simple body has no aggregates (they route to GroupBy). Reject a
        // function with the same message the old path produces: qualifiers first,
        // then an aggregate (`SUM(x)` in a WHERE fails downstream at compile with
        // exactly this message), then any other name.
        reject_unsupported_fn_qualifiers(f, "aggregates")?;
        if single_fn_name(f).and_then(agg_func_from_name).is_some() {
            return Err(GnitzSqlError::Unsupported(
                "aggregate function not allowed in expression context".to_string(),
            ));
        }
        Err(GnitzSqlError::Unsupported(format!(
            "function '{}' not supported",
            f.name.to_string().to_ascii_lowercase()
        )))
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<HirExpr, GnitzSqlError> {
        let c = self.resolve(inner)?;
        Ok(fold_null_test(c.def.is_nullable, HirRef::Col(c.id), want_null))
    }
}

// ── FROM-join binding ──────────────────────────────────────────────────────────

/// Bind a FROM-join body to `Project(Filter?(Join(...)))`. The join list folds
/// left-deep in syntactic order (no reordering), so `a LEFT JOIN b JOIN c` is
/// `(a LEFT JOIN b) JOIN c`. ON conjuncts bind raw into `Join.on` (the predicate
/// rewrite classifies them); the WHERE binds to a `Filter` over the top join (the
/// rewrite folds INNER into the residual, keeps OUTER as a post-null-fill filter).
fn bind_join_select(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &mut ColIdGen,
    select: &Select,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let grouped = body_is_grouped(select);
    let distinct = select.distinct.is_some();
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: grouped && !distinct,
            distinct,
        },
        "CREATE VIEW JOIN",
    )?;
    let from = &select.from[0];

    // Leftmost relation.
    let (lname, lalias) = extract_table_name_and_alias(&from.relation, "CREATE VIEW JOIN")?;
    let (ltid, lschema) = binder.resolve(client, &lname)?;
    let lget = RelExpr::get(ids, ltid, lschema, binder.is_catalog_relation(&lname));
    let mut scope = JoinScope::new();
    scope.push(&lalias, lget.cols());
    let mut left = lget;

    // Each join step: bind the right relation, bind the ON conjuncts against the
    // accumulated scope, then fold into a `Join` node.
    for join in &from.joins {
        let (on_expr, kind) = join_on_and_type(join)?;
        let (rname, ralias) = extract_table_name_and_alias(&join.relation, "CREATE VIEW JOIN")?;
        let (rtid, rschema) = binder.resolve(client, &rname)?;
        let rget = RelExpr::get(ids, rtid, rschema, binder.is_catalog_relation(&rname));
        scope.push(&ralias, rget.cols());
        let mut on_ast = Vec::new();
        flatten_conjuncts(on_expr, &mut on_ast);
        let on = {
            let leaf = JoinLeaf { scope: &scope };
            on_ast
                .iter()
                .map(|c| bind_structural(c, &leaf))
                .collect::<Result<Vec<_>, _>>()?
        };
        left = RelExpr::join(left, rget, kind, on);
        // Reflect this step's null-widening back into the scope so a later ON /
        // the WHERE / the projection resolve against the widened nullability.
        scope.rewiden(left.cols());
    }

    // WHERE → a `Filter` over the top join (raw conjuncts; the rewrite places them).
    let mut rel = left;
    if let Some(where_expr) = &select.selection {
        let mut wc = Vec::new();
        flatten_conjuncts(where_expr, &mut wc);
        let preds = {
            let leaf = JoinLeaf { scope: &scope };
            wc.iter()
                .map(|c| bind_structural(c, &leaf))
                .collect::<Result<Vec<_>, _>>()?
        };
        rel = RelExpr::filter(rel, preds);
    }

    // DISTINCT / GROUP BY over a join: the operator sits above the join tree (the
    // lowering cuts the join to a hidden segment). DISTINCT outranks grouped.
    if distinct {
        // Guarded by `lower_distinct` over the full output columns (see above).
        let items = bind_join_projection(&select.projection, &scope, ids)?;
        return Ok(RelExpr::distinct(RelExpr::project(rel, items)));
    }
    if grouped {
        let leaf = JoinLeaf { scope: &scope };
        return bind_grouped_suffix(ids, select, rel, &leaf);
    }

    // Projection over the join output scope (column references + wildcard only,
    // matching `build_join_view_projection`).
    let items = bind_join_projection(&select.projection, &scope, ids)?;
    reject_dup_proj_names(&items, &select.projection, "join view")?;
    Ok(RelExpr::project(rel, items))
}

/// The name-resolution scope of a FROM-join body: all in-scope (null-widened)
/// `HirCol`s in relation order, plus each relation's lowercased alias and span.
/// Resolves a qualified/unqualified reference to a `ColId`, mirroring
/// `resolve_qualified_column` / `resolve_unqualified_column` (same messages).
struct JoinScope {
    combined: Vec<HirCol>,
    relations: Vec<(String, Range<usize>)>,
}

impl JoinScope {
    fn new() -> Self {
        JoinScope {
            combined: Vec::new(),
            relations: Vec::new(),
        }
    }

    fn push(&mut self, alias: &str, cols: Vec<HirCol>) {
        let start = self.combined.len();
        self.combined.extend(cols);
        self.relations
            .push((alias.to_ascii_lowercase(), start..self.combined.len()));
    }

    /// Adopt the join node's (null-widened) combined output as the scope columns;
    /// the per-relation spans are unchanged (widening never changes column count).
    fn rewiden(&mut self, combined: Vec<HirCol>) {
        debug_assert_eq!(combined.len(), self.combined.len());
        self.combined = combined;
    }

    fn rel_cols(&self, span: &Range<usize>) -> &[HirCol] {
        &self.combined[span.clone()]
    }

    fn resolve_qualified(&self, alias: &str, name: &str) -> Result<ColId, GnitzSqlError> {
        let (_, span) = self
            .relations
            .iter()
            .find(|(a, _)| *a == alias.to_ascii_lowercase())
            .ok_or_else(|| GnitzSqlError::Bind(format!("table alias '{alias}' not found")))?;
        let cols = self.rel_cols(span);
        let idx = find_unique_column(cols.iter().map(|c| &c.def), name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found in table '{alias}'")))?;
        Ok(cols[idx].id)
    }

    fn resolve_unqualified(&self, name: &str) -> Result<ColId, GnitzSqlError> {
        let mut found: Option<ColId> = None;
        for (_, span) in &self.relations {
            let cols = self.rel_cols(span);
            if let Some(idx) = find_unique_column(cols.iter().map(|c| &c.def), name)? {
                if found.is_some() {
                    return Err(GnitzSqlError::Bind(format!(
                        "ambiguous column '{name}' — qualify with table alias"
                    )));
                }
                found = Some(cols[idx].id);
            }
        }
        found.ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found in any table")))
    }

    fn is_nullable(&self, id: ColId) -> bool {
        col_by_id(&self.combined, id)
            .map(|c| c.def.is_nullable)
            .unwrap_or(false)
    }
}

/// The leaf for a FROM-join scope (ON + WHERE binding).
struct JoinLeaf<'a> {
    scope: &'a JoinScope,
}

impl JoinLeaf<'_> {
    /// A qualified / unqualified / parenthesized column reference → its `ColId`
    /// (mirrors `resolve_join_col_ref`, which unwraps `Nested`).
    fn col_id(&self, e: &Expr) -> Result<ColId, GnitzSqlError> {
        match e {
            Expr::Identifier(id) => self.scope.resolve_unqualified(&id.value),
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                self.scope.resolve_qualified(&parts[0].value, &parts[1].value)
            }
            Expr::Nested(inner) => self.col_id(inner),
            _ => Err(GnitzSqlError::Unsupported(
                "JOIN ON: only column references supported".into(),
            )),
        }
    }
}

impl LeafBinder<HirRef> for JoinLeaf<'_> {
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        Ok(BExpr::ColRef(HirRef::Col(self.col_id(e)?)))
    }
    fn bind_function(&self, _f: &Function) -> Result<HirExpr, GnitzSqlError> {
        Err(GnitzSqlError::Unsupported(
            "JOIN ON: aggregate functions are not allowed".into(),
        ))
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<HirExpr, GnitzSqlError> {
        let id = self.col_id(inner)?;
        Ok(fold_null_test(self.scope.is_nullable(id), HirRef::Col(id), want_null))
    }
}

/// Bind a join view's projection — column references + wildcard only (matching
/// `build_join_view_projection`); a computed expression is rejected. A bare
/// (possibly aliased) column resolves to a pass-through carrying the combined
/// output col's (null-widened) def.
fn bind_join_projection(
    projection: &[SelectItem],
    scope: &JoinScope,
    ids: &mut ColIdGen,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    let mut items = Vec::new();
    for item in projection {
        if matches!(item, SelectItem::Wildcard(_)) {
            items.extend(expand_wildcard(item, &scope.combined, "JOIN view", ids)?);
            continue;
        }
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "unsupported SELECT item in JOIN view".into(),
                ))
            }
        };
        let id = match expr {
            Expr::Identifier(ident) => scope.resolve_unqualified(&ident.value)?,
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                scope.resolve_qualified(&parts[0].value, &parts[1].value)?
            }
            _ if alias.is_some() => {
                return Err(GnitzSqlError::Unsupported(
                    "JOIN view: only column references supported in AS clause".into(),
                ))
            }
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "unsupported SELECT item in JOIN view".into(),
                ))
            }
        };
        let mut def = hircol_of(&scope.combined, id).def.clone();
        if let Some(alias) = alias {
            def.name = alias.value.clone();
        }
        items.push(ProjEntry {
            expr: BExpr::ColRef(HirRef::Col(id)),
            out: HirCol { id: ids.next(), def },
        });
    }
    Ok(items)
}

// ── GROUP BY / aggregate / HAVING binding ────────────────────────────────────────

/// One collected aggregate over the grouped input: the `HirAgg` that goes into
/// the `Reduce` node, plus the finalize (view) output type / nullability — bind
/// state the reduce itself has no use for, since they describe the *finalize*
/// projection's column, not the raw reduce one.
struct GroupAgg {
    agg: HirAgg,
    view_type: TypeCode,
    output_nullable: bool,
}

/// Peel `Nested` (parenthesis) wrappers off an expression.
fn peel_nested(e: &Expr) -> &Expr {
    let mut cur = e;
    while let Expr::Nested(inner) = cur {
        cur = inner;
    }
    cur
}

/// Extract the `ColId` of a bound bare column reference.
fn col_of(e: HirExpr) -> Result<ColId, GnitzSqlError> {
    match e {
        BExpr::ColRef(HirRef::Col(id)) => Ok(id),
        _ => Err(GnitzSqlError::Plan("internal: expected a column reference".into())),
    }
}

/// Classify an aggregate function call into `(func, arg)` — mirroring
/// `SingleTable::bind_function`'s argument-shape dispatch (COUNT(*) vs COUNT(x),
/// SUM/MIN/MAX/AVG single-arg). Orderability/type validation happens later via
/// `push_agg_specs`.
fn agg_call(f: &Function) -> Result<(AggFunc, Option<&Expr>), GnitzSqlError> {
    reject_unsupported_fn_qualifiers(f, "aggregates")?;
    let base = single_fn_name(f).and_then(agg_func_from_name).ok_or_else(|| {
        GnitzSqlError::Unsupported(format!(
            "function '{}' not supported",
            f.name.to_string().to_ascii_lowercase()
        ))
    })?;
    match base {
        AggFunc::Count => {
            if let FunctionArguments::List(list) = &f.args {
                if list.args.len() == 1 {
                    match &list.args[0] {
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => return Ok((AggFunc::Count, None)),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                            return Ok((AggFunc::CountNonNull, Some(inner)))
                        }
                        _ => {}
                    }
                }
            }
            Err(GnitzSqlError::Unsupported("COUNT: unsupported argument form".into()))
        }
        AggFunc::Sum | AggFunc::Min | AggFunc::Max | AggFunc::Avg => {
            if let FunctionArguments::List(list) = &f.args {
                if list.args.len() == 1 {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &list.args[0] {
                        return Ok((base, Some(inner)));
                    }
                }
            }
            Err(GnitzSqlError::Unsupported(format!(
                "{base:?}: requires exactly one column argument"
            )))
        }
        AggFunc::CountNonNull => unreachable!("agg_func_from_name never yields CountNonNull"),
    }
}

/// The AVG / nullable-SUM / Direct finalize composite over the raw reduce output.
fn finalize_agg_expr(ga: &GroupAgg) -> HirExpr {
    let out = BExpr::ColRef(HirRef::Col(ga.agg.out.id));
    match (&ga.agg.companion, ga.agg.func) {
        (Some(cnt), AggFunc::Avg) => {
            // AVG = (sum * 1.0) / cnt — forces float division; div-by-zero → NULL.
            let cnt = BExpr::ColRef(HirRef::Col(cnt.id));
            BExpr::BinOp(
                Box::new(BExpr::BinOp(Box::new(out), BinOp::Mul, Box::new(BExpr::LitFloat(1.0)))),
                BinOp::Div,
                Box::new(cnt),
            )
        }
        (Some(cnt), _) => {
            // Nullable SUM = sum / (cnt != 0) — an exact identity divisor while the
            // non-null count is positive; div-by-zero → NULL when it hits zero.
            let cnt = BExpr::ColRef(HirRef::Col(cnt.id));
            BExpr::BinOp(
                Box::new(out),
                BinOp::Div,
                Box::new(BExpr::BinOp(Box::new(cnt), BinOp::Ne, Box::new(BExpr::LitInt(0)))),
            )
        }
        (None, _) => out,
    }
}

/// Resolve GROUP BY columns to `ColId`s (bare/qualified refs only). A computed
/// group column gives a GROUP-BY-context rejection (not the leaf's JOIN-ON message).
fn resolve_group_cols<L: LeafBinder<HirRef>>(
    select: &Select,
    env: &[HirCol],
    leaf: &L,
) -> Result<Vec<ColId>, GnitzSqlError> {
    let group_exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs,
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "GROUP BY: only expression list supported".into(),
            ))
        }
    };
    let mut cols = Vec::new();
    for ge in group_exprs {
        if !matches!(peel_nested(ge), Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            return Err(GnitzSqlError::Unsupported(
                "GROUP BY: only simple column references supported".into(),
            ));
        }
        let id = col_of(leaf.bind_column(ge)?)?;
        let hc = col_by_id(env, id).expect("group col in env");
        reject_float_key(&hc.def, "GROUP BY")?;
        cols.push(id);
    }
    Ok(cols)
}

/// Collect the aggregate calls referenced in an expression, appending each not
/// already present (deduped by `(func, arg)`). Recurses through non-aggregate
/// operands, mirroring the binder's node set. Returns the `aggs` index of the
/// aggregate the expression's own top level resolved to (`None` for anything
/// else — a bare column ref, a buried aggregate, or no aggregate at all): the
/// finalize projection consults this instead of re-parsing/re-binding a
/// top-level aggregate call a second time.
fn collect_aggs<L: LeafBinder<HirRef>>(
    expr: &Expr,
    env: &[HirCol],
    leaf: &L,
    is_global: bool,
    aggs: &mut Vec<GroupAgg>,
    ids: &mut ColIdGen,
) -> Result<Option<usize>, GnitzSqlError> {
    let peeled = peel_nested(expr);
    if let Expr::Function(f) = peeled {
        if single_fn_name(f).and_then(agg_func_from_name).is_some() {
            let (func, arg_expr) = agg_call(f)?;
            let arg = match arg_expr {
                Some(e) => Some(col_of(leaf.bind_column(e)?)?),
                None => None,
            };
            // MIN/MAX orderability — checked here (Unsupported) so the message and
            // error variant match the old leaf binder, ahead of `push_agg_specs`'s
            // `Bind` backstop.
            if matches!(func, AggFunc::Min | AggFunc::Max) {
                if let Some(id) = arg {
                    let ty = col_by_id(env, id).expect("agg arg in env").def.type_code;
                    if !crate::types::is_min_max_orderable(ty) {
                        return Err(GnitzSqlError::Unsupported(format!(
                            "{}: not supported on {ty:?} columns",
                            if func == AggFunc::Min { "MIN" } else { "MAX" }
                        )));
                    }
                }
            }
            if let Some(idx) = aggs.iter().position(|a| a.agg.func == func && a.agg.arg == arg) {
                return Ok(Some(idx));
            }
            let arg_def = arg.map(|id| &col_by_id(env, id).expect("agg arg ColId in env").def);
            let typing = agg_typing(func, arg_def)?;
            let output_nullable = match typing.shape {
                AggShape::Avg | AggShape::NullfillSum => true,
                AggShape::Direct => {
                    direct_agg_nullable(func, arg_def.map(|d| d.is_nullable).unwrap_or(false), is_global)
                }
            };
            aggs.push(GroupAgg {
                agg: HirAgg::new(ids, func, arg, &typing),
                view_type: typing.view_type,
                output_nullable,
            });
            return Ok(Some(aggs.len() - 1));
        }
    }
    for op in crate::ast_util::expr_operands(peeled) {
        let _ = collect_aggs(op, env, leaf, is_global, aggs, ids)?;
    }
    Ok(None)
}

/// Bind the GROUP BY / aggregate / HAVING suffix over `input` (the `Filter?(source)`
/// tree built by the FROM binder), producing `Project(Filter_having?(Reduce(input)))`.
fn bind_grouped_suffix<L: LeafBinder<HirRef>>(
    ids: &mut ColIdGen,
    select: &Select,
    input: Rc<RelExpr>,
    leaf: &L,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let env = input.cols();

    let group_cols = resolve_group_cols(select, &env, leaf)?;
    let is_global = group_cols.is_empty();

    // Aggregates from the projection ∪ HAVING (deduped). `item_agg[i]` is the
    // `aggs` index a top-level projection item resolved to, when it is itself an
    // aggregate call — reused by `bind_finalize_projection` so it doesn't re-parse
    // and re-bind the same top-level call a second time.
    let mut aggs: Vec<GroupAgg> = Vec::new();
    let mut item_agg: Vec<Option<usize>> = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let idx = match projection_item_expr(item) {
            Some(expr) => collect_aggs(expr, &env, leaf, is_global, &mut aggs, ids)?,
            None => None,
        };
        item_agg.push(idx);
    }
    if let Some(having) = &select.having {
        let _ = collect_aggs(having, &env, leaf, is_global, &mut aggs, ids)?;
    }

    let hir_aggs: Vec<HirAgg> = aggs.iter().map(|a| a.agg.clone()).collect();
    let reduce = RelExpr::reduce(input, group_cols.clone(), hir_aggs);

    // HAVING → a Filter over the raw reduce output.
    let mut rel = reduce;
    if let Some(having) = &select.having {
        let grouped_leaf = GroupedLeaf {
            leaf,
            env: &env,
            group_cols: &group_cols,
            aggs: &aggs,
        };
        let hexpr = bind_structural(having, &grouped_leaf)?;
        rel = RelExpr::filter(rel, vec![hexpr]);
    }

    // Finalize projection (strict grouped-projection validator).
    // The dup-name guard fires in `lower_reduce` over the full output column list
    // (the reduce PK region included), a strict superset of this projection.
    let items = bind_finalize_projection(select, &env, &group_cols, &aggs, &item_agg, leaf, ids)?;
    Ok(RelExpr::project(rel, items))
}

/// The finalize (SELECT) projection over the raw reduce output: every item is a
/// bare group-col reference or an aggregate call (the strict validator).
/// `item_agg[i]` is `bind_grouped_suffix`'s pre-resolved `aggs` index for a
/// top-level aggregate item — the aggregate call itself is never re-parsed or
/// re-bound here.
fn bind_finalize_projection<L: LeafBinder<HirRef>>(
    select: &Select,
    env: &[HirCol],
    group_cols: &[ColId],
    aggs: &[GroupAgg],
    item_agg: &[Option<usize>],
    leaf: &L,
    ids: &mut ColIdGen,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    let mut items = Vec::new();
    for (idx, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => return Err(GnitzSqlError::Unsupported("GROUP BY: unsupported SELECT item".into())),
        };
        if let Some(agg_idx) = item_agg[idx] {
            let ga = &aggs[agg_idx];
            let prefix = match ga.agg.func {
                AggFunc::Count | AggFunc::CountNonNull => "_count",
                AggFunc::Sum => "_sum",
                AggFunc::Min => "_min",
                AggFunc::Max => "_max",
                AggFunc::Avg => "_avg",
            };
            let name = alias.unwrap_or_else(|| format!("{prefix}{idx}"));
            items.push(ProjEntry {
                expr: finalize_agg_expr(ga),
                out: HirCol {
                    id: ids.next(),
                    def: ColumnDef::new(name, ga.view_type, ga.output_nullable),
                },
            });
            continue;
        }
        let peeled = peel_nested(expr);
        if matches!(peeled, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            let id = col_of(leaf.bind_column(peeled)?)?;
            if !group_cols.contains(&id) {
                let name = col_by_id(env, id).map(|c| c.def.name.clone()).unwrap_or_default();
                return Err(GnitzSqlError::Plan(format!(
                    "column '{name}' must appear in GROUP BY or an aggregate function"
                )));
            }
            let mut def = col_by_id(env, id).expect("group col in env").def.clone();
            if let Some(a) = alias {
                def.name = a;
            }
            items.push(ProjEntry {
                expr: BExpr::ColRef(HirRef::Col(id)),
                out: HirCol { id: ids.next(), def },
            });
            continue;
        }
        return Err(GnitzSqlError::Plan(
            "GROUP BY SELECT: only column refs and aggregates supported".into(),
        ));
    }
    Ok(items)
}

/// The leaf for HAVING over the grouped relation: group columns resolve to their
/// source `ColId` (enforcing GROUP BY membership), aggregate calls to their
/// finalize composite over the raw reduce output. Resolution routes through the
/// original FROM leaf so `ColId`s match the collected aggregates and group cols.
struct GroupedLeaf<'a, L: LeafBinder<HirRef>> {
    leaf: &'a L,
    env: &'a [HirCol],
    group_cols: &'a [ColId],
    aggs: &'a [GroupAgg],
}

impl<L: LeafBinder<HirRef>> GroupedLeaf<'_, L> {
    fn resolve_group(&self, e: &Expr) -> Result<(ColId, bool), GnitzSqlError> {
        // HAVING references the grouped relation by source name (matching the old
        // `Having` binder's messages), enforcing GROUP BY membership.
        let name = single_relation_col_name(e)
            .ok_or_else(|| GnitzSqlError::Unsupported(format!("HAVING: unsupported column reference {e:?}")))?;
        let idx = find_unique_column(self.env.iter().map(|c| &c.def), name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("HAVING: column '{name}' not found")))?;
        let hc = &self.env[idx];
        if !self.group_cols.contains(&hc.id) {
            return Err(GnitzSqlError::Bind(format!(
                "HAVING: column '{name}' must appear in GROUP BY or an aggregate function"
            )));
        }
        Ok((hc.id, hc.def.is_nullable))
    }

    fn find_agg(&self, f: &Function) -> Result<&GroupAgg, GnitzSqlError> {
        let (func, arg_expr) = agg_call(f)?;
        let arg = match arg_expr {
            Some(e) => Some(col_of(self.leaf.bind_column(e)?)?),
            None => None,
        };
        self.aggs
            .iter()
            .find(|a| a.agg.func == func && a.agg.arg == arg)
            .ok_or_else(|| GnitzSqlError::Bind(format!("HAVING: aggregate {func:?} could not be resolved")))
    }
}

impl<L: LeafBinder<HirRef>> LeafBinder<HirRef> for GroupedLeaf<'_, L> {
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        let (id, _) = self.resolve_group(e)?;
        Ok(BExpr::ColRef(HirRef::Col(id)))
    }
    fn bind_function(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        if single_fn_name(f).and_then(agg_func_from_name).is_some() {
            return Ok(finalize_agg_expr(self.find_agg(f)?));
        }
        self.leaf.bind_function(f)
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<HirExpr, GnitzSqlError> {
        let peeled = peel_nested(inner);
        if let Expr::Function(f) = peeled {
            if single_fn_name(f).and_then(agg_func_from_name).is_some() {
                let ga = self.find_agg(f)?;
                if let Some(cnt) = &ga.agg.companion {
                    // Nullable SUM / AVG: NULL ⇔ COUNT_NON_NULL companion is 0.
                    let bop = if want_null { BinOp::Eq } else { BinOp::Ne };
                    return Ok(BExpr::BinOp(
                        Box::new(BExpr::ColRef(HirRef::Col(cnt.id))),
                        bop,
                        Box::new(BExpr::LitInt(0)),
                    ));
                }
                return Ok(fold_null_test(
                    ga.output_nullable,
                    HirRef::Col(ga.agg.out.id),
                    want_null,
                ));
            }
        }
        // A bare group-column reference; anything else (an arithmetic expression)
        // gets the old aggregate-or-group-column rejection.
        if single_relation_col_name(peeled).is_none() {
            return Err(GnitzSqlError::Unsupported(
                "HAVING: IS [NOT] NULL is only supported on an aggregate or a group column".into(),
            ));
        }
        let (id, nullable) = self.resolve_group(peeled)?;
        Ok(fold_null_test(nullable, HirRef::Col(id), want_null))
    }
}

// ── Set-operation binding ────────────────────────────────────────────────────────

/// Bind a set operation, binding both sides recursively; the `SetOp` constructor
/// pairs columns positionally and promotes cross-width types.
fn bind_set_op(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &mut ColIdGen,
    op: SetOperator,
    quantifier: SetQuantifier,
    left: &SetExpr,
    right: &SetExpr,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    match quantifier {
        SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName => {
            return Err(GnitzSqlError::Unsupported(
                "BY NAME set operations are not supported".into(),
            ))
        }
        _ => {}
    }
    let kind = match op {
        SetOperator::Union => SetOpKind::Union,
        SetOperator::Intersect => SetOpKind::Intersect,
        SetOperator::Except => SetOpKind::Except,
        _ => {
            return Err(GnitzSqlError::Unsupported(format!(
                "set operation {op:?} not supported"
            )))
        }
    };
    let all = matches!(quantifier, SetQuantifier::All);
    let left_rel = bind_set_side(client, binder, ids, left)?;
    let right_rel = bind_set_side(client, binder, ids, right)?;
    RelExpr::set_op(ids, kind, all, left_rel, right_rel)
}

/// Bind one set-operation side. A derived-table side is deferred (box 8) — rejected
/// here with a clean message rather than the drifted resolution error.
fn bind_set_side(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &mut ColIdGen,
    side: &SetExpr,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    if let SetExpr::Select(s) = side {
        for item in &s.from {
            for tf in std::iter::once(&item.relation).chain(item.joins.iter().map(|j| &j.relation)) {
                if matches!(tf, TableFactor::Derived { .. }) {
                    return Err(GnitzSqlError::Unsupported(
                        "set operation: a derived table set-op side is not supported".into(),
                    ));
                }
            }
        }
    }
    bind_body(client, binder, ids, side)
}
