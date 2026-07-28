//! AST → HIR binding. `bind_body` resolves every name to an opaque `ColId`,
//! validates the honored clauses, and produces a logical `RelExpr` tree for any
//! relational body (linear / join / grouped / DISTINCT / set operation). The CTE
//! phase (`bind_ctes`) compiles each CTE body to a hidden segment ahead of the
//! main bind and registers its alias, so a `Get` resolves a CTE by name; a
//! derived table binds its subquery recursively to an inline subtree
//! (`resolve_table_factor`), never a segment.

use super::{
    as_col, bind_and_lower, col_by_id, widen_if, ColId, ColIdGen, HirAgg, HirCol, HirExpr, HirRef, InPair, JoinType,
    ProjEntry, RelExpr, SetOpKind, SubqueryKind, SubqueryRef,
};
use crate::agg::{agg_output_nullable, agg_typing, default_agg_name, finalize_agg_bexpr, reject_min_max_unorderable};
use crate::ast_util::{
    body_is_grouped, classify_agg_call, classify_from, expand_wildcard_item, extract_table_name_and_alias,
    flatten_conjuncts, for_each_agg_call, group_by_exprs, has_exists_in_subquery, has_scalar_subquery, is_agg_call,
    is_wildcard_projection, peel_nested, projection_item_expr, reject_computed_grouped_item, reject_ungrouped_column,
    reject_unsupported_fn_qualifiers, single_relation_col_name, FromShape,
};
use crate::bind::{apply_positional_aliases, cte_passthrough};
use crate::bind::{bind_structural, find_unique_column, fold_null_test, single_relation_col_idx, Binder, LeafBinder};
use crate::error::GnitzSqlError;
use crate::hir::chain::ViewChain;
use crate::hir::guards::join_on_and_type;
use crate::ir::{AggFunc, BExpr, BinOp};
use crate::validate::{
    cte_body, non_recursive_ctes, plain_select_body, reject_duplicate_names, reject_float_key,
    reject_query_envelope_body, reject_unhonored_select_clauses, validate_user_name, HonoredClauses,
};
use gnitz_core::{ColumnDef, GnitzClient, TypeCode};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, TableFactor,
};
use std::cell::RefCell;
use std::collections::HashSet;
use std::ops::Range;
use std::rc::Rc;

/// Compile a query's CTEs ahead of the main body bind. A plain pass-through CTE
/// (`SELECT * FROM t`, no WHERE/GROUP/DISTINCT) aliases to the source's real
/// catalog id — preserving its downstream index-bound eligibility; every other
/// CTE body compiles to a hidden segment via the shared HIR pipeline
/// (`bind_and_lower`), registered under the CTE name so the body (and later CTEs)
/// resolve it by name. Scoping precedence as SQL defines it: a CTE shadows a
/// catalog name, and a later CTE sees earlier ones (registration is immediate).
pub(crate) fn bind_ctes(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    query: &Query,
) -> Result<(), GnitzSqlError> {
    for cte in non_recursive_ctes(query)? {
        let name = cte.alias.name.value.clone();
        let ctx = format!("CTE '{name}'");
        let body = cte_body(cte, &ctx)?;
        // Pass-through fast path: an identity `SELECT * FROM t` body with no
        // WHERE / GROUP BY / DISTINCT aliases straight to its source table. A
        // WHERE'd / grouped / DISTINCT body compiles instead (it carries real
        // work), and the exotic tail (TOP, PREWHERE, …) is rejected here.
        if let SetExpr::Select(s) = body {
            if s.selection.is_none() && !body_is_grouped(s) && s.distinct.is_none() {
                reject_unhonored_select_clauses(s, HonoredClauses::PLAIN, &ctx)?;
                if let Some(resolved) = cte_passthrough(client, s, &cte.alias.columns, binder)? {
                    binder.cache_alias(&name, resolved, true)?;
                    continue;
                }
            }
        }
        // Compiled path: a hidden HIR segment, with the CTE's positional column
        // aliases applied to the emitted visible columns.
        let (seg_vid, seg_schema, ()) = chain.add_segment(client, |client, chain, vid| {
            let (circuit, mut cols, pk) = bind_and_lower(client, binder, chain, body, vid)?;
            apply_positional_aliases(&cte.alias.columns, cols.iter_mut().collect(), &ctx)?;
            Ok(((circuit, cols, pk), ()))
        })?;
        let resolved = (seg_vid, seg_schema);
        // A chain-minted segment id, not a catalog one: no index bound.
        binder.cache_alias(&name, resolved, false)?;
    }
    Ok(())
}

/// Bind one query body — a single SELECT (linear / join / grouped / DISTINCT) or
/// a set operation whose sides bind recursively.
pub(crate) fn bind_body(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &ColIdGen,
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

/// Flatten `expr` into its top-level AND conjuncts and bind each through `leaf` —
/// the one home for "a WHERE / ON clause → its bound conjunct list", shared by the
/// linear, join, grouped, and subquery binds.
fn bind_conjuncts<L: LeafBinder<HirRef>>(expr: &Expr, leaf: &L) -> Result<Vec<HirExpr>, GnitzSqlError> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts);
    conjuncts.iter().map(|c| bind_structural(c, leaf)).collect()
}

/// Resolve one FROM table factor to `(source subtree, alias, output cols)`. A
/// table/CTE name resolves via the binder to a base or segment `Get`; a derived
/// table binds its subquery recursively to an **inline** subtree (never a
/// segment — single-use and non-LATERAL, so uncorrelated) whose `AS d(col…)`
/// aliases are applied to the returned cols (same `ColId`s, overridden names) the
/// caller pushes into its scope/env. The subtree itself keeps its own names; a
/// derived alias resolves through the caller's scope, never the binder cache.
fn resolve_table_factor(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &ColIdGen,
    factor: &TableFactor,
) -> Result<(Rc<RelExpr>, String, Vec<HirCol>), GnitzSqlError> {
    if let TableFactor::Derived {
        lateral,
        subquery,
        alias,
        sample,
    } = factor
    {
        let Some(alias) = alias else {
            return Err(GnitzSqlError::Unsupported(
                "a derived table (subquery in FROM) needs an alias".to_string(),
            ));
        };
        // The alias is a user-visible name, so it is held to the general
        // user-identifier rule (a leading `_` is reserved for the `__h…`
        // hidden-view namespace) even though an inline derived subtree never
        // enters the binder's alias cache.
        validate_user_name(&alias.name.value)?;
        let ctx = format!("derived table '{}'", alias.name.value);
        if *lateral {
            return Err(GnitzSqlError::Unsupported(format!("{ctx}: LATERAL is not supported")));
        }
        // Silently dropping TABLESAMPLE would return all rows — a wrong result.
        if sample.is_some() {
            return Err(GnitzSqlError::Unsupported(format!(
                "{ctx}: TABLESAMPLE is not supported"
            )));
        }
        let body = reject_query_envelope_body(subquery, &ctx)?;
        let subtree = bind_body(client, binder, ids, body)?;
        let mut cols = subtree.cols();
        apply_positional_aliases(&alias.columns, cols.iter_mut().map(|c| &mut c.def).collect(), &ctx)?;
        return Ok((subtree, alias.name.value.clone(), cols));
    }
    let (name, alias) = extract_table_name_and_alias(factor, "CREATE VIEW")?;
    let (tid, schema) = binder.resolve(client, &name)?;
    let from_catalog = binder.is_catalog_relation(&name);
    let get = RelExpr::get(ids, tid, schema, from_catalog);
    let cols = get.cols();
    Ok((get, alias, cols))
}

/// Bind one single-SELECT body — a linear body or a FROM-join body.
fn bind_select(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &ColIdGen,
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
    ids: &ColIdGen,
    select: &Select,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let grouped = body_is_grouped(select);
    let distinct = select.distinct.is_some();
    reject_unhonored_select_clauses(select, HonoredClauses::for_body(grouped, distinct), "CREATE VIEW")?;

    // The sole FROM relation (a table, a CTE, or a derived table) → its source
    // subtree, alias, and the cols that form the leaf environment for name
    // resolution.
    let (source, outer_alias, env) = resolve_table_factor(client, binder, ids, &select.from[0].relation)?;

    // A subquery-carrying single-table body (EXISTS/IN, scalar aggregate, ANY/ALL)
    // routes to the subquery-aware leaf, which binds each subquery as a
    // `HirRef::Subquery` leaf for decorrelation. GROUP BY / DISTINCT cannot host a
    // subquery in one circuit: EXISTS/IN + GROUP BY gets the targeted message here,
    // and every other combination falls through to the plain leaf's per-kind
    // default `bind_subquery` rejection below.
    let has_exists_in = has_exists_in_subquery(select);
    if has_exists_in || has_scalar_subquery(select) {
        if grouped && has_exists_in {
            return Err(GnitzSqlError::Unsupported(
                "EXISTS/IN subqueries are not supported together with GROUP BY/aggregates; \
                 put the subquery in an inner view"
                    .into(),
            ));
        }
        if !grouped && !distinct {
            return bind_linear_subquery_body(client, binder, ids, select, source, env, &outer_alias);
        }
    }

    let leaf = HirSingleTable { env: &env };

    // WHERE — flatten to conjuncts, bind each through the HIR leaf.
    let mut rel = source;
    if let Some(where_expr) = &select.selection {
        rel = RelExpr::filter(rel, bind_conjuncts(where_expr, &leaf)?);
    }

    // DISTINCT outranks a grouped shape (matching the old classify order); a real
    // GROUP BY is already rejected by the gate above on this path.
    if distinct {
        // The dup-name guard fires in `lower_distinct` over the full output
        // column list (the synthetic `_distinct_pk` included) — a strict superset
        // of this projection, so checking it again here would be redundant.
        let items = bind_projection(&select.projection, &env, &leaf, ids, "CREATE VIEW projection")?;
        return Ok(RelExpr::distinct(RelExpr::project(rel, items)));
    }
    if grouped {
        return bind_grouped_suffix(ids, select, rel, &leaf);
    }

    // Projection — SELECT order (place_pk_front is physical, applied at lowering).
    let items = bind_projection(&select.projection, &env, &leaf, ids, "CREATE VIEW projection")?;
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
    // Hidden slots are skipped, exactly as `reject_duplicate_column_names` does:
    // they are excluded from name resolution, so they cannot bind ambiguously.
    reject_duplicate_names(
        items
            .iter()
            .filter(|e| !e.out.def.is_hidden)
            .map(|e| e.out.def.name.as_str()),
        ctx,
    )
}

/// Expand a bare `*` item over `cols` (honoring `EXCEPT`/`EXCLUDE`/`RENAME`,
/// skipping hidden columns) into pass-through `ProjEntry`s — the one wildcard
/// expansion, shared by the linear and join projections.
fn expand_wildcard(
    item: &SelectItem,
    cols: &[HirCol],
    ctx: &str,
    ids: &ColIdGen,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    Ok(expand_wildcard_item(item, cols.iter().map(|c| &c.def), ctx)?
        .into_iter()
        .map(|(i, def)| ProjEntry {
            expr: BExpr::ColRef(HirRef::Col(cols[i].id)),
            out: HirCol { id: ids.next(), def },
        })
        .collect())
}

/// Resolve every SELECT item into a `ProjEntry` in SELECT order, expanding a bare
/// `*` via [`expand_wildcard`]. This is `resolve_projection_items` over the HIR
/// leaf; the pass-through-vs-computed split, `_expr{idx}` naming,
/// hardcoded-`true` computed nullability, and `infer_type` typing are the same
/// rules `resolve_proj_col` fixes.
fn bind_projection<L: LeafBinder<HirRef>>(
    projection: &[SelectItem],
    env: &[HirCol],
    leaf: &L,
    ids: &ColIdGen,
    ctx: &str,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    let mut items = Vec::new();
    for (idx, item) in projection.iter().enumerate() {
        match item {
            // Only a *bare* `*` expands (a `tbl.*` QualifiedWildcard is not a
            // single-table projection item — it falls to the `_` reject arm, as in
            // `resolve_projection_items`).
            SelectItem::Wildcard(_) => items.extend(expand_wildcard(item, env, ctx, ids)?),
            SelectItem::UnnamedExpr(expr) => items.push(bind_proj_expr(expr, None, idx, env, leaf, ids)?),
            SelectItem::ExprWithAlias { expr, alias } => {
                items.push(bind_proj_expr(expr, Some(alias.value.clone()), idx, env, leaf, ids)?)
            }
            _ => return Err(GnitzSqlError::Unsupported(format!("unsupported SELECT item in {ctx}"))),
        }
    }
    Ok(items)
}

/// Bind one non-wildcard SELECT expression into a `ProjEntry`. A bare (possibly
/// aliased/qualified/parenthesized) column reference binds to a pass-through
/// carrying the source column's def (alias only renames); anything else is a
/// computed column, built by `ColumnDef::computed` — the same naming, typing and
/// always-nullable rules `resolve_proj_col` applies on the ad-hoc path. (The
/// nullability is a fixed constant, never inferred: inferring it from operand
/// nullability would diverge a downstream `IS NOT NULL` const-elision.)
fn bind_proj_expr<L: LeafBinder<HirRef>>(
    expr: &Expr,
    alias: Option<String>,
    idx: usize,
    env: &[HirCol],
    leaf: &L,
    ids: &ColIdGen,
) -> Result<ProjEntry, GnitzSqlError> {
    let bound = bind_structural(expr, leaf)?;
    let out_def = if let BExpr::ColRef(HirRef::Col(id)) = &bound {
        let mut def = hircol_of(env, *id).def.clone();
        if let Some(name) = alias {
            def.name = name;
        }
        def
    } else {
        ColumnDef::computed(alias, idx, bound.infer_type_with(&|r: &HirRef| type_of(env, r)))
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

/// The declared type of a leaf reference — the env column's type code, or the
/// bound subquery's contributed value type (a computed projection may embed a
/// subquery leaf before decorrelation substitutes it).
fn type_of(env: &[HirCol], r: &HirRef) -> TypeCode {
    match r {
        HirRef::Col(id) => hircol_of(env, *id).def.type_code,
        HirRef::Subquery(sq) => sq.value_type(),
    }
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
        Ok(&self.env[single_relation_col_idx(self.env.iter().map(|c| &c.def), e)?])
    }
}

impl LeafBinder<HirRef> for HirSingleTable<'_> {
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        Ok(BExpr::ColRef(HirRef::Col(self.resolve(e)?.id)))
    }
    fn bind_function(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        // A Simple body has no aggregates (they route to GroupBy). Reject a
        // function with the standard message: qualifiers first,
        // then an aggregate (`SUM(x)` in a WHERE fails downstream at compile with
        // exactly this message), then any other name.
        reject_unsupported_fn_qualifiers(f, "aggregates")?;
        if is_agg_call(f) {
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

// ── Subquery binding (EXISTS/IN, scalar aggregate, ANY/ALL) ──────────────────────
//
// A subquery-carrying single-table linear body binds in ONE pass over the real
// `bind_structural`, so every desugar/fold the structural recursion performs —
// COALESCE truncation at a never-NULL COUNT, provably-non-null elision, CASE
// short-circuit — is reached for free. `SubqueryLeaf` binds each subquery node to a
// `HirRef::Subquery` leaf where the walk meets it, reaching `&mut client, &mut
// binder` through a `RefCell` (the `LeafBinder` methods take `&self`). Decorrelation
// (`hir::rewrite`) consumes the leaves.

/// Whether an expression node is itself a subquery of any kind (an opaque leaf to
/// the structural walk) — the `SubqueryLeaf` uses it to route an `IS [NOT] NULL`
/// over a subquery operand to the subquery bind rather than to a column resolve.
fn is_subquery_expr(e: &Expr) -> bool {
    matches!(
        e,
        Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::Subquery(_) | Expr::AnyOp { .. } | Expr::AllOp { .. }
    )
}

/// Whether a bound subquery's value is provably never NULL — an EXISTS/IN test
/// (`0/1`) or a COUNT. Derived from the leaf rather than carried alongside it, so
/// [`SubqueryRef::never_null`] stays the one home of the rule: only a bare
/// subquery leaf can be never-NULL, since every composite shape
/// (`bind_quantifier_sub`'s 3VL wrapper) is a comparison over a nullable operand.
fn value_never_null(value: &HirExpr) -> bool {
    matches!(value, BExpr::ColRef(HirRef::Subquery(s)) if s.never_null())
}

/// Everything a subquery bind resolves against: the two mutable compilation
/// handles plus the outer scope it correlates to. Behind one `RefCell` on the leaf,
/// so `LeafBinder`'s `&self` methods can reach it; borrowed only for the duration of
/// a single `bind_one_subquery` call, which never re-enters this leaf (a nested
/// subquery is rejected by the inner correlation leaf).
struct SubCtx<'a, 'b, 'c> {
    client: &'a mut GnitzClient,
    binder: &'a mut Binder<'b>,
    ids: &'c ColIdGen,
    outer_env: &'c [HirCol],
    outer_alias: &'c str,
}

/// The outer single-table leaf extended with subquery handling: it binds an
/// EXISTS/IN/scalar/ANY/ALL node — and a subquery operand of `IS [NOT] NULL` — in
/// place, delegating every other decision to the inner `HirSingleTable`.
struct SubqueryLeaf<'a, 'b, 'c> {
    inner: HirSingleTable<'c>,
    ctx: RefCell<SubCtx<'a, 'b, 'c>>,
}

impl SubqueryLeaf<'_, '_, '_> {
    fn bind_sub(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        bind_one_subquery(&mut self.ctx.borrow_mut(), e)
    }
}

impl LeafBinder<HirRef> for SubqueryLeaf<'_, '_, '_> {
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        self.inner.bind_column(e)
    }
    fn bind_function(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        self.inner.bind_function(f)
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<HirExpr, GnitzSqlError> {
        let peeled = peel_nested(inner);
        if !is_subquery_expr(peeled) {
            return self.inner.bind_null_test(inner, want_null);
        }
        let value = self.bind_sub(peeled)?;
        if value_never_null(&value) {
            // COUNT / EXISTS / IN — never NULL, so the test folds to a constant.
            return Ok(BExpr::LitInt(i64::from(!want_null)));
        }
        // A nullable scalar: fold over the subquery leaf; decorrelation rewrites the
        // IS [NOT] NULL over the substituted value column.
        match value {
            BExpr::ColRef(r) => Ok(fold_null_test(true, r, want_null)),
            _ => Err(GnitzSqlError::Unsupported(
                "IS [NOT] NULL over this subquery form is not supported".into(),
            )),
        }
    }
    fn bind_subquery(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        self.bind_sub(e)
    }
}

/// Bind a subquery-carrying single-table linear body to `Project(Filter?(Get))`.
/// One pass: each subquery binds where `bind_structural` meets it, so the WHERE
/// and the projection are walked exactly once and in source order.
fn bind_linear_subquery_body(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    ids: &ColIdGen,
    select: &Select,
    get: Rc<RelExpr>,
    env: Vec<HirCol>,
    outer_alias: &str,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let leaf = SubqueryLeaf {
        inner: HirSingleTable { env: &env },
        ctx: RefCell::new(SubCtx {
            client,
            binder,
            ids,
            outer_env: &env,
            outer_alias,
        }),
    };
    let mut rel = get;
    if let Some(where_expr) = &select.selection {
        rel = RelExpr::filter(rel, bind_conjuncts(where_expr, &leaf)?);
    }
    let items = bind_projection(&select.projection, &env, &leaf, ids, "CREATE VIEW projection")?;
    reject_dup_proj_names(&items, &select.projection, "CREATE VIEW projection")?;
    Ok(RelExpr::project(rel, items))
}

/// The resolved inner relation of a subquery: `Filter?(Get)` with the inner-local
/// WHERE fused, plus the mixed-scope correlation conjuncts (over the outer ∪ inner
/// `ColId` space) that become the decorrelated join's ON.
struct InnerResolved<'e> {
    rel: Rc<RelExpr>,
    inner_cols: Vec<HirCol>,
    /// `inner_cols`' `ColId`s — built for the correlation split below, and reused by
    /// the scalar/quantifier binders for their GROUP BY derivation.
    inner_ids: HashSet<ColId>,
    inner_select: &'e Select,
    correlation: Vec<HirExpr>,
}

/// The `ColId` set of an inner relation's columns.
fn id_set(cols: &[HirCol]) -> HashSet<ColId> {
    cols.iter().map(|c| c.id).collect()
}

/// Resolve a subquery's inner relation and split its WHERE into inner-local vs
/// correlation conjuncts. The inner FROM must be one plain relation (no JOINs, no
/// derived table, no GROUP BY / HAVING / DISTINCT); a conjunct referencing only
/// the outer relation is rejected (hoisting would be wrong for NOT EXISTS).
fn resolve_inner<'e>(cx: &mut SubCtx<'_, '_, '_>, subquery: &'e Query) -> Result<InnerResolved<'e>, GnitzSqlError> {
    let (outer_env, outer_alias) = (cx.outer_env, cx.outer_alias);
    let inner_select = plain_select_body(subquery, "subquery")?;
    reject_unhonored_select_clauses(inner_select, HonoredClauses::PLAIN, "subquery")?;
    if !matches!(classify_from(&inner_select.from), FromShape::SinglePlainRelation) {
        return Err(GnitzSqlError::Unsupported(
            "EXISTS/IN subquery: only a single FROM table without JOINs is supported; compose via views".into(),
        ));
    }
    let (inner_name, inner_alias) = extract_table_name_and_alias(&inner_select.from[0].relation, "subquery")?;
    if outer_alias.eq_ignore_ascii_case(&inner_alias) {
        return Err(GnitzSqlError::Bind(format!(
            "relation alias '{outer_alias}' is used by both the view FROM and its subquery; rename one"
        )));
    }
    let (inner_tid, inner_schema) = cx.binder.resolve(cx.client, &inner_name)?;
    let from_catalog = cx.binder.is_catalog_relation(&inner_name);
    let inner_get = RelExpr::get(cx.ids, inner_tid, inner_schema, from_catalog);
    let inner_cols = inner_get.cols();

    // Split the inner WHERE against the (outer, inner) scope by `ColId` membership.
    let mut scope = JoinScope::new();
    scope.push(outer_alias, outer_env.to_vec());
    scope.push(&inner_alias, inner_cols.clone());
    let outer_ids = id_set(outer_env);
    let inner_ids = id_set(&inner_cols);

    let mut local_preds: Vec<HirExpr> = Vec::new();
    let mut correlation: Vec<HirExpr> = Vec::new();
    if let Some(where_expr) = &inner_select.selection {
        let leaf = JoinLeaf {
            scope: &scope,
            nested_subquery_msg: Some(
                "nested subqueries inside an EXISTS/IN subquery are not supported; compose via views",
            ),
        };
        for bound in bind_conjuncts(where_expr, &leaf)? {
            let (mut has_outer, mut has_inner) = (false, false);
            bound.for_each_ref(&mut |r| {
                if let HirRef::Col(id) = r {
                    has_outer |= outer_ids.contains(id);
                    has_inner |= inner_ids.contains(id);
                }
            });
            if has_outer && has_inner {
                correlation.push(bound);
            } else if has_outer {
                return Err(GnitzSqlError::Unsupported(
                    "a subquery WHERE conjunct references only the outer relation; \
                     hoist it into the view's own WHERE clause"
                        .into(),
                ));
            } else {
                local_preds.push(bound);
            }
        }
    }
    let rel = if local_preds.is_empty() {
        inner_get
    } else {
        RelExpr::filter(inner_get, local_preds)
    };
    Ok(InnerResolved {
        rel,
        inner_cols,
        inner_ids,
        inner_select,
        correlation,
    })
}

/// The single projection expression of a subquery's SELECT, else `err`.
fn single_projection_expr<'e>(select: &'e Select, err: &str) -> Result<&'e Expr, GnitzSqlError> {
    match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(e)] | [SelectItem::ExprWithAlias { expr: e, .. }] => Ok(e),
        _ => Err(GnitzSqlError::Unsupported(err.into())),
    }
}

/// Bind one subquery node into a `BoundSub`, dispatching on its kind. ANY/ALL is
/// normalized here (`= ANY → IN`, `<> ALL → NOT IN`, range → `x OP (SELECT MIN/MAX)`).
fn bind_one_subquery(cx: &mut SubCtx<'_, '_, '_>, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
    match e {
        Expr::Exists { subquery, negated } => bind_exists_sub(cx, subquery, None, *negated),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => bind_exists_sub(cx, subquery, Some(expr), *negated),
        Expr::Subquery(q) => bind_scalar_sub(cx, q),
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => bind_quantifier_sub(cx, left, compare_op, right, true),
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => bind_quantifier_sub(cx, left, compare_op, right, false),
        other => Err(GnitzSqlError::Plan(format!(
            "internal: bind_one_subquery on a non-subquery node: {other:?}"
        ))),
    }
}

/// Bind an EXISTS / IN subquery into an `Exists`-kind `SubqueryRef`. For IN, the
/// `(outer, inner)` equality is carried in `in_pair` (folded into the decorrelated
/// join's ON); an EXISTS must be correlated.
fn bind_exists_sub(
    cx: &mut SubCtx<'_, '_, '_>,
    subquery: &Query,
    in_operand: Option<&Expr>,
    negated: bool,
) -> Result<HirExpr, GnitzSqlError> {
    let outer_env = cx.outer_env;
    let ir = resolve_inner(cx, subquery)?;
    let mut in_pair = None;
    if let Some(operand) = in_operand {
        // IN shape: reject a tuple / non-plain-column LHS before extracting the pair.
        if matches!(operand, Expr::Tuple(_)) {
            return Err(GnitzSqlError::Unsupported(
                "IN (SELECT …): tuple operands are not supported".into(),
            ));
        }
        let outer = bind_plain_col(
            operand,
            outer_env,
            "IN (SELECT …): the outer operand must be a plain column",
        )?;
        let inner_proj = single_projection_expr(
            ir.inner_select,
            "IN (SELECT …): the subquery must select exactly one plain column",
        )?;
        let inner = bind_plain_col(
            inner_proj,
            &ir.inner_cols,
            "IN (SELECT …): the subquery must select exactly one plain column",
        )?;
        let nullable = col_nullable(outer_env, outer) || col_nullable(&ir.inner_cols, inner);
        // NOT IN over a nullable operand diverges from the anti-join (SQL 3VL).
        if negated && nullable {
            return Err(GnitzSqlError::Unsupported(
                "NOT IN (SELECT …) requires the outer operand and the subquery column to be \
                 NOT NULL (SQL's NULL semantics diverge from the anti-join otherwise); \
                 use NOT EXISTS with an explicit equality instead"
                    .into(),
            ));
        }
        in_pair = Some(InPair { outer, inner, nullable });
    } else if ir.correlation.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "uncorrelated EXISTS (no conjunct pairing an outer and an inner column) is not supported".into(),
        ));
    }
    let subref = SubqueryRef {
        kind: SubqueryKind::Exists { negated },
        rel: ir.rel,
        correlation: ir.correlation,
        in_pair,
    };
    Ok(BExpr::ColRef(HirRef::Subquery(Box::new(subref))))
}

/// Bind a scalar aggregate subquery into a `Scalar`-kind `SubqueryRef` whose `rel`
/// is a grouped (correlated) / global (uncorrelated) `Reduce`.
fn bind_scalar_sub(cx: &mut SubCtx<'_, '_, '_>, q: &Query) -> Result<HirExpr, GnitzSqlError> {
    let ids = cx.ids;
    let ir = resolve_inner(cx, q)?;
    let proj_expr = single_projection_expr(
        ir.inner_select,
        "a scalar subquery must be a single aggregate over its correlation group",
    )?;
    let (func, arg) = classify_scalar_agg(proj_expr, &ir.inner_cols)?;
    let group_cols = scalar_group_cols(&ir.correlation, &ir.inner_ids)?;
    let reduce = build_scalar_reduce(ids, ir.rel, group_cols, func, arg, &ir.inner_cols)?;
    Ok(BExpr::ColRef(HirRef::Subquery(Box::new(SubqueryRef {
        kind: SubqueryKind::Scalar,
        rel: reduce,
        correlation: ir.correlation,
        in_pair: None,
    }))))
}

/// Bind an ANY/ALL quantified comparison. `= ANY`/`<> ALL` route to IN/NOT IN;
/// range ANY/ALL becomes `x OP (SELECT MIN/MAX)` — the 3VL null-fill wrapper for a
/// correlated subquery, the bare comparison for an uncorrelated top-level one.
fn bind_quantifier_sub(
    cx: &mut SubCtx<'_, '_, '_>,
    left: &Expr,
    compare_op: &BinaryOperator,
    right: &Expr,
    is_any: bool,
) -> Result<HirExpr, GnitzSqlError> {
    let Expr::Subquery(q) = right else {
        return Err(GnitzSqlError::Unsupported(
            "an ANY/ALL operator's right operand must be a subquery".into(),
        ));
    };
    match (is_any, compare_op) {
        (true, BinaryOperator::Eq) => return bind_exists_sub(cx, q, Some(left), false),
        (false, BinaryOperator::NotEq) => return bind_exists_sub(cx, q, Some(left), true),
        (true, BinaryOperator::NotEq) => {
            return Err(GnitzSqlError::Unsupported(
                "`<> ANY (SELECT …)` is not supported (use NOT (x = ALL …) semantics via a wrapping view)".into(),
            ))
        }
        (false, BinaryOperator::Eq) => {
            return Err(GnitzSqlError::Unsupported("`= ALL (SELECT …)` is not supported".into()))
        }
        _ => {}
    }
    let (bop, less) = match compare_op {
        BinaryOperator::Lt => (BinOp::Lt, true),
        BinaryOperator::LtEq => (BinOp::Le, true),
        BinaryOperator::Gt => (BinOp::Gt, false),
        BinaryOperator::GtEq => (BinOp::Ge, false),
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "only range (<, <=, >, >=), `= ANY`, and `<> ALL` quantified comparisons are supported".into(),
            ))
        }
    };
    // x < ANY ⟺ < MAX; x > ANY ⟺ > MIN; x < ALL ⟺ < MIN; x > ALL ⟺ > MAX.
    let agg_func = if is_any == less { AggFunc::Max } else { AggFunc::Min };
    let (ids, outer_env) = (cx.ids, cx.outer_env);
    let ir = resolve_inner(cx, q)?;
    let proj_expr = single_projection_expr(
        ir.inner_select,
        "a range ANY/ALL subquery must select a single non-nullable column",
    )?;
    let inner_col = bind_plain_col(
        proj_expr,
        &ir.inner_cols,
        "a range ANY/ALL subquery must select a single non-nullable column",
    )?;
    if col_nullable(&ir.inner_cols, inner_col) {
        return Err(GnitzSqlError::Unsupported(
            "a range ANY/ALL subquery's column must be NOT NULL — a NULL in the set makes ANY/ALL \
             three-valued in a way the MIN/MAX rewrite cannot reproduce"
                .into(),
        ));
    }
    let correlated = !ir.correlation.is_empty();
    let group_cols = scalar_group_cols(&ir.correlation, &ir.inner_ids)?;
    let reduce = build_scalar_reduce(ids, ir.rel, group_cols, agg_func, Some(inner_col), &ir.inner_cols)?;
    let subref = SubqueryRef {
        kind: SubqueryKind::Scalar,
        rel: reduce,
        correlation: ir.correlation,
        in_pair: None,
    };
    let m_leaf = HirRef::Subquery(Box::new(subref));
    let outer_leaf = HirSingleTable { env: outer_env };
    let x = bind_structural(left, &outer_leaf)?;
    let cmp = BExpr::BinOp(Box::new(x), bop, Box::new(BExpr::ColRef(m_leaf.clone())));
    let value = if correlated {
        // ANY over ∅ = FALSE, ALL over ∅ = TRUE — the LEFT join sets m = NULL for
        // an empty group, so the explicit null test makes the edge a definite
        // constant (exact under negation).
        if is_any {
            BExpr::BinOp(Box::new(fold_null_test(true, m_leaf, false)), BinOp::And, Box::new(cmp))
        } else {
            BExpr::BinOp(Box::new(fold_null_test(true, m_leaf, true)), BinOp::Or, Box::new(cmp))
        }
    } else if is_any {
        cmp
    } else {
        return Err(GnitzSqlError::Unsupported(
            "an uncorrelated range ANY subquery is only supported as a top-level WHERE conjunct, \
             and uncorrelated range ALL is unsupported"
                .into(),
        ));
    };
    Ok(value)
}

/// Bind `e` against `env` and require a bare column reference, returning its `ColId`.
fn bind_plain_col(e: &Expr, env: &[HirCol], err: &str) -> Result<ColId, GnitzSqlError> {
    let leaf = HirSingleTable { env };
    match bind_structural(e, &leaf) {
        Ok(BExpr::ColRef(HirRef::Col(id))) => Ok(id),
        _ => Err(GnitzSqlError::Unsupported(err.into())),
    }
}

/// Whether a `ColId`'s column in `cols` is nullable (false if absent).
fn col_nullable(cols: &[HirCol], id: ColId) -> bool {
    col_by_id(cols, id).map(|c| c.def.is_nullable).unwrap_or(false)
}

/// Classify a scalar subquery's single-aggregate projection into `(func, arg)`.
fn classify_scalar_agg(e: &Expr, inner_cols: &[HirCol]) -> Result<(AggFunc, Option<ColId>), GnitzSqlError> {
    let peeled = peel_nested(e);
    let non_agg =
        || GnitzSqlError::Unsupported("a scalar subquery must be a single aggregate over its correlation group".into());
    let Expr::Function(f) = peeled else {
        return Err(non_agg());
    };
    if !is_agg_call(f) {
        return Err(non_agg());
    }
    let (func, arg_expr) = classify_agg_call(f)?;
    let arg = match arg_expr {
        Some(a) => Some(bind_plain_col(
            a,
            inner_cols,
            "a scalar subquery aggregate must be over a plain column or `*`",
        )?),
        None => None,
    };
    if let Some(id) = arg {
        reject_min_max_unorderable(func, col_by_id(inner_cols, id).expect("agg arg in inner").def.type_code)?;
    }
    Ok((func, arg))
}

/// The `Reduce` group columns of a scalar/quantifier subquery: the inner-side
/// `ColId` of each equality correlation conjunct. A range or non-equality
/// correlation is rejected (aggregation needs an equality GROUP BY key).
fn scalar_group_cols(correlation: &[HirExpr], inner_ids: &HashSet<ColId>) -> Result<Vec<ColId>, GnitzSqlError> {
    let mut cols = Vec::with_capacity(correlation.len());
    for conj in correlation {
        if let BExpr::BinOp(l, BinOp::Eq, r) = conj {
            if let (Some(a), Some(b)) = (as_col(l), as_col(r)) {
                match (inner_ids.contains(&a), inner_ids.contains(&b)) {
                    (true, false) => {
                        cols.push(a);
                        continue;
                    }
                    (false, true) => {
                        cols.push(b);
                        continue;
                    }
                    _ => {}
                }
            }
        }
        // A range comparison between two columns gets the range-specific message.
        if let BExpr::BinOp(l, BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge, r) = conj {
            if as_col(l).is_some() && as_col(r).is_some() {
                return Err(GnitzSqlError::Unsupported(
                    "a scalar/quantifier subquery cannot use a range correlation (only equality \
                     `inner_col = outer_col` conjuncts are supported)"
                        .into(),
                ));
            }
        }
        return Err(GnitzSqlError::Unsupported(
            "a scalar/quantifier subquery's correlation contains a conjunct that is not an \
             `inner_col = outer_col` equality; filter inside the subquery or a wrapping view"
                .into(),
        ));
    }
    Ok(cols)
}

/// Build a scalar subquery's raw `Reduce` (one aggregate). The finalize composite
/// is applied later by decorrelation over the raw reduce output.
fn build_scalar_reduce(
    ids: &ColIdGen,
    rel: Rc<RelExpr>,
    group_cols: Vec<ColId>,
    func: AggFunc,
    arg: Option<ColId>,
    inner_cols: &[HirCol],
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let arg_def = arg.map(|id| &col_by_id(inner_cols, id).expect("agg arg in inner").def);
    let typing = agg_typing(func, arg_def)?;
    let arg_nullable = arg_def.map(|d| d.is_nullable).unwrap_or(false);
    let hir_agg = HirAgg::new(ids, func, arg, &typing, arg_nullable, group_cols.is_empty());
    Ok(RelExpr::reduce(rel, group_cols, vec![hir_agg]))
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
    ids: &ColIdGen,
    select: &Select,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let grouped = body_is_grouped(select);
    let distinct = select.distinct.is_some();
    reject_unhonored_select_clauses(select, HonoredClauses::for_body(grouped, distinct), "CREATE VIEW JOIN")?;
    let from = &select.from[0];

    // Leftmost relation (a table, a CTE, or a derived table).
    let (left_src, lalias, lcols) = resolve_table_factor(client, binder, ids, &from.relation)?;
    let mut scope = JoinScope::new();
    scope.push(&lalias, lcols);
    let mut left = left_src;

    // Each join step: resolve the right relation, bind the ON conjuncts against the
    // accumulated scope, then fold into a `Join` node.
    for join in &from.joins {
        let (on_expr, kind) = join_on_and_type(join)?;
        let (right_src, ralias, rcols) = resolve_table_factor(client, binder, ids, &join.relation)?;
        scope.push(&ralias, rcols);
        let on = bind_conjuncts(on_expr, &join_leaf(&scope))?;
        left = RelExpr::join(left, right_src, kind, on, None);
        // Reflect this step's null-widening back into the scope so a later ON /
        // the WHERE / the projection resolve against the widened nullability.
        scope.widen_step(kind);
    }

    // WHERE → a `Filter` over the top join (raw conjuncts; the rewrite places them).
    let mut rel = left;
    if let Some(where_expr) = &select.selection {
        rel = RelExpr::filter(rel, bind_conjuncts(where_expr, &join_leaf(&scope))?);
    }

    // DISTINCT / GROUP BY over a join: the operator sits above the join tree (the
    // lowering cuts the join to a hidden segment). DISTINCT outranks grouped.
    if distinct {
        // Guarded by `lower_distinct` over the full output columns (see above).
        let items = bind_projection(
            &select.projection,
            &scope.combined,
            &join_leaf(&scope),
            ids,
            "JOIN view",
        )?;
        return Ok(RelExpr::distinct(RelExpr::project(rel, items)));
    }
    if grouped {
        let leaf = join_leaf(&scope);
        return bind_grouped_suffix(ids, select, rel, &leaf);
    }

    // Projection over the join output scope (column references + wildcard only —
    // a computed expression over a join output is rejected).
    let items = bind_projection(
        &select.projection,
        &scope.combined,
        &join_leaf(&scope),
        ids,
        "JOIN view",
    )?;
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

    /// Apply one join step's outer null-widening to the scope, in place: the
    /// accumulated left side widens iff the step preserves its right, and the
    /// just-pushed right relation iff it preserves its left — the same rule, through
    /// the same [`widen_if`] primitive, that [`RelExpr::cols`] widens the join's
    /// logical output with.
    ///
    /// In place rather than adopting `join.cols()`: a derived table's `AS d(col…)`
    /// aliases live only on the cols `resolve_table_factor` handed the scope — the
    /// bound subtree deliberately keeps its own inner names — so rebuilding the scope
    /// from the tree would drop every positional alias in a join body. Widening
    /// touches only `is_nullable`, so the aliases (and the `ColId`s) survive.
    fn widen_step(&mut self, kind: JoinType) {
        let split = self.relations.last().expect("a right relation was pushed").1.start;
        let (left, right) = self.combined.split_at_mut(split);
        widen_if(left.iter_mut().map(|c| &mut c.def), kind.preserves_right());
        widen_if(right.iter_mut().map(|c| &mut c.def), kind.preserves_left());
    }

    fn rel_cols(&self, span: &Range<usize>) -> &[HirCol] {
        &self.combined[span.clone()]
    }

    fn resolve_qualified(&self, alias: &str, name: &str) -> Result<ColId, GnitzSqlError> {
        let lc = alias.to_ascii_lowercase();
        let (_, span) = self
            .relations
            .iter()
            .find(|(a, _)| *a == lc)
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
    /// The rejection for a subquery met at this leaf. `None` keeps the generic
    /// `LeafBinder` message (a FROM-clause `ON`); the subquery-correlation leaf
    /// sets the nested-subquery wording.
    nested_subquery_msg: Option<&'static str>,
}

/// The FROM-clause `ON` / projection leaf: no nested-subquery override, so a
/// subquery there gets the generic per-kind rejection.
fn join_leaf(scope: &JoinScope) -> JoinLeaf<'_> {
    JoinLeaf {
        scope,
        nested_subquery_msg: None,
    }
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
    fn bind_subquery(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        Err(match self.nested_subquery_msg {
            Some(msg) => GnitzSqlError::Unsupported(msg.into()),
            None => crate::bind::structural::unsupported_subquery(e),
        })
    }
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

/// Extract the `ColId` of a bound bare column reference.
fn col_of(e: HirExpr) -> Result<ColId, GnitzSqlError> {
    match e {
        BExpr::ColRef(HirRef::Col(id)) => Ok(id),
        _ => Err(GnitzSqlError::Plan("internal: expected a column reference".into())),
    }
}

/// The AVG / nullable-SUM / Direct finalize composite over the raw reduce output
/// — the shared rule, over this binder's column-identity leaf.
fn finalize_agg_expr(ga: &GroupAgg) -> HirExpr {
    finalize_agg_bexpr(
        HirRef::Col(ga.agg.out.id),
        ga.agg.companion.as_ref().map(|c| HirRef::Col(c.id)),
        ga.agg.func,
    )
}

/// Resolve GROUP BY columns to `ColId`s (bare/qualified refs only). A computed
/// group column gives a GROUP-BY-context rejection (not the leaf's JOIN-ON message).
fn resolve_group_cols<L: LeafBinder<HirRef>>(
    select: &Select,
    env: &[HirCol],
    leaf: &L,
) -> Result<Vec<ColId>, GnitzSqlError> {
    let group_exprs = group_by_exprs(select)?;
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
    ids: &ColIdGen,
) -> Result<Option<usize>, GnitzSqlError> {
    let mut top: Option<usize> = None;
    let is_top = for_each_agg_call(expr, &mut |f| -> Result<(), GnitzSqlError> {
        let (func, arg_expr) = classify_agg_call(f)?;
        let arg = match arg_expr {
            Some(e) => Some(col_of(leaf.bind_column(e)?)?),
            None => None,
        };
        // MIN/MAX orderability — checked here (Unsupported) so the message and error
        // variant match the leaf binder's, ahead of `agg_typing`'s `Bind` backstop.
        if let Some(id) = arg {
            reject_min_max_unorderable(func, col_by_id(env, id).expect("agg arg in env").def.type_code)?;
        }
        top = Some(match aggs.iter().position(|a| a.agg.func == func && a.agg.arg == arg) {
            Some(idx) => idx,
            None => {
                let arg_def = arg.map(|id| &col_by_id(env, id).expect("agg arg ColId in env").def);
                let typing = agg_typing(func, arg_def)?;
                let arg_nullable = arg_def.map(|d| d.is_nullable).unwrap_or(false);
                let output_nullable = agg_output_nullable(&typing, arg_nullable, is_global);
                aggs.push(GroupAgg {
                    agg: HirAgg::new(ids, func, arg, &typing, arg_nullable, is_global),
                    view_type: typing.view_type,
                    output_nullable,
                });
                aggs.len() - 1
            }
        });
        Ok(())
    })?;
    Ok(is_top.then_some(top).flatten())
}

/// Bind the GROUP BY / aggregate / HAVING suffix over `input` (the `Filter?(source)`
/// tree built by the FROM binder), producing `Project(Filter_having?(Reduce(input)))`.
fn bind_grouped_suffix<L: LeafBinder<HirRef>>(
    ids: &ColIdGen,
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
    ids: &ColIdGen,
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
            let name = alias.unwrap_or_else(|| default_agg_name(ga.agg.func, idx));
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
                return Err(reject_ungrouped_column(&name));
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
        return Err(reject_computed_grouped_item());
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
        let (func, arg_expr) = classify_agg_call(f)?;
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
        if is_agg_call(f) {
            return Ok(finalize_agg_expr(self.find_agg(f)?));
        }
        self.leaf.bind_function(f)
    }
    fn bind_null_test(&self, inner: &Expr, want_null: bool) -> Result<HirExpr, GnitzSqlError> {
        let peeled = peel_nested(inner);
        if let Expr::Function(f) = peeled {
            if is_agg_call(f) {
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
    ids: &ColIdGen,
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
    // Each side binds as any relational body — a plain SELECT, a join, a grouped
    // query, a nested set operation, or a derived table (all handled by
    // `bind_body` → `resolve_table_factor`).
    let left_rel = bind_body(client, binder, ids, left)?;
    let right_rel = bind_body(client, binder, ids, right)?;
    RelExpr::set_op(ids, kind, all, left_rel, right_rel)
}
