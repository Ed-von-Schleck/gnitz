//! AST → HIR binding. The one entry, `bind_query`, resolves every name to an
//! opaque `ColId`, validates the honored clauses, and produces a logical
//! `RelExpr` tree — single-table linear (`Simple`) and FROM-join (`Join`)
//! bodies. CTE/derived aliases are already in the binder cache (the old
//! `inline_ctes`/`compile_derived_tables` ran first in `build_query_segments`),
//! so a `Get` resolves them by name.

use super::{col_by_id, ColId, ColIdGen, HirCol, HirExpr, HirRef, ProjEntry, RelExpr};
use crate::ast_util::{
    agg_func_from_name, extract_relation_name, extract_table_name_and_alias, flatten_conjuncts, is_wildcard_projection,
    reject_unsupported_fn_qualifiers, single_fn_name, single_relation_col_name, wildcard_name_is_visible,
    WildcardRewrite,
};
use crate::bind::{bind_structural, find_unique_column, fold_null_test, Binder, LeafBinder};
use crate::error::GnitzSqlError;
use crate::ir::BExpr;
use crate::plan::validate::{reject_duplicate_column_names, reject_unhonored_select_clauses, HonoredClauses};
use crate::plan::view::join::join_on_and_type;
use gnitz_core::{ColumnDef, GnitzClient, TypeCode};
use sqlparser::ast::{Expr, Function, Query, Select, SelectItem, SetExpr};
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
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW only supports SELECT".to_string(),
        ));
    };
    let rel = bind_select(client, binder, &mut ids, select)?;
    Ok((rel, ids))
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
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
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

    // Projection — SELECT order (place_pk_front is physical, applied at lowering).
    let items = bind_projection(&select.projection, &env, &leaf, ids)?;
    // Dup-name check for a non-wildcard projection (matching lower_linear); a pure
    // `SELECT *` carries duplicates through positionally.
    if !is_wildcard_projection(&select.projection) {
        let out_defs: Vec<ColumnDef> = items.iter().map(|e| e.out.def.clone()).collect();
        reject_duplicate_column_names(&out_defs, "CREATE VIEW projection")?;
    }
    Ok(RelExpr::project(rel, items))
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
    reject_unhonored_select_clauses(
        select,
        HonoredClauses {
            where_filter: true,
            grouping: false,
            distinct: false,
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

    // Projection over the join output scope (column references + wildcard only,
    // matching `build_join_view_projection`).
    let items = bind_join_projection(&select.projection, &scope, ids)?;
    if !is_wildcard_projection(&select.projection) {
        let out_defs: Vec<ColumnDef> = items.iter().map(|e| e.out.def.clone()).collect();
        reject_duplicate_column_names(&out_defs, "join view")?;
    }
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
