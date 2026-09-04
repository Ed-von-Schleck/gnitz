//! AST → HIR binding. `bind_body` resolves every name to an opaque `ColId`,
//! validates the honored clauses, and produces a logical `RelExpr` tree for any
//! relational body (linear / join / grouped / DISTINCT / set operation). The CTE
//! phase (`bind_ctes`) binds each CTE body to a shared subtree ahead of the
//! main bind and registers it by name, so a reference aliases it; a derived
//! table binds its subquery recursively to an inline subtree
//! (`resolve_table_factor`).

use super::{
    as_col, col_by_id, hircol_of, widen_if, ColId, ColIdGen, HirAgg, HirCol, HirExpr, HirRef, InPair, JoinType,
    ProjEntry, RelExpr, SetOpKind, SubqueryKind, SubqueryRef,
};
use crate::agg::default_agg_name;
use crate::ast_util::{
    aliased_def, body_is_grouped, classify_agg_call, classify_from, expand_wildcard_item, extract_table_name_and_alias,
    for_each_agg_call, group_by_exprs, group_by_target, has_exists_in_subquery, has_scalar_subquery, is_agg_call,
    peel_nested, projection_item_expr, scalar_projection_item, select_has_window, single_relation_col_name,
    wildcard_name_is_visible, window_spec_keys, FromShape,
};
use crate::bind::apply_positional_aliases;
use crate::bind::{bind_conjuncts, bind_structural, find_unique_column, single_relation_col_idx, Binder, LeafBinder};
use crate::error::{reject_if, GnitzSqlError};
use crate::hir::guards::{join_keys_and_type, JoinKeys};
use crate::ir::{AggFunc, BExpr, BinOp};
use crate::validate::{
    cte_body, non_recursive_ctes, plain_select_body, reject_duplicate_projection_names, reject_float_key,
    reject_float_key_of, reject_query_envelope_body, reject_unhonored_select_clauses, validate_user_name,
    HonoredClauses,
};
use gnitz_core::{CatalogSnapshot, ColumnDef, Schema, TypeCode};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, NamedWindowExpr, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier,
    TableFactor,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;

/// Bind a query's CTEs ahead of its body. Each binds to one subtree registered
/// under its name, which the body (and later CTEs) read through an
/// [`RelExpr::Alias`] and the lowering cuts once. Scoping precedence as SQL
/// defines it: a CTE shadows a catalog name, and a later CTE sees earlier ones.
pub(crate) fn bind_ctes(cx: &mut BindCx<'_, '_>, query: &Query) -> Result<(), GnitzSqlError> {
    for cte in non_recursive_ctes(query)? {
        let name = cte.alias.name.value.clone();
        // A CTE name is a relation name later references resolve, so it is held
        // to the reserved-prefix rule every such name passes.
        validate_user_name(&name)?;
        let ctx = format!("CTE '{name}'");
        let rel = bind_body(cx, cte_body(cte, &ctx)?)?;
        let (rel, mut defs) = collapse_identity(rel);
        apply_positional_aliases(
            cte.alias.columns.iter().map(|a| &a.name),
            defs.iter_mut().collect(),
            &ctx,
        )?;
        cx.ctes.insert(name.to_ascii_lowercase(), Cte { rel, defs });
    }
    Ok(())
}

/// A bound CTE: the shared subtree every reference aliases, and the column
/// defs a reference carries — the subtree's own, renamed by the SELECT list's
/// aliases and the CTE's positional ones.
pub(crate) struct Cte {
    rel: Rc<RelExpr>,
    defs: Vec<ColumnDef>,
}

/// A body that only renames its source's visible columns, in order, collapses
/// to that source under the new names: the projection carries no work, and an
/// alias over the source reads it in place where an alias over the projection
/// would cut a copy. Any other body is returned as it is, under its own defs.
fn collapse_identity(rel: Rc<RelExpr>) -> (Rc<RelExpr>, Vec<ColumnDef>) {
    if let RelExpr::Project { input, items } = rel.as_ref() {
        let in_cols = input.cols();
        let visible: Vec<&HirCol> = in_cols.iter().filter(|c| !c.def.is_hidden).collect();
        let identity = items.len() == visible.len()
            && items
                .iter()
                .zip(&visible)
                .all(|(it, c)| as_col(&it.expr) == Some(c.id) && !it.out.def.is_hidden);
        if identity {
            let mut renamed = items.iter();
            let defs = in_cols
                .iter()
                .map(|c| {
                    if c.def.is_hidden {
                        c.def.clone()
                    } else {
                        renamed.next().expect("one item per visible column").out.def.clone()
                    }
                })
                .collect();
            return (Rc::clone(input), defs);
        }
    }
    let defs = rel.cols().into_iter().map(|c| c.def).collect();
    (rel, defs)
}

/// Everything a body bind resolves against: the catalog snapshot, the alias
/// binder, the `ColId` minter, and the query's CTEs. One value threaded through
/// the recursion, so a derived table, a set-operation side and a subquery bind
/// against the same four.
pub(crate) struct BindCx<'c, 'b> {
    pub(crate) cat: &'c CatalogSnapshot,
    pub(crate) binder: &'c mut Binder<'b>,
    pub(crate) ids: &'c ColIdGen,
    /// The CTEs bound so far, by canonical (ASCII-lowercase) name.
    ctes: HashMap<String, Cte>,
}

impl<'c, 'b> BindCx<'c, 'b> {
    pub(crate) fn new(cat: &'c CatalogSnapshot, binder: &'c mut Binder<'b>, ids: &'c ColIdGen) -> Self {
        BindCx { cat, binder, ids, ctes: HashMap::new() }
    }
}

/// Resolve a relation name to the subtree a FROM reference reads: an alias of
/// the CTE it names, else a `Get` on the catalog relation. The one resolution
/// every relation reference goes through — a FROM item, a join step, a
/// subquery's inner relation — so a CTE is visible to all of them alike.
fn resolve_relation(cx: &mut BindCx<'_, '_>, name: &str) -> Result<Rc<RelExpr>, GnitzSqlError> {
    if let Some(cte) = cx.ctes.get(&name.to_ascii_lowercase()) {
        return Ok(RelExpr::alias_as(cx.ids, Rc::clone(&cte.rel), &cte.defs));
    }
    let (tid, schema, desc) = cx.binder.resolve(cx.cat, name)?;
    Ok(RelExpr::get(cx.ids, tid, schema, Some(desc)))
}

/// Bind one query body — a single SELECT (linear / join / grouped / DISTINCT) or
/// a set operation whose sides bind recursively. A parenthesized side is a whole
/// `Query`, whose envelope is rejected before its body binds.
pub(crate) fn bind_body(cx: &mut BindCx<'_, '_>, body: &SetExpr) -> Result<Rc<RelExpr>, GnitzSqlError> {
    match body {
        SetExpr::Select(select) => bind_select(cx, select),
        SetExpr::SetOperation { op, set_quantifier, left, right } => bind_set_op(cx, *op, *set_quantifier, left, right),
        SetExpr::Query(q) => bind_body(cx, reject_query_envelope_body(q, "parenthesized query")?),
        _ => Err(GnitzSqlError::Unsupported(
            "CREATE VIEW only supports SELECT and set operations".to_string(),
        )),
    }
}

/// Resolve one FROM table factor to `(source subtree, alias, output cols)`. A
/// table/CTE name resolves via the binder to a base or segment `Get`; a derived
/// table binds its subquery recursively to an **inline** subtree (never a
/// segment — single-use and non-LATERAL, so uncorrelated) whose `AS d(col…)`
/// aliases are applied to the returned cols (same `ColId`s, overridden names) the
/// caller pushes into its scope/env. The subtree itself keeps its own names; a
/// derived alias resolves through the caller's scope, never the binder cache.
fn resolve_table_factor(
    cx: &mut BindCx<'_, '_>,
    factor: &TableFactor,
) -> Result<(Rc<RelExpr>, String, Vec<HirCol>), GnitzSqlError> {
    if let TableFactor::Derived { lateral, subquery, alias, sample } = factor {
        let Some(alias) = alias else {
            return Err(GnitzSqlError::Unsupported(
                "a derived table (subquery in FROM) needs an alias".to_string(),
            ));
        };
        // The alias is a user-visible name, so it is held to the general
        // user-identifier rule (a leading `_` is reserved for the `_seg…`
        // hidden-segment namespace) even though an inline derived subtree never
        // enters the binder's alias cache.
        validate_user_name(&alias.name.value)?;
        let ctx = format!("derived table '{}'", alias.name.value);
        reject_if(*lateral, &ctx, "LATERAL")?;
        // Silently dropping TABLESAMPLE would return all rows — a wrong result.
        reject_if(sample.is_some(), &ctx, "TABLESAMPLE")?;
        let body = reject_query_envelope_body(subquery, &ctx)?;
        let subtree = bind_body(cx, body)?;
        let mut cols = subtree.cols();
        apply_positional_aliases(
            alias.columns.iter().map(|a| &a.name),
            cols.iter_mut().map(|c| &mut c.def).collect(),
            &ctx,
        )?;
        return Ok((subtree, alias.name.value.clone(), cols));
    }
    let (name, alias) = extract_table_name_and_alias(factor, "CREATE VIEW")?;
    let rel = resolve_relation(cx, &name)?;
    let cols = rel.cols();
    Ok((rel, alias, cols))
}

/// Bind one single-SELECT body to `Project(Filter?(source))`, where `source` is
/// the FROM relation or the left-deep fold of its join steps — one relation
/// being the same tree with no step. Steps fold in syntactic order (no
/// reordering), so `a LEFT JOIN b JOIN c` is `(a LEFT JOIN b) JOIN c`, and a
/// comma binds loosest: `FROM a JOIN b ON …, c` is `((a JOIN b) , c)`.
///
/// Conjuncts bind raw — into `Join.on` from the step, into a `Filter` from the
/// WHERE. `hir::rewrite` is what classifies them into keys.
fn bind_select(cx: &mut BindCx<'_, '_>, select: &Select) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let Some(first) = select.from.first() else {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW: a view body reads at least one relation; this one has no FROM clause".to_string(),
        ));
    };
    let grouped = body_is_grouped(select);
    let distinct = select.distinct.is_some();
    reject_unhonored_select_clauses(
        select,
        HonoredClauses::for_body(grouped, distinct).with_windows(),
        "CREATE VIEW",
    )?;

    // The first FROM relation (a table, a CTE, or a derived table) seeds the
    // accumulator and the scope; its cols are what name resolution starts from.
    let (mut left, alias, cols) = resolve_table_factor(cx, &first.relation)?;
    let mut scope = JoinScope::single(&alias, cols);

    for (i, item) in select.from.iter().enumerate() {
        // Item 0's relation seeded the accumulator above; every later item is one
        // more INNER step carrying no keys of its own — the comma's whole meaning.
        if i > 0 {
            let comma = (JoinKeys::None, JoinType::Inner);
            left = fold_join_step(cx, &mut scope, left, &item.relation, comma)?;
        }
        // Then that item's own JOIN chain, left-deep in syntactic order.
        for join in &item.joins {
            let step = join_keys_and_type(join)?;
            left = fold_join_step(cx, &mut scope, left, &join.relation, step)?;
        }
    }

    // GROUP BY and DISTINCT cannot host a subquery in one circuit; every
    // combination this does not route falls to the body leaf's own rejection.
    // Inside the one-relation gate, so a join body pays neither AST walk.
    if scope.relations.len() == 1 {
        let has_exists_in = has_exists_in_subquery(select);
        if has_exists_in && (grouped || distinct) {
            let clause = if grouped {
                "GROUP BY/aggregates"
            } else {
                "SELECT DISTINCT"
            };
            return Err(GnitzSqlError::Unsupported(format!(
                "EXISTS/IN subqueries are not supported together with {clause}; \
                 put the subquery in an inner view"
            )));
        }
        if !grouped && !distinct && (has_exists_in || has_scalar_subquery(select)) {
            return bind_linear_subquery_body(cx, select, left, &scope, &alias);
        }
    }

    // The WHERE lands as a `Filter` over the source (raw conjuncts; the rewrite
    // places them). A DISTINCT / GROUP BY over a join sits above the join tree —
    // the lowering cuts the join to a hidden segment.
    let leaf = ScopeLeaf {
        scope: &scope,
        clause: "CREATE VIEW",
        sub: SubPolicy::PerKind,
    };
    bind_body_suffix(cx.ids, select, left, &leaf, "CREATE VIEW projection")
}

/// WHERE, then the projection in whichever shape the body carries — the tail
/// every single-table, subquery and join body shares once its source relation
/// and leaf binder are resolved. `ctx` names the surface in messages.
///
/// DISTINCT outranks a grouped shape. The projection is bound in SELECT order
/// (`place_pk_front` is physical, applied at lowering).
fn bind_body_suffix(
    ids: &ColIdGen,
    select: &Select,
    source: Rc<RelExpr>,
    leaf: &ScopeLeaf<'_>,
    ctx: &str,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let mut rel = source;
    if let Some(where_expr) = &select.selection {
        rel = RelExpr::filter(rel, bind_conjuncts(where_expr, leaf)?);
    }
    if body_is_grouped(select) && select.distinct.is_none() {
        return bind_grouped_suffix(ids, select, rel, leaf);
    }
    // The window desugar owns its own projection (it must place the SELECT list
    // over the joined-in window values), so it hands back the projected relation.
    let projected = if select_has_window(select) {
        super::window::bind_window_final(ids, select, rel, leaf, ctx)?
    } else {
        let items = bind_projection(&select.projection, leaf, ids, ctx)?;
        reject_duplicate_projection_names(&select.projection, items.iter().map(|e| &e.out.def), ctx)?;
        RelExpr::project(rel, items)
    };
    Ok(if select.distinct.is_some() {
        RelExpr::distinct(projected)
    } else {
        projected
    })
}

/// Expand a bare `*` item over `cols` (honoring `EXCEPT`/`EXCLUDE`/`RENAME`,
/// skipping hidden and merged columns) into pass-through `ProjEntry`s — the one
/// wildcard expansion, shared by the linear and join projections.
fn expand_wildcard(
    item: &SelectItem,
    cols: &[HirCol],
    ctx: &str,
    ids: &ColIdGen,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    // Merged columns are dropped before the expansion rather than inside it, so
    // the indices it returns address this list.
    let cols: Vec<&HirCol> = cols.iter().filter(|c| !c.merged).collect();
    Ok(expand_wildcard_item(item, cols.iter().map(|c| &c.def), ctx)?
        .into_iter()
        .map(|(i, def)| ProjEntry {
            expr: BExpr::ColRef(HirRef::Col(cols[i].id)),
            out: HirCol::new(ids.next(), def),
        })
        .collect())
}

/// The leaf a SELECT list binds through, beyond expression binding: the
/// columns it types, whether a bare `*` is an item, and what a top-level call
/// item's output column is. One rule for the linear, grouped and windowed
/// projections, so an item means the same thing wherever it is written.
pub(crate) trait ItemLeaf: LeafBinder<HirRef> {
    /// The columns in scope — the typing table for every `ColId` this leaf
    /// hands out.
    fn env(&self) -> &[HirCol];
    /// The declared type of a leaf reference. A leaf minting columns of its own
    /// (a window placeholder) answers for those; everything else is the env's.
    fn type_of(&self, r: &HirRef) -> TypeCode {
        type_of(self.env(), r)
    }
    /// Whether a bare `*` is an item here — false for a grouped body, where
    /// every item is a group key or an aggregate.
    fn expands_wildcard(&self) -> bool;
    /// A top-level function-call item's value and output def — an aggregate in
    /// a grouped body takes its own name, type and nullability, a window call
    /// its placeholder's — or `None` to bind the item as any other expression.
    fn call_item(
        &self,
        _f: &Function,
        _alias: &Option<String>,
        _idx: usize,
    ) -> Option<Result<(HirExpr, ColumnDef), GnitzSqlError>> {
        None
    }
}

/// Resolve every SELECT item into a `ProjEntry` in SELECT order, expanding a bare
/// `*` via [`expand_wildcard`].
pub(crate) fn bind_projection<L: ItemLeaf>(
    projection: &[SelectItem],
    leaf: &L,
    ids: &ColIdGen,
    ctx: &str,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    let mut items = Vec::new();
    for (idx, item) in projection.iter().enumerate() {
        // Only a *bare* `*` expands: a `tbl.*` QualifiedWildcard is not a
        // single-table projection item, so it falls to the `_` reject arm.
        if matches!(item, SelectItem::Wildcard(_)) && leaf.expands_wildcard() {
            items.extend(expand_wildcard(item, leaf.env(), ctx, ids)?);
            continue;
        }
        let (expr, alias) = scalar_projection_item(item, ctx)?;
        if let Expr::Function(f) = peel_nested(expr) {
            if let Some(call) = leaf.call_item(f, &alias, idx) {
                let (expr, def) = call?;
                items.push(ProjEntry { expr, out: HirCol::new(ids.next(), def) });
                continue;
            }
        }
        items.push(bind_proj_expr(expr, alias, idx, leaf, ids)?);
    }
    Ok(items)
}

/// Bind one non-wildcard SELECT expression into a `ProjEntry`. A bare (possibly
/// aliased/qualified/parenthesized) column reference binds to a pass-through
/// carrying the source column's def (alias only renames); anything else is a
/// computed column, named and typed by [`crate::validate::computed_column`].
///
/// A hidden column is computed, not passed through: it has no name the user
/// wrote, so there is nothing to inherit. Only the grouped leaf can reach one —
/// it resolves a written composite GROUP BY key to the pre-map column holding it
/// — since every other leaf resolves names through `find_unique_column`, which
/// skips hidden columns.
fn bind_proj_expr<L: ItemLeaf>(
    expr: &Expr,
    alias: Option<String>,
    idx: usize,
    leaf: &L,
    ids: &ColIdGen,
) -> Result<ProjEntry, GnitzSqlError> {
    let bound = bind_structural(expr, leaf)?;
    // A leaf's own minted column (a window placeholder) is hidden, so it never
    // passes through — the computed branch types it through `leaf.type_of`.
    let src_def = as_col(&bound)
        .and_then(|id| col_by_id(leaf.env(), id))
        .map(|c| &c.def)
        .filter(|d| !d.is_hidden);
    let out_def = match src_def {
        Some(d) => aliased_def(d, alias),
        None => crate::validate::computed_column(alias, idx, bound.infer_type_with(&|r| leaf.type_of(r))),
    };
    Ok(ProjEntry {
        expr: bound,
        out: HirCol::new(ids.next(), out_def),
    })
}

/// The declared type of a leaf reference — the env column's type code, or the
/// bound subquery's contributed value type (a computed projection may embed a
/// subquery leaf before decorrelation substitutes it).
pub(crate) fn type_of(env: &[HirCol], r: &HirRef) -> TypeCode {
    match r {
        HirRef::Col(id) => hircol_of(env, *id).def.type_code,
        HirRef::Subquery(sq) => sq.value_type(),
    }
}

/// What a leaf does with a subquery node it meets.
enum SubPolicy<'a> {
    /// Bind it in place (the subquery-carrying single-table linear body).
    Bind(&'a dyn Fn(&Expr) -> Result<HirExpr, GnitzSqlError>),
    /// No subquery here: the per-kind rejection, named with this leaf's clause.
    PerKind,
    /// A more specific reason than "not here", stated verbatim: the per-kind
    /// wording would contradict itself where the problem is nesting, not placement.
    Reject(&'static str),
}

/// The name-resolution leaf for every HIR body. One relation is a scope with
/// `relations.len() == 1`, so the linear and join bodies resolve through one
/// rule; only the clause and the subquery policy differ.
///
/// This leaf rejects every aggregate, because a Simple body's aggregates route
/// to the GroupBy builder instead.
struct ScopeLeaf<'a> {
    scope: &'a JoinScope,
    /// Names this leaf's function and subquery rejections, and the "not a column
    /// reference" one. Scope-resolution errors ("column 'x' not found in any
    /// table") keep their own wording.
    clause: &'static str,
    sub: SubPolicy<'a>,
}

impl ItemLeaf for ScopeLeaf<'_> {
    /// The columns in scope, in relation order.
    fn env(&self) -> &[HirCol] {
        &self.scope.combined
    }
    fn expands_wildcard(&self) -> bool {
        true
    }
}

impl LeafBinder<HirRef> for ScopeLeaf<'_> {
    /// A qualified / unqualified / parenthesized column reference → its `ColId`.
    /// Peels like `single_relation_col_name`, which it cannot reuse: a qualified
    /// reference needs both of its parts.
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        let id = match peel_nested(e) {
            Expr::Identifier(id) => self.scope.resolve_unqualified(&id.value)?,
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                self.scope.resolve_qualified(&parts[0].value, &parts[1].value)?
            }
            _ => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "{}: only column references supported",
                    self.clause
                )))
            }
        };
        Ok(BExpr::ColRef(HirRef::Col(id)))
    }

    fn bind_function(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        // Classify first, so an unknown or malformed call is named as such —
        // reaching past it means the name really is an aggregate.
        classify_agg_call(f)?;
        Err(clause_error(
            self.clause,
            GnitzSqlError::Unsupported("aggregate functions are not allowed here".to_string()),
        ))
    }

    fn is_nullable(&self, r: &HirRef) -> bool {
        hir_ref_nullable(self.env(), r)
    }

    fn bind_subquery(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        match self.sub {
            SubPolicy::Bind(f) => f(e),
            SubPolicy::PerKind => Err(clause_error(
                self.clause,
                crate::bind::structural::unsupported_subquery(e),
            )),
            SubPolicy::Reject(m) => Err(GnitzSqlError::Unsupported(m.to_string())),
        }
    }
}

/// Whether a leaf reference over `env` can be NULL: a column by its definition,
/// a subquery by its shape (an EXISTS/IN test or a COUNT never is).
pub(crate) fn hir_ref_nullable(env: &[HirCol], r: &HirRef) -> bool {
    match r {
        HirRef::Col(id) => hircol_of(env, *id).def.is_nullable,
        HirRef::Subquery(s) => !s.never_null(),
    }
}

// ── Subquery binding (EXISTS/IN, scalar aggregate, ANY/ALL) ──────────────────────
//
// A subquery-carrying single-table linear body binds in ONE pass over the real
// `bind_structural`, so every desugar/fold the structural recursion performs —
// COALESCE truncation at a never-NULL COUNT, provably-non-null elision, CASE
// short-circuit — is reached for free. The body leaf's `SubPolicy::Bind` binds
// each subquery node to a `HirRef::Subquery` leaf where the walk meets it,
// reaching the snapshot and the binder through a `RefCell` (the `LeafBinder`
// methods take `&self`). Decorrelation (`hir::rewrite`) consumes the leaves.

/// Everything a subquery bind resolves against: the body bind's own context,
/// plus the outer scope it correlates to.
struct SubCtx<'a, 'c, 'b> {
    cx: &'a mut BindCx<'c, 'b>,
    outer_env: &'a [HirCol],
    outer_alias: &'a str,
}

/// Bind a subquery-carrying single-table linear body to `Project(Filter?(Get))`.
/// One pass: each subquery binds where `bind_structural` meets it, so the WHERE
/// and the projection are walked exactly once and in source order.
///
/// The `RefCell` is `LeafBinder`'s `&self`; nothing re-enters it, since a nested
/// subquery is rejected by the inner correlation leaf.
fn bind_linear_subquery_body(
    cx: &mut BindCx<'_, '_>,
    select: &Select,
    get: Rc<RelExpr>,
    scope: &JoinScope,
    outer_alias: &str,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let ids = cx.ids;
    let sub = RefCell::new(SubCtx {
        cx,
        outer_env: &scope.combined,
        outer_alias,
    });
    let bind_sub = |e: &Expr| bind_one_subquery(&mut sub.borrow_mut(), e);
    let leaf = ScopeLeaf {
        scope,
        clause: "CREATE VIEW",
        sub: SubPolicy::Bind(&bind_sub),
    };
    bind_body_suffix(ids, select, get, &leaf, "CREATE VIEW projection")
}

/// The resolved inner relation of a subquery: `Filter?(Get)` with the inner-local
/// WHERE fused, plus the mixed-scope correlation conjuncts (over the outer ∪ inner
/// `ColId` space) that become the decorrelated join's ON.
struct InnerResolved<'e> {
    rel: Rc<RelExpr>,
    inner_cols: Vec<HirCol>,
    /// The inner relation's effective alias — what a qualified reference to an
    /// inner column must name.
    inner_alias: String,
    inner_select: &'e Select,
    correlation: Vec<HirExpr>,
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
    let inner_get = resolve_relation(cx.cx, &inner_name)?;
    let inner_cols = inner_get.cols();

    let mut local_preds: Vec<HirExpr> = Vec::new();
    let mut correlation: Vec<HirExpr> = Vec::new();
    if let Some(where_expr) = &inner_select.selection {
        // The (outer, inner) scope the split resolves against — built here, and
        // read nowhere else: a subquery with no WHERE has nothing to split.
        let mut scope = JoinScope::new();
        scope.push(outer_alias, outer_env.to_vec());
        scope.push(&inner_alias, inner_cols.clone());
        let leaf = ScopeLeaf {
            scope: &scope,
            clause: "subquery",
            sub: SubPolicy::Reject(
                "nested subqueries inside an EXISTS/IN subquery are not supported; compose via views",
            ),
        };
        for bound in bind_conjuncts(where_expr, &leaf)? {
            let (mut has_outer, mut has_inner) = (false, false);
            bound.for_each_ref(&mut |r| {
                if let HirRef::Col(id) = r {
                    has_outer |= col_by_id(outer_env, *id).is_some();
                    has_inner |= col_by_id(&inner_cols, *id).is_some();
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
        inner_alias,
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
        Expr::InSubquery { expr, subquery, negated } => bind_exists_sub(cx, subquery, Some(expr), *negated),
        Expr::Subquery(q) => bind_scalar_sub(cx, q),
        Expr::AnyOp { left, compare_op, right, .. } => bind_quantifier_sub(cx, left, compare_op, right, true),
        Expr::AllOp { left, compare_op, right } => bind_quantifier_sub(cx, left, compare_op, right, false),
        other => Err(GnitzSqlError::Internal(format!(
            "bind_one_subquery on a non-subquery node: {other:?}"
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
    let (outer_env, outer_alias) = (cx.outer_env, cx.outer_alias);
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
            outer_alias,
            "IN (SELECT …): the outer operand must be a plain column",
        )?;
        let inner_proj = single_projection_expr(
            ir.inner_select,
            "IN (SELECT …): the subquery must select exactly one plain column",
        )?;
        let inner = bind_plain_col(
            inner_proj,
            &ir.inner_cols,
            &ir.inner_alias,
            "IN (SELECT …): the subquery must select exactly one plain column",
        )?;
        let nullable = hircol_of(outer_env, outer).def.is_nullable || hircol_of(&ir.inner_cols, inner).def.is_nullable;
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
    let ids = cx.cx.ids;
    let ir = resolve_inner(cx, q)?;
    let err = "a scalar subquery must be a single aggregate over its correlation group";
    let proj_expr = single_projection_expr(ir.inner_select, err)?;
    let (func, arg) = classify_scalar_agg(proj_expr, &ir.inner_cols, &ir.inner_alias, err)?;
    Ok(BExpr::ColRef(scalar_leaf(ids, ir, func, arg)?))
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
    // `less` is whether the operator points at the low end, which decides the
    // MIN/MAX below; the equality forms are IN/NOT IN and never reach it.
    let (bop, less) = match (is_any, compare_op) {
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
        (_, BinaryOperator::Lt) => (BinOp::Lt, true),
        (_, BinaryOperator::LtEq) => (BinOp::Le, true),
        (_, BinaryOperator::Gt) => (BinOp::Gt, false),
        (_, BinaryOperator::GtEq) => (BinOp::Ge, false),
        _ => {
            return Err(GnitzSqlError::Unsupported(
                "only range (<, <=, >, >=), `= ANY`, and `<> ALL` quantified comparisons are supported".into(),
            ))
        }
    };
    // x < ANY ⟺ < MAX; x > ANY ⟺ > MIN; x < ALL ⟺ < MIN; x > ALL ⟺ > MAX.
    let agg_func = if is_any == less { AggFunc::Max } else { AggFunc::Min };
    let (ids, outer_env, outer_alias) = (cx.cx.ids, cx.outer_env, cx.outer_alias);
    let ir = resolve_inner(cx, q)?;
    let err = "a range ANY/ALL subquery must select a single non-nullable column";
    let proj_expr = single_projection_expr(ir.inner_select, err)?;
    let inner_col = bind_plain_col(proj_expr, &ir.inner_cols, &ir.inner_alias, err)?;
    if hircol_of(&ir.inner_cols, inner_col).def.is_nullable {
        return Err(GnitzSqlError::Unsupported(
            "a range ANY/ALL subquery's column must be NOT NULL — a NULL in the set makes ANY/ALL \
             three-valued in a way the MIN/MAX rewrite cannot reproduce"
                .into(),
        ));
    }
    let correlated = !ir.correlation.is_empty();
    let m_leaf = scalar_leaf(ids, ir, agg_func, Some(inner_col))?;
    let outer_scope = JoinScope::single(outer_alias, outer_env.to_vec());
    let outer_leaf = ScopeLeaf {
        scope: &outer_scope,
        clause: "CREATE VIEW",
        sub: SubPolicy::PerKind,
    };
    let x = bind_structural(left, &outer_leaf)?;
    let cmp = BExpr::BinOp(Box::new(x), bop, Box::new(BExpr::ColRef(m_leaf.clone())));
    let value = if correlated {
        // ANY over ∅ = FALSE, ALL over ∅ = TRUE — the LEFT join sets m = NULL for
        // an empty group, so the explicit null test makes the edge a definite
        // constant (exact under negation).
        let m_test = BExpr::NullTest {
            inner: Box::new(BExpr::ColRef(m_leaf)),
            want_null: !is_any,
        };
        let op = if is_any { BinOp::And } else { BinOp::Or };
        BExpr::BinOp(Box::new(m_test), op, Box::new(cmp))
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

/// Resolve `e` against `env` as a bare column reference, returning its `ColId`.
/// Every rejection is `err`: the caller's surface states what shape it needed,
/// which is more use here than "column 'x' not found".
fn bind_plain_col(e: &Expr, env: &[HirCol], alias: &str, err: &str) -> Result<ColId, GnitzSqlError> {
    single_relation_col_idx(env.iter().map(|c| &c.def), alias, e)
        .map(|i| env[i].id)
        .map_err(|_| GnitzSqlError::Unsupported(err.into()))
}

/// Classify a scalar subquery's single-aggregate projection into `(func, arg)`.
fn classify_scalar_agg(
    e: &Expr,
    inner_cols: &[HirCol],
    inner_alias: &str,
    non_agg: &str,
) -> Result<(AggFunc, Option<ColId>), GnitzSqlError> {
    let Expr::Function(f) = peel_nested(e) else {
        return Err(GnitzSqlError::Unsupported(non_agg.into()));
    };
    if !is_agg_call(f) {
        return Err(GnitzSqlError::Unsupported(non_agg.into()));
    }
    let (func, arg_expr) = classify_agg_call(f)?;
    let arg = match arg_expr {
        Some(a) => Some(bind_plain_col(
            a,
            inner_cols,
            inner_alias,
            "a scalar subquery aggregate must be over a plain column or `*`",
        )?),
        None => None,
    };
    Ok((func, arg))
}

/// The `Reduce` group columns of a scalar/quantifier subquery: the inner-side
/// `ColId` of each equality correlation conjunct. A range or non-equality
/// correlation is rejected (aggregation needs an equality GROUP BY key).
fn scalar_group_cols(correlation: &[HirExpr], inner_cols: &[HirCol]) -> Result<Vec<ColId>, GnitzSqlError> {
    let mut cols = Vec::with_capacity(correlation.len());
    for conj in correlation {
        if let BExpr::BinOp(l, BinOp::Eq, r) = conj {
            if let (Some(a), Some(b)) = (as_col(l), as_col(r)) {
                match (col_by_id(inner_cols, a).is_some(), col_by_id(inner_cols, b).is_some()) {
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

/// The single construction site of `SubqueryKind::Scalar`, which
/// [`SubqueryRef::scalar_agg`] reads back as an invariant: the correlation's
/// inner-side columns are the reduce's GROUP BY, and its one aggregate is minted
/// over the inner scope. The finalize composite is applied later by
/// decorrelation over the raw reduce output.
fn scalar_leaf(
    ids: &ColIdGen,
    ir: InnerResolved<'_>,
    func: AggFunc,
    arg: Option<ColId>,
) -> Result<HirRef, GnitzSqlError> {
    let group_cols = scalar_group_cols(&ir.correlation, &ir.inner_cols)?;
    let agg = HirAgg::new(ids, func, arg, &ir.inner_cols, group_cols.is_empty())?;
    Ok(HirRef::Subquery(Box::new(SubqueryRef {
        kind: SubqueryKind::Scalar,
        rel: RelExpr::reduce(ir.rel, group_cols, vec![agg]),
        correlation: ir.correlation,
        in_pair: None,
    })))
}

// ── FROM-join binding ──────────────────────────────────────────────────────────

/// Fold one join step onto the accumulated left input: resolve the right
/// relation, derive the step's ON conjuncts from its constraint, merge away the
/// duplicate copy of each `USING` / `NATURAL` column, and widen the scope by the
/// step's null semantics.
fn fold_join_step(
    cx: &mut BindCx<'_, '_>,
    scope: &mut JoinScope,
    left: Rc<RelExpr>,
    relation: &TableFactor,
    (keys, kind): (JoinKeys<'_>, JoinType),
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let (right_src, ralias, rcols) = resolve_table_factor(cx, relation)?;

    // Before `scope.push` below: a `USING`/`NATURAL` name pairs against the left
    // side alone, where an `ON` (bound after the push) sees both sides.
    let pairs = match &keys {
        JoinKeys::Using(cols) => {
            let mut names = Vec::with_capacity(cols.len());
            for c in *cols {
                names.push(crate::ast_util::extract_name(c, "JOIN USING")?);
            }
            crate::validate::reject_duplicate_names(names.iter().map(String::as_str), "JOIN USING")?;
            merge_pairs(scope, &rcols, &names, "USING")?
        }
        JoinKeys::Natural => {
            // SQL's rule: no shared name makes the step keyless, i.e. a CROSS
            // JOIN. The keyless guard decides it like any other keyless step.
            merge_pairs(scope, &rcols, &scope.shared_names(&rcols), "NATURAL")?
        }
        JoinKeys::On(_) | JoinKeys::None => Vec::new(),
    };

    scope.push(&ralias, rcols);

    // Every other form states its keys as `pairs`, empty for a keyless step — so a
    // step with no constraint of its own needs no arm, and the WHERE may key it.
    let on = match &keys {
        JoinKeys::On(e) => bind_conjuncts(
            e,
            &ScopeLeaf {
                scope,
                clause: "JOIN ON",
                sub: SubPolicy::PerKind,
            },
        )?,
        _ => pairs
            .iter()
            .map(|&(l, r)| {
                BExpr::BinOp(
                    Box::new(BExpr::ColRef(HirRef::Col(l))),
                    BinOp::Eq,
                    Box::new(BExpr::ColRef(HirRef::Col(r))),
                )
            })
            .collect(),
    };

    // The merged column's value is the PRESERVED side's copy, so the other side's
    // stops answering an unqualified reference and leaves `*`. `alias.col` still
    // reaches it — the two are distinct columns of the join output, and only the
    // one name they share was ambiguous.
    for &(l, r) in &pairs {
        scope.merge_away(if kind == JoinType::Right { l } else { r });
    }

    let out = RelExpr::join(left, right_src, kind, on, None);
    // Reflect this step's null-widening back into the scope so a later ON /
    // the WHERE / the projection resolve against the widened nullability.
    scope.widen_step(kind);
    Ok(out)
}

/// The `(left col, right col)` pair for each merged column name: the left copy
/// resolved against the scope as it stands, the right copy by name within the
/// incoming relation. `clause` names the surface for the error.
fn merge_pairs(
    scope: &JoinScope,
    rcols: &[HirCol],
    names: &[String],
    clause: &str,
) -> Result<Vec<(ColId, ColId)>, GnitzSqlError> {
    let mut pairs = Vec::with_capacity(names.len());
    for name in names {
        // `?` first: a name the left side carries twice is ambiguous, and the
        // scope's own wording says so — reporting it as "not found" would send
        // the reader looking for a column that is right there, twice.
        let l = scope.find_unqualified(name)?.ok_or_else(|| {
            GnitzSqlError::Bind(format!(
                "JOIN {clause}: column '{name}' not found on the left of the join"
            ))
        })?;
        let idx = find_unique_column(rcols.iter().map(|c| &c.def), name)?.ok_or_else(|| {
            GnitzSqlError::Bind(format!(
                "JOIN {clause}: column '{name}' not found on the right of the join"
            ))
        })?;
        pairs.push((l, rcols[idx].id));
    }
    Ok(pairs)
}

/// The name-resolution scope of a FROM-join body: all in-scope (null-widened)
/// `HirCol`s in relation order, plus each relation's alias (as written) and span.
/// Resolves a qualified or unqualified reference to a `ColId`, raising the same
/// not-found and ambiguity messages the single-relation binder does.
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

    /// The scope of one relation — a `new` plus one `push`.
    fn single(alias: &str, cols: Vec<HirCol>) -> Self {
        let mut scope = JoinScope::new();
        scope.push(alias, cols);
        scope
    }

    /// Mark the non-preserved side's copy of a `USING` / `NATURAL` column: it
    /// stops answering an unqualified name and leaves `*`, but stays reachable as
    /// `alias.col`.
    fn merge_away(&mut self, id: ColId) {
        for c in self.combined.iter_mut().filter(|c| c.id == id) {
            c.merged = true;
        }
    }

    /// The visible column names this scope currently answers unqualified — what
    /// `NATURAL` intersects the incoming relation's names against. Hidden slots
    /// are not names a user can write, so they pair with nothing.
    fn shared_names(&self, rcols: &[HirCol]) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for c in self.combined.iter().filter(|c| !c.def.is_hidden && !c.merged) {
            // A NAME, once. Two like-named visible left columns are one name to
            // pair on, and `merge_pairs` is what reports it as ambiguous; listing
            // it twice would pair and merge the same column twice.
            if wildcard_name_is_visible(rcols.iter().map(|r| &r.def), &c.def.name)
                && !names.iter().any(|n| n.eq_ignore_ascii_case(&c.def.name))
            {
                names.push(c.def.name.clone());
            }
        }
        names
    }

    fn push(&mut self, alias: &str, cols: Vec<HirCol>) {
        let start = self.combined.len();
        self.combined.extend(cols);
        self.relations.push((alias.to_string(), start..self.combined.len()));
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
        let (_, span) = self
            .relations
            .iter()
            .find(|(a, _)| a.eq_ignore_ascii_case(alias))
            .ok_or_else(|| GnitzSqlError::Bind(format!("table alias '{alias}' not found")))?;
        let cols = self.rel_cols(span);
        let idx = find_unique_column(cols.iter().map(|c| &c.def), name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found in table '{alias}'")))?;
        Ok(cols[idx].id)
    }

    /// Look up an unqualified reference across the whole scope — one lookup, so a
    /// name two relations both carry is the same ambiguity as one relation carrying
    /// it twice. Merged columns are skipped, which is what makes a merged name
    /// resolve rather than collide; absence is `Ok(None)` so a caller can word it.
    fn find_unqualified(&self, name: &str) -> Result<Option<ColId>, GnitzSqlError> {
        let visible: Vec<&HirCol> = self.combined.iter().filter(|c| !c.merged).collect();
        Ok(find_unique_column(visible.iter().map(|c| &c.def), name)?.map(|i| visible[i].id))
    }

    fn resolve_unqualified(&self, name: &str) -> Result<ColId, GnitzSqlError> {
        self.find_unqualified(name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("column '{name}' not found in any table")))
    }
}

// ── GROUP BY / aggregate / HAVING binding ────────────────────────────────────────

/// The columns a grouped query computes below its reduce: a GROUP BY key or an
/// aggregate argument that is not already a bare column reference becomes one
/// column of a `Project` under the `Reduce`, so the `Reduce` takes plain `ColId`s.
/// `lower_reduce` emits that `Project` as a map in the reduce's own circuit, so it
/// costs no second relation.
///
/// One column per distinct expression, keyed by its **bound** form — which is what
/// makes `GROUP BY t.a + b` and `SELECT a + b` one expression, and folds `Nested`
/// away so `SUM((a * b))` and `SUM(a * b)` reach one aggregate.
struct PreMap<'a> {
    ids: &'a ColIdGen,
    /// The reduce input's columns: the source env, then one per materialized
    /// expression — so `env[..env.len() - extra.len()]` is the source env, which
    /// [`Self::source_cols`] hands back. Every `ColId` this mints resolves here.
    env: Vec<HirCol>,
    /// The materialized columns and the expressions computing them — also the
    /// memo, since `extra[i].expr` is what `extra[i].out.id` holds.
    extra: Vec<ProjEntry>,
}

impl<'a> PreMap<'a> {
    fn new(ids: &'a ColIdGen, env: Vec<HirCol>) -> Self {
        PreMap { ids, env, extra: Vec::new() }
    }

    /// The column `e` names, materializing one on first sight.
    fn column_for(&mut self, e: &Expr, leaf: &ScopeLeaf<'_>) -> Result<ColId, GnitzSqlError> {
        let bound = bind_structural(e, leaf)?;
        if let Some(id) = find_bound(&self.extra, &bound) {
            return Ok(id);
        }
        let ty = bound.infer_type_with(&|r: &HirRef| type_of(&self.env, r));
        // Declared nullable unconditionally (a computed value can be NULL: `a / 0`),
        // so an aggregate over one takes the null-skipping shape. Hidden because
        // only the expression that minted it may reach it.
        let out = HirCol::new(
            self.ids.next(),
            ColumnDef::new(format!("_pre{}", self.extra.len()), ty, true).hidden(),
        );
        let id = out.id;
        self.env.push(out.clone());
        self.extra.push(ProjEntry { expr: bound, out });
        Ok(id)
    }

    /// The scope this pre-map was built over — the body's *resolved* columns, so
    /// a derived table's `AS d(col…)` aliases are the names, where `input.cols()`
    /// would give the subtree's own.
    fn source_cols(&self) -> &[HirCol] {
        &self.env[..self.env.len() - self.extra.len()]
    }

    /// The reduce's input: `input` when nothing was materialized, else an identity
    /// `Project` over it carrying the materialized columns as extra items.
    fn reduce_input(&self, input: Rc<RelExpr>) -> Rc<RelExpr> {
        if self.extra.is_empty() {
            return input;
        }
        let mut items = RelExpr::passthrough_items(self.source_cols().iter().cloned());
        items.extend(self.extra.iter().cloned());
        RelExpr::project(input, items)
    }
}

/// A rejection raised while binding one clause, named with it. The message already
/// says what is wrong with the reference; only the clause is added.
fn clause_error(clause: &str, e: GnitzSqlError) -> GnitzSqlError {
    let prefixed = |m: String| format!("{clause}: {m}");
    match e {
        GnitzSqlError::Bind(m) => GnitzSqlError::Bind(prefixed(m)),
        GnitzSqlError::Plan(m) => GnitzSqlError::Plan(prefixed(m)),
        GnitzSqlError::Unsupported(m) => GnitzSqlError::Unsupported(prefixed(m)),
        // Not a verdict about the written reference — a control signal or an
        // internal break, which must reach its handler unedited.
        other => other,
    }
}

/// The column already holding `bound`'s value: its own when it is a bare column
/// reference, else the pre-map column computing an equal expression. One rule for
/// minting and for looking up, so an expression cannot resolve to one column in
/// the reduce and another above it.
fn find_bound(extra: &[ProjEntry], bound: &HirExpr) -> Option<ColId> {
    as_col(bound).or_else(|| extra.iter().find(|x| &x.expr == bound).map(|x| x.out.id))
}

/// Resolve the GROUP BY list to the `ColId`s the reduce groups by: a 1-based
/// position names its SELECT item, a bare reference is its own column, and
/// anything computed is materialized below the reduce by `pre`.
fn resolve_group_cols(
    select: &Select,
    leaf: &ScopeLeaf<'_>,
    pre: &mut PreMap<'_>,
) -> Result<Vec<ColId>, GnitzSqlError> {
    let mut cols = Vec::new();
    for ge in group_by_exprs(select)? {
        let target = group_by_target(ge, select)?;
        // The key binds through the FROM leaf, whose rejection names the column but
        // not the clause it was written in.
        let id = pre.column_for(target, leaf).map_err(|e| clause_error("GROUP BY", e))?;
        // A key the pre-map minted is hidden and has no name the user wrote, so
        // it is described rather than named; a source column names itself.
        let def = &hircol_of(&pre.env, id).def;
        if def.is_hidden {
            if def.type_code.is_float() {
                return Err(reject_float_key_of("a float-valued expression", "GROUP BY"));
            }
        } else {
            reject_float_key(def, "GROUP BY")?;
        }
        cols.push(id);
    }
    Ok(cols)
}

/// Collect the aggregate calls referenced in an expression, appending each not
/// already present (deduped by `(func, arg)`). Recurses through non-aggregate
/// operands, mirroring the binder's node set — what this walk reaches is exactly
/// what the reduce materializes.
fn collect_aggs(
    expr: &Expr,
    leaf: &ScopeLeaf<'_>,
    is_global: bool,
    aggs: &mut Vec<HirAgg>,
    pre: &mut PreMap<'_>,
) -> Result<(), GnitzSqlError> {
    for_each_agg_call(expr, &mut |f| -> Result<(), GnitzSqlError> {
        let (func, arg_expr) = classify_agg_call(f)?;
        let arg = match arg_expr {
            Some(e) => Some(pre.column_for(e, leaf)?),
            None => None,
        };
        if !aggs.iter().any(|a| a.func == func && a.arg == arg) {
            aggs.push(HirAgg::new(pre.ids, func, arg, &pre.env, is_global)?);
        }
        Ok(())
    })?;
    Ok(())
}

/// Bind an ad-hoc single-relation grouped body to
/// `Project(Filter_having?(Reduce(PreMap?(Get))))` — the same suffix, over the
/// same leaf, that a grouped `CREATE VIEW` body binds. The fold lowering
/// (`dml::group_by`) reads the result instead of running a second binder, which
/// is what makes one written statement mean one thing on both paths.
///
/// The WHERE is deliberately *not* bound here: the ad-hoc read path carries it
/// as the `ReadSpec` predicate, with its scan bound extracted, so binding it
/// again would compile it twice. Nothing in the grouped suffix reads it.
pub(crate) fn bind_adhoc_grouped(
    ids: &ColIdGen,
    select: &Select,
    schema: Arc<Schema>,
    alias: &str,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let (source, scope) = adhoc_source(ids, schema, alias);
    let leaf = ScopeLeaf {
        scope: &scope,
        clause: "SELECT",
        sub: SubPolicy::PerKind,
    };
    bind_grouped_suffix(ids, select, source, &leaf)
}

/// Bind an ad-hoc single-relation `SELECT DISTINCT` body to `Distinct(Project(Get))`
/// — the same tree, over the same leaf, that a `SELECT DISTINCT` `CREATE VIEW`
/// body binds, so one written projection means one thing on both paths (a
/// computed item included). The WHERE is not bound here, for the reason
/// [`bind_adhoc_grouped`] states.
pub(crate) fn bind_adhoc_distinct(
    ids: &ColIdGen,
    select: &Select,
    schema: Arc<Schema>,
    alias: &str,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let (source, scope) = adhoc_source(ids, schema, alias);
    let leaf = ScopeLeaf {
        scope: &scope,
        clause: "SELECT DISTINCT",
        sub: SubPolicy::PerKind,
    };
    let items = bind_projection(&select.projection, &leaf, ids, "SELECT DISTINCT")?;
    Ok(RelExpr::distinct(RelExpr::project(source, items)))
}

/// The `Get` an ad-hoc body binds over and the one-relation scope its names
/// resolve through. `tid` is the lowering's business, not the bind's — an ad-hoc
/// read names its relation in the `ReadSpec` — so the `Get` is minted without one.
fn adhoc_source(ids: &ColIdGen, schema: Arc<Schema>, alias: &str) -> (Rc<RelExpr>, JoinScope) {
    let source = RelExpr::get(ids, 0, schema, None);
    let scope = JoinScope::single(alias, source.cols());
    (source, scope)
}

/// Bind the GROUP BY / aggregate / HAVING suffix over `input` (the `Filter?(source)`
/// tree built by the FROM binder), producing
/// `Project(Filter_having?(Reduce(PreMap?(input))))` — through the window
/// desugar when the SELECT list carries a window call.
fn bind_grouped_suffix(
    ids: &ColIdGen,
    select: &Select,
    input: Rc<RelExpr>,
    leaf: &ScopeLeaf<'_>,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let mut pre = PreMap::new(ids, leaf.env().to_vec());
    let group_cols = resolve_group_cols(select, leaf, &mut pre)?;
    let is_global = group_cols.is_empty();

    // Aggregates from the projection ∪ HAVING ∪ QUALIFY ∪ the WINDOW clause,
    // deduped by `(func, arg)`. An inline window specification's keys are
    // operands of the call, which the walk reaches on its own.
    let mut aggs: Vec<HirAgg> = Vec::new();
    let named_keys = select.named_window.iter().flat_map(|d| match &d.1 {
        NamedWindowExpr::WindowSpec(s) => window_spec_keys(s).collect::<Vec<_>>(),
        NamedWindowExpr::NamedWindow(_) => Vec::new(),
    });
    for expr in select
        .projection
        .iter()
        .filter_map(projection_item_expr)
        .chain(select.having.iter())
        .chain(select.qualify.iter())
        .chain(named_keys)
    {
        collect_aggs(expr, leaf, is_global, &mut aggs, &mut pre)?;
    }

    let mut rel = RelExpr::reduce(pre.reduce_input(input), group_cols.clone(), aggs.clone());

    // The pre-map's columns, extended with each aggregate's raw value and
    // companion: everything a `ColId` in an expression over the reduce output can
    // resolve to when it is typed.
    let PreMap { mut env, extra, .. } = pre;
    for a in &aggs {
        env.push(a.out.clone());
        env.extend(a.companion.iter().cloned());
    }
    // One leaf per clause over the same grouped relation: only the wording of a
    // rejection differs, so only the clause does.
    let grouped = |clause| GroupedLeaf {
        leaf,
        env: &env,
        group_cols: &group_cols,
        aggs: &aggs,
        extra: &extra,
        clause,
    };

    // HAVING → a Filter over the raw reduce output.
    if let Some(having) = &select.having {
        let hexpr = bind_structural(having, &grouped("HAVING"))?;
        rel = RelExpr::filter(rel, vec![hexpr]);
    }

    let select_leaf = grouped("GROUP BY SELECT");
    if select_has_window(select) {
        return super::window::bind_window_final(ids, select, rel, &select_leaf, "GROUP BY");
    }
    // The dup-name guard is `lower_reduce`'s: its output carries the group
    // columns, including ones this projection never named.
    let items = bind_projection(&select.projection, &select_leaf, ids, "GROUP BY")?;
    Ok(RelExpr::project(rel, items))
}

/// The leaf for an expression over the grouped relation — HAVING and the finalize
/// SELECT list alike. A GROUP BY key binds to the column holding it and an
/// aggregate call to its finalize composite; nothing else is available.
struct GroupedLeaf<'a> {
    leaf: &'a ScopeLeaf<'a>,
    /// The typing table for a `ColId` an expression over the reduce output holds:
    /// the reduce's input columns plus each aggregate's raw value and companion.
    /// Names resolve through `leaf`, never against this list.
    env: &'a [HirCol],
    group_cols: &'a [ColId],
    aggs: &'a [HirAgg],
    /// The pre-map's materialized columns, so a written expression matches the
    /// column the reduce already computes for it.
    extra: &'a [ProjEntry],
    /// The clause being bound — this leaf serves HAVING and the SELECT list, and
    /// every rejection below names it.
    clause: &'static str,
}

impl GroupedLeaf<'_> {
    /// The group-key column an expression names, if it names one. Binding through
    /// the **body's own leaf** is what makes one rule serve both spellings: a bare
    /// reference binds to its own column, a written key to the pre-map column
    /// holding it, and `l.v` in a join resolves the way the body resolves it.
    fn group_key(&self, e: &Expr) -> Option<ColId> {
        let bound = bind_structural(e, self.leaf).ok()?;
        find_bound(self.extra, &bound).filter(|id| self.group_cols.contains(id))
    }

    fn find_agg(&self, f: &Function) -> Result<&HirAgg, GnitzSqlError> {
        let (func, arg_expr) = classify_agg_call(f)?;
        // Through the pre-map, so `SUM(a * b)` here names the column the reduce
        // already aggregates rather than a second one.
        let arg = match arg_expr {
            Some(e) => Some(find_bound(self.extra, &bind_structural(e, self.leaf)?).ok_or_else(|| {
                GnitzSqlError::Unsupported(format!("{}: unsupported aggregate argument {e}", self.clause))
            })?),
            None => None,
        };
        // Defensive on both paths: an aggregate reaching this leaf was collected
        // into the reduce first, so no SQL body resolves to the rejection.
        self.aggs
            .iter()
            .find(|a| a.func == func && a.arg == arg)
            .ok_or_else(|| {
                let name = arg.map_or("*", |id| hircol_of(self.env, id).def.name.as_str());
                GnitzSqlError::Bind(format!(
                    "{}: aggregate {func:?}({name}) could not be resolved",
                    self.clause
                ))
            })
    }
}

impl ItemLeaf for GroupedLeaf<'_> {
    fn env(&self) -> &[HirCol] {
        self.env
    }
    fn expands_wildcard(&self) -> bool {
        false
    }
    /// An item that *is* an aggregate call takes that aggregate's own name,
    /// type and nullability — kept out of the computed branch, which hardcodes
    /// nullability to `true` and would declare a COUNT column nullable.
    fn call_item(
        &self,
        f: &Function,
        alias: &Option<String>,
        idx: usize,
    ) -> Option<Result<(HirExpr, ColumnDef), GnitzSqlError>> {
        if !is_agg_call(f) {
            return None;
        }
        Some(self.find_agg(f).map(|agg| {
            let name = alias.clone().unwrap_or_else(|| default_agg_name(agg.func, idx));
            (
                agg.finalize(),
                ColumnDef::new(name, agg.view_type(), agg.view_nullable()),
            )
        }))
    }
}

impl LeafBinder<HirRef> for GroupedLeaf<'_> {
    /// A written GROUP BY key, wherever it appears: `(a + b) * 2` binds over
    /// `GROUP BY a + b`. With no computed key there is nothing to match, and
    /// `bind_column` answers the bare names on its own.
    fn bind_node(&self, e: &Expr) -> Option<HirExpr> {
        // `extra` also holds aggregate arguments, which are never group keys —
        // testing it alone would bind and discard the subtree at every node.
        if !self.extra.iter().any(|x| self.group_cols.contains(&x.out.id)) {
            return None;
        }
        Some(BExpr::ColRef(HirRef::Col(self.group_key(e)?)))
    }

    /// A group key binds to the column holding it; a name the body resolves but
    /// the grouping does not cover was written outside both, and anything the
    /// body itself refuses is that refusal, re-read as this clause's.
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        let bound = bind_structural(e, self.leaf).map_err(|err| clause_error(self.clause, err))?;
        match find_bound(self.extra, &bound).filter(|id| self.group_cols.contains(id)) {
            Some(id) => Ok(BExpr::ColRef(HirRef::Col(id))),
            None => Err(match single_relation_col_name(e) {
                Some(name) => GnitzSqlError::Plan(format!(
                    "{}: column '{name}' must appear in GROUP BY or an aggregate function",
                    self.clause
                )),
                None => GnitzSqlError::Unsupported(format!("{}: expected a group key or an aggregate", self.clause)),
            }),
        }
    }

    /// An aggregate call binds to its finalize composite. There is no
    /// non-aggregate arm: `find_agg` classifies first, and `classify_agg_call` is
    /// already that name's qualifier + unknown-name rejection.
    fn bind_function(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        Ok(self.find_agg(f)?.finalize())
    }
    fn is_nullable(&self, r: &HirRef) -> bool {
        hir_ref_nullable(self.env, r)
    }
}

// ── Set-operation binding ────────────────────────────────────────────────────────

/// Bind a set operation, binding both sides recursively; the `SetOp` constructor
/// pairs columns positionally and promotes cross-width types.
fn bind_set_op(
    cx: &mut BindCx<'_, '_>,
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
    // Exhaustive (no `_`): a set operator a future `sqlparser` adds stops the
    // build here rather than reaching `RelExpr::set_op` as a silent EXCEPT.
    let kind = match op {
        SetOperator::Union => SetOpKind::Union,
        SetOperator::Intersect => SetOpKind::Intersect,
        // MINUS is Oracle's spelling of EXCEPT, quantifier included.
        SetOperator::Except | SetOperator::Minus => SetOpKind::Except,
    };
    let all = matches!(quantifier, SetQuantifier::All);
    // Each side binds as any relational body — a plain SELECT, a join, a grouped
    // query, a nested set operation, or a derived table (all handled by
    // `bind_body` → `resolve_table_factor`).
    let left_rel = bind_body(cx, left)?;
    let right_rel = bind_body(cx, right)?;
    RelExpr::set_op(cx.ids, kind, all, left_rel, right_rel)
}

#[cfg(test)]
#[path = "tests/bind.rs"]
mod tests;
