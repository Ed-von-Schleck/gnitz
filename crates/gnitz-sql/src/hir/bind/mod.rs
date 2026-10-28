//! AST → HIR binding. `bind_body` resolves every name to an opaque `ColId`,
//! validates the honored clauses, and produces a logical `RelExpr` tree for any
//! relational body (linear / join / grouped / DISTINCT / set operation). The CTE
//! phase (`bind_ctes`) binds each CTE body to a shared subtree ahead of the
//! main bind and registers it by name, so a reference aliases it; a derived
//! table binds its subquery recursively to an inline subtree
//! (`resolve_table_factor`).
//!
//! The body's shape-specific parts are this module's children: `join` folds
//! the FROM clause's join steps, `subquery` binds the subqueries a linear body
//! reads, and `group` binds the GROUP BY / aggregate / HAVING suffix.

mod group;
mod join;
mod subquery;

use super::{
    as_col, col_by_id, ColId, ColIdGen, HirCol, HirExpr, JoinType, OrderCol, ProjEntry, RelExpr, SetOpKind,
    SubqueryKind, SubqueryRef,
};
use crate::ast_util::{
    alias_column_names, body_is_grouped, expand_wildcard_item, extract_table_ref, has_visible_column, is_agg_call,
    peel_nested, scalar_projection_item, select_is_distinct, single_part_ident, TableRef,
};
use crate::bind::apply_positional_aliases;
use crate::bind::{
    bind_conjuncts, bind_structural, find_unique_column, output_column, single_relation_col_idx, unsupported_subquery,
    Catalog, LeafBinder,
};
use crate::error::{derivation, reject_if, GnitzSqlError};
use crate::ir::{BExpr, BoundExpr, LeafTyping, BOOL};
use crate::rules::{canonical_user_name, reject_duplicate_names, require_class, ClassWant};
use crate::tail::{extract_limit, extract_offset, key_slots, order_exprs, parse_order_by, OrderKey};
use crate::validate::{
    reject_query_envelope_body, reject_unhonored_query_clauses, reject_unhonored_select_clauses, HonoredClauses,
    QueryEnvelope,
};
use gnitz_core::{RelDescriptor, Schema};
use gnitz_wire::{ColType, ColumnDef};
use group::bind_grouped_suffix;
use join::{fold_join_step, join_keys_and_type};
use sqlparser::ast::{
    Expr, JoinConstraint, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, SetOperator,
    SetQuantifier, TableFactor,
};
use sqlparser::tokenizer::Span;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;

/// Bind a query's CTEs ahead of its body. Each binds to one subtree registered
/// under its name, which the body (and later CTEs) read through an
/// [`RelExpr::Alias`] and the lowering cuts once. Scoping precedence as SQL
/// defines it: a CTE shadows a catalog name, and a later CTE sees earlier ones.
/// `WITH RECURSIVE` has no builder, and a CTE's trailing FROM is refused.
pub(super) fn bind_ctes(cx: &mut BindCx<'_>, query: &Query) -> Result<(), GnitzSqlError> {
    let Some(with) = &query.with else {
        return Ok(());
    };
    reject_if(with.recursive, "WITH", "RECURSIVE")?;
    for cte in &with.cte_tables {
        // `materialized` parses only under PostgreSqlDialect — always `None` here.
        let sqlparser::ast::Cte {
            alias,
            query,
            from,
            materialized: _,
            closing_paren_token: _,
        } = cte;
        let name = &alias.name.value;
        // A CTE name is a relation name later references resolve, so it is held
        // to the reserved-prefix rule every such name passes.
        let canonical = canonical_user_name(name)?;
        let ctx = format!("CTE '{name}'");
        reject_if(from.is_some(), &ctx, "a trailing FROM")?;
        let rel = bind_body(cx, reject_query_envelope_body(query, &ctx)?)?;
        let (rel, mut defs) = collapse_identity(rel);
        apply_positional_aliases(alias_column_names(&alias.columns, &ctx)?, defs.iter_mut(), &ctx)?;
        cx.ctes.insert(canonical, Cte { rel, defs });
    }
    Ok(())
}

/// A bound CTE: the shared subtree every reference aliases, and the column
/// defs a reference carries — the subtree's own, renamed by the SELECT list's
/// aliases and the CTE's positional ones.
struct Cte {
    rel: Rc<RelExpr>,
    defs: Vec<ColumnDef>,
}

/// A body that only renames its source's visible columns, in order, collapses
/// to that source under the new names, hidden columns included. Any other body
/// is returned as it is, under its own defs.
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

/// The view statement a body is bound for.
#[derive(Clone, Copy)]
pub(crate) struct ViewBody {
    /// `CREATE VIEW` or `ALTER VIEW`, so a rejection names the statement the user
    /// wrote.
    pub stmt: &'static str,
    /// The id of the view this statement supersedes, which the body may not read.
    pub replacing: Option<u64>,
}

/// One statement's bind state (its CTEs and body), shared by the whole recursion.
pub(super) struct BindCx<'c> {
    cat: &'c dyn Catalog,
    ids: &'c ColIdGen,
    surface: Surface,
    /// Each place the text names a catalog relation, in bind order.
    pub(super) refs: RefCell<Vec<BoundRef>>,
    /// The CTEs bound so far, by canonical (ASCII-lowercase) name.
    ctes: HashMap<String, Cte>,
}

impl<'c> BindCx<'c> {
    pub(super) fn new(cat: &'c dyn Catalog, ids: &'c ColIdGen, surface: Surface) -> Self {
        BindCx {
            cat,
            ids,
            surface,
            refs: RefCell::new(Vec::new()),
            ctes: HashMap::new(),
        }
    }
}

/// One place a body's text names a catalog relation: what a stored view
/// definition needs to write the body again under the names the catalog holds
/// later.
#[derive(Clone, Debug)]
pub(crate) struct BoundRef {
    pub(crate) tid: u64,
    /// The relation's name as written.
    pub(crate) name: String,
    /// Where the relation name lies in the statement text.
    pub(crate) name_span: Span,
    /// Where the declared alias's name lies; `None` without one.
    pub(crate) alias_span: Option<Span>,
    /// The name the body reads the relation by.
    pub(crate) alias: String,
    /// The relation's visible column names, which the body reads it by; `None`
    /// where the reference renames them with a column alias list of its own.
    pub(crate) columns: Option<Vec<String>>,
}

/// The subtree a relation name reads, under the column aliases the reference
/// declares: the CTE it names, else the catalog relation. Every relation a body
/// names in FROM resolves here.
fn resolve_table_ref(cx: &BindCx<'_>, tref: &TableRef<'_>) -> Result<(Rc<RelExpr>, Vec<HirCol>), GnitzSqlError> {
    let rel = resolve_relation(cx, tref)?;
    let mut cols = rel.cols();
    if !tref.columns.is_empty() {
        let ctx = format!("table '{}'", tref.alias);
        apply_positional_aliases(
            alias_column_names(tref.columns, &ctx)?,
            cols.iter_mut().map(|c| &mut c.def),
            &ctx,
        )?;
    }
    Ok((rel, cols))
}

/// The subtree a relation name reads: the CTE it names, else the catalog relation.
fn resolve_relation(cx: &BindCx<'_>, tref: &TableRef<'_>) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let name = &tref.name;
    // A CTE lives in no schema.
    let cte = (!name.is_qualified()).then(|| cx.ctes.get(name.name())).flatten();
    if let Some(cte) = cte {
        return Ok(RelExpr::alias(cx.ids, Rc::clone(&cte.rel), cte.defs.iter().cloned()));
    }
    let rel = cx.cat.probe_relation(name)?;
    let (want, op) = match cx.surface {
        Surface::Adhoc { op } => (ClassWant::Readable, op),
        // The replaced view is retracted in the same bundle, taking the body's input with it.
        Surface::View(ViewBody { stmt, replacing }) if replacing == Some(rel.tid) => {
            return Err(GnitzSqlError::Rejected(format!(
                "{stmt} '{name}' AS a query referencing the view itself is not supported"
            )))
        }
        Surface::View(v) => (ClassWant::ViewSource, v.stmt),
    };
    require_class(&rel, name, want, op)?;
    let columns = tref
        .columns
        .is_empty()
        .then(|| rel.schema.visible_columns().map(|(_, c)| c.name.clone()).collect());
    cx.refs.borrow_mut().push(BoundRef {
        tid: rel.tid,
        name: name.to_string(),
        name_span: tref.name_span,
        alias_span: tref.alias_span,
        alias: tref.alias.clone(),
        columns,
    });
    Ok(RelExpr::get(cx.ids, rel))
}

/// A view body's `ORDER BY … LIMIT n [OFFSET m]` tail — the top-N the body
/// maintains. Parsed once here so every body shape reads one rule: the two
/// clauses need each other (a view is unordered, and a LIMIT with no order names
/// no rows), and the counts are literals.
struct QueryTail<'a> {
    keys: Vec<OrderKey<'a>>,
    limit: u64,
    offset: u64,
}

impl<'a> QueryTail<'a> {
    fn parse(query: &'a Query, stmt: &str) -> Result<Option<Self>, GnitzSqlError> {
        let keys = parse_order_by(query.order_by.as_ref())?;
        let limit = extract_limit(query)?;
        match (keys.is_empty(), limit) {
            // The envelope guard waves the whole sink through to this parse, so
            // what this parse does not consume it must refuse — a bare `OFFSET`.
            (true, None) => {
                reject_if(query.limit_clause.is_some(), stmt, "OFFSET without ORDER BY … LIMIT")?;
                Ok(None)
            }
            (false, None) => Err(GnitzSqlError::Rejected(format!(
                "{stmt}: ORDER BY without LIMIT — a view holds an unordered set, so an order alone \
                 maintains nothing; add LIMIT n to maintain the top n rows"
            ))),
            (true, Some(_)) => Err(GnitzSqlError::Rejected(format!(
                "{stmt}: LIMIT without ORDER BY names no rows in particular; add ORDER BY"
            ))),
            (false, Some(0)) => Err(GnitzSqlError::Rejected(format!("{stmt}: LIMIT 0 selects nothing"))),
            (false, Some(limit)) => Ok(Some(QueryTail {
                keys,
                limit: limit as u64,
                offset: extract_offset(query)? as u64,
            })),
        }
    }

    /// The expression keys, in key order — what a body's projection places one
    /// hidden item each for.
    fn exprs(&self) -> Vec<&'a Expr> {
        order_exprs(&self.keys)
    }

    /// Wrap `rel` in the top-N this tail names. `placed` is the output slot of
    /// each *expression* key, parallel to [`Self::exprs`]; a positional key
    /// names a visible output column.
    fn wrap(&self, rel: Rc<RelExpr>, placed: &[usize]) -> Result<Rc<RelExpr>, GnitzSqlError> {
        let cols = rel.cols();
        let order = key_slots(&self.keys, cols.iter().map(|c| &c.def), placed.iter().copied())?
            .into_iter()
            .zip(&self.keys)
            .map(|(at, key)| OrderCol {
                col: cols[at].id,
                desc: key.desc,
                nulls_first: key.nulls_first,
            })
            .collect();
        Ok(RelExpr::top_n(rel, Vec::new(), order, self.limit, self.offset))
    }

    /// [`Self::wrap`] over a body whose keys can only be its output columns — a
    /// set operation or a parenthesized query, which has no scope of its own.
    fn wrap_by_output(&self, rel: Rc<RelExpr>, stmt: &str) -> Result<Rc<RelExpr>, GnitzSqlError> {
        let cols = rel.cols();
        let mut placed = Vec::new();
        for e in self.exprs() {
            match output_column(e, cols.iter().map(|c| &c.def))? {
                Some(at) => placed.push(at),
                None => {
                    return Err(GnitzSqlError::Rejected(format!(
                        "{stmt}: ORDER BY over a set operation names an output column or position"
                    )))
                }
            }
        }
        self.wrap(rel, &placed)
    }
}

/// Bind a whole view query: its envelope checked, its CTEs, its body, then the
/// `ORDER BY … LIMIT` tail as a top-N over it. A SELECT body places the keys in
/// its own scope; any other body orders by its output columns.
pub(super) fn bind_query(cx: &mut BindCx<'_>, query: &Query) -> Result<Rc<RelExpr>, GnitzSqlError> {
    reject_unhonored_query_clauses(query, QueryEnvelope::WithAndTail, cx.surface.stmt())?;
    bind_ctes(cx, query)?;
    let cx = &*cx;
    let tail = QueryTail::parse(query, cx.surface.stmt())?;
    match (query.body.as_ref(), tail) {
        (SetExpr::Select(select), tail) => {
            let order_exprs: Vec<&Expr> = tail.as_ref().map(QueryTail::exprs).unwrap_or_default();
            let (rel, placed) = bind_select(cx, select, &order_exprs)?;
            match tail {
                Some(t) => t.wrap(rel, &placed),
                None => Ok(rel),
            }
        }
        (body, None) => bind_body(cx, body),
        (body, Some(tail)) => {
            let rel = bind_body(cx, body)?;
            tail.wrap_by_output(rel, cx.surface.stmt())
        }
    }
}

/// Bind one query body — a single SELECT (linear / join / grouped / DISTINCT) or
/// a set operation whose sides bind recursively. A parenthesized side is a whole
/// `Query`, whose envelope is rejected before its body binds.
fn bind_body(cx: &BindCx<'_>, body: &SetExpr) -> Result<Rc<RelExpr>, GnitzSqlError> {
    match body {
        SetExpr::Select(select) => Ok(bind_select(cx, select, &[])?.0),
        SetExpr::SetOperation { op, set_quantifier, left, right } => bind_set_op(cx, *op, *set_quantifier, left, right),
        SetExpr::Query(q) => bind_body(cx, reject_query_envelope_body(q, "parenthesized query")?),
        _ => Err(GnitzSqlError::Rejected(format!(
            "{} only supports SELECT and set operations",
            cx.surface.stmt()
        ))),
    }
}

/// Resolve one FROM table factor to `(source subtree, alias, output cols)`. A
/// table name resolves via `resolve_relation` to a `Get`, a CTE name to an
/// `Alias` over its shared subtree; a derived table binds its subquery
/// recursively to an **inline** subtree (never a segment — single-use and
/// non-LATERAL, so uncorrelated) whose `AS d(col…)` aliases are applied to the
/// returned cols (same `ColId`s, overridden names) the caller pushes into its
/// scope/env. The subtree itself keeps its own names; a derived alias resolves
/// through the caller's scope, never the statement catalog.
fn resolve_table_factor(
    cx: &BindCx<'_>,
    factor: &TableFactor,
) -> Result<(Rc<RelExpr>, String, Vec<HirCol>), GnitzSqlError> {
    if let TableFactor::Derived { lateral, subquery, alias, sample } = factor {
        let Some(alias) = alias else {
            return Err(GnitzSqlError::Rejected(
                "a derived table (subquery in FROM) needs an alias".to_string(),
            ));
        };
        let ctx = format!("derived table '{}'", alias.name.value);
        reject_if(*lateral, &ctx, "LATERAL")?;
        // Silently dropping TABLESAMPLE would return all rows — a wrong result.
        reject_if(sample.is_some(), &ctx, "TABLESAMPLE")?;
        let body = reject_query_envelope_body(subquery, &ctx)?;
        let subtree = bind_body(cx, body)?;
        let mut cols = subtree.cols();
        apply_positional_aliases(
            alias_column_names(&alias.columns, &ctx)?,
            cols.iter_mut().map(|c| &mut c.def),
            &ctx,
        )?;
        return Ok((subtree, alias.name.value.clone(), cols));
    }
    let tref = extract_table_ref(factor, cx.cat.schema_name(), cx.surface.stmt())?;
    let (rel, cols) = resolve_table_ref(cx, &tref)?;
    Ok((rel, tref.alias, cols))
}

/// Bind one single-SELECT body to its projection over `Filter?(source)`, in the
/// body's shape: plain, DISTINCT, grouped or windowed. `source` is the FROM
/// relation or the left-deep fold of its join steps — one relation being the
/// same tree with no step. Steps fold in syntactic order (no reordering), so
/// `a LEFT JOIN b JOIN c` is `(a LEFT JOIN b) JOIN c`, and a comma binds
/// loosest: `FROM a JOIN b ON …, c` is `((a JOIN b) , c)`.
///
/// Conjuncts are placed as each join and filter is built — the step's into its
/// join, the WHERE's over the whole fold (`hir::place`).
pub(super) fn bind_select(
    cx: &BindCx<'_>,
    select: &Select,
    order_exprs: &[&Expr],
) -> Result<(Rc<RelExpr>, Vec<usize>), GnitzSqlError> {
    let (stmt, surface) = (cx.surface.stmt(), cx.surface);
    let grouped = body_is_grouped(select);
    let distinct = select_is_distinct(select);
    let honored = HonoredClauses::for_body(distinct);
    let honored = if surface.is_view() {
        honored.with_windows()
    } else {
        honored
    };
    reject_unhonored_select_clauses(select, honored, stmt)?;

    // The first FROM relation (a table, a CTE, or a derived table) seeds the
    // accumulator and the scope; its cols are what name resolution starts from.
    let (mut left, mut scope) = match (select.from.first(), surface) {
        (Some(first), _) => {
            let (rel, alias, cols) = resolve_table_factor(cx, &first.relation)?;
            (rel, JoinScope::single(&alias, cols))
        }
        // No relation is in scope, so every written name is unresolvable.
        (None, Surface::Adhoc { .. }) => (Rc::new(RelExpr::Unit), JoinScope::new()),
        (None, Surface::View(_)) => {
            return Err(GnitzSqlError::Rejected(format!(
                "{stmt}: a view body reads at least one relation; this one has no FROM clause"
            )))
        }
    };

    for (i, item) in select.from.iter().enumerate() {
        // Item 0's relation seeded the accumulator above; every later item is one
        // more INNER step carrying no keys of its own — the comma's whole meaning.
        if i > 0 {
            let comma = (&JoinConstraint::None, JoinType::Inner);
            left = fold_join_step(cx, &mut scope, left, &item.relation, comma)?;
        }
        // Then that item's own JOIN chain, left-deep in syntactic order.
        for join in &item.joins {
            let step = join_keys_and_type(join)?;
            left = fold_join_step(cx, &mut scope, left, &join.relation, step)?;
        }
    }

    // Whether a subquery may stand in this body is settled by what the body is:
    // only a view's one-relation body, neither grouped nor DISTINCT, binds one.
    let subs = RefCell::new(Vec::new());
    let sub = match (surface, scope.relations.len()) {
        (Surface::Adhoc { .. }, _) => SubPolicy::Reject(adhoc_subquery),
        (Surface::View(_), 1) if grouped => SubPolicy::Reject(|_| {
            GnitzSqlError::Rejected(
                "a subquery is not supported together with GROUP BY/aggregates; put the subquery in an inner view"
                    .into(),
            )
        }),
        (Surface::View(_), 1) if distinct => SubPolicy::Reject(|_| {
            GnitzSqlError::Rejected(
                "a subquery is not supported together with SELECT DISTINCT; put the subquery in an inner view".into(),
            )
        }),
        (Surface::View(_), 1) => SubPolicy::Bind { cx, subs: &subs },
        (Surface::View(_), _) => SubPolicy::Reject(unsupported_subquery),
    };
    let leaf = ScopeLeaf { scope: &scope, sub };

    // WHERE, then the projection in whichever shape the body carries. DISTINCT
    // outranks a grouped shape, and either sits above a join, which lowering
    // cuts. The projection is bound in SELECT order (`place_pk_front` is
    // physical, applied at lowering).
    let ctx = &format!("{stmt} projection");
    if let Some(where_expr) = &select.selection {
        let preds = bind_conjuncts(where_expr, &leaf).map_err(|e| e.in_clause("WHERE"))?;
        left = RelExpr::filter(left, preds)?;
    }
    if grouped && !distinct {
        return bind_grouped_suffix(cx, select, left, &leaf, order_exprs);
    }
    let (projected, placed) = bind_select_list(cx, select, || Ok(left), &leaf, ctx, order_exprs)?;
    if !distinct {
        return Ok((projected, placed));
    }
    reject_unselected_distinct_keys(&projected, &placed, ctx)?;
    Ok((RelExpr::distinct(projected, "SELECT DISTINCT")?, placed))
}

/// An ad-hoc read reads one relation, and a subquery derives another.
fn adhoc_subquery(e: &Expr) -> GnitzSqlError {
    derivation(match e {
        Expr::Exists { .. } | Expr::InSubquery { .. } => "EXISTS/IN subquery",
        _ => "scalar subquery",
    })
}

/// The SELECT list over the relation `rel` builds, and the item each ORDER BY key
/// sorts on; a view body also binds its window calls and its QUALIFY. `rel` is
/// called once everything that binds through `leaf` has, so a leaf that
/// registers what it binds builds its relation from all of it.
pub(super) fn bind_select_list<L: ItemLeaf>(
    cx: &BindCx<'_>,
    select: &Select,
    rel: impl FnOnce() -> Result<Rc<RelExpr>, GnitzSqlError>,
    leaf: &L,
    ctx: &str,
    order_exprs: &[&Expr],
) -> Result<(Rc<RelExpr>, Vec<usize>), GnitzSqlError> {
    let ids = cx.ids;
    if cx.surface.is_view() {
        return super::window::bind_view_select_list(ids, select, rel, leaf, ctx, order_exprs);
    }
    let mut items = bind_projection(&select.projection, leaf, ids, ctx)?;
    let placed = place_order_keys(order_exprs, &mut items, ids, leaf)?;
    Ok((leaf.project(rel()?, items)?, placed))
}

/// DISTINCT dedups the selected columns, so an ORDER BY key that is not one of them —
/// a hidden item `place_order_keys` appended — would change the set it orders.
fn reject_unselected_distinct_keys(projected: &RelExpr, placed: &[usize], ctx: &str) -> Result<(), GnitzSqlError> {
    let cols = projected.cols();
    if placed.iter().any(|&at| cols[at].def.is_hidden) {
        return Err(GnitzSqlError::Rejected(format!(
            "{ctx}: an ORDER BY key under SELECT DISTINCT must be a selected column"
        )));
    }
    Ok(())
}

/// What a SELECT list needs of the leaf it binds through, beyond expression
/// binding.
pub(super) trait ItemLeaf: LeafBinder<ColId> {
    /// The name a copied column is selected under: its visible source column's.
    fn source_name(&self, id: ColId) -> Option<String>;
    /// The projection of `items` over `source`. The subquery-binding leaf joins in
    /// the subqueries its items and `source`'s filter read.
    fn project(&self, source: Rc<RelExpr>, items: Vec<ProjEntry>) -> Result<Rc<RelExpr>, GnitzSqlError> {
        Ok(RelExpr::project(source, items))
    }
    /// The columns `*` — or `qualifier.*` — expands to here, or `None` where a
    /// wildcard is not an item: a grouped body, where every item is a group key
    /// or an aggregate, and a body reading no relation.
    fn wildcard_cols(&self, qualifier: Option<&str>) -> Result<Option<Vec<&HirCol>>, GnitzSqlError>;
}

/// Resolve every SELECT item into a `ProjEntry` in SELECT order, expanding a bare
/// `*` over the leaf's columns (honoring `EXCEPT`/`EXCLUDE`/`RENAME`, skipping
/// hidden columns), and refuse two items of one name.
pub(super) fn bind_projection<L: ItemLeaf>(
    projection: &[SelectItem],
    leaf: &L,
    ids: &ColIdGen,
    ctx: &str,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    let mut items = Vec::new();
    for (idx, item) in projection.iter().enumerate() {
        // A wildcard the leaf does not expand falls to `scalar_projection_item`,
        // which names it.
        let star = match item {
            SelectItem::Wildcard(o) => Some((None, o)),
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(q), o) => {
                single_part_ident(q).map(|q| (Some(q), o))
            }
            _ => None,
        };
        if let Some((qualifier, o)) = star {
            if let Some(cols) = leaf.wildcard_cols(qualifier)? {
                let expanded = expand_wildcard_item(o, cols.iter().map(|c| &c.def), ctx)?;
                items.extend(expanded.into_iter().map(|(i, def)| ProjEntry {
                    expr: BExpr::ColRef(cols[i].id),
                    out: HirCol::new(ids.next(), def),
                }));
                continue;
            }
        }
        let (expr, alias) = scalar_projection_item(item, ctx)?;
        items.push(bind_scalar_item(expr, alias, idx, leaf, ids).map_err(|e| e.in_clause(ctx))?);
    }
    // A projection naming nothing of its own (`*`, `* EXCEPT/EXCLUDE`) is exempt: a
    // duplicate among the source's own names rides through positionally.
    if !crate::ast_util::is_name_preserving_wildcard_projection(projection) {
        reject_duplicate_names(items.iter().map(|e| e.out.def.name.as_str()), ctx)?;
    }
    Ok(items)
}

/// Bind one scalar SELECT item to its output column. Unaliased, the item at
/// position `idx` is named `_{stem}{idx}`.
fn bind_scalar_item<L: ItemLeaf>(
    expr: &Expr,
    alias: Option<String>,
    idx: usize,
    leaf: &L,
    ids: &ColIdGen,
) -> Result<ProjEntry, GnitzSqlError> {
    let bound = bind_structural(expr, leaf)?;
    let copied = as_col(&bound);
    let src_name = copied.and_then(|id| leaf.source_name(id));
    // An aggregate or window call is named for its function.
    let stem = match peel_nested(expr) {
        Expr::Function(f) if is_agg_call(f) || f.over.is_some() => {
            single_part_ident(&f.name).map(str::to_ascii_lowercase)
        }
        _ => None,
    };
    let ty = bound.infer_ty(leaf);
    let def = ColumnDef::typed(
        alias
            .or(src_name)
            .unwrap_or_else(|| format!("_{}{idx}", stem.as_deref().unwrap_or("expr"))),
        // A copied column keeps its type; a computed one is stored as its register.
        if copied.is_some() { ty } else { ty.register_image() },
        copied.is_none_or(|id| leaf.decl(&id).1),
    );
    Ok(ProjEntry {
        expr: bound,
        out: HirCol::new(ids.next(), def),
    })
}

/// What a leaf does with a subquery node it meets.
enum SubPolicy<'a> {
    /// Bind it in `cx` as the column its decorrelation produces, recording it in
    /// `subs`.
    Bind {
        cx: &'a BindCx<'a>,
        subs: &'a RefCell<Vec<SubqueryRef>>,
    },
    /// No subquery here, and why.
    Reject(fn(&Expr) -> GnitzSqlError),
}

/// The name-resolution leaf for every HIR body. One relation is a scope with
/// `relations.len() == 1`, so the linear and join bodies resolve through one
/// rule; only the subquery policy differs.
struct ScopeLeaf<'a> {
    scope: &'a JoinScope,
    sub: SubPolicy<'a>,
}

impl ScopeLeaf<'_> {
    /// The kind of the subquery `id` is the column of, when this leaf bound one.
    fn recorded(&self, id: ColId) -> Option<SubqueryKind> {
        match self.sub {
            SubPolicy::Bind { subs, .. } => subs.borrow().iter().find(|s| s.id == id).map(|s| s.kind),
            SubPolicy::Reject(_) => None,
        }
    }
}

impl ItemLeaf for ScopeLeaf<'_> {
    fn source_name(&self, id: ColId) -> Option<String> {
        col_by_id(&self.scope.combined, id)
            .filter(|c| !c.def.is_hidden)
            .map(|c| c.def.name.clone())
    }
    fn wildcard_cols(&self, qualifier: Option<&str>) -> Result<Option<Vec<&HirCol>>, GnitzSqlError> {
        Ok(match qualifier {
            _ if self.scope.relations.is_empty() => None,
            None => Some(self.scope.unmerged().collect()),
            Some(alias) => Some(self.scope.relation(alias)?.iter().collect()),
        })
    }
    fn project(&self, source: Rc<RelExpr>, items: Vec<ProjEntry>) -> Result<Rc<RelExpr>, GnitzSqlError> {
        match self.sub {
            SubPolicy::Bind { subs, .. } => super::decorrelate::decorrelate(source, items, &subs.borrow()),
            SubPolicy::Reject(_) => Ok(RelExpr::project(source, items)),
        }
    }
}

impl LeafTyping<ColId> for ScopeLeaf<'_> {
    /// A subquery's column by its shape (an EXISTS/IN over NOT NULL operands or
    /// a COUNT never is NULL); every other column by its definition.
    fn decl(&self, id: &ColId) -> (ColType, bool) {
        match self.recorded(*id) {
            Some(SubqueryKind::Exists { nullable }) => (BOOL, nullable),
            Some(SubqueryKind::Scalar { ty, count }) => (ty, !count),
            None => self.scope.combined[..].decl(id),
        }
    }
}

impl LeafBinder<ColId> for ScopeLeaf<'_> {
    fn bind_column(&self, qual: Option<&str>, name: &str) -> Result<HirExpr, GnitzSqlError> {
        Ok(BExpr::ColRef(self.scope.resolve(qual, name)?))
    }

    fn bind_subquery(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        match self.sub {
            SubPolicy::Bind { cx, subs } => subquery::bind_one_subquery(cx, self.scope, subs, e),
            SubPolicy::Reject(why) => Err(why(e)),
        }
    }
}

/// Bind an expression against one relation's schema under `alias`, the qualifier
/// a written column reference must name: a column to its schema position. It
/// hosts no aggregate, window call or subquery.
pub(crate) fn bind_single_table(expr: &Expr, schema: &Schema, alias: &str) -> Result<BoundExpr, GnitzSqlError> {
    let ids = ColIdGen::new();
    let cols = schema
        .columns()
        .iter()
        .map(|c| HirCol::new(ids.next(), c.clone()))
        .collect();
    let scope = JoinScope::single(alias, cols);
    let leaf = ScopeLeaf {
        scope: &scope,
        sub: SubPolicy::Reject(unsupported_subquery),
    };
    // A column's position in a one-relation scope is its schema position.
    let at = |id: &ColId| {
        let pos = scope.combined.iter().position(|c| c.id == *id);
        pos.expect("bound in this scope")
    };
    Ok(bind_structural(expr, &leaf)?.rebuild(&mut |id| BExpr::ColRef(at(id))))
}

/// The name-resolution scope of a FROM-join body: all in-scope (null-widened)
/// `HirCol`s in relation order, plus each relation's alias (as written) and span.
/// Resolves a qualified or unqualified reference to a `ColId`.
#[derive(Clone)]
struct JoinScope {
    combined: Vec<HirCol>,
    /// The columns a `USING` / `NATURAL` step merged away. A `Vec` because it is
    /// empty for every query without one, where `contains` is a length check.
    merged: Vec<ColId>,
    relations: Vec<(String, Range<usize>)>,
}

impl JoinScope {
    fn new() -> Self {
        JoinScope {
            combined: Vec::new(),
            merged: Vec::new(),
            relations: Vec::new(),
        }
    }

    /// The scope of one relation — a `new` plus one `push`.
    fn single(alias: &str, cols: Vec<HirCol>) -> Self {
        let mut scope = JoinScope::new();
        scope.push(alias, cols).expect("an empty scope takes any alias");
        scope
    }

    /// The columns still answering an unqualified name and appearing in `*`.
    fn unmerged(&self) -> impl Iterator<Item = &HirCol> {
        self.combined.iter().filter(|c| !self.merged.contains(&c.id))
    }

    /// The visible column names this scope currently answers unqualified — what
    /// `NATURAL` intersects the incoming relation's names against. Hidden slots
    /// are not names a user can write, so they pair with nothing.
    fn shared_names(&self, rcols: &[HirCol]) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for c in self.unmerged().filter(|c| !c.def.is_hidden) {
            // A NAME, once. Two like-named visible left columns are one name to
            // pair on, and `merge_pairs` is what reports it as ambiguous; listing
            // it twice would pair and merge the same column twice.
            if has_visible_column(rcols.iter().map(|r| &r.def), &c.def.name)
                && !names.iter().any(|n| n.eq_ignore_ascii_case(&c.def.name))
            {
                names.push(c.def.name.clone());
            }
        }
        names
    }

    /// Add a relation under `alias`. Under one alias twice, `alias.col` would name
    /// a column of either relation, so a taken alias is refused.
    fn push(&mut self, alias: &str, cols: Vec<HirCol>) -> Result<(), GnitzSqlError> {
        if self.relations.iter().any(|(a, _)| a.eq_ignore_ascii_case(alias)) {
            return Err(GnitzSqlError::Rejected(format!(
                "relation alias '{alias}' is used by two relations in one scope; rename one"
            )));
        }
        let start = self.combined.len();
        self.combined.extend(cols);
        self.relations.push((alias.to_string(), start..self.combined.len()));
        Ok(())
    }

    /// Apply one join step's outer null-widening to the scope, in place, through
    /// the same [`JoinType::widen_sides`] the join's logical output uses.
    ///
    /// In place rather than adopting `join.cols()`: a derived table's `AS d(col…)`
    /// aliases live only on the cols `resolve_table_factor` handed the scope — the
    /// bound subtree keeps its own inner names — so rebuilding the scope from the
    /// tree would drop every positional alias in a join body.
    fn widen_step(&mut self, kind: JoinType) {
        let split = self.relations.last().expect("a right relation was pushed").1.start;
        let (left, right) = self.combined.split_at_mut(split);
        kind.widen_sides(
            left.iter_mut().map(|c| &mut c.def),
            right.iter_mut().map(|c| &mut c.def),
        );
    }

    /// The columns of the relation `alias` names.
    fn relation(&self, alias: &str) -> Result<&[HirCol], GnitzSqlError> {
        let (_, span) = self
            .relations
            .iter()
            .find(|(a, _)| a.eq_ignore_ascii_case(alias))
            .ok_or_else(|| GnitzSqlError::Rejected(format!("table alias '{alias}' not found")))?;
        Ok(&self.combined[span.clone()])
    }

    fn resolve_qualified(&self, alias: &str, name: &str) -> Result<ColId, GnitzSqlError> {
        // One relation in scope: the single-relation rule, worded as every other
        // statement words it.
        if let [(only, _)] = self.relations.as_slice() {
            let defs = self.combined.iter().map(|c| &c.def);
            return Ok(self.combined[single_relation_col_idx(defs, only, Some(alias), name)?].id);
        }
        let cols = self.relation(alias)?;
        let idx = find_unique_column(cols.iter().map(|c| &c.def), name)?
            .ok_or_else(|| GnitzSqlError::Rejected(format!("column '{name}' not found in table '{alias}'")))?;
        Ok(cols[idx].id)
    }

    /// Look up an unqualified reference across the whole scope — one lookup, so a
    /// name two relations both carry is the same ambiguity as one relation carrying
    /// it twice. Merged columns are skipped, which is what makes a merged name
    /// resolve rather than collide; absence is `Ok(None)` so a caller can word it.
    fn find_unqualified(&self, name: &str) -> Result<Option<ColId>, GnitzSqlError> {
        let at = find_unique_column(self.unmerged().map(|c| &c.def), name)?;
        Ok(at.and_then(|i| self.unmerged().nth(i)).map(|c| c.id))
    }

    fn resolve_unqualified(&self, name: &str) -> Result<ColId, GnitzSqlError> {
        let scope = if self.relations.len() > 1 { " in any table" } else { "" };
        self.find_unqualified(name)?
            .ok_or_else(|| GnitzSqlError::Rejected(format!("column '{name}' not found{scope}")))
    }

    /// Resolve a column reference, qualified by `qual` or not.
    fn resolve(&self, qual: Option<&str>, name: &str) -> Result<ColId, GnitzSqlError> {
        match qual {
            None => self.resolve_unqualified(name),
            Some(qual) => self.resolve_qualified(qual, name),
        }
    }
}

/// A relation's own columns projected by `projection`, bound through the leaf a
/// view body binds through — an `INSERT … RETURNING` list.
pub(super) fn bind_returning(
    ids: &ColIdGen,
    projection: &[SelectItem],
    desc: Arc<RelDescriptor>,
    alias: &str,
) -> Result<(Vec<HirCol>, Vec<ProjEntry>), GnitzSqlError> {
    let source = RelExpr::get(ids, desc);
    let scope = JoinScope::single(alias, source.cols());
    let leaf = ScopeLeaf {
        scope: &scope,
        sub: SubPolicy::Reject(unsupported_subquery),
    };
    let items = bind_projection(projection, &leaf, ids, "SELECT")?;
    Ok((scope.combined, items))
}

/// Which statement a body is bound for.
#[derive(Clone, Copy)]
pub(super) enum Surface {
    /// A view body.
    View(ViewBody),
    /// An ad-hoc read, which lowers to one stateless read of one relation. `op`
    /// names the read in a class rejection.
    Adhoc { op: &'static str },
}

impl Surface {
    /// The statement a rejection names: the view statement, or `SELECT` for an
    /// ad-hoc read.
    pub(super) fn stmt(self) -> &'static str {
        match self {
            Surface::View(v) => v.stmt,
            Surface::Adhoc { .. } => "SELECT",
        }
    }

    pub(super) fn is_view(self) -> bool {
        matches!(self, Surface::View(_))
    }
}

/// The projection item each key of `order_exprs` sorts on: the output column it
/// names, else the item whose bound expression equals the key's — so `t.a + b` and
/// `a + b` are one item — else a hidden one appended here.
pub(super) fn place_order_keys<L: ItemLeaf>(
    order_exprs: &[&Expr],
    items: &mut Vec<ProjEntry>,
    ids: &ColIdGen,
    leaf: &L,
) -> Result<Vec<usize>, GnitzSqlError> {
    let mut cols = Vec::with_capacity(order_exprs.len());
    for (i, e) in order_exprs.iter().enumerate() {
        if let Some(at) = output_column(e, items.iter().map(|it| &it.out.def))? {
            cols.push(at);
            continue;
        }
        // A leaf that registers while binding (a subquery) hands back a column no
        // existing item names, so such an entry never matches and is appended.
        let mut entry =
            bind_scalar_item(e, Some(format!("_order{i}")), i, leaf, ids).map_err(|e| e.in_clause("ORDER BY"))?;
        cols.push(match items.iter().position(|it| it.expr == entry.expr) {
            Some(at) => at,
            None => {
                entry.out.def.is_hidden = true;
                items.push(entry);
                items.len() - 1
            }
        });
    }
    Ok(cols)
}

/// Bind a set operation, binding both sides recursively; the `SetOp` constructor
/// pairs columns positionally and promotes cross-width types.
fn bind_set_op(
    cx: &BindCx<'_>,
    op: SetOperator,
    quantifier: SetQuantifier,
    left: &SetExpr,
    right: &SetExpr,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    reject_if(
        matches!(
            quantifier,
            SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName
        ),
        "set operations",
        "BY NAME",
    )?;
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
pub(super) mod tests;
