//! AST → HIR binding. `bind_body` resolves every name to an opaque `ColId`,
//! validates the honored clauses, and produces a logical `RelExpr` tree for any
//! relational body (linear / join / grouped / DISTINCT / set operation). The CTE
//! phase (`bind_ctes`) binds each CTE body to a shared subtree ahead of the
//! main bind and registers it by name, so a reference re-reads it; a derived
//! table binds its subquery recursively to an unshared subtree
//! (`resolve_table_factor`).
//!
//! The body's shape-specific parts are this module's children: `join` folds the
//! FROM clause's join steps, `subquery` binds the subqueries a linear body reads
//! and `decorrelate` joins them in where the projection is built, `group` binds
//! the GROUP BY / aggregate / HAVING suffix, `window` binds the SELECT list and
//! desugars its window calls, and
//! `wildcard` expands `SELECT *`.

mod decorrelate;
mod group;
mod join;
mod subquery;
mod wildcard;
mod window;

pub(crate) use window::ranking_names;

use super::{
    ColId, ColIdGen, HirCol, HirExpr, JoinType, OrderCol, OrderedOp, ProjEntry, RelExpr, SetOpKind, col_by_id,
    hircol_of, schema_cols,
};
use crate::ast_util::{
    body_is_grouped, col_ref_parts, is_agg_call, peel_nested, select_is_distinct, single_part_ident,
};
use crate::bind::{LeafBinder, bind_conjuncts, bind_structural, unsupported_subquery};
use crate::catalog::Catalog;
use crate::error::{GnitzSqlError, derivation, reject_if, unsupported_clause};
use crate::ir::{BExpr, BOOL, LeafTyping};
use crate::names::{TableRef, extract_table_ref};
use crate::rules::{
    ClassWant, canonical_user_name, find_unique_column, has_visible_column, reject_duplicate_names, require_class,
    single_relation_col_idx,
};
use crate::tail::{OrderKey, OrderTarget, extract_limit_offset, parse_order_by, reject_position_out_of_range};
use crate::validate::{QueryEnvelope, SelectSite, reject_unhonored_query_clauses, reject_unhonored_select_clauses};
use gnitz_core::Schema;
use gnitz_wire::{ColType, ColumnDef, WeightSlots};
use group::bind_grouped_suffix;
use join::{fold_join_step, join_keys_and_type};
use sqlparser::ast::{
    Expr, Ident, InTarget, JoinConstraint, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr,
    SetOperator, SetQuantifier, TableAliasColumnDef, TableFactor,
};
use sqlparser::tokenizer::Span;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;
use subquery::{SubqueryKind, SubqueryRef};

/// Bind a query's CTEs ahead of its body. Each binds to one subtree registered
/// under its name, which the body (and later CTEs) read through a
/// [`RelExpr::alias`]. Scoping precedence as SQL
/// defines it: a CTE shadows a catalog name, a later CTE sees earlier ones, and
/// one WITH list names each CTE once.
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
        if cx.ctes.contains_key(&canonical) {
            return Err(GnitzSqlError::rejected(format!(
                "WITH query name '{name}' is specified more than once"
            )));
        }
        reject_if(from.is_some(), &ctx, "a trailing FROM")?;
        let rel = bind_body(cx, reject_unhonored_query_clauses(query, QueryEnvelope::Bare, &ctx)?)?;
        let (rel, mut defs) = collapse_identity(rel);
        apply_alias_columns(&alias.columns, defs.iter_mut(), &ctx)?;
        cx.ctes.insert(canonical, Cte { rel, defs });
    }
    Ok(())
}

/// Apply a reference's column alias list (`WITH d(a, b)`, `FROM t AS d(a, b)`,
/// `(subquery) AS d(a, b)`) to `defs`. A type on an alias is refused: it would
/// have to be checked against the body's own or silently ignored.
fn apply_alias_columns<'b>(
    columns: &[TableAliasColumnDef],
    defs: impl IntoIterator<Item = &'b mut ColumnDef>,
    ctx: &str,
) -> Result<(), GnitzSqlError> {
    reject_if(
        columns.iter().any(|c| c.data_type.is_some()),
        ctx,
        "a type on a column alias",
    )?;
    apply_positional_aliases(columns.iter().map(|c| &c.name), defs, ctx)
}

/// Apply positional column aliases (`WITH d(a, b) AS …` / `(subquery) AS d(a, b)`
/// / `CREATE VIEW v (a, b) AS …`) to the *visible* columns of a body's output, in
/// order — a rename only, leaving `ColId`s and layout untouched. Hidden columns
/// (a JOIN body's synthetic-PK region) are skipped, so an alias list names exactly
/// what a downstream query sees.
///
/// A rename can collide where the original names did not, so the result is checked
/// for duplicates.
pub(super) fn apply_positional_aliases<'a, 'b>(
    aliases: impl ExactSizeIterator<Item = &'a Ident>,
    defs: impl IntoIterator<Item = &'b mut ColumnDef>,
    ctx: &str,
) -> Result<(), GnitzSqlError> {
    let n_aliases = aliases.len();
    if n_aliases == 0 {
        return Ok(());
    }
    let mut visible: Vec<&mut ColumnDef> = defs.into_iter().filter(|c| !c.is_hidden).collect();
    if n_aliases != visible.len() {
        return Err(GnitzSqlError::rejected(format!(
            "{ctx} defines {n_aliases} column aliases but body returns {} columns",
            visible.len(),
        )));
    }
    for (col, alias) in visible.iter_mut().zip(aliases) {
        col.name = Arc::from(alias.value.as_str());
    }
    reject_duplicate_names(visible.iter().map(|c| &*c.name), &format!("{ctx} column aliases"))
}

/// The column of `cols` an ORDER BY key names — a 1-based position among the
/// visible ones, or an unqualified visible name — else the expression it sorts on.
/// Only an unqualified name can name an output column: an output column belongs to
/// no relation, so a qualified reference is left to the caller's scope, which
/// checks the qualifier.
fn output_key<'e, 'c>(
    target: &OrderTarget<'e>,
    cols: impl Iterator<Item = &'c HirCol> + Clone,
) -> Result<Result<ColId, &'e Expr>, GnitzSqlError> {
    match *target {
        OrderTarget::Position(pos) => {
            let mut visible = cols.filter(|c| !c.def.is_hidden);
            reject_position_out_of_range(pos, visible.clone().count(), "ORDER BY")?;
            Ok(Ok(visible.nth(pos - 1).expect("a position in range").id))
        }
        OrderTarget::Expr(e) => Ok(match col_ref_parts(e) {
            Some((None, name)) => match find_unique_column(cols.clone().map(|c| &c.def), name)? {
                Some(at) => Ok({ cols }.nth(at).expect("a found column").id),
                None => Err(e),
            },
            _ => Err(e),
        }),
    }
}

/// A bound CTE: the shared subtree every reference re-reads, and the column
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
                .all(|(it, c)| it.expr.as_col() == Some(c.id) && !it.out.def.is_hidden);
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
    let defs = rel.cols().iter().map(|c| c.def.clone()).collect();
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
    /// Whether the view's options, stated or standing, give it a capacity.
    pub bounded: bool,
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
    /// Also `None` on the ad-hoc surface, which reads only `tid` and `name`.
    pub(crate) columns: Option<Vec<String>>,
}

/// The subtree a relation name reads, under the column aliases the reference
/// declares: the CTE it names, else the catalog relation. Every relation a body
/// names in FROM resolves here.
fn resolve_table_ref(cx: &BindCx<'_>, tref: &TableRef<'_>) -> Result<(Rc<RelExpr>, Vec<HirCol>), GnitzSqlError> {
    let name = &tref.name;
    // A CTE lives in no schema.
    let rel = match (!name.is_qualified()).then(|| cx.ctes.get(name.name())).flatten() {
        Some(cte) => RelExpr::alias(cx.ids, Rc::clone(&cte.rel), cte.defs.iter().cloned()),
        None => {
            let rel = cx.cat.probe_relation(name)?;
            let want = match cx.surface {
                Surface::Adhoc => ClassWant::Readable,
                // The replaced view is retracted in the same bundle, taking the body's input with it.
                Surface::View(ViewBody { stmt, replacing, .. }) if replacing == Some(rel.tid) => {
                    return Err(GnitzSqlError::rejected(format!(
                        "{stmt} '{name}' AS a query referencing the view itself is not supported"
                    )));
                }
                Surface::View(_) => ClassWant::ViewSource,
            };
            require_class(&rel, name, want, cx.surface.stmt())?;
            let columns = (cx.surface.is_view() && tref.columns.is_empty()).then(|| {
                rel.schema
                    .visible_columns()
                    .map(|(_, c)| String::from(&*c.name))
                    .collect()
            });
            cx.refs.borrow_mut().push(BoundRef {
                tid: rel.tid,
                name: name.to_string(),
                name_span: tref.name_span,
                alias_span: tref.alias_span,
                alias: tref.alias.clone(),
                columns,
            });
            RelExpr::get(cx.ids, rel)
        }
    };
    let mut cols = rel.cols().into_owned();
    if !tref.columns.is_empty() {
        apply_alias_columns(
            tref.columns,
            cols.iter_mut().map(|c| &mut c.def),
            &format!("table '{}'", tref.alias),
        )?;
    }
    Ok((rel, cols))
}

/// Bind a whole view query: its envelope checked, its CTEs, its body, then the
/// `ORDER BY … LIMIT n [OFFSET m]` tail as a top-N over it. A SELECT body places
/// the keys in its own scope; any other body (a set operation or a parenthesized
/// query, which has no scope of its own) orders by its output columns.
///
/// The two tail clauses need each other — a view is unordered, and a LIMIT with no
/// order names no rows — and the counts are literals.
pub(super) fn bind_query(cx: &mut BindCx<'_>, query: &Query) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let stmt = cx.surface.stmt();
    reject_unhonored_query_clauses(query, QueryEnvelope::WithAndTail, stmt)?;
    bind_ctes(cx, query)?;
    let cx = &*cx;
    let keys = parse_order_by(query.order_by.as_ref())?;
    let slots = match (keys.is_empty(), extract_limit_offset(query)?) {
        (true, (None, Some(_))) => return Err(unsupported_clause(stmt, "OFFSET without ORDER BY … LIMIT")),
        (true, (None, None)) => None,
        (false, (None, _)) => {
            return Err(GnitzSqlError::rejected(format!(
                "{stmt}: ORDER BY without LIMIT — a view holds an unordered set, so an order alone \
                 maintains nothing; add LIMIT n to maintain the top n rows"
            )));
        }
        (true, (Some(_), _)) => {
            return Err(GnitzSqlError::rejected(format!(
                "{stmt}: LIMIT without ORDER BY names no rows in particular; add ORDER BY"
            )));
        }
        (false, (Some(0), _)) => return Err(GnitzSqlError::rejected(format!("{stmt}: LIMIT 0 selects nothing"))),
        (false, (Some(limit), offset)) => Some(WeightSlots {
            skip: offset.unwrap_or(0) as u64,
            keep: limit as u64,
        }),
    };
    let (rel, order) = match query.body.as_ref() {
        SetExpr::Select(select) => bind_select(cx, select, &keys)?,
        body => {
            let rel = bind_body(cx, body)?;
            let order = keys
                .iter()
                .map(|key| {
                    let col = output_key(&key.target, rel.cols().iter())?.map_err(|_| {
                        GnitzSqlError::rejected(format!(
                            "{stmt}: ORDER BY over a set operation names an output column or position"
                        ))
                    })?;
                    Ok(OrderCol::of(key, col))
                })
                .collect::<Result<_, GnitzSqlError>>()?;
            (rel, order)
        }
    };
    match slots {
        Some(slots) => RelExpr::ordered(rel, Vec::new(), order, OrderedOp::TopN(slots)),
        None => Ok(rel),
    }
}

/// Bind one query body — a single SELECT (linear / join / grouped / DISTINCT) or
/// a set operation whose sides bind recursively as any body. A parenthesized side
/// is a whole `Query`, whose envelope is rejected before its body binds. The
/// `SetOp` constructor pairs a set operation's columns positionally and promotes
/// cross-width types.
fn bind_body(cx: &BindCx<'_>, body: &SetExpr) -> Result<Rc<RelExpr>, GnitzSqlError> {
    match body {
        SetExpr::Select(select) => Ok(bind_select(cx, select, &[])?.0),
        SetExpr::SetOperation { op, set_quantifier, left, right } => {
            reject_if(
                matches!(
                    set_quantifier,
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
            let all = matches!(set_quantifier, SetQuantifier::All);
            RelExpr::set_op(cx.ids, kind, all, bind_body(cx, left)?, bind_body(cx, right)?)
        }
        SetExpr::Query(q) => bind_body(
            cx,
            reject_unhonored_query_clauses(q, QueryEnvelope::Bare, "parenthesized query")?,
        ),
        _ => Err(GnitzSqlError::rejected(format!(
            "{} only supports SELECT and set operations",
            cx.surface.stmt()
        ))),
    }
}

/// Resolve one FROM table factor to `(subtree, alias, output cols)`: a table name
/// to a `Get`, a CTE name to a fresh read of its shared subtree, a derived table
/// to its bound subquery, unshared — collapsed onto its source when it only
/// renames, as a CTE is. A column alias list renames the returned cols, keeping
/// their `ColId`s; the subtree keeps its own names.
fn resolve_table_factor(
    cx: &BindCx<'_>,
    factor: &TableFactor,
) -> Result<(Rc<RelExpr>, String, Vec<HirCol>), GnitzSqlError> {
    if let TableFactor::Derived { lateral, subquery, alias, sample } = factor {
        let Some(alias) = alias else {
            return Err(GnitzSqlError::rejected(
                "a derived table (subquery in FROM) needs an alias",
            ));
        };
        let ctx = format!("derived table '{}'", alias.name.value);
        reject_if(*lateral, &ctx, "LATERAL")?;
        // Silently dropping TABLESAMPLE would return all rows — a wrong result.
        reject_if(sample.is_some(), &ctx, "TABLESAMPLE")?;
        let body = reject_unhonored_query_clauses(subquery, QueryEnvelope::Bare, &ctx)?;
        let (subtree, mut defs) = collapse_identity(bind_body(cx, body)?);
        apply_alias_columns(&alias.columns, defs.iter_mut(), &ctx)?;
        let cols = (subtree.cols().iter())
            .zip(defs)
            .map(|(c, def)| HirCol::new(c.id, def))
            .collect();
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
    order: &[OrderKey<'_>],
) -> Result<(Rc<RelExpr>, Vec<OrderCol>), GnitzSqlError> {
    let (stmt, surface) = (cx.surface.stmt(), cx.surface);
    let grouped = body_is_grouped(select);
    let distinct = select_is_distinct(select);
    let site = if surface.is_view() {
        SelectSite::View
    } else {
        SelectSite::Read
    };
    reject_unhonored_select_clauses(select, site, stmt)?;

    // The first FROM relation (a table, a CTE, or a derived table) seeds the
    // accumulator and the scope; its cols are what name resolution starts from.
    let (mut left, mut scope) = match (select.from.first(), surface) {
        (Some(first), _) => {
            let (rel, alias, cols) = resolve_table_factor(cx, &first.relation)?;
            (rel, JoinScope::single(alias, cols))
        }
        // No relation is in scope, so every written name is unresolvable.
        (None, Surface::Adhoc) => (Rc::new(RelExpr::Unit), JoinScope::default()),
        (None, Surface::View(_)) => {
            return Err(GnitzSqlError::rejected(format!(
                "{stmt}: a view body reads at least one relation; this one has no FROM clause"
            )));
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
    // only a view's body that is not grouped binds one.
    let subs = RefCell::new(Vec::new());
    let sub = match surface {
        Surface::Adhoc => SubPolicy::Reject(adhoc_subquery),
        Surface::View(_) if grouped => SubPolicy::Reject(|_| {
            GnitzSqlError::rejected(
                "a subquery is not supported together with GROUP BY/aggregates; put the subquery in an inner view",
            )
        }),
        Surface::View(_) => SubPolicy::Bind { cx, subs: &subs },
    };

    // WHERE, then the projection in whichever shape the body carries. DISTINCT
    // outranks a grouped shape, and either sits above a join, which lowering
    // cuts. The projection is bound in SELECT order (the PK-front
    // placement is physical, applied at lowering).
    let ctx = &format!("{stmt} projection");
    if let Some(where_expr) = &select.selection {
        let leaf = ScopeLeaf { scope: &scope, sub };
        let preds = bind_conjuncts(where_expr, &leaf).map_err(|e| e.in_clause("WHERE"))?;
        left = RelExpr::filter(left, preds)?;
        // A WHERE that narrows an outer join leaves the side it null-filled as declared.
        scope.adopt_nullability(&left.cols());
    }
    let leaf = ScopeLeaf { scope: &scope, sub };
    if grouped && !distinct {
        return bind_grouped_suffix(cx, select, left, &leaf, order);
    }
    let (projected, order) = window::bind_select_list(cx, select, || Ok(left), &leaf, ctx, order)?;
    if !distinct {
        return Ok((projected, order));
    }
    // DISTINCT dedups the selected columns, so an ORDER BY key that is not one of
    // them — a hidden item `place_order_keys` appended — would change the set it orders.
    if (order.iter()).any(|o| hircol_of(&projected.cols(), o.col).def.is_hidden) {
        return Err(GnitzSqlError::rejected(format!(
            "{ctx}: an ORDER BY key under SELECT DISTINCT must be a selected column"
        )));
    }
    Ok((RelExpr::distinct(projected, "SELECT DISTINCT")?, order))
}

/// An ad-hoc read reads one relation, and a subquery derives another.
fn adhoc_subquery(e: &Expr) -> GnitzSqlError {
    derivation(match e {
        Expr::Exists { .. } | Expr::In { target: InTarget::Subquery(_), .. } => "EXISTS/IN subquery",
        _ => "scalar subquery",
    })
}

/// What a SELECT list needs of the leaf it binds through, beyond expression
/// binding.
trait ItemLeaf: LeafBinder<ColId> {
    /// The name a copied column is selected under: its visible source column's.
    fn source_name(&self, id: ColId) -> Option<Arc<str>>;
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
fn bind_projection<L: ItemLeaf>(
    projection: &[SelectItem],
    leaf: &L,
    ids: &ColIdGen,
    ctx: &str,
) -> Result<Vec<ProjEntry>, GnitzSqlError> {
    let mut items = Vec::new();
    for (idx, item) in projection.iter().enumerate() {
        // A wildcard the leaf does not expand falls to the scalar match below,
        // which names it.
        let star = match item {
            SelectItem::Wildcard(o) => Some((None, o)),
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(q), o) => {
                single_part_ident(q).map(|q| (Some(q), o))
            }
            _ => None,
        };
        if let Some((qualifier, o)) = star
            && let Some(cols) = leaf.wildcard_cols(qualifier)?
        {
            let expanded = wildcard::expand_wildcard_item(o, cols.iter().map(|c| &c.def), ctx)?;
            items.extend(expanded.into_iter().map(|(i, def)| ProjEntry {
                expr: BExpr::ColRef(cols[i].id),
                out: HirCol::new(ids.next(), def),
            }));
            continue;
        }
        // Anything that is not one scalar expression rejects, naming the shape written.
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::ExprWithAliases { .. } => {
                return Err(GnitzSqlError::rejected(format!(
                    "{ctx}: a multi-alias (`AS (a, b)`) SELECT item is not a supported SELECT item"
                )));
            }
            SelectItem::Wildcard(_) => {
                return Err(GnitzSqlError::rejected(format!(
                    "{ctx}: SELECT * is not a supported SELECT item"
                )));
            }
            SelectItem::QualifiedWildcard(..) => {
                return Err(GnitzSqlError::rejected(format!(
                    "{ctx}: SELECT <table>.* is not a supported SELECT item"
                )));
            }
        };
        items.push(bind_scalar_item(expr, alias, idx, leaf, ids).map_err(|e| e.in_clause(ctx))?);
    }
    // A projection naming nothing of its own (`*`, `* EXCEPT/EXCLUDE`) is exempt: a
    // duplicate among the source's own names rides through positionally.
    if !wildcard::is_name_preserving_wildcard_projection(projection) {
        reject_duplicate_names(items.iter().map(|e| &*e.out.def.name), ctx)?;
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
    let copied = bound.as_col();
    let src_name = copied.and_then(|id| leaf.source_name(id));
    // An aggregate or window call is named for its function.
    let stem = match peel_nested(expr) {
        Expr::Function(f) if is_agg_call(f) || f.over.is_some() => {
            single_part_ident(&f.name).map(str::to_ascii_lowercase)
        }
        _ => None,
    };
    let (ty, nullable) = bound.declared(leaf);
    let name = alias
        .map(Arc::from)
        .or(src_name)
        .unwrap_or_else(|| format!("_{}{idx}", stem.as_deref().unwrap_or("expr")).into());
    let def = ColumnDef::new(name, ty, nullable);
    Ok(ProjEntry {
        expr: bound,
        out: HirCol::new(ids.next(), def),
    })
}

/// What a leaf does with a subquery node it meets.
#[derive(Clone, Copy)]
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

impl<'a> ScopeLeaf<'a> {
    /// A leaf over `scope` that binds no subquery.
    fn plain(scope: &'a JoinScope) -> Self {
        ScopeLeaf {
            scope,
            sub: SubPolicy::Reject(unsupported_subquery),
        }
    }
}

impl ItemLeaf for ScopeLeaf<'_> {
    fn source_name(&self, id: ColId) -> Option<Arc<str>> {
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
            SubPolicy::Bind { subs, .. } => decorrelate::decorrelate(source, items, &subs.borrow()),
            SubPolicy::Reject(_) => Ok(RelExpr::project(source, items)),
        }
    }
}

impl LeafTyping<ColId> for ScopeLeaf<'_> {
    /// A subquery's column by its shape (an EXISTS/IN over NOT NULL operands or
    /// a COUNT never is NULL); every other column by its definition.
    fn decl(&self, id: &ColId) -> (ColType, bool) {
        let recorded = match self.sub {
            SubPolicy::Bind { subs, .. } => subs.borrow().iter().find(|s| s.id == *id).map(|s| s.kind),
            SubPolicy::Reject(_) => None,
        };
        match recorded {
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

/// The name-resolution scope of a FROM-join body: all in-scope (null-widened)
/// `HirCol`s in relation order, plus each relation's alias (as written) and span.
/// Resolves a qualified or unqualified reference to a `ColId`.
#[derive(Clone, Default)]
struct JoinScope {
    combined: Vec<HirCol>,
    /// The columns a `USING` / `NATURAL` step merged away.
    merged: Vec<ColId>,
    relations: Vec<(String, Range<usize>)>,
}

impl JoinScope {
    /// The scope of one relation.
    fn single(alias: String, cols: Vec<HirCol>) -> Self {
        let n = cols.len();
        JoinScope {
            combined: cols,
            merged: Vec::new(),
            relations: vec![(alias, 0..n)],
        }
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
                names.push(String::from(&*c.def.name));
            }
        }
        names
    }

    /// Add a relation under `alias`. Under one alias twice, `alias.col` would name
    /// a column of either relation, so a taken alias is refused.
    fn push(&mut self, alias: &str, cols: Vec<HirCol>) -> Result<(), GnitzSqlError> {
        if self.relations.iter().any(|(a, _)| a.eq_ignore_ascii_case(alias)) {
            return Err(GnitzSqlError::rejected(format!(
                "relation alias '{alias}' is used by two relations in one scope; rename one"
            )));
        }
        let start = self.combined.len();
        self.combined.extend(cols);
        self.relations.push((alias.to_string(), start..self.combined.len()));
        Ok(())
    }

    /// Take the nullability `cols` give each scope column among them.
    fn adopt_nullability(&mut self, cols: &[HirCol]) {
        for c in &mut self.combined {
            if let Some(out) = col_by_id(cols, c.id) {
                c.def.is_nullable = out.def.is_nullable;
            }
        }
    }

    /// The columns of the relation `alias` names.
    fn relation(&self, alias: &str) -> Result<&[HirCol], GnitzSqlError> {
        let (_, span) = self
            .relations
            .iter()
            .find(|(a, _)| a.eq_ignore_ascii_case(alias))
            .ok_or_else(|| GnitzSqlError::rejected(format!("table alias '{alias}' not found")))?;
        Ok(&self.combined[span.clone()])
    }

    /// Look up an unqualified reference across the whole scope — one lookup, so a
    /// name two relations both carry is the same ambiguity as one relation carrying
    /// it twice. Merged columns are skipped, which is what makes a merged name
    /// resolve rather than collide; absence is `Ok(None)` so a caller can word it.
    fn find_unqualified(&self, name: &str) -> Result<Option<ColId>, GnitzSqlError> {
        let at = find_unique_column(self.unmerged().map(|c| &c.def), name)?;
        Ok(at.and_then(|i| self.unmerged().nth(i)).map(|c| c.id))
    }

    /// Resolve a column reference, qualified by `qual` or not.
    fn resolve(&self, qual: Option<&str>, name: &str) -> Result<ColId, GnitzSqlError> {
        // One relation in scope resolves by the single-relation rule every surface
        // shares: a qualifier must name it, so `SELECT b.val FROM a` is a rejection
        // rather than a read of `a.val`, and it disambiguates nothing more. One
        // relation merges no column, so the whole scope is its columns.
        if let [(only, _)] = self.relations.as_slice() {
            let cols = self.combined.iter().map(|c| &c.def);
            return Ok(self.combined[single_relation_col_idx(cols, only, qual, name)?].id);
        }
        match qual {
            None => self.find_unqualified(name)?.ok_or_else(|| {
                let scope = if self.relations.is_empty() { "" } else { " in any table" };
                GnitzSqlError::rejected(format!("column '{name}' not found{scope}"))
            }),
            Some(alias) => {
                let cols = self.relation(alias)?;
                let idx = find_unique_column(cols.iter().map(|c| &c.def), name)?
                    .ok_or_else(|| GnitzSqlError::rejected(format!("column '{name}' not found in table '{alias}'")))?;
                Ok(cols[idx].id)
            }
        }
    }
}

/// A relation's own columns projected by `projection`, bound through the leaf a
/// view body binds through — an `INSERT … RETURNING` list.
pub(super) fn bind_returning(
    ids: &ColIdGen,
    projection: &[SelectItem],
    schema: &Schema,
    alias: &str,
) -> Result<(Vec<HirCol>, Vec<ProjEntry>), GnitzSqlError> {
    let scope = JoinScope::single(alias.to_string(), schema_cols(ids, schema));
    let leaf = ScopeLeaf::plain(&scope);
    let items = bind_projection(projection, &leaf, ids, "SELECT")?;
    Ok((scope.combined, items))
}

/// Which statement a body is bound for.
#[derive(Clone, Copy)]
pub(super) enum Surface {
    /// A view body.
    View(ViewBody),
    /// An ad-hoc read, which lowers to one stateless read of one relation.
    Adhoc,
}

impl Surface {
    /// The statement a rejection names: the view statement, or `SELECT` for an
    /// ad-hoc read.
    pub(super) fn stmt(self) -> &'static str {
        match self {
            Surface::View(v) => v.stmt,
            Surface::Adhoc => "SELECT",
        }
    }

    pub(super) fn is_view(self) -> bool {
        matches!(self, Surface::View(_))
    }
}

/// The projection column each of `keys` sorts on: the output column it names,
/// else the item whose bound expression equals the key's — so `t.a + b` and
/// `a + b` are one item — else a hidden one appended here.
fn place_order_keys<L: ItemLeaf>(
    keys: &[OrderKey<'_>],
    items: &mut Vec<ProjEntry>,
    ids: &ColIdGen,
    leaf: &L,
) -> Result<Vec<OrderCol>, GnitzSqlError> {
    let mut order = Vec::with_capacity(keys.len());
    for (i, key) in keys.iter().enumerate() {
        let col = match output_key(&key.target, items.iter().map(|it| &it.out))? {
            Ok(col) => col,
            Err(e) => {
                // A leaf that registers while binding (a subquery) hands back a column
                // no existing item names, so such an entry never matches and is appended.
                let mut entry = bind_scalar_item(e, Some(format!("_order{i}")), i, leaf, ids)
                    .map_err(|e| e.in_clause("ORDER BY"))?;
                match items.iter().find(|it| it.expr == entry.expr) {
                    Some(it) => it.out.id,
                    None => {
                        entry.out.def.is_hidden = true;
                        let id = entry.out.id;
                        items.push(entry);
                        id
                    }
                }
            }
        };
        order.push(OrderCol::of(key, col));
    }
    Ok(order)
}

#[cfg(test)]
#[path = "tests/bind.rs"]
pub(super) mod tests;
