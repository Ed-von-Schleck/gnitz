//! CREATE VIEW front door: validate the query envelope, inline CTEs, classify
//! the view's shape, and dispatch to the matching builder. The single module in
//! `plan/view` that knows about all the others.

use crate::ast_util::{
    collect_column_refs, collect_projection_column_refs, extract_name, extract_relation_name,
    extract_table_factor_name, flatten_conjuncts, group_by_is_present, is_wildcard_projection,
    projection_has_aggregate,
};
use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::plan::validate::{
    reject_unhonored_query_clauses, reject_unhonored_select_clauses, unsupported_clause, validate_user_name,
    HonoredClauses, HonoredQueryClauses,
};
use crate::plan::view::{exists, group_by, join, set_op, simple, ViewChain};
use crate::SqlResult;
use gnitz_core::{ColumnDef, GnitzClient, PlannedView, Schema};
use sqlparser::ast::{
    Expr, GroupByExpr, Ident, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor,
    UnaryOperator, WildcardAdditionalOptions,
};
use std::collections::HashSet;
use std::rc::Rc;

pub(crate) fn execute_create_view(
    client: &mut GnitzClient,
    schema_name: &str,
    view_name_obj: &sqlparser::ast::ObjectName,
    query: &Query,
    stmt: &Statement,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let view_name = extract_name(view_name_obj, "CREATE VIEW")?;
    validate_user_name(&view_name)?;

    // The CREATE VIEW envelope honors only `WITH` (inlined just below by `inline_ctes`); every
    // other tail clause (ORDER BY, LIMIT/OFFSET, FETCH, FOR UPDATE/SHARE, FOR XML/JSON, SETTINGS,
    // FORMAT) has no incremental-view semantics and would otherwise be silently dropped.
    reject_unhonored_query_clauses(
        query,
        HonoredQueryClauses {
            with: true,
            limit: false,
        },
        "CREATE VIEW",
    )?;

    let sql_text = format!("{stmt}");

    // Compile the sub-plans first. Pass-through CTEs inline into the binder cache;
    // non-pass-through CTEs and top-level derived tables compile into hidden view
    // segments on `chain`, registered in the binder so the final body below
    // resolves them by name.
    let mut chain = ViewChain::new();
    inline_ctes(client, query, binder, &mut chain)?;
    compile_derived_tables(client, query, binder, &mut chain)?;

    // Classify the final body's shape once (the load-bearing precedence the old
    // guard ladder encoded), then emit its circuit pieces.
    let shape = ViewShape::classify(query)?;
    let final_vid = chain.owner_vid(client)?;
    let (circuit, out_cols, pk_cols) = match shape {
        ViewShape::SetOp {
            op,
            set_quantifier,
            left,
            right,
        } => set_op::emit_set_op_pieces(client, final_vid, op, set_quantifier, left, right, binder)?,
        // DISTINCT / GROUP BY resolve their input relation first: a plain FROM
        // resolves through the binder; a join FROM compiles to a hidden view H whose
        // circuit already applied the top-level WHERE, and the operator runs over H,
        // resolving its projection / group columns against H by name. `resolve_operator_input`
        // returns the `Select` the operator should see — with `selection` cleared on
        // the join path, so the WHERE is never re-applied over H.
        ViewShape::Distinct(select) => {
            let (src, op_select) = resolve_operator_input(client, binder, select, &mut chain, "SELECT DISTINCT")?;
            set_op::emit_distinct_pieces(final_vid, &op_select, src)?
        }
        ViewShape::GroupBy(select) => {
            let (src, op_select) = resolve_operator_input(client, binder, select, &mut chain, "GROUP BY")?;
            group_by::emit_group_by_pieces(client, final_vid, &op_select, src)?
        }
        // Any join FROM — 2-way, N-way, or self-referential — plans as a left-deep
        // chain; intermediate segments and self-join pass-through wrappers land on
        // `chain`, and the final step is emitted with `final_vid`.
        ViewShape::Join(select) => join::plan_join_chain(client, binder, final_vid, select, &mut chain)?,
        ViewShape::Simple(select) => {
            // Linear filter/map view: lower to `Project(Filter?(Source))`, then emit.
            let rel = crate::plan::lp::lower_linear(client, binder, select)?;
            simple::emit_linear(final_vid, rel)?
        }
        ViewShape::Subquery {
            select,
            subq,
            outer_not,
            local,
        } => {
            // The exists builder resolves its two relations against base tables
            // only; composing it over compiled CTE/derived-table sub-plans is an
            // unsupported combination.
            if !chain.segments.is_empty() {
                return Err(GnitzSqlError::Unsupported(
                    "an EXISTS/IN subquery over a compiled CTE/derived-table sub-plan is not supported".to_string(),
                ));
            }
            exists::emit_exists_pieces(client, final_vid, select, subq, outer_not, &local, binder)?
        }
    };

    // One atomic bundle: the hidden segments (dependency order) then the
    // user-named final view.
    let mut segments = chain.segments;
    segments.push(PlannedView {
        name: view_name,
        sql_text,
        circuit,
        output_columns: out_cols,
        pk_cols,
    });
    client
        .create_view_chain(schema_name, segments)
        .map_err(GnitzSqlError::Exec)?;
    Ok(SqlResult::ViewCreated { view_id: final_vid })
}

/// The classified shape of a CREATE VIEW body. Classifying once — in this fixed
/// order — preserves the precedence the old guard ladder encoded: a set
/// operation outranks everything; `DISTINCT` is checked before the single-FROM
/// requirement; a grouped/aggregate JOIN classifies as `GroupBy` (its input
/// resolution compiles the join to a hidden view). Everything else is a `Simple`
/// filter/map view.
enum ViewShape<'a> {
    SetOp {
        op: SetOperator,
        set_quantifier: SetQuantifier,
        left: &'a SetExpr,
        right: &'a SetExpr,
    },
    /// `SELECT DISTINCT …` — over a plain FROM or a join (the input resolution
    /// compiles a join to a hidden view first).
    Distinct(&'a Select),
    /// A plain (non-grouped) join FROM.
    Join(&'a Select),
    /// A Simple-shaped view whose WHERE carries exactly one top-level
    /// `[NOT] EXISTS (…)` / `x [NOT] IN (SELECT …)` conjunct — routed to the
    /// semi/anti-join builder.
    Subquery {
        select: &'a Select,
        /// The peeled `Exists`/`InSubquery` node (`Nested` and `NOT` wrappers
        /// removed).
        subq: &'a Expr,
        /// True when a net-negating stack of `NOT` wrappers surrounded the
        /// node; the builder folds it into the node's own `negated` flag.
        outer_not: bool,
        /// The remaining WHERE conjuncts, filtering the outer relation.
        local: Vec<&'a Expr>,
    },
    GroupBy(&'a Select),
    Simple(&'a Select),
}

/// Peel `Nested` wrappers and `NOT`s off a WHERE conjunct; when the core is a
/// subquery test (`EXISTS` / `IN (SELECT …)`), return it with the net-negation
/// flag — each peeled `NOT` toggles it, so `NOT (EXISTS …)` classifies like
/// `NOT EXISTS …` and a double negation cancels.
fn as_subquery_conjunct(e: &Expr) -> Option<(&Expr, bool)> {
    let mut cur = e;
    let mut outer_not = false;
    loop {
        match cur {
            Expr::Nested(inner) => cur = inner,
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => {
                outer_not = !outer_not;
                cur = expr;
            }
            _ => break,
        }
    }
    matches!(cur, Expr::Exists { .. } | Expr::InSubquery { .. }).then_some((cur, outer_not))
}

impl<'a> ViewShape<'a> {
    fn classify(query: &'a Query) -> Result<ViewShape<'a>, GnitzSqlError> {
        // Set operations (UNION/INTERSECT/EXCEPT) outrank the single-SELECT shapes.
        let select = match query.body.as_ref() {
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                return Ok(ViewShape::SetOp {
                    op: *op,
                    set_quantifier: *set_quantifier,
                    left,
                    right,
                })
            }
            SetExpr::Select(s) => s,
            _ => {
                return Err(GnitzSqlError::Unsupported(
                    "CREATE VIEW only supports SELECT".to_string(),
                ))
            }
        };

        // DISTINCT is checked before the single-FROM requirement. Both plain DISTINCT and DISTINCT
        // ON route here; the shared side-compiler (compile_set_op_side) honors only FROM/WHERE/
        // projection and rejects every other clause (DISTINCT ON, GROUP BY, HAVING, PREWHERE, TOP,
        // …), none of which has incremental-view semantics here.
        if select.distinct.is_some() {
            return Ok(ViewShape::Distinct(select));
        }
        if select.from.len() != 1 {
            return Err(GnitzSqlError::Unsupported(
                "CREATE VIEW: only single FROM item supported".to_string(),
            ));
        }
        // A JOIN outranks the WHERE-subquery check; a grouped/aggregate join
        // classifies as GroupBy (its input resolution compiles the join to a
        // hidden view and the reduce runs over it).
        if !select.from[0].joins.is_empty() {
            return Ok(
                if group_by_is_present(&select.group_by) || projection_has_aggregate(select) {
                    ViewShape::GroupBy(select)
                } else {
                    ViewShape::Join(select)
                },
            );
        }
        // A single top-level `[NOT] EXISTS` / `[NOT] IN (SELECT …)` WHERE
        // conjunct routes to the semi/anti-join builder. Checked after JOIN (a
        // join-view WHERE is rejected wholesale by that builder) and before
        // GROUP BY, which cannot host the subquery in one circuit (the exists
        // builder rejects that combination with a targeted message).
        if let Some(selection) = &select.selection {
            let mut conjuncts = Vec::new();
            flatten_conjuncts(selection, &mut conjuncts);
            let mut subq: Option<(&Expr, bool)> = None;
            let mut local: Vec<&Expr> = Vec::new();
            for &c in &conjuncts {
                match as_subquery_conjunct(c) {
                    Some(peeled) => {
                        if subq.is_some() {
                            return Err(GnitzSqlError::Unsupported(
                                "at most one EXISTS/IN subquery conjunct per view WHERE; \
                                 stack views instead"
                                    .into(),
                            ));
                        }
                        subq = Some(peeled);
                    }
                    None => local.push(c),
                }
            }
            if let Some((node, outer_not)) = subq {
                return Ok(ViewShape::Subquery {
                    select,
                    subq: node,
                    outer_not,
                    local,
                });
            }
        }
        // GROUP BY *or* a no-`GROUP BY` aggregate (`SELECT MIN(x) FROM t`) routes
        // to the grouped builder: the latter is one logical group compiled as a
        // reduce with an empty group-column set. Routing it here (rather than to
        // `Simple`, which would refuse the aggregate in scalar lowering with a
        // less specific error) reuses the entire grouped pipeline — the
        // synthetic-PK branch and the strict projection validator both already
        // handle the empty group set. A `MIN(x)+1` is detected too and rejected
        // by that validator, not by `Simple`'s lowering.
        if group_by_is_present(&select.group_by) || projection_has_aggregate(select) {
            return Ok(ViewShape::GroupBy(select));
        }
        Ok(ViewShape::Simple(select))
    }
}

/// Whether a single-SELECT body compiles straight into a hidden view: a JOIN
/// body, or a single-table body carrying a WHERE, a GROUP BY, or an aggregate
/// projection. A projection-only reshape without any of those goes to the
/// pass-through path first (aliased if identity, else compiled after all). A
/// multi-FROM or DISTINCT body is not compilable here.
fn is_compilable_hidden_body(s: &Select) -> bool {
    if s.from.len() != 1 || s.distinct.is_some() {
        return false;
    }
    // A JOIN body compiles into a hidden join segment: it composes as a first-class
    // relation (its synthetic PK is handled by ordinary schema resolution — no `vis`).
    if !s.from[0].joins.is_empty() {
        return true;
    }
    // A single-table body compiles when it carries a WHERE, a GROUP BY, or an aggregate.
    s.selection.is_some() || group_by_is_present(&s.group_by) || projection_has_aggregate(s)
}

/// Apply positional column aliases (`WITH d(a, b) AS …` / `(subquery) AS d(a, b)`) to a
/// hidden segment's columns. Rejected for a JOIN body: its leading synthetic-PK region is
/// invisible, so positional aliases would misalign — alias in the subquery's SELECT.
fn apply_hidden_column_aliases(
    aliases: &[sqlparser::ast::TableAliasColumnDef],
    cols: &mut [ColumnDef],
    is_join: bool,
    ctx_name: &str,
) -> Result<(), GnitzSqlError> {
    if aliases.is_empty() {
        return Ok(());
    }
    if is_join {
        return Err(GnitzSqlError::Unsupported(format!(
            "{ctx_name}: column aliases on a JOIN subquery are not supported; alias columns in the \
             subquery's SELECT instead"
        )));
    }
    if aliases.len() != cols.len() {
        return Err(GnitzSqlError::Plan(format!(
            "{ctx_name} defines {} column aliases but body returns {} columns",
            aliases.len(),
            cols.len()
        )));
    }
    for (col, alias) in cols.iter_mut().zip(aliases) {
        col.name = alias.name.value.clone();
    }
    Ok(())
}

/// Inline a query's CTEs. A plain pass-through CTE (single table, identity
/// projection, no WHERE) is aliased into the binder cache. A compilable CTE body
/// (`is_compilable_hidden_body`) becomes a hidden view segment on `chain`,
/// registered under the CTE name so the final view resolves it.
fn inline_ctes(
    client: &mut GnitzClient,
    query: &Query,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
) -> Result<(), GnitzSqlError> {
    let Some(with) = &query.with else {
        return Ok(());
    };
    if with.recursive {
        return Err(GnitzSqlError::Unsupported("recursive CTEs not supported".to_string()));
    }
    for cte in &with.cte_tables {
        let ctx = format!("CTE '{}'", cte.alias.name.value);
        // Exhaustively destructure the `Cte` envelope so a future field can't be silently
        // dropped. `materialized` parses only under PostgreSqlDialect (always None here).
        let sqlparser::ast::Cte {
            alias: _,
            query: _,
            from: cte_from,
            materialized: _,
            closing_paren_token: _,
        } = cte;
        if cte_from.is_some() {
            return Err(unsupported_clause(&ctx, "a trailing FROM"));
        }
        // Reject the `Query`-envelope clauses a CTE body cannot honor (a nested `WITH`,
        // LIMIT/OFFSET, FETCH, FOR UPDATE/SHARE, SETTINGS, …) before the body match.
        reject_unhonored_query_clauses(
            &cte.query,
            HonoredQueryClauses {
                with: false,
                limit: false,
            },
            &ctx,
        )?;
        let cte_select = match cte.query.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => return Err(GnitzSqlError::Unsupported(format!("{ctx}: only SELECT supported"))),
        };
        if is_compilable_hidden_body(cte_select) {
            // A later CTE may reference an earlier one, so register immediately.
            let resolved = compile_hidden_body(client, binder, cte_select, &cte.alias.columns, &ctx, chain)?;
            binder.cache_alias(&cte.alias.name.value, resolved);
        } else {
            inline_passthrough_cte(client, cte, cte_select, binder, &ctx, chain)?;
        }
    }
    Ok(())
}

/// Alias a plain pass-through CTE (single table, identity projection, no WHERE) to
/// its source table in the binder cache — the fast path that adds no view. A
/// non-identity (subset/reordered/computed) projection is not aliasable and falls
/// back to `compile_hidden_body`, exactly like the same body as a derived table.
fn inline_passthrough_cte(
    client: &mut GnitzClient,
    cte: &sqlparser::ast::Cte,
    cte_select: &Select,
    binder: &mut Binder<'_>,
    ctx: &str,
    chain: &mut ViewChain,
) -> Result<(), GnitzSqlError> {
    // Reject every Select-body clause the pass-through cannot honor (WHERE — a WHERE'd
    // CTE is routed to `compile_hidden_body` before reaching here — DISTINCT, GROUP BY,
    // HAVING, and the exotic tail: PREWHERE, TOP, …).
    reject_unhonored_select_clauses(
        cte_select,
        HonoredClauses {
            where_filter: false,
            grouping: false,
            distinct: false,
        },
        ctx,
    )?;
    if cte_select.from.len() != 1 || !cte_select.from[0].joins.is_empty() {
        return Err(GnitzSqlError::Unsupported(format!(
            "{ctx}: only single table without JOINs"
        )));
    }
    let cte_table_name = extract_table_factor_name(&cte_select.from[0].relation, ctx)?;
    let (cte_tid, cte_schema) = binder.resolve(client, &cte_table_name)?;
    // Positional identity projection: `*`, or one identifier per source column in
    // order. The qualified form (`SELECT t.a, t.b FROM t`) parses as `CompoundIdentifier`
    // and is the same positional pass-through; a dup-named source fails the per-position
    // compare and compiles instead.
    let proj_is_identity = is_wildcard_projection(&cte_select.projection)
        || (cte_select.projection.len() == cte_schema.columns.len()
            && cte_select.projection.iter().enumerate().all(|(i, item)| {
                let want = &cte_schema.columns[i].name;
                match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.value.eq_ignore_ascii_case(want),
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                        parts[1].value.eq_ignore_ascii_case(want)
                    }
                    _ => false,
                }
            }));
    if !proj_is_identity {
        let resolved = compile_hidden_body(client, binder, cte_select, &cte.alias.columns, ctx, chain)?;
        binder.cache_alias(&cte.alias.name.value, resolved);
        return Ok(());
    }
    // Apply CTE column aliases (`WITH cte(a, b) AS ...`).
    let cte_schema = if !cte.alias.columns.is_empty() {
        let mut s = (*cte_schema).clone();
        apply_hidden_column_aliases(&cte.alias.columns, &mut s.columns, false, ctx)?;
        std::rc::Rc::new(s)
    } else {
        cte_schema
    };
    binder.cache_alias(&cte.alias.name.value, (cte_tid, cte_schema));
    Ok(())
}

/// Compile a hidden-segment body — a JOIN, a GROUP BY / aggregate (over a table
/// or a join), or a linear filter/map — into a hidden view pushed onto `chain`,
/// returning its `(view id, schema)`. Does **not** register the alias in the
/// binder: the caller owns that. The CTE callers register immediately (a later
/// CTE may reference an earlier one), while the derived-table caller defers
/// registration until the whole FROM is compiled (a sibling is out of scope). The
/// single routing site for chainable sub-plan bodies, shared by the CTE and
/// derived-table entries (which keep only their distinct envelope validation).
fn compile_hidden_body(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    select: &Select,
    column_aliases: &[sqlparser::ast::TableAliasColumnDef],
    ctx: &str,
    chain: &mut ViewChain,
) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
    // A sub-plan body's FROM must be plain relations: the front door pre-compiles
    // derived tables only in the view's top-level FROM, so a nested one would
    // mis-resolve by alias here.
    let from = &select.from[0];
    for tf in std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation)) {
        if matches!(tf, TableFactor::Derived { .. }) {
            return Err(GnitzSqlError::Unsupported(format!(
                "{ctx}: a nested derived table is not supported; use a CTE"
            )));
        }
    }
    // Route by body shape, mirroring the final-body classification: a grouped /
    // aggregate body emits a reduce (group key = natural PK) — over a hidden join
    // view when its FROM is a JOIN; a plain JOIN body plans as a join chain
    // (synthetic PK, but it composes as a first-class relation — no `vis`);
    // everything else is a filter/map.
    let grouped = group_by_is_present(&select.group_by) || projection_has_aggregate(select);
    let is_plain_join = !from.joins.is_empty() && !grouped;
    chain.add_segment(client, |client, chain, vid| {
        let (circuit, mut cols, pk) = if grouped {
            let (src, op_select) = resolve_operator_input(client, binder, select, chain, ctx)?;
            group_by::emit_group_by_pieces(client, vid, &op_select, src)?
        } else if is_plain_join {
            join::plan_join_chain(client, binder, vid, select, chain)?
        } else {
            let rel = crate::plan::lp::lower_linear(client, binder, select)?;
            simple::emit_linear(vid, rel)?
        };
        // Positional column aliases (`WITH d(a, b) AS …` / `(subquery) AS d(a, b)`).
        apply_hidden_column_aliases(column_aliases, &mut cols, is_plain_join, ctx)?;
        Ok((circuit, cols, pk))
    })
}

/// Resolve a DISTINCT / GROUP BY operator's input relation — the source
/// `(id, schema)` — and the `Select` the operator should evaluate over it: a JOIN
/// FROM compiles to a hidden view H on `chain` first (the join circuit already
/// consumed the top-level WHERE), so the operator receives a clone with
/// `selection` cleared — it must not re-apply the WHERE over H. A plain FROM
/// resolves through the binder and keeps the `Select` as written (its WHERE is a
/// filter over one base relation, already minimal).
fn resolve_operator_input(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    select: &Select,
    chain: &mut ViewChain,
    context: &str,
) -> Result<((u64, Rc<Schema>), Select), GnitzSqlError> {
    if select.from.len() != 1 {
        return Err(GnitzSqlError::Unsupported(format!(
            "{context}: only a single table without JOINs is supported"
        )));
    }
    let mut op_select = select.clone();
    let src = if !select.from[0].joins.is_empty() {
        op_select.selection = None;
        compile_join_to_hidden(client, binder, select, chain)?
    } else {
        let name = extract_relation_name(&select.from[0].relation, context)?;
        binder.resolve(client, &name)?
    };
    Ok((src, op_select))
}

/// Compile every derived table in the query's top-level FROM (the base relation
/// and each join's relation) into a hidden view segment on `chain`, registered
/// under its alias so the final view resolves it. Only a single-SELECT body has
/// a scannable top-level FROM; set-op branches and DML keep the strict
/// no-derived-tables rule (`extract_table_factor_name`).
fn compile_derived_tables(
    client: &mut GnitzClient,
    query: &Query,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
) -> Result<(), GnitzSqlError> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(());
    };
    if select.from.len() != 1 {
        return Ok(());
    }
    let from = &select.from[0];
    // A non-LATERAL derived table may reference CTEs and catalog relations but not
    // a sibling FROM item. `compile_hidden_body` returns the compiled alias without
    // registering it, so nothing a sibling compiles is visible to a later sibling;
    // register them all only after the whole FROM is compiled. A later sibling
    // naming an earlier one thus falls through to a catalog relation (or errors) —
    // never the sibling — and, in the final body, a sibling alias shadows a
    // same-named CTE (standard SQL).
    let mut deferred: Vec<(String, (u64, Rc<Schema>))> = Vec::new();
    for tf in std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation)) {
        let TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } = tf
        else {
            continue;
        };
        let Some(alias) = alias else {
            return Err(GnitzSqlError::Unsupported(
                "a derived table (subquery in FROM) needs an alias".to_string(),
            ));
        };
        let ctx = format!("derived table '{}'", alias.name.value);
        if *lateral {
            return Err(GnitzSqlError::Unsupported(format!("{ctx}: LATERAL is not supported")));
        }
        reject_unhonored_query_clauses(
            subquery,
            HonoredQueryClauses {
                with: false,
                limit: false,
            },
            &ctx,
        )?;
        let sub_select = match subquery.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => {
                return Err(GnitzSqlError::Unsupported(format!(
                    "{ctx}: only a SELECT subquery is supported"
                )))
            }
        };
        // A JOIN, single-table filter/projection, or GROUP BY / aggregate subquery is
        // chained; a multi-FROM or DISTINCT derived table is not supported.
        if sub_select.from.len() != 1 || sub_select.distinct.is_some() {
            return Err(GnitzSqlError::Unsupported(format!(
                "{ctx}: only a single-FROM, non-DISTINCT subquery is supported"
            )));
        }
        let resolved = compile_hidden_body(client, binder, sub_select, &alias.columns, &ctx, chain)?;
        deferred.push((alias.name.value.clone(), resolved));
    }
    // The final body resolves every sibling; a sibling alias shadows a same-named CTE.
    for (name, resolved) in deferred {
        binder.cache_alias(&name, resolved);
    }
    Ok(())
}

/// Compile a `select`'s join input into hidden segment(s) on `chain`, ending in
/// a full-column view `H`. Returns `(H's view id, H's schema)` for the operator
/// that runs over H. The top-level WHERE stays in `H` — the join circuit consumes
/// it (`classify_join_where`), so `H` materializes the *filtered* join. Only the
/// grouping / HAVING / DISTINCT / projection clauses are stripped for `H`: they
/// belong to that outer operator, which resolves H's columns by name.
fn compile_join_to_hidden(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    select: &Select,
    chain: &mut ViewChain,
) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
    // The join projected to only the columns the outer operator reads over `H`
    // (the join's ON stays in `from`, the WHERE in `selection` — the join circuit
    // filters it); GROUP/HAVING/DISTINCT are the outer operator's, so blank them
    // here so they never enter the join.
    let mut join_select = select.clone();
    join_select.projection = match collect_operator_names(select) {
        Some(names) => names
            .into_iter()
            .map(|n| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(n))))
            .collect(),
        None => vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
    };
    join_select.group_by = GroupByExpr::Expressions(Vec::new(), Vec::new());
    join_select.having = None;
    join_select.distinct = None;

    chain.add_segment(client, |client, chain, vid| {
        join::plan_join_chain(client, binder, vid, &join_select, chain)
    })
}

/// The bare column names a DISTINCT / GROUP BY operator evaluates over its hidden
/// join input `H`: the projection's column refs (group cols + aggregate arguments,
/// or the DISTINCT items), the GROUP BY expressions, and HAVING — collected in
/// BARE-NAME mode (qualifier discarded), deduped case-insensitively (first spelling
/// kept). Returns `None` when a wildcard projection item is present, GROUP BY ALL is
/// used, or nothing is collected (e.g. `SELECT COUNT(*)`), so the caller keeps the
/// wildcard `H` — the degenerate no-pruning case. Bare-name collection mirrors the
/// operator's own name binding over `H` (`bind_single_table` discards the
/// qualifier), so it keeps every same-named candidate and leaves the deterministic
/// "ambiguous column" behavior unchanged — collecting `SUM(a.amt)` as
/// qualified-to-`a` would instead prune a same-named `b.amt` and turn that error
/// into a liveness-dependent acceptance.
fn collect_operator_names(select: &Select) -> Option<Vec<String>> {
    // GROUP BY ALL is rejected downstream; keep the wildcard rather than mis-prune.
    if matches!(select.group_by, GroupByExpr::All(_)) {
        return None;
    }
    let mut refs: Vec<(Option<&str>, &str)> = Vec::new();
    // A wildcard projection (e.g. `SELECT DISTINCT *`) is the no-pruning case.
    if !collect_projection_column_refs(&select.projection, &mut refs) {
        return None;
    }
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for e in exprs {
            collect_column_refs(e, &mut refs);
        }
    }
    if let Some(h) = &select.having {
        collect_column_refs(h, &mut refs);
    }
    let mut seen: HashSet<String> = HashSet::new();
    let mut names: Vec<String> = Vec::new();
    for (_, name) in refs {
        if seen.insert(name.to_ascii_lowercase()) {
            names.push(name.to_string());
        }
    }
    (!names.is_empty()).then_some(names)
}
