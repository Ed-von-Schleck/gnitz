//! Ad-hoc `WITH`: every CTE is a macro over one plain SELECT, expanded into the
//! body that names it, and the flat query that comes out is what the read route
//! plans. A fold (GROUP BY, HAVING, an aggregate) is not a macro — it derives a
//! relation, which is what a view is for. A view body's CTEs bind to shared
//! subtrees instead (`hir::bind::bind_ctes`).
//!
//! Expansion writes names down, so `*` over a CTE exposing two same-named
//! columns is refused where the flat wildcard passes them positionally.

use crate::ast_util::{
    body_is_grouped, classify_from, col_ref_parts, expand_wildcard_item, expr_node_count, expr_operands_mut,
    extract_table_name_and_alias, is_bare_wildcard_projection, object_name_ident, scalar_projection_item, FromShape,
};
use crate::bind::{apply_positional_aliases, reject_foreign_qualifier, single_relation_col_idx, Binder};
use crate::error::{derivation, unsupported_clause, GnitzSqlError};
use crate::validate::{
    as_plain_select, computed_column_name, cte_body, non_recursive_ctes, reject_duplicate_projection_names,
    reject_unhonored_select_clauses, validate_user_name, HonoredClauses,
};
use gnitz_core::{CatalogSnapshot, ColumnDef, TypeCode};
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, OrderBy, OrderByKind, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableWithJoins, WildcardAdditionalOptions,
};

/// The ceiling on one CTE's expanded output expressions: a chain naming each
/// predecessor's column twice is `2^n` nodes from `O(n)` bytes of SQL, and the
/// planner runs in the caller's own process. Far above any hand-written CTE.
const MAX_CTE_EXPANDED_NODES: usize = 10_000;

struct Cte {
    name: String,
    /// The columns this CTE exposes and the expression behind each; `None` for a
    /// bare `*`, which exposes the source as it is. Defs, because the helpers
    /// here take them; none reads the type, which is why [`name_only`] supplies
    /// one.
    cols: Option<Vec<(ColumnDef, Expr)>>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
}

/// Expand `query`'s CTEs into its body. `None` when it has no `WITH`.
pub(super) fn inline_ctes(
    cat: &CatalogSnapshot,
    binder: &Binder<'_>,
    query: &Query,
) -> Result<Option<Query>, GnitzSqlError> {
    let ctes = non_recursive_ctes(query)?;
    if ctes.is_empty() {
        return Ok(None);
    }
    let mut macros: Vec<Cte> = Vec::new();
    for cte in ctes {
        let name = cte.alias.name.value.clone();
        validate_user_name(&name)?;
        let ctx = format!("CTE '{name}'");
        let body = as_plain_select(cte_body(cte, &ctx)?, &ctx)?;
        reject_unhonored_select_clauses(body, HonoredClauses::for_body(true, false), &ctx)?;
        if body_is_grouped(body) {
            return Err(derivation("grouped CTE"));
        }
        let mut body = body.clone();
        expand(&macros, &mut body, None, binder.schema_name())?;
        let cols = if is_bare_wildcard_projection(&body.projection) && cte.alias.columns.is_empty() {
            None
        } else {
            let mut cols = cte_columns(cat, binder, &body, &ctx)?;
            apply_positional_aliases(
                cte.alias.columns.iter().map(|a| &a.name),
                cols.iter_mut().map(|(d, _)| d).collect(),
                &ctx,
            )?;
            // The projection-shaped check, not the raw one: a wildcard naming no
            // output column of its own carries a duplicate among the *source's*
            // names through positionally, exactly as the flat query would.
            reject_duplicate_projection_names(&body.projection, cols.iter().map(|(d, _)| d), &ctx)?;
            reject_oversized_expansion(&cols, &ctx)?;
            Some(cols)
        };
        macros.push(Cte {
            name,
            cols,
            from: body.from,
            selection: body.selection,
        });
    }
    let mut flat = query.clone();
    flat.with = None;
    if let SetExpr::Select(sel) = flat.body.as_mut() {
        expand(&macros, sel, flat.order_by.as_mut(), binder.schema_name())?;
    }
    Ok(Some(flat))
}

/// A CTE body's output columns by name. A wildcard needs the source schema, so
/// the FROM must be one relation; any other FROM derives and is rejected as the
/// route would reject it.
fn cte_columns(
    cat: &CatalogSnapshot,
    binder: &Binder<'_>,
    body: &Select,
    ctx: &str,
) -> Result<Vec<(ColumnDef, Expr)>, GnitzSqlError> {
    let mut cols = Vec::new();
    for (idx, item) in body.projection.iter().enumerate() {
        let SelectItem::Wildcard(o) = item else {
            let (expr, alias) = scalar_projection_item(item, ctx)?;
            let name = alias
                .or_else(|| written_name(expr).map(str::to_string))
                .unwrap_or_else(|| computed_column_name(idx));
            cols.push((name_only(&name), expr.clone()));
            continue;
        };
        let tf = match classify_from(&body.from) {
            FromShape::SinglePlainRelation(tf) => tf,
            FromShape::Empty => return Err(unsupported_clause(ctx, "SELECT * without FROM")),
            FromShape::Derived(c) => return Err(derivation(c)),
        };
        let (table, _) = extract_table_name_and_alias(tf, binder.schema_name(), ctx)?;
        let desc = binder.resolve(cat, &table)?;
        for (i, def) in expand_wildcard_item(o, &desc.schema.columns, ctx)? {
            cols.push((def, Expr::Identifier(Ident::new(desc.schema.columns[i].name.clone()))));
        }
    }
    Ok(cols)
}

/// A def for a column nothing here binds, so nothing here can type: name-only,
/// for the by-name helpers [`Cte::cols`] feeds.
fn name_only(name: &str) -> ColumnDef {
    ColumnDef::new(name, TypeCode::I64, false)
}

/// The name a SELECT item exposes when it carries no alias: the column it
/// references, else none — the caller supplies its own fallback.
fn written_name(e: &Expr) -> Option<&str> {
    col_ref_parts(e).map(|(_, n)| n)
}

/// Reject a CTE whose expansion has outgrown [`MAX_CTE_EXPANDED_NODES`].
fn reject_oversized_expansion(cols: &[(ColumnDef, Expr)], ctx: &str) -> Result<(), GnitzSqlError> {
    let nodes: usize = cols.iter().map(|(_, e)| expr_node_count(e)).sum();
    if nodes > MAX_CTE_EXPANDED_NODES {
        return Err(GnitzSqlError::Unsupported(format!(
            "{ctx} expands to {nodes} expression nodes, over the limit of {MAX_CTE_EXPANDED_NODES}.\n\
             CREATE VIEW <name> AS <the CTE body> — the engine maintains it incrementally — then SELECT from it."
        )));
    }
    Ok(())
}

/// Expand into `sel` the macro its FROM names, if any: the FROM becomes the
/// macro's, the WHEREs conjoin, and every reference to a macro column becomes the
/// expression behind it. A name the macro does not expose is not found.
fn expand(
    macros: &[Cte],
    sel: &mut Select,
    order_by: Option<&mut OrderBy>,
    schema_name: &str,
) -> Result<(), GnitzSqlError> {
    let FromShape::SinglePlainRelation(tf) = classify_from(&sel.from) else {
        return Ok(());
    };
    let (name, alias) = extract_table_name_and_alias(tf, schema_name, "FROM")?;
    // A later CTE shadows an earlier one of the same name.
    let Some(m) = macros.iter().rev().find(|m| m.name.eq_ignore_ascii_case(&name)) else {
        return Ok(());
    };
    sel.from = m.from.clone();

    let mut projection = Vec::with_capacity(sel.projection.len());
    for item in sel.projection.drain(..) {
        match item {
            SelectItem::Wildcard(o) => push_wildcard(m, &o, &mut projection)?,
            // `alias.*` over the macro is `*` over it.
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(q), o)
                if object_name_ident(&q).is_some_and(|id| id.value.eq_ignore_ascii_case(&alias)) =>
            {
                push_wildcard(m, &o, &mut projection)?
            }
            SelectItem::UnnamedExpr(mut e) => {
                let written = written_name(&e).map(str::to_string);
                subst(&mut e, m, &alias)?;
                projection.push(match written {
                    Some(n) => item_for(&n, e),
                    None => SelectItem::UnnamedExpr(e),
                });
            }
            SelectItem::ExprWithAlias { mut expr, alias: a } => {
                subst(&mut expr, m, &alias)?;
                projection.push(SelectItem::ExprWithAlias { expr, alias: a });
            }
            other => projection.push(other),
        }
    }
    sel.projection = projection;

    if let Some(w) = sel.selection.as_mut() {
        subst(w, m, &alias)?;
    }
    sel.selection = match (m.selection.clone(), sel.selection.take()) {
        (Some(a), Some(b)) => Some(Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(a))),
            op: BinaryOperator::And,
            right: Box::new(Expr::Nested(Box::new(b))),
        }),
        (a, b) => a.or(b),
    };
    if let GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
        for e in exprs {
            subst(e, m, &alias)?;
        }
    }
    if let Some(h) = sel.having.as_mut() {
        subst(h, m, &alias)?;
    }
    // An ORDER BY name resolves output-first, so one naming an output column
    // stays as written; every other key is an expression over the macro.
    if let Some(OrderBy { kind: OrderByKind::Expressions(keys), .. }) = order_by {
        let out_names: Vec<&str> = sel
            .projection
            .iter()
            .filter_map(|it| match it {
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.as_str()),
                SelectItem::UnnamedExpr(e) => written_name(e),
                _ => None,
            })
            .collect();
        for k in keys {
            if let Some((None, n)) = col_ref_parts(&k.expr) {
                if out_names.iter().any(|o| o.eq_ignore_ascii_case(n)) {
                    continue;
                }
            }
            subst(&mut k.expr, m, &alias)?;
        }
    }
    Ok(())
}

/// `*` over the macro: the macro's own columns, through the wildcard's
/// modifiers; a bare-`*` macro exposes the source, so `*` stays `*`.
fn push_wildcard(m: &Cte, o: &WildcardAdditionalOptions, out: &mut Vec<SelectItem>) -> Result<(), GnitzSqlError> {
    let Some(cols) = &m.cols else {
        out.push(SelectItem::Wildcard(o.clone()));
        return Ok(());
    };
    for (i, def) in expand_wildcard_item(o, cols.iter().map(|(d, _)| d), "SELECT")? {
        out.push(item_for(&def.name, cols[i].1.clone()));
    }
    Ok(())
}

/// The outer item for macro column `name`: the expression it stands for, under
/// that name unless the expression already carries it.
fn item_for(name: &str, expr: Expr) -> SelectItem {
    match expr {
        Expr::Identifier(id) if id.value == name => SelectItem::UnnamedExpr(Expr::Identifier(id)),
        expr => SelectItem::ExprWithAlias { expr, alias: Ident::new(name) },
    }
}

/// Replace every reference to a macro column in `e` with the expression behind
/// it; `alias` is what a written qualifier must name.
fn subst(e: &mut Expr, m: &Cte, alias: &str) -> Result<(), GnitzSqlError> {
    let is_col_ref = match e {
        Expr::Identifier(_) => true,
        Expr::CompoundIdentifier(p) => p.len() == 2,
        _ => false,
    };
    if !is_col_ref {
        for o in expr_operands_mut(e) {
            subst(o, m, alias)?;
        }
        return Ok(());
    }
    match &m.cols {
        // A bare-`*` macro exposes the source as it is, so the name stands and
        // only the qualifier is the macro's to check. The written qualifier is
        // dropped with it: the FROM is now the source, under its own name.
        None => {
            let (qual, name) = col_ref_parts(e).expect("a column reference");
            reject_foreign_qualifier(qual, name, alias)?;
            *e = Expr::Identifier(Ident::new(name.to_string()));
        }
        Some(cols) => {
            let i = single_relation_col_idx(cols.iter().map(|(d, _)| d), alias, e)?;
            let sub = &cols[i].1;
            // Parenthesized unless it already binds tighter than any operator it
            // can land under, so `q * 2` over `q = v + 1` stays `(v + 1) * 2`.
            *e = match sub {
                Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) | Expr::Nested(_) => sub.clone(),
                _ => Expr::Nested(Box::new(sub.clone())),
            };
        }
    }
    Ok(())
}
