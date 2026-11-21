//! CREATE / ALTER VIEW: decode the statement's own clauses and options, compile
//! the body (`hir::bind_and_lower`) into a view bundle carrying the definition it
//! records, and commit it atomically.

use super::options::decode_view_options;
use super::view_def::{RelationRef, ViewDefinition};
use crate::catalog::Catalog;
use crate::error::{GnitzSqlError, missing_relation, reject_if};
use crate::hir::{BoundRef, LoweredView, ViewBody};
use crate::names::extract_object_name;
use crate::rules::{ClassWant, require_class};
use crate::source::StatementText;
use gnitz_core::{RelName, ViewBundle};
use gnitz_wire::{RelClass, ViewBudget, ViewProps};
use sqlparser::ast::{AlterView, AlterViewOperation, CreateView, Ident, Query};
use sqlparser::tokenizer::Location;

/// Reject every `CREATE VIEW` clause the planner does not consume.
fn reject_unhonored_create_view_clauses(cv: &CreateView) -> Result<(), GnitzSqlError> {
    const CTX: &str = "CREATE VIEW";
    let CreateView {
        name: _,
        query: _,
        options: _,
        materialized: _, // accepted: names gnitz's real behavior
        // Consumed here (they are mutually exclusive) and by the planner's
        // replace / skip routes.
        or_replace,
        if_not_exists,
        name_before_not_exists: _, // positional flag for if_not_exists
        with_no_schema_binding: _, // no-op optimizer hint
        secure: _,                 // Snowflake SECURE modifier: no result impact
        copy_grants: _,            // Snowflake COPY GRANTS: no result impact
        columns,                   // consumed: positional output aliases, checked for decorations below
        params: _,                 // cannot populate under GenericDialect
        // Rejected: each would be silently dropped.
        cluster_by,
        comment,
        or_alter,
        temporary,
        to,
    } = cv;
    reject_if(*or_alter, CTX, "OR ALTER (spell it OR REPLACE)")?;
    reject_if(*temporary, CTX, "TEMPORARY")?;
    reject_if(to.is_some(), CTX, "TO (target table)")?;
    // `CREATE TABLE` honours CLUSTER BY; a view would silently build unclustered.
    reject_if(!cluster_by.is_empty(), CTX, "CLUSTER BY")?;
    reject_if(comment.is_some(), CTX, "COMMENT")?;
    // A view column alias names a column and nothing else: a declared type or a
    // column option would have to be checked against the body's derived type or
    // silently ignored, and gnitz does neither.
    for col in columns {
        let sqlparser::ast::ViewColumnDef { name: _, data_type, options } = col;
        reject_if(data_type.is_some(), CTX, "a type on an output column alias")?;
        reject_if(options.is_some(), CTX, "an option list on an output column alias")?;
    }
    // `sqlparser` parses the pair; no dialect defines it.
    if *or_replace && *if_not_exists {
        return Err(GnitzSqlError::rejected(format!(
            "{CTX}: OR REPLACE and IF NOT EXISTS ask for opposite outcomes — replace what \
             is there, or leave what is there alone. Write one of them."
        )));
    }
    Ok(())
}

/// Plan a `CREATE VIEW`; `None` when `IF NOT EXISTS` finds the name taken.
pub(crate) fn plan_create_view(
    cv: &CreateView,
    text: StatementText<'_>,
    cat: &dyn Catalog,
) -> Result<Option<ViewBundle>, GnitzSqlError> {
    reject_unhonored_create_view_clauses(cv)?;
    let view_name = extract_object_name(&cv.name, cat.schema_name(), "CREATE VIEW")?;
    let props = decode_view_options(&cv.options)?;

    // Each clause tests the name alone; a plain create leaves a taken name to
    // the engine's refusal.
    let standing = match cv.or_replace || cv.if_not_exists {
        true => cat.probe(&view_name),
        false => None,
    };
    let replacing = match standing {
        // A free name: `OR REPLACE` degenerates to a plain create.
        None => None,
        // `IF NOT EXISTS`: any relation under the name ends the statement,
        // whatever its kind.
        Some(_) if cv.if_not_exists => return Ok(None),
        Some(rel) => {
            require_class(&rel, &view_name, ClassWant::View, "CREATE OR REPLACE VIEW")?;
            Some(rel.tid)
        }
    };

    let view = ViewBody {
        stmt: "CREATE VIEW",
        replacing,
        bounded: matches!(props.budget, ViewBudget::Bounded { .. }),
    };
    let aliases = cv.columns.iter().map(|c| &c.name);
    plan_segments(cat, view_name, view, &cv.query, text, aliases, Some(props)).map(Some)
}

/// Plan an `ALTER VIEW [IF EXISTS] <name> [(columns)] AS <query>`: the chain that
/// supersedes the view, under the options it already has; `None` when `IF
/// EXISTS` finds no relation under the name.
pub(crate) fn plan_alter_view(
    av: &AlterView,
    text: StatementText<'_>,
    cat: &dyn Catalog,
) -> Result<Option<ViewBundle>, GnitzSqlError> {
    const CTX: &str = "ALTER VIEW";
    let AlterView {
        name,
        materialized: _, // accepted: names gnitz's real behavior
        if_exists,
        operation,
    } = av;
    let AlterViewOperation::As { columns, query, with_options } = operation else {
        return Err(GnitzSqlError::rejected(format!(
            "{CTX}: only `AS <query>` is supported"
        )));
    };
    // Only `CREATE OR REPLACE VIEW` states a view's options.
    reject_if(!with_options.is_empty(), CTX, "WITH options")?;
    let named = extract_object_name(name, cat.schema_name(), CTX)?;
    let old = match cat.probe(&named) {
        Some(old) => old,
        None if *if_exists => return Ok(None),
        None => return Err(missing_relation(&named)),
    };
    require_class(&old, &named, ClassWant::View, CTX)?;
    let view = ViewBody {
        stmt: CTX,
        replacing: Some(old.tid),
        bounded: old.class == RelClass::BoundedView,
    };
    plan_segments(cat, named, view, query, text, columns.iter(), None).map(Some)
}

/// Compile a view body to its segment bundle under `name`, the user-named view's
/// visible output columns renamed by `aliases`, carrying the definition `text`
/// declares and the options `props`.
fn plan_segments<'a>(
    cat: &dyn Catalog,
    name: RelName,
    view: ViewBody,
    query: &Query,
    text: StatementText<'_>,
    aliases: impl ExactSizeIterator<Item = &'a Ident>,
    props: Option<ViewProps>,
) -> Result<ViewBundle, GnitzSqlError> {
    let ViewBody { stmt, replacing, bounded } = view;
    let LoweredView { segments, view, refs, body_columns } = crate::hir::bind_and_lower(cat, query, view, aliases)?;
    // A body whose inputs cut a segment of their own bounds a view over unbounded copies.
    if bounded && !segments.is_empty() {
        return Err(GnitzSqlError::rejected(format!(
            "{stmt}: a capacity-bounded view's body that compiles to more than one view is not supported"
        )));
    }
    let definition = record_definition(text, refs, body_columns)?;
    Ok(ViewBundle {
        name,
        props,
        replacing,
        segments,
        view,
        definition: definition.encode(),
    })
}

/// The definition of the view `text` declares, as `CREATE VIEW` and `ALTER VIEW`
/// record it: the body's text and every place it names a catalog relation
/// (`refs`, as binding found them), relative to that text.
fn record_definition(
    text: StatementText<'_>,
    refs: Vec<BoundRef>,
    body_columns: Vec<String>,
) -> Result<ViewDefinition, GnitzSqlError> {
    let unrecorded = |why: &str| GnitzSqlError::rejected(format!("cannot record the view's definition: {why}"));
    let body = text
        .body_after_as()
        .ok_or_else(|| unrecorded("no AS ahead of the body"))?;
    let query = text.script[body.clone()].to_string();
    // A location in the script, as an offset into the body.
    let in_body = |at: Location| -> Result<u32, GnitzSqlError> {
        text.offset(at)
            .and_then(|at| at.checked_sub(body.start))
            .filter(|&at| at <= query.len())
            .and_then(|at| u32::try_from(at).ok())
            .ok_or_else(|| unrecorded("a relation reference lies outside the body"))
    };
    let mut recorded = Vec::with_capacity(refs.len());
    for r in refs {
        recorded.push(RelationRef {
            tid: r.tid,
            name: in_body(r.name_span.start)?..in_body(r.name_span.end)?,
            alias_end: r.alias_span.map(|s| in_body(s.end)).transpose()?,
            alias: r.alias,
            columns: r.columns,
        });
    }
    // A body bound twice over one stretch of text names each relation once.
    recorded.sort_by_key(|r| r.name.start);
    recorded.dedup_by_key(|r| r.name.start);
    let def = ViewDefinition { query, body_columns, refs: recorded };
    // Held to exactly the rules a reader holds it to.
    def.check().map_err(|e| unrecorded(&e))?;
    Ok(def)
}
