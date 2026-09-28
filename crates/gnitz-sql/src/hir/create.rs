//! CREATE / ALTER VIEW front door: validate the query envelope, then drive the
//! HIR pipeline — the CTE phase (`hir::bind::bind_ctes`) and then
//! `bind_and_lower` of the body — into a view bundle committed atomically.

use crate::bind::{apply_positional_aliases, Catalog};
use crate::error::{reject_if, GnitzSqlError};
use crate::hir::bind::ViewBody;
use crate::validate::{
    kv_options, reject_unhonored_create_view_clauses, reject_unhonored_query_clauses, require_class, ClassWant,
    QueryEnvelope,
};
use crate::SqlResult;
use gnitz_core::{GnitzClient, RelClass, ViewBundle, ViewProps};
use sqlparser::ast::{CreateTableOptions, CreateView, Ident, ObjectName, Query, Value, ValueWithSpan};
use std::sync::Arc;

/// Binary units accepted by a `WITH (<option> = '<uint><unit>')` size string.
const SIZE_UNITS: [(&str, u64); 3] = [("KB", 1 << 10), ("MB", 1 << 20), ("GB", 1 << 30)];

/// Decode the `WITH (...)` clause of a `CREATE VIEW`.
fn decode_view_options(options: &CreateTableOptions) -> Result<ViewProps, GnitzSqlError> {
    let [capacity, delta] = kv_options(options, "CREATE VIEW", ["capacity", "delta"])?;
    ViewProps::from_budgets(
        capacity.map(|v| size_option("capacity", v)).transpose()?,
        delta.map(|v| size_option("delta", v)).transpose()?,
    )
    .map_err(|e| GnitzSqlError::Unsupported(format!("CREATE VIEW WITH (capacity …, delta …): {e}")))
}

/// The byte count of one size-valued `CREATE VIEW` option.
fn size_option(option: &str, value: &sqlparser::ast::Expr) -> Result<u64, GnitzSqlError> {
    let sqlparser::ast::Expr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(text), ..
    }) = value
    else {
        return Err(GnitzSqlError::Plan(format!(
            "CREATE VIEW option `{option}` takes a single-quoted size string, e.g. '256 MB'"
        )));
    };
    parse_size(option, text)
}

/// `<uint><unit>` with an optional space, unit in {KB, MB, GB}, binary
/// (KB = 2^10). Zero and a `u64`-overflowing product are rejected: a zero
/// capacity names a store that cannot hold its own skeleton, and a zero delta
/// budget one that retains nothing.
fn parse_size(option: &str, text: &str) -> Result<u64, GnitzSqlError> {
    let bad = || {
        GnitzSqlError::Plan(format!(
            "CREATE VIEW option `{option}`: '{text}' is not a size like '256 MB' \
             (a positive integer followed by KB, MB or GB)"
        ))
    };
    let trimmed = text.trim();
    let digits = trimmed.len() - trimmed.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    let (num, unit) = trimmed.split_at(digits);
    let num: u64 = num.parse().map_err(|_| bad())?;
    let unit = unit.trim();
    let mult = SIZE_UNITS
        .iter()
        .find(|(u, _)| unit.eq_ignore_ascii_case(u))
        .map(|&(_, m)| m)
        .ok_or_else(bad)?;
    match num.checked_mul(mult) {
        Some(0) | None => Err(GnitzSqlError::Plan(format!(
            "CREATE VIEW option `{option}`: '{text}' is out of range (must be positive and fit a u64)"
        ))),
        Some(bytes) => Ok(bytes),
    }
}

/// A planned view: its segment bundle, and the view it supersedes.
pub(crate) struct PlannedChain {
    pub(crate) name: String,
    pub(crate) bundle: ViewBundle,
    pub(crate) props: ViewProps,
    /// The id of the view this chain replaces, retracted in the same bundle.
    pub(crate) replacing: Option<u64>,
}

/// Plan a `CREATE VIEW`; `None` when `IF NOT EXISTS` finds the name taken.
pub(crate) fn plan_create_view(cv: &CreateView, cat: &Catalog<'_>) -> Result<Option<PlannedChain>, GnitzSqlError> {
    reject_unhonored_create_view_clauses(cv)?;
    let view_name = crate::ast_util::extract_object_name(&cv.name, cat.schema_name(), "CREATE VIEW")?;

    // Each clause tests the name alone: a view's catalog rows hold its circuit, not its text.
    let replacing = if cv.if_not_exists {
        // Any relation under the name ends the statement, whatever its kind.
        if cat.probe(&view_name)?.is_some() {
            return Ok(None);
        }
        None
    } else if cv.or_replace {
        match cat.probe(&view_name)? {
            Some(rel) => {
                require_class(&rel, &view_name, ClassWant::View, "CREATE OR REPLACE VIEW")?;
                Some(rel.tid)
            }
            // A free name: `OR REPLACE` degenerates to a plain create.
            None => None,
        }
    } else {
        None
    };

    let props = decode_view_options(&cv.options)?;
    let view = ViewBody { stmt: "CREATE VIEW", replacing };
    let aliases = cv.columns.iter().map(|c| &c.name);
    let bundle = plan_segments(cat, view, &cv.query, aliases, props)?;
    Ok(Some(PlannedChain {
        name: view_name,
        bundle,
        props,
        replacing,
    }))
}

/// Plan an `ALTER VIEW <name> [(columns)] AS <query>`: the chain that replaces the
/// view under a fresh id.
pub(crate) fn plan_alter_view(
    name: &ObjectName,
    columns: &[Ident],
    query: &Query,
    with_options: &[sqlparser::ast::SqlOption],
    cat: &Catalog<'_>,
) -> Result<PlannedChain, GnitzSqlError> {
    // Only `CREATE OR REPLACE VIEW` states a view's options.
    reject_if(!with_options.is_empty(), "ALTER VIEW", "WITH options")?;
    let view_name = crate::ast_util::extract_object_name(name, cat.schema_name(), "ALTER VIEW")?;
    let old_vid = resolve_view_id(cat, &view_name)?;
    let view = ViewBody {
        stmt: "ALTER VIEW",
        replacing: Some(old_vid),
    };
    let props = ViewProps::Plain;
    let bundle = plan_segments(cat, view, query, columns.iter(), props)?;
    Ok(PlannedChain {
        name: view_name,
        bundle,
        props,
        replacing: Some(old_vid),
    })
}

/// Compile a view body to its segment bundle, the user-named view's visible
/// output columns renamed by `aliases`.
fn plan_segments<'a>(
    cat: &Catalog<'_>,
    view: ViewBody,
    query: &Query,
    aliases: impl ExactSizeIterator<Item = &'a Ident>,
    props: ViewProps,
) -> Result<ViewBundle, GnitzSqlError> {
    reject_unhonored_query_clauses(query, QueryEnvelope::WithAndTail, view.stmt)?;
    let bounded = matches!(props, ViewProps::Bounded { .. });
    let mut bundle = crate::hir::bind_and_lower(cat, query, view, bounded)?;
    apply_positional_aliases(
        aliases,
        Arc::make_mut(&mut bundle.view.schema).columns.iter_mut(),
        view.stmt,
    )?;
    Ok(bundle)
}

/// Commit a planned `CREATE VIEW` or `ALTER VIEW … AS`.
pub(crate) fn execute_view_chain(
    client: &mut GnitzClient,
    schema_name: &str,
    chain: PlannedChain,
) -> Result<SqlResult, GnitzSqlError> {
    client.create_view_chain(schema_name, &chain.name, chain.bundle, chain.props, chain.replacing)?;
    Ok(SqlResult::Ddl)
}

/// Resolve `name` to the id of a VIEW `ALTER VIEW … AS` may retarget.
fn resolve_view_id(cat: &Catalog<'_>, name: &str) -> Result<u64, GnitzSqlError> {
    let rel = cat.probe_relation(name)?;
    require_class(&rel, name, ClassWant::View, "ALTER VIEW")?;
    // `ALTER VIEW` states no options, so it would drop these.
    let option = match rel.class {
        RelClass::BoundedView => "a capacity-bounded view",
        RelClass::FedView => "a view with a delta feed",
        RelClass::View => return Ok(rel.tid),
        RelClass::Table | RelClass::Stream => unreachable!("require_class admits views only"),
    };
    Err(GnitzSqlError::Unsupported(format!(
        "ALTER VIEW cannot retarget {option}; DROP and CREATE '{name}' instead"
    )))
}

#[cfg(test)]
#[path = "tests/create.rs"]
mod tests;
