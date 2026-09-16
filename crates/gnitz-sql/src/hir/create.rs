//! CREATE / ALTER VIEW front door: validate the query envelope, then drive the
//! HIR pipeline — the CTE phase (`hir::bind::bind_ctes`) and then
//! `bind_and_lower` of the body — into a `ViewChain` committed atomically.

use crate::bind::{apply_positional_aliases, probe};
use crate::error::{reject_if, GnitzSqlError};
use crate::hir::bind::ViewBody;
use crate::hir::chain::ViewChain;
use crate::validate::{
    kv_options, reject_unhonored_create_view_clauses, reject_unhonored_query_clauses, require_class,
    validate_user_name, ClassWant, QueryEnvelope,
};
use crate::SqlResult;
use gnitz_core::{CatalogSnapshot, GnitzClient, PlannedView, RelClass, ViewProps};
use sqlparser::ast::{CreateTableOptions, CreateView, Ident, ObjectName, Query, Value, ValueWithSpan};

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

/// What a `CREATE VIEW` statement asks the connection to do.
pub enum ViewPlan {
    /// Register `chain`, superseding the view that already holds its name when
    /// `replace` says so.
    Create { chain: PlannedChain, replace: bool },
    /// As [`crate::TablePlan::Skip`].
    Skip { existing_id: u64 },
}

/// A planned view: its segment bundle, the user-named view last.
pub struct PlannedChain {
    pub name: String,
    pub views: Vec<PlannedView>,
    pub props: ViewProps,
}

/// Plan a `CREATE VIEW`: a pure function of `(cv, cat)`, reaching no server.
pub fn plan_create_view(cv: &CreateView, cat: &CatalogSnapshot, schema_name: &str) -> Result<ViewPlan, GnitzSqlError> {
    reject_unhonored_create_view_clauses(cv)?;
    let view_name = crate::ast_util::extract_object_name(&cv.name, schema_name, "CREATE VIEW")?;
    validate_user_name(&view_name)?;

    // Each clause tests the name alone: a view's catalog rows hold its circuit, not its text.
    let replacing = if cv.if_not_exists {
        // Any relation under the name ends the statement, whatever its kind.
        if let Some(rel) = probe(cat, schema_name, &view_name)? {
            return Ok(ViewPlan::Skip { existing_id: rel.tid });
        }
        None
    } else if cv.or_replace {
        match probe(cat, schema_name, &view_name)? {
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
    let views = plan_segments(cat, schema_name, view, &cv.query, aliases, props)?;
    Ok(ViewPlan::Create {
        chain: PlannedChain { name: view_name, views, props },
        replace: replacing.is_some(),
    })
}

/// Plan an `ALTER VIEW <name> [(columns)] AS <query>`: the chain that replaces the
/// view under a fresh id.
pub fn plan_alter_view(
    name: &ObjectName,
    columns: &[Ident],
    query: &Query,
    with_options: &[sqlparser::ast::SqlOption],
    cat: &CatalogSnapshot,
    schema_name: &str,
) -> Result<PlannedChain, GnitzSqlError> {
    // Only `CREATE OR REPLACE VIEW` states a view's options.
    reject_if(!with_options.is_empty(), "ALTER VIEW", "WITH options")?;
    let view_name = crate::ast_util::extract_object_name(name, schema_name, "ALTER VIEW")?;
    validate_user_name(&view_name)?;
    let old_vid = resolve_view_id(cat, schema_name, &view_name)?;
    let view = ViewBody {
        stmt: "ALTER VIEW",
        replacing: Some(old_vid),
    };
    let props = ViewProps::Plain;
    let views = plan_segments(cat, schema_name, view, query, columns.iter(), props)?;
    Ok(PlannedChain { name: view_name, views, props })
}

/// Compile a view body to its segment bundle, the user-named view last, its
/// visible output columns renamed by `aliases`.
fn plan_segments<'a>(
    cat: &CatalogSnapshot,
    schema_name: &str,
    view: ViewBody,
    query: &Query,
    aliases: impl ExactSizeIterator<Item = &'a Ident>,
    props: ViewProps,
) -> Result<Vec<PlannedView>, GnitzSqlError> {
    reject_unhonored_query_clauses(query, QueryEnvelope::WithAndTail, view.stmt)?;
    let mut chain = ViewChain::new(matches!(props, ViewProps::Bounded { .. }));
    let pieces = crate::hir::bind_and_lower(cat, schema_name, &mut chain, query, view)?;
    let final_view = chain.push_final(pieces)?;
    apply_positional_aliases(aliases, final_view.output_columns.iter_mut(), view.stmt)?;
    Ok(chain.segments)
}

/// Commit a planned `CREATE VIEW`; a skip answers with the id standing under the name.
pub(crate) fn execute_create_view(
    client: &mut GnitzClient,
    schema_name: &str,
    plan: ViewPlan,
) -> Result<SqlResult, GnitzSqlError> {
    let (chain, replace) = match plan {
        ViewPlan::Skip { existing_id } => return Ok(SqlResult::ViewCreated { view_id: existing_id }),
        ViewPlan::Create { chain, replace } => (chain, replace),
    };
    let vids = client.create_view_chain(schema_name, &chain.name, chain.views, chain.props, replace)?;
    let view_id = *vids.last().expect("a non-empty bundle");
    Ok(SqlResult::ViewCreated { view_id })
}

/// Commit a planned `ALTER VIEW … AS`.
pub(crate) fn execute_alter_view(
    client: &mut GnitzClient,
    schema_name: &str,
    chain: PlannedChain,
) -> Result<SqlResult, GnitzSqlError> {
    client.create_view_chain(schema_name, &chain.name, chain.views, chain.props, true)?;
    Ok(SqlResult::Altered {
        object: "view".to_string(),
        name: chain.name,
    })
}

/// Resolve `name` to the id of a VIEW `ALTER VIEW … AS` may retarget.
fn resolve_view_id(cat: &CatalogSnapshot, schema_name: &str, name: &str) -> Result<u64, GnitzSqlError> {
    let rel =
        probe(cat, schema_name, name)?.ok_or_else(|| crate::error::missing_relation("View", schema_name, name))?;
    require_class(&rel, name, ClassWant::View, "ALTER VIEW")?;
    // `ALTER VIEW` states no options, so it would drop these.
    let option = match rel.class {
        RelClass::BoundedView => "a capacity-bounded view",
        RelClass::FedView => "a view with a delta feed",
        _ => return Ok(rel.tid),
    };
    Err(GnitzSqlError::Unsupported(format!(
        "ALTER VIEW cannot retarget {option}; DROP and CREATE '{name}' instead"
    )))
}

#[cfg(test)]
#[path = "tests/create.rs"]
mod tests;
