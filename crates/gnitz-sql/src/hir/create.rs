//! CREATE / ALTER VIEW front door: validate the query envelope, then drive the
//! HIR pipeline — the CTE phase (`hir::bind::bind_ctes`) followed by
//! `bind_and_lower` of the body — into a `ViewChain` committed atomically.

use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::hir::chain::{debug_assert_exchange_topology, ViewChain};
use crate::validate::{reject_unhonored_query_clauses, validate_user_name, HonoredQueryClauses};
use crate::SqlResult;
use gnitz_core::{GnitzClient, PlannedView};
use sqlparser::ast::{ObjectName, Query};

pub(crate) fn execute_create_view(
    client: &mut GnitzClient,
    schema_name: &str,
    cv: &sqlparser::ast::CreateView,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let query: &Query = &cv.query;
    let view_name = crate::ast_util::extract_name(&cv.name, "CREATE VIEW")?;
    validate_user_name(&view_name)?;

    // The CREATE VIEW envelope honors only `WITH` (compiled by the CTE phase in
    // `build_query_segments`); every other tail clause (ORDER BY, LIMIT/OFFSET, FETCH, FOR
    // UPDATE/SHARE, FOR XML/JSON, SETTINGS, FORMAT) has no incremental-view semantics and would
    // otherwise be silently dropped.
    reject_unhonored_query_clauses(
        query,
        HonoredQueryClauses {
            with: true,
            ..HonoredQueryClauses::NONE
        },
        "CREATE VIEW",
    )?;

    // `CreateView`'s `Display` is exactly what `Statement::CreateView` delegates
    // to, so this is the statement's full SQL text.
    let sql_text = format!("{cv}");

    // Compile the body into a durable chain (real `alloc_table_id` ids), then
    // commit it atomically. `build_query_segments` owns every shape rule; CREATE
    // VIEW adds only the durable id origin and the `create_view_chain` commit.
    let mut chain = ViewChain::new();
    let final_vid = build_query_segments(client, query, binder, &mut chain, view_name, sql_text)?;
    client
        .create_view_chain(schema_name, chain.segments)
        .map_err(GnitzSqlError::Exec)?;
    Ok(SqlResult::ViewCreated { view_id: final_vid })
}

/// `ALTER VIEW <v> AS <query>` — drop-then-create under the same name with a
/// FRESH vid (ids are never reused). Deliberately NOT atomic: the new plan is
/// compiled and validated first, then two sequential DDL zones (drop old vid +
/// hidden segments, then create the fresh chain), so a fault after the drop
/// commits leaves no view `v` at all (reported with a re-issue-as-CREATE hint).
/// sqlparser's `AlterView` has no `if_exists`, so a missing view is a hard error.
pub(crate) fn execute_alter_view(
    client: &mut GnitzClient,
    schema_name: &str,
    name: &ObjectName,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    let view_name = crate::ast_util::extract_name(name, "ALTER VIEW")?;
    validate_user_name(&view_name)?;

    // Resolve the old vid; reject `ALTER VIEW <table>` and a missing relation.
    let old_vid = resolve_view_id(client, schema_name, &view_name)?;

    // Same envelope rules as CREATE VIEW (only `WITH` honored).
    reject_unhonored_query_clauses(
        query,
        HonoredQueryClauses {
            with: true,
            ..HonoredQueryClauses::NONE
        },
        "ALTER VIEW",
    )?;

    // Re-render CREATE-VIEW-shaped so the stored sql_definition matches a fresh
    // CREATE VIEW of the new definition.
    let sql_text = format!("CREATE VIEW {view_name} AS {query}");

    // Compile + validate the new plan (fresh vids) BEFORE issuing either zone, so
    // a compile error leaves the old view fully intact.
    let mut chain = ViewChain::new();
    build_query_segments(client, query, binder, &mut chain, view_name.clone(), sql_text)?;

    // Reject self-reference: `FROM v` in the new query resolves to the still-live
    // old vid, which would appear as a source of the new plan — dropping it in
    // zone 1 would remove the new definition's own input. Rejected before any zone.
    if chain
        .segments
        .iter()
        .any(|s| s.circuit.dependencies().contains(&old_vid))
    {
        return Err(GnitzSqlError::Unsupported(format!(
            "ALTER VIEW '{schema_name}.{view_name}' AS a query referencing the view itself is not supported"
        )));
    }

    // Zone 1: drop the old view + its hidden segments. The engine's
    // view-dependency guard (re-evaluated under the catalog write lock) rejects
    // the drop if dependents exist — RESTRICT, before anything is torn down.
    client.drop_view(schema_name, &view_name).map_err(GnitzSqlError::Exec)?;

    // Zone 2: create the fresh chain under the same name. A failure here (engine/
    // I/O fault, or a qname race — a concurrent same-name CREATE in the gap) is
    // post-drop: the old view is durably gone, so surface a re-issue hint.
    client.create_view_chain(schema_name, chain.segments).map_err(|e| {
        GnitzSqlError::Plan(format!(
            "ALTER VIEW '{schema_name}.{view_name}': the old view was dropped but installing the new \
             definition failed ({e}); re-issue as CREATE VIEW {view_name} AS <query>"
        ))
    })?;

    Ok(SqlResult::Altered {
        object: "view".to_string(),
        name: view_name,
    })
}

/// Resolve `name` to a VIEW id, rejecting `ALTER VIEW <table>` and a missing
/// relation.
fn resolve_view_id(client: &mut GnitzClient, schema_name: &str, name: &str) -> Result<u64, GnitzSqlError> {
    match client
        .resolve_relation_kind(schema_name, name)
        .map_err(GnitzSqlError::Exec)?
    {
        Some((vid, true)) => Ok(vid),
        Some((_, false)) => Err(GnitzSqlError::Unsupported(format!(
            "'{name}' is a table; ALTER VIEW requires a view (use ALTER TABLE)"
        ))),
        None => Err(GnitzSqlError::Bind(format!(
            "View '{schema_name}.{name}' does not exist"
        ))),
    }
}

/// Compile one query body into a chain of `PlannedView` segments (hidden
/// segments in dependency order, then the final view named `final_name`),
/// filling `chain.segments` and returning the final view's id. Does NOT reject
/// unhonored tail clauses — the caller owns that (CREATE VIEW rejects ORDER
/// BY/LIMIT before calling).
fn build_query_segments(
    client: &mut GnitzClient,
    query: &Query,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    final_name: String,
    sql_text: String,
) -> Result<u64, GnitzSqlError> {
    // The CTE phase compiles each CTE body (a pass-through alias into the binder
    // cache, or a hidden segment on `chain`) and registers it, so the body below
    // resolves a CTE by name. A derived table is not pre-compiled — it binds as an
    // inline subtree inside the body bind (`resolve_table_factor`).
    let final_vid = chain.owner_vid(client)?;
    crate::hir::bind::bind_ctes(client, binder, chain, query)?;

    // Every view shape — linear, join, GROUP BY, DISTINCT, set operation, and every
    // subquery form (EXISTS/IN, scalar aggregate, ANY/ALL) — routes through the HIR
    // pipeline: bind the `query` body to a logical `RelExpr` tree, decorrelate
    // subqueries into `Join`/`Reduce` structure, classify predicates, then lower to
    // circuit(s) — nested combine segments / self-collision pass-through wrappers
    // land on `chain`, and the final step is emitted with `final_vid`.
    let (circuit, out_cols, pk_cols) =
        crate::hir::bind_and_lower(client, binder, chain, query.body.as_ref(), final_vid)?;

    // The final segment: the hidden segments already sit on the chain in
    // dependency order; append the (user-named or synthetic) final view. The
    // hidden ones were checked inside `add_segment`; this is the other of the two
    // paths every emitted circuit reaches.
    debug_assert_exchange_topology(&circuit);
    chain.segments.push(PlannedView {
        name: final_name,
        sql_text,
        circuit,
        output_columns: out_cols,
        pk_cols,
    });
    Ok(final_vid)
}
