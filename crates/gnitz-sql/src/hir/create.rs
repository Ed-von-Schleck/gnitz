//! CREATE / ALTER VIEW front door: validate the query envelope, then drive the
//! HIR pipeline — the CTE phase (`hir::bind::bind_ctes`) followed by
//! `bind_and_lower` of the body — into a `ViewChain` committed atomically.

use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::hir::chain::{debug_assert_exchange_topology, ViewChain};
use crate::validate::{reject_unhonored_query_clauses, validate_user_name, HonoredQueryClauses};
use crate::SqlResult;
use gnitz_core::{GnitzClient, PlannedView, RelClass};
use sqlparser::ast::{CreateTableOptions, ObjectName, Query, Value, ValueWithSpan};

/// Binary units accepted by `WITH (capacity = '<uint><unit>')`.
const CAPACITY_UNITS: [(&str, u64); 3] = [("KB", 1 << 10), ("MB", 1 << 20), ("GB", 1 << 30)];

/// Decode the `WITH (...)` clause of a `CREATE VIEW` into a byte capacity.
///
/// `sqlparser::parse_create_view` fills `options` from `parse_options(WITH)` with
/// no dialect gate, so no grammar of ours is involved — only the meaning of the
/// one key we honour. Presence of a capacity *is* the bounded classification;
/// there is no second word to keep consistent with it.
fn decode_capacity(options: &CreateTableOptions) -> Result<Option<u64>, GnitzSqlError> {
    let mut capacity = None;
    for opt in crate::validate::with_options(options)? {
        let (key, value) = crate::validate::require_kv_option(opt, "CREATE VIEW")?;
        if !key.value.eq_ignore_ascii_case("capacity") {
            return Err(GnitzSqlError::Plan(format!(
                "unknown CREATE VIEW option '{}'; the only supported option is `capacity`",
                key.value
            )));
        }
        let sqlparser::ast::Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text),
            ..
        }) = value
        else {
            return Err(GnitzSqlError::Plan(
                "CREATE VIEW option `capacity` takes a single-quoted size string, e.g. '256 MB'".to_string(),
            ));
        };
        capacity = Some(parse_capacity(text)?);
    }
    Ok(capacity)
}

/// `<uint><unit>` with an optional space, unit in {KB, MB, GB}, binary
/// (KB = 2^10). Zero and a `u64`-overflowing product are rejected: a zero
/// capacity names a store that cannot hold its own skeleton.
fn parse_capacity(text: &str) -> Result<u64, GnitzSqlError> {
    let bad = || {
        GnitzSqlError::Plan(format!(
            "CREATE VIEW option `capacity`: '{text}' is not a size like '256 MB' \
             (a positive integer followed by KB, MB or GB)"
        ))
    };
    let trimmed = text.trim();
    let digits = trimmed.len() - trimmed.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    let (num, unit) = trimmed.split_at(digits);
    let num: u64 = num.parse().map_err(|_| bad())?;
    let unit = unit.trim();
    let mult = CAPACITY_UNITS
        .iter()
        .find(|(u, _)| unit.eq_ignore_ascii_case(u))
        .map(|&(_, m)| m)
        .ok_or_else(bad)?;
    match num.checked_mul(mult) {
        Some(0) | None => Err(GnitzSqlError::Plan(format!(
            "CREATE VIEW option `capacity`: '{text}' is out of range (must be positive and fit a u64)"
        ))),
        Some(bytes) => Ok(bytes),
    }
}

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
    let capacity = decode_capacity(&cv.options)?;

    let mut chain = ViewChain::new();
    let final_vid = build_query_segments(client, query, binder, &mut chain, view_name, sql_text, capacity)?;
    client
        .create_view_chain(schema_name, chain.segments, None)
        .map_err(GnitzSqlError::Exec)?;
    Ok(SqlResult::ViewCreated { view_id: final_vid })
}

/// `ALTER VIEW <v> AS <query>` — drop-then-create under the same name with a
/// FRESH vid (ids are never reused), as ONE DDL zone: the old vid's and its
/// hidden segments' `-1` rows ride in the same bundle as the new chain's `+1`s.
/// The engine compiles every new view's circuit before the bundle is durable, so
/// a bundle that fails there — or on any other guard — leaves the old view
/// serving its rows untouched.
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

    // Compile + validate the new plan (fresh vids) BEFORE issuing the zone, so a
    // planner error leaves the old view fully intact.
    let mut chain = ViewChain::new();
    build_query_segments(client, query, binder, &mut chain, view_name.clone(), sql_text, None)?;

    // Reject self-reference: `FROM v` in the new query resolves to the still-live
    // old vid, which would appear as a source of the new plan — the bundle
    // retracts that vid, so the new definition would lose its own input.
    if chain
        .segments
        .iter()
        .any(|s| s.circuit.dependencies().contains(&old_vid))
    {
        return Err(GnitzSqlError::Unsupported(format!(
            "ALTER VIEW '{schema_name}.{view_name}' AS a query referencing the view itself is not supported"
        )));
    }

    // One zone: the old view's retractions plus the new chain. The engine's
    // view-dependency guard (re-evaluated under the catalog write lock) still
    // rejects the retraction if dependents exist — RESTRICT, and nothing is torn
    // down when it fires.
    client
        .create_view_chain(schema_name, chain.segments, Some(&view_name))
        .map_err(GnitzSqlError::Exec)?;

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
        // `ALTER VIEW … AS` re-renders its body as a bare `CREATE VIEW … AS …`,
        // dropping any option clause — so retargeting a bounded view would
        // silently convert it into an unbounded one.
        Some(rel) if rel.class == RelClass::BoundedView => Err(GnitzSqlError::Unsupported(format!(
            "ALTER VIEW cannot retarget a capacity-bounded view; DROP and CREATE '{name}' instead"
        ))),
        Some(rel) if rel.class.is_view() => Ok(rel.tid),
        Some(rel) => Err(GnitzSqlError::Unsupported(format!(
            "'{name}' is a {}; ALTER VIEW requires a view (use ALTER TABLE)",
            rel.class.noun()
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
    capacity: Option<u64>,
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
    let (circuit, out_cols, pk_cols) = crate::hir::bind_and_lower(
        client,
        binder,
        chain,
        query.body.as_ref(),
        final_vid,
        capacity.is_some(),
    )?;

    // Structural eligibility, over what the body actually compiled to rather than
    // over the shapes it was written in: both bounded shapes are a single segment,
    // and anything that cut — a derived table, EXISTS, a nested join, a
    // non-trivial CTE — left a hidden unbounded segment holding the same rows at
    // full width, so a capacity above it would bound nothing. `lower_body`'s
    // per-arm rejections name the shape and come first; this catches the shapes
    // that reach an eligible arm through a cut input, which no arm can see.
    if capacity.is_some() && !chain.segments.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW WITH (capacity …): this body compiles to more than one view, whose \
             intermediate results are unbounded; only a filter/projection over one relation \
             and an inner equi-join are supported"
                .to_string(),
        ));
    }

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
        capacity_bytes: capacity,
    });
    Ok(final_vid)
}
