//! CREATE / ALTER VIEW front door: validate the query envelope, then drive the
//! HIR pipeline — the CTE phase (`hir::bind::bind_ctes`) followed by
//! `bind_and_lower` of the body — into a `ViewChain` committed atomically.

use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::hir::chain::{debug_assert_exchange_topology, ViewChain};
use crate::validate::{reject_unhonored_query_clauses, validate_user_name, HonoredQueryClauses};
use crate::SqlResult;
use gnitz_core::{CatalogSnapshot, GnitzClient, PlannedView, RelClass, ViewName};
use sqlparser::ast::{CreateTableOptions, ObjectName, Query, Statement, Value, ValueWithSpan};

/// Binary units accepted by a `WITH (<option> = '<uint><unit>')` size string.
const SIZE_UNITS: [(&str, u64); 3] = [("KB", 1 << 10), ("MB", 1 << 20), ("GB", 1 << 30)];

/// The two byte budgets a `CREATE VIEW`'s `WITH (...)` clause can carry.
/// Presence of a budget *is* the classification on each side — a second flag word
/// could only ever disagree with it.
#[derive(Default, Clone, Copy)]
pub(crate) struct ViewOptions {
    /// `capacity`: bound the view's own output store, dehydrating past it.
    pub capacity: Option<u64>,
    /// `delta`: keep the view's recent deltas in a store of its own, so a client
    /// can read "what changed since round N".
    pub delta: Option<u64>,
}

/// Decode the `WITH (...)` clause of a `CREATE VIEW`.
///
/// `sqlparser::parse_create_view` fills `options` from `parse_options(WITH)` with
/// no dialect gate, so no grammar of ours is involved — only the meaning of the
/// keys we honour. Both are decoded here rather than in a second pass over the
/// same list, because an option this loop does not know about is rejected by
/// name: adding one anywhere else would refuse `WITH (delta = …)` with a message
/// naming `capacity` as the only option.
///
/// **The two are refused together.** A bounded view's read hydrates its missing
/// keys from the *source relation's live store*, which no tick round governs, so
/// its `Delta(0)` cannot be the view's value at round `T` — and the whole feed
/// contract is that it is. The bootstrap would report `T` while already carrying
/// an un-ticked push, and the next poll would deliver that same push again at
/// double weight, with no error and no row-set difference. The engine's
/// `view_registration` refuses the pair as well: it is the trust boundary, and
/// this is where the message is legible.
fn decode_view_options(options: &CreateTableOptions) -> Result<ViewOptions, GnitzSqlError> {
    let mut out = ViewOptions::default();
    for opt in crate::validate::with_options(options)? {
        let (key, value) = crate::validate::require_kv_option(opt, "CREATE VIEW")?;
        // The option's name and the field it fills are bound together, so a third
        // option is one arm rather than an arm plus a second dispatch on the name.
        let (name, slot) = if key.value.eq_ignore_ascii_case("capacity") {
            ("capacity", &mut out.capacity)
        } else if key.value.eq_ignore_ascii_case("delta") {
            ("delta", &mut out.delta)
        } else {
            return Err(GnitzSqlError::Plan(format!(
                "unknown CREATE VIEW option '{}'; the supported options are `capacity` and `delta`",
                key.value
            )));
        };
        let sqlparser::ast::Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text),
            ..
        }) = value
        else {
            return Err(GnitzSqlError::Plan(format!(
                "CREATE VIEW option `{name}` takes a single-quoted size string, e.g. '256 MB'"
            )));
        };
        *slot = Some(parse_size(name, text)?);
    }
    if out.capacity.is_some() && out.delta.is_some() {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW WITH (capacity …, delta …): a capacity-bounded view cannot carry a \
             delta feed — its bootstrap read hydrates from the source relation's live store, \
             which no tick round governs"
                .to_string(),
        ));
    }
    Ok(out)
}

/// `<uint><unit>` with an optional space, unit in {KB, MB, GB}, binary
/// (KB = 2^10). Zero and a `u64`-overflowing product are rejected: a zero
/// capacity names a store that cannot hold its own skeleton, and a zero delta
/// budget one that retains nothing.
///
/// `option` is threaded through rather than duplicated into a second parser: the
/// grammar is shared verbatim between the two options, only the error text names
/// which one.
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

/// Plan a `CREATE VIEW` or an `ALTER VIEW … AS` into the bundle its commit
/// writes: a pure function of `(stmt, cat)`, minting symbolic segment ids and
/// reaching no server.
///
/// The last segment is the user-named final view; every earlier one is a hidden
/// segment it depends on, which is the dependency order `create_view_chain`
/// wants anyway. The commit half reads the owner off that position rather than
/// re-deriving it from the AST.
pub fn plan_view(
    stmt: &Statement,
    cat: &CatalogSnapshot,
    schema_name: &str,
) -> Result<Vec<PlannedView>, GnitzSqlError> {
    // A fresh binder per pass: the alias cache a re-run's discarded pass filled
    // must not reach the next one.
    let mut binder = Binder::new(schema_name).for_view_body();
    match stmt {
        Statement::CreateView(cv) => plan_create_view(cat, cv, &mut binder),
        Statement::AlterView { name, query, .. } => plan_alter_view(cat, schema_name, name, query, &mut binder),
        _ => Err(GnitzSqlError::Unsupported(
            "plan_view describes a CREATE VIEW or an ALTER VIEW … AS; this statement is neither".to_string(),
        )),
    }
}

/// The bundle owner's name — `build_query_segments` pushes the user-named view
/// last, after every hidden segment.
fn owner_name(views: &[PlannedView]) -> Result<&str, GnitzSqlError> {
    match views.last().map(|pv| &pv.name) {
        Some(ViewName::Named(n)) => Ok(n),
        _ => Err(GnitzSqlError::Internal(
            "a planned view bundle must end with its user-named segment".to_string(),
        )),
    }
}

fn plan_create_view(
    cat: &CatalogSnapshot,
    cv: &sqlparser::ast::CreateView,
    binder: &mut Binder<'_>,
) -> Result<Vec<PlannedView>, GnitzSqlError> {
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
    let options = decode_view_options(&cv.options)?;

    let mut chain = ViewChain::new();
    build_query_segments(cat, query, binder, &mut chain, view_name, sql_text, options)?;
    Ok(chain.segments)
}

/// `ALTER VIEW <v> AS <query>` — drop-then-create under the same name with a
/// FRESH vid (ids are never reused), as ONE DDL zone: the old vid's and its
/// hidden segments' `-1` rows ride in the same bundle as the new chain's `+1`s.
/// The engine compiles every new view's circuit before the bundle is durable, so
/// a bundle that fails there — or on any other guard — leaves the old view
/// serving its rows untouched.
/// sqlparser's `AlterView` has no `if_exists`, so a missing view is a hard error.
fn plan_alter_view(
    cat: &CatalogSnapshot,
    schema_name: &str,
    name: &ObjectName,
    query: &Query,
    binder: &mut Binder<'_>,
) -> Result<Vec<PlannedView>, GnitzSqlError> {
    let view_name = crate::ast_util::extract_name(name, "ALTER VIEW")?;
    validate_user_name(&view_name)?;

    // Resolve the old vid; reject `ALTER VIEW <table>` and a missing relation.
    let old_vid = resolve_view_id(cat, schema_name, &view_name)?;

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

    let mut chain = ViewChain::new();
    build_query_segments(
        cat,
        query,
        binder,
        &mut chain,
        view_name.clone(),
        sql_text,
        ViewOptions::default(),
    )?;

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

    Ok(chain.segments)
}

/// Commit a planned `CREATE VIEW` bundle; the owner's real id comes back from it.
pub(crate) fn execute_create_view(
    client: &mut GnitzClient,
    schema_name: &str,
    views: Vec<PlannedView>,
) -> Result<SqlResult, GnitzSqlError> {
    owner_name(&views)?;
    let vids = client.create_view_chain(schema_name, views, None)?;
    let view_id = *vids
        .last()
        .ok_or_else(|| GnitzSqlError::Internal("create_view_chain returned no ids".to_string()))?;
    Ok(SqlResult::ViewCreated { view_id })
}

/// Commit a planned `ALTER VIEW … AS` bundle: one zone carrying the old view's
/// retractions and the new chain. The engine's view-dependency guard
/// (re-evaluated under the catalog write lock) still rejects the retraction if
/// dependents exist — RESTRICT, and nothing is torn down when it fires.
pub(crate) fn execute_alter_view(
    client: &mut GnitzClient,
    schema_name: &str,
    views: Vec<PlannedView>,
) -> Result<SqlResult, GnitzSqlError> {
    let view_name = owner_name(&views)?.to_string();
    client.create_view_chain(schema_name, views, Some(&view_name))?;
    Ok(SqlResult::Altered {
        object: "view".to_string(),
        name: view_name,
    })
}

/// Resolve `name` to a VIEW id, rejecting `ALTER VIEW <table>` and a missing
/// relation.
fn resolve_view_id(cat: &CatalogSnapshot, schema_name: &str, name: &str) -> Result<u64, GnitzSqlError> {
    let resolved = cat
        .get(schema_name, name)
        .ok_or_else(|| GnitzSqlError::CatalogMiss(name.to_string()))?;
    match resolved {
        // `ALTER VIEW … AS` re-renders its body as a bare `CREATE VIEW … AS …`,
        // dropping any option clause — so retargeting a bounded view would
        // silently convert it into an unbounded one.
        Some(rel) if rel.class == RelClass::BoundedView => Err(GnitzSqlError::Unsupported(format!(
            "ALTER VIEW cannot retarget a capacity-bounded view; DROP and CREATE '{name}' instead"
        ))),
        // The same rule for a fed view, and it needs its own clause: `delta` is
        // deliberately not part of the relation class, so a fed unbounded view is
        // `RelClass::View` and would sail past the test above into exactly the
        // failure it exists to prevent — the feed gone, and every later delta read
        // (`after_tick = 0` included) the typed error a relation with no delta
        // store gets. There is no ALTER that turns a feed *on* either, for the
        // reason a feed cannot start mid-life: it would begin empty while its view
        // already held rows, and every `after_tick > 0` against it would be a lie
        // rather than an expiry.
        Some(rel) if rel.delta => Err(GnitzSqlError::Unsupported(format!(
            "ALTER VIEW cannot retarget a view with a delta feed; DROP and CREATE '{name}' instead"
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
/// segments in dependency order, then the final view named `final_name`), filling
/// `chain.segments`. Does NOT reject unhonored tail clauses — the caller owns that
/// (CREATE VIEW rejects ORDER BY/LIMIT before calling).
fn build_query_segments(
    cat: &CatalogSnapshot,
    query: &Query,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    final_name: String,
    sql_text: String,
    options: ViewOptions,
) -> Result<(), GnitzSqlError> {
    let capacity = options.capacity;
    // The CTE phase compiles each CTE body (a pass-through alias into the binder
    // cache, or a hidden segment on `chain`) and registers it, so the body below
    // resolves a CTE by name. A derived table is not pre-compiled — it binds as an
    // inline subtree inside the body bind (`resolve_table_factor`).
    let final_vid = chain.owner_vid();
    crate::hir::bind::bind_ctes(cat, binder, chain, query)?;

    // Every view shape — linear, join, GROUP BY, DISTINCT, set operation, and every
    // subquery form (EXISTS/IN, scalar aggregate, ANY/ALL) — routes through the HIR
    // pipeline: bind the `query` body to a logical `RelExpr` tree, decorrelate
    // subqueries into `Join`/`Reduce` structure, classify predicates, then lower to
    // circuit(s) — nested combine segments / self-collision pass-through wrappers
    // land on `chain`, and the final step is emitted with `final_vid`.
    let (circuit, out_cols, pk_cols) =
        crate::hir::bind_and_lower(cat, binder, chain, query.body.as_ref(), final_vid, capacity.is_some())?;

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
        name: ViewName::Named(final_name),
        sql_text,
        circuit,
        output_columns: out_cols,
        pk_cols,
        capacity_bytes: capacity,
        delta_bytes: options.delta,
    });
    Ok(())
}
