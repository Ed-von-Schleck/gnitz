//! CREATE / ALTER VIEW front door: validate the query envelope, then drive the
//! HIR pipeline — the CTE phase (`hir::bind::bind_ctes`) and then
//! `bind_and_lower` of the body — into a `ViewChain` committed atomically.
//!
//! Both statements plan into one [`ViewPlan`], which also carries what the
//! statement's clauses decided about a name already in use: create it, supersede
//! what stands there, or leave it alone.

use crate::bind::{apply_positional_aliases, probe, Binder};
use crate::error::{reject_if, GnitzSqlError};
use crate::hir::bind::ViewSurface;
use crate::hir::chain::ViewChain;
use crate::validate::{
    kv_options, reject_unhonored_create_view_clauses, reject_unhonored_query_clauses, validate_user_name, QueryEnvelope,
};
use crate::SqlResult;
use gnitz_core::{CatalogSnapshot, GnitzClient, OpNode, PlannedView, RelClass};
use sqlparser::ast::{CreateTableOptions, Ident, ObjectName, Query, Statement, Value, ValueWithSpan};

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
    for (key, value) in kv_options(options, "CREATE VIEW")? {
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
            value: Value::SingleQuotedString(text), ..
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
/// The last segment is the user-named final view — the name is returned beside
/// the bundle rather than carried on it, because only that one element has one;
/// every earlier segment is an internal one it depends on, which is the
/// dependency order `create_view_chain` wants anyway.
pub fn plan_view(stmt: &Statement, cat: &CatalogSnapshot, schema_name: &str) -> Result<ViewPlan, GnitzSqlError> {
    let binder = Binder::new(schema_name).for_view_body();
    match stmt {
        Statement::CreateView(cv) => plan_create_view(cat, schema_name, cv, &binder),
        // `columns` is the positional output alias list, honored exactly as
        // `CREATE VIEW v (a, b) AS` is.
        Statement::AlterView { name, query, columns, with_options } => {
            // `ALTER VIEW` retargets a body; letting it set a budget would make
            // the clause-less form silently drop one. `CREATE OR REPLACE VIEW`
            // restates both.
            reject_if(!with_options.is_empty(), "ALTER VIEW", "WITH options")?;
            plan_alter_view(cat, schema_name, name, columns, query, &binder)
        }
        _ => Err(GnitzSqlError::Unsupported(
            "plan_view describes a CREATE VIEW or an ALTER VIEW … AS; this statement is neither".to_string(),
        )),
    }
}

/// What a `CREATE VIEW` / `ALTER VIEW … AS` statement asks the connection to do.
pub enum ViewPlan {
    /// Register `chain`, superseding the view that already holds its name when
    /// `replace` says so.
    Create { chain: PlannedChain, replace: bool },
    /// `IF NOT EXISTS`, and the name is taken: nothing to register. Carries the
    /// id of the relation already standing there.
    Skip { existing_id: u64 },
}

/// A planned `CREATE VIEW` / `ALTER VIEW … AS`: the user-facing name and the
/// segment bundle that implements it, the user-named view last.
pub struct PlannedChain {
    pub name: String,
    pub views: Vec<PlannedView>,
}

fn plan_create_view(
    cat: &CatalogSnapshot,
    schema_name: &str,
    cv: &sqlparser::ast::CreateView,
    binder: &Binder<'_>,
) -> Result<ViewPlan, GnitzSqlError> {
    reject_unhonored_create_view_clauses(cv)?;
    let query: &Query = &cv.query;
    let view_name = crate::ast_util::extract_object_name(&cv.name, schema_name, "CREATE VIEW")?;
    validate_user_name(&view_name)?;

    // Both clauses test the NAME, not the standing definition — a view's catalog
    // rows are its compiled circuit, never its text. Probed only under a clause, so
    // a plain CREATE VIEW still resolves nothing beyond what its body reads.
    let replaced_vid = if cv.if_not_exists {
        // Any relation under the name ends the statement, whatever its kind.
        if let Some(rel) = probe(cat, schema_name, &view_name)? {
            return Ok(ViewPlan::Skip { existing_id: rel.tid });
        }
        None
    } else if cv.or_replace {
        match probe(cat, schema_name, &view_name)? {
            Some(rel) => {
                // Replacing a table would destroy its rows; `DROP TABLE` says that
                // out loud.
                crate::validate::require_class(
                    &rel,
                    &format!("{schema_name}.{view_name}"),
                    crate::validate::ClassWant::View,
                    "CREATE OR REPLACE VIEW",
                )?;
                Some(rel.tid)
            }
            // A free name: `OR REPLACE` degenerates to a plain create.
            None => None,
        }
    } else {
        None
    };

    reject_unhonored_query_clauses(query, QueryEnvelope::ViewBody, "CREATE VIEW")?;

    let options = decode_view_options(&cv.options)?;

    let mut chain = ViewChain::new();
    build_query_segments(cat, query, binder, &mut chain, options, ViewSurface::CREATE)?;

    if let Some(old_vid) = replaced_vid {
        reject_self_reference(&chain, old_vid, schema_name, &view_name, "CREATE OR REPLACE VIEW")?;
    }

    apply_output_aliases(&mut chain, cv.columns.iter().map(|c| &c.name), "CREATE VIEW")?;

    Ok(ViewPlan::Create {
        chain: PlannedChain { name: view_name, views: chain.segments },
        replace: replaced_vid.is_some(),
    })
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
    columns: &[Ident],
    query: &Query,
    binder: &Binder<'_>,
) -> Result<ViewPlan, GnitzSqlError> {
    let view_name = crate::ast_util::extract_object_name(name, schema_name, "ALTER VIEW")?;
    validate_user_name(&view_name)?;

    // Resolve the old vid; reject `ALTER VIEW <table>` and a missing relation.
    let old_vid = resolve_view_id(cat, schema_name, &view_name)?;

    reject_unhonored_query_clauses(query, QueryEnvelope::ViewBody, "ALTER VIEW")?;

    let mut chain = ViewChain::new();
    build_query_segments(
        cat,
        query,
        binder,
        &mut chain,
        ViewOptions::default(),
        ViewSurface::ALTER,
    )?;

    reject_self_reference(&chain, old_vid, schema_name, &view_name, "ALTER VIEW")?;
    apply_output_aliases(&mut chain, columns.iter(), "ALTER VIEW")?;

    Ok(ViewPlan::Create {
        chain: PlannedChain { name: view_name, views: chain.segments },
        // `resolve_view_id` above already refused a bounded or fed view, which is
        // what `ALTER VIEW … AS` cannot restate — it carries no option clause.
        replace: true,
    })
}

/// Rename the chain's final segment's visible output columns positionally —
/// `CREATE VIEW v (x, y) AS …` and `ALTER VIEW v (x, y) AS …` alike. The final
/// segment is the user-named view, the only one whose column names a user wrote.
fn apply_output_aliases<'a>(
    chain: &mut ViewChain,
    aliases: impl ExactSizeIterator<Item = &'a Ident>,
    what: &str,
) -> Result<(), GnitzSqlError> {
    let final_seg = chain
        .segments
        .last_mut()
        .expect("build_query_segments pushes the final view");
    apply_positional_aliases(aliases, final_seg.output_columns.iter_mut().collect(), what)
}

/// A replacing bundle retracts `old_vid` in the same zone as it registers the new
/// chain, so the new body may not scan it: `FROM v` in the new query resolves to
/// the still-live old vid, and the new definition would lose its own input.
/// `what` names the statement for the message.
fn reject_self_reference(
    chain: &ViewChain,
    old_vid: u64,
    schema_name: &str,
    view_name: &str,
    what: &str,
) -> Result<(), GnitzSqlError> {
    if chain.segments.iter().any(|s| {
        s.circuit
            .nodes()
            .iter()
            .any(|n| matches!(n.op, OpNode::ScanDelta { source, .. } if source == old_vid))
    }) {
        return Err(GnitzSqlError::Unsupported(format!(
            "{what} '{schema_name}.{view_name}' AS a query referencing the view itself is not supported"
        )));
    }
    Ok(())
}

/// Commit a planned `CREATE [OR REPLACE] VIEW [IF NOT EXISTS]`; the owner's real
/// id comes back from the bundle. A skipped statement answers with the id of the
/// view already standing under the name — the same shape a create answers with,
/// because the caller asked for that name to hold a view and it does.
pub(crate) fn execute_create_view(
    client: &mut GnitzClient,
    schema_name: &str,
    plan: ViewPlan,
) -> Result<SqlResult, GnitzSqlError> {
    let (chain, replace) = match plan {
        ViewPlan::Skip { existing_id } => return Ok(SqlResult::ViewCreated { view_id: existing_id }),
        ViewPlan::Create { chain, replace } => (chain, replace),
    };
    let vids = client.create_view_chain(schema_name, &chain.name, chain.views, replace)?;
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
    plan: ViewPlan,
) -> Result<SqlResult, GnitzSqlError> {
    let ViewPlan::Create { chain, replace } = plan else {
        return Err(GnitzSqlError::Internal(
            "ALTER VIEW … AS always plans a bundle; only CREATE VIEW IF NOT EXISTS skips".to_string(),
        ));
    };
    client.create_view_chain(schema_name, &chain.name, chain.views, replace)?;
    Ok(SqlResult::Altered {
        object: "view".to_string(),
        name: chain.name,
    })
}

/// Resolve `name` to a VIEW id, rejecting `ALTER VIEW <table>` and a missing
/// relation.
fn resolve_view_id(cat: &CatalogSnapshot, schema_name: &str, name: &str) -> Result<u64, GnitzSqlError> {
    match probe(cat, schema_name, name)? {
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
        Some(rel) => {
            crate::validate::require_class(&rel, name, crate::validate::ClassWant::View, "ALTER VIEW")?;
            Ok(rel.tid)
        }
        None => Err(crate::error::missing_relation("View", schema_name, name)),
    }
}

/// Compile one query body into a chain of `PlannedView` segments (hidden
/// segments in dependency order, then the final view named `final_name`), filling
/// `chain.segments`. Does NOT reject unhonored tail clauses — the caller owns that
/// (CREATE VIEW rejects ORDER BY/LIMIT before calling).
fn build_query_segments(
    cat: &CatalogSnapshot,
    query: &Query,
    binder: &Binder<'_>,
    chain: &mut ViewChain,
    options: ViewOptions,
    surface: ViewSurface,
) -> Result<(), GnitzSqlError> {
    let capacity = options.capacity;
    let pieces = crate::hir::bind_and_lower(cat, binder, chain, query, capacity.is_some(), surface)?;

    // Both bounded shapes compile to one segment; a body that cut left an unbounded
    // copy of its rows behind, which a capacity would not bound.
    if capacity.is_some() && !chain.segments.is_empty() {
        return Err(GnitzSqlError::Unsupported(
            "CREATE VIEW WITH (capacity …): this body compiles to more than one view, whose \
             intermediate results are unbounded; only a filter/projection over one relation \
             and an inner equi-join are supported"
                .to_string(),
        ));
    }

    // The final segment: the hidden segments already sit on the chain in
    // dependency order; append the (user-named or synthetic) final view.
    chain.push_final(pieces, capacity, options.delta)
}

#[cfg(test)]
#[path = "tests/create.rs"]
mod tests;
