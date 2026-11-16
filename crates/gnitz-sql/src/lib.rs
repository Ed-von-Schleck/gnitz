//! The SQL front end: parses, plans and runs statements against a `GnitzClient`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items; tests no single module owns live in `suites/`.

#![warn(unreachable_pub)]

mod access;
mod agg;
mod ast_util;
mod bind;
mod catalog;
mod ddl;
mod dialect;
mod dispatch;
mod dml;
mod error;
mod exec;
mod expr_lower;
mod hir;
mod ir;
mod literal;
mod names;
mod project;
mod rules;
mod source;
#[cfg(test)]
mod suites;
mod tail;
#[cfg(test)]
mod test_support;
mod types;
mod validate;

pub use ddl::options::{with_option_names, with_options};
pub use ddl::render::{
    create_index_statements, create_schema_statement, create_table_statement, create_view_statement, qualified_name,
};
pub use dialect::GnitzDialect;
pub use error::GnitzSqlError;
pub use literal::{Placed, parse_bool, place_scaled};
pub use names::{
    column_text, parse_identifier, parse_relation_name, parse_schema_name, relation_name, relation_text,
    table_factor_name,
};
pub use source::Offsets;
pub use sqlparser;
pub use types::{sql_type_name, type_names};

use gnitz_core::{ClientError, GnitzClient, Planner, PollOutcome, ScanReply, Subscription};
use sqlparser::ast::Statement;
use std::sync::Arc;

/// Result of executing a single SQL statement.
#[derive(Debug)]
pub enum SqlResult {
    /// A CREATE, DROP or ALTER succeeded — or, under `IF [NOT] EXISTS`, had
    /// nothing to do. `tag` is its command tag, e.g. `CREATE INDEX`.
    Ddl {
        tag: &'static str,
    },
    /// An INSERT, UPDATE or DELETE wrote `count` rows; `tag` is its verb.
    RowsAffected {
        tag: &'static str,
        count: usize,
    },
    Rows(ScanReply),
    /// `BEGIN` / `START TRANSACTION`: a client-side transaction buffer opened.
    TransactionStarted,
    /// `COMMIT`: the buffer shipped as one atomic frame. `lsn` is the zone LSN
    /// (0 for an empty commit).
    TransactionCommitted {
        lsn: u64,
    },
    /// `ROLLBACK`: the buffer discarded, nothing sent.
    TransactionRolledBack,
}

/// Parse `sql` and run each statement, one `SqlResult` per statement. A `SELECT`
/// (and its `EXPLAIN`) over a relation the client's local copy holds is answered
/// off that copy; everything else runs on the connection.
pub async fn execute(client: &mut GnitzClient, schema_name: &str, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError> {
    let stmts = source::split_statements(sql)?;
    let mut results = Vec::with_capacity(stmts.len());
    // One begun by an earlier call is the caller's to end.
    let mut began_here = false;
    for (stmt, text) in &stmts {
        match dispatch::execute_statement(client, schema_name, stmt, *text).await {
            Ok(r) => {
                began_here |= matches!(r, SqlResult::TransactionStarted);
                results.push(r);
            }
            Err(e) => {
                if began_here && client.txn_active() {
                    let _ = client.txn_rollback();
                }
                return Err(e);
            }
        }
    }
    Ok(results)
}

/// Every function name a statement may call, in the spelling the binder matches
/// case-insensitively: the scalar functions, the ones sqlparser parses into
/// their own nodes, the aggregates and the window-only ranking functions.
pub fn function_names() -> impl Iterator<Item = &'static str> {
    bind::scalar_call_names()
        .chain(bind::KEYWORD_CALLS.iter().copied())
        .chain(agg::agg_names())
        .chain(hir::ranking_names())
}

/// Every unit name `EXTRACT` and `DATE_PART` accept, singular and lowercase.
pub fn calendar_field_names() -> impl Iterator<Item = &'static str> {
    bind::CALENDAR_FIELDS.iter().map(|&(n, _)| n)
}

/// Plan `sql`, one `SELECT` over one view with a delta feed, as a subscription:
/// what a delta read of the view carries to be answered with only the rows and
/// columns the `SELECT` keeps. The view's key rides along, hidden where the
/// `SELECT` does not name it.
pub async fn plan_subscription(
    client: &mut GnitzClient,
    schema_name: &str,
    sql: &str,
) -> Result<Subscription, GnitzSqlError> {
    match &source::split_statements(sql)?[..] {
        [(Statement::Query(query), _)] => dispatch::plan_subscription(client, schema_name, query).await,
        [_] => Err(error::unsupported_clause(
            "a subscription",
            "a statement other than SELECT",
        )),
        [] => Err(GnitzSqlError::Rejected(
            "a subscription needs a SELECT over a view".into(),
        )),
        _ => Err(error::unsupported_clause("a subscription", "more than one statement")),
    }
}

/// Mirror `sql` as the local relation `alias`; see
/// [`GnitzClient::mirror_subscription`]. A poll may plan `sql` again, in
/// `schema_name`.
pub async fn mirror_subscription(
    client: &mut GnitzClient,
    schema_name: &str,
    alias: &str,
    sql: &str,
) -> Result<PollOutcome, GnitzSqlError> {
    let (schema_name, sql) = (schema_name.to_string(), sql.to_string());
    let plan: Planner = Arc::new(move |client| {
        let (schema_name, sql) = (schema_name.clone(), sql.clone());
        Box::pin(async move {
            plan_subscription(client, &schema_name, &sql)
                .await
                .map_err(|e| match e {
                    GnitzSqlError::Client(e) => e,
                    e => ClientError::from(e.to_string()),
                })
        })
    });
    Ok(client.mirror_subscription(alias, plan).await?)
}

// A host spawns a statement onto a multi-thread runtime, or runs it with the
// GIL released; both need its future `Send`.
const _: fn() = || {
    fn assert_send_value<T: Send>(_: T) {}
    let _ = |client: &mut GnitzClient| assert_send_value(execute(client, "", ""));
};
