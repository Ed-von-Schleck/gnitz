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
mod codec;
mod ddl;
mod dispatch;
mod dml;
mod error;
mod exec;
mod expr_lower;
mod hir;
mod ir;
mod project;
mod rules;
#[cfg(test)]
mod suites;
mod tail;
#[cfg(test)]
mod test_support;
mod types;
mod validate;

pub use error::GnitzSqlError;

use gnitz_core::{ClientError, GnitzClient, Planner, PollOutcome, ScanReply, Subscription};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

/// Result of executing a single SQL statement.
#[derive(Debug)]
pub enum SqlResult {
    /// A CREATE, DROP or ALTER succeeded — or, under `IF [NOT] EXISTS`, had nothing to do.
    Ddl,
    RowsAffected {
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
    let stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let mut results = Vec::with_capacity(stmts.len());
    // One begun by an earlier call is the caller's to end.
    let mut began_here = false;
    for stmt in &stmts {
        match dispatch::execute_statement(client, schema_name, stmt).await {
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

/// Plan `sql`, one `SELECT` over one view with a delta feed, as a subscription:
/// what a delta read of the view carries to be answered with only the rows and
/// columns the `SELECT` keeps. The view's key rides along, hidden where the
/// `SELECT` does not name it.
pub async fn plan_subscription(
    client: &mut GnitzClient,
    schema_name: &str,
    sql: &str,
) -> Result<Subscription, GnitzSqlError> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql)?;
    let [stmt] = &stmts[..] else {
        return Err(error::unsupported_clause("a subscription", "more than one statement"));
    };
    dispatch::plan_subscription(client, schema_name, stmt).await
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
