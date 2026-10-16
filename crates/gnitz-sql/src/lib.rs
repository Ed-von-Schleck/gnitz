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
mod source;
#[cfg(test)]
mod suites;
mod tail;
#[cfg(test)]
mod test_support;
mod types;
mod validate;

pub use codec::literal::parse_bool;
pub use ddl::render::{
    create_index_statements, create_schema_statement, create_table_statement, create_view_statement, qualified_name,
    view_body,
};
pub use error::GnitzSqlError;
pub use source::Offsets;
pub use sqlparser;
pub use types::{sql_type_name, type_names};

use gnitz_core::{ClientError, GnitzClient, Planner, PollOutcome, RelName, RelProps, ScanReply, Subscription};
use sqlparser::ast::{Expr, Ident, ObjectName, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;
use std::borrow::Cow;
use std::sync::Arc;

/// The dialect every statement is parsed under. A client that tokenizes text it
/// will submit uses this one, so it splits statements where the parser does.
pub const DIALECT: GenericDialect = GenericDialect {};

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

/// The relation a `FROM` item names, and the name the statement reads it by:
/// its alias, else its unqualified name. `None` for anything but a plain
/// table reference the binder accepts.
pub fn table_factor_name(tf: &TableFactor, schema: &str) -> Option<(RelName, String)> {
    let tref = ast_util::extract_table_ref(tf, schema, "").ok()?;
    Some((tref.name, tref.alias))
}

/// The relation `name` names, unqualified ones in `schema`.
pub fn relation_name(name: &ObjectName, schema: &str) -> Option<RelName> {
    ast_util::extract_object_name(name, schema, "").ok()
}

/// `text`, the whole of it, read as a relation name in `schema`.
pub fn parse_relation_name(text: &str, schema: &str) -> Option<RelName> {
    relation_name(&read_back(text, |p| p.parse_object_name(false))?, schema)
}

/// `text` parsed whole by `parse`.
fn read_back<T>(
    text: &str,
    parse: impl FnOnce(&mut Parser<'_>) -> Result<T, sqlparser::parser::ParserError>,
) -> Option<T> {
    let mut p = Parser::new(&DIALECT).try_with_sql(text).ok()?;
    let out = parse(&mut p).ok()?;
    p.expect_token(&Token::EOF).ok()?;
    Some(out)
}

fn quoted(name: &str) -> Cow<'_, str> {
    Cow::Owned(Ident::with_quote('"', name).to_string())
}

/// `name` as a column reference reads it: bare when it parses back as that
/// identifier in a select list, else quoted. A standalone expression is too
/// lenient a test: sqlparser reads `from` there as an identifier.
pub fn column_text(name: &str) -> Cow<'_, str> {
    let stmts = Parser::parse_sql(&DIALECT, &format!("SELECT {name} FROM t")).unwrap_or_default();
    let bare = match &stmts[..] {
        [Statement::Query(q)] => match &*q.body {
            SetExpr::Select(s) => match &s.projection[..] {
                [SelectItem::UnnamedExpr(Expr::Identifier(i))] => i.quote_style.is_none() && i.value == name,
                _ => false,
            },
            _ => false,
        },
        _ => false,
    };
    match bare {
        true => Cow::Borrowed(name),
        false => quoted(name),
    }
}

/// One part of a relation name as a `FROM` item reads it: bare when it parses
/// back as a table of that one-part name, else quoted.
pub fn relation_text(part: &str) -> Cow<'_, str> {
    let bare = match read_back(part, |p| p.parse_table_factor()) {
        Some(TableFactor::Table { name, alias: None, .. }) => match &name.0[..] {
            [p] => p.as_ident().is_some_and(|i| i.quote_style.is_none() && i.value == part),
            _ => false,
        },
        _ => false,
    };
    match bare {
        true => Cow::Borrowed(part),
        false => quoted(part),
    }
}

/// Every `WITH (…)` option key a `CREATE TABLE` or `CREATE VIEW` accepts.
pub fn with_option_names() -> impl Iterator<Item = &'static str> {
    ddl::TABLE_OPTIONS.into_iter().chain(ddl::VIEW_OPTIONS)
}

/// The `WITH (…)` entries that declare `props`, as a `CREATE` writes them; the
/// defaults are omitted.
pub fn with_options(props: &RelProps) -> Vec<String> {
    match props {
        RelProps::Table(t) => ddl::table_with_options(t),
        RelProps::View(v) => ddl::view_with_options(v),
    }
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
    let stmts = Parser::parse_sql(&DIALECT, sql)?;
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

#[cfg(test)]
#[path = "tests/lib.rs"]
mod tests;
