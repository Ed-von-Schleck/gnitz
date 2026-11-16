//! Statement dispatch: routes a `Statement` to its handler in `ddl` or `dml`,
//! each planned against the statement's catalog and then run on the client.

use crate::SqlResult;
use crate::catalog::Catalog;
use crate::error::GnitzSqlError;
use crate::error::reject_if;
use crate::source::StatementText;
use crate::{ddl, dml};
use gnitz_core::{GnitzClient, RelDescriptor, RelName};
use gnitz_wire::WireStatus;
use sqlparser::ast::{Query, Statement};
use std::cell::{Cell, RefCell};
use std::sync::Arc;

/// Which descriptors the client holds a plan may read before it asks the server:
/// a mirrored copy's, then a kept one.
#[derive(Clone, Copy)]
struct Descriptors {
    mirrored: bool,
    kept: bool,
}

impl Descriptors {
    /// Only the server's: DDL, a subscription, and DML no longer trusting kept ones.
    const SERVER: Self = Descriptors { mirrored: false, kept: false };
    /// A mirrored copy's: an EXPLAIN, and a SELECT no longer trusting kept ones.
    const MIRRORED: Self = Descriptors { mirrored: true, kept: false };
    /// A mirrored copy's, then a kept one: a SELECT.
    const MIRRORED_OR_KEPT: Self = Descriptors { mirrored: true, kept: true };
    /// A kept one: DML.
    const KEPT: Self = Descriptors { mirrored: false, kept: true };
}

/// One run of a plan: the names the server answered in earlier runs, then what
/// the client holds, else a miss that ends the run.
struct Run<'a> {
    client: &'a GnitzClient,
    schema_name: &'a str,
    descriptors: Descriptors,
    /// What the server answered, absence included.
    resolved: &'a [(RelName, Option<Arc<RelDescriptor>>)],
    /// Whether a kept descriptor went into this run.
    kept: Cell<bool>,
    /// The name this run asked for that nothing above answers.
    missed: RefCell<Option<RelName>>,
}

impl Catalog for Run<'_> {
    fn schema_name(&self) -> &str {
        self.schema_name
    }

    fn probe(&self, name: &RelName) -> Result<Option<Arc<RelDescriptor>>, GnitzSqlError> {
        if let Some((_, desc)) = self.resolved.iter().find(|(n, _)| n == name) {
            return Ok(desc.clone());
        }
        if self.descriptors.mirrored
            && let Some(desc) = self.client.mirrored_desc(name)
        {
            return Ok(Some(desc));
        }
        if self.descriptors.kept
            && let Some(desc) = self.client.kept_desc(name)
        {
            self.kept.set(true);
            return Ok(Some(desc));
        }
        *self.missed.borrow_mut() = Some(name.clone());
        Err(GnitzSqlError::Internal(format!(
            "relation '{name}' was planned before it was resolved"
        )))
    }
}

/// Plan against the descriptors `client` supplies for the names the plan asks
/// for: one it holds that `descriptors` admits, else the server's. Also answers
/// whether a kept one went into the plan.
async fn planned<T>(
    client: &mut GnitzClient,
    schema_name: &str,
    descriptors: Descriptors,
    plan: impl Fn(&dyn Catalog) -> Result<T, GnitzSqlError>,
) -> (Result<T, GnitzSqlError>, bool) {
    let mut resolved = Vec::new();
    loop {
        // `run` borrows `client`, so it ends before the resolve below.
        let missing = {
            let run = Run {
                client: &*client,
                schema_name,
                descriptors,
                resolved: &resolved,
                kept: Cell::new(false),
                missed: RefCell::new(None),
            };
            let planned = plan(&run);
            // Read before `planned`: a plan may swallow the miss's error.
            match run.missed.into_inner() {
                None => return (planned, run.kept.get()),
                Some(name) => name,
            }
        };
        match client.resolve(&missing).await {
            Ok(desc) => resolved.push((missing, desc)),
            // The server's answer for a name the client did not hold: no kept
            // descriptor decided it, so it stands.
            Err(e) => return (Err(e.into()), false),
        }
    }
}

/// Times [`planned_kept`] plans a statement before a refusal that wrote
/// nothing — `StaleCatalog` or `TxnConflict` — is the caller's to see. There is
/// no backoff.
const RUN_MAX_ATTEMPTS: usize = 4;

/// Plan and `run` a statement from the descriptors the client holds, until one
/// of them is in doubt and every later plan asks the server. A kept descriptor
/// may be stale, and only a request carrying its token finds that out, so:
///
/// - a `run` refused `StaleCatalog` wrote nothing, and the statement is planned
///   and run again, every later plan asking the server;
/// - a `run` refused `TxnConflict` — an autocommit read-modify-write whose
///   table was written after its read — wrote nothing, and the statement is
///   planned from the same descriptors and run again, reading afresh;
/// - a plan that fails, or one `checked` denies sends such a request, stands
///   only when no kept descriptor went into it; otherwise the statement is
///   planned again.
async fn planned_kept<P>(
    client: &mut GnitzClient,
    schema_name: &str,
    mut descriptors: Descriptors,
    plan: impl Fn(&dyn Catalog) -> Result<P, GnitzSqlError>,
    checked: impl Fn(&P) -> bool,
    run: impl AsyncFn(&mut GnitzClient, P) -> Result<SqlResult, GnitzSqlError>,
) -> Result<SqlResult, GnitzSqlError> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let plan = match planned(client, schema_name, descriptors, &plan).await {
            (Ok(plan), from_kept) if !from_kept || checked(&plan) => plan,
            (Err(e), false) => return Err(e),
            _ => {
                descriptors.kept = false;
                continue;
            }
        };
        match run(client, plan).await {
            Err(GnitzSqlError::Client(e)) if attempt < RUN_MAX_ATTEMPTS => match e.status() {
                Some(WireStatus::StaleCatalog) => descriptors.kept = false,
                // Only an autocommit statement's push is its own.
                Some(WireStatus::TxnConflict) if !client.txn_active() => {}
                _ => return Err(GnitzSqlError::Client(e)),
            },
            done => return done,
        }
    }
}

/// Route one statement, each planned from the [`Descriptors`] it admits: an
/// EXPLAIN sends no request to check a kept one.
pub(crate) async fn execute_statement(
    client: &mut GnitzClient,
    schema_name: &str,
    stmt: &Statement,
    text: StatementText<'_>,
) -> Result<SqlResult, GnitzSqlError> {
    match stmt {
        // Bare `DESC t` is `Statement::ExplainTable`, table introspection, and falls to
        // the catch-all below; `explained_query` rejects the EXPLAIN of a non-SELECT.
        Statement::Explain { .. } => {
            let query = dml::explained_query(stmt)?;
            let plan = planned(client, schema_name, Descriptors::MIRRORED, |cat| {
                dml::plan_read(query, cat)
            })
            .await
            .0?;
            Ok(dml::execute_explain(client, &plan))
        }
        Statement::Query(query) => {
            planned_kept(
                client,
                schema_name,
                Descriptors::MIRRORED_OR_KEPT,
                |cat| dml::plan_read(query, cat),
                |plan| !plan.answers_from_schema(),
                dml::execute_select,
            )
            .await
        }
        // Transaction control is a pure client-state-machine transition, so the
        // arm is the whole consumer: `transaction already open` and `no
        // transaction open` come from the `client.txn_*` calls below.
        // `transaction`, `begin` and `has_end_keyword` are inert phrasing.
        Statement::StartTransaction {
            modes,
            begin: _,
            transaction: _,
            modifier,
            statements,
            exception,
            has_end_keyword: _,
        } => {
            const CTX: &str = "BEGIN";
            // gnitz has one fixed isolation: atomicity + constraint consistency.
            reject_if(
                !modes.is_empty(),
                CTX,
                "transaction modes (READ ONLY / ISOLATION LEVEL)",
            )?;
            reject_if(modifier.is_some(), CTX, "a BEGIN modifier (DEFERRED / TRY / CATCH)")?;
            reject_if(!statements.is_empty(), CTX, "a BEGIN ... END block")?;
            reject_if(exception.is_some(), CTX, "an EXCEPTION clause")?;
            client.txn_begin()?;
            Ok(SqlResult::TransactionStarted)
        }
        // `end` is inert (`END` is a `COMMIT` spelling).
        Statement::Commit { chain, end: _, modifier } => {
            const CTX: &str = "COMMIT";
            // AND CHAIN would open an immediate successor transaction.
            reject_if(*chain, CTX, "AND CHAIN")?;
            reject_if(modifier.is_some(), CTX, "a COMMIT modifier (TRY / CATCH)")?;
            // A conflict is surfaced, not retried: the transaction is already
            // closed.
            Ok(SqlResult::TransactionCommitted { lsn: client.txn_commit().await? })
        }
        Statement::Rollback { chain, savepoint } => {
            const CTX: &str = "ROLLBACK";
            reject_if(*chain, CTX, "AND CHAIN")?;
            reject_if(savepoint.is_some(), CTX, "TO SAVEPOINT")?;
            client.txn_rollback()?;
            Ok(SqlResult::TransactionRolledBack)
        }
        // A write is checked wherever it lands: sent, by its own request;
        // buffered in a transaction, by the COMMIT that ships it.
        Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
            planned_kept(
                client,
                schema_name,
                Descriptors::KEPT,
                |cat| dml::plan_write(stmt, cat),
                |_| true,
                dml::execute_write,
            )
            .await
        }
        Statement::CreateTable(_)
        | Statement::CreateSchema { .. }
        | Statement::Drop { .. }
        | Statement::CreateView(_)
        | Statement::CreateIndex(_)
        | Statement::AlterTable(_)
        | Statement::AlterView(_) => {
            // Refused, not failed: the transaction stays open.
            if client.txn_active() {
                return Err(GnitzSqlError::Rejected(
                    "this statement is not allowed inside a transaction".to_string(),
                ));
            }
            Ok(SqlResult::Ddl {
                tag: execute_ddl(client, schema_name, stmt, text).await?,
            })
        }
        _ => Err(GnitzSqlError::Rejected(format!("unsupported SQL statement: {stmt}"))),
    }
}

/// Run one DDL statement and return its command tag; [`execute_statement`]
/// has refused it inside a transaction.
async fn execute_ddl(
    client: &mut GnitzClient,
    schema_name: &str,
    stmt: &Statement,
    text: StatementText<'_>,
) -> Result<&'static str, GnitzSqlError> {
    match stmt {
        Statement::CreateTable(create) => {
            if let Some(plan) = planned(client, schema_name, Descriptors::SERVER, |cat| {
                ddl::plan_create_table(create, cat)
            })
            .await
            .0?
            {
                client.create_table(&plan).await?;
            }
            Ok("CREATE TABLE")
        }
        // `Drop` and `CreateSchema` reject their clauses in the router: sqlparser
        // gives neither a payload struct, so their executors could destructure
        // them only by taking the whole `Statement` back.
        Statement::Drop {
            object_type,
            names,
            if_exists,
            cascade,
            restrict,
            purge,
            temporary,
            table,
        } => {
            const CTX: &str = "DROP";
            reject_if(*cascade, CTX, "CASCADE")?;
            reject_if(*restrict, CTX, "RESTRICT")?;
            reject_if(*purge, CTX, "PURGE")?;
            reject_if(*temporary, CTX, "TEMPORARY")?;
            reject_if(table.is_some(), CTX, "ON <table> (MySQL DROP INDEX target)")?;
            ddl::execute_drop(client, schema_name, object_type, names, *if_exists).await
        }
        Statement::CreateSchema {
            schema_name: name,
            or_replace,
            if_not_exists,
            with,
            options,
            default_collate_spec,
            clone,
        } => {
            const CTX: &str = "CREATE SCHEMA";
            reject_if(*or_replace, CTX, "OR REPLACE")?;
            reject_if(with.is_some(), CTX, "WITH options")?;
            reject_if(options.is_some(), CTX, "OPTIONS")?;
            reject_if(default_collate_spec.is_some(), CTX, "DEFAULT COLLATE")?;
            reject_if(clone.is_some(), CTX, "CLONE")?;
            ddl::execute_create_schema(client, name, *if_not_exists).await?;
            Ok("CREATE SCHEMA")
        }
        Statement::CreateView(cv) => {
            if let Some(chain) = planned(client, schema_name, Descriptors::SERVER, |cat| {
                ddl::plan_create_view(cv, text, cat)
            })
            .await
            .0?
            {
                client.create_view_chain(chain).await?;
            }
            Ok("CREATE VIEW")
        }
        Statement::CreateIndex(ci) => {
            ddl::execute_create_index(client, schema_name, ci).await?;
            Ok("CREATE INDEX")
        }
        Statement::AlterTable(a) => {
            ddl::execute_alter_table(client, schema_name, a).await?;
            Ok("ALTER TABLE")
        }
        Statement::AlterView(av) => {
            if let Some(chain) = planned(client, schema_name, Descriptors::SERVER, |cat| {
                ddl::plan_alter_view(av, text, cat)
            })
            .await
            .0?
            {
                client.create_view_chain(chain).await?;
            }
            Ok("ALTER VIEW")
        }
        _ => unreachable!("execute_statement routes only DDL here"),
    }
}

/// Plan `query`, a `SELECT` over one fed view, as a subscription, against the
/// server's own descriptors: what a copy holds is no source to subscribe to.
pub(crate) async fn plan_subscription(
    client: &mut GnitzClient,
    schema_name: &str,
    query: &Query,
) -> Result<gnitz_core::Subscription, GnitzSqlError> {
    planned(client, schema_name, Descriptors::SERVER, |cat| {
        dml::plan_read(query, cat)
    })
    .await
    .0?
    .into_subscription()
}

#[cfg(all(test, feature = "integration"))]
#[path = "tests/dispatch.rs"]
mod tests;
