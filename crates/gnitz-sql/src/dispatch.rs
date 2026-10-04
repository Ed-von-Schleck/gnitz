//! Statement dispatch: routes a `Statement` to its handler in `ddl` or `dml`,
//! each planned against the statement's catalog and then run on the client.

use crate::bind::Catalog;
use crate::error::reject_if;
use crate::error::GnitzSqlError;
use crate::SqlResult;
use crate::{ddl, dml};
use gnitz_core::{ClientError, GnitzClient, Held};
use gnitz_wire::{WireFault, WireStatus};
use sqlparser::ast::Statement;
use std::cell::{Cell, RefCell};

/// Plan against a catalog that asks `client` for each name on first use: what
/// it holds — the copy's descriptor for a `read`, a kept one when `use_kept` —
/// else the server. Also answers whether a kept descriptor went into the plan.
fn planned<T>(
    client: &mut GnitzClient,
    schema_name: &str,
    read: bool,
    use_kept: bool,
    plan: impl FnOnce(&Catalog<'_>) -> Result<T, GnitzSqlError>,
) -> (Result<T, GnitzSqlError>, bool) {
    let client = RefCell::new(client);
    let from_kept = Cell::new(false);
    let ask = |name: &str| {
        let mut client = client.borrow_mut();
        let held = client
            .held(schema_name, name, read)
            .filter(|&(_, held)| use_kept || held == Held::Copy);
        Ok(match held {
            Some((desc, held)) => {
                from_kept.set(from_kept.get() || held == Held::Kept);
                Some(desc)
            }
            None => client.resolve(schema_name, name)?,
        })
    };
    let planned = plan(&Catalog::new(schema_name, &ask));
    (planned, from_kept.get())
}

/// Times [`planned_kept`] plans a statement before its `StaleCatalog` refusal
/// is the caller's to see.
const STALE_MAX_ATTEMPTS: usize = 4;

/// Plan and `run` a statement from the descriptors the client holds, until one
/// of them is in doubt and every later plan asks the server. A kept descriptor
/// may be stale, and only a request carrying its token finds that out, so:
///
/// - a `run` refused `StaleCatalog` wrote nothing, and the statement is planned
///   and run again;
/// - a plan that fails, or one `checked` denies sends such a request, stands
///   only when no kept descriptor went into it; otherwise the statement is
///   planned again.
fn planned_kept<P>(
    client: &mut GnitzClient,
    schema_name: &str,
    read: bool,
    plan: impl Fn(&Catalog<'_>) -> Result<P, GnitzSqlError>,
    checked: impl Fn(&P) -> bool,
    run: impl Fn(&mut GnitzClient, P) -> Result<SqlResult, GnitzSqlError>,
) -> Result<SqlResult, GnitzSqlError> {
    let mut use_kept = true;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let plan = match planned(client, schema_name, read, use_kept, &plan) {
            (Ok(plan), from_kept) if !from_kept || checked(&plan) => plan,
            (Err(e), false) => return Err(e),
            _ => {
                use_kept = false;
                continue;
            }
        };
        match run(client, plan) {
            Err(GnitzSqlError::Client(ClientError::Refused(WireFault {
                status: WireStatus::StaleCatalog, ..
            }))) if attempt < STALE_MAX_ATTEMPTS => use_kept = false,
            done => return done,
        }
    }
}

/// Route one statement. Only a read is planned from a mirrored copy's
/// descriptor, and only DML and a `SELECT` from kept ones: an EXPLAIN sends no
/// request to check one.
pub(crate) fn execute_statement(
    client: &mut GnitzClient,
    schema_name: &str,
    stmt: &Statement,
) -> Result<SqlResult, GnitzSqlError> {
    match stmt {
        // Bare `DESC t` is `Statement::ExplainTable`, table introspection, and falls to
        // the catch-all below; `plan_read` rejects the EXPLAIN of a non-SELECT.
        Statement::Explain { .. } => {
            let plan = planned(client, schema_name, true, false, |cat| dml::plan_read(stmt, cat)).0?;
            Ok(dml::execute_explain(client, &plan))
        }
        Statement::Query(_) => planned_kept(
            client,
            schema_name,
            true,
            |cat| dml::plan_read(stmt, cat),
            |plan| !plan.answers_from_schema(),
            dml::execute_select,
        ),
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
            Ok(SqlResult::TransactionCommitted { lsn: client.txn_commit()? })
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
        Statement::Insert(insert) => planned_kept(
            client,
            schema_name,
            false,
            |cat| dml::plan_insert(insert, cat),
            |_| true,
            dml::execute_insert,
        ),
        Statement::Update(update) => planned_kept(
            client,
            schema_name,
            false,
            |cat| dml::plan_update(update, cat),
            |_| true,
            dml::execute_mutation,
        ),
        Statement::Delete(del) => planned_kept(
            client,
            schema_name,
            false,
            |cat| dml::plan_delete(del, cat),
            |_| true,
            dml::execute_mutation,
        ),
        // Refused, not failed: the transaction stays open.
        Statement::CreateTable(_)
        | Statement::Drop { .. }
        | Statement::CreateView(_)
        | Statement::CreateIndex(_)
        | Statement::AlterTable(_)
        | Statement::AlterView { .. }
            if client.txn_active() =>
        {
            Err(GnitzSqlError::Rejected(
                "this statement is not allowed inside a transaction".to_string(),
            ))
        }
        Statement::CreateTable(create) => {
            match planned(client, schema_name, false, false, |cat| {
                ddl::plan_create_table(create, cat)
            })
            .0?
            {
                Some(plan) => ddl::execute_create_table(client, schema_name, plan),
                None => Ok(SqlResult::Ddl),
            }
        }
        // The one statement whose clause rejections live in the router: sqlparser
        // gives `Drop` no payload struct, so `execute_drop` could destructure it
        // only by taking the whole `Statement` back.
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
            ddl::execute_drop(client, schema_name, object_type, names, *if_exists)
        }
        Statement::CreateView(cv) => {
            match planned(client, schema_name, false, false, |cat| ddl::plan_create_view(cv, cat)).0? {
                Some(chain) => ddl::execute_view_chain(client, schema_name, chain),
                None => Ok(SqlResult::Ddl),
            }
        }
        Statement::CreateIndex(ci) => ddl::execute_create_index(client, schema_name, ci),
        Statement::AlterTable(a) => ddl::execute_alter_table(client, schema_name, a),
        Statement::AlterView { name, query, columns, with_options } => {
            let chain = planned(client, schema_name, false, false, |cat| {
                ddl::plan_alter_view(name, columns, query, with_options, cat)
            })
            .0?;
            ddl::execute_view_chain(client, schema_name, chain)
        }
        _ => Err(GnitzSqlError::Rejected(format!("unsupported SQL statement: {stmt}"))),
    }
}
