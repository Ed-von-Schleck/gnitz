//! Statement dispatch: routes a `Statement` to its handler in `ddl` or `dml`,
//! each planned against the statement's catalog and then run on the client.

use crate::bind::Catalog;
use crate::error::reject_if;
use crate::error::GnitzSqlError;
use crate::SqlResult;
use crate::{ddl, dml};
use gnitz_core::{ClientError, GnitzClient, RelDescriptor};
use gnitz_wire::{WireFault, WireStatus};
use sqlparser::ast::Statement;
use std::cell::{Cell, RefCell};
use std::sync::Arc;

/// A client's answer for one relation name under a schema.
type Resolved = Result<Option<Arc<RelDescriptor>>, ClientError>;

/// Plan against a catalog that resolves each name through `resolve` on first use.
fn planned<T>(
    client: &mut GnitzClient,
    schema_name: &str,
    resolve: impl Fn(&mut GnitzClient, &str, &str) -> Resolved,
    plan: impl FnOnce(&Catalog<'_>) -> Result<T, GnitzSqlError>,
) -> Result<T, GnitzSqlError> {
    let client = RefCell::new(client);
    let ask = |name: &str| Ok(resolve(&mut client.borrow_mut(), schema_name, name)?);
    plan(&Catalog::new(schema_name, &ask))
}

/// Times [`planned_kept`] plans a statement before its `StaleCatalog` refusal
/// is the caller's to see.
const STALE_MAX_ATTEMPTS: usize = 4;

/// Plan and `run` a statement, the first time against the descriptors the
/// client keeps and every later time against `resolve`'s answers alone. A kept
/// descriptor may be stale, and only a request carrying its token finds that
/// out, so:
///
/// - a `run` refused `StaleCatalog` wrote nothing, and the statement is planned
///   and run again;
/// - a plan that fails, or one `checked` denies sends such a request, stands
///   only when no kept descriptor went into it; otherwise the statement is
///   planned again.
fn planned_kept<P>(
    client: &mut GnitzClient,
    schema_name: &str,
    resolve: fn(&mut GnitzClient, &str, &str) -> Resolved,
    plan: impl Fn(&Catalog<'_>) -> Result<P, GnitzSqlError>,
    checked: impl Fn(&P) -> bool,
    run: impl Fn(&mut GnitzClient, P) -> Result<SqlResult, GnitzSqlError>,
) -> Result<SqlResult, GnitzSqlError> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let from_kept = Cell::new(false);
        let planned = planned(
            client,
            schema_name,
            |client, schema_name, name| match (attempt == 1).then(|| client.kept(schema_name, name)).flatten() {
                Some(kept) => {
                    from_kept.set(true);
                    Ok(Some(kept))
                }
                None => resolve(client, schema_name, name),
            },
            &plan,
        );
        let plan = match planned {
            Ok(plan) if !from_kept.get() || checked(&plan) => plan,
            Err(e) if !from_kept.get() => return Err(e),
            _ => continue,
        };
        match run(client, plan) {
            Err(GnitzSqlError::Client(ClientError::Refused(WireFault {
                status: WireStatus::StaleCatalog, ..
            }))) if attempt < STALE_MAX_ATTEMPTS => {}
            done => return done,
        }
    }
}

/// Route one statement. Only a read resolves local-first, and only DML and a
/// `SELECT` plan from kept descriptors: an EXPLAIN sends no request to check one.
pub(crate) fn execute_statement(
    client: &mut GnitzClient,
    schema_name: &str,
    stmt: &Statement,
) -> Result<SqlResult, GnitzSqlError> {
    match stmt {
        // Bare `DESC t` is `Statement::ExplainTable`, table introspection, and falls to
        // the catch-all below; `plan_read` rejects the EXPLAIN of a non-SELECT.
        Statement::Explain { .. } => {
            let plan = planned(client, schema_name, GnitzClient::resolve_local_first, |cat| {
                dml::plan_read(stmt, cat)
            })?;
            Ok(dml::execute_explain(client, &plan))
        }
        Statement::Query(_) => planned_kept(
            client,
            schema_name,
            GnitzClient::resolve_local_first,
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
            // A COMMIT-time OCC conflict is not auto-retried — the buffered reads
            // are stale by definition. `txn_commit` already took the buffer out
            // (transaction closed), so surfacing the `TxnConflict` refusal leaves
            // nothing open; the application re-runs the whole transaction from
            // BEGIN. `txn_commit` reports a written relation altered since this
            // client resolved it as the same conflict.
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
            GnitzClient::resolve,
            |cat| dml::plan_insert(insert, cat),
            |_| true,
            dml::execute_insert,
        ),
        Statement::Update(update) => planned_kept(
            client,
            schema_name,
            GnitzClient::resolve,
            |cat| dml::plan_update(update, cat),
            |_| true,
            dml::execute_mutation,
        ),
        Statement::Delete(del) => planned_kept(
            client,
            schema_name,
            GnitzClient::resolve,
            |cat| dml::plan_delete(del, cat),
            |_| true,
            dml::execute_mutation,
        ),
        // Refused, not failed: the transaction stays open.
        _ if client.txn_active() => Err(GnitzSqlError::Rejected(
            "this statement is not allowed inside a transaction".to_string(),
        )),
        Statement::CreateTable(create) => {
            match planned(client, schema_name, GnitzClient::resolve, |cat| {
                ddl::plan_create_table(create, cat)
            })? {
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
            match planned(client, schema_name, GnitzClient::resolve, |cat| {
                ddl::plan_create_view(cv, cat)
            })? {
                Some(chain) => ddl::execute_view_chain(client, schema_name, chain),
                None => Ok(SqlResult::Ddl),
            }
        }
        Statement::CreateIndex(ci) => ddl::execute_create_index(client, schema_name, ci),
        Statement::AlterTable(a) => ddl::execute_alter_table(client, schema_name, a),
        Statement::AlterView { name, query, columns, with_options } => {
            let chain = planned(client, schema_name, GnitzClient::resolve, |cat| {
                ddl::plan_alter_view(name, columns, query, with_options, cat)
            })?;
            ddl::execute_view_chain(client, schema_name, chain)
        }
        _ => Err(GnitzSqlError::Rejected(format!("unsupported SQL statement: {stmt}"))),
    }
}
