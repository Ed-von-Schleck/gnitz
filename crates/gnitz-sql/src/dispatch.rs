//! Statement dispatch — the one module that reaches both the compile side
//! (`ddl` and the `hir` view compiler) and the execute side (`dml`). Routes a
//! `Statement` to the matching handler.

use crate::bind::Catalog;
use crate::error::reject_if;
use crate::error::GnitzSqlError;
use crate::SqlResult;
use crate::{ddl, dml, hir};
use gnitz_core::{ClientError, GnitzClient, RelDescriptor};
use sqlparser::ast::Statement;
use std::cell::RefCell;
use std::sync::Arc;

/// A client's resolve of one relation name under a schema.
type ClientResolve = fn(&mut GnitzClient, &str, &str) -> Result<Option<Arc<RelDescriptor>>, ClientError>;

/// Plan against a catalog that resolves each name through `resolve` on first use.
fn planned<T>(
    client: &mut GnitzClient,
    schema_name: &str,
    resolve: ClientResolve,
    plan: impl FnOnce(&Catalog<'_>) -> Result<T, GnitzSqlError>,
) -> Result<T, GnitzSqlError> {
    let client = RefCell::new(client);
    let ask = |name: &str| Ok(resolve(&mut client.borrow_mut(), schema_name, name)?);
    plan(&Catalog::new(schema_name, &ask))
}

/// Route one statement. Only a read resolves local-first.
pub(crate) fn execute_statement(
    client: &mut GnitzClient,
    schema_name: &str,
    stmt: &Statement,
) -> Result<SqlResult, GnitzSqlError> {
    match stmt {
        // Bare `DESC t` is `Statement::ExplainTable`, table introspection, and falls to
        // the catch-all below; `plan_read` rejects the EXPLAIN of a non-SELECT.
        Statement::Query(_) | Statement::Explain { .. } => {
            let plan = planned(client, schema_name, GnitzClient::resolve_local_first, |cat| {
                dml::plan_read(stmt, cat)
            })?;
            match stmt {
                Statement::Explain { .. } => Ok(dml::execute_explain(client, &plan)),
                _ => dml::execute_select(client, plan),
            }
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
            // A COMMIT-time OCC conflict is not auto-retried — the buffered reads
            // are stale by definition. `txn_commit` already took the buffer out
            // (transaction closed), so surfacing the `TxnConflict` refusal leaves
            // nothing open; the application re-runs the whole transaction from
            // BEGIN.
            Ok(SqlResult::TransactionCommitted { lsn: client.txn_commit()? })
        }
        Statement::Rollback { chain, savepoint } => {
            const CTX: &str = "ROLLBACK";
            reject_if(*chain, CTX, "AND CHAIN")?;
            reject_if(savepoint.is_some(), CTX, "TO SAVEPOINT")?;
            client.txn_rollback()?;
            Ok(SqlResult::TransactionRolledBack)
        }
        Statement::Insert(insert) => dml::execute_insert(client, schema_name, insert),
        Statement::Update(update) => dml::execute_update(client, schema_name, update),
        Statement::Delete(del) => dml::execute_delete(client, schema_name, del),
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
                hir::plan_create_view(cv, cat)
            })? {
                Some(chain) => hir::execute_view_chain(client, schema_name, chain),
                None => Ok(SqlResult::Ddl),
            }
        }
        Statement::CreateIndex(ci) => ddl::execute_create_index(client, schema_name, ci),
        Statement::AlterTable(a) => ddl::execute_alter_table(client, schema_name, a),
        Statement::AlterView { name, query, columns, with_options } => {
            let chain = planned(client, schema_name, GnitzClient::resolve, |cat| {
                hir::plan_alter_view(name, columns, query, with_options, cat)
            })?;
            hir::execute_view_chain(client, schema_name, chain)
        }
        _ => Err(GnitzSqlError::Rejected(format!("unsupported SQL statement: {stmt}"))),
    }
}
