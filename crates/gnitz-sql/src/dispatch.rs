//! Statement dispatch — the one module that reaches both the compile side
//! (`ddl` and the `hir` view compiler) and the execute side (`dml`). Builds the
//! per-statement `Binder` and routes a `Statement` to the matching handler.

use crate::bind::Binder;
use crate::error::reject_if;
use crate::error::GnitzSqlError;
use crate::SqlResult;
use crate::{ddl, dml};
use gnitz_core::CatalogSnapshot;
use gnitz_core::{ClientError, GnitzClient, RelDescriptor};
use sqlparser::ast::Statement;
use std::sync::Arc;

/// Inside a transaction, only DML and transaction control may run. Everything
/// else — today's DDL, and every statement added later — is rejected by default:
/// DDL has its own atomic commit mechanism and cannot interleave with a
/// transaction's buffered user-table writes (a batch buffered under the old
/// schema would guarantee a commit-time schema-mismatch). A non-poisoning
/// statement error: the transaction stays open.
fn reject_in_transaction(client: &GnitzClient, stmt: &Statement) -> Result<(), GnitzSqlError> {
    let allowed = matches!(
        stmt,
        Statement::Insert(_)
            | Statement::Query(_)
            // EXPLAIN issues strictly less than the `Query` above: catalog
            // lookups and no data-path request.
            | Statement::Explain { .. }
            | Statement::Update(_)
            | Statement::Delete(_)
            // A nested BEGIN is allowed through to `txn_begin`, which raises
            // "transaction already open" — the accurate error.
            | Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
    );
    if client.txn_active() && !allowed {
        return Err(GnitzSqlError::Unsupported(
            "this statement is not allowed inside a transaction".to_string(),
        ));
    }
    Ok(())
}

/// The two spellings of "describe one relation" a statement can plan against:
/// [`GnitzClient::resolve_local_first`], which answers off a mirrored
/// registration, and [`GnitzClient::resolve`], which always asks the server.
type Resolve = fn(&mut GnitzClient, &str, &str) -> Result<Option<Arc<RelDescriptor>>, ClientError>;

/// Run `plan` against the statement's catalog snapshot, resolving each name it
/// reports missing through `resolve` and re-running.
///
/// A planning pass has no side effects — it reads the snapshot and builds owned
/// values, minting segment ids symbolically — so a discarded pass costs CPU over
/// an already-parsed AST, not a round trip. One resolve per name the planner
/// asks for, and none for a name it does not.
///
/// **`resolve` is a parameter and not a fixed choice**, because two of the four
/// call sites are `CREATE VIEW` and `ALTER VIEW`: routing those through a local
/// copy would compile a shipped circuit, or an `ALTER`'s outgoing view id,
/// against a name → id binding only as fresh as the last poll.
pub(crate) fn plan_resolving<T>(
    client: &mut GnitzClient,
    resolve: Resolve,
    schema_name: &str,
    mut plan: impl FnMut(&CatalogSnapshot) -> Result<T, GnitzSqlError>,
) -> Result<T, GnitzSqlError> {
    loop {
        let missing = match plan(client.catalog()) {
            Err(GnitzSqlError::CatalogMiss(name)) => name,
            other => return other,
        };
        // Progress, and so termination: both resolves record their answer, and
        // `CatalogSnapshot` keys both sides through `qualified_name`, so a repeat
        // ask is a broken invariant rather than a second round trip.
        if client.catalog().get(schema_name, &missing).is_some() {
            return Err(GnitzSqlError::Internal(format!(
                "planning re-asked for relation '{missing}', which the statement's snapshot already holds"
            )));
        }
        resolve(client, schema_name, &missing)?;
    }
}

/// Route one statement.
///
/// A query and the `EXPLAIN` of one resolve and read **local-first**: a relation
/// the client's own copy holds is described and read off it, which is what keeps
/// a mirrored `SELECT` round-trip-free. Everything below the split is DDL, DML or
/// transaction control, which only a connection can serve — and what stops one of
/// those planning against a binding only as fresh as the last poll is the resolve
/// its arm names, [`GnitzClient::resolve`].
pub(crate) fn execute_statement(
    client: &mut GnitzClient,
    schema_name: &str,
    stmt: &Statement,
) -> Result<SqlResult, GnitzSqlError> {
    reject_in_transaction(client, stmt)?;

    match stmt {
        Statement::Query(_) => {
            let plan = plan_resolving(client, GnitzClient::resolve_local_first, schema_name, |cat| {
                crate::plan_read(stmt, cat, schema_name)
            })?;
            return dml::execute_select(client, plan);
        }
        // Bare `DESC t` is `Statement::ExplainTable` — table introspection, a
        // separate feature — and falls to the catch-all below. `plan_read` rejects
        // the EXPLAIN of a non-SELECT.
        Statement::Explain { .. } => {
            let plan = plan_resolving(client, GnitzClient::resolve_local_first, schema_name, |cat| {
                crate::plan_read(stmt, cat, schema_name)
            })?;
            return Ok(dml::execute_explain(&plan));
        }
        _ => {}
    }

    // Only the write verbs below reach a binder: the read paths above bind inside
    // the pure planner, against the statement's snapshot rather than a connection.
    let mut binder = Binder::new(schema_name);

    match stmt {
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
            // (transaction closed), so surfacing `Conflict` leaves nothing open;
            // the application re-runs the whole transaction from BEGIN.
            match client.txn_commit() {
                Ok(lsn) => Ok(SqlResult::TransactionCommitted { lsn }),
                Err(ClientError::TxnConflict { .. }) => Err(GnitzSqlError::Conflict { table: None }),
                Err(e) => Err(GnitzSqlError::Exec(e)),
            }
        }
        Statement::Rollback { chain, savepoint } => {
            const CTX: &str = "ROLLBACK";
            reject_if(*chain, CTX, "AND CHAIN")?;
            reject_if(savepoint.is_some(), CTX, "TO SAVEPOINT")?;
            client.txn_rollback()?;
            Ok(SqlResult::TransactionRolledBack)
        }
        Statement::CreateTable(create) => {
            let plan = plan_resolving(client, GnitzClient::resolve, schema_name, |cat| {
                crate::plan_create_table(create, cat, schema_name)
            })?;
            ddl::execute_create_table(client, schema_name, plan)
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
        Statement::CreateView(_) => {
            let plan = plan_resolving(client, GnitzClient::resolve, schema_name, |cat| {
                crate::plan_view(stmt, cat, schema_name)
            })?;
            crate::hir::execute_create_view(client, schema_name, plan)
        }
        Statement::Insert(insert) => dml::execute_insert(client, insert, &mut binder),
        Statement::CreateIndex(ci) => ddl::execute_create_index(client, schema_name, ci, &mut binder),
        Statement::Update(update) => dml::execute_update(client, update, &mut binder),
        Statement::Delete(del) => dml::execute_delete(client, del, &mut binder),
        Statement::AlterTable(a) => ddl::execute_alter_table(client, schema_name, a),
        Statement::AlterView { .. } => {
            let plan = plan_resolving(client, GnitzClient::resolve, schema_name, |cat| {
                crate::plan_view(stmt, cat, schema_name)
            })?;
            crate::hir::execute_alter_view(client, schema_name, plan)
        }
        _ => Err(GnitzSqlError::Unsupported(format!("unsupported SQL statement: {stmt}"))),
    }
}
