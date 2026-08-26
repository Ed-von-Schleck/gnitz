//! Statement dispatch — the one module that reaches both the compile side
//! (`ddl` and the `hir` view compiler) and the execute side (`dml`). Builds the
//! per-statement `Binder` and routes a `Statement` to the matching handler.

use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::validate::{
    reject_unhonored_alter_table_clauses, reject_unhonored_alter_view_clauses, reject_unhonored_commit_clauses,
    reject_unhonored_create_index_clauses, reject_unhonored_create_table_clauses, reject_unhonored_create_view_clauses,
    reject_unhonored_delete_clauses, reject_unhonored_drop_clauses, reject_unhonored_explain_clauses,
    reject_unhonored_insert_clauses, reject_unhonored_rollback_clauses, reject_unhonored_start_transaction_clauses,
    reject_unhonored_update_clauses,
};
use crate::SqlResult;
use crate::{ddl, dml};
use gnitz_core::CatalogSnapshot;
use gnitz_core::{ClientError, GnitzClient, ReadTarget};
use sqlparser::ast::Statement;

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

/// Run `plan` against the statement's catalog snapshot, resolving each name it
/// reports missing and re-running.
///
/// A planning pass has no side effects — it reads the snapshot and builds owned
/// values, minting segment ids symbolically — so a discarded pass costs CPU over
/// an already-parsed AST, not a round trip. One resolve per name the planner
/// asks for, and none for a name it does not.
pub(crate) fn plan_resolving<T>(
    reads: &mut dyn ReadTarget,
    schema_name: &str,
    mut plan: impl FnMut(&CatalogSnapshot) -> Result<T, GnitzSqlError>,
) -> Result<T, GnitzSqlError> {
    loop {
        let missing = match plan(reads.client_mut().catalog()) {
            Err(GnitzSqlError::CatalogMiss(name)) => name,
            other => return other,
        };
        // Progress, and so termination: `describe_relation` records its answer, and
        // `CatalogSnapshot` keys both sides through `qualified_name`, so a repeat
        // ask is a broken invariant rather than a second round trip.
        if reads.client_mut().catalog().get(schema_name, &missing).is_some() {
            return Err(GnitzSqlError::Internal(format!(
                "planning re-asked for relation '{missing}', which the statement's snapshot already holds"
            )));
        }
        reads.describe_relation(schema_name, &missing)?;
    }
}

/// Route one statement.
///
/// A query and the `EXPLAIN` of one go to `reads`, which is what holds the
/// relation: routing them to the connection instead would cost a round trip and
/// describe the *server's* relation, which a stale local registration need not
/// agree with. Everything below the split is DDL, DML or transaction control,
/// which only a connection can serve. Those arms take `reads.client_mut()`, and
/// that borrow is what stops a DDL statement from planning against a local
/// binding only as fresh as the last poll.
pub(crate) fn execute_statement(
    reads: &mut dyn ReadTarget,
    schema_name: &str,
    stmt: &Statement,
) -> Result<SqlResult, GnitzSqlError> {
    reject_in_transaction(reads.client_mut(), stmt)?;

    match stmt {
        Statement::Query(_) => {
            let plan = plan_resolving(reads, schema_name, |cat| crate::plan_read(stmt, cat, schema_name))?;
            return dml::execute_select(reads, plan);
        }
        // Bare `DESC t` is `Statement::ExplainTable` — table introspection, a
        // separate feature — and falls to the catch-all below. `plan_read` rejects
        // the EXPLAIN of a non-SELECT.
        Statement::Explain { .. } => {
            reject_unhonored_explain_clauses(stmt, "EXPLAIN")?;
            let plan = plan_resolving(reads, schema_name, |cat| crate::plan_read(stmt, cat, schema_name))?;
            return dml::execute_explain(plan);
        }
        _ => {}
    }

    // Only the write verbs below reach a binder: the read paths above bind inside
    // the pure planner, against the statement's snapshot rather than a connection.
    let mut binder = Binder::new(schema_name);

    let client = reads.client_mut();
    match stmt {
        // Transaction control. Each is a pure client-state-machine transition
        // (no compile, no data reshape), so the handler is inlined here; the
        // clause-reject lives in `crate::validate` like every other statement's.
        // All state-machine errors (`transaction already open`, `no transaction
        // open`) are raised by the `client.txn_*` calls.
        Statement::StartTransaction { .. } => {
            reject_unhonored_start_transaction_clauses(stmt, "BEGIN")?;
            client.txn_begin()?;
            Ok(SqlResult::TransactionStarted)
        }
        Statement::Commit { .. } => {
            reject_unhonored_commit_clauses(stmt, "COMMIT")?;
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
        Statement::Rollback { .. } => {
            reject_unhonored_rollback_clauses(stmt, "ROLLBACK")?;
            client.txn_rollback()?;
            Ok(SqlResult::TransactionRolledBack)
        }
        Statement::CreateTable(create) => {
            reject_unhonored_create_table_clauses(create, "CREATE TABLE")?;
            ddl::execute_create_table(client, schema_name, create)
        }
        Statement::Drop { object_type, names, .. } => {
            reject_unhonored_drop_clauses(stmt, "DROP")?;
            ddl::execute_drop(client, schema_name, object_type, names)
        }
        Statement::CreateView(cv) => {
            reject_unhonored_create_view_clauses(cv, "CREATE VIEW")?;
            let views = plan_resolving(client, schema_name, |cat| crate::plan_view(stmt, cat, schema_name))?;
            crate::hir::execute_create_view(client, schema_name, views)
        }
        Statement::Insert(insert) => {
            reject_unhonored_insert_clauses(insert, "INSERT")?;
            dml::execute_insert(client, insert, &mut binder)
        }
        Statement::CreateIndex(ci) => {
            reject_unhonored_create_index_clauses(ci, "CREATE INDEX")?;
            ddl::execute_create_index(client, schema_name, ci, &mut binder)
        }
        Statement::Update(update) => {
            reject_unhonored_update_clauses(update, "UPDATE")?;
            dml::execute_update(client, update, &mut binder)
        }
        Statement::Delete(del) => {
            reject_unhonored_delete_clauses(del, "DELETE")?;
            dml::execute_delete(client, del, &mut binder)
        }
        Statement::AlterTable(a) => {
            reject_unhonored_alter_table_clauses(a, "ALTER TABLE")?;
            // Exactly one operation per statement; a comma-separated multi-op ALTER
            // is rejected (each op has distinct commit/validation needs, and DROP
            // COLUMN's `column_names: Vec<Ident>` already collapses `DROP a, b` into
            // one operation — so this only rejects genuinely separate operations).
            if a.operations.len() != 1 {
                return Err(GnitzSqlError::Unsupported(
                    "ALTER TABLE with multiple comma-separated operations is not supported".to_string(),
                ));
            }
            ddl::execute_alter_table(client, schema_name, a, &mut binder)
        }
        Statement::AlterView {
            columns, with_options, ..
        } => {
            reject_unhonored_alter_view_clauses(columns, with_options, "ALTER VIEW")?;
            let views = plan_resolving(client, schema_name, |cat| crate::plan_view(stmt, cat, schema_name))?;
            crate::hir::execute_alter_view(client, schema_name, views)
        }
        _ => Err(GnitzSqlError::Unsupported(format!(
            "unsupported SQL statement: {stmt:?}"
        ))),
    }
}
