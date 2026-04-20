//! Client-side re-export of the pure SQL→DesiredState parser, plus
//! the view-compile bridge used by `push_migration` to populate each
//! `ViewDef.circuit` before the AST is canonicalised.
//!
//! The parser itself lives in `gnitz-sql-parse` (no client dep) so
//! `gnitz-engine` can link it without pulling in `GnitzClient`. This
//! module preserves the existing path (`gnitz_sql::migration::...`)
//! for any client-side consumer.

pub use gnitz_sql_parse::migration::{
    canonicalize, decanonicalize, compute_migration_hash, diff_by_name,
    topo_sort_diff, validate_drop_closure,
    parse_desired_state,
    ColumnDef, DesiredState, Diff, IndexDef, TableDef, ViewDef, ViewDep,
};

use gnitz_core::GnitzClient;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use crate::error::GnitzSqlError;
use crate::planner::compile_view_for_migration;

/// Parse migration SQL into `DesiredState`, then compile every
/// `CREATE VIEW` into its wire-form `CircuitGraph` and attach it to
/// the matching `ViewDef`. Consumed by the client-side
/// `push_migration` path so a single `CREATE TABLE ...; CREATE VIEW
/// ...` script produces a self-contained migration row that the
/// server applies atomically.
pub fn parse_desired_state_with_circuits(
    sql: &str, client: &GnitzClient, default_schema: &str,
) -> Result<DesiredState, GnitzSqlError> {
    let mut state = parse_desired_state(sql, default_schema)?;
    if state.views.is_empty() {
        return Ok(state);
    }

    // Re-parse to find each CREATE VIEW statement in order; the
    // resulting Vec lines up with `state.views` since `parse_desired_state`
    // preserves declaration order.
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    let view_stmts: Vec<&Statement> = statements.iter()
        .filter(|s| matches!(s, Statement::CreateView { .. }))
        .collect();
    if view_stmts.len() != state.views.len() {
        return Err(GnitzSqlError::Bind(format!(
            "internal: parsed {} ViewDefs but found {} CREATE VIEW statements",
            state.views.len(), view_stmts.len(),
        )));
    }

    for (i, stmt) in view_stmts.iter().enumerate() {
        // Compile with each view's own schema as the binder default so
        // qualified / unqualified source references in FROM both resolve
        // correctly.
        let view_schema = state.views[i].schema.clone();
        let (circuit, _out_cols) =
            compile_view_for_migration(client, &view_schema, stmt)?;
        state.views[i].circuit = circuit;
    }
    Ok(state)
}
