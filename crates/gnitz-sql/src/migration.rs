//! Client-side migration helpers: re-exports the pure SQL→DesiredState
//! parser from `gnitz-sql-parse` and adds the view-circuit compile
//! bridge that `push_migration` needs.

pub use gnitz_sql_parse::migration::{
    canonicalize, decanonicalize, compute_migration_hash, diff_by_name,
    topo_sort_diff, validate_drop_closure,
    parse_desired_state,
    ColumnDef, DesiredState, Diff, IndexDef, TableDef, ViewDef, ViewDep,
};

use gnitz_core::GnitzClient;
use sqlparser::ast::Statement;
use gnitz_sql_parse::migration::parse_desired_state_with_stmts;
use crate::error::GnitzSqlError;
use crate::planner::compile_view_for_migration;

/// Parse migration SQL into a `DesiredState` with every `CREATE VIEW`
/// circuit compiled and attached. The SQL is parsed only once.
pub fn parse_desired_state_with_circuits(
    sql: &str, client: &GnitzClient, default_schema: &str,
) -> Result<DesiredState, GnitzSqlError> {
    let (mut state, stmts) = parse_desired_state_with_stmts(sql, default_schema)?;
    if state.views.is_empty() {
        return Ok(state);
    }
    let view_stmts = stmts.iter()
        .filter(|s| matches!(s, Statement::CreateView { .. }));
    for (view_def, stmt) in state.views.iter_mut().zip(view_stmts) {
        let (circuit, _out_cols) =
            compile_view_for_migration(client, &view_def.schema, stmt)?;
        view_def.circuit = circuit;
    }
    Ok(state)
}
