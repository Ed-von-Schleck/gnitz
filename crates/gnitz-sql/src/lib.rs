mod error;
mod types;
mod logical_plan;
mod binder;
mod expr;
pub mod dml;
pub mod migration;
pub mod planner;

pub use error::GnitzSqlError;

use gnitz_core::GnitzClient;
use gnitz_protocol::{Schema, ZSetBatch};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Result of executing a single SQL statement.
pub enum SqlResult {
    TableCreated  { table_id:  u64 },
    ViewCreated   { view_id:   u64 },
    IndexCreated  { index_id:  u64 },
    Dropped,
    RowsAffected  { count: usize },
    Rows          { schema: Schema, batch: ZSetBatch },
}

/// High-level SQL execution planner.
///
/// Uses a shared reference to `GnitzClient` — no `Arc` needed.
pub struct SqlPlanner<'a> {
    client:      &'a GnitzClient,
    schema_name: String,
}

impl<'a> SqlPlanner<'a> {
    pub fn new(client: &'a GnitzClient, schema_name: impl Into<String>) -> Self {
        SqlPlanner { client, schema_name: schema_name.into() }
    }

    /// Parse `sql` and execute each statement, returning one `SqlResult` per statement.
    pub fn execute(&self, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError> {
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)?;
        let mut results = Vec::with_capacity(stmts.len());
        for stmt in &stmts {
            let r = planner::execute_statement(self.client, &self.schema_name, stmt)?;
            results.push(r);
        }
        Ok(results)
    }
}
