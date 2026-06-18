mod binder;
pub mod dml;
mod error;
mod expr;
mod logical_plan;
pub mod planner;
mod types;

pub use error::GnitzSqlError;

use gnitz_core::GnitzClient;
use gnitz_core::{Schema, ZSetBatch};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Result of executing a single SQL statement.
pub enum SqlResult {
    TableCreated { table_id: u64 },
    ViewCreated { view_id: u64 },
    IndexCreated { index_id: u64 },
    Dropped,
    RowsAffected { count: usize },
    Rows { schema: Schema, batch: ZSetBatch },
}

/// High-level SQL execution planner.
pub struct SqlPlanner<'a> {
    client: &'a mut GnitzClient,
    schema_name: String,
}

impl<'a> SqlPlanner<'a> {
    pub fn new(client: &'a mut GnitzClient, schema_name: impl Into<String>) -> Self {
        SqlPlanner {
            client,
            schema_name: schema_name.into(),
        }
    }

    /// Parse `sql` and execute each statement, returning one `SqlResult` per statement.
    pub fn execute(&mut self, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError> {
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
