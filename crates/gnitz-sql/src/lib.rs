mod ast_util;
mod bind;
mod codec;
mod dispatch;
mod dml;
mod error;
mod exec;
mod ir;
mod lower;
mod plan;
#[cfg(test)]
mod test_support;
mod types;

pub use error::GnitzSqlError;

use gnitz_core::GnitzClient;
use gnitz_core::{Schema, ZSetBatch};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Result of executing a single SQL statement.
#[derive(Debug)]
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
    ///
    /// Each statement's catalog reads run against a **statement-scoped
    /// snapshot**: the first read of each system table scans it over the wire
    /// once, and every later read within the same statement's planning is
    /// served from that in-memory copy (one relation resolution otherwise costs
    /// 3-4 full-system-table wire scans, repeated per relation). The snapshot is
    /// dropped at the end of each statement — one statement, one snapshot, no
    /// cross-statement state — so the next statement sees this one's DDL writes.
    pub fn execute(&mut self, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError> {
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)?;
        let mut results = Vec::with_capacity(stmts.len());
        for stmt in &stmts {
            self.client.begin_catalog_snapshot();
            let r = dispatch::execute_statement(self.client, &self.schema_name, stmt);
            self.client.end_catalog_snapshot();
            results.push(r?);
        }
        Ok(results)
    }
}
