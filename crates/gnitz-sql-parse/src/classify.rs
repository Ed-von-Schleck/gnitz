//! Classify a SQL submission as DDL-only, DML-only, or mixed. Used by
//! server-side `execute_sql` dispatch to decide between the migration
//! path and the push path without pulling sqlparser into the engine.

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::GnitzSqlParseError;

/// Result of classifying a SQL submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Classification {
    pub has_ddl: bool,
    pub has_dml: bool,
}

/// Parse `sql` and walk the statement list, flagging whether any
/// statement is DDL (CREATE/DROP/schema-create) and/or DML
/// (INSERT/UPDATE/DELETE/SELECT). Returns a parse error on invalid SQL
/// and an `Unsupported` error on statement kinds that are neither.
pub fn classify_sql(sql: &str) -> Result<Classification, GnitzSqlParseError> {
    let dialect = GenericDialect {};
    let stmts   = Parser::parse_sql(&dialect, sql)
        .map_err(GnitzSqlParseError::Parse)?;

    let mut out = Classification { has_ddl: false, has_dml: false };
    for stmt in &stmts {
        match stmt {
            Statement::CreateTable(_)
            | Statement::CreateView { .. }
            | Statement::CreateIndex(_)
            | Statement::CreateSchema { .. }
            | Statement::Drop { .. } => out.has_ddl = true,

            Statement::Insert(_)
            | Statement::Update { .. }
            | Statement::Delete(_)
            | Statement::Query(_) => out.has_dml = true,

            _ => return Err(GnitzSqlParseError::Unsupported(
                format!("statement kind not supported in execute_sql: {:?}", stmt),
            )),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_only() {
        let c = classify_sql("CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY)").unwrap();
        assert!(c.has_ddl && !c.has_dml);
    }

    #[test]
    fn dml_only_insert() {
        let c = classify_sql("INSERT INTO t VALUES (1)").unwrap();
        assert!(!c.has_ddl && c.has_dml);
    }

    #[test]
    fn dml_only_select() {
        let c = classify_sql("SELECT * FROM t").unwrap();
        assert!(!c.has_ddl && c.has_dml);
    }

    #[test]
    fn mixed_flags_both() {
        let c = classify_sql(
            "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY); INSERT INTO t VALUES (1)",
        ).unwrap();
        assert!(c.has_ddl && c.has_dml);
    }

    #[test]
    fn drop_counts_as_ddl() {
        let c = classify_sql("DROP TABLE t").unwrap();
        assert!(c.has_ddl && !c.has_dml);
    }
}
