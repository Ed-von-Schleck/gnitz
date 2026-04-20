//! Client-side SQL parser for migrations.
//!
//! The canonical AST types, serialisation, diff, and hash live in
//! `gnitz_wire::migration` so the server can share them without
//! depending on sqlparser. This file re-exports those items and adds
//! the SQL→DesiredState parser.

pub use gnitz_wire::migration::{
    canonicalize, decanonicalize, compute_migration_hash, diff_by_name,
    topo_sort_diff, validate_drop_closure,
    ColumnDef, DesiredState, Diff, IndexDef, TableDef, ViewDef, ViewDep,
};

use sqlparser::ast::{
    ColumnOption, ColumnOptionDef, CreateIndex, CreateTable, Ident,
    ObjectName, Statement, TableConstraint,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::GnitzSqlError;
use crate::types::sql_type_to_typecode;

/// Parse a migration's declarative SQL into a complete desired state.
///
/// Input is one-or-more top-level `CREATE TABLE / VIEW / INDEX`
/// statements separated by semicolons. Any other statement kind
/// (INSERT, UPDATE, DROP, ALTER, ...) is rejected — migrations carry
/// the full target catalog, not a diff script.
pub fn parse_desired_state(
    sql: &str, default_schema: &str,
) -> Result<DesiredState, GnitzSqlError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(GnitzSqlError::Parse)?;

    let mut tables: Vec<TableDef> = Vec::new();
    let mut views: Vec<ViewDef> = Vec::new();
    let mut indices: Vec<IndexDef> = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::CreateTable(ct) => tables.push(table_from_create(ct, default_schema)?),
            Statement::CreateView { name, query, .. } => {
                let (schema, vname) = split_qualified(&name, default_schema);
                views.push(ViewDef { schema, name: vname, sql: query.to_string() });
            }
            Statement::CreateIndex(ci) => indices.push(index_from_create(ci, default_schema)?),
            _ => return Err(GnitzSqlError::Unsupported(
                "migration SQL may only contain CREATE TABLE / VIEW / INDEX".into(),
            )),
        }
    }
    Ok(DesiredState { tables, views, indices })
}

fn split_qualified(name: &ObjectName, default_schema: &str) -> (String, String) {
    let parts: Vec<String> = name.0.iter().map(obj_part_to_string).collect();
    match parts.len() {
        1 => (default_schema.to_string(), parts.into_iter().next().unwrap()),
        2 => {
            let mut it = parts.into_iter();
            let s = it.next().unwrap();
            let n = it.next().unwrap();
            (s, n)
        }
        _ => (default_schema.to_string(), parts.join(".")),
    }
}

fn obj_part_to_string(part: &sqlparser::ast::ObjectNamePart) -> String {
    use sqlparser::ast::ObjectNamePart;
    match part {
        ObjectNamePart::Identifier(id) => id.value.clone(),
    }
}

fn ident_to_string(id: &Ident) -> String { id.value.clone() }

fn table_from_create(
    ct: CreateTable, default_schema: &str,
) -> Result<TableDef, GnitzSqlError> {
    let (schema, name) = split_qualified(&ct.name, default_schema);

    let mut pk_col_idx: Option<u32> = None;
    let mut columns: Vec<ColumnDef> = Vec::with_capacity(ct.columns.len());

    for (col_idx, col) in ct.columns.iter().enumerate() {
        let tc = sql_type_to_typecode(&col.data_type)?;
        let mut is_nullable = true;
        let mut fk_schema = String::new();
        let mut fk_table = String::new();

        for ColumnOptionDef { option, .. } in &col.options {
            match option {
                ColumnOption::NotNull => { is_nullable = false; }
                ColumnOption::Null => { is_nullable = true; }
                ColumnOption::Unique { is_primary, .. } if *is_primary => {
                    pk_col_idx = Some(col_idx as u32);
                    is_nullable = false;
                }
                ColumnOption::ForeignKey { foreign_table, .. } => {
                    let (fs, fn_) = split_qualified(foreign_table, default_schema);
                    fk_schema = fs; fk_table = fn_;
                }
                _ => {}
            }
        }

        columns.push(ColumnDef {
            name: ident_to_string(&col.name),
            type_code: tc as u8,
            is_nullable,
            fk_schema, fk_table, fk_col_idx: 0,
        });
    }

    for c in &ct.constraints {
        match c {
            TableConstraint::PrimaryKey { columns: pk_cols, .. } => {
                if let Some(first) = pk_cols.first() {
                    let name = ident_to_string(first);
                    if let Some(idx) = columns.iter().position(|c| c.name == name) {
                        pk_col_idx = Some(idx as u32);
                        columns[idx].is_nullable = false;
                    }
                }
            }
            TableConstraint::ForeignKey {
                columns: fk_cols, foreign_table, ..
            } => {
                let Some(first) = fk_cols.first() else { continue; };
                let col_name = ident_to_string(first);
                let Some(idx) = columns.iter().position(|c| c.name == col_name) else { continue; };
                let (fs, fn_) = split_qualified(foreign_table, default_schema);
                columns[idx].fk_schema = fs;
                columns[idx].fk_table = fn_;
            }
            _ => {}
        }
    }

    let pk_col_idx = pk_col_idx.ok_or_else(|| GnitzSqlError::Unsupported(format!(
        "table {}.{} has no PRIMARY KEY", schema, name,
    )))?;

    Ok(TableDef {
        schema, name, columns, pk_col_idx,
        unique_pk: true,
    })
}

fn index_from_create(
    ci: CreateIndex, default_schema: &str,
) -> Result<IndexDef, GnitzSqlError> {
    let idx_qualified = ci.name.clone().ok_or_else(|| GnitzSqlError::Unsupported(
        "CREATE INDEX without explicit name".into(),
    ))?;
    let (schema, name) = split_qualified(&idx_qualified, default_schema);
    let (owner_schema, owner_name) = split_qualified(&ci.table_name, default_schema);
    let first_col = ci.columns.first().ok_or_else(|| GnitzSqlError::Unsupported(
        "CREATE INDEX requires at least one column".into(),
    ))?;
    let source_col = match &first_col.column.expr {
        sqlparser::ast::Expr::Identifier(id) => ident_to_string(id),
        other => format!("{}", other),
    };
    Ok(IndexDef {
        schema, name,
        owner_schema, owner_name,
        source_col,
        is_unique: ci.unique,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_create_table() {
        let sql = "CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY, email TEXT NOT NULL)";
        let s = parse_desired_state(sql, "public").unwrap();
        assert_eq!(s.tables.len(), 1);
        assert_eq!(s.tables[0].schema, "public");
        assert_eq!(s.tables[0].name, "users");
        assert_eq!(s.tables[0].columns.len(), 2);
        assert_eq!(s.tables[0].pk_col_idx, 0);
    }

    #[test]
    fn parse_create_index() {
        let sql = "CREATE TABLE t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL); \
                   CREATE UNIQUE INDEX i_x ON t (x)";
        let s = parse_desired_state(sql, "public").unwrap();
        assert_eq!(s.indices.len(), 1);
        assert_eq!(s.indices[0].name, "i_x");
        assert_eq!(s.indices[0].owner_name, "t");
        assert_eq!(s.indices[0].source_col, "x");
        assert!(s.indices[0].is_unique);
    }

    #[test]
    fn parse_rejects_dml_in_migration() {
        let sql = "INSERT INTO t VALUES (1)";
        let err = parse_desired_state(sql, "public").unwrap_err();
        assert!(err.to_string().contains("CREATE TABLE"));
    }

    #[test]
    fn diff_classifies_basic() {
        let parent = DesiredState {
            tables: vec![TableDef {
                schema: "public".into(), name: "a".into(),
                columns: vec![ColumnDef {
                    name: "id".into(), type_code: 8, is_nullable: false,
                    fk_schema: String::new(), fk_table: String::new(), fk_col_idx: 0,
                }],
                pk_col_idx: 0, unique_pk: true,
            }],
            views: vec![], indices: vec![],
        };
        let new = DesiredState {
            tables: vec![],
            views: vec![], indices: vec![],
        };
        let d = diff_by_name(&parent, &new);
        assert_eq!(d.dropped_tables.len(), 1);
        assert_eq!(d.created_tables.len(), 0);
    }
}
