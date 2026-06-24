use crate::error::GnitzSqlError;
use gnitz_core::{ColumnDef, GnitzClient, Schema};
use std::collections::HashMap;
use std::rc::Rc;

/// A relation resolved by name — the single encoding of "name → resolved
/// relation" shared by the `Binder` cache (a single-table context, `col_offset`
/// always 0) and a join `AliasMap` (each alias carries its base offset into the
/// combined A‖B column space).
pub(crate) struct ResolvedRelation {
    pub table_id: u64,
    pub schema: Rc<Schema>,
    /// 0 for a single-table `Binder` cache; the real base offset of this
    /// relation's columns within a join's combined column space.
    pub col_offset: usize,
}

/// alias/name → resolved relation, for multi-table (join) column resolution.
pub(crate) type AliasMap = HashMap<String, ResolvedRelation>;

/// Find the column named `col_name` in `columns`, case-insensitively.
///
/// - `Ok(Some(idx))` — exactly one column matches.
/// - `Ok(None)` — no column matches (callers attach their own "not found" text).
/// - `Err(Bind)` — more than one column matches. A `SELECT *` join view carries
///   same-named columns from both sides; an unqualified reference to such a name
///   is ambiguous (standard SQL) and must be rejected, not silently bound to the
///   first match.
///
/// Single home for the name→index lookup that was previously
/// `columns.iter().position(...)` (first-match) at every call site.
pub(crate) fn find_unique_column(columns: &[ColumnDef], col_name: &str) -> Result<Option<usize>, GnitzSqlError> {
    let mut found: Option<usize> = None;
    for (i, c) in columns.iter().enumerate() {
        if c.name.eq_ignore_ascii_case(col_name) {
            if found.is_some() {
                return Err(GnitzSqlError::Bind(format!(
                    "column reference '{col_name}' is ambiguous"
                )));
            }
            found = Some(i);
        }
    }
    Ok(found)
}

/// Resolves a column in a multi-table context (for joins).
/// `tables` is keyed by lowercased aliases, so the `table_alias` probe is lowercased
/// to match — SQL identifiers are case-insensitive, but the case-preserving dialect
/// hands us the raw spelling (e.g. `A` for alias `a`).
pub(crate) fn resolve_qualified_column(
    table_alias: &str,
    col_name: &str,
    tables: &AliasMap,
) -> Result<usize, GnitzSqlError> {
    let rel = tables
        .get(&table_alias.to_lowercase())
        .ok_or_else(|| GnitzSqlError::Bind(format!("table alias '{table_alias}' not found")))?;
    let idx = find_unique_column(&rel.schema.columns, col_name)?
        .ok_or_else(|| GnitzSqlError::Bind(format!("column '{col_name}' not found in table '{table_alias}'")))?;
    Ok(rel.col_offset + idx)
}

/// Resolves an unqualified column in a multi-table context.
/// Tries each table in order; errors on ambiguity.
pub(crate) fn resolve_unqualified_column(col_name: &str, tables: &AliasMap) -> Result<usize, GnitzSqlError> {
    let mut found: Option<usize> = None;
    for rel in tables.values() {
        // The `?` rejects a name duplicated *within* one source relation (joining
        // a dup-named `SELECT *` view); the `found.is_some()` check below rejects a
        // name that appears across two source relations.
        if let Some(idx) = find_unique_column(&rel.schema.columns, col_name)? {
            if found.is_some() {
                return Err(GnitzSqlError::Bind(format!(
                    "ambiguous column '{col_name}' — qualify with table alias"
                )));
            }
            found = Some(rel.col_offset + idx);
        }
    }
    found.ok_or_else(|| GnitzSqlError::Bind(format!("column '{col_name}' not found in any table")))
}

pub(crate) struct Binder<'a> {
    schema_name: &'a str,
    cache: AliasMap,
}

impl<'a> Binder<'a> {
    pub(crate) fn new(schema_name: &'a str) -> Self {
        Binder {
            schema_name,
            cache: HashMap::new(),
        }
    }

    /// Cache a `name → relation` entry. The cache is a single-table context, so
    /// `col_offset` is always 0; a join's real per-relation offset lives only in
    /// the planner's `AliasMap`, never here.
    fn cache_relation(&mut self, name: String, table_id: u64, schema: Rc<Schema>) {
        self.cache.insert(
            name,
            ResolvedRelation {
                table_id,
                schema,
                col_offset: 0,
            },
        );
    }

    pub(crate) fn resolve(&mut self, client: &mut GnitzClient, name: &str) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        if let Some(entry) = self.cache.get(name) {
            return Ok((entry.table_id, Rc::clone(&entry.schema)));
        }
        let (tid, schema) = client
            .resolve_table_or_view_id(self.schema_name, name)
            .map_err(GnitzSqlError::Exec)?;
        let rc = Rc::new(schema);
        self.cache_relation(name.to_string(), tid, Rc::clone(&rc));
        Ok((tid, rc))
    }

    /// Resolve a write/index target that must be a base table. INSERT, UPDATE,
    /// DELETE and CREATE INDEX may only act on base tables: a view is a read-only
    /// derived relation whose store is maintained solely by its circuit, so
    /// writing to it or indexing it corrupts that state. `resolve` (used by
    /// SELECT and view definitions) still accepts views; this is the
    /// writable-target variant.
    pub(crate) fn resolve_base_table(
        &mut self,
        client: &mut GnitzClient,
        name: &str,
    ) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        // `resolve_table_id` consults TABLE_TAB only, so a view name misses it.
        match client.resolve_table_id(self.schema_name, name) {
            Ok((tid, schema)) => {
                let rc = Rc::new(schema);
                self.cache_relation(name.to_string(), tid, Rc::clone(&rc));
                Ok((tid, rc))
            }
            // Miss: re-probe including views to tell "is a view" (reject as
            // read-only) from "does not exist" (propagate the original error).
            Err(not_found) => match client.resolve_table_or_view_id(self.schema_name, name) {
                Ok(_) => Err(GnitzSqlError::Unsupported(format!(
                    "'{name}' is a view; INSERT, UPDATE, DELETE and CREATE INDEX \
                     require a base table"
                ))),
                Err(_) => Err(GnitzSqlError::Exec(not_found)),
            },
        }
    }

    /// Cache a CTE or alias name as resolving to the given (table_id, schema).
    pub(crate) fn cache_alias(&mut self, name: String, resolved: (u64, Rc<Schema>)) {
        self.cache_relation(name, resolved.0, resolved.1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_core::{ColumnDef, TypeCode};

    fn col(name: &str, tc: TypeCode) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            type_code: tc,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
        }
    }

    #[test]
    fn test_find_unique_column_unique() {
        let cols = vec![col("a", TypeCode::U64), col("b", TypeCode::I64)];
        assert_eq!(find_unique_column(&cols, "a").unwrap(), Some(0));
        assert_eq!(find_unique_column(&cols, "B").unwrap(), Some(1)); // case-insensitive
    }

    #[test]
    fn test_find_unique_column_absent() {
        let cols = vec![col("a", TypeCode::U64)];
        assert_eq!(find_unique_column(&cols, "missing").unwrap(), None);
    }

    #[test]
    fn test_find_unique_column_duplicate_is_ambiguous() {
        // Two case-insensitively equal names (as a `SELECT *` join view produces).
        let cols = vec![col("Id", TypeCode::U64), col("ID", TypeCode::U64)];
        match find_unique_column(&cols, "id") {
            Err(GnitzSqlError::Bind(s)) => assert!(s.contains("ambiguous"), "got: {s}"),
            other => panic!("expected Bind(ambiguous), got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_qualified_column_case_insensitive_alias() {
        // The AliasMap is keyed by lowercased aliases (join.rs builds it with
        // `.to_lowercase()`); under the case-preserving GenericDialect a reference
        // like `ON A.x = ...` arrives as raw "A". The probe must lowercase to hit,
        // and `col_offset` must still be added through.
        let mut map: AliasMap = HashMap::new();
        map.insert(
            "a".to_string(),
            ResolvedRelation {
                table_id: 1,
                schema: Rc::new(Schema {
                    columns: vec![col("x", TypeCode::I64), col("y", TypeCode::I64)],
                    pk_cols: vec![0],
                }),
                col_offset: 10,
            },
        );
        assert_eq!(resolve_qualified_column("A", "x", &map).unwrap(), 10);
        assert_eq!(resolve_qualified_column("A", "y", &map).unwrap(), 11);
        assert_eq!(resolve_qualified_column("a", "x", &map).unwrap(), 10); // lower still works
    }
}
