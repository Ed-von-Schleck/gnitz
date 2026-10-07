//! The clause guards DDL statements share: a `WITH (…)` option list, and the
//! column options and PRIMARY KEY / UNIQUE / FOREIGN KEY constraint fields that
//! CREATE TABLE, CREATE INDEX and ALTER TABLE each meet. They keep the guard
//! contract `crate::validate` states.

use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use gnitz_wire::sys_rows::FkAction;

/// The values of a `CREATE` statement's `WITH (key = value, …)` clause, one slot per
/// entry of `keys`. Any other option form, unknown key or repeated key is rejected.
pub(super) fn kv_options<'a, const N: usize>(
    options: &'a sqlparser::ast::CreateTableOptions,
    context: &str,
    keys: [&str; N],
) -> Result<[Option<&'a sqlparser::ast::Expr>; N], GnitzSqlError> {
    let mut slots = [None; N];
    let form = match options {
        sqlparser::ast::CreateTableOptions::With(opts) => {
            for opt in opts {
                let sqlparser::ast::SqlOption::KeyValue { key, value } = opt else {
                    return Err(GnitzSqlError::Rejected(format!(
                        "unsupported {context} option in WITH (…), which takes `key = value` entries: {opt:?}"
                    )));
                };
                let Some(at) = keys.iter().position(|k| key.value.eq_ignore_ascii_case(k)) else {
                    let listed: Vec<String> = keys.iter().map(|k| format!("`{k}`")).collect();
                    return Err(GnitzSqlError::Rejected(format!(
                        "unknown {context} option '{}'; the supported options are {}",
                        key.value,
                        listed.join(", ")
                    )));
                };
                if slots[at].replace(value).is_some() {
                    return Err(GnitzSqlError::Rejected(format!(
                        "{context} option `{}` is given more than once",
                        keys[at]
                    )));
                }
            }
            return Ok(slots);
        }
        sqlparser::ast::CreateTableOptions::None => return Ok(slots),
        sqlparser::ast::CreateTableOptions::Options(_) => "OPTIONS (…)",
        sqlparser::ast::CreateTableOptions::Plain(_) => "space-separated options",
        sqlparser::ast::CreateTableOptions::TableProperties(_) => "TBLPROPERTIES (…)",
    };
    Err(GnitzSqlError::Rejected(format!(
        "{form} is not supported; options must be given as WITH (…)"
    )))
}

/// A FOREIGN KEY's `ON DELETE` action, rejecting every field gnitz does not
/// implement. Column-level and table-level FKs both read through here.
pub(super) fn fk_on_delete(
    fk: &sqlparser::ast::ForeignKeyConstraint,
    context: &str,
) -> Result<FkAction, GnitzSqlError> {
    let sqlparser::ast::ForeignKeyConstraint {
        // Consumed by `resolve_fk_target`.
        columns: _,
        foreign_table: _,
        referred_columns: _,
        // Inert metadata: gnitz has no FK constraint/index naming surface.
        name: _,
        index_name: _,
        on_delete,
        // Rejected: unimplemented semantics.
        on_update,
        match_kind,
        characteristics,
    } = fk;
    reject_if(on_update.is_some(), context, "FOREIGN KEY ON UPDATE action")?;
    reject_if(match_kind.is_some(), context, "FOREIGN KEY MATCH")?;
    reject_constraint_characteristics(characteristics, context)?;
    use sqlparser::ast::ReferentialAction as A;
    match on_delete {
        // Both name the check a write's fold is held to.
        None | Some(A::NoAction | A::Restrict) => Ok(FkAction::Restrict),
        Some(A::Cascade) => Ok(FkAction::Cascade),
        Some(A::SetNull | A::SetDefault) => Err(unsupported_clause(
            context,
            "FOREIGN KEY ON DELETE SET NULL / SET DEFAULT",
        )),
    }
}

/// Constraint characteristics (`DEFERRABLE …`) are semantics gnitz does not
/// implement; every constraint kind that can carry them rejects them here.
fn reject_constraint_characteristics(
    characteristics: &Option<sqlparser::ast::ConstraintCharacteristics>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_if(
        characteristics.is_some(),
        context,
        "constraint characteristics (DEFERRABLE …)",
    )
}

/// Reject every PRIMARY KEY constraint field beyond the column list and the
/// inert names. `index_type` follows CREATE INDEX's rule: the BTree default is
/// accepted (it names gnitz's real ordered index), anything else is rejected.
/// Shared by the column-level and table-level guards (both wrap
/// `PrimaryKeyConstraint` since sqlparser 0.60; exhaustive destructure, no `..`).
pub(super) fn reject_unhonored_pk_fields(
    pk: &sqlparser::ast::PrimaryKeyConstraint,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::PrimaryKeyConstraint {
        // Consumed by `collect_declarations`.
        columns: _,
        // Inert metadata: gnitz has no PK constraint/index naming surface.
        name: _,
        index_name: _,
        // Conditionally accepted / rejected below.
        index_type,
        index_options,
        characteristics,
    } = pk;
    reject_index_constraint_extras(index_type, index_options, characteristics, context)
}

/// Reject every UNIQUE constraint field beyond the column list and the
/// constraint name (which names the created index). `NULLS [NOT] DISTINCT`
/// is rejected like CREATE INDEX rejects it — gnitz defines no NULL-conflict
/// semantics to honor. Shared by the column-level and table-level guards
/// (exhaustive destructure, no `..`).
pub(super) fn reject_unhonored_unique_fields(
    u: &sqlparser::ast::UniqueConstraint,
    context: &str,
) -> Result<(), GnitzSqlError> {
    let sqlparser::ast::UniqueConstraint {
        // Consumed by `collect_declarations` on both spellings (the name becomes
        // the index name); a column-level one arrives as `ColumnOptionDef.name`.
        name: _,
        columns: _,
        // Inert keyword phrasing (`UNIQUE KEY` vs `UNIQUE INDEX`).
        index_type_display: _,
        // Rejected: a separate index name would be undroppable (DROP INDEX
        // resolves the constraint-derived name).
        index_name,
        // Conditionally accepted / rejected below.
        index_type,
        index_options,
        characteristics,
        nulls_distinct,
    } = u;
    reject_if(
        index_name.is_some(),
        context,
        "a UNIQUE index name (use CONSTRAINT <name>)",
    )?;
    reject_if(
        !matches!(nulls_distinct, sqlparser::ast::NullsDistinctOption::None),
        context,
        "NULLS [NOT] DISTINCT",
    )?;
    reject_index_constraint_extras(index_type, index_options, characteristics, context)
}

/// The index-shape tail every index-creating surface shares: accept only the
/// BTree default `USING` (it names gnitz's real ordered index), and no index
/// options. CREATE INDEX, PRIMARY KEY and UNIQUE all spell the same two
/// rejections, so they read from one place and cannot drift apart.
pub(super) fn reject_index_type_and_options(
    index_type: &Option<sqlparser::ast::IndexType>,
    index_options: &[sqlparser::ast::IndexOption],
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_if(
        index_type
            .as_ref()
            .is_some_and(|t| !matches!(t, sqlparser::ast::IndexType::BTree)),
        context,
        "USING (non-default index type)",
    )?;
    reject_if(!index_options.is_empty(), context, "index options")
}

/// The PK/UNIQUE-shared tail: the index shape above, plus no constraint
/// characteristics (DEFERRABLE …), which a CREATE INDEX cannot carry.
fn reject_index_constraint_extras(
    index_type: &Option<sqlparser::ast::IndexType>,
    index_options: &[sqlparser::ast::IndexOption],
    characteristics: &Option<sqlparser::ast::ConstraintCharacteristics>,
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_index_type_and_options(index_type, index_options, context)?;
    reject_constraint_characteristics(characteristics, context)
}

/// Which site is walking a column definition, and so which constraint-bearing
/// options are actually acted on. CREATE TABLE consumes NOT NULL / PRIMARY KEY /
/// UNIQUE / REFERENCES; ADD COLUMN consumes none of them, so it must reject them
/// rather than silently drop them.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnOptionSite {
    CreateTable,
    AddColumn,
}

impl ColumnOptionSite {
    /// How the site names itself in a rejection. Derived rather than passed
    /// beside it: the two are 1:1, and a second parameter is a hole a caller can
    /// spell the wrong way round.
    fn context(self) -> &'static str {
        match self {
            ColumnOptionSite::CreateTable => "column definition",
            ColumnOptionSite::AddColumn => "ADD COLUMN",
        }
    }
}

/// Reject every column option `site` does not honor. Honored by CREATE TABLE: NULL/NOT NULL
/// (nullability), PRIMARY KEY, UNIQUE, FOREIGN KEY target — the honored constraint variants are
/// descended into (`reject_unhonored_{pk,unique}_fields`, `fk_on_delete`) so an unimplemented field inside them
/// (an `ON UPDATE` action, DEFERRABLE, NULLS NOT DISTINCT, …) is rejected too. Every
/// constraint/semantic option gnitz lacks (DEFAULT, CHECK, GENERATED, IDENTITY, ON UPDATE, COLLATE,
/// SRID, INVISIBLE, …) is rejected; pure metadata (COMMENT/OPTIONS/POLICY/TAGS) is accepted. Exhaustive
/// over the `ColumnDef` / `ColumnOptionDef` fields (no `..`) and over all 23 `ColumnOption` variants
/// (no `_`): a new field or variant stops the build.
pub(super) fn reject_unhonored_column_options(
    col: &sqlparser::ast::ColumnDef,
    site: ColumnOptionSite,
) -> Result<(), GnitzSqlError> {
    use sqlparser::ast::ColumnOption as O;
    let context = site.context();
    // Consumed at CREATE TABLE, unhonored at ADD COLUMN.
    let honored = |clause: &str| reject_if(site == ColumnOptionSite::AddColumn, context, clause);
    let sqlparser::ast::ColumnDef {
        // The name and type are the column; this guard is about its options.
        name: _,
        data_type: _,
        options,
    } = col;
    for opt in options {
        // `name` is consumed for UNIQUE (it becomes the index name, as on the
        // table-level spelling) and inert elsewhere — gnitz names no other
        // constraint.
        let sqlparser::ast::ColumnOptionDef { name: _, option } = opt;
        match option {
            // A new column over existing rows is nullable either way, so bare
            // NULL is consumed at both sites.
            O::Null => {}
            O::NotNull => honored("NOT NULL (a new column over existing rows is nullable)")?,
            // Consumed, but only the column list / constraint name — descend into
            // the wrapped constraint so an unimplemented field is rejected, not
            // silently dropped.
            O::Unique(u) => {
                honored("UNIQUE (add the column, then CREATE UNIQUE INDEX)")?;
                reject_unhonored_unique_fields(u, context)?
            }
            O::PrimaryKey(pk) => {
                honored("PRIMARY KEY (a new column cannot join the primary key)")?;
                reject_unhonored_pk_fields(pk, context)?
            }
            // Target consumed; the rest rejected.
            O::ForeignKey(fk) => {
                honored("REFERENCES (a foreign key is declared only at CREATE TABLE)")?;
                fk_on_delete(fk, context).map(drop)?
            }
            O::Comment(_) | O::Options(_) | O::Policy(_) | O::Tags(_) => {} // inert metadata
            O::Default(_) => return Err(unsupported_clause(context, "DEFAULT")),
            O::Check(_) => return Err(unsupported_clause(context, "CHECK")),
            O::Generated { .. } => return Err(unsupported_clause(context, "GENERATED")),
            O::Identity(_) => return Err(unsupported_clause(context, "IDENTITY / AUTO_INCREMENT")),
            O::OnUpdate(_) => return Err(unsupported_clause(context, "ON UPDATE")),
            O::OnConflict(_) => return Err(unsupported_clause(context, "ON CONFLICT")),
            O::Collation(_) => return Err(unsupported_clause(context, "COLLATE")),
            O::CharacterSet(_) => return Err(unsupported_clause(context, "CHARACTER SET")),
            O::Materialized(_) => return Err(unsupported_clause(context, "MATERIALIZED column")),
            O::Ephemeral(_) => return Err(unsupported_clause(context, "EPHEMERAL column")),
            O::Alias(_) => return Err(unsupported_clause(context, "ALIAS column")),
            O::Srid(_) => return Err(unsupported_clause(context, "SRID")),
            O::Invisible => return Err(unsupported_clause(context, "INVISIBLE column")),
            O::DialectSpecific(_) => return Err(unsupported_clause(context, "dialect-specific column option")),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/guard.rs"]
mod tests;
