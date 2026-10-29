//! The clause guards DDL statements share: the column options and PRIMARY KEY /
//! UNIQUE / FOREIGN KEY constraint fields that CREATE TABLE, CREATE INDEX and
//! ALTER TABLE each meet. They keep the guard contract `crate::validate` states.

use crate::error::{reject_if, unsupported_clause, GnitzSqlError};
use crate::types::declared_type;
use gnitz_wire::{ColumnDef, FkAction};

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
/// semantics to honor.
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
/// options. CREATE INDEX, PRIMARY KEY and UNIQUE all read it.
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

/// Which site is reading a column definition, and so which constraint-bearing
/// options are acted on: CREATE TABLE consumes NOT NULL / PRIMARY KEY / UNIQUE /
/// REFERENCES, ADD COLUMN none of them.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnOptionSite {
    CreateTable,
    AddColumn,
}

impl ColumnOptionSite {
    /// How the site names itself in a rejection.
    fn context(self) -> &'static str {
        match self {
            ColumnOptionSite::CreateTable => "column definition",
            ColumnOptionSite::AddColumn => "ADD COLUMN",
        }
    }
}

/// A constraint one column option declares on its own column.
pub(super) enum ColumnConstraint<'a> {
    PrimaryKey,
    /// The option's `CONSTRAINT <name>`, which names the created index.
    Unique(Option<&'a sqlparser::ast::Ident>),
    ForeignKey {
        fk: &'a sqlparser::ast::ForeignKeyConstraint,
        on_delete: FkAction,
    },
}

/// One column definition, read: the column, whether it was spelled SERIAL, and
/// the constraints its options declare, in the order written. Only
/// [`ColumnOptionSite::CreateTable`] yields a constraint.
pub(super) struct ColumnDecl<'a> {
    pub(super) def: ColumnDef,
    pub(super) serial: bool,
    pub(super) constraints: Vec<ColumnConstraint<'a>>,
}

/// Read `col`, rejecting every column option `site` does not honor; pure
/// metadata (COMMENT/OPTIONS/POLICY/TAGS) is accepted. A constraint option is
/// descended into, so a field gnitz does not implement (an `ON UPDATE` action,
/// DEFERRABLE, NULLS NOT DISTINCT, …) is rejected with it. A SERIAL column is its
/// signed integer, NOT NULL.
pub(super) fn read_column(
    col: &sqlparser::ast::ColumnDef,
    site: ColumnOptionSite,
) -> Result<ColumnDecl<'_>, GnitzSqlError> {
    use sqlparser::ast::ColumnOption as O;
    let context = site.context();
    let honored = |clause: &str| reject_if(site == ColumnOptionSite::AddColumn, context, clause);
    let sqlparser::ast::ColumnDef { name, data_type, options } = col;
    // `Some(true)` once NOT NULL is written, `Some(false)` once NULL is.
    let mut not_null: Option<bool> = None;
    let conflicting = || {
        GnitzSqlError::Rejected(format!(
            "{context}: conflicting NULL/NOT NULL declarations for column '{}'",
            name.value
        ))
    };
    let mut constraints = Vec::new();
    for opt in options {
        // `name` is read for UNIQUE and inert elsewhere: gnitz names no other
        // column constraint.
        let sqlparser::ast::ColumnOptionDef { name: constraint_name, option } = opt;
        match option {
            // A new column over existing rows is nullable either way, so bare
            // NULL is honored at both sites.
            O::Null | O::NotNull => {
                let written = matches!(option, O::NotNull);
                if written {
                    honored("NOT NULL (a new column over existing rows is nullable)")?;
                }
                if not_null.is_some_and(|prev| prev != written) {
                    return Err(conflicting());
                }
                not_null = Some(written);
            }
            O::Unique(u) => {
                honored("UNIQUE (add the column, then CREATE UNIQUE INDEX)")?;
                reject_unhonored_unique_fields(u, context)?;
                constraints.push(ColumnConstraint::Unique(constraint_name.as_ref()));
            }
            O::PrimaryKey(pk) => {
                honored("PRIMARY KEY (a new column cannot join the primary key)")?;
                reject_unhonored_pk_fields(pk, context)?;
                constraints.push(ColumnConstraint::PrimaryKey);
            }
            O::ForeignKey(fk) => {
                honored("REFERENCES (a foreign key is declared only at CREATE TABLE)")?;
                let on_delete = fk_on_delete(fk, context)?;
                constraints.push(ColumnConstraint::ForeignKey { fk, on_delete });
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
    let (ty, serial) = declared_type(data_type)?;
    if serial && not_null == Some(false) {
        return Err(conflicting());
    }
    let nullable = !serial && not_null != Some(true);
    Ok(ColumnDecl {
        def: ColumnDef::new(name.value.clone(), ty, nullable),
        serial,
        constraints,
    })
}
