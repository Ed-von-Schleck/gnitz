//! The clause guards DDL statements share: the column options, the PRIMARY KEY
//! / UNIQUE / FOREIGN KEY constraints and the index column lists that CREATE
//! TABLE, CREATE INDEX and ALTER TABLE each meet. They keep the guard contract
//! `crate::validate` states.

use crate::error::{GnitzSqlError, reject_if, unsupported_clause};
use crate::rules::canonical_user_name;
use crate::types::declared_type;
use gnitz_wire::{ColumnDef, FkAction};
use sqlparser::ast::{Expr, IndexColumn, OrderByExpr, OrderByOptions, TableConstraint};

/// A key constraint, read: every field gnitz does not implement rejected, the
/// column lists still names. Either spelling reads to it — a column option over
/// its own column, a table constraint over the list it names.
pub(super) enum KeyConstraint<'a> {
    PrimaryKey(Vec<&'a str>),
    /// With its `CONSTRAINT <name>`, canonical, which names the created index.
    Unique(Vec<&'a str>, Option<String>),
    ForeignKey {
        child: &'a str,
        parent: &'a sqlparser::ast::ObjectName,
        /// The parent column named; `None` defaults to the parent's lone PK
        /// column.
        referred: Option<&'a str>,
        on_delete: FkAction,
    },
}

/// Read a table constraint. Exhaustive over `TableConstraint`: CHECK, inline
/// INDEX, FULLTEXT/SPATIAL and the `{PRIMARY KEY,UNIQUE} USING INDEX`
/// promotions (no pre-existing index at CREATE TABLE) are rejected.
pub(super) fn read_table_constraint<'a>(
    constraint: &'a TableConstraint,
    context: &str,
) -> Result<KeyConstraint<'a>, GnitzSqlError> {
    match constraint {
        TableConstraint::PrimaryKey(pk) => read_pk(pk, None, context),
        TableConstraint::Unique(u) => {
            let (cols, name) = read_unique(u, None, None, context)?;
            Ok(KeyConstraint::Unique(cols, name))
        }
        TableConstraint::ForeignKey(fk) => read_fk(fk, None, context),
        TableConstraint::Check(_) => Err(unsupported_clause(context, "CHECK constraint")),
        TableConstraint::Index(_) => Err(unsupported_clause(context, "INDEX in table definition")),
        TableConstraint::FulltextOrSpatial(_) => Err(unsupported_clause(context, "FULLTEXT/SPATIAL index")),
        TableConstraint::PrimaryKeyUsingIndex(_) => Err(unsupported_clause(context, "PRIMARY KEY USING INDEX")),
        TableConstraint::UniqueUsingIndex(_) => Err(unsupported_clause(context, "UNIQUE USING INDEX")),
        TableConstraint::Exclude(_) => Err(unsupported_clause(context, "EXCLUDE constraint")),
    }
}

/// Constraint characteristics are semantics gnitz does not implement; every
/// constraint kind that can carry them rejects them.
const CHARACTERISTICS: &str = "constraint characteristics (DEFERRABLE …)";

/// A PRIMARY KEY over `own` column, or over its own list where `own` is `None`.
/// `index_type` follows CREATE INDEX's rule: the BTree default is accepted (it
/// names gnitz's real ordered index), anything else is rejected.
fn read_pk<'a>(
    pk: &'a sqlparser::ast::PrimaryKeyConstraint,
    own: Option<&'a str>,
    context: &str,
) -> Result<KeyConstraint<'a>, GnitzSqlError> {
    let sqlparser::ast::PrimaryKeyConstraint {
        columns,
        // Inert metadata: gnitz has no PK constraint/index naming surface.
        name: _,
        index_name: _,
        // Conditionally accepted / rejected below.
        index_type,
        index_options,
        characteristics,
        include,
    } = pk;
    reject_index_type_and_options(index_type, index_options, include, context)?;
    reject_if(characteristics.is_some(), context, CHARACTERISTICS)?;
    let cols = match own {
        Some(c) => vec![c],
        None => index_column_names(columns, "PRIMARY KEY")?,
    };
    Ok(KeyConstraint::PrimaryKey(cols))
}

/// A UNIQUE's column names and canonical name: over `own` column, or over its own list where `own` is `None`. A
/// column-level one's `CONSTRAINT <name>` arrives as `option_name`. `NULLS [NOT]
/// DISTINCT` is rejected like CREATE INDEX rejects it — gnitz defines no
/// NULL-conflict semantics to honor.
pub(super) fn read_unique<'a>(
    u: &'a sqlparser::ast::UniqueConstraint,
    option_name: Option<&sqlparser::ast::Ident>,
    own: Option<&'a str>,
    context: &str,
) -> Result<(Vec<&'a str>, Option<String>), GnitzSqlError> {
    let sqlparser::ast::UniqueConstraint {
        name,
        columns,
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
        include,
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
    reject_index_type_and_options(index_type, index_options, include, context)?;
    reject_if(characteristics.is_some(), context, CHARACTERISTICS)?;
    let cols = match own {
        Some(c) => vec![c],
        None => index_column_names(columns, "UNIQUE")?,
    };
    let name = option_name.or(name.as_ref());
    Ok((cols, name.map(|n| canonical_user_name(&n.value)).transpose()?))
}

/// A FOREIGN KEY from `own` column, or from its own single-column list where
/// `own` is `None`.
fn read_fk<'a>(
    fk: &'a sqlparser::ast::ForeignKeyConstraint,
    own: Option<&'a str>,
    context: &str,
) -> Result<KeyConstraint<'a>, GnitzSqlError> {
    let sqlparser::ast::ForeignKeyConstraint {
        columns,
        foreign_table,
        referred_columns,
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
    reject_if(characteristics.is_some(), context, CHARACTERISTICS)?;
    use sqlparser::ast::ReferentialAction as A;
    let on_delete = match on_delete {
        // Both name the check a write's fold is held to.
        None | Some(A::NoAction | A::Restrict) => FkAction::Restrict,
        Some(A::Cascade) => FkAction::Cascade,
        Some(A::SetNull | A::SetDefault) => {
            return Err(unsupported_clause(
                context,
                "FOREIGN KEY ON DELETE SET NULL / SET DEFAULT",
            ));
        }
    };
    let child = match (own, columns.as_slice()) {
        (Some(c), _) => c,
        (None, [c]) => c.value.as_str(),
        (None, _) => return Err(unsupported_clause(context, "a multi-column FOREIGN KEY")),
    };
    let referred = match referred_columns.as_slice() {
        [] => None,
        [c] => Some(c.value.as_str()),
        _ => return Err(unsupported_clause(context, "a multi-column REFERENCES list")),
    };
    Ok(KeyConstraint::ForeignKey {
        child,
        parent: foreign_table,
        referred,
        on_delete,
    })
}

/// A strictly simple identifier expression's name, or the shared
/// "column must be a simple identifier" rejection. Backs [`index_column_names`]
/// and the CLUSTER BY column list (a `Vec<Expr>` in sqlparser).
pub(super) fn simple_ident_expr<'a>(e: &'a Expr, context: &str) -> Result<&'a str, GnitzSqlError> {
    match e {
        Expr::Identifier(id) => Ok(&id.value),
        _ => Err(GnitzSqlError::rejected(format!(
            "{context}: column must be a simple identifier"
        ))),
    }
}

/// The bare column names of an index column list — a PRIMARY KEY or UNIQUE
/// constraint's, or CREATE INDEX's — each of which gnitz requires to be a simple
/// identifier. Any expression, operator class, or ordering qualifier is rejected
/// — gnitz indexes are ordered ascending by key bytes and carry no per-column
/// ordering.
pub(super) fn index_column_names<'a>(columns: &'a [IndexColumn], context: &str) -> Result<Vec<&'a str>, GnitzSqlError> {
    columns
        .iter()
        .map(|c| {
            let IndexColumn {
                column:
                    OrderByExpr {
                        expr,
                        options: OrderByOptions { sort, nulls_first },
                        with_fill,
                    },
                operator_class,
            } = c;
            reject_if(
                operator_class.is_some(),
                context,
                "an operator class on an index column",
            )?;
            reject_if(
                sort.is_some() || nulls_first.is_some() || with_fill.is_some(),
                context,
                "per-column ordering (ASC/DESC, NULLS FIRST/LAST, WITH FILL)",
            )?;
            simple_ident_expr(expr, context)
        })
        .collect()
}

/// The index-shape tail every index-creating surface shares: accept only the
/// BTree default `USING` (it names gnitz's real ordered index), no index
/// options and no INCLUDE columns. CREATE INDEX, PRIMARY KEY and UNIQUE all
/// read it.
pub(super) fn reject_index_type_and_options(
    index_type: &Option<sqlparser::ast::IndexType>,
    index_options: &[sqlparser::ast::IndexOption],
    include: &[sqlparser::ast::Ident],
    context: &str,
) -> Result<(), GnitzSqlError> {
    reject_if(
        index_type
            .as_ref()
            .is_some_and(|t| !matches!(t, sqlparser::ast::IndexType::BTree)),
        context,
        "USING (non-default index type)",
    )?;
    reject_if(!index_options.is_empty(), context, "index options")?;
    reject_if(!include.is_empty(), context, "INCLUDE (covering columns)")
}

/// One column definition, read: the column, whether it was spelled SERIAL, and
/// every constraint its options declare, read, in the order written; the caller
/// decides which it honours.
pub(super) struct ColumnDecl<'a> {
    pub(super) def: ColumnDef,
    pub(super) serial: bool,
    pub(super) constraints: Vec<KeyConstraint<'a>>,
}

/// Read `col`, rejecting every column option no site honours; pure metadata
/// (COMMENT/OPTIONS/POLICY/TAGS) is accepted, and each constraint option read
/// as [`KeyConstraint`]. A SERIAL column is its signed
/// integer, NOT NULL. `context` names the site in a rejection.
pub(super) fn read_column<'a>(
    col: &'a sqlparser::ast::ColumnDef,
    context: &str,
) -> Result<ColumnDecl<'a>, GnitzSqlError> {
    use sqlparser::ast::ColumnOption as O;
    let sqlparser::ast::ColumnDef { name, data_type, options } = col;
    let own = Some(name.value.as_str());
    // `Some(true)` once NOT NULL is written, `Some(false)` once NULL is.
    let mut not_null: Option<bool> = None;
    let conflicting = || {
        GnitzSqlError::rejected(format!(
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
            O::Null | O::NotNull => {
                let written = matches!(option, O::NotNull);
                if not_null.is_some_and(|prev| prev != written) {
                    return Err(conflicting());
                }
                not_null = Some(written);
            }
            O::Unique(u) => {
                let (cols, name) = read_unique(u, constraint_name.as_ref(), own, context)?;
                constraints.push(KeyConstraint::Unique(cols, name));
            }
            O::PrimaryKey(pk) => constraints.push(read_pk(pk, own, context)?),
            O::ForeignKey(fk) => constraints.push(read_fk(fk, own, context)?),
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
            O::GenericOptions(_) => return Err(unsupported_clause(context, "foreign-table column OPTIONS")),
        }
    }
    let (ty, serial) = declared_type(data_type)?;
    if serial && not_null == Some(false) {
        return Err(conflicting());
    }
    let nullable = !serial && not_null != Some(true);
    Ok(ColumnDecl {
        def: ColumnDef::new(name.value.as_str(), ty, nullable),
        serial,
        constraints,
    })
}
