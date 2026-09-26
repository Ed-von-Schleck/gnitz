use std::fmt;

#[derive(Debug)]
pub enum GnitzSqlError {
    Parse(sqlparser::parser::ParserError),
    /// The statement disagrees with a **catalog relation**: a column name that
    /// does not resolve, a value count against the schema, a nullability. A
    /// relation that does not resolve is a `NotFound` refusal.
    Bind(String),
    /// Two parts of the **statement** disagree, or a literal is out of range —
    /// no catalog decides it. A set-op column-count mismatch is `Plan`; an arity
    /// against a table's schema is `Bind`.
    Plan(String),
    Exec(gnitz_core::ClientError),
    /// Well-formed and schema-consistent; gnitz does not implement it.
    Unsupported(String),
    /// A planner invariant broke: a shape an earlier guard was supposed to have
    /// rejected reached a stage that cannot represent it. Never raisable by any
    /// SQL a user can write, which is what makes it worth its own variant — a
    /// `Plan` string prefix cannot be asserted on.
    Internal(String),
    /// A planning pass asked for a relation the statement's catalog snapshot does
    /// not hold. A control signal for `dispatch::plan_resolving`, which resolves
    /// the name and re-runs the pass; it never reaches a caller of
    /// `SqlPlanner::execute`.
    CatalogMiss(String),
}

impl fmt::Display for GnitzSqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GnitzSqlError::Parse(e) => write!(f, "parse error: {e}"),
            GnitzSqlError::Bind(s) => write!(f, "bind error: {s}"),
            GnitzSqlError::Plan(s) => write!(f, "plan error: {s}"),
            GnitzSqlError::Exec(e) => write!(f, "exec error: {e}"),
            GnitzSqlError::Unsupported(s) => write!(f, "unsupported: {s}"),
            GnitzSqlError::Internal(s) => write!(f, "internal error: {s}"),
            GnitzSqlError::CatalogMiss(name) => {
                write!(f, "internal error: relation '{name}' was not resolved before planning")
            }
        }
    }
}

impl std::error::Error for GnitzSqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // The two wrapped-error variants expose their cause (sqlparser's
        // `ParserError` gained a std `Error` impl in 0.61); the string/marker
        // variants have none.
        match self {
            GnitzSqlError::Parse(e) => Some(e),
            GnitzSqlError::Exec(e) => Some(e),
            GnitzSqlError::Bind(_)
            | GnitzSqlError::Plan(_)
            | GnitzSqlError::Unsupported(_)
            | GnitzSqlError::Internal(_)
            | GnitzSqlError::CatalogMiss(_) => None,
        }
    }
}

impl From<gnitz_core::ClientError> for GnitzSqlError {
    fn from(e: gnitz_core::ClientError) -> Self {
        GnitzSqlError::Exec(e)
    }
}

impl From<sqlparser::parser::ParserError> for GnitzSqlError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        GnitzSqlError::Parse(e)
    }
}

/// A shared-evaluator rejection as a SQL-layer `Unsupported`. The wording is
/// `ExprValidateErr`'s own `Display`, so a query rejected here and the same
/// program rejected by the engine's circuit compiler read identically.
impl From<gnitz_expr::ExprValidateErr> for GnitzSqlError {
    fn from(e: gnitz_expr::ExprValidateErr) -> Self {
        GnitzSqlError::Unsupported(e.to_string())
    }
}

/// The `"{context}: {clause} is not supported"` spelling, shared by every
/// statement guard. `ast_util::unsupported_on` is the inverted template, for a
/// qualifier on a call rather than a clause on a statement.
pub(crate) fn unsupported_clause(context: &str, clause: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{context}: {clause} is not supported"))
}

/// An ad-hoc SELECT reads one relation; this query derives a new one, which a view
/// maintains. `construct` names what was detected.
pub(crate) fn derivation(construct: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!(
        "ad-hoc SELECT reads a single relation; this query derives a new one ({construct}).\n\
         CREATE VIEW <name> AS <your query> — the engine maintains it incrementally — then SELECT from it."
    ))
}

/// A relation miss read from the statement's snapshot: the snapshot-side twin of
/// `GnitzClient::resolve_relation`'s miss, so both report the same `ClientError`.
pub(crate) fn missing_relation(schema: &str, name: &str) -> GnitzSqlError {
    GnitzSqlError::Exec(gnitz_core::not_found("relation", schema, name))
}

/// [`unsupported_clause`] when `present`. A run of these reads as the table of
/// clauses a statement does not honor, short-circuiting on the first present one.
pub(crate) fn reject_if(present: bool, context: &str, clause: &str) -> Result<(), GnitzSqlError> {
    if present {
        return Err(unsupported_clause(context, clause));
    }
    Ok(())
}
