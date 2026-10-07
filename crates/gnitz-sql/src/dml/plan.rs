//! WHERE → access path for every direct-read verb ([`access_path`], over the
//! candidates [`crate::access`] recognizes).

use crate::access::{candidates, residual, Candidate};
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_wire_conjuncts;
use crate::ir::BoundExpr;
use gnitz_core::Schema;
use gnitz_wire::ReadBound;
use gnitz_wire::RelIndex;
use sqlparser::ast::Expr;

// ---------------------------------------------------------------------------
// The access-path ladder
// ---------------------------------------------------------------------------

/// A single-table WHERE (or its absence) as a pushed-down bound and the compiled
/// predicate of the conjuncts it does not apply. `alias` is what a qualifier must name.
pub(super) fn access_path(
    schema: &Schema,
    alias: &str,
    selection: Option<&Expr>,
    indexes: &[RelIndex],
) -> Result<(ReadBound, Vec<u8>), GnitzSqlError> {
    let conjuncts = match selection {
        Some(we) => crate::hir::bind_single_table(we, schema, alias)?.conjuncts(),
        None => Vec::new(),
    };
    bound_and_predicate(schema, &conjuncts, indexes)
}

/// Bind-once WHERE → the access path that serves it: the first of [`candidates`]
/// whose residual compiles, else an unbounded scan under the whole WHERE.
/// `indexes` is the relation's declared secondary-index list.
pub(super) fn bound_and_predicate(
    schema: &Schema,
    conjuncts: &[BoundExpr],
    indexes: &[RelIndex],
) -> Result<(ReadBound, Vec<u8>), GnitzSqlError> {
    let mut blocked = None;
    for Candidate { bound, consumed } in candidates(conjuncts, schema, indexes) {
        let residual = residual(conjuncts, &consumed);
        match compile_wire_conjuncts(residual.iter().copied(), &schema.columns) {
            Ok(predicate) => return Ok((bound, predicate)),
            // A conjunct the VM cannot carry; a later candidate may consume it.
            Err(e @ GnitzSqlError::Rejected(_)) => {
                blocked.get_or_insert(e);
            }
            Err(e) => return Err(e),
        }
    }
    // A candidate's residual is part of the whole WHERE, which then fails too.
    if let Some(e) = blocked {
        return Err(e);
    }
    Ok((ReadBound::None, compile_wire_conjuncts(conjuncts, &schema.columns)?))
}

/// Whether a worker reading under `bound` yields its rows in ascending PK order:
/// every walk but a secondary index's.
pub(super) fn walks_in_pk_order(bound: &ReadBound, pk_cols: &[u32]) -> bool {
    match bound {
        ReadBound::None | ReadBound::PkSet(_) => true,
        ReadBound::Range(r) => r.walks_pk(pk_cols),
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/plan.rs"]
mod tests;
