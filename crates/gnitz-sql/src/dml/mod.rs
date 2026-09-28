//! The execute side of the four DML verbs (INSERT, SELECT, UPDATE, DELETE), and
//! the EXPLAIN of a SELECT. No `dml` module reaches a view *emitter*.
//!
//! Transactions need no special casing in the verbs: the client buffers every
//! write while one is open, and a read-modify-write reads through that buffer.

mod cte;
mod explain;
mod insert;
mod mutate;
mod plan;
mod select;

pub(crate) use explain::execute_explain;
#[cfg(test)]
pub(crate) use explain::explain_lines;
pub(crate) use insert::execute_insert;
#[cfg(test)]
pub(crate) use insert::PkPlan;
pub(crate) use mutate::{execute_delete, execute_update};
#[cfg(test)]
pub(crate) use select::ReadPlan;
pub(crate) use select::{execute_select, plan_read};

/// An ad-hoc SELECT reads one relation; this query derives a new one, which a view
/// maintains. `construct` names what was detected.
fn derivation(construct: &str) -> crate::error::GnitzSqlError {
    crate::error::GnitzSqlError::Rejected(format!(
        "ad-hoc SELECT reads a single relation; this query derives a new one ({construct}).\n\
         CREATE VIEW <name> AS <your query> — the engine maintains it incrementally — then SELECT from it."
    ))
}
