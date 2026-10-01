//! The four DML verbs (INSERT, SELECT, UPDATE, DELETE) and the EXPLAIN of a
//! SELECT, each as a plan over the statement's catalog and the execution of that
//! plan against a client. No `dml` module reaches a view *emitter*.
//!
//! Transactions need no special casing in the verbs: the client buffers every
//! write while one is open, and a read-modify-write reads through that buffer.

mod explain;
mod insert;
mod mutate;
mod plan;
mod select;

pub(crate) use explain::execute_explain;
#[cfg(test)]
pub(crate) use explain::explain_lines;
pub(crate) use insert::{execute_insert, plan_insert};
pub(crate) use mutate::{execute_mutation, plan_delete, plan_update};
#[cfg(test)]
pub(crate) use select::ReadPlan;
pub(crate) use select::{execute_select, plan_read};
