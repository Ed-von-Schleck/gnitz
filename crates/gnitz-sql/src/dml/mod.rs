//! The four DML verbs (INSERT, SELECT, UPDATE, DELETE) and the EXPLAIN of a
//! SELECT, each as a plan over the statement's catalog and the execution of that
//! plan against a client. No `dml` module reaches a view *emitter*.
//!
//! Transactions need no special casing in the verbs: the client buffers every
//! write while one is open, and a read-modify-write reads through that buffer.

mod colwrite;
mod explain;
mod insert;
mod mutate;
mod select;
mod set;
mod write;

#[cfg(test)]
pub(crate) use explain::explain_lines;
pub(crate) use explain::{execute_explain, explained_query};
pub(crate) use select::{ReadPlan, execute_select, plan_read};
pub(crate) use write::{execute_write, plan_write};
