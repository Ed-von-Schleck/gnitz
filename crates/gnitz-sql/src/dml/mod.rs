//! The execute side: the four DML verbs (INSERT, SELECT, UPDATE, DELETE) over the
//! shared WHERE access-path planning in `plan`. UPDATE and DELETE share `mutate`;
//! INSERT and SELECT get their own module. Each verb issues `GnitzClient`
//! RPCs and reshapes the reply client-side through the `exec`/`codec` layers.
//! `dml` is a peer of `plan/` (the compile side) and never references it;
//! `dispatch.rs` is the only module that reaches both.
//!
//! Transactions need no special casing here. The client buffers every write while
//! one is open, so the verbs just write; and they resolve UPDATE/DELETE/ON
//! CONFLICT against a read-your-own-writes view of that buffer (the `overlay`
//! module) which is empty — the identity — in autocommit.

mod insert;
mod mutate;
mod overlay;
mod plan;
mod rmw;
mod select;

pub(crate) use insert::execute_insert;
pub(crate) use mutate::{execute_delete, execute_update};
pub(crate) use select::execute_select;
