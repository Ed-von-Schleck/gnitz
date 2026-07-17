//! The compile side: DDL (`ddl`) and CREATE VIEW circuit compilation (`view`),
//! over the shared validation helpers in `validate`. `dispatch.rs` is the only
//! module that reaches both `plan` and `dml`; `plan` itself never references
//! `dml`.

mod ddl;
pub(crate) mod index_bound;
pub(crate) mod lp;
pub(crate) mod validate;
mod view;

pub(crate) use ddl::{execute_create_index, execute_create_table, execute_drop};
pub(crate) use view::{compile_query_to_circuit, execute_create_view};
