//! The compile side: DDL (`ddl`) and CREATE VIEW circuit compilation (`view`),
//! over the shared validation helpers in `validate`. `dispatch.rs` is the only
//! module that reaches both `plan` and `dml`; `plan` itself never references
//! `dml`.

mod alter;
mod ddl;
pub(crate) mod index_bound;
pub(crate) mod lp;
pub(crate) mod validate;
mod view;

pub(crate) use alter::execute_alter_table;
pub(crate) use ddl::{execute_create_index, execute_create_table, execute_drop};
pub(crate) use view::{
    analyze_group_by, bind_having_expr, cte_passthrough, execute_alter_view, execute_create_view, has_scalar_subquery,
    resolve_set_projection, HavingCtx,
};
