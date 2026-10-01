//! DDL: CREATE / DROP TABLE and CREATE INDEX (`table`), ALTER TABLE (`alter`),
//! and CREATE / ALTER VIEW (`view`), whose body `crate::hir` compiles. `ddl`
//! never references `dml`, nor `dml` it.

mod alter;
mod guard;
mod table;
mod view;

pub(crate) use alter::execute_alter_table;
#[cfg(test)]
pub(crate) use table::TablePlan;
pub(crate) use table::{execute_create_index, execute_create_table, execute_drop, plan_create_table};
#[cfg(test)]
pub(crate) use view::PlannedChain;
pub(crate) use view::{execute_view_chain, plan_alter_view, plan_create_view};
