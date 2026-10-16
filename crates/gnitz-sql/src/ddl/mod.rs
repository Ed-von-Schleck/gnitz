//! DDL: CREATE SCHEMA (`schema`), CREATE / DROP TABLE and CREATE INDEX
//! (`table`), ALTER TABLE (`alter`), and CREATE / ALTER VIEW (`view`), whose
//! body `crate::hir` compiles; a view's recorded definition (`definition`), and
//! the catalog written back out as the statements that create it (`render`).
//! `ddl` never references `dml`, nor `dml` it.

mod alter;
mod definition;
mod guard;
pub(crate) mod render;
mod schema;
mod table;
mod view;

pub(crate) use alter::execute_alter_table;
pub(crate) use schema::execute_create_schema;
#[cfg(test)]
pub(crate) use table::TablePlan;
pub(crate) use table::{
    execute_create_index, execute_create_table, execute_drop, plan_create_table, table_with_options, TABLE_OPTIONS,
};
#[cfg(test)]
pub(crate) use view::PlannedChain;
pub(crate) use view::{execute_view_chain, plan_alter_view, plan_create_view, view_with_options, VIEW_OPTIONS};
