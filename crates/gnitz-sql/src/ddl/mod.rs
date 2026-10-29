//! DDL: CREATE SCHEMA (`schema`), CREATE / DROP TABLE and CREATE INDEX
//! (`table`), ALTER TABLE (`alter`), CREATE / ALTER VIEW (`view`), whose body
//! `crate::hir` compiles, and the definition each records (`view_def`); the
//! `WITH (…)` codec, read and written (`options`); and the catalog written back
//! out as the statements that create it (`render`).
//! `ddl` never references `dml`, nor `dml` it.

mod alter;
mod guard;
pub(crate) mod options;
pub(crate) mod render;
mod schema;
mod table;
mod view;
pub(crate) mod view_def;

pub(crate) use alter::execute_alter_table;
pub(crate) use schema::execute_create_schema;
pub(crate) use table::{create_planned_table, execute_create_index, execute_drop, plan_create_table};
pub(crate) use view::{plan_alter_view, plan_create_view};
