//! DDL execution: CREATE / DROP TABLE and CREATE INDEX (`table`), and ALTER TABLE
//! (`alter`), over the shared validation leaves in `crate::validate`. CREATE /
//! ALTER VIEW circuit compilation lives in `crate::hir`; the statement dispatcher
//! (`crate::dispatch`) is the only module that reaches both `ddl` and `dml`, and
//! `ddl` itself never references `dml`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod alter;
mod table;

pub(crate) use alter::execute_alter_table;
pub(crate) use table::{execute_create_index, execute_create_table, execute_drop};
