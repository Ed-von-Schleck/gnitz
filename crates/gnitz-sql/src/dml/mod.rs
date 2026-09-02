//! The execute side: the four DML verbs (INSERT, SELECT, UPDATE, DELETE). UPDATE
//! and DELETE share `mutate`; INSERT and SELECT get their own module. Each verb
//! issues `GnitzClient` RPCs and reshapes the reply client-side through the
//! `exec`/`codec` layers. `explain` is SELECT's other tail: it runs `select`'s
//! shared route and shape builders and formats the access decisions instead of
//! dispatching them. `dml` is a peer of `ddl/` (the DDL side), and drives the
//! shared query core the same way it does: a grouped / global-aggregate body
//! goes through `hir::bind_and_lower_fold` — the binder a grouped `CREATE VIEW`
//! body uses, lowered to the fold sink instead of to a circuit — so one written
//! statement means one thing on both surfaces. `dml::group_by` turns the layout
//! that comes back into the executable fold shape and owns the `SELECT DISTINCT`
//! resolver; pass-through-CTE inlining reuses `bind::cte_passthrough`. What `dml`
//! never reaches for is a view *emitter*: it consumes the shared validation
//! leaves (`crate::validate`) and access-path recognizers (`crate::access`), and
//! from `hir` only that one entry point.
//!
//! Transactions need no special casing here. The client buffers every write while
//! one is open, so the verbs just write; and they resolve UPDATE/DELETE/ON
//! CONFLICT against a read-your-own-writes view of that buffer (the `overlay`
//! module) which is empty — the identity — in autocommit.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod explain;
pub(crate) mod group_by;
mod insert;
mod mutate;
mod overlay;
pub(crate) mod plan;
mod rmw;
mod select;

pub(crate) use explain::execute_explain;
pub use explain::explain_lines;
pub(crate) use insert::execute_insert;
pub(crate) use mutate::{execute_delete, execute_update};
pub(crate) use select::execute_select;
pub use select::{plan_read, ReadKind, ReadPlan};
