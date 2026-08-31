//! The execute side: the four DML verbs (INSERT, SELECT, UPDATE, DELETE). UPDATE
//! and DELETE share `mutate`; INSERT and SELECT get their own module. Each verb
//! issues `GnitzClient` RPCs and reshapes the reply client-side through the
//! `exec`/`codec` layers. `explain` is SELECT's other tail: it runs `select`'s
//! shared route and shape builders and formats the access decisions instead of
//! dispatching them. `dml` is a peer of `ddl/` (the DDL side): it consumes
//! only the shared validation leaves (`crate::validate`) and the access-path
//! recognizers (`crate::access`); its own single-relation aggregate / DISTINCT
//! analysis for the fold path lives in `dml::group_by`, and pass-through-CTE
//! inlining reuses `bind::cte_passthrough` — never a view emitter directly.
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
pub(crate) use insert::execute_insert;
pub(crate) use mutate::{execute_delete, execute_update};
pub(crate) use select::execute_select;
pub use select::{plan_read, ReadKind, ReadPlan};
