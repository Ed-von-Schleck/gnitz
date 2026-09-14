//! The client-side read path: the aggregate-fold finisher (`agg_finish::FoldFinish`) and the
//! batch reshaping (`residual`, `batch`, and the ORDER BY / OFFSET / LIMIT sink
//! `order`) that `dml` drives after a seek/scan reply. Sinks only into the shared
//! lower layers (`bind`, `codec`, `agg`, `expr_lower`); holds no edge back up into
//! `dml` or the `hir` view compiler.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub(crate) mod agg_finish;
pub(crate) mod batch;
pub(crate) mod order;
pub(crate) mod residual;
