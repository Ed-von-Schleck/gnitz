//! The client-side read path: the aggregate-fold finisher (`agg_finish::FoldFinish`) and the
//! batch reshaping (`client_map`, and the ORDER BY / OFFSET / LIMIT sink
//! `order`) that `dml` drives after a seek/scan reply. Sinks only into the shared
//! lower layers (`bind`, `codec`, `agg`, `expr_lower`); holds no edge back up into
//! `dml` or the `hir` view compiler.

pub(crate) mod agg_finish;
pub(crate) mod client_map;
pub(crate) mod order;
