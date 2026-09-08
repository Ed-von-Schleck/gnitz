//! Incremental top-N operator: per group, the rows filling weight slots
//! `offset .. offset + limit` of the group in ORDER BY order. The reduce's
//! MIN/MAX are its top-1: like them it keeps an ordered index of **every** input
//! row (the integral, `I` in `Q^Δ = D ∘ Q ∘ I`), so retracting the leader is a
//! prefix walk of the index, never a search the operator cannot make.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod index;
mod op_topn;
mod plan;

#[cfg(test)]
#[path = "tests/topn.rs"]
mod tests;

pub use index::op_populate_topn;
pub use op_topn::op_topn;
pub use plan::TopNPlan;
