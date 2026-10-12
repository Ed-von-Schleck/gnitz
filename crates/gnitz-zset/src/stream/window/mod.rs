//! Incremental window operator: every row of a group, widened by its running
//! values — aggregates and ranks over the rows of the group at or before it in
//! ORDER BY order. It is maintained through the order index of every
//! input row (the integral, `I` in `Q^Δ = D ∘ Q ∘ I`): a touched group is walked
//! once in order, under its old and its new contents at the same time, and each
//! row whose values differ between the two is retracted and re-emitted.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod op_window;
mod plan;

#[cfg(test)]
#[path = "benches/window.rs"]
mod bench;

#[cfg(test)]
#[path = "tests/window.rs"]
mod tests;

pub use op_window::op_window;
pub use plan::WindowPlan;
