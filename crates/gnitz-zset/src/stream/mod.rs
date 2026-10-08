//! The stream operators: the kernels that take this tick's delta, open a cursor
//! over its history, and emit the output delta.
//!
//!   - `clamp`  — distinct and positive_part, the weight clamps
//!   - `join`   — the bilinear operator, equi, range and cross, over one probe
//!   - `reduce` — the incremental aggregate and its combined value index (`avi`)
//!   - `topn`   — per-group top-N over an ordered index of every input row
//!
//! These are the operators whose incremental form is not the operator itself:
//! each consults an integral. Only a circuit dispatches to them, and each builds
//! its artifact (plan, probe, output schema) through the constructor here that
//! owns the guards a client-supplied circuit clears.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod clamp;
mod join;
mod reduce;
mod topn;

use crate::repr::ReadCursor;

/// Opens a cursor over one store of an operator's history, for probing at the
/// keys in `[first, last]` — whole PKs, or the same leading bytes of one, each
/// then naming every row it prefixes — positioned on the band they span, which
/// is all it need hold. Supplied by
/// the caller, which holds the store; the operator calls it once it knows which
/// keys the delta touches, and not at all for a store it turns out not to read.
pub type OpenAt<'a> = &'a mut dyn FnMut(&[u8], &[u8]) -> ReadCursor;

pub use clamp::op_weight_clamp;
pub use join::{op_join_delta_trace, JoinPlan, JoinProbe};
pub use reduce::{op_reduce, ReducePlan};
pub use topn::{op_topn, TopNPlan};
