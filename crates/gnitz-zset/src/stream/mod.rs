//! The stream operators: the kernels that take this tick's delta, open a cursor
//! over its history, and emit the output delta.
//!
//!   - `clamp`   — distinct and positive_part, the weight clamps
//!   - `join`    — the bilinear operator, equi, range and cross, over one probe
//!   - `reduce`  — the incremental aggregate and its combined value index
//!   - `ordered` — the index of every input row in ORDER BY order, and the top-N
//!     and the window maintained through it
//!
//! These are the operators whose incremental form is not the operator itself:
//! each consults an integral. Only a circuit dispatches to them. The join, the
//! reduce and the ordered operators run under a plan whose constructor here
//! refuses a shape a client-supplied circuit can name and the operator cannot
//! execute.

mod clamp;
mod join;
mod ordered;
mod reduce;

use crate::repr::ReadCursor;

/// Opens a cursor over one of an operator's histories, positioned on the band
/// of keys in `[first, last]`, which is all it need hold. A key is a whole PK,
/// or the same leading bytes of one, naming every row it prefixes. The caller
/// supplies it; the operator calls it once it knows which keys the delta
/// touches, and not at all for a history it turns out not to read.
pub type OpenAt<'a> = &'a mut dyn FnMut(&[u8], &[u8]) -> ReadCursor;

/// A history of an operator's input, tagged with whether it holds this epoch's
/// delta.
pub enum History<'a> {
    /// `z⁻¹(I(X))`: the integral before this epoch's delta.
    Before(OpenAt<'a>),
    /// `I(X)`: the integral with this epoch's delta absorbed.
    Through(OpenAt<'a>),
}

pub use clamp::op_weight_clamp;
pub use join::{JoinPlan, op_join_delta_trace};
pub use ordered::{OrderedEpoch, OrderedPlan};
pub use reduce::{ReducePlan, op_reduce};
