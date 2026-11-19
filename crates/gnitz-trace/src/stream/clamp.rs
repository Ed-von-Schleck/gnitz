//! The weight clamps: distinct (`[0, 1]`) and positive_part (`[0, ∞)`).

use std::borrow::Cow;

use gnitz_wire::ClampKind;

use crate::stream::History;
use gnitz_zset::repr::{Batch, materialize_carrying, with_picks};

/// Per consolidated (PK, payload) of `delta`, emits `clamp(w_old + Δw, 0, cap) −
/// clamp(w_old, 0, cap)` against its weight `w_old` in `hist` — the lift of a
/// per-element weight clamp to its delta. When every row emits its own weight,
/// the input is the output, moved when it is owned.
pub fn op_weight_clamp(delta: Cow<'_, Batch>, hist: History<'_>, kind: ClampKind) -> Batch {
    debug_assert!(delta.is_consolidated());
    if delta.is_empty() {
        return Batch::empty_with_schema(delta.schema());
    }
    let (open, hist_holds_delta) = match hist {
        History::Before(open) => (open, false),
        History::Through(open) => (open, true),
    };
    let cursor = &mut open(delta.get_pk_bytes(0), delta.get_pk_bytes(delta.len() - 1));
    let cap = clamp_cap(kind);
    let mb = delta.as_mem_batch();
    let clamped = with_picks(|rows| {
        let mut identity = true;
        cursor.for_each_mem_row_weight(&mb, |i, w_hist| {
            let dw = mb.get_weight(i);
            let w_old = if hist_holds_delta {
                w_hist.wrapping_sub(dw)
            } else {
                w_hist
            };
            let out_w = w_old.wrapping_add(dw).clamp(0, cap) - w_old.clamp(0, cap);
            identity &= out_w == dw;
            if out_w != 0 {
                rows.push((0, i as u32, out_w));
            }
        });
        (!identity).then(|| {
            let mut out = materialize_carrying(std::slice::from_ref(&mb), delta.schema(), rows);
            // One row per transitioning element, in delta order: (PK, payload)-sorted, no ghosts.
            out.certify_consolidated();
            out
        })
    });
    clamped.unwrap_or_else(|| delta.into_owned())
}

/// `kind`'s upper bound; every clamp's floor is 0.
pub(crate) fn clamp_cap(kind: ClampKind) -> i64 {
    match kind {
        ClampKind::Distinct => 1,
        ClampKind::PositivePart => i64::MAX,
    }
}

#[cfg(test)]
#[path = "tests/clamp.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/clamp.rs"]
mod bench;
