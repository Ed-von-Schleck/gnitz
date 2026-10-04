//! The weight clamps: distinct (`[0, 1]`) and positive_part (`[0, ∞)`).

use gnitz_wire::ClampKind;

use crate::repr::{materialize_carrying, Batch, ReadCursor};

/// The weight a clamp caps at; every clamp's floor is 0.
fn cap(kind: ClampKind) -> i64 {
    match kind {
        ClampKind::Distinct => 1,
        ClampKind::PositivePart => i64::MAX,
    }
}

/// Per consolidated (PK, payload) of `delta`, emits `clamp(w_old + Δw, 0, cap) −
/// clamp(w_old, 0, cap)` against its weight `w_old` in `cursor` — the lift of a
/// per-element weight clamp to its delta.
pub fn op_weight_clamp(delta: &Batch, cursor: &mut ReadCursor, kind: ClampKind) -> Batch {
    let cap = cap(kind);
    debug_assert!(delta.is_consolidated());
    let mb = delta.as_mem_batch();
    let mut rows: Vec<(u32, u32, i64)> = Vec::new();
    // Every row emitting its own weight — an insert-only tick — is the delta itself.
    let mut identity = true;
    cursor.for_each_mem_row_weight(&mb, |i, w_old| {
        let dw = mb.get_weight(i);
        let out_w = w_old.wrapping_add(dw).clamp(0, cap) - w_old.clamp(0, cap);
        identity &= out_w == dw;
        if out_w != 0 {
            rows.push((0, i as u32, out_w));
        }
    });
    if identity {
        return delta.clone();
    }
    let mut out = materialize_carrying(std::slice::from_ref(&mb), delta.schema(), &rows);
    // One row per transitioning element, in delta order: (PK, payload)-sorted, no ghosts.
    out.certify_consolidated();
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/clamp.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/clamp.rs"]
mod bench;
