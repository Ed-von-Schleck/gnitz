//! Shared range-probe key arithmetic.
//!
//! `increment_key_in_place` is the fixed-width byte successor used by both the
//! single-table secondary-index range scan (`catalog/store.rs`,
//! `seek_by_index_range`) and the range-join probe (`ops/join.rs`).
//!
//! `range_cut_points` derives the half-open `[start, end)` byte interval one
//! range-join term walks over the other side's reindex trace, implementing the
//! cut-point table for each range relation. It drives off the delta row's
//! raw OPK PK bytes (eq prefix + range slot) rather than the index scan's
//! native-`u128` `Cut` values, so the two derivations are not shared — only the
//! byte successor is.

use gnitz_wire::{RangeRel, MAX_PK_BYTES};

/// Fixed-width byte-string successor: `p + 1` with carry, in place. Returns
/// `false` when `p` is all-`0xFF` (or empty) — no successor exists at this width
/// (carry-out), which a caller reads as `+∞` (scan to the table end, or a
/// provably-empty start).
pub(crate) fn increment_key_in_place(p: &mut [u8]) -> bool {
    for b in p.iter_mut().rev() {
        *b = b.wrapping_add(1);
        if *b != 0 {
            return true;
        }
    }
    false
}

/// A `stride`-byte OPK cut key over the trace PK space. Backed by stack storage
/// (`MAX_PK_BYTES` bounds every trace PK stride); read the live key via
/// [`Self::as_slice`].
#[derive(Clone, Copy)]
pub(crate) struct CutKey {
    buf: [u8; MAX_PK_BYTES],
    len: usize,
}

impl CutKey {
    fn zeroed(len: usize) -> Self {
        debug_assert!(len <= MAX_PK_BYTES);
        CutKey {
            buf: [0u8; MAX_PK_BYTES],
            len,
        }
    }

    /// The `stride`-byte key.
    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// Half-open `[start, end)` cut points over the trace PK space for one delta
/// row's range probe, per the §3 table. `eq` is the equality-prefix OPK bytes
/// (`n_eq` slots, possibly empty) and `d` is the range-slot OPK bytes; together
/// they are the delta row's own PK region `p = eq ‖ d`, so the bounds need no
/// decode/re-encode.
///
/// Returns `None` when the interval is provably empty (the row matches nothing).
/// On `Some((start, end))`, `start` is always a full `stride`-byte key; `end ==
/// None` means "scan to the table end". The cut table (`rel` is the relation a
/// trace slot must satisfy versus `d`):
///
/// | rel  | start                | end                                       |
/// |------|----------------------|-------------------------------------------|
/// | `Gt` | `succ(eq‖d)`; carry⇒∅ | `succ(eq)` zero-padded; no eq / carry ⇒ end |
/// | `Ge` | `eq‖d`               | same as `Gt`                              |
/// | `Lt` | `eq ‖ 0x00*slot`     | `Some(eq‖d)`                              |
/// | `Le` | `eq ‖ 0x00*slot`     | `succ(eq‖d)`; carry ⇒ end                  |
pub(crate) fn range_cut_points(eq: &[u8], d: &[u8], rel: RangeRel) -> Option<(CutKey, Option<CutKey>)> {
    let eq_size = eq.len();
    let slot_size = d.len();
    let stride = eq_size + slot_size;
    debug_assert!(stride <= MAX_PK_BYTES && slot_size > 0);

    // p = eq ‖ d — the delta row's own PK region.
    let mut p = CutKey::zeroed(stride);
    p.buf[..eq_size].copy_from_slice(eq);
    p.buf[eq_size..stride].copy_from_slice(d);

    // eq ‖ 0x00*slot — the lowest key in this eq group (the Lt/Le start). The
    // slot region is already zero from `zeroed`.
    let mut eq_low = CutKey::zeroed(stride);
    eq_low.buf[..eq_size].copy_from_slice(eq);

    // succ(eq) zero-padded — the first key of the NEXT eq group (the Gt/Ge end).
    // `None` (scan to table end) when there is no eq prefix or `succ(eq)` carries
    // out (the last eq group). The slot bytes stay zero, so `succ` ripples only
    // through the eq prefix sub-slice.
    let next_group = || -> Option<CutKey> {
        if eq_size == 0 {
            return None;
        }
        let mut k = eq_low;
        if increment_key_in_place(&mut k.buf[..eq_size]) {
            Some(k)
        } else {
            None
        }
    };

    match rel {
        RangeRel::Gt => {
            // start = succ(eq‖d); a full carry-out means eq‖d is the maximal key —
            // no key space above it, so the row matches nothing.
            let mut start = p;
            if !increment_key_in_place(&mut start.buf[..stride]) {
                return None;
            }
            Some((start, next_group()))
        }
        RangeRel::Ge => Some((p, next_group())),
        RangeRel::Lt => Some((eq_low, Some(p))),
        RangeRel::Le => {
            // end = succ(eq‖d); the carry ripples into the eq prefix for free
            // (eq‖0xFF… → the next eq group's first key), and a full carry-out
            // means scan to the table end.
            let mut end = p;
            let end = increment_key_in_place(&mut end.buf[..stride]).then_some(end);
            Some((eq_low, end))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- increment_key_in_place -------------------------------------------

    #[test]
    fn succ_increments_low_byte() {
        let mut k = [0x00, 0x00, 0x05];
        assert!(increment_key_in_place(&mut k));
        assert_eq!(k, [0x00, 0x00, 0x06]);
    }

    #[test]
    fn succ_ripples_carry() {
        let mut k = [0x00, 0x00, 0xFF];
        assert!(increment_key_in_place(&mut k));
        assert_eq!(k, [0x00, 0x01, 0x00]);
    }

    #[test]
    fn succ_carries_out_on_all_ff() {
        let mut k = [0xFF, 0xFF];
        assert!(!increment_key_in_place(&mut k));
        assert_eq!(k, [0x00, 0x00]);
    }

    #[test]
    fn succ_empty_carries_out() {
        let mut k: [u8; 0] = [];
        assert!(!increment_key_in_place(&mut k));
    }

    // --- range_cut_points -------------------------------------------------
    //
    // Helpers build a 1-byte-slot, n_eq=0 probe so the cut keys are tiny and
    // exact-comparable, plus a 1-eq-slot variant for prefix coverage.

    fn cuts(eq: &[u8], d: &[u8], rel: RangeRel) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        range_cut_points(eq, d, rel).map(|(s, e)| (s.as_slice().to_vec(), e.map(|e| e.as_slice().to_vec())))
    }

    #[test]
    fn no_eq_interior_value() {
        // d = 0x05, slot only (stride = 1).
        assert_eq!(cuts(&[], &[0x05], RangeRel::Gt), Some((vec![0x06], None)));
        assert_eq!(cuts(&[], &[0x05], RangeRel::Ge), Some((vec![0x05], None)));
        assert_eq!(cuts(&[], &[0x05], RangeRel::Lt), Some((vec![0x00], Some(vec![0x05]))));
        assert_eq!(cuts(&[], &[0x05], RangeRel::Le), Some((vec![0x00], Some(vec![0x06]))));
    }

    #[test]
    fn no_eq_all_zero_value() {
        // d = 0x00: Gt of 0 starts at 1; Lt of 0 is empty interval [0,0).
        assert_eq!(cuts(&[], &[0x00], RangeRel::Gt), Some((vec![0x01], None)));
        assert_eq!(cuts(&[], &[0x00], RangeRel::Ge), Some((vec![0x00], None)));
        assert_eq!(cuts(&[], &[0x00], RangeRel::Lt), Some((vec![0x00], Some(vec![0x00]))));
        assert_eq!(cuts(&[], &[0x00], RangeRel::Le), Some((vec![0x00], Some(vec![0x01]))));
    }

    #[test]
    fn no_eq_all_ff_value() {
        // d = 0xFF (maximal slot, n_eq=0): Gt matches nothing (provably empty);
        // Ge starts at 0xFF; Le's succ carries out → scan to table end.
        assert_eq!(cuts(&[], &[0xFF], RangeRel::Gt), None);
        assert_eq!(cuts(&[], &[0xFF], RangeRel::Ge), Some((vec![0xFF], None)));
        assert_eq!(cuts(&[], &[0xFF], RangeRel::Lt), Some((vec![0x00], Some(vec![0xFF]))));
        assert_eq!(cuts(&[], &[0xFF], RangeRel::Le), Some((vec![0x00], None)));
    }

    #[test]
    fn eq_prefix_interior_value() {
        // eq = 0x07, d = 0x05 (stride = 2). Cut keys stay within / cap at the group.
        let eq = [0x07];
        assert_eq!(
            cuts(&eq, &[0x05], RangeRel::Gt),
            Some((vec![0x07, 0x06], Some(vec![0x08, 0x00])))
        );
        assert_eq!(
            cuts(&eq, &[0x05], RangeRel::Ge),
            Some((vec![0x07, 0x05], Some(vec![0x08, 0x00])))
        );
        assert_eq!(
            cuts(&eq, &[0x05], RangeRel::Lt),
            Some((vec![0x07, 0x00], Some(vec![0x07, 0x05])))
        );
        assert_eq!(
            cuts(&eq, &[0x05], RangeRel::Le),
            Some((vec![0x07, 0x00], Some(vec![0x07, 0x06])))
        );
    }

    #[test]
    fn eq_prefix_ripple_into_eq() {
        // eq = 0x07, d = 0xFF (maximal slot). The Le end and the Gt/Ge end both
        // ripple to the next eq group's first key 0x08‖0x00 — byte-identical.
        let eq = [0x07];
        assert_eq!(
            cuts(&eq, &[0xFF], RangeRel::Le),
            Some((vec![0x07, 0x00], Some(vec![0x08, 0x00])))
        );
        assert_eq!(
            cuts(&eq, &[0xFF], RangeRel::Ge),
            Some((vec![0x07, 0xFF], Some(vec![0x08, 0x00])))
        );
        // Gt of the maximal slot in a non-maximal group: start == end == next
        // group key (a zero-width, provably-empty interval).
        assert_eq!(
            cuts(&eq, &[0xFF], RangeRel::Gt),
            Some((vec![0x08, 0x00], Some(vec![0x08, 0x00])))
        );
    }

    #[test]
    fn eq_prefix_maximal_group_scans_to_end() {
        // eq = 0xFF (last eq group): the Gt/Ge end carries out of the eq prefix →
        // scan to the table end (None), not a wrapped lower key.
        let eq = [0xFF];
        assert_eq!(cuts(&eq, &[0x05], RangeRel::Ge), Some((vec![0xFF, 0x05], None)));
        assert_eq!(cuts(&eq, &[0x05], RangeRel::Gt), Some((vec![0xFF, 0x06], None)));
    }
}
