//! The OPK lower-bound search every sorted PK region seeks through, stateless
//! and galloping, plus the equal-PK group bracket. `Batch`, `MappedShard` and
//! the `Run` that is either all seek through the `(count, stride, ColPtr)`
//! entry points here.

use super::merge::ColPtr;
use crate::schema::key::{pk_width_dispatch, PkSortKey};
use gnitz_expr::RowSource;

/// Lower bound over `[lo, hi)`: the first index whose row sorts at-or-after the
/// probe, where `lt(i)` reports row `i < probe`.
#[inline]
fn lower_bound_by(mut lo: usize, mut hi: usize, lt: impl Fn(usize) -> bool) -> usize {
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if lt(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Galloping lower bound over `[0, count)`, seeded at `hint`: `O(log gap)` forward
/// when the boundary is after the hint, `O(1)` when the boundary IS the hint
/// (consecutive keys in one inter-row gap, or a run past the source end with
/// `hint == count`), and a bounded `[0, hint)` search when it is before. Correct
/// for ANY hint, and since `[0, hint] ⊆ [0, count)` it is **never asymptotically
/// worse** than `lower_bound_by(0, count, …)` — a backward or stale hint forfeits
/// only the speedup, at the cost of at most two extra comparisons. `lt(i)` reports
/// row `i < probe`, as in [`lower_bound_by`].
#[inline]
fn gallop_by(count: usize, hint: usize, lt: impl Fn(usize) -> bool) -> usize {
    let h = hint.min(count);
    if h < count && lt(h) {
        // boundary strictly after the hint
        let mut lo = h; // invariant: row `lo` sorts before the probe
        let mut step = 1usize;
        while lo + step < count && lt(lo + step) {
            lo += step;
            step *= 2;
        }
        let hi = (lo + step).min(count); // row `hi` sorts >= probe, or hi == count
        return lower_bound_by(lo + 1, hi, lt);
    }
    if h == 0 || lt(h - 1) {
        return h;
    } // boundary is exactly h (incl. h == count)
    lower_bound_by(0, h, lt) // genuine overshoot: bounded [0, h)
}

/// First row of `pk` whose OPK bytes are `>= key`; `key` is exactly `stride`
/// bytes.
///
/// # Safety
/// `pk` must address at least `count` rows of `stride` bytes.
#[inline]
pub(crate) unsafe fn seek_lower_bound(count: usize, stride: usize, pk: ColPtr, key: &[u8]) -> usize {
    debug_assert_eq!(key.len(), stride, "seek probe width must equal pk_stride");
    pk_width_dispatch!(stride, |K| {
        let p = K::from_opk(key);
        lower_bound_by(0, count, |i| K::from_opk(pk.row(i, stride)) < p)
    })
}

/// [`seek_lower_bound`] seeded at `hint`: `O(log gap)` ahead, `O(1)` at the hint,
/// never worse than the from-scratch search.
///
/// # Safety
/// As [`seek_lower_bound`].
#[inline]
pub(crate) unsafe fn seek_advance_to(count: usize, stride: usize, pk: ColPtr, key: &[u8], hint: usize) -> usize {
    debug_assert_eq!(key.len(), stride, "seek probe width must equal pk_stride");
    pk_width_dispatch!(stride, |K| {
        let p = K::from_opk(key);
        gallop_by(count, hint, |i| K::from_opk(pk.row(i, stride)) < p)
    })
}

/// First row index past the equal-PK group beginning at `start`, in a PK-sorted
/// source. Requires `start < src.row_count()`. A linear step, not a seek: every
/// caller reaches it having just landed on the group's first row, where the
/// group is short and a gallop would cost more.
#[inline]
pub(crate) fn pk_group_end<S: RowSource>(src: &S, start: usize) -> usize {
    let k = src.get_pk_bytes(start);
    let count = src.row_count();
    let mut j = start + 1;
    while j < count && crate::schema::key::pk_bytes_eq(src.get_pk_bytes(j), k) {
        j += 1;
    }
    j
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[path = "tests/seek.rs"]
mod tests;
