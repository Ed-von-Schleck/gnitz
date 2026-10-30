//! Lower-bound search over a sorted row range, and the seeks over a PK column.

use std::ops::Range;

use crate::schema::{pk_width_dispatch, PkSortKey};
use gnitz_wire::RowSource;

/// A column as `base + row * stride`. Stride 0 reads one shared element for
/// every row. `base` itself need not lie inside the column: a decoded window is
/// rebased so that its first row lands on its first cell.
#[derive(Clone, Copy)]
pub(crate) struct ColPtr {
    pub base: *const u8,
    pub stride: usize,
}

impl ColPtr {
    /// Row `i`'s address.
    #[inline(always)]
    pub(crate) fn row_ptr(self, i: usize) -> *const u8 {
        self.base.wrapping_add(i * self.stride)
    }

    /// Row `i`'s first `len` bytes.
    ///
    /// # Safety
    /// Row `i` is a row of a live column this addresses, and `len` does not
    /// exceed its element width.
    #[inline(always)]
    pub(crate) unsafe fn row<'a>(self, i: usize, len: usize) -> &'a [u8] {
        std::slice::from_raw_parts(self.row_ptr(i), len)
    }

    /// Copy rows `start..` of this column of `width`-byte elements onto `dst`,
    /// `dst.len() / width` rows.
    ///
    /// # Safety
    /// Those rows are rows of a live column this addresses.
    pub(crate) unsafe fn copy_rows(self, start: usize, width: usize, dst: &mut [u8]) {
        if self.stride != 0 {
            return dst.copy_from_slice(std::slice::from_raw_parts(self.row_ptr(start), dst.len()));
        }
        if dst.is_empty() {
            return;
        }
        // One element repeated: write it once, then double the written prefix,
        // so any element width costs O(log rows) copies.
        dst[..width].copy_from_slice(self.row(0, width));
        let mut filled = width;
        while filled < dst.len() {
            let n = filled.min(dst.len() - filled);
            dst.copy_within(..n, filled);
            filled += n;
        }
    }
}

/// Lower bound over `[lo, hi)`: the first index whose row sorts at-or-after the
/// probe, where `lt(i)` reports row `i < probe`.
#[inline]
pub(crate) fn lower_bound_by(mut lo: usize, mut hi: usize, lt: impl Fn(usize) -> bool) -> usize {
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

/// Galloping lower bound over `(lo, count)`, where row `lo` already sorts before
/// the probe: `O(1)` for the next row, `O(log gap)` for a far one. `lt(i)`
/// reports row `i < probe`, as in [`lower_bound_by`].
#[inline(always)]
pub(crate) fn gallop_from(count: usize, mut lo: usize, lt: impl Fn(usize) -> bool) -> usize {
    debug_assert!(lo < count, "gallop_from: row {lo} out of {count}");
    let mut step = 1usize;
    while lo + step < count && lt(lo + step) {
        lo += step;
        step *= 2;
    }
    let hi = (lo + step).min(count); // row `hi` sorts >= probe, or hi == count
    lower_bound_by(lo + 1, hi, lt)
}

/// Lower bound over `[0, count)`, galloping from `hint` where there is one, which
/// may be any row. `lt(i)` reports row `i < probe`, as in [`lower_bound_by`].
#[inline]
pub(crate) fn gallop_by(count: usize, hint: Option<usize>, lt: impl Fn(usize) -> bool) -> usize {
    let Some(hint) = hint else {
        return lower_bound_by(0, count, lt);
    };
    let h = hint.min(count);
    if h < count && lt(h) {
        return gallop_from(count, h, lt);
    }
    if h == 0 || lt(h - 1) {
        return h;
    }
    lower_bound_by(0, h, lt)
}

/// First row of `pk` whose `width`-byte OPK is `>= key`, galloping from `hint`
/// where there is one.
///
/// # Safety
/// `pk` must address at least `count` rows of `width` bytes.
#[inline]
pub(crate) unsafe fn seek_lower_bound(
    count: usize,
    width: usize,
    pk: ColPtr,
    key: &[u8],
    hint: Option<usize>,
) -> usize {
    debug_assert_eq!(key.len(), width, "seek probe width must equal pk_stride");
    pk_width_dispatch!(width, |K| {
        let p = K::from_opk(key);
        gallop_by(count, hint, |i| K::from_opk(pk.row(i, width)) < p)
    })
}

/// The rows of `pk` whose OPK bytes are `key`: none, at its lower bound, where
/// no row holds it. The lower bound gallops from `hint` where there is one.
///
/// # Safety
/// As [`seek_lower_bound`].
#[inline]
pub(crate) unsafe fn seek_pk_rows(
    count: usize,
    width: usize,
    pk: ColPtr,
    key: &[u8],
    hint: Option<usize>,
) -> Range<usize> {
    debug_assert_eq!(key.len(), width, "seek probe width must equal pk_stride");
    pk_width_dispatch!(width, |K| {
        let p = K::from_opk(key);
        let lo = gallop_by(count, hint, |i| K::from_opk(pk.row(i, width)) < p);
        let mut hi = lo;
        while hi < count && K::from_opk(pk.row(hi, width)) == p {
            hi += 1;
        }
        lo..hi
    })
}

/// The first row past `start` of a PK-sorted source not sharing row `start`'s
/// leading `width` key bytes.
#[inline]
pub(crate) fn pk_group_end<S: RowSource>(src: &S, start: usize, width: usize) -> usize {
    let count = src.row_count();
    debug_assert!(start < count, "pk_group_end: row {start} out of {count}");
    let k = &src.get_pk_bytes(start)[..width];
    let mut j = start + 1;
    while j < count && crate::schema::compare_pk_ordering(&src.get_pk_bytes(j)[..width], k).is_eq() {
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

#[cfg(test)]
#[path = "benches/seek.rs"]
mod bench;
