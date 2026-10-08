//! Range membership: which rows of a batch lie in the walk a [`KeyRange`] names over
//! its key column list, decided on the columns' key images — the order an index or PK
//! walk compares in — one column at a time into filter words.

use gnitz_wire::{image_mask, KeyRange};

use crate::{BatchView, ColumnLocator, ExprValidateErr, SchemaDescriptor};

/// A range walk over a column list, as a batch filter.
pub(crate) struct RangeMembership {
    /// The null-word bits of the list's payload columns: no index entry holds a NULL.
    null_mask: u64,
    /// Per bounded column, its admitted key images `lo ..= lo + span`: an equality
    /// column is a span of 0. Empty when the cuts admit nothing.
    bounds: Vec<(ColumnLocator, u128, u128)>,
}

impl RangeMembership {
    /// The walk `range` names; `Err` when a column is out of range or its type has no
    /// key order.
    pub(crate) fn new(range: &KeyRange, schema: &SchemaDescriptor) -> Result<Self, ExprValidateErr> {
        let bad = |e: String| ExprValidateErr::BadWalk(format!("range walk: {e}"));
        let cols = range.cols();
        let n = schema.num_columns();
        if let Some(&c) = cols.as_slice().iter().find(|&&c| c as usize >= n) {
            return Err(bad(format!("column {c} is out of range for a {n}-column schema")));
        }
        let locs: Vec<ColumnLocator> = cols.as_slice().iter().map(|&c| schema.locate(c as usize)).collect();
        if let Some(l) = locs.iter().find(|l| !l.type_code().is_pk_eligible()) {
            return Err(bad(format!("column type {} has no key order", l.type_code())));
        }
        let range_col = locs[range.eq_vals().len()];
        let null_mask = locs.iter().fold(0, |m, l| m | l.null_bit());
        let mask = image_mask(range_col.size());
        let (start, end) = (range.start.image & mask, range.end.image & mask);
        let lo = if range.start.after {
            start.checked_add(1)
        } else {
            Some(start)
        };
        let hi = if range.end.after { Some(end) } else { end.checked_sub(1) };
        let bounds = match lo.zip(hi).filter(|(lo, hi)| lo <= hi) {
            Some((lo, hi)) => locs
                .iter()
                .zip(range.eq_vals())
                .map(|(&l, &v)| (l, v & image_mask(l.size()), 0))
                .chain([(range_col, lo, hi - lo)])
                .collect(),
            None => Vec::new(),
        };
        Ok(RangeMembership { null_mask, bounds })
    }

    /// Clear the bit of every row of `mb` outside the walk in `words`, one bit per row.
    pub(crate) fn and_into(&self, mb: &dyn BatchView, words: &mut [u64]) {
        debug_assert_eq!(words.len(), mb.row_count().div_ceil(64), "and_into: one bit per row");
        if self.bounds.is_empty() {
            words.fill(0);
            return;
        }
        if self.null_mask != 0 {
            let level = crate::simd::level();
            for (words, rows) in words.chunks_mut(4).zip(mb.null_bmp().chunks(4 * 64 * 8)) {
                let mut nulls = [0u64; 4];
                crate::simd::null_bits(level, rows, self.null_mask, &mut nulls[..words.len()]);
                for (w, n) in words.iter_mut().zip(nulls) {
                    *w &= !n;
                }
            }
        }
        for &(loc, lo, span) in &self.bounds {
            match loc.size() {
                1 => and_column::<u8, 1>(words, mb, loc, lo, span),
                2 => and_column::<u16, 2>(words, mb, loc, lo, span),
                4 => and_column::<u32, 4>(words, mb, loc, lo, span),
                8 => and_column::<u64, 8>(words, mb, loc, lo, span),
                16 => and_column::<u128, 16>(words, mb, loc, lo, span),
                w => unreachable!("RangeMembership: a key column is 1/2/4/8/16 bytes, not {w}"),
            }
        }
    }
}

/// An unsigned integer the width of a key column.
trait KeyCell<const W: usize>: Copy + PartialOrd {
    fn from_le(b: [u8; W]) -> Self;
    fn from_be(b: [u8; W]) -> Self;
    fn low(v: u128) -> Self;
    fn sub(self, o: Self) -> Self;
}

macro_rules! key_cell {
    ($($t:ty),*) => {$(
        impl KeyCell<{ size_of::<$t>() }> for $t {
            fn from_le(b: [u8; size_of::<$t>()]) -> Self { <$t>::from_le_bytes(b) }
            fn from_be(b: [u8; size_of::<$t>()]) -> Self { <$t>::from_be_bytes(b) }
            fn low(v: u128) -> Self { v as $t }
            fn sub(self, o: Self) -> Self { self.wrapping_sub(o) }
        }
    )*};
}
key_cell!(u8, u16, u32, u64, u128);

/// Clear the bit of every row whose image in column `loc` falls outside `lo ..= lo + span`,
/// both images of the column's own type.
fn and_column<C: KeyCell<W>, const W: usize>(
    words: &mut [u64],
    mb: &dyn BatchView,
    loc: ColumnLocator,
    lo: u128,
    span: u128,
) {
    let span = C::low(span);
    match loc {
        // A PK column is stored as its image.
        ColumnLocator::Pk { byte_off, .. } => {
            let (region, stride) = mb.pk_region();
            let lo = C::low(lo);
            and_cells(words, region, stride, byte_off as usize, |c| {
                C::from_be(c).sub(lo) <= span
            });
        }
        // A payload column's image is its native value plus the sign bias.
        ColumnLocator::Payload { slot, type_code, .. } => {
            let col = mb.col_data(slot as usize, W);
            let lo = C::low(lo.wrapping_sub(gnitz_wire::opk_bias(type_code)));
            and_cells(words, col, W, 0, |c| C::from_le(c).sub(lo) <= span);
        }
    }
}

/// AND into `words` one bit per row of `region`, `stride` bytes a row: `keep` of the row's
/// `W` bytes at `off`.
#[inline(always)]
fn and_cells<const W: usize>(
    words: &mut [u64],
    region: &[u8],
    stride: usize,
    off: usize,
    keep: impl Fn([u8; W]) -> bool,
) {
    for (word, rows) in words.iter_mut().zip(region.chunks(64 * stride)) {
        let mut bits = 0u64;
        for (i, row) in rows.chunks_exact(stride).enumerate() {
            bits |= (keep(row[off..off + W].try_into().unwrap()) as u64) << i;
        }
        *word &= bits;
    }
}

#[cfg(test)]
#[path = "tests/range.rs"]
mod tests;
