//! Range membership: which rows of a batch lie in the walk a [`KeyRange`] names over
//! its key column list, decided on the columns' key images — the order an index or PK
//! walk compares in — one column at a time into filter words.

use std::ops::Range;

use gnitz_wire::{KeyRange, image_mask};

use crate::simd::Level;
use crate::{BatchView, ColumnLocator, ExprValidateErr, SchemaDescriptor};

/// A range walk over a column list, as a batch filter.
#[derive(Clone)]
pub(crate) struct RangeMembership {
    /// The null-word bits of the list's payload columns: no index entry holds a NULL.
    null_mask: u64,
    /// Per bounded column, its admitted key images `lo ..= lo + span`: each equality
    /// column a span of 0, then the range column where it admits one interval.
    bounds: Vec<(ColumnLocator, u128, u128)>,
    /// The range column where it admits no interval or several: its intervals'
    /// `(first, last)` images, ascending and disjoint.
    spans: Option<(ColumnLocator, Vec<(u128, u128)>)>,
    /// The instruction set the null exclusion's kernel runs at.
    pub(crate) level: Level,
}

impl RangeMembership {
    /// The walk `range` names; `Err` when a column is out of range or its type has no
    /// key order.
    pub(crate) fn new(range: &KeyRange, schema: &SchemaDescriptor) -> Result<Self, ExprValidateErr> {
        let bad = |e: String| ExprValidateErr::BadWalk(format!("range walk: {e}"));
        let locs = (range.cols().as_slice().iter())
            .map(|&c| schema.wire_col("column", c).map(|(_, l)| l).map_err(bad))
            .collect::<Result<Vec<ColumnLocator>, _>>()?;
        if let Some(l) = locs.iter().find(|l| !l.type_code().is_pk_eligible()) {
            return Err(bad(format!("column type {} has no key order", l.type_code())));
        }
        let range_col = locs[range.eq_vals().len()];
        let null_mask = locs.iter().fold(0, |m, l| m | l.null_bit());
        // Each interval's ends masked to the column's width; one inverted so
        // admits nothing.
        let mask = image_mask(range_col.size());
        let spans: Vec<(u128, u128)> = (range.spans().iter())
            .map(|&(first, last)| (first & mask, last & mask))
            .filter(|(first, last)| first <= last)
            .collect();
        let mut bounds: Vec<_> = (locs.iter().zip(range.eq_vals()))
            .map(|(&l, &v)| (l, v & image_mask(l.size()), 0))
            .collect();
        let spans = match spans[..] {
            [(first, last)] => {
                bounds.push((range_col, first, last - first));
                None
            }
            _ => Some((range_col, spans)),
        };
        Ok(RangeMembership {
            null_mask,
            bounds,
            spans,
            level: Level::new(),
        })
    }

    /// Clear the bit of every row of `rows` of `mb` outside the walk in `words`, one
    /// bit per row, bit `i` for row `rows.start + i`.
    pub(crate) fn and_into(&self, mb: &dyn BatchView, rows: Range<usize>, words: &mut [u64]) {
        debug_assert_eq!(words.len(), rows.len().div_ceil(64), "and_into: one bit per row");
        if self.spans.as_ref().is_some_and(|(_, s)| s.is_empty()) {
            words.fill(0);
            return;
        }
        #[cfg(debug_assertions)]
        if !rows.is_empty() {
            let cols = self
                .bounds
                .iter()
                .map(|&(loc, ..)| loc)
                .chain(self.spans.iter().map(|&(loc, _)| loc));
            let reads = cols.filter_map(|loc| match loc {
                ColumnLocator::Payload { slot, size, .. } => Some((slot as usize, size as usize)),
                ColumnLocator::Pk { .. } => None,
            });
            crate::view::debug_assert_coherent(mb, rows.start, reads);
        }
        if self.null_mask != 0 {
            let null_bmp = &mb.null_bmp()[rows.start * 8..rows.end * 8];
            for (words, nbm) in words.chunks_mut(4).zip(null_bmp.chunks(4 * 64 * 8)) {
                let mut nulls = [0u64; 4];
                crate::simd::null_bits(self.level, nbm, self.null_mask, &mut nulls[..words.len()]);
                for (w, n) in words.iter_mut().zip(nulls) {
                    *w &= !n;
                }
            }
        }
        // Clear the bit of every row whose image falls outside `lo ..= lo + span`,
        // both bounds images of the column's own type.
        for &(loc, lo, span) in &self.bounds {
            gnitz_wire::by_cell_width!(loc.size(), |T| {
                const W: usize = size_of::<T>();
                let span = span as T;
                match loc {
                    // A PK column is stored as its image.
                    ColumnLocator::Pk { byte_off, .. } => {
                        let (region, stride) = mb.pk_region();
                        let region = &region[rows.start * stride..rows.end * stride];
                        let lo = lo as T;
                        and_cells::<W>(words, region, stride, byte_off as usize, |c| {
                            T::from_be_bytes(c).wrapping_sub(lo) <= span
                        })
                    }
                    // A payload column's image is its native value plus the sign bias.
                    ColumnLocator::Payload { slot, type_code, .. } => {
                        let lo = lo.wrapping_sub(gnitz_wire::opk_bias(type_code)) as T;
                        let region = &mb.col_data(slot as usize, W)[rows.start * W..rows.end * W];
                        and_cells::<W>(words, region, W, 0, |c| T::from_le_bytes(c).wrapping_sub(lo) <= span)
                    }
                }
            })
        }
        // Clear the bit of every row whose image lies in none of the intervals.
        if let Some((loc, spans)) = &self.spans {
            gnitz_wire::by_cell_width!(loc.size(), |T| {
                const W: usize = size_of::<T>();
                // The intervals are masked to the column's images, so each fits `T`.
                let admits = |image: T| {
                    let i = spans.partition_point(|&(first, _)| first as T <= image);
                    i > 0 && image <= spans[i - 1].1 as T
                };
                match *loc {
                    ColumnLocator::Pk { byte_off, .. } => {
                        let (region, stride) = mb.pk_region();
                        let region = &region[rows.start * stride..rows.end * stride];
                        and_cells::<W>(words, region, stride, byte_off as usize, |c| {
                            admits(T::from_be_bytes(c))
                        })
                    }
                    ColumnLocator::Payload { slot, type_code, .. } => {
                        let bias = gnitz_wire::opk_bias(type_code) as T;
                        let region = &mb.col_data(slot as usize, W)[rows.start * W..rows.end * W];
                        and_cells::<W>(words, region, W, 0, |c| admits(T::from_le_bytes(c).wrapping_add(bias)))
                    }
                }
            })
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
        gnitz_wire::zip_cells::<W, _>(rows, stride, off, 0..64usize, |c, i| bits |= (keep(*c) as u64) << i);
        *word &= bits;
    }
}

#[cfg(test)]
#[path = "tests/range.rs"]
mod tests;
