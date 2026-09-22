//! Range membership: which rows of a batch lie in the walk a [`RangeDescriptor`]
//! names over a key column list, decided on the columns' key images — the order an
//! index or PK walk compares in — one column at a time into filter words.

use gnitz_wire::{key_image, Cut, RangeDescriptor};

use crate::batch::scan_filter_bits;
use crate::{BatchView, ColumnLocator, SchemaFacts};

/// A range walk over a column list, as a batch filter.
pub struct RangeMembership {
    /// The null-word bits of the list's payload columns: no index entry holds a NULL.
    null_mask: u64,
    /// Per bounded column, its admitted key images `lo ..= lo + span`: an equality
    /// column is a span of 0. Empty when the cuts admit nothing.
    bounds: Vec<(ColumnLocator, u128, u128)>,
}

impl RangeMembership {
    /// The walk `desc` names over `cols`; `Err` when `desc` leaves no range column in
    /// `cols`, or a column's type has no key order.
    pub fn new(cols: &[u32], desc: RangeDescriptor, schema: &dyn SchemaFacts) -> Result<Self, String> {
        let locs: Vec<ColumnLocator> = cols.iter().map(|&c| schema.locate(c as usize)).collect();
        for l in &locs {
            gnitz_wire::index_key_type(l.type_code())?;
        }
        let range_col = *locs
            .get(desc.eq_vals().len())
            .ok_or("range walk: the equality prefix leaves no range column")?;
        let null_mask = locs.iter().fold(0, |m, l| match *l {
            ColumnLocator::Payload { slot, .. } => m | 1 << slot,
            ColumnLocator::Pk { .. } => m,
        });
        let image = |c: Cut| key_image(range_col.type_code(), c.value());
        let lo = match desc.start {
            Cut::Before(_) => Some(image(desc.start)),
            Cut::After(_) => image(desc.start).checked_add(1),
        };
        let hi = match desc.end {
            Cut::Before(_) => image(desc.end).checked_sub(1),
            Cut::After(_) => Some(image(desc.end)),
        };
        let bounds = match lo.zip(hi).filter(|(lo, hi)| lo <= hi) {
            Some((lo, hi)) => locs
                .iter()
                .zip(desc.eq_vals())
                .map(|(&l, &v)| (l, key_image(l.type_code(), v), 0))
                .chain([(range_col, lo, hi - lo)])
                .collect(),
            None => Vec::new(),
        };
        Ok(RangeMembership { null_mask, bounds })
    }

    /// The rows of `mb` in the walk, as maximal runs into `out`; `words` is scratch.
    pub fn filter_ranges(&self, mb: &dyn BatchView, words: &mut Vec<u64>, out: &mut Vec<(usize, usize)>) {
        self.filter_words(mb, words);
        out.clear();
        scan_filter_bits(words, mb.row_count(), out);
    }

    /// One bit per row of `mb`, set when the row lies in the walk, into `words`.
    pub fn filter_words(&self, mb: &dyn BatchView, words: &mut Vec<u64>) {
        let n = mb.row_count();
        words.clear();
        if self.bounds.is_empty() {
            words.resize(n.div_ceil(64), 0);
            return;
        }
        words.resize(n / 64, u64::MAX);
        if !n.is_multiple_of(64) {
            words.push(gnitz_wire::low_bits_mask(n % 64));
        }
        if self.null_mask != 0 {
            let mask = self.null_mask;
            and_cells(words, mb.null_bmp(), 8, 0, |w: [u8; 8]| {
                u64::from_le_bytes(w) & mask == 0
            });
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
            let lo = C::low(lo.wrapping_sub(gnitz_wire::opk_bias(type_code, W)));
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
