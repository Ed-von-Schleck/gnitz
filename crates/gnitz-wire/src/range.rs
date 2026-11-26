//! Range bounds: the wire form of an ordered range walk over a key list.

use crate::codec::{Reader, Wire, Writer};
use crate::{PK_LIST_MAX_COLS, PkColList};
use smallvec::SmallVec;

/// The most intervals a [`KeyRange`] admits on its range column: a worker walking
/// an index opens a cursor per interval.
pub const MAX_RANGE_SPANS: usize = 4096;

/// A range walk over the key list `cols`: the leading `eq_vals()` columns pinned to one
/// key image ([`crate::key_image`]) each, the next admitting the inclusive
/// intervals `spans()`, ascending and disjoint — one for a comparison, a point per
/// key for an `IN` list. No interval walks nothing.
///
/// Wire layout (`11 + 16·n_eq + 32·n_spans` bytes):
///
/// | bytes        | content                                                  |
/// |--------------|----------------------------------------------------------|
/// | 0            | `cols`, packed by `PkColList::pack`                     |
/// | 8            | `n_eq` — count of equality-pinned leading columns        |
/// | 9 + 16·i     | i-th equality image, LE `u128`                           |
/// | then 2       | `n_spans`, LE `u16`                                      |
/// | then 32 each | an interval's `first` and `last` images, LE `u128`       |
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    cols: PkColList,
    eq: [u128; PK_LIST_MAX_COLS],
    n_eq: usize,
    spans: SmallVec<[(u128, u128); 1]>,
}

impl KeyRange {
    /// The walk admitting `[first, last]` on the range column; an inverted
    /// interval admits nothing. Panics when `eq` leaves no column of `cols` for
    /// the range.
    pub fn new(cols: PkColList, eq: &[u128], first: u128, last: u128) -> Self {
        let spans = (first <= last).then_some((first, last));
        KeyRange::with_spans(cols, eq, spans).unwrap_or_else(|e| panic!("KeyRange: {e}"))
    }

    /// The walk pinning the leading `vals.len()` columns of `cols` to `vals`.
    /// Panics when `vals` is empty or longer than `cols`.
    pub fn point(cols: PkColList, vals: &[u128]) -> Self {
        let (&v, eq) = vals.split_last().expect("KeyRange::point: no value");
        KeyRange::new(cols, eq, v, v)
    }

    /// The walk admitting each interval of `spans` on the range column; `Err`
    /// when `eq` leaves no column of `cols` for the range, or the intervals are
    /// more than [`MAX_RANGE_SPANS`], inverted, out of order or overlapping.
    pub fn with_spans(
        cols: PkColList,
        eq: &[u128],
        spans: impl IntoIterator<Item = (u128, u128)>,
    ) -> Result<Self, String> {
        check_eq_count(eq.len(), &cols)?;
        let spans: SmallVec<[(u128, u128); 1]> = spans.into_iter().collect();
        if spans.len() > MAX_RANGE_SPANS {
            return Err(format!("{} intervals exceed {MAX_RANGE_SPANS}", spans.len()));
        }
        if spans.iter().any(|(first, last)| first > last) {
            return Err("an interval is inverted".into());
        }
        if spans.windows(2).any(|w| w[0].1 >= w[1].0) {
            return Err("the intervals overlap or are out of order".into());
        }
        let mut vals = [0u128; PK_LIST_MAX_COLS];
        vals[..eq.len()].copy_from_slice(eq);
        Ok(KeyRange { cols, eq: vals, n_eq: eq.len(), spans })
    }

    pub fn cols(&self) -> PkColList {
        self.cols
    }

    /// The equality-pinned leading images; the range column sits right after them at
    /// list position `eq_vals().len()`.
    pub fn eq_vals(&self) -> &[u128] {
        &self.eq[..self.n_eq]
    }

    /// The range column's admitted intervals `(first, last)`, inclusive,
    /// ascending and disjoint.
    pub fn spans(&self) -> &[(u128, u128)] {
        &self.spans
    }

    /// Whether the walk is every row its bounds admit. It holds no row NULL in a column
    /// of `cols`, so it is not where a column it leaves unbounded is nullable.
    pub fn is_exact(&self, nullable: impl Fn(u32) -> bool) -> bool {
        !self.cols.as_slice()[self.n_eq + 1..].iter().any(|&c| nullable(c))
    }

    /// Whether this walk reads the relation's own store: `cols` leads its PK list.
    pub fn walks_pk(&self, pk_cols: &[u32]) -> bool {
        pk_cols.starts_with(self.cols.as_slice())
    }
}

/// `Err` when `n_eq` equality values leave no column of `cols` for the range.
fn check_eq_count(n_eq: usize, cols: &PkColList) -> Result<(), String> {
    let n_cols = cols.as_slice().len();
    if n_eq >= n_cols {
        return Err(format!(
            "{n_eq} equality values leave no range column within its {n_cols} key columns"
        ));
    }
    Ok(())
}

impl Wire for KeyRange {
    fn write(&self, w: &mut Writer) {
        w.put(&self.cols).u8(self.n_eq as u8);
        for v in self.eq_vals() {
            w.u128(*v);
        }
        w.count(self.spans.len());
        for &(first, last) in &self.spans {
            w.u128(first).u128(last);
        }
    }

    fn read(r: &mut Reader) -> Result<KeyRange, String> {
        let cols: PkColList = r.get()?;
        let n_eq = r.u8()? as usize;
        check_eq_count(n_eq, &cols)?;
        let mut eq = [0u128; PK_LIST_MAX_COLS];
        for slot in &mut eq[..n_eq] {
            *slot = r.u128()?;
        }
        let spans = r.items("range intervals", MAX_RANGE_SPANS, |r| Ok((r.u128()?, r.u128()?)))?;
        KeyRange::with_spans(cols, &eq[..n_eq], spans)
    }
}

#[cfg(test)]
#[path = "tests/range.rs"]
mod tests;
