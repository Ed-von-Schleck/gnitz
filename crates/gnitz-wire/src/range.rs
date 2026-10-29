//! Range bounds: the wire form of an ordered range walk over a key list.

use crate::codec::{Reader, Wire, Writer};
use crate::{PkColList, PK_LIST_MAX_COLS};

/// A range walk over the key list `cols`: the leading `eq_vals()` columns pinned to one
/// key image ([`crate::key_image`]) each, the next bounded by the inclusive
/// `[first, last]`. An inverted interval walks nothing.
///
/// Wire layout (`9 + 16·(n_eq + 2)` bytes):
///
/// | bytes        | content                                                  |
/// |--------------|----------------------------------------------------------|
/// | 0            | `cols`, packed by `PkColList::pack`                     |
/// | 8            | `n_eq` — count of equality-pinned leading columns        |
/// | 9 + 16·i     | i-th equality image, LE `u128`                           |
/// | then 16      | `first` image, LE `u128`                                 |
/// | then 16      | `last` image, LE `u128`                                  |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    cols: PkColList,
    eq: [u128; PK_LIST_MAX_COLS],
    n_eq: usize,
    pub first: u128,
    pub last: u128,
}

impl KeyRange {
    /// Panics when `eq` leaves no column of `cols` for the range.
    pub fn new(cols: PkColList, eq: &[u128], first: u128, last: u128) -> Self {
        if let Err(e) = check_eq_count(eq.len(), &cols) {
            panic!("KeyRange: {e}");
        }
        let mut vals = [0u128; PK_LIST_MAX_COLS];
        vals[..eq.len()].copy_from_slice(eq);
        KeyRange {
            cols,
            eq: vals,
            n_eq: eq.len(),
            first,
            last,
        }
    }

    /// The walk pinning the leading `vals.len()` columns of `cols` to `vals`.
    /// Panics when `vals` is empty or longer than `cols`.
    pub fn point(cols: PkColList, vals: &[u128]) -> Self {
        let (&v, eq) = vals.split_last().expect("KeyRange::point: no value");
        KeyRange::new(cols, eq, v, v)
    }

    pub fn cols(&self) -> PkColList {
        self.cols
    }

    /// The equality-pinned leading images; the range column sits right after them at
    /// list position `eq_vals().len()`.
    pub fn eq_vals(&self) -> &[u128] {
        &self.eq[..self.n_eq]
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
        w.u128(self.first).u128(self.last);
    }

    fn read(r: &mut Reader) -> Result<KeyRange, String> {
        let cols: PkColList = r.get()?;
        let n_eq = r.u8()? as usize;
        check_eq_count(n_eq, &cols)?;
        let mut eq = [0u128; PK_LIST_MAX_COLS];
        for slot in &mut eq[..n_eq] {
            *slot = r.u128()?;
        }
        let (first, last) = (r.u128()?, r.u128()?);
        Ok(KeyRange { cols, eq, n_eq, first, last })
    }
}

#[cfg(test)]
#[path = "tests/range.rs"]
mod tests;
