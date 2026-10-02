// Range bounds — the wire form of an ordered range walk over a key list.

use crate::catalog::PK_LIST_MAX_COLS;
use crate::codec::{Reader, Wire, Writer};
use crate::{PkColList, PkListRole};

const START_AFTER: u8 = 1 << 0;
const END_AFTER: u8 = 1 << 1;

/// A cut between adjacent key images ([`crate::key_image`]) of a column: below every
/// entry whose image is `image`, or above them when `after`. The derived order is cut
/// order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cut {
    pub image: u128,
    pub after: bool,
}

impl Cut {
    pub const fn before(image: u128) -> Cut {
        Cut { image, after: false }
    }

    pub const fn after(image: u128) -> Cut {
        Cut { image, after: true }
    }
}

/// A range walk over the key list `cols`: the leading `eq_vals()` columns pinned to one
/// image each, the next bounded by `[start, end)`. An empty or inverted interval walks
/// nothing.
///
/// Wire layout (`10 + 16·(n_eq + 2)` bytes):
///
/// | bytes        | content                                                  |
/// |--------------|----------------------------------------------------------|
/// | 0            | `cols`, packed by `PkColList::pack`                     |
/// | 8            | `n_eq` — count of equality-pinned leading columns        |
/// | 9            | cut kinds: bit 0 start is `after`, bit 1 end is `after`  |
/// | 10 + 16·i    | i-th equality image, LE `u128`                           |
/// | then 16      | start cut image, LE `u128`                               |
/// | then 16      | end cut image, LE `u128`                                 |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    cols: PkColList,
    eq: [u128; PK_LIST_MAX_COLS],
    n_eq: usize,
    pub start: Cut,
    pub end: Cut,
}

impl KeyRange {
    /// Panics when `eq` leaves no column of `cols` for the range.
    pub fn new(cols: PkColList, eq: &[u128], start: Cut, end: Cut) -> Self {
        assert!(
            eq.len() < cols.as_slice().len(),
            "KeyRange: {} equality values leave no range column within its {} key columns",
            eq.len(),
            cols.as_slice().len(),
        );
        let mut vals = [0u128; PK_LIST_MAX_COLS];
        vals[..eq.len()].copy_from_slice(eq);
        KeyRange {
            cols,
            eq: vals,
            n_eq: eq.len(),
            start,
            end,
        }
    }

    /// `eq` pins the leading columns and the next column is exactly `v`.
    pub fn point(cols: PkColList, eq: &[u128], v: u128) -> Self {
        KeyRange::new(cols, eq, Cut::before(v), Cut::after(v))
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

impl Wire for KeyRange {
    fn write(&self, w: &mut Writer) {
        let flags = ((self.start.after as u8) * START_AFTER) | ((self.end.after as u8) * END_AFTER);
        w.u64(self.cols.pack()).u8(self.n_eq as u8).u8(flags);
        for v in self.eq_vals() {
            w.u128(*v);
        }
        w.u128(self.start.image).u128(self.end.image);
    }

    fn read(r: &mut Reader) -> Result<KeyRange, String> {
        let cols = PkColList::unpack(r.u64()?).map_err(|e| e.for_role(PkListRole::ColumnList))?;
        let n_eq = r.u8()? as usize;
        let flags = r.flags(START_AFTER | END_AFTER)?;
        if n_eq >= cols.as_slice().len() {
            return Err(format!(
                "{n_eq} equality values leave no range column within its {} key columns",
                cols.as_slice().len()
            ));
        }
        let mut eq = [0u128; PK_LIST_MAX_COLS];
        for slot in &mut eq[..n_eq] {
            *slot = r.u128()?;
        }
        let start = Cut {
            image: r.u128()?,
            after: flags & START_AFTER != 0,
        };
        let end = Cut {
            image: r.u128()?,
            after: flags & END_AFTER != 0,
        };
        Ok(KeyRange { cols, eq, n_eq, start, end })
    }
}

#[cfg(test)]
#[path = "tests/range.rs"]
mod tests;
