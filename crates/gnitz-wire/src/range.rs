// ---------------------------------------------------------------------------
// Range-bound descriptor — the shared wire form of an ordered range bound:
// a `ReadSpec` PK / index bound, the circuit `ScanBound`, and the engine's
// bounded index walks all carry one.
//
// Both client (gnitz-core) and engine (gnitz-server) MUST share this
// encoder/decoder so they cannot drift on the encoding — the same rule
// `pack_pk_cols`/`unpack_pk_cols` follow for the column list the descriptor
// travels with.
// ---------------------------------------------------------------------------

use crate::catalog::PK_LIST_MAX_COLS;
use crate::codec::{Reader, Writer};
use crate::types::{FixedInt, TypeCode};

const START_AFTER: u8 = 1 << 0;
const END_AFTER: u8 = 1 << 1;

/// A cut point in an index's group-key space: `Before(v)` falls below every
/// index entry whose range column equals `v` (and above every smaller group),
/// `After(v)` falls above every such entry. Values are **native** (packed LE
/// `u128`, the same convention as equality values); the worker is the sole OPK
/// encoder and maps a cut to its byte key — `pad(group(v))` for `Before`,
/// `pad(succ(group(v)))` for `After`.
///
/// Every SQL bound shape is one cut: `> v` ⇒ start `After(v)`, `>= v` ⇒ start
/// `Before(v)`, `< v` ⇒ end `Before(v)`, `<= v` ⇒ end `After(v)`, and an
/// unconstrained or saturated side ⇒ the type-edge cut (`Before(type_min)` /
/// `After(type_max)` — no index entry lies outside its column's type range).
/// There is deliberately no `Unbounded` variant: the three-state
/// `std::ops::Bound` split this type replaces needed lower/upper-specific
/// handling at every layer; a cut is the same thing at either end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cut {
    Before(u128),
    After(u128),
}

impl Cut {
    /// `After(v)` when `after`, else `Before(v)` — the constructor dual of
    /// [`Self::is_after`], for callers holding the wire bit.
    const fn new(after: bool, v: u128) -> Cut {
        if after {
            Cut::After(v)
        } else {
            Cut::Before(v)
        }
    }

    /// Whether this cut falls above its group — `After`'s wire flag bit.
    const fn is_after(self) -> bool {
        matches!(self, Cut::After(_))
    }

    /// The cut's group value, native-packed.
    pub const fn value(self) -> u128 {
        match self {
            Cut::Before(v) | Cut::After(v) => v,
        }
    }

    /// The cuts at a column type's representable edges — what an unconstrained
    /// range side widens to and what an out-of-type-range literal saturates
    /// to: no index entry lies outside its column's type range, so
    /// `(Before(type_min), After(type_max))` ARE "unbounded below/above".
    /// `None` for a type that cannot carry an ordered range bound (UUID,
    /// float, string, I128).
    pub const fn type_edges(tc: TypeCode) -> Option<(Cut, Cut)> {
        if matches!(tc, TypeCode::U128) {
            return Some((Cut::Before(0), Cut::After(u128::MAX)));
        }
        let Some(fi) = FixedInt::from_type_code(tc) else {
            return None;
        };
        let (min, max) = fi.range();
        Some((Cut::Before(fi.pack(min)), Cut::After(fi.pack(max))))
    }
}

/// An ordered secondary-index range scan: the leading `eq_vals().len()` index
/// columns are equality-pinned, and the next index column is bounded by the
/// half-open cut interval `[start, end)`. A zero-width (or inverted) interval
/// is a legitimate descriptor — the worker detects it byte-wise and returns
/// nothing — so the planner needs no empty-range special case.
///
/// Wire layout (fixed size, `2 + 16·(n_eq + 2)` bytes):
///
/// | bytes        | content                                                  |
/// |--------------|----------------------------------------------------------|
/// | 0            | `n_eq` — count of equality-pinned leading columns        |
/// | 1            | cut kinds: bit 0 start is `After`, bit 1 end is `After`  |
/// | 2 + 16·i     | i-th equality value, LE `u128`                           |
/// | then 16      | start cut value, LE `u128`                               |
/// | then 16      | end cut value, LE `u128`                                 |
///
/// Maximum encoded size: 82 bytes at the 4-column index-arity cap — past the
/// 64-byte `PkTuple` extra cap, which is why the descriptor rides an explicit
/// control-block blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeDescriptor {
    eq: [u128; PK_LIST_MAX_COLS],
    n_eq: usize,
    pub start: Cut,
    pub end: Cut,
}

impl RangeDescriptor {
    /// Construct from validated parts. Panics when `eq_vals` leaves no slot for
    /// the range column (`len >= PK_LIST_MAX_COLS`) — like `pack_pk_cols`,
    /// callers must validate the arity first (the client checks
    /// `eq_vals.len() < col_indices.len()`, and `validate_pk_col_list` caps the
    /// column list at `PK_LIST_MAX_COLS`).
    pub fn new(eq_vals: &[u128], start: Cut, end: Cut) -> Self {
        assert!(
            eq_vals.len() < PK_LIST_MAX_COLS,
            "RangeDescriptor: {} equality values leave no range column within \
             the {PK_LIST_MAX_COLS}-column arity cap",
            eq_vals.len(),
        );
        let mut eq = [0u128; PK_LIST_MAX_COLS];
        eq[..eq_vals.len()].copy_from_slice(eq_vals);
        RangeDescriptor { eq, n_eq: eq_vals.len(), start, end }
    }

    /// A full or partial equality lowered to its degenerate point range:
    /// `eq_vals` pin the leading columns and the next column is exactly `v`
    /// (`[Before(v), After(v))`) — the single spelling of "equality = a
    /// zero-width range on the last pinned column".
    pub fn point(eq_vals: &[u128], v: u128) -> Self {
        RangeDescriptor::new(eq_vals, Cut::Before(v), Cut::After(v))
    }

    /// True iff the range column is pinned to a single value — the shape
    /// [`Self::point`] builds. The recognizer dual of that constructor.
    pub fn is_point(&self) -> bool {
        matches!((self.start, self.end), (Cut::Before(a), Cut::After(b)) if a == b)
    }

    /// True iff this descriptor pins every column of an `n_cols`-column key
    /// list: a point whose equality prefix plus its own range column cover the
    /// whole list, so it names one key group and nothing wider.
    pub fn pins_all(&self, n_cols: usize) -> bool {
        self.is_point() && self.n_eq + 1 == n_cols
    }

    /// True iff this descriptor pins no column at all — no equality prefix, and a
    /// range column left as an interval rather than one value. Such a walk is
    /// bounded but names no key group, so it can cover the whole relation.
    pub fn pins_none(&self) -> bool {
        self.n_eq == 0 && !self.is_point()
    }

    /// The equality-pinned leading values; the range column sits right after
    /// them at index position `eq_vals().len()`.
    pub fn eq_vals(&self) -> &[u128] {
        &self.eq[..self.n_eq]
    }

    /// The exact wire span of a descriptor with `n_eq` equality values — the
    /// one definition of the `2 + 16·(n_eq + 2)` layout.
    pub(crate) const fn encoded_len(n_eq: usize) -> usize {
        2 + 16 * (n_eq + 2)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut flags = 0u8;
        if self.start.is_after() {
            flags |= START_AFTER;
        }
        if self.end.is_after() {
            flags |= END_AFTER;
        }
        let mut w = Writer::with_capacity(Self::encoded_len(self.n_eq));
        w.u8(self.n_eq as u8).u8(flags);
        for v in self.eq_vals() {
            w.u128(*v);
        }
        w.u128(self.start.value()).u128(self.end.value());
        w.into_vec()
    }

    /// Decode and validate at the trust boundary: the exact length implied by
    /// `n_eq` must match `buf.len()`, `n_eq` must leave a slot for the range
    /// column, and no unknown flag bits may be set — a malformed frame is
    /// rejected, never mis-decoded or allowed to index out of bounds. (The
    /// arity check against the actual column list stays with the engine
    /// method, which holds the list.)
    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        let mut r = Reader::new(buf, "range descriptor");
        let n_eq = r.u8()? as usize;
        let flags = r.u8()?;
        if n_eq >= PK_LIST_MAX_COLS {
            return Err(format!(
                "range descriptor n_eq {n_eq} leaves no range column within \
                 the {PK_LIST_MAX_COLS}-column arity cap"
            ));
        }
        if flags & !(START_AFTER | END_AFTER) != 0 {
            return Err(format!("range descriptor has unknown flag bits {flags:#04x}"));
        }
        let mut eq = [0u128; PK_LIST_MAX_COLS];
        for slot in eq.iter_mut().take(n_eq) {
            *slot = r.u128()?;
        }
        let start = Cut::new(flags & START_AFTER != 0, r.u128()?);
        let end = Cut::new(flags & END_AFTER != 0, r.u128()?);
        r.expect_consumed()?;
        Ok(RangeDescriptor { eq, n_eq, start, end })
    }
}

/// Read an embedded `RangeDescriptor` out of a larger blob: peek its `n_eq` to
/// learn its span, slice exactly that many bytes, and defer full validation to
/// [`RangeDescriptor::decode`]. (`n_eq` is one byte, so `encoded_len` cannot
/// overflow; a pathological value exceeds `remaining` and `take` rejects it, and
/// a valid one is re-validated — arity, flags, exact length — by the descriptor
/// decoder.)
pub(crate) fn read_range_descriptor(r: &mut Reader) -> Result<RangeDescriptor, String> {
    let n_eq = r.peek_u8()? as usize;
    let bytes = r.take(RangeDescriptor::encoded_len(n_eq))?;
    RangeDescriptor::decode(bytes)
}

#[cfg(test)]
#[path = "tests/range.rs"]
mod tests;
