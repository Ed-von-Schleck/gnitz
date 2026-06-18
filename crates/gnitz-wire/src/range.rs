// ---------------------------------------------------------------------------
// SEEK_BY_INDEX_RANGE descriptor — the wire payload of an ordered range scan
// over a secondary index.
//
// Both client (gnitz-core) and engine (gnitz-engine worker) MUST share this
// encoder/decoder so they cannot drift on the encoding — the same rule
// `pack_pk_cols`/`unpack_pk_cols` follow for the column list the descriptor
// travels with.
// ---------------------------------------------------------------------------

use crate::catalog::PK_LIST_MAX_COLS;
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
    pub const fn new(after: bool, v: u128) -> Cut {
        if after {
            Cut::After(v)
        } else {
            Cut::Before(v)
        }
    }

    /// Whether this cut falls above its group — `After`'s wire flag bit.
    pub const fn is_after(self) -> bool {
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
        RangeDescriptor {
            eq,
            n_eq: eq_vals.len(),
            start,
            end,
        }
    }

    /// The equality-pinned leading values; the range column sits right after
    /// them at index position `eq_vals().len()`.
    pub fn eq_vals(&self) -> &[u128] {
        &self.eq[..self.n_eq]
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + 16 * (self.n_eq + 2));
        out.push(self.n_eq as u8);
        let mut flags = 0u8;
        if self.start.is_after() {
            flags |= START_AFTER;
        }
        if self.end.is_after() {
            flags |= END_AFTER;
        }
        out.push(flags);
        for v in self.eq_vals() {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&self.start.value().to_le_bytes());
        out.extend_from_slice(&self.end.value().to_le_bytes());
        out
    }

    /// Decode and validate at the trust boundary: the exact length implied by
    /// `n_eq` must match `buf.len()`, `n_eq` must leave a slot for the range
    /// column, and no unknown flag bits may be set — a malformed frame is
    /// rejected, never mis-decoded or allowed to index out of bounds. (The
    /// arity check against the actual column list stays with the engine
    /// method, which holds the list.)
    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < 2 {
            return Err("range descriptor shorter than 2 bytes".to_string());
        }
        let n_eq = buf[0] as usize;
        let flags = buf[1];
        if n_eq >= PK_LIST_MAX_COLS {
            return Err(format!(
                "range descriptor n_eq {n_eq} leaves no range column within \
                 the {PK_LIST_MAX_COLS}-column arity cap"
            ));
        }
        if flags & !(START_AFTER | END_AFTER) != 0 {
            return Err(format!("range descriptor has unknown flag bits {flags:#04x}"));
        }
        let expected = 2 + 16 * (n_eq + 2);
        if buf.len() != expected {
            return Err(format!("range descriptor length {} != expected {expected}", buf.len()));
        }
        let mut off = 2;
        let mut next = || {
            let v = u128::from_le_bytes(buf[off..off + 16].try_into().unwrap());
            off += 16;
            v
        };
        let mut eq = [0u128; PK_LIST_MAX_COLS];
        for slot in eq.iter_mut().take(n_eq) {
            *slot = next();
        }
        let start = Cut::new(flags & START_AFTER != 0, next());
        let end = Cut::new(flags & END_AFTER != 0, next());
        Ok(RangeDescriptor { eq, n_eq, start, end })
    }
}

#[cfg(test)]
mod tests {
    use super::Cut::{After, Before};
    use super::*;

    #[test]
    fn roundtrips_every_shape() {
        let shapes: [(&[u128], Cut, Cut); 5] = [
            (&[], After(10), After(u64::MAX as u128)), // pure x > 10
            (&[], Before(0), After(20)),               // pure x <= 20
            (&[7], Before(1), Before(u128::MAX)),      // a = 7 AND 1 <= b < MAX
            (&[1, 2, 3], After(10), Before(50)),       // max arity, 82 bytes
            (&[], After(5), After(5)),                 // zero-width (empty)
        ];
        for (eq, start, end) in shapes {
            let d = RangeDescriptor::new(eq, start, end);
            let bytes = d.encode();
            assert_eq!(RangeDescriptor::decode(&bytes), Ok(d), "{eq:?} {start:?} {end:?}");
        }
    }

    #[test]
    fn max_arity_encodes_to_82_bytes() {
        let d = RangeDescriptor::new(&[1, 2, 3], After(10), Before(50));
        assert_eq!(d.encode().len(), 82);
    }

    #[test]
    fn decode_rejects_malformed() {
        // Too short for the fixed header.
        assert!(RangeDescriptor::decode(&[]).is_err());
        assert!(RangeDescriptor::decode(&[0]).is_err());
        // n_eq with no slot left for the range column.
        let mut d = RangeDescriptor::new(&[1, 2, 3], Before(0), Before(1)).encode();
        d[0] = PK_LIST_MAX_COLS as u8;
        assert!(RangeDescriptor::decode(&d).is_err());
        // Unknown flag bits.
        let mut stray = RangeDescriptor::new(&[7], Before(1), After(2)).encode();
        stray[1] |= 1 << 4;
        assert!(RangeDescriptor::decode(&stray).is_err());
        // Length disagreeing with n_eq — both directions.
        let good = RangeDescriptor::new(&[7], Before(1), After(2)).encode();
        assert!(RangeDescriptor::decode(&good[..good.len() - 1]).is_err());
        let mut long = good.clone();
        long.push(0);
        assert!(RangeDescriptor::decode(&long).is_err());
    }
}
