// ---------------------------------------------------------------------------
// SEEK_BY_INDEX_RANGE descriptor — the wire payload of an ordered range scan
// over a secondary index (§2 of `plans/secondary-index-range-scan.md`).
//
// Both client (gnitz-core) and engine (gnitz-engine worker) MUST share this
// encoder/decoder so they cannot drift on the encoding — the same rule
// `pack_pk_cols`/`unpack_pk_cols` follow for the column list the descriptor
// travels with.
// ---------------------------------------------------------------------------

use std::ops::Bound;

use crate::catalog::PK_LIST_MAX_COLS;

const LO_INCLUSIVE: u8 = 1 << 0;
const HI_INCLUSIVE: u8 = 1 << 1;
const LO_UNBOUNDED: u8 = 1 << 2;
const HI_UNBOUNDED: u8 = 1 << 3;

/// An ordered secondary-index range scan: the leading `eq_vals().len()` index
/// columns are equality-pinned, and the next index column is bounded by
/// `lo`/`hi`. Values are **native** (packed LE `u128`); the worker is the sole
/// OPK encoder, so the descriptor ships them verbatim. An `Unbounded` side
/// covers every indexed value on that side — it is inherently inclusive of the
/// OPK sentinel, which is why the bounds are `std::ops::Bound` and not a
/// `(value, inclusive)` pair that could mis-state inclusivity for an unbounded
/// side.
///
/// Wire layout:
///
/// | bytes        | content                                                  |
/// |--------------|----------------------------------------------------------|
/// | 0            | `n_eq` — count of equality-pinned leading columns        |
/// | 1            | bound flags: bit 0 lo inclusive, bit 1 hi inclusive,     |
/// |              | bit 2 lo unbounded, bit 3 hi unbounded                   |
/// | 2 + 16·i     | i-th equality value, LE `u128`                           |
/// | then 16      | lo value (absent when unbounded)                         |
/// | then 16      | hi value (absent when unbounded)                         |
///
/// Maximum encoded size: `2 + 16·(PK_LIST_MAX_COLS - 1 + 2)` = 82 bytes at the
/// 4-column index-arity cap — past the 64-byte `PkTuple` extra cap, which is
/// why the descriptor rides an explicit control-block blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeDescriptor {
    eq:   [u128; PK_LIST_MAX_COLS],
    n_eq: usize,
    pub lo: Bound<u128>,
    pub hi: Bound<u128>,
}

impl RangeDescriptor {
    /// Construct from validated parts. Panics when `eq_vals` leaves no slot for
    /// the range column (`len >= PK_LIST_MAX_COLS`) — like `pack_pk_cols`,
    /// callers must validate the arity first (the client checks
    /// `eq_vals.len() < col_indices.len()`, and `validate_pk_col_list` caps the
    /// column list at `PK_LIST_MAX_COLS`).
    pub fn new(eq_vals: &[u128], lo: Bound<u128>, hi: Bound<u128>) -> Self {
        assert!(
            eq_vals.len() < PK_LIST_MAX_COLS,
            "RangeDescriptor: {} equality values leave no range column within \
             the {PK_LIST_MAX_COLS}-column arity cap", eq_vals.len(),
        );
        let mut eq = [0u128; PK_LIST_MAX_COLS];
        eq[..eq_vals.len()].copy_from_slice(eq_vals);
        RangeDescriptor { eq, n_eq: eq_vals.len(), lo, hi }
    }

    /// The equality-pinned leading values; the range column sits right after
    /// them at index position `eq_vals().len()`.
    pub fn eq_vals(&self) -> &[u128] { &self.eq[..self.n_eq] }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + 16 * (self.n_eq + 2));
        out.push(self.n_eq as u8);
        out.push(
            bound_bits(&self.lo, LO_INCLUSIVE, LO_UNBOUNDED)
                | bound_bits(&self.hi, HI_INCLUSIVE, HI_UNBOUNDED),
        );
        for v in self.eq_vals() {
            out.extend_from_slice(&v.to_le_bytes());
        }
        if let Bound::Included(v) | Bound::Excluded(v) = self.lo {
            out.extend_from_slice(&v.to_le_bytes());
        }
        if let Bound::Included(v) | Bound::Excluded(v) = self.hi {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    /// Decode and validate at the trust boundary: the exact length implied by
    /// `n_eq` and the unbounded bits must match `buf.len()`, and `n_eq` must
    /// leave a slot for the range column — a malformed frame is rejected,
    /// never mis-decoded or allowed to index out of bounds. (The arity check
    /// against the actual column list stays with the caller, which holds it.)
    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < 2 {
            return Err("range descriptor shorter than 2 bytes".to_string());
        }
        let n_eq = buf[0] as usize;
        let flags = buf[1];
        if n_eq >= PK_LIST_MAX_COLS {
            return Err(format!(
                "range descriptor n_eq {n_eq} leaves no range column within \
                 the {PK_LIST_MAX_COLS}-column arity cap"));
        }
        let lo_unbounded = flags & LO_UNBOUNDED != 0;
        let hi_unbounded = flags & HI_UNBOUNDED != 0;
        let expected =
            2 + 16 * (n_eq + usize::from(!lo_unbounded) + usize::from(!hi_unbounded));
        if buf.len() != expected {
            return Err(format!(
                "range descriptor length {} != expected {expected}", buf.len()));
        }
        let mut off = 2;
        let mut next = || {
            let v = u128::from_le_bytes(buf[off..off + 16].try_into().unwrap());
            off += 16;
            v
        };
        let mut eq = [0u128; PK_LIST_MAX_COLS];
        for slot in eq.iter_mut().take(n_eq) { *slot = next(); }
        let lo = decode_bound(lo_unbounded, flags & LO_INCLUSIVE != 0, &mut next);
        let hi = decode_bound(hi_unbounded, flags & HI_INCLUSIVE != 0, &mut next);
        Ok(RangeDescriptor { eq, n_eq, lo, hi })
    }
}

fn bound_bits(b: &Bound<u128>, incl_bit: u8, unbounded_bit: u8) -> u8 {
    match b {
        Bound::Included(_) => incl_bit,
        Bound::Excluded(_) => 0,
        Bound::Unbounded   => unbounded_bit,
    }
}

fn decode_bound(
    unbounded: bool, inclusive: bool, next: &mut impl FnMut() -> u128,
) -> Bound<u128> {
    if unbounded {
        Bound::Unbounded
    } else if inclusive {
        Bound::Included(next())
    } else {
        Bound::Excluded(next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    #[test]
    fn roundtrips_every_shape() {
        let shapes: [(&[u128], Bound<u128>, Bound<u128>); 5] = [
            (&[], Excluded(10), Unbounded),                  // pure x > 10
            (&[], Unbounded, Included(20)),                  // pure x <= 20
            (&[7], Included(1), Excluded(u128::MAX)),        // a = 7 AND 1 <= b < MAX
            (&[1, 2, 3], Excluded(10), Excluded(50)),        // max arity, 82 bytes
            (&[], Unbounded, Unbounded),                     // saturated full scan
        ];
        for (eq, lo, hi) in shapes {
            let d = RangeDescriptor::new(eq, lo, hi);
            let bytes = d.encode();
            assert_eq!(RangeDescriptor::decode(&bytes), Ok(d), "{eq:?} {lo:?} {hi:?}");
        }
    }

    #[test]
    fn max_arity_encodes_to_82_bytes() {
        let d = RangeDescriptor::new(&[1, 2, 3], Excluded(10), Excluded(50));
        assert_eq!(d.encode().len(), 82);
    }

    #[test]
    fn decode_rejects_malformed() {
        // Too short for the fixed header.
        assert!(RangeDescriptor::decode(&[]).is_err());
        assert!(RangeDescriptor::decode(&[0]).is_err());
        // n_eq with no slot left for the range column.
        let mut d = RangeDescriptor::new(&[1, 2, 3], Unbounded, Unbounded).encode();
        d[0] = PK_LIST_MAX_COLS as u8;
        assert!(RangeDescriptor::decode(&d).is_err());
        // Length disagreeing with n_eq / unbounded bits — both directions.
        let good = RangeDescriptor::new(&[7], Included(1), Unbounded).encode();
        assert!(RangeDescriptor::decode(&good[..good.len() - 1]).is_err());
        let mut long = good.clone();
        long.push(0);
        assert!(RangeDescriptor::decode(&long).is_err());
    }
}
