//! Group-column comparator and PK-ordered argsort used by the reduce operator.

use std::cmp::Ordering;

use crate::schema::key::{compare_pk_bytes, pk_width_dispatch};
use crate::schema::{key::PkSortKey, ColumnLocator};
use crate::storage::MemBatch;
use gnitz_expr::RowSource;

use super::super::group_key::GroupKeyCols;

/// Compare two rows by group columns through pre-resolved [`ColumnLocator`]s
/// (the reduce plan's baked `group_key.cols`). Generic over two
/// [`ColumnarSource`]s, so the group-walk boundary test and the ad-hoc fold's
/// bucket confirmation share this one body.
pub(super) fn compare_by_group_cols<A: RowSource, B: RowSource>(
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
    descs: &[ColumnLocator],
) -> Ordering {
    let a_null_word = src_a.get_null_word(row_a);
    let b_null_word = src_b.get_null_word(row_b);

    for loc in descs {
        // NULLs sort before non-NULLs (NULLS FIRST), so all NULLs on a column are
        // adjacent and form a single group. A non-nullable column never has the
        // bit set, so the gate is harmless there; a PK column answers `false` on
        // both sides and falls through to the value compare.
        match (loc.is_null_word(a_null_word), loc.is_null_word(b_null_word)) {
            (true, true) => continue,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (false, false) => {}
        }
        // Addressing and order rule both come off the locator: a PK column
        // compares its own OPK byte window, not the whole PK region — the latter
        // would split compound-PK groups that agree on the addressed column but
        // differ elsewhere in the key.
        let ord = loc.cmp_non_null(src_a, row_a, src_b, row_b);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Argsort `0..n` by a per-row key, materialised ONCE — `key` is never
/// re-invoked per comparison (nor a 32-byte `[u128; 2]` key re-copied, as
/// `sort_unstable_by_key` would).
///
/// Sorts `(key, index)` pairs rather than sorting indices against a side table:
/// the side table costs two random loads per comparison, where the pair carries
/// its key inline. The whole pair is the sort key, so rows sharing a key come
/// back in ascending source-index order — which is what makes a float SUM over
/// a group reproducible for a fixed access path.
fn argsort_by_key<K: Ord>(n: usize, key: impl Fn(usize) -> K) -> Vec<u32> {
    let mut pairs: Vec<(K, u32)> = (0..n).map(|i| (key(i), i as u32)).collect();
    pairs.sort_unstable();
    pairs.into_iter().map(|(_, i)| i).collect()
}

/// Argsort a delta batch into group order: the rows of one group land
/// contiguously, ordered by the plan's baked group key.
///
/// Sorting by the key rather than by `compare_by_group_cols` cannot fragment a
/// group — every row of one group yields the identical key. A non-injective key
/// can only *merge* two groups, and the emitted output PK is that same digest,
/// so the merge is already decided elsewhere.
///
/// The `u64` arm is exact, not a truncation: a canonical key over a ≤8-byte
/// column fits 64 bits (`pk_route_key` widens a ≤8-byte OPK window;
/// `payload_route_key`'s narrow arm is an 8-byte unsigned read XOR a sign bit
/// below `2^63`), and it halves the sorted payload for `GROUP BY <BIGINT>`.
pub(super) fn argsort_delta(mb: &MemBatch, keyer: &GroupKeyCols) -> Vec<u32> {
    let n = mb.count;
    if n <= 1 {
        return (0..n as u32).collect();
    }
    if keyer.canonical_col().is_some_and(|c| c.size() <= 8) {
        argsort_by_key(n, |i| keyer.key_row(mb, i) as u64)
    } else {
        argsort_by_key(n, |i| keyer.key_row(mb, i))
    }
}

/// Argsort into canonical PK order via a width-matched `PkSortKey`. The key is
/// the whole OPK image, so the compare is exact; rows sharing a PK come back in
/// ascending source-index order (the reduce groups them regardless).
fn sort_indices_keyed<K: PkSortKey>(mb: &MemBatch) -> Vec<u32> {
    argsort_by_key(mb.count, |i| K::from_opk(mb.get_pk_bytes(i)))
}

/// Argsort indices into canonical PK order (`compare_pk_bytes` order).
/// `pk_stride` selects the width-matched key — `u64`/`u128`/`[u128; 2]` for
/// strides ≤8/≤16/≤32 (the cutoffs are the key widths) — each the full OPK
/// image, so a plain key compare is exact for unsigned, signed, and compound
/// PKs alike. PKs too wide to pack (`> 32` B — exotic 3–5 wide-column
/// composites) byte-walk the OPK regions via `compare_pk_bytes`.
pub(super) fn argsort_pk_canonical(mb: &MemBatch) -> Vec<u32> {
    pk_width_dispatch!(mb.pk_stride as usize, |K| sort_indices_keyed::<K>(mb), {
        let mut idx: Vec<u32> = (0..mb.count as u32).collect();
        idx.sort_unstable_by(|&a, &b| compare_pk_bytes(mb.get_pk_bytes(a as usize), mb.get_pk_bytes(b as usize)));
        idx
    })
}

#[cfg(test)]
#[path = "tests/sort.rs"]
mod tests;
