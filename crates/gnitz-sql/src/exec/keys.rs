//! A client batch grouped by its key, in row order: what the fold combine merges, and
//! which VALUES row of a key an ON CONFLICT resolves.

use std::collections::hash_map::Entry;
use std::hash::Hash;

use gnitz_core::PkColumn;
use rustc_hash::FxHashMap;

/// A batch's rows split by whether each is its key's first.
pub(crate) struct Firsts {
    /// Every later row of a key as `(row, its key's first row)`, in row order; empty when
    /// no key repeats.
    pub(crate) absorbed: Vec<(u32, u32)>,
    /// The runs of first rows, ready for `retain_ranges`.
    pub(crate) keep: Vec<(usize, usize)>,
}

/// Each key's first row, read off `pks`.
pub(crate) fn firsts(pks: &PkColumn) -> Firsts {
    let (stride, region) = (pks.stride(), pks.region());
    // A narrow key groups by its image, a wide one by its bytes.
    match stride <= gnitz_wire::NARROW_PK_MAX_BYTES {
        true => firsts_of(region.chunks_exact(stride).map(gnitz_wire::widen_pk_be)),
        false => firsts_of(region.chunks_exact(stride)),
    }
}

fn firsts_of<K: Hash + Eq>(keys: impl ExactSizeIterator<Item = K>) -> Firsts {
    let n = keys.len();
    assert!(n <= u32::MAX as usize, "row count exceeds u32");
    let mut first_of: FxHashMap<K, u32> = FxHashMap::with_capacity_and_hasher(n, Default::default());
    let mut absorbed = Vec::new();
    let mut keep: Vec<(usize, usize)> = Vec::new();
    for (row, key) in keys.enumerate() {
        match first_of.entry(key) {
            Entry::Occupied(e) => absorbed.push((row as u32, *e.get())),
            Entry::Vacant(e) => {
                e.insert(row as u32);
                match keep.last_mut() {
                    Some((_, end)) if *end == row => *end += 1,
                    _ => keep.push((row, row + 1)),
                }
            }
        }
    }
    Firsts { absorbed, keep }
}
