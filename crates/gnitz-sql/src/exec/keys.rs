//! A client batch grouped by its key, in row order: what the fold combine merges, and
//! which VALUES row of a key an ON CONFLICT resolves.

use std::collections::hash_map::Entry;
use std::hash::Hash;

use rustc_hash::FxHashMap;

/// A batch's rows grouped by key, each key's group numbered by its first row.
pub(crate) struct Firsts {
    /// Each row's group: `g` for the key whose first row is the `g`-th first row.
    pub(crate) groups: Vec<u32>,
    /// The runs of first rows, ready for `retain_ranges`.
    pub(crate) keep: Vec<(usize, usize)>,
}

impl Firsts {
    /// Some key is on more than one row.
    pub(crate) fn repeats(&self) -> bool {
        self.keep != [(0, self.groups.len())] && !self.groups.is_empty()
    }
}

/// Each key's first row, read off `region`, keys of `stride` OPK bytes back to back.
pub(crate) fn firsts(region: &[u8], stride: usize) -> Firsts {
    // A narrow key groups by its image, a wide one by its bytes.
    match stride <= gnitz_wire::NARROW_PK_MAX_BYTES {
        true => firsts_of(region.chunks_exact(stride).map(gnitz_wire::widen_pk_be)),
        false => firsts_of(region.chunks_exact(stride)),
    }
}

fn firsts_of<K: Hash + Eq>(keys: impl ExactSizeIterator<Item = K>) -> Firsts {
    let n = keys.len();
    assert!(n <= u32::MAX as usize, "row count exceeds u32");
    let mut group_of: FxHashMap<K, u32> = FxHashMap::with_capacity_and_hasher(n, Default::default());
    let mut groups = Vec::with_capacity(n);
    let mut keep: Vec<(usize, usize)> = Vec::new();
    for (row, key) in keys.enumerate() {
        let next = group_of.len() as u32;
        match group_of.entry(key) {
            Entry::Occupied(e) => groups.push(*e.get()),
            Entry::Vacant(e) => {
                groups.push(*e.insert(next));
                match keep.last_mut() {
                    Some((_, end)) if *end == row => *end += 1,
                    _ => keep.push((row, row + 1)),
                }
            }
        }
    }
    Firsts { groups, keep }
}
