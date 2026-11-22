//! [`PkSetGather`] — every live row of a strictly ascending OPK key list, in one
//! forward sweep of one cursor. Each key is settled against where the previous
//! one left the cursor. Each group is visited whole, at net weights in
//! (PK, payload) order, so a chunk boundary never splits one.
//!
//! A key is a whole PK, or the same leading columns of one: then its group is
//! every row whose PK it prefixes.

use gnitz_wire::PkKeys;
use std::ops::ControlFlow;

use super::{ReadCursor, SkeletonKeys, hydrate_skeletons};
use gnitz_zset::repr::Batch;
use gnitz_zset::repr::{gather_rows, with_picks};

pub struct PkSetGather {
    cursor: ReadCursor,
    keys: PkKeys,
    /// Index of the next key, in keys — not bytes.
    next: usize,
}

impl PkSetGather {
    /// A gather of `keys` over `cursor`, which stands at or below the first.
    pub fn over(cursor: ReadCursor, keys: PkKeys) -> Self {
        debug_assert!(keys.stride() <= cursor.schema.pk_stride());
        PkSetGather { cursor, keys, next: 0 }
    }

    /// The whole key list, the keys already gathered included.
    pub fn into_keys(self) -> PkKeys {
        self.keys
    }

    /// Start over on `keys` against the same snapshot.
    pub fn reload(&mut self, keys: PkKeys) {
        self.keys = keys;
        self.next = 0;
        if let Some(first) = self.keys.iter().next() {
            self.cursor.advance_to(first);
        }
    }

    /// [`ReadCursor::skip_payload`] on the cursor the keys are gathered from.
    pub fn skip_payload(&mut self, skip: u64) {
        self.cursor.skip_payload(skip);
    }

    /// Call `f` on the positive-weight rows of each remaining key's group, in
    /// key order, and on at most `total` rows in all.
    pub fn for_each_positive_capped(&mut self, total: usize, mut f: impl FnMut(&ReadCursor)) {
        let mut room = total;
        for key in self.keys.iter().skip(self.next) {
            if room == 0 {
                break;
            }
            if !self.cursor.seek_pk_group_ascending(key) {
                continue;
            }
            self.cursor.walk_until(
                |pk| pk.starts_with(key),
                |c| {
                    if c.current_weight() <= 0 {
                        return ControlFlow::Continue(());
                    }
                    f(c);
                    room -= 1;
                    match room {
                        0 => ControlFlow::Break(()),
                        _ => ControlFlow::Continue(()),
                    }
                },
            );
        }
        self.next = self.keys.len();
    }

    /// The next chunk with skeleton rows split out, as
    /// [`ReadCursor::drain_live_chunk`]: every row of each remaining key's
    /// group, in ascending (PK, payload) order, until `max_rows` are visited.
    /// The budget is tested before each key and each group is visited whole,
    /// so a chunk can overshoot to `max_rows - 1 + |largest group|`; `None`
    /// once no key is left.
    pub(super) fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        assert!(max_rows > 0, "drain_live_chunk: max_rows must be positive");
        let remaining = self.keys.len() - self.next;
        if remaining == 0 {
            return None;
        }
        with_picks(|picks| {
            picks.reserve(remaining.min(max_rows));
            for key in self.keys.iter().skip(self.next) {
                if picks.len() >= max_rows {
                    break;
                }
                self.next += 1;
                if self.cursor.seek_pk_group_ascending(key) {
                    self.cursor
                        .for_each_pk_group_row(key, |c| picks.push(c.current_position()));
                }
            }
            (!picks.is_empty()).then(|| {
                let c = &self.cursor;
                if c.merge.coarsens() {
                    skeletons.split_from(&c.sources, picks);
                }
                let mut out = gather_rows(&c.sources, &c.schema, picks, c.skip_payload);
                out.certify_consolidated();
                out
            })
        })
    }

    /// [`Self::drain_chunk`] over a store that may hold skeleton rows, each one
    /// replaced by its key's rows as [`ReadCursor::drain_hydrated`]; `Some` and
    /// empty where every row visited was a skeleton and hydrated to nothing.
    pub fn drain_hydrated(
        &mut self,
        max_rows: usize,
        hydrate: &mut impl FnMut(PkKeys) -> Result<Batch, String>,
    ) -> Result<Option<Batch>, String> {
        let mut skeletons = SkeletonKeys::default();
        let Some(live) = self.drain_live_chunk(max_rows, &mut skeletons) else {
            return Ok(None);
        };
        hydrate_skeletons(live, skeletons, hydrate).map(Some)
    }

    /// The next non-empty chunk of source rows, or `None` once the key list is
    /// exhausted; `max_rows` as [`Self::drain_live_chunk`] reads it.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        SkeletonKeys::raw(|s| self.drain_live_chunk(max_rows, s))
    }
}
