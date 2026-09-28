//! [`PkSetGather`] — every live row of a strictly ascending OPK key list, in one
//! forward sweep of one cursor. Each key is settled against where the previous
//! one left the cursor. Each group is visited whole, at net weights in
//! (PK, payload) order, so a chunk boundary never splits one.

use gnitz_wire::PkKeys;

use super::{ReadCursor, SkeletonKeys};
use crate::schema::SchemaDescriptor;
use crate::storage::repr::batch::{Batch, Layout};

pub struct PkSetGather {
    cursor: ReadCursor,
    keys: PkKeys,
    /// Index of the next key, in keys — not bytes.
    next: usize,
}

impl PkSetGather {
    /// A gather of `keys` over `cursor`, from wherever it stands.
    pub(crate) fn new(cursor: ReadCursor, keys: PkKeys) -> Self {
        debug_assert_eq!(keys.stride(), cursor.schema.pk_stride());
        let mut gather = PkSetGather { cursor, keys, next: 0 };
        gather.position();
        gather
    }

    /// Start over on `keys` against the same snapshot.
    pub(crate) fn reload(&mut self, keys: PkKeys) {
        self.keys = keys;
        self.next = 0;
        self.position();
    }

    fn position(&mut self) {
        if let Some(first) = self.keys.iter().next() {
            self.cursor.advance_to(first);
        }
    }

    pub(crate) fn schema(&self) -> &SchemaDescriptor {
        &self.cursor.schema
    }

    fn remaining_keys(&self) -> usize {
        self.keys.len() - self.next
    }

    /// Call `f` on every row of each remaining key's group, in ascending
    /// (PK, payload) order, until it has run `max_rows` times; returns how often it ran.
    /// The budget is tested before each key and each group is visited whole, so the count
    /// can overshoot to `max_rows - 1 + |largest group|`. `0` means the list is exhausted.
    pub(crate) fn for_each_live_row(&mut self, max_rows: usize, mut f: impl FnMut(&ReadCursor)) -> usize {
        assert!(max_rows > 0, "for_each_live_row: max_rows must be positive");
        let stride = self.keys.stride();
        let mut visited = 0;
        while visited < max_rows && self.next * stride < self.keys.as_bytes().len() {
            let key = &self.keys.as_bytes()[self.next * stride..(self.next + 1) * stride];
            self.next += 1;
            if self.cursor.seek_pk_group_ascending(key) {
                self.cursor.for_each_pk_group_row(key, |c| {
                    visited += 1;
                    f(c);
                });
            }
        }
        visited
    }

    /// The next chunk with skeleton rows split out, as [`ReadCursor::drain_live_chunk`].
    pub(crate) fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        if self.remaining_keys() == 0 {
            return None;
        }
        let mut out = Batch::with_capacity(&self.cursor.schema, self.remaining_keys().min(max_rows));
        let split = self.cursor.any_skeleton;
        let visited = self.for_each_live_row(max_rows, |c| {
            if split && c.current_is_skeleton() {
                skeletons.push(c.current_pk_bytes(), c.current_weight);
            } else {
                c.copy_current_row_into(&mut out, c.current_weight);
            }
        });
        out.certify_layout(Layout::Consolidated);
        (visited > 0).then_some(out)
    }

    /// The next non-empty chunk of source rows, or `None` once the key list is
    /// exhausted; `max_rows` as [`Self::for_each_live_row`].
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        let mut skeletons = SkeletonKeys::default();
        let chunk = self.drain_live_chunk(max_rows, &mut skeletons);
        skeletons.assert_none();
        chunk
    }
}
