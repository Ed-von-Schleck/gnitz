//! [`PkSetGather`] — every live row of a strictly ascending OPK key list, in one
//! forward sweep of one cursor. Each key is settled against where the previous
//! one left the cursor. Each group is visited whole, at net weights in
//! (PK, payload) order, so a chunk boundary never splits one.
//!
//! A key is a whole PK, or the same leading columns of one: then its group is
//! every row whose PK it prefixes.

use gnitz_wire::PkKeys;
use std::ops::ControlFlow;

use super::{ReadCursor, SkeletonKeys};
use crate::repr::batch::Batch;
use crate::repr::scatter::gather_rows;
use crate::schema::{project_schema, ColumnLocator, SchemaDescriptor, SchemaFacts};

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

    /// Start over on `keys` against the same snapshot.
    pub(crate) fn reload(&mut self, keys: PkKeys) {
        self.keys = keys;
        self.next = 0;
        if let Some(first) = self.keys.iter().next() {
            self.cursor.advance_to(first);
        }
    }

    pub(super) fn schema(&self) -> &SchemaDescriptor {
        &self.cursor.schema
    }

    fn remaining_keys(&self) -> usize {
        self.keys.len() - self.next
    }

    /// Call `f` on every row of each remaining key's group, in ascending
    /// (PK, payload) order, until it has run `max_rows` times; returns how often it ran.
    /// The budget is tested before each key and each group is visited whole, so the count
    /// can overshoot to `max_rows - 1 + |largest group|`. `0` means the list is exhausted.
    fn for_each_live_row(&mut self, max_rows: usize, mut f: impl FnMut(&ReadCursor)) -> usize {
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

    /// Call `f` on up to `per_key` positive-weight rows of each remaining key's
    /// group, in key order, and on at most `total` rows in all.
    pub fn for_each_positive_capped(&mut self, per_key: usize, total: usize, mut f: impl FnMut(&ReadCursor)) {
        let mut room = total;
        for key in self.keys.iter().skip(self.next) {
            let mut left = per_key.min(room);
            if left == 0 {
                break;
            }
            if !self.cursor.seek_pk_group_ascending(key) {
                continue;
            }
            self.cursor.walk_positive_with_prefix_until(key, |c| {
                f(c);
                left -= 1;
                room -= 1;
                match left {
                    0 => ControlFlow::Break(()),
                    _ => ControlFlow::Continue(()),
                }
            });
        }
        self.next = self.keys.len();
    }

    /// The next chunk with skeleton rows split out, as [`ReadCursor::drain_live_chunk`].
    pub(super) fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        if self.remaining_keys() == 0 {
            return None;
        }
        let split = self.cursor.any_skeleton;
        let mut picks: Vec<(u32, u32, i64)> = Vec::with_capacity(self.remaining_keys().min(max_rows));
        let visited = self.for_each_live_row(max_rows, |c| {
            if split && c.current_is_skeleton() {
                skeletons.push(c.current_pk_bytes(), c.current_weight);
            } else {
                let (src, row) = c.current_position();
                picks.push((src as u32, row as u32, c.current_weight));
            }
        });
        let mut out = gather_rows(&self.cursor.sources, &self.cursor.schema, &picks);
        out.certify_consolidated();
        (visited > 0).then_some(out)
    }

    /// Every remaining live row at weight 1, in the [`project_schema`] layout of
    /// `cols`: this source's PK, then the payload columns `cols` names.
    /// `Err` where `project_schema` refuses `cols`.
    pub fn project_live(&mut self, cols: &[u32]) -> Result<Batch, String> {
        let schema = self.cursor.schema;
        let out_schema = project_schema(&schema, cols)?;
        let locs: Vec<ColumnLocator> = cols.iter().map(|&c| schema.locate(c as usize)).collect();
        let mut out = Batch::with_capacity(&out_schema, self.remaining_keys());
        self.for_each_live_row(usize::MAX, |c| {
            let (src, row) = c.current_row_source();
            out.begin_row(c.current_pk_bytes(), 1);
            out.append_cells_from(0, &locs, src, row);
            out.commit_row();
        });
        Ok(out)
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
