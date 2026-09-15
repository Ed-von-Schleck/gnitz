//! [`PkSetGather`] — every live row of a sorted list of PK groups, one forward
//! sweep of one cursor.
//!
//! The keys are visited in ascending OPK order, which is typed PK order, so the
//! walk never seeks backward within a list: each key is settled against where the
//! previous one left the cursor, and a key the store holds no live row for costs
//! a comparison. Each group is visited whole, at net positive weights in (PK,
//! payload) order, so a chunk boundary never splits one.

use super::super::batch::{Batch, Layout};
use super::{ReadCursor, SkeletonKeys};
use crate::schema::SchemaDescriptor;

pub struct PkSetGather {
    cursor: ReadCursor,
    /// The keys' OPK images, one `pk_stride` each, concatenated and strictly
    /// ascending — OPK order is typed PK order, so sorting the images makes the
    /// walk one forward sweep whatever order the caller received them in.
    keys: Vec<u8>,
    /// Index of the next key, in keys — not bytes.
    next: usize,
}

/// The key range a flat **ascending** OPK key list spans — its first and last
/// key — or `None` for an empty list. `stride` is the schema's `pk_stride`. A
/// list spread across the whole key space degenerates to the whole store, which
/// is the case a whole-store open was right for anyway.
fn key_list_range(keys: &[u8], stride: usize) -> Option<(&[u8], &[u8])> {
    let first = keys.chunks_exact(stride).next()?;
    Some((first, &keys[keys.len() - stride..]))
}

impl PkSetGather {
    /// Gather `keys` out of a store, opening over exactly the range they span —
    /// `open` is that store's ranged cursor open, and the bound comes from the
    /// list this gather already owns. `src_schema` is a parameter rather than the
    /// cursor's because the empty-key arm has no cursor to read it off.
    pub fn open(
        keys: Vec<u8>,
        src_schema: SchemaDescriptor,
        open: impl FnOnce(&[u8], Option<&[u8]>) -> ReadCursor,
    ) -> Self {
        let stride = src_schema.pk_stride();
        debug_assert!(
            stride > 0 && keys.len().is_multiple_of(stride),
            "key buffer is not a whole key list"
        );
        let cursor = match key_list_range(&keys, stride) {
            Some((lo, hi)) => open(lo, Some(hi)),
            // No key to gather, so nothing to open over.
            None => super::empty(src_schema),
        };
        PkSetGather { cursor, keys, next: 0 }
    }

    /// A gather over `cursor` holding no keys yet, for a walker that keeps one snapshot
    /// across several key lists; see [`Self::reload`].
    pub(crate) fn over(cursor: ReadCursor) -> Self {
        PkSetGather { cursor, keys: Vec::new(), next: 0 }
    }

    /// Replace the key list (flat, strictly ascending) and end the sweep, so its first key
    /// may sort below the last one visited. Returns the previous list's buffer.
    pub(crate) fn reload(&mut self, keys: Vec<u8>) -> Vec<u8> {
        self.next = 0;
        self.cursor.sweep_open = false;
        std::mem::replace(&mut self.keys, keys)
    }

    pub(crate) fn schema(&self) -> &SchemaDescriptor {
        &self.cursor.schema
    }

    /// Keys not yet visited.
    pub(crate) fn remaining_keys(&self) -> usize {
        self.keys.len() / self.cursor.schema.pk_stride() - self.next
    }

    /// Call `f` on every positive-weight row of each remaining key's group, in ascending
    /// (PK, payload) order, until it has run `max_rows` times; returns how often it ran.
    /// The budget is tested before each key and each group is visited whole, so the count
    /// can overshoot to `max_rows - 1 + |largest group|`. `0` means the list is exhausted.
    pub fn for_each_live_row(&mut self, max_rows: usize, mut f: impl FnMut(&ReadCursor)) -> usize {
        debug_assert!(max_rows > 0, "a zero row budget would never consume a key");
        let stride = self.cursor.schema.pk_stride();
        let mut visited = 0;
        while visited < max_rows && self.next * stride < self.keys.len() {
            let key = &self.keys[self.next * stride..(self.next + 1) * stride];
            self.next += 1;
            if self.cursor.seek_pk_group_ascending(key) {
                // The weight gate is per row, not a presence test: a retracted member
                // an uncompacted source still holds can head the group by payload
                // order, and rejecting the key on it would drop the live rows behind it.
                self.cursor.for_each_pk_group_row(key, |c| {
                    if c.current_weight > 0 {
                        visited += 1;
                        f(c);
                    }
                });
            }
        }
        visited
    }

    /// The next chunk with skeleton rows split out, as [`ReadCursor::drain_live_chunk`].
    pub(crate) fn next_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        let mut out = Batch::with_capacity(&self.cursor.schema, self.remaining_keys().min(max_rows));
        let split = self.cursor.any_skeleton();
        let visited = self.for_each_live_row(max_rows, |c| {
            if split && c.current_is_skeleton() {
                skeletons.push(c.current_pk_bytes(), c.current_weight);
            } else {
                c.copy_current_row_into(&mut out, c.current_weight);
            }
        });
        // Ascending keys, each group in (PK, payload) order at positive net weights.
        // Certifying it spares an ingest tail an O(chunk log chunk) re-sort.
        out.certify_layout(Layout::Consolidated);
        (visited > 0).then_some(out)
    }

    /// The next non-empty chunk of source rows, or `None` once the key list is
    /// exhausted; `max_rows` as [`Self::for_each_live_row`].
    pub fn next_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        let mut skeletons = SkeletonKeys::default();
        let chunk = self.next_live_chunk(max_rows, &mut skeletons);
        skeletons.assert_none();
        chunk
    }
}
