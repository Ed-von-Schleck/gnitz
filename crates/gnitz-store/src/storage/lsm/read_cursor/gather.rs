//! [`PkSetGather`] — every live row of a listed set of PK groups, chunked by
//! rows.
//!
//! One walk shared by the two readers that address a store by an explicit key
//! list: the ad-hoc `pk IN (…)` scan and a capacity-bounded view's per-key
//! hydration seed. Both want the same thing — position on each key's group, copy
//! its live rows, stop on a row budget — and both want it in ascending OPK
//! order, which is what makes the walk one monotone forward sweep of the cursor
//! rather than a seek per key.

use super::super::batch::Batch;
use super::ReadCursor;
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

    /// Whether the range this gather opened over holds a skeleton shard — see
    /// [`ReadCursor::any_skeleton`]. A caller that cannot read a skeleton row
    /// asks here rather than of the store, so a key list that misses every
    /// skeleton shard still walks.
    pub(crate) fn any_skeleton(&self) -> bool {
        self.cursor.any_skeleton()
    }

    /// Consume the gather and hand back the cursor it opened and the key list it
    /// walks — the same buffer `open` was given. By value, so a caller wanting
    /// only the keys drops the cursor's merge tree here rather than holding it
    /// alive across whatever it does with them.
    pub fn into_parts(self) -> (ReadCursor, Vec<u8>) {
        (self.cursor, self.keys)
    }

    /// The next non-empty chunk of source rows, or `None` once the key list is
    /// exhausted.
    ///
    /// A returned batch always carries rows: the fill loop stops either on the row
    /// budget — which is `> 0`, so it was reached — or with the key list drained,
    /// and the latter is reported as `None`. So a window of keys this store holds
    /// no live row for is skipped rather than surfacing as an empty chunk.
    ///
    /// `max_rows` is tested before each key and then that key's whole group is
    /// drained, so a chunk can overshoot to `max_rows - 1 + |largest group|`.
    pub fn next_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        debug_assert!(max_rows > 0, "a zero row budget would never consume a key");
        let schema = self.cursor.schema;
        let stride = schema.pk_stride();
        let total = self.keys.len() / stride;
        if self.next >= total {
            return None;
        }
        let cap = (total - self.next).min(max_rows);
        let mut out = Batch::with_capacity(&schema, cap);
        while self.next < total && out.count < max_rows {
            let off = self.next * stride;
            let key = &self.keys[off..off + stride];
            self.next += 1;
            // `next` only rises, across chunks too, so the whole list is one
            // ascending sweep; an absent key copies nothing.
            if self.cursor.seek_pk_group_ascending(key) {
                self.cursor.copy_positioned_pk_group_into(key, &mut out);
            }
        }
        (out.count > 0).then_some(out)
    }
}
