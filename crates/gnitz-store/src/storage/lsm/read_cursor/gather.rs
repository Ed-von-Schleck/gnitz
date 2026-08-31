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
    /// The keys' OPK images, `stride` bytes each, concatenated. **Ascending**:
    /// OPK order is typed PK order, so a sorted list makes the walk monotone
    /// regardless of the order the caller received the keys in.
    keys: Vec<u8>,
    stride: usize,
    /// Index of the next key, in keys — not bytes.
    next: usize,
    src_schema: SchemaDescriptor,
}

/// The key range a flat **ascending** OPK key list spans — its first and last
/// key — or `None` for an empty list. `stride` is the schema's `pk_stride`. A
/// list spread across the whole key space degenerates to the whole store, which
/// is the case a whole-store open was right for anyway.
pub(crate) fn key_list_range(keys: &[u8], stride: usize) -> Option<(&[u8], &[u8])> {
    let first = keys.chunks_exact(stride).next()?;
    Some((first, &keys[keys.len() - stride..]))
}

impl PkSetGather {
    /// Gather `keys` out of a store, opening over exactly the range they span —
    /// `open` is that store's ranged cursor open. The bound comes from the list
    /// this gather already owns, so no caller re-derives it.
    pub fn open(
        keys: Vec<u8>,
        src_schema: SchemaDescriptor,
        open: impl FnOnce(&[u8], Option<&[u8]>) -> ReadCursor,
    ) -> Self {
        let stride = src_schema.pk_stride() as usize;
        let cursor = match key_list_range(&keys, stride) {
            Some((lo, hi)) => open(lo, Some(hi)),
            // No key to gather, so nothing to open over.
            None => super::empty(src_schema),
        };
        Self::new(cursor, keys, src_schema)
    }

    /// `keys` is the flat concatenation of the OPK images, each exactly
    /// `src_schema.pk_stride()` bytes, in ascending order.
    pub fn new(cursor: ReadCursor, keys: Vec<u8>, src_schema: SchemaDescriptor) -> Self {
        let stride = src_schema.pk_stride() as usize;
        debug_assert!(
            stride > 0 && keys.len().is_multiple_of(stride),
            "key buffer is not a whole key list"
        );
        debug_assert!(
            keys.chunks_exact(stride)
                .zip(keys.chunks_exact(stride).skip(1))
                .all(|(a, b)| a <= b),
            "PkSetGather keys must be sorted ascending",
        );
        PkSetGather {
            cursor,
            keys,
            stride,
            next: 0,
            src_schema,
        }
    }

    /// Consume the gather and hand back the key list it walked — the same buffer
    /// `new` was given. By value, not by reference: a borrow would keep the
    /// cursor's merge tree alive across whatever the caller does with the keys.
    pub fn into_keys(self) -> Vec<u8> {
        self.keys
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
        let total = self.keys.len() / self.stride;
        if self.next >= total {
            return None;
        }
        let cap = (total - self.next).min(max_rows);
        let mut out = Batch::with_capacity(self.src_schema, cap);
        while self.next < total && out.count < max_rows {
            let off = self.next * self.stride;
            let key = &self.keys[off..off + self.stride];
            self.next += 1;
            // An absent key, or one this worker holds no row for, copies nothing.
            self.cursor.copy_live_pk_group_into(key, &mut out);
        }
        (out.count > 0).then_some(out)
    }
}
