//! Index-indirected row gather: walk one cursor's key range, resolve each collected
//! source PK against a second cursor. An index owner is a base table, so a source PK
//! has one live row and at most one live index entry.

use super::batch::Batch;
use super::read_cursor::{PkSetGather, ReadCursor, SkeletonKeys};
use crate::schema::key::sort_indices;
use crate::schema::IndexKeySpec;

/// A chunked walk of one secondary-index key range, gathering each in-range live
/// entry's source row from the base table, over one snapshot of each.
pub struct BoundedIndexCursor {
    /// Positioned on the index range and clamped at its end.
    idx: ReadCursor,
    src: PkSetGather,
    /// The key buffer passed back and forth with `src`, so a chunk allocates none.
    spare: Vec<u8>,
    /// The chunk's collected source PKs, flat at `src`'s `pk_stride`.
    pks: Vec<u8>,
    order: Vec<u32>,
    spec: IndexKeySpec,
}

impl BoundedIndexCursor {
    /// `pk_capacity` pre-sizes the per-chunk PK scratch, in keys.
    pub(crate) fn new(idx: ReadCursor, src: ReadCursor, spec: IndexKeySpec, pk_capacity: usize) -> Self {
        let stride = src.schema.pk_stride();
        BoundedIndexCursor {
            idx,
            src: PkSetGather::over(src),
            spare: Vec::new(),
            pks: Vec::with_capacity(pk_capacity * stride),
            order: Vec::with_capacity(pk_capacity),
            spec,
        }
    }

    /// The next non-empty chunk of up to `n` in-range rows; `None` once the range is
    /// exhausted.
    pub(crate) fn drain_chunk(&mut self, n: usize) -> Option<Batch> {
        assert!(n > 0, "BoundedIndexCursor::drain_chunk: n must be positive");
        let stride = self.src.schema().pk_stride();
        loop {
            self.pks.clear();
            let mut collected = 0;
            while self.idx.valid && collected < n {
                if self.idx.current_weight > 0 {
                    self.pks
                        .extend_from_slice(self.spec.split_entry(self.idx.current_pk_bytes()).1);
                    collected += 1;
                }
                self.idx.advance();
            }
            if collected == 0 {
                return None;
            }
            sort_indices(&self.pks, stride, &mut self.order);
            let mut keys = std::mem::take(&mut self.spare);
            keys.clear();
            let mut prev: &[u8] = &[];
            for &i in &self.order {
                let pk = &self.pks[i as usize * stride..(i as usize + 1) * stride];
                if pk != prev {
                    keys.extend_from_slice(pk);
                    prev = pk;
                }
            }
            self.spare = self.src.reload(keys);
            if let Some(chunk) = self.src.next_chunk(usize::MAX) {
                return Some(chunk);
            }
        }
    }
}

/// A chunked source of `Batch`es over one relation, in every shape a bound can
/// take.
pub enum SourceCursor {
    Full(Box<ReadCursor>),
    Bounded(Box<BoundedIndexCursor>),
    /// `pk IN (…)` gather over a listed key set.
    PkSet(Box<PkSetGather>),
}

impl SourceCursor {
    /// The next non-empty source chunk, or `None` once the source is exhausted.
    /// `PkSet` exceeds `max_rows` rather than split a PK group.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        let mut skeletons = SkeletonKeys::default();
        let chunk = self.drain_live_chunk(max_rows, &mut skeletons);
        skeletons.assert_none();
        chunk
    }

    /// [`Self::drain_chunk`] with skeleton rows split out; see [`ReadCursor::drain_live_chunk`].
    /// `Some` and empty means every row of the chunk was a skeleton.
    pub(crate) fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_live_chunk(max_rows, skeletons),
            SourceCursor::Bounded(c) => c.drain_chunk(max_rows),
            SourceCursor::PkSet(g) => g.next_live_chunk(max_rows, skeletons),
        }
    }
}
