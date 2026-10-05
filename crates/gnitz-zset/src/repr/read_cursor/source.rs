//! [`SourceCursor`] — a chunked source of batches, whichever cursor backs it —
//! and [`BoundedIndexCursor`], the walk of a secondary-index range that gathers
//! each entry's row.

use gnitz_wire::PkKeys;

use super::{PkSetGather, ReadCursor, SkeletonKeys};
use crate::repr::batch::Batch;
use crate::schema::key::{sort_indices, KeySpec};

/// A chunked source of `Batch`es: a whole cursor, an index range, or a key list.
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

    /// [`Self::drain_chunk`] with skeleton rows split out; `Some` and empty means every
    /// row of the chunk was a skeleton.
    pub fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_live_chunk(max_rows, skeletons),
            SourceCursor::Bounded(c) => c.drain_live_chunk(max_rows, skeletons),
            SourceCursor::PkSet(g) => g.drain_live_chunk(max_rows, skeletons),
        }
    }
}

/// A walk of one secondary-index key range, gathering each entry's row from the
/// relation it indexes, over one snapshot of each. The gather returns every row
/// under an entry's source PK, so the walk is exact only over rows unique on
/// their PK: a source PK then has one live row and at most one live index entry.
pub struct BoundedIndexCursor {
    /// Positioned on the index range and clamped at its end.
    idx: ReadCursor,
    src: PkSetGather,
    spec: KeySpec,
    /// A refill's source PKs in index order, flat at `src`'s `pk_stride`.
    pks: Vec<u8>,
    order: Vec<u32>,
}

impl BoundedIndexCursor {
    /// A walk of `idx`, positioned on the index range and clamped at its end,
    /// gathering from `src`, a cursor over the relation `spec` indexes.
    pub fn new(idx: ReadCursor, src: ReadCursor, spec: KeySpec) -> Self {
        let none = PkKeys::from_sorted(src.schema.pk_stride(), Vec::new());
        BoundedIndexCursor {
            idx,
            src: PkSetGather::new(src, none),
            spec,
            pks: Vec::new(),
            order: Vec::new(),
        }
    }

    fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        assert!(
            max_rows > 0,
            "BoundedIndexCursor::drain_live_chunk: max_rows must be positive"
        );
        loop {
            if let Some(chunk) = self.src.drain_live_chunk(max_rows, skeletons) {
                debug_assert!(
                    (1..chunk.len()).all(|i| chunk.get_pk_bytes(i - 1) != chunk.get_pk_bytes(i)),
                    "index owner holds two rows under one PK"
                );
                return Some(chunk);
            }
            if !self.refill(max_rows) {
                return None;
            }
        }
    }

    /// Reload the gather with the source PKs of the next `max_rows` index entries;
    /// `false` once the index range is spent.
    fn refill(&mut self, max_rows: usize) -> bool {
        let stride = self.src.schema().pk_stride();
        self.pks.clear();
        let mut taken = 0;
        while taken < max_rows && self.idx.valid {
            debug_assert!(self.idx.current_weight > 0, "an index entry at a non-positive weight");
            self.pks
                .extend_from_slice(self.spec.split_entry(self.idx.current_pk_bytes()).1);
            self.idx.advance();
            taken += 1;
        }
        if taken == 0 {
            return false;
        }
        sort_indices(&self.pks, stride, &mut self.order);
        let mut keys = Vec::with_capacity(self.pks.len());
        for &i in &self.order {
            keys.extend_from_slice(&self.pks[i as usize * stride..][..stride]);
        }
        self.src.reload(PkKeys::from_sorted(stride, keys));
        true
    }
}
