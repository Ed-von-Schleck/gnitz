//! [`SourceCursor`] — a chunked source of batches, whichever cursor backs it —
//! and [`BoundedIndexCursor`], the walk of a secondary-index range that gathers
//! each entry's row.

use gnitz_wire::PkKeys;

use super::{PkSetGather, ReadCursor, SkeletonKeys};
use crate::repr::batch::Batch;
use crate::repr::merge::merge_consolidated;

/// A chunked source of `Batch`es: a whole cursor, an index range, or a key list.
// One per read, held on the reader's stack; boxing `Full` or `PkSet` would cost
// every read an allocation. `Bounded` holds a cursor and a gather, so it stays boxed.
#[allow(clippy::large_enum_variant)]
pub enum SourceCursor {
    Full(ReadCursor),
    Bounded(Box<BoundedIndexCursor>),
    /// `pk IN (…)` gather over a listed key set.
    PkSet(PkSetGather),
}

impl SourceCursor {
    /// The next non-empty source chunk, or `None` once the source is exhausted.
    /// `PkSet` exceeds `max_rows` rather than split a PK group.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        SkeletonKeys::raw(|s| self.drain_live_chunk(max_rows, s))
    }

    /// [`Self::drain_chunk`] over a store that may hold skeleton rows: each one the
    /// chunk meets is replaced by its key's rows, which `hydrate` recomputes from
    /// the keys, ascending. A skeleton row counts once against `max_rows` however
    /// many rows it hydrates to.
    pub fn drain_hydrated(
        &mut self,
        max_rows: usize,
        hydrate: impl FnOnce(PkKeys) -> Result<Batch, String>,
    ) -> Result<Option<Batch>, String> {
        let mut skeletons = SkeletonKeys::default();
        let Some(live) = self.drain_live_chunk(max_rows, &mut skeletons) else {
            return Ok(None);
        };
        if skeletons.keys.is_empty() {
            return Ok(Some(live));
        }
        let keys = PkKeys::from_sorted(live.schema().pk_stride(), skeletons.keys);
        #[cfg(debug_assertions)]
        let asked = keys.clone();
        let hydrated = hydrate(keys)?;
        #[cfg(debug_assertions)]
        assert_hydration_matches(&hydrated, &asked, &skeletons.coarse);
        // Both consolidated and PK-disjoint.
        Ok(Some(if live.is_empty() {
            hydrated
        } else {
            merge_consolidated(&[hydrated.as_mem_batch(), live.as_mem_batch()], live.schema())
        }))
    }

    /// [`Self::drain_chunk`] with skeleton rows split out; `Some` and empty means every
    /// row of the chunk was a skeleton.
    fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_live_chunk(max_rows, skeletons),
            SourceCursor::Bounded(c) => c.drain_live_chunk(max_rows, skeletons),
            SourceCursor::PkSet(g) => g.drain_live_chunk(max_rows, skeletons),
        }
    }
}

/// Tripwire: a hydration's rows sum, per PK, to the coarse weight the skeleton row
/// at that key carried — by linearity of the PK projection — and name exactly the
/// keys asked.
#[cfg(debug_assertions)]
fn assert_hydration_matches(out: &Batch, keys: &PkKeys, coarse: &[i64]) {
    assert_eq!(keys.len(), coarse.len());
    let mut got = Vec::new();
    let mut i = 0;
    while i < out.len() {
        let j = crate::repr::pk_group_end(out, i, out.schema().pk_stride());
        got.push((out.get_pk_bytes(i), out.as_mem_batch().sum_weights(i, j)));
        i = j;
    }
    let want: Vec<_> = keys.iter().zip(coarse.iter().copied()).collect();
    assert_eq!(
        got, want,
        "hydration off its skeleton rows: (pk, summed weight) per key"
    );
}

/// A walk of one secondary-index key range, gathering each entry's row from the
/// relation it indexes, over one snapshot of each. The gather returns every row
/// under an entry's source PK, so the walk is exact only over rows unique on
/// their PK: a source PK then has one live row and at most one live index entry.
pub struct BoundedIndexCursor {
    /// Positioned on the index range and clamped at its end.
    idx: ReadCursor,
    src: PkSetGather,
    /// The width of an entry's leading span; the source PK follows it.
    span: usize,
    /// A refill's source PKs in index order, flat at `src`'s `pk_stride`.
    pks: Vec<u8>,
}

impl BoundedIndexCursor {
    /// A walk of `idx`, positioned on the index range and clamped at its end,
    /// gathering from `src`, a cursor over the relation `idx` indexes.
    pub fn new(idx: ReadCursor, src: ReadCursor) -> Self {
        let none = PkKeys::from_sorted(src.schema.pk_stride(), Vec::new());
        let span = idx.schema.pk_stride() - src.schema.pk_stride();
        BoundedIndexCursor {
            idx,
            src: PkSetGather::over(src, none),
            span,
            pks: Vec::new(),
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
            self.pks.extend_from_slice(&self.idx.current_pk_bytes()[self.span..]);
            self.idx.advance();
            taken += 1;
        }
        if taken == 0 {
            return false;
        }
        self.src
            .reload(PkKeys::from_keys(stride, self.pks.chunks_exact(stride)));
        true
    }
}
