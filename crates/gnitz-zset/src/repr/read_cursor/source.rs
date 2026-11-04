//! [`SourceCursor`] — a chunked source of batches, whichever cursor backs it —
//! [`BoundedIndexCursor`], the walk of a secondary-index range that gathers
//! each entry's row, and [`KeyGather`], the rows of a key list.

use gnitz_expr::RowFilter;
use gnitz_wire::PkKeys;

use super::{PkSetGather, ReadCursor, SkeletonKeys};
use crate::repr::batch::Batch;
use crate::repr::merge::merge_consolidated;
use crate::repr::run::StoredRow;
use crate::schema::SchemaDescriptor;

/// A chunked source of `Batch`es: a whole cursor, an index range, or a key list.
// One per read, held on the reader's stack; boxing `Full` or `PkSet` would cost
// every read an allocation. `Bounded` holds a cursor and a gather, so it stays boxed.
#[allow(clippy::large_enum_variant)]
pub enum SourceCursor<'a> {
    Full(ReadCursor),
    Bounded(Box<BoundedIndexCursor<'a>>),
    /// `pk IN (…)` gather over a listed key set.
    PkSet(KeyGather<'a>),
}

impl SourceCursor<'_> {
    /// The next non-empty source chunk, or `None` once the source is exhausted.
    /// `PkSet` exceeds `max_rows` rather than split a PK group.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        SkeletonKeys::raw(|s| self.drain_live_chunk(max_rows, s, &mut RowFilter::default()))
    }

    /// [`Self::drain_chunk`] over a store that may hold skeleton rows: each one the
    /// chunk meets is replaced by its key's rows, which `hydrate` recomputes from
    /// the keys, ascending. A skeleton row counts once against `max_rows` however
    /// many rows it hydrates to. A whole cursor leaves out the rows of its
    /// in-memory runs that `filter` drops; every other row stays, hydrated ones
    /// included, for the caller to filter.
    pub fn drain_hydrated(
        &mut self,
        max_rows: usize,
        filter: &mut RowFilter,
        hydrate: impl FnOnce(PkKeys) -> Result<Batch, String>,
    ) -> Result<Option<Batch>, String> {
        let mut skeletons = SkeletonKeys::default();
        let Some(live) = self.drain_live_chunk(max_rows, &mut skeletons, filter) else {
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
    fn drain_live_chunk(
        &mut self,
        max_rows: usize,
        skeletons: &mut SkeletonKeys,
        filter: &mut RowFilter,
    ) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_live_chunk(max_rows, skeletons, filter),
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
pub struct BoundedIndexCursor<'a> {
    /// Positioned on the index range and clamped at its end.
    idx: ReadCursor,
    src: KeyGather<'a>,
    /// The width of an entry's leading span; the source PK follows it.
    span: usize,
    /// A refill's source PKs in index order, flat at `src`'s `pk_stride`.
    pks: Vec<u8>,
}

impl<'a> BoundedIndexCursor<'a> {
    /// A walk of `idx`, positioned on the index range and clamped at its end,
    /// gathering from `src`, a cursor over the relation `idx` indexes.
    pub fn new(idx: ReadCursor, src: ReadCursor) -> Self {
        let none = PkKeys::from_sorted(src.schema.pk_stride(), Vec::new());
        Self::over(idx, KeyGather::Merge(PkSetGather::over(src, none)))
    }

    /// [`Self::new`], probing `store` — the relation `idx` indexes, in the
    /// layout `schema` — for each entry's row.
    pub fn new_unique(idx: ReadCursor, store: &'a dyn UniqueRows, schema: SchemaDescriptor) -> Self {
        let none = PkKeys::from_sorted(schema.pk_stride(), Vec::new());
        Self::over(idx, KeyGather::unique(store, schema, none))
    }

    fn over(idx: ReadCursor, src: KeyGather<'a>) -> Self {
        let span = idx.schema.pk_stride() - src.schema().pk_stride();
        BoundedIndexCursor { idx, src, span, pks: Vec::new() }
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

/// The rows of a strictly ascending list of keys: gathered off a cursor, or
/// probed key by key from a store whose every key nets 0 or 1.
// Held inline by `SourceCursor::PkSet` for the reason that holds `PkSet` inline.
#[allow(clippy::large_enum_variant)]
pub enum KeyGather<'a> {
    Merge(PkSetGather),
    Unique(UniqueGather<'a>),
}

impl<'a> KeyGather<'a> {
    /// A gather of `keys`, whole PKs of `schema`, probed from `store`.
    pub fn unique(store: &'a dyn UniqueRows, schema: SchemaDescriptor, keys: PkKeys) -> Self {
        debug_assert_eq!(keys.stride(), schema.pk_stride());
        KeyGather::Unique(UniqueGather { store, schema, keys, next: 0 })
    }

    fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        match self {
            KeyGather::Merge(g) => g.drain_live_chunk(max_rows, skeletons),
            // A store of one row per key holds no skeleton rows.
            KeyGather::Unique(g) => g.drain_chunk(max_rows),
        }
    }

    fn reload(&mut self, keys: PkKeys) {
        match self {
            KeyGather::Merge(g) => g.reload(keys),
            KeyGather::Unique(g) => (g.keys, g.next) = (keys, 0),
        }
    }

    fn schema(&self) -> &SchemaDescriptor {
        match self {
            KeyGather::Merge(g) => g.schema(),
            KeyGather::Unique(g) => &g.schema,
        }
    }
}

/// A store whose every key nets 0 or 1, and so holds at most one live row per
/// key — a base table's, under its unique-PK rule.
pub trait UniqueRows {
    /// The live row at whole OPK `key`, `None` when the key nets 0.
    fn unique_live_row_at(&self, key: &[u8]) -> Option<StoredRow>;
}

/// Every live row of a strictly ascending list of whole keys, one
/// [`UniqueRows`] probe per key.
pub struct UniqueGather<'a> {
    store: &'a dyn UniqueRows,
    schema: SchemaDescriptor,
    keys: PkKeys,
    /// Index of the next key, in keys — not bytes.
    next: usize,
}

impl UniqueGather<'_> {
    /// The live rows of the next keys, at most `max_rows` of them, at weight 1;
    /// `None` once the list is exhausted.
    fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        let mut rows = Vec::new();
        for key in self.keys.iter().skip(self.next) {
            if rows.len() >= max_rows {
                break;
            }
            self.next += 1;
            rows.extend(self.store.unique_live_row_at(key));
        }
        if rows.is_empty() {
            return None;
        }
        let mut out = Batch::empty_with_schema(&self.schema);
        out.append_stored(&rows, 1);
        out.certify_consolidated();
        Some(out)
    }
}
