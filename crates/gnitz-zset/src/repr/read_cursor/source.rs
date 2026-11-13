//! [`SourceCursor`] — a chunked source of batches, whichever cursor backs it —
//! [`KeyGather`], the rows of a key list, and [`IndexWalk`], the walk of a
//! secondary-index range that feeds a gather each entry's key.

use gnitz_expr::RowFilter;
use gnitz_wire::PkKeys;

use super::{PkSetGather, ReadCursor, SkeletonKeys};
use crate::repr::batch::Batch;
use crate::repr::merge::merge_consolidated;
use crate::repr::run::StoredRow;
use crate::schema::{project_schema, SchemaDescriptor};
use gnitz_expr::ColumnLocator;

/// A chunked source of `Batch`es: a whole cursor, or the rows of a key list.
// An index walk holds a cursor beside the gather's, so it is boxed.
pub enum SourceCursor<'a> {
    Full(ReadCursor),
    /// The rows of a key list, resolved by `gather`: listed whole (`pk IN (…)`)
    /// with no `walk`, or fed a refill at a time by an index range's `walk`.
    Keyed {
        gather: KeyGather<'a>,
        walk: Option<Box<IndexWalk>>,
    },
}

impl<'a> SourceCursor<'a> {
    /// The walk of `idx` — a secondary-index range, positioned on it and
    /// clamped at its end — gathering each entry's row through `gather`, which
    /// holds no keys yet.
    pub fn index_walk(idx: ReadCursor, gather: KeyGather<'a>) -> Self {
        let walk = IndexWalk { idx, pks: Vec::new() };
        SourceCursor::Keyed { gather, walk: Some(Box::new(walk)) }
    }
}

impl SourceCursor<'_> {
    /// [`ReadCursor::skip_payload`] on whichever cursor or probe the rows are
    /// drained from.
    pub fn skip_payload(&mut self, skip: u64) {
        match self {
            SourceCursor::Full(c) => c.skip_payload(skip),
            SourceCursor::Keyed { gather, .. } => gather.skip_payload(skip),
        }
    }

    /// The next non-empty source chunk, or `None` once the source is exhausted.
    /// A key list's merge gather exceeds `max_rows` rather than split a PK group.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        SkeletonKeys::raw(|s| {
            self.drain_live_chunk(max_rows, s, &mut RowFilter::default())
                .map(|(b, _)| b)
        })
    }

    /// [`Self::drain_chunk`] over a store that may hold skeleton rows: each one the
    /// chunk meets is replaced by its key's rows, which `hydrate` recomputes from
    /// the keys, ascending. A skeleton row counts once against `max_rows` however
    /// many rows it hydrates to. A whole cursor leaves out the rows of its
    /// in-memory runs that `filter` drops; every other row stays, hydrated ones
    /// included, for the caller to filter. The `bool` is whether `filter` already
    /// decided every row of the chunk, so the caller need not filter it again:
    /// only a whole cursor's chunk drawn from in-memory runs alone, with no
    /// skeleton row hydrated.
    pub fn drain_hydrated(
        &mut self,
        max_rows: usize,
        filter: &mut RowFilter,
        hydrate: impl FnOnce(PkKeys) -> Result<Batch, String>,
    ) -> Result<Option<(Batch, bool)>, String> {
        let mut skeletons = SkeletonKeys::default();
        let Some((live, decided)) = self.drain_live_chunk(max_rows, &mut skeletons, filter) else {
            return Ok(None);
        };
        if skeletons.keys.is_empty() {
            return Ok(Some((live, decided)));
        }
        let keys = PkKeys::from_sorted(live.schema().pk_stride(), skeletons.keys);
        #[cfg(debug_assertions)]
        let asked = keys.clone();
        let hydrated = hydrate(keys)?;
        #[cfg(debug_assertions)]
        assert_hydration_matches(&hydrated, &asked, &skeletons.coarse);
        // Both consolidated and PK-disjoint.
        Ok(Some((
            if live.is_empty() {
                hydrated
            } else {
                merge_consolidated(&[hydrated.as_mem_batch(), live.as_mem_batch()], live.schema())
            },
            false,
        )))
    }

    /// [`Self::drain_chunk`] with skeleton rows split out; `Some` and empty means every
    /// row of the chunk was a skeleton. The `bool` is whether `filter` decided every
    /// row, as [`ReadCursor::drain_live_chunk`]; an index range or a key list never
    /// reads `filter`.
    fn drain_live_chunk(
        &mut self,
        max_rows: usize,
        skeletons: &mut SkeletonKeys,
        filter: &mut RowFilter,
    ) -> Option<(Batch, bool)> {
        let (gather, mut walk) = match self {
            SourceCursor::Full(c) => return c.drain_live_chunk(max_rows, skeletons, filter),
            SourceCursor::Keyed { gather, walk } => (gather, walk.as_deref_mut()),
        };
        loop {
            if let Some(chunk) = gather.drain_live_chunk(max_rows, skeletons) {
                debug_assert!(
                    walk.is_none() || (1..chunk.len()).all(|i| chunk.get_pk_bytes(i - 1) != chunk.get_pk_bytes(i)),
                    "index owner holds two rows under one PK"
                );
                return Some((chunk, false));
            }
            if !walk.as_mut()?.refill(gather, max_rows) {
                return None;
            }
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

/// The walk of one secondary-index key range, feeding each entry's source PK
/// to a [`KeyGather`] over the relation it indexes, over one snapshot of each.
/// The gather returns every row under an entry's source PK, so the walk is
/// exact only over rows unique on their PK: a source PK then has one live row
/// and at most one live index entry.
pub struct IndexWalk {
    /// Positioned on the index range and clamped at its end; an entry is a
    /// leading span followed by the source PK.
    idx: ReadCursor,
    /// A refill's source PKs in index order, flat at the gather's `pk_stride`.
    pks: Vec<u8>,
}

impl IndexWalk {
    /// Reload `gather` with the source PKs of the next `max_rows` index entries;
    /// `false` once the index range is spent.
    fn refill(&mut self, gather: &mut KeyGather<'_>, max_rows: usize) -> bool {
        let stride = gather.schema().pk_stride();
        let span = self.idx.schema.pk_stride() - stride;
        self.pks.clear();
        let mut taken = 0;
        while taken < max_rows && self.idx.valid {
            debug_assert!(self.idx.current_weight > 0, "an index entry at a non-positive weight");
            self.pks.extend_from_slice(&self.idx.current_pk_bytes()[span..]);
            self.idx.advance();
            taken += 1;
        }
        if taken == 0 {
            return false;
        }
        gather.reload(PkKeys::from_keys(stride, self.pks.chunks_exact(stride)));
        true
    }
}

/// The rows of a strictly ascending list of keys: gathered off a cursor, or
/// probed key by key from a store whose every key nets 0 or 1.
pub enum KeyGather<'a> {
    Merge(PkSetGather),
    Unique(UniqueGather<'a>),
}

impl<'a> KeyGather<'a> {
    /// A gather of `keys`, whole PKs of `schema`, probed from `store`.
    pub fn unique(store: &'a dyn UniqueRows, schema: SchemaDescriptor, keys: PkKeys) -> Self {
        debug_assert_eq!(keys.stride(), schema.pk_stride());
        KeyGather::Unique(UniqueGather { store, schema, keys, next: 0, skip: 0 })
    }

    fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        match self {
            KeyGather::Merge(g) => g.drain_live_chunk(max_rows, skeletons),
            // A store of one row per key holds no skeleton rows.
            KeyGather::Unique(g) => g.drain_chunk(max_rows),
        }
    }

    fn skip_payload(&mut self, skip: u64) {
        match self {
            KeyGather::Merge(g) => g.skip_payload(skip),
            KeyGather::Unique(g) => g.skip = skip,
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

/// Every live row of `keys`, a strictly ascending list of whole PKs of
/// `schema`, probed from `store`, at weight 1 and in the [`project_schema`]
/// layout of `cols`: the PK, then the payload columns `cols` names. `Err` where
/// `project_schema` refuses `cols`.
pub fn project_unique(
    store: &dyn UniqueRows,
    schema: &SchemaDescriptor,
    keys: &PkKeys,
    cols: &[u32],
) -> Result<Batch, String> {
    debug_assert_eq!(keys.stride(), schema.pk_stride());
    let out_schema = project_schema(schema, cols)?;
    let locs: Vec<ColumnLocator> = cols.iter().map(|&c| schema.locate(c as usize)).collect();
    let mut out = Batch::with_capacity(&out_schema, keys.len());
    for key in keys.iter() {
        if let Some(row) = store.unique_live_row_at(key) {
            let (src, i) = row.source();
            out.begin_row(key, 1);
            out.append_cells_from(&locs, src, i);
            out.commit_row();
        }
    }
    // One row per key, in ascending key order.
    out.certify_consolidated();
    Ok(out)
}

/// Every live row of a strictly ascending list of whole keys, one
/// [`UniqueRows`] probe per key.
pub struct UniqueGather<'a> {
    store: &'a dyn UniqueRows,
    schema: SchemaDescriptor,
    keys: PkKeys,
    /// Index of the next key, in keys — not bytes.
    next: usize,
    /// See [`ReadCursor::skip_payload`].
    skip: u64,
}

impl UniqueGather<'_> {
    /// The live rows of the next keys, at most `max_rows` of them, at weight 1;
    /// `None` once the list is exhausted.
    fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        assert!(max_rows > 0, "UniqueGather::drain_chunk: max_rows must be positive");
        let mut rows = Vec::with_capacity(max_rows.min(self.keys.len() - self.next));
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
        out.append_stored(&rows, 1, self.skip);
        out.certify_consolidated();
        Some(out)
    }
}
