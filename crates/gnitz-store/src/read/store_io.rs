//! Read I/O on relation families. [`RelationRegistry::open_bound`] opens every
//! source a bound can name; [`LiveSource`] drains one chunk by chunk, hydrating
//! each capacity-bounded view's skeleton row it meets within that chunk.
//! [`RelationRegistry::gather_bytes`] is the batched FK parent probe.

use super::SkeletonHydrator;
use crate::relation::{Relation, RelationKind, RelationRegistry};
use crate::schema::key::{compare_pk_bytes, sort_indices, IndexKeySpec};
use crate::schema::{project_schema, ColumnLocator};
use crate::schema::{ColumnTable, SchemaFacts};
use crate::storage::{pk_group_end, Batch, PkSetGather, ReadCursor, SkeletonKeys};
use gnitz_wire::{KeyRange, PkKeys, ReadBound};

const INDEX_SCAN_RATIO: usize = 16;

/// A source drained chunk by chunk, each skeleton row it meets replaced by that key's
/// rows recomputed through `hydrator`.
pub(super) struct LiveSource<'a, 'h> {
    registry: &'a RelationRegistry,
    id: u64,
    source: SourceCursor,
    hydrator: Option<&'h mut dyn SkeletonHydrator>,
}

impl<'a, 'h> LiveSource<'a, 'h> {
    pub(super) fn new(
        registry: &'a RelationRegistry,
        id: u64,
        source: SourceCursor,
        hydrator: Option<&'h mut dyn SkeletonHydrator>,
    ) -> Self {
        LiveSource { registry, id, source, hydrator }
    }

    /// The next chunk, or `None` once the source is exhausted. `max_rows` bounds the merge
    /// groups visited; a skeleton row counts once however many rows it hydrates to.
    pub(super) fn next_chunk(&mut self, max_rows: usize) -> Result<Option<Batch>, String> {
        let mut skeletons = SkeletonKeys::default();
        let Some(live) = self.source.drain_live_chunk(max_rows, &mut skeletons) else {
            return Ok(None);
        };
        if skeletons.keys.is_empty() {
            return Ok(Some(live));
        }
        let Some(hydrator) = self.hydrator.as_deref_mut() else {
            return Err(format!(
                "relation {} holds skeleton rows but this process maintains no circuit",
                self.id
            ));
        };
        let SkeletonKeys { keys, coarse } = skeletons;
        let keys = PkKeys::from_sorted(live.schema().pk_stride(), keys);
        let expected = cfg!(debug_assertions).then(|| keys.clone());
        let hydrated = hydrator
            .hydrate_keys(self.registry, self.id, keys)
            .map_err(|e| format!("hydrate: view {}: {e}", self.id))?;
        if let Some(keys) = expected {
            debug_assert_hydration_matches(&hydrated, &keys, &coarse);
        }
        // Both consolidated and PK-disjoint.
        let schema = *live.schema();
        Ok(Some(match live.is_empty() {
            true => hydrated,
            false => hydrated.merged_consolidated(&live, &schema),
        }))
    }
}

impl RelationRegistry {
    /// The FK parent probe: every live row of `keys` at weight 1, projected to the payload
    /// column `ref_col`.
    pub fn gather_bytes(&self, id: u64, keys: PkKeys, ref_col: u8) -> Result<Batch, String> {
        let entry = self.relation_or_err(id)?;
        let schema = entry.schema();
        let out_schema = project_schema(&schema, &[ref_col as u32]).expect("a one-column projection fits MAX_COLUMNS");
        // A PK `ref_col` would be dropped by `project_schema`, leaving the reply payload-less.
        let loc = schema.locate(ref_col as usize);
        assert!(
            matches!(loc, ColumnLocator::Payload { .. }),
            "FK projection excludes PK columns"
        );
        let n = keys.len();
        let mut gather = entry.gather(keys, None);
        let mut out = Batch::with_capacity(&out_schema, n);
        gather.for_each_live_row(usize::MAX, |c| {
            let (src, row) = c.current_row_source();
            let mut null_word = 0;
            out.begin_row(c.current_pk_bytes(), 1);
            out.append_cells_from(0, std::slice::from_ref(&loc), src, row, &mut null_word);
            out.commit_row(null_word);
        });
        Ok(out)
    }

    /// Open `bound`'s source over `id`, without walking it, and the part of `bound`
    /// the source does not apply.
    pub fn open_bound(&self, id: u64, bound: ReadBound) -> Result<(SourceCursor, ReadBound), String> {
        let entry = self.relation_or_err(id)?;
        // A stream holds no rows, but a backfill over one still feeds an empty
        // epoch: that is what mints a global aggregate's ground row.
        if entry.kind() == RelationKind::Stream {
            let empty = crate::storage::empty_cursor(entry.schema());
            return Ok((SourceCursor::Full(Box::new(empty)), ReadBound::None));
        }
        let cursor = match bound {
            ReadBound::None => SourceCursor::Full(Box::new(entry.cursor())),
            ReadBound::PkSet(keys) => {
                let schema = entry.schema();
                if keys.stride() != schema.pk_stride() {
                    return Err(format!(
                        "open_bound: PkSet key stride {} != pk_stride {} (table {id})",
                        keys.stride(),
                        schema.pk_stride()
                    ));
                }
                // A key this worker holds no row for copies nothing.
                SourceCursor::PkSet(Box::new(entry.gather(keys, None)))
            }
            ReadBound::Range(r) => return open_range(entry, r),
        };
        Ok((cursor, ReadBound::None))
    }
}

/// The walk `r` names over `entry`, and the part of it the cursor leaves unapplied.
fn open_range(entry: &Relation, r: KeyRange) -> Result<(SourceCursor, ReadBound), String> {
    let cols = entry.bound_cols(r.cols(), "open_bound")?;
    let schema = entry.schema();
    if r.walks_pk(schema.pk_cols()) {
        let (cursor, _) = entry.store().held().range_cursor(schema.pk_range_keys(&r));
        return Ok((SourceCursor::Full(Box::new(cursor)), ReadBound::None));
    }
    if let Some(ic) = entry.index_on(cols.as_slice()) {
        let (idx, matches) = ic.cursor_over(&r);
        if matches <= entry.store().held().estimated_rows() / INDEX_SCAN_RATIO {
            let walk = BoundedIndexCursor {
                idx,
                src: PkSetGather::new(entry.cursor(), PkKeys::from_sorted(schema.pk_stride(), Vec::new())),
                spec: ic.key_spec(),
                pks: Vec::new(),
                order: Vec::new(),
            };
            return Ok((SourceCursor::Bounded(Box::new(walk)), ReadBound::None));
        }
    }
    Ok((SourceCursor::Full(Box::new(entry.cursor())), ReadBound::Range(r)))
}

/// Tripwire: the replay's per-PK weight sum must equal the coarse weight the
/// skeleton row carried, by linearity of the PK projection. `keys` and `out` are
/// both ascending, so one co-walk checks every key and catches a PK no key named.
fn debug_assert_hydration_matches(out: &Batch, keys: &PkKeys, coarse: &[i64]) {
    debug_assert_eq!(keys.len(), coarse.len());
    let mut expected = keys.iter().zip(coarse).peekable();
    let mut i = 0;
    while i < out.len() {
        let pk = out.get_pk_bytes(i);
        // Every skeleton key carries a strictly positive coarse weight.
        while let Some((key, _)) = expected.next_if(|(key, _)| compare_pk_bytes(key, pk).is_lt()) {
            debug_assert!(false, "hydration produced no rows for skeleton key {key:?}");
        }
        let named = expected.next().filter(|(key, _)| *key == pk);
        debug_assert!(
            named.is_some(),
            "hydration produced rows for a PK no skeleton row named"
        );
        let j = pk_group_end(out, i);
        let sum = out.as_mem_batch().sum_weights(i, j);
        i = j;
        if let Some((_, &weight)) = named {
            debug_assert_eq!(sum, weight, "hydration weight mismatch for key {pk:?}");
        }
    }
    debug_assert!(
        expected.next().is_none(),
        "hydration produced no rows for a trailing skeleton key"
    );
}

/// A chunked source of `Batch`es over one relation, in every shape a bound can take.
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
    fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_live_chunk(max_rows, skeletons),
            SourceCursor::Bounded(c) => c.drain_live_chunk(max_rows, skeletons),
            SourceCursor::PkSet(g) => g.drain_live_chunk(max_rows, skeletons),
        }
    }
}

/// A walk of one secondary-index key range, gathering each entry's row from the base
/// table it indexes, over one snapshot of each. The owner is a base table, so a source
/// PK has one live row and at most one live index entry, at weight 1.
pub struct BoundedIndexCursor {
    /// Positioned on the index range and clamped at its end.
    idx: ReadCursor,
    src: PkSetGather,
    spec: IndexKeySpec,
    /// A refill's source PKs in index order, flat at `src`'s `pk_stride`.
    pks: Vec<u8>,
    order: Vec<u32>,
}

impl BoundedIndexCursor {
    fn drain_live_chunk(&mut self, max_rows: usize, skeletons: &mut SkeletonKeys) -> Option<Batch> {
        assert!(
            max_rows > 0,
            "BoundedIndexCursor::drain_live_chunk: max_rows must be positive"
        );
        loop {
            if let Some(chunk) = self.src.drain_live_chunk(max_rows, skeletons) {
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
