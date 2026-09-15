//! Read I/O on relation families. [`RelationRegistry::open_bound`] opens every
//! source a bound can name; [`LiveSource`] drains one chunk by chunk, hydrating
//! each capacity-bounded view's skeleton row it meets within that chunk. The verbs
//! — whole-relation scan, point seek, and the batched FK parent probe — sit on
//! those two.

use std::rc::Rc;

use super::SkeletonHydrator;
use crate::relation::RelationRegistry;
use crate::schema::key::compare_pk_bytes;
use crate::schema::{project_schema, ColumnLocator};
use crate::storage::{Batch, BoundedIndexCursor, PkSetGather, SkeletonKeys, SourceCursor, StoreError};
use gnitz_wire::{IndexWalk, ReadBound};

/// An optional index walk is taken only while its range covers at most
/// `1/INDEX_SCAN_RATIO` of the local base slice.
const INDEX_SCAN_RATIO: usize = 16;

/// A source drained chunk by chunk, each skeleton row it meets replaced by that key's
/// rows recomputed through `hydrator`.
pub(super) struct LiveSource<'a, 'h> {
    registry: &'a RelationRegistry,
    id: i64,
    source: SourceCursor,
    hydrator: Option<&'h mut dyn SkeletonHydrator>,
}

impl<'a, 'h> LiveSource<'a, 'h> {
    pub(super) fn new(
        registry: &'a RelationRegistry,
        id: i64,
        source: SourceCursor,
        hydrator: Option<&'h mut dyn SkeletonHydrator>,
    ) -> Self {
        LiveSource { registry, id, source, hydrator }
    }

    /// The next chunk, or `None` once the source is exhausted. `max_rows` bounds the merge
    /// groups visited; a skeleton row counts once however many rows it hydrates to.
    pub(super) fn next_chunk(&mut self, max_rows: usize) -> Result<Option<Batch>, StoreError> {
        let mut skeletons = SkeletonKeys::default();
        let Some(live) = self.source.drain_live_chunk(max_rows, &mut skeletons) else {
            return Ok(None);
        };
        if skeletons.keys.is_empty() {
            return Ok(Some(live));
        }
        let Some(hydrator) = self.hydrator.as_deref_mut() else {
            return Err(StoreError::rejected(format!(
                "relation {} holds skeleton rows but this process maintains no circuit",
                self.id
            )));
        };
        let SkeletonKeys { keys, coarse } = skeletons;
        let expected = cfg!(debug_assertions).then(|| keys.clone());
        let hydrated = hydrator.hydrate_keys(self.registry, self.id, keys)?;
        if let Some(keys) = expected {
            debug_assert_hydration_matches(&hydrated, &keys, &coarse);
        }
        // Both consolidated and PK-disjoint; `hydrated` first so a chunk of skeleton rows
        // alone passes through `op_union`'s empty-operand arm.
        let schema = *live.schema();
        Ok(Some(crate::ops::op_union(hydrated, &live, &schema)))
    }
}

impl RelationRegistry {
    /// Scan all positive-weight rows from a relation. One registry lookup serves
    /// every id — a system family is an ordinary entry, so the CIRCUIT_* tables
    /// are SQL-introspectable like any other relation.
    pub fn scan(&self, id: i64, hydrator: Option<&mut dyn SkeletonHydrator>) -> Result<Rc<Batch>, StoreError> {
        let entry = self.relation_or_err(id)?;
        // Asked of the store, not a cursor, so the common case keeps `full_scan`'s cached snapshot.
        if !entry.store().has_skeleton_rows() {
            return Ok(entry.full_scan());
        }
        let mut rows = LiveSource::new(self, id, SourceCursor::Full(Box::new(entry.cursor())), hydrator);
        let batch = rows.next_chunk(usize::MAX)?;
        Ok(Rc::new(
            batch.unwrap_or_else(|| Batch::empty_with_schema(&entry.schema())),
        ))
    }

    /// Every live row of `pk`'s group — a view's synthetic key names one row per row its body
    /// produced — or `None` for a miss.
    pub fn seek(
        &self,
        id: i64,
        pk: &[u8],
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<Option<Batch>, StoreError> {
        let entry = self.relation_or_err(id)?;
        let gather = PkSetGather::open(pk.to_vec(), entry.schema(), |s, e| entry.cursor_in_range(s, e));
        LiveSource::new(self, id, SourceCursor::PkSet(Box::new(gather)), hydrator).next_chunk(usize::MAX)
    }

    /// The FK parent probe: every live row of `keys` (flat OPK images, strictly ascending) at
    /// weight 1, projected to the payload column `ref_col`.
    pub fn gather_bytes(&self, id: i64, keys: Vec<u8>, ref_col: u8) -> Result<Batch, StoreError> {
        let entry = self.relation_or_err(id)?;
        let schema = entry.schema();
        let out_schema = project_schema(&schema, &[ref_col as u32]).expect("a one-column projection fits MAX_COLUMNS");
        // A PK `ref_col` would be dropped by `project_schema`, leaving the reply payload-less.
        let loc = schema.locate(ref_col as usize);
        assert!(
            matches!(loc, ColumnLocator::Payload { .. }),
            "FK projection excludes PK columns"
        );
        let mut gather = PkSetGather::open(keys, schema, |s, e| entry.cursor_in_range(s, e));
        let mut out = Batch::with_capacity(&out_schema, gather.remaining_keys());
        gather.for_each_live_row(usize::MAX, |c| {
            let (src, row) = c.current_row_source();
            let mut null_word = 0;
            out.begin_row(c.current_pk_bytes(), 1);
            out.append_cell_from(0, &loc, src, row, &mut null_word);
            out.commit_row(null_word);
        });
        Ok(out)
    }

    /// Open `bound`'s source over `id`, without walking it. An `Optional` index walk that
    /// finds no index, or whose range is too unselective to pay for itself, opens the full scan.
    pub fn open_bound(&self, id: i64, bound: ReadBound) -> Result<SourceCursor, StoreError> {
        let entry = self.relation_or_err(id)?;
        let full = || SourceCursor::Full(Box::new(entry.cursor()));
        Ok(match bound {
            ReadBound::None => full(),
            ReadBound::PkRange(desc) => SourceCursor::Full(Box::new(
                entry.store().pk_range_cursor(&desc).map_err(StoreError::rejected)?,
            )),
            ReadBound::PkSet(keys) => {
                let schema = entry.schema();
                if keys.stride() != schema.pk_stride() {
                    return Err(StoreError::rejected(format!(
                        "open_bound: PkSet key stride {} != pk_stride {} (table {id})",
                        keys.stride(),
                        schema.pk_stride()
                    )));
                }
                // A key this worker holds no row for copies nothing.
                let gather = PkSetGather::open(keys.into_bytes(), schema, |s, e| entry.cursor_in_range(s, e));
                SourceCursor::PkSet(Box::new(gather))
            }
            ReadBound::IndexRange { bound, walk } => {
                let cols = self.bound_cols_against(id, bound.idx_cols, "open_bound")?;
                let Some(ic) = entry.index_on(cols.as_slice()) else {
                    return match walk {
                        IndexWalk::Optional => Ok(full()),
                        IndexWalk::Required => Err(StoreError::rejected(format!(
                            "No index on cols {:?} for table {id}",
                            cols.as_slice()
                        ))),
                    };
                };
                let spec = ic.key_spec();
                let (idx, matches) = ic
                    .store()
                    .cursor_over(&spec, &bound.desc)
                    .map_err(StoreError::rejected)?;
                if walk == IndexWalk::Optional && matches > entry.store().estimated_rows() / INDEX_SCAN_RATIO {
                    return Ok(full());
                }
                SourceCursor::Bounded(Box::new(BoundedIndexCursor::new(
                    idx,
                    entry.cursor(),
                    spec,
                    matches.min(self.config.scan_chunk_rows),
                )))
            }
        })
    }
}

/// Tripwire: the replay's per-PK weight sum must equal the coarse weight the
/// skeleton row carried, by linearity of the PK projection.
///
/// One co-walk, not a scan per key: `keys` is ascending by construction and
/// `into_consolidated` sorted `out` by (PK, payload), so the two run in step.
/// That also catches a PK `out` holds rows for that `keys` never named, which a
/// per-key lookup cannot see.
///
/// The `cfg!` return is what a `#[cfg(debug_assertions)]` on the function would
/// not give: the assertions vanish in release either way, but the co-walk itself
/// is O(rows) and would otherwise still run.
fn debug_assert_hydration_matches(out: &Batch, keys: &[u8], coarse: &[i64]) {
    if !cfg!(debug_assertions) {
        return;
    }
    let stride = keys.len() / coarse.len();
    let mut ki = 0;
    let mut i = 0;
    while i < out.len() {
        let pk = out.get_pk_bytes(i);
        // Every skeleton key carries a strictly positive coarse weight, so a key
        // the replay produced nothing for is a bug — as is a key it produced rows
        // for that no skeleton row named.
        while ki < coarse.len() && compare_pk_bytes(&keys[ki * stride..(ki + 1) * stride], pk).is_lt() {
            debug_assert!(false, "hydration produced no rows for skeleton key {ki}");
            ki += 1;
        }
        debug_assert!(
            ki < coarse.len() && keys[ki * stride..(ki + 1) * stride] == *pk,
            "hydration produced rows for a PK no skeleton row named",
        );
        let mut sum = 0i64;
        while i < out.len() && out.get_pk_bytes(i) == pk {
            sum += out.get_weight(i);
            i += 1;
        }
        debug_assert_eq!(
            sum,
            coarse.get(ki).copied().unwrap_or(0),
            "hydration weight mismatch for key {pk:?}",
        );
        ki += 1;
    }
    debug_assert_eq!(
        ki,
        coarse.len(),
        "hydration produced no rows for a trailing skeleton key"
    );
}
