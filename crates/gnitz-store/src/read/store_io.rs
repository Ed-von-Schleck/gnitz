//! Read I/O on relation families — whole-relation scan, point and range seek,
//! the batched FK parent probe, and the index-bounded cursor opener. Every one is
//! a direct cursor walk except one that meets a capacity-bounded view's skeleton
//! row, which goes through [`RelationRegistry::materialize_hydrated`].

use std::rc::Rc;

use super::SkeletonHydrator;
use crate::relation::RelationRegistry;
use crate::schema::{project_schema, ColumnLocator, SchemaDescriptor};
use crate::storage::{Batch, BoundedIndexCursor, ReadCursor, SourceCursor, StoreError};
use gnitz_expr::RowSource;
use gnitz_wire::IndexWalk;

impl RelationRegistry {
    /// Scan all positive-weight rows from a relation. One registry lookup serves
    /// every id — a system family is an ordinary entry, so the CIRCUIT_* tables
    /// are SQL-introspectable like any other relation. Returns the scan plus the
    /// schema descriptor the entry already holds, so the reply never re-resolves it.
    pub fn scan(
        &self,
        id: i64,
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<(Rc<Batch>, SchemaDescriptor), StoreError> {
        let entry = self.relation_or_err(id)?;
        // Asked of the store, not of a cursor: the non-hydrating answer is
        // `full_scan`'s cached `Rc` snapshot, and opening a cursor to ask
        // whether this walk meets a skeleton row would defeat that cache.
        if entry.store().has_skeleton_rows() {
            let schema = entry.schema();
            let cursor = entry.cursor();
            // The hydrated scan is not cached: `full_scan`'s snapshot is
            // invalidated on every ingest, so under live churn it would hold at
            // most one scan and cost a full hydrated copy of the store to do so.
            return Ok((Rc::new(self.materialize_hydrated(id, cursor, None, hydrator)?), schema));
        }
        Ok((entry.full_scan(), entry.schema()))
    }

    /// Every live row `cursor` walks, with each skeleton key recomputed — one
    /// consolidated batch in the walk's own schema. `keys` names the ascending
    /// OPK key list to visit group by group; `None` sweeps from where the caller
    /// positioned the cursor to its own bound, which the open already cut.
    ///
    /// Walks the cursor once, copying each hydrated row verbatim and pushing each
    /// skeleton `(PK, coarse weight)` onto a key list, then hydrates that list and
    /// merges the two. Both halves are consolidated, so the union merges and folds
    /// them in one pass rather than sorting the hydrated relation.
    ///
    /// Row-at-a-time on purpose: `drain_chunk` goes through
    /// `slice_to_owned_batch_with`, which dereferences every German-string cell
    /// and certifies the view schema's NOT NULL bits — neither of which a
    /// skeleton shard can survive.
    pub(crate) fn materialize_hydrated(
        &self,
        view_id: i64,
        mut cursor: ReadCursor,
        keys: Option<&[u8]>,
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<Batch, StoreError> {
        let schema = cursor.schema;
        let stride = schema.pk_stride();
        // Flat OPK images, ascending — every walk below visits keys in that
        // order, so the list is sorted by construction.
        let mut skeleton_keys: Vec<u8> = Vec::new();
        let mut coarse: Vec<i64> = Vec::new();

        // A bounded view's terminal partition is the finest in the tree, and this
        // is the read that meets it, so the pre-size is this walk's own upper row
        // bound — each `out` growth below re-copies every live byte into a fresh
        // arena.
        let cap = match keys {
            // For a key list the bound is the key count, which a view's synthetic
            // PK can exceed — one key names a whole group there.
            Some(k) => k.len() / stride,
            None => cursor.estimated_length(),
        };
        let mut out = Batch::with_capacity(&schema, cap);

        // Tested *before* any copy: `copy_current_row_into` reads every payload
        // column of the row and relocates its German-string blobs, which a
        // skeleton shard has no bytes for.
        let mut visit = |c: &ReadCursor| {
            if c.current_weight <= 0 {
                return;
            }
            if c.current_is_skeleton() {
                skeleton_keys.extend_from_slice(c.current_pk_bytes());
                coarse.push(c.current_weight);
            } else {
                c.copy_current_row_into(&mut out, c.current_weight);
            }
        };
        match keys {
            // A listed key set walks group by group; anything else is one sweep
            // from wherever the caller left the cursor.
            Some(k) => {
                for key in k.chunks_exact(stride) {
                    if cursor.seek_pk_group_ascending(key) {
                        cursor.for_each_pk_group_row(key, &mut visit);
                    }
                }
            }
            None => cursor.for_each_row_while(|_| true, &mut visit),
        }
        // Not borrowck (a `ReadCursor` owns its runs by `Rc`): an early free, so
        // the merge tree is gone before `hydrate_keys` allocates its replay.
        drop(cursor);

        // The cursor emits strictly ascending (PK, payload) with net weights and
        // drops ghosts, and `visit` keeps only positive ones — so the walk's output
        // is consolidated as built, and saying so is what lets the union below take
        // its O(n) merge instead of a full sort of the hydrated relation.
        out.certify_layout(crate::storage::Layout::Consolidated);
        if coarse.is_empty() {
            return Ok(out);
        }
        let hydrated = match hydrator {
            Some(h) => h.hydrate_keys(self, view_id, skeleton_keys, &coarse)?,
            // Unreachable: reaching here means a walk met a skeleton row, which
            // requires a capacity budget, which no host passing `None` ever sets.
            None => {
                return Err(StoreError::rejected(format!(
                    "relation {view_id} holds skeleton rows but this process maintains no circuit"
                )))
            }
        };
        // The fully-dehydrated store is what the feature exists for, and
        // `op_union`'s empty-`a` arm is a whole `clone_batch` of the other side.
        if out.count == 0 {
            return Ok(hydrated);
        }
        // Both operands are consolidated — `out` by the walk above, `hydrated` by
        // `hydrate_keys` — so the union takes its folding merge and certifies the
        // result itself; nothing here re-folds through a second arena.
        Ok(crate::ops::op_union(out, &hydrated, &schema))
    }

    /// Point lookup by OPK bytes: every live row of `pk`'s group, or `None` for
    /// a miss. The skeleton test is the opened cursor's, so a point lookup that
    /// misses every skeleton shard reads without hydrating.
    pub fn seek(
        &self,
        id: i64,
        pk: &[u8],
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<Option<Batch>, StoreError> {
        let entry = self.relation_or_err(id)?;
        let mut cursor = entry.cursor_in_range(pk, Some(pk));
        if !cursor.any_skeleton() {
            return Ok(live_pk_group(&mut cursor, pk));
        }
        // One key is a one-element key list, so the hydrating read is the same
        // walk every other one takes — it just hydrates at most one key rather
        // than the store.
        let b = self.materialize_hydrated(id, cursor, Some(pk), hydrator)?;
        Ok((b.count > 0).then_some(b))
    }

    /// Batched point lookup for the FK parent probe. Seek each PK in `pks`
    /// (verbatim OPK bytes) in this worker's store, appending the stored row at
    /// weight 1 for every present, live key into a result batch projected to
    /// `ref_col` (a parent column index, non-PK and scalar). Absent / retracted
    /// keys contribute nothing, and passing `pks` ascending keeps the cursor's
    /// probes monotonic. The projected schema is returned alongside the batch —
    /// it is synthetic, so the caller cannot look it up from the catalog.
    ///
    /// Each PK resolves to its group's FIRST live row, which is also its only
    /// one: `validate_fk_column` admits only a base table as an FK parent, and a
    /// base table's PK is kept unique by `enforce_unique_pk`. The consumer
    /// requires that — it indexes the result by PK, so a second row of a group
    /// would overwrite the first rather than join it. The seek and `pk IN (…)`
    /// readers, whose consumers take whole groups, walk instead.
    ///
    /// Ascending is a producer guarantee: the master sorts before scattering and
    /// `scatter::with_group` preserves per-worker order, which is what lets an
    /// absent key — most of a broadcast list, at W workers — cost a comparison.
    pub fn gather_bytes<'k>(
        &self,
        id: i64,
        pks: impl ExactSizeIterator<Item = &'k [u8]>,
        ref_col: u8,
    ) -> Result<(Batch, SchemaDescriptor), StoreError> {
        let entry = self.relation_or_err(id)?;
        let schema = entry.schema();
        let result_schema =
            project_schema(&schema, &[ref_col as u32]).expect("a one-column projection fits MAX_COLUMNS");
        // The column is master-picked and is never a PK column (the FK rules
        // gather only a non-PK referenced column), which this rejects: a PK
        // `ref_col` would be skipped by `project_schema` and leave the reply
        // payload-less.
        // Once, outside the loop: `locate`'s own doc rules out running it per row.
        let loc = schema.locate(ref_col as usize);
        assert!(
            matches!(loc, ColumnLocator::Payload { .. }),
            "FK projection excludes PK columns"
        );
        // The master SCATTERS the key list, so `pks` is this worker's own sublist
        // and `pks.len()` is a tight bound, not a W× over-allocation.
        let mut out = Batch::with_capacity(&result_schema, pks.len());
        let mut cursor = entry.cursor();
        for pk in pks {
            // The weight gate rejects a tombstone an uncompacted source holds.
            if cursor.seek_pk_group_ascending(pk) && cursor.current_weight > 0 {
                copy_cursor_col_to_batch(&cursor, &mut out, loc);
            }
        }
        Ok((out, result_schema))
    }

    /// The index walk over `desc` on `cols` of `source`, or — for an
    /// [`IndexWalk::Optional`] walk that is declined — the full-scan cursor.
    pub fn open_index_source(
        &self,
        source: i64,
        cols: &[u32],
        desc: &gnitz_wire::RangeDescriptor,
        walk: IndexWalk,
    ) -> Result<SourceCursor, StoreError> {
        match self.open_index_range(source, cols, desc, walk) {
            Ok(c) => Ok(SourceCursor::Bounded(c)),
            Err(_) if walk == IndexWalk::Optional => {
                Ok(SourceCursor::Full(Box::new(self.relation_or_err(source)?.cursor())))
            }
            Err(e) => Err(e),
        }
    }

    /// Open the index walk. `Err` is a decline: no such index, a malformed range,
    /// or — for an [`IndexWalk::Optional`] walk only — an unselective one.
    fn open_index_range(
        &self,
        id: i64,
        col_indices: &[u32],
        range: &gnitz_wire::RangeDescriptor,
        walk: IndexWalk,
    ) -> Result<Box<BoundedIndexCursor>, StoreError> {
        let entry = self.relation_or_err(id)?;
        // The index was dropped since the plan compiled.
        let Some(ic) = entry.index_on(col_indices) else {
            return Err(StoreError::rejected(format!(
                "No index on cols {col_indices:?} for table {id}"
            )));
        };
        let (idx, matches) = ic
            .store()
            .cursor_over(&ic.key_spec(), range)
            .map_err(StoreError::rejected)?;
        if walk == IndexWalk::Optional {
            // Only user base tables own index circuits, so a resolved index
            // implies a base store unless this process detached it (the post-fork
            // master), which degrades to the full scan like any other decline.
            let Some(base) = entry.store().table() else {
                return Err(StoreError::rejected("index owner holds no local base store"));
            };
            if matches > base.estimated_rows() / INDEX_SCAN_RATIO {
                return Err(StoreError::rejected(
                    "index range is not selective enough to pay for the walk",
                ));
            }
        }
        // Boxed here, so it reaches `SourceCursor::Bounded` without a second
        // allocation.
        Ok(Box::new(BoundedIndexCursor::new(
            idx,
            entry.cursor(),
            ic.key_spec(),
            matches.min(self.config.scan_chunk_rows),
        )))
    }
}

/// An optional index walk is taken only while its range covers at most
/// `1/INDEX_SCAN_RATIO` of the local base slice.
const INDEX_SCAN_RATIO: usize = 16;

/// Every live row of `pk`'s **group** off a cursor opened over it. A *group*,
/// because a base table's PK is unique but a synthetic view key (`_join_pk`)
/// names one row per row the join produced for it — walking it is what makes a
/// seek answer the same rows a point-range read of that key does.
fn live_pk_group(cursor: &mut ReadCursor, pk: &[u8]) -> Option<Batch> {
    let mut batch = Batch::empty_with_schema(&cursor.schema);
    cursor.copy_live_pk_group_into(pk, &mut batch);
    (batch.count > 0).then_some(batch)
}

/// Projecting sibling of `ReadCursor::copy_current_row_into`: append the cursor's
/// current row to `out` (which has the one-column `project_schema` layout) at
/// weight 1, copying only the column `loc` addresses into payload slot 0.
///
/// `current_pk_bytes()` is the verbatim OPK region at any width, so one path
/// serves narrow and wide PKs alike; the cell goes through the shared appender,
/// so a NULL zero-fills and a German string relocates rather than carrying a
/// source-heap offset into a batch that does not own it.
fn copy_cursor_col_to_batch(cursor: &ReadCursor, out: &mut Batch, loc: ColumnLocator) {
    out.begin_row(cursor.current_pk_bytes(), 1);
    let (src, row) = cursor.current_row_source();
    let mut proj_null = 0u64;
    // Off the word the cursor already caches, where `ColumnLocator::is_null`
    // would re-load it through `Run`'s dispatch.
    let cell = if loc.is_null_word(cursor.current_null_word) {
        gnitz_wire::null_word_set(&mut proj_null, 0, true);
        None
    } else {
        Some(loc.bytes(src, row))
    };
    out.append_payload_cell(0, loc.type_code(), loc.size(), cell, src.blob(), None);
    out.commit_row(proj_null);
}
