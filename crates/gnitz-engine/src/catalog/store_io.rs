//! Server-facing read I/O on table families — ingest, whole-relation scan,
//! point and range seek (including secondary-index lookup), the batched FK
//! parent probe, and the source cursors a circuit backfill or a `ScanSpec`
//! drives. Every one is a direct cursor walk except a capacity-bounded view's,
//! which goes through [`CatalogEngine::materialize_bounded_store`].

use super::*;
use crate::schema::project_schema;
use crate::storage::{BoundedIndexCursor, SourceCursor};

/// Why an ingest did not happen. The two variants differ in what a caller may
/// do next: `Rejected` means nothing was applied and the request is at fault, so
/// answering the caller with the message is the whole response; `Storage` means
/// committed data did not reach the store, which leaves this process's state
/// diverged from whatever durable log carried the batch.
#[derive(Debug)]
pub enum IngestError {
    /// The target or the batch was refused before anything was applied.
    Rejected(String),
    /// The store failed to absorb the batch.
    Storage(StorageError),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::Rejected(m) => write!(f, "{m}"),
            IngestError::Storage(e) => write!(f, "{e}"),
        }
    }
}

/// What part of a capacity-bounded view's store one read wants. `Keys` carries
/// the flat concatenation of the OPK images, ascending.
pub(crate) enum BoundedRead<'a> {
    All,
    Range(&'a [u8], Option<&'a [u8]>),
    Keys(&'a [u8]),
}

impl CatalogEngine {
    /// Ingest a user-table batch and return the effective delta (after PK
    /// enforcement).  Used by multi-worker push where the worker needs the effective
    /// batch for later DAG evaluation but does NOT evaluate immediately.
    /// System tables are NOT supported (use `ingest_to_family` for those).
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: Batch) -> Result<Batch, IngestError> {
        if table_id < FIRST_USER_TABLE_ID {
            return Err(IngestError::Rejected(
                "ingest_returning_effective not supported for system tables".to_string(),
            ));
        }
        // Width guard, ahead of `enforce_unique_pk`. A worker parked mid-epoch
        // stashes an incoming `DdlSync` and replays it at the next top-level
        // drain while still serving pushes inline, so its `TableEntry.schema` can
        // lag a frame the master framed from its own widened catalog — and the
        // worker decodes with no hint, so nothing else compares the two. A real
        // check, not a `debug_assert`: the append path is driven by the
        // destination's region count, so a release build would silently drop the
        // extra column and ACK the push as success.
        if let Ok(entry) = self.table_entry(table_id) {
            let want = entry.schema.num_payload_cols();
            if batch.num_payload_cols() != want {
                return Err(IngestError::Rejected(format!(
                    "push for table_id={table_id} carries {} payload columns, table schema has {}",
                    batch.num_payload_cols(),
                    want
                )));
            }
        }
        self.dag
            .ingest_returning_effective(table_id, batch)
            .map_err(IngestError::Storage)?
            .ok_or_else(|| IngestError::Rejected(format!("ingest failed for table_id={table_id}: not registered")))
    }

    /// Scan all positive-weight rows from a relation. One registry lookup serves
    /// every id — a system family's `Borrowed` handle preserves `full_scan`'s
    /// `Rc` snapshot cache, so the CIRCUIT_* tables are SQL-introspectable like
    /// any other relation. Returns the scan plus the schema descriptor, which the
    /// entry already holds, so the reply path never re-resolves it.
    pub fn scan_family(&mut self, table_id: i64) -> Result<(Rc<Batch>, SchemaDescriptor), String> {
        let entry = self.table_entry(table_id)?;
        if entry.needs_hydration() {
            let schema = entry.schema;
            // The hydrated scan is not cached: `full_scan`'s snapshot is
            // invalidated on every ingest, so under live churn it would hold at
            // most one scan and cost a full hydrated copy of the store to do so.
            return Ok((
                Rc::new(self.materialize_bounded_store(table_id, BoundedRead::All)?),
                schema,
            ));
        }
        Ok((entry.full_scan(), entry.schema))
    }

    /// Every live row of a capacity-bounded view's store over `read`, with each
    /// skeleton key recomputed. One consolidated batch in the view's own schema.
    ///
    /// Walks the store cursor once, copying each hydrated row verbatim and pushing
    /// each skeleton `(PK, coarse weight)` onto a key list, then hydrates that list
    /// and merges the two. Both halves are ascending and disjoint by PK — a key is
    /// skeleton or it is not, and the read cursor's coarsening emits a mixed PK
    /// group as exactly one skeleton row — so the merge is exact and no sort is
    /// needed.
    ///
    /// The walk is row-at-a-time on purpose: `drain_chunk` / `materialize` go
    /// through `slice_to_owned_batch_with`, which force-NULLs every absent column,
    /// dereferences every German-string cell, and certifies the result against
    /// the view schema's NOT NULL bits — none of which a skeleton shard can
    /// survive.
    pub(crate) fn materialize_bounded_store(&mut self, view_id: i64, read: BoundedRead<'_>) -> Result<Batch, String> {
        let entry = self.table_entry(view_id)?;
        let schema = entry.schema;
        let stride = schema.pk_stride() as usize;
        // Flat OPK images, ascending — every walk below visits keys in that
        // order, so the list is sorted by construction.
        let mut skeleton_keys: Vec<u8> = Vec::new();
        let mut coarse: Vec<i64> = Vec::new();

        // A bounded view's terminal partition is the finest in the tree, and this
        // is the read that meets it, so every bounded arm opens over its own range
        // and yields that walk's own upper row bound — each `out` growth below
        // re-copies every live byte into a fresh arena.
        let (mut cursor, cap) = match read {
            // For a key list the bound is the key count, which a view's synthetic
            // PK can exceed — one key names a whole group there.
            BoundedRead::Keys(keys) => match crate::storage::key_list_range(keys, stride) {
                Some((lo, hi)) => (entry.open_cursor_in_range(lo, Some(hi)), keys.len() / stride),
                // No key to gather, so nothing to open over: falling through to
                // the whole store would build a merge tree over every shard and
                // then walk it zero times.
                None => (crate::storage::empty_cursor(schema), 0),
            },
            BoundedRead::Range(start, end) => {
                let mut cursor = entry.open_cursor_in_range(start, end);
                cursor.seek_range_bytes(start, end);
                let cap = cursor.estimated_length();
                (cursor, cap)
            }
            BoundedRead::All => {
                let cursor = entry.open_cursor();
                let cap = cursor.estimated_length();
                (cursor, cap)
            }
        };
        let mut out = Batch::with_capacity(schema, cap);

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
        match read {
            // A listed key set walks group by group; a whole store or a range is
            // one sweep from wherever the open above left the cursor.
            BoundedRead::Keys(keys) => {
                for key in keys.chunks_exact(stride) {
                    cursor.seek_pk_group(key);
                    cursor.for_each_pk_group_row(key, &mut visit);
                }
            }
            BoundedRead::All | BoundedRead::Range(..) => {
                while cursor.valid {
                    visit(&cursor);
                    cursor.advance();
                }
            }
        }
        // Not borrowck (a `ReadCursor` owns its runs by `Rc`): an early free, so
        // the merge tree is gone before `hydrate_keys` allocates its replay.
        drop(cursor);

        // The cursor emits strictly ascending (PK, payload) with net weights and
        // drops ghosts, and `visit` keeps only positive ones — so the walk's output
        // is consolidated as built, and saying so is what lets the union below take
        // its O(n) merge instead of a full sort of the hydrated relation.
        out.certify_layout(crate::storage::Layout::Consolidated, &schema);
        if coarse.is_empty() {
            return Ok(out);
        }
        let chunk_rows = self.ddl_scan_chunk_rows.max(1);
        let hydrated = self.dag.hydrate_keys(view_id, skeleton_keys, &coarse, chunk_rows)?;
        // The fully-dehydrated store is what the feature exists for, and
        // `op_union`'s empty-`a` arm is a whole `clone_batch` of the other side.
        if out.count == 0 {
            return Ok(hydrated);
        }
        // Disjoint by PK, so the merge emits a consolidated batch — which
        // `op_union` cannot certify in general and `into_consolidated` would
        // therefore re-fold through a second full arena.
        let mut merged = crate::ops::op_union(out, &hydrated, &schema);
        merged.certify_layout(crate::storage::Layout::Consolidated, &schema);
        Ok(merged)
    }

    /// Point lookup by the wire seek pair. Decodes `(seek_pk, seek_pk_extra)` to
    /// the OPK key at any PK width via `seek_opk_bytes`, then seeks.
    /// Returns the hit (if any) plus the table's schema descriptor — a miss
    /// still needs the schema for its STATUS_OK reply block.
    pub fn seek_family(
        &mut self,
        table_id: i64,
        seek_pk: u128,
        seek_pk_extra: &[u8],
    ) -> Result<(Option<Batch>, SchemaDescriptor), String> {
        let entry = self.table_entry(table_id)?;
        let schema = entry.schema;
        let opk = crate::schema::key::seek_opk_bytes(&schema, seek_pk, seek_pk_extra)?;
        Ok((self.seek_family_bytes(table_id, opk.pk_bytes())?, schema))
    }

    /// Byte-keyed [`seek_family`] — the primitive both spellings resolve to, for
    /// callers that already hold the OPK bytes.
    pub(crate) fn seek_family_bytes(&mut self, table_id: i64, pk: &[u8]) -> Result<Option<Batch>, String> {
        let entry = self.table_entry(table_id)?;
        if entry.needs_hydration() {
            // One key is a one-element key list, so the hydrating read is the same
            // walk every other one takes — it just hydrates at most one key rather
            // than the store.
            let b = self.materialize_bounded_store(table_id, BoundedRead::Keys(pk))?;
            return Ok((b.count > 0).then_some(b));
        }
        Ok(Self::seek_entry_bytes(entry, pk))
    }

    /// The seek+materialise primitive: open a cursor over this worker's store and
    /// copy every live row of `pk`'s group. Correct at any PK width. A base
    /// table's PK is unique (`enforce_unique_pk` on ingest) so this emits one row;
    /// a view output store enforces nothing, and a synthetic view key
    /// (`_join_pk`) names one row per row the join produced for it — walking the
    /// group is what makes a seek answer the same rows a point-range read of that
    /// key does.
    fn seek_entry_bytes(entry: &crate::query::TableEntry, pk: &[u8]) -> Option<Batch> {
        let mut cursor = entry.open_cursor_in_range(pk, Some(pk));
        let mut batch = Batch::empty_with_schema(&entry.schema);
        cursor.copy_live_pk_group_into(pk, &mut batch);
        (batch.count > 0).then_some(batch)
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
    pub fn gather_family_bytes<'k>(
        &mut self,
        table_id: i64,
        pks: impl ExactSizeIterator<Item = &'k [u8]>,
        ref_col: u8,
    ) -> Result<(Batch, SchemaDescriptor), String> {
        let entry = self.table_entry(table_id)?;
        let schema = entry.schema;
        let result_schema =
            project_schema(&schema, &[ref_col as u32]).expect("a one-column projection fits MAX_COLUMNS");
        // The column is master-picked and is never a PK column (the FK rules
        // gather only a non-PK referenced column), which this rejects: a PK
        // `ref_col` would be skipped by `project_schema` and leave the reply
        // payload-less.
        let ci = ref_col as usize;
        let pi = schema.try_payload_idx(ci).expect("FK projection excludes PK columns");
        let col_size = schema.columns[ci].size() as usize;
        // The master SCATTERS the key list, so `pks` is this worker's own sublist
        // and `pks.len()` is a tight bound, not a W× over-allocation.
        let mut out = Batch::with_capacity(result_schema, pks.len());
        let mut cursor = entry.open_cursor();
        for pk in pks {
            if cursor.advance_to_exact_live(pk) {
                copy_cursor_col_to_batch(&cursor, &mut out, ci, pi, col_size);
            }
        }
        Ok((out, result_schema))
    }

    /// Index-assisted lookup: the source rows whose leading `natives.len()`
    /// indexed columns equal `natives` (`natives.len()` may be
    /// `< col_indices.len()` for a leading-prefix scan).
    ///
    /// An equality seek IS the degenerate point range on the last supplied
    /// column — by the OPK group-key property its cuts bound exactly the whole
    /// duplicate group of the full supplied prefix — so this delegates to
    /// [`Self::seek_by_index_range`]: one walk/gather mechanism under the
    /// point seek, the range seek, and the bounded backfill.
    ///
    /// Rows with a NULL in ANY indexed column are absent from the index
    /// (`batch_project_index` skips them), so a prefix scan returns only rows
    /// whose trailing indexed columns are all non-NULL — the SQL planner must
    /// not serve a prefix predicate from an index whose uncovered trailing
    /// columns are nullable. (The gather's full-arity entry filter re-applies
    /// that gate; see `BoundedIndexCursor::drain_chunk`.)
    pub fn seek_by_index(
        &mut self,
        table_id: i64,
        col_indices: &[u32],
        natives: &[u128],
    ) -> Result<(Option<Batch>, SchemaDescriptor), String> {
        let (&last, eq) = natives
            .split_last()
            .ok_or_else(|| "seek_by_index: no key values supplied".to_string())?;
        let range = gnitz_wire::RangeDescriptor::point(eq, last);
        self.seek_by_index_range(table_id, col_indices, &range)
    }

    /// Ordered range scan over a secondary index: the leading
    /// `range.eq_vals().len()` columns are equality-pinned, and the next index
    /// column is bounded by the descriptor's half-open cut interval
    /// `[start, end)`. The cut → byte-key mapping and its correctness argument
    /// live on [`index_range_keys`]; the walk is then uniform — seek to `start`,
    /// advance while `key < end`. SQL bound semantics (inclusivity,
    /// unboundedness, out-of-range saturation) are resolved to cuts in the
    /// planner; none of them reach this layer.
    ///
    /// Returns this worker's matching source rows; the master broadcasts to every
    /// worker and merges (the index shadows this worker's own base slice, so a
    /// range's matches scatter across workers).
    pub(crate) fn seek_by_index_range(
        &mut self,
        table_id: i64,
        col_indices: &[u32],
        range: &gnitz_wire::RangeDescriptor,
    ) -> Result<(Option<Batch>, SchemaDescriptor), String> {
        let src_schema = self.table_entry(table_id)?.schema;
        // The wire seek IS one unchunked drain of the bounded cursor — the same
        // walk/gather the backfill scan drives chunk-wise, so the two paths
        // cannot diverge on the weight-consolidation subtleties. A provably-empty
        // range drains `None`; the `.filter` maps the cursor's `Some(empty)`
        // ("in-range entries, none resolved") back to this API's `None` too.
        let mut cur = self.open_index_source(table_id, col_indices, range, false)?;
        Ok((cur.drain_chunk(usize::MAX).filter(|b| b.count > 0), src_schema))
    }

    /// Cursor-returning sibling of `scan_family` for callers that stream the
    /// relation chunk-wise (`drain_chunk`) instead of materializing it whole.
    /// `None` when the table is unregistered — callers treat that as empty.
    /// User tables only; system tables keep `scan_family`.
    ///
    /// The handle owns its sources via `Rc`, so it stays valid while the
    /// caller mutates OTHER relations (index table, view family) between
    /// chunks; the scanned relation itself must not be written mid-loop.
    pub fn open_store_cursor(&self, table_id: i64) -> Option<ReadCursor> {
        self.dag.tables.get(&table_id).map(|e| e.open_cursor())
    }

    /// The source cursor for driving `source` through `view_id`'s circuit: an
    /// index-bounded cursor when the compiled plan pushed a bound down and
    /// [`Self::open_index_source`]'s gate takes it, else the full-scan cursor.
    /// The circuit's `Filter` is authoritative either way, so the choice only
    /// decides how many rows are read. The open may COMPILE the view.
    ///
    /// A registered-but-empty table yields a cursor, and a provably empty range
    /// yields `SourceCursor::Empty` — collapsing that into an error would skip
    /// the source rather than feed it one empty epoch. `Err` is a view that does
    /// not compile, or an unregistered source: DDL_SYNC applies in SAL order, so
    /// a worker that cannot see the source has diverged from the catalog.
    pub fn open_source_cursor(&mut self, view_id: i64, source: i64) -> Result<SourceCursor, String> {
        // A store-less handle opens an EMPTY cursor, not an error — right for a
        // stream, silently wrong for the post-fork master. Hard, not
        // `debug_assert!`: release is a supported deployment, one compare per
        // backfill source.
        assert!(
            self.owns_stores,
            "source cursor in a process owning no base store (view {view_id}, source {source})",
        );
        // Must precede `source_scan_bound`: `handle_backfill` reaches here before
        // anything compiles the view, and an uncached plan would silently report
        // "no bound" — the motivating GROUP BY case would full-scan invisibly.
        // Cache-first and idempotent.
        let bound = self
            .dag
            .ensure_compiled(view_id)?
            .then(|| self.dag.source_scan_bound(view_id, source))
            .flatten();
        let Some(bound) = bound else {
            return Ok(SourceCursor::Full(Box::new(self.table_entry(source)?.open_cursor())));
        };
        self.open_index_source(source, bound.idx_cols.as_slice(), &bound.desc, true)
    }

    /// The one index-bounded source cursor opener: the walk over `desc` on `cols`
    /// of `source`, `SourceCursor::Empty` for a provably-empty range, and — under
    /// `gate` — the full-scan cursor when [`Self::open_index_range`]'s cost model
    /// declines.
    ///
    /// `gate` is "the caller re-imposes the range itself" (a circuit's `Filter`,
    /// a ScanSpec residual), which is what makes degrading to a full scan a
    /// performance choice. Ungated, a decline is surfaced instead — that caller
    /// has nothing left to re-filter a full scan with.
    pub(crate) fn open_index_source(
        &self,
        source: i64,
        cols: &[u32],
        desc: &gnitz_wire::RangeDescriptor,
        gate: bool,
    ) -> Result<SourceCursor, String> {
        match self.open_index_range(source, cols, desc, gate) {
            IndexScan::Cursor(c) => Ok(SourceCursor::Bounded(c)),
            IndexScan::Empty => Ok(SourceCursor::Empty),
            IndexScan::Decline(_) if gate => Ok(SourceCursor::Full(Box::new(self.table_entry(source)?.open_cursor()))),
            IndexScan::Decline(e) => Err(e),
        }
    }

    /// The one place an index-bounded walk is opened: resolve the circuit, encode
    /// the range bounds, open the index cursor and measure the range — each
    /// exactly once. `gate` additionally applies the cost model below, declining
    /// an unselective range before the base cursor is opened.
    ///
    /// Index cursor before base cursor: `ingest_store_and_indices` writes
    /// base-then-index non-atomically, so snapshotting the index no later than
    /// the base is what makes every entry the walk yields already have its base
    /// row written.
    ///
    /// The cost model, and the only one: a bounded scan is not unconditionally
    /// cheaper. For a range matching M of N rows it costs an index walk of M, an
    /// M log M sort, and M galloping base probes, where a full scan is one
    /// sequential columnar drain of N — so it loses badly as M → N
    /// (`WHERE indexed > 0` matches everything). M is not estimated:
    /// `count_range_raw` measures it exactly in O(sources × log N) off the index
    /// cursor already open, and repositions nothing. N comes from `estimated_rows` —
    /// arithmetic over the children's run and shard counts — rather than a
    /// cursor's `estimated_length`, so the base cursor is never opened
    /// speculatively. M also sizes the walk's per-chunk PK scratch exactly,
    /// gated or not.
    fn open_index_range(
        &self,
        table_id: i64,
        col_indices: &[u32],
        range: &gnitz_wire::RangeDescriptor,
        gate: bool,
    ) -> IndexScan {
        let entry = match self.table_entry(table_id) {
            Ok(e) => e,
            Err(e) => return IndexScan::Decline(e),
        };
        // The index was dropped since the plan compiled.
        let Some(ic) = entry.index_circuit_on(col_indices) else {
            return IndexScan::Decline(format!("No index on cols {col_indices:?} for table {table_id}"));
        };
        let (start, end) = match index_range_keys(ic, range) {
            Ok(Some(keys)) => keys,
            Ok(None) => return IndexScan::Empty,
            // Malformed: `n_eq` pins every column with no range column left.
            Err(e) => return IndexScan::Decline(e),
        };
        let end_bytes = end.as_ref().map(|e| e.pk_bytes());
        let idx = ic.table_mut().open_cursor_in_range(start.pk_bytes(), end_bytes);
        let matches = idx.count_range_raw(start.pk_bytes(), end_bytes);
        if gate {
            // Only user base tables own index circuits, so a resolved index
            // implies an owned base store; a borrowed system table degrades to
            // the full scan like any other decline.
            let Some(store) = entry.handle.as_owned() else {
                return IndexScan::Decline("index owner holds no local base store".into());
            };
            if matches > store.estimated_rows() / INDEX_SCAN_RATIO {
                return IndexScan::Decline("index range is not selective enough to pay for the walk".into());
            }
        }
        IndexScan::Cursor(Box::new(BoundedIndexCursor::new(
            idx,
            entry.open_cursor(),
            start,
            end,
            ic.key_spec,
            matches.min(self.ddl_scan_chunk_rows),
        )))
    }
}

/// Use the index only when its range covers at most `1/INDEX_SCAN_RATIO` of the
/// local base slice.
const INDEX_SCAN_RATIO: usize = 16;

/// The outcome of [`CatalogEngine::open_index_range`]. The cursor is boxed
/// where it is built, so it reaches `SourceCursor::Bounded` without a second
/// allocation.
enum IndexScan {
    Cursor(Box<BoundedIndexCursor>),
    /// The range is provably empty — there is nothing to read either way.
    Empty,
    /// No walk: no such table or index, a malformed descriptor, or — only under
    /// `gate` — an unselective range or an unowned base store. Under `gate` the
    /// opener answers this with a full scan; ungated it surfaces the message.
    Decline(String),
}

/// The half-open OPK key range `[start, end)` for `range` over `ic`'s index, each
/// key exactly `ic.index_schema.pk_stride()` bytes — [`eq_prefix_range_keys`]
/// with the index-specific group-prefix encoder.
///
/// The prefix is encoded through the circuit's baked spec, the same path the
/// write side uses (`write_span` / `batch_project_index`), so the two are
/// byte-identical by construction.
///
/// Correctness rests on the OPK ordering invariant: the index PK region is
/// `[promoted leading-key OPK ‖ source-PK OPK]` and memcmp order on those bytes
/// equals typed order (signed and composite included). For any `prefix_len`-byte
/// group key `p`, every full key `k` with `k[..prefix_len] == p` satisfies
/// `pad(p) ≤ k < pad(succ(p))`, so a cut key includes or excludes whole duplicate
/// groups with no per-row inclusivity test.
fn index_range_keys(
    ic: &crate::query::IndexCircuitEntry,
    range: &gnitz_wire::RangeDescriptor,
) -> Result<Option<(crate::schema::key::PkBuf, Option<crate::schema::key::PkBuf>)>, String> {
    crate::schema::key::eq_prefix_range_keys(
        range,
        ic.col_indices.as_slice().len(),
        ic.index_schema.pk_stride() as usize, // leading + source PK
        "index range",
        |natives| ic.key_spec.seek_prefix(natives),
    )
}

/// Projecting sibling of `ReadCursor::copy_current_row_into`: append the cursor's
/// current row to `out` (which has the one-column `project_schema` layout) with
/// weight 1, copying only column `ci` — whose caller-resolved source payload
/// slot is `pi` and whose width is `col_size`. The projected row's single
/// payload column and null bit 0 mirror that source column. The column is
/// scalar, so no blob relocation is required.
fn copy_cursor_col_to_batch(cursor: &ReadCursor, out: &mut Batch, ci: usize, pi: usize, col_size: usize) {
    // `current_pk_bytes()` is the verbatim OPK PK region for any width, and the
    // read cursor always tracks it regardless of stride. For narrow PKs it
    // equals `widen_pk_be(current_pk_bytes) == current_key_narrow()`; for wide
    // PKs it is the only PK form, so one path serves both.
    out.extend_pk_bytes(cursor.current_pk_bytes());
    out.extend_weight(&1i64.to_le_bytes());

    // The regions are independent append buffers, so the null word can be
    // appended after the column data.
    let mut proj_null = 0u64;
    if gnitz_wire::null_word_get(cursor.current_null_word, pi) {
        gnitz_wire::null_word_set(&mut proj_null, 0, true);
    }
    match cursor.col_bytes(ci, col_size) {
        Some(data) => out.extend_col(0, data),
        None => out.fill_col_zero(0, col_size),
    }
    out.extend_null_bmp(&proj_null.to_le_bytes());
    out.count += 1;
}
