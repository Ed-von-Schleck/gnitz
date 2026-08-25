//! Server-facing I/O on table families — ingest, scan, point/range seek
//! (including secondary-index lookup), and the multi-phase flush/replay
//! paths. System tables route through the catalog write path; user tables
//! delegate to `DagEngine`.

use super::*;
use crate::schema::project_schema;
use crate::storage::{BoundedIndexCursor, PkSetGather};

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
    /// The registry entry for `table_id`, or the shared "Unknown table_id"
    /// error every hard-resolving store path reports.
    pub(crate) fn table_entry(&self, table_id: i64) -> Result<&crate::query::TableEntry, String> {
        self.dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))
    }

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
        if let Some(schema) = self.get_schema_desc(table_id) {
            if batch.num_payload_cols() != schema.num_payload_cols() {
                return Err(IngestError::Rejected(format!(
                    "push for table_id={table_id} carries {} payload columns, table schema has {}",
                    batch.num_payload_cols(),
                    schema.num_payload_cols()
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

        // Tested *before* any copy: `copy_current_row_into` reads every payload
        // column of the row and relocates its German-string blobs, which a
        // skeleton shard has no bytes for.
        let mut visit = |c: &ReadCursor, out: &mut Batch| {
            if c.current_weight <= 0 {
                return;
            }
            if c.current_is_skeleton() {
                skeleton_keys.extend_from_slice(c.current_pk_bytes());
                coarse.push(c.current_weight);
            } else {
                c.copy_current_row_into(out, c.current_weight);
            }
        };

        // A bounded view's terminal partition is the finest in the tree, and this
        // is the read that meets it, so every bounded arm opens over its own range.
        let mut cursor = match read {
            BoundedRead::Keys(keys) => match crate::storage::key_list_range(keys, stride) {
                Some((lo, hi)) => entry.open_cursor_in_range(lo, Some(hi)),
                None => entry.open_cursor(),
            },
            BoundedRead::Range(start, end) => entry.open_cursor_in_range(start, end),
            BoundedRead::All => entry.open_cursor(),
        };
        // Position first, then size the output off the walk's own upper bound so
        // the appends below re-grow as little as possible (each growth re-copies
        // every live byte). For a key list that bound is the key count, which a
        // view's synthetic PK can exceed — one key names a whole group there.
        let cap = match read {
            BoundedRead::Keys(keys) => keys.len() / stride,
            BoundedRead::Range(start, end) => {
                cursor.seek_range_bytes(start, end);
                cursor.estimated_length()
            }
            BoundedRead::All => {
                cursor.rewind();
                cursor.estimated_length()
            }
        };
        let mut out = Batch::with_capacity(schema, cap);
        match read {
            // A listed key set walks group by group; a whole store or a range is
            // one sweep from wherever the bound above left the cursor.
            BoundedRead::Keys(keys) => {
                for key in keys.chunks_exact(stride) {
                    cursor.seek_pk_group(key);
                    cursor.for_each_pk_group_row(key, |c| visit(c, &mut out));
                }
            }
            BoundedRead::All | BoundedRead::Range(..) => {
                while cursor.valid {
                    visit(&cursor, &mut out);
                    cursor.advance();
                }
            }
        }
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
        Ok(crate::ops::op_union(out, &hydrated, &schema).into_consolidated(&schema))
    }

    /// Point lookup by the wire seek pair. Decodes `(seek_pk, seek_pk_extra)` to
    /// the OPK key at any PK width via `seek_opk_bytes`, then seeks — resolving
    /// the registry entry once.
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
        let result_schema = project_schema(&schema, &[ref_col]);
        // Resolve the projection once — payload slot and column size — instead
        // of re-deriving both per row inside the seek loop. The column is
        // master-picked and is never a PK column (the FK rules gather only a
        // non-PK referenced column; `project_schema` asserts it one frame up),
        // so it has a payload slot.
        let ci = ref_col as usize;
        let pi = schema.try_payload_idx(ci).expect("FK projection excludes PK columns");
        let col_size = schema.columns[ci].size() as usize;
        let mut out = Batch::with_capacity(result_schema, pks.len());
        // The cursor also drops the keys this process holds no row for — the
        // master broadcasts the list, so most of it belongs elsewhere.
        let mut cursor = entry.open_cursor();
        for pk in pks {
            if cursor.advance_to_exact_live(pk) {
                copy_cursor_col_to_batch(&cursor, &mut out, ci, pi, col_size);
            }
        }
        Ok((out, result_schema))
    }

    /// Resolve the `(table entry, index circuit)` pair for an index seek on
    /// `(table_id, cols)`, with the shared unknown-table / no-index errors.
    fn table_and_index(
        &self,
        table_id: i64,
        cols: &[u32],
    ) -> Result<(&crate::query::TableEntry, &crate::query::IndexCircuitEntry), String> {
        let entry = self.table_entry(table_id)?;
        let ic = entry
            .index_circuit_on(cols)
            .ok_or_else(|| format!("No index on cols {cols:?} for table {table_id}"))?;
        Ok((entry, ic))
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
    /// that gate; see `gather_source_rows`.)
    pub fn seek_by_index(
        &mut self,
        table_id: i64,
        col_indices: &[u32],
        natives: &[u128],
    ) -> Result<(Option<Batch>, SchemaDescriptor), String> {
        let (&last, eq) = natives
            .split_last()
            .ok_or_else(|| "seek_by_index: no key values supplied".to_string())?;
        let range = gnitz_wire::RangeDescriptor::new(eq, gnitz_wire::Cut::Before(last), gnitz_wire::Cut::After(last));
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
    pub fn seek_by_index_range(
        &mut self,
        table_id: i64,
        col_indices: &[u32],
        range: &gnitz_wire::RangeDescriptor,
    ) -> Result<(Option<Batch>, SchemaDescriptor), String> {
        let src_schema = self.table_entry(table_id)?.schema;
        // The wire seek IS one unchunked drain of the bounded cursor — the same
        // walk/gather the backfill scan drives chunk-wise, so the two paths
        // cannot diverge on the weight-consolidation subtleties. `Ok(None)` from
        // the opener is a provably-empty range (a `+∞` start, or an inverted /
        // zero-width interval); the `.filter` maps the cursor's `Some(empty)`
        // ("in-range entries, none resolved") back to this API's `None`.
        let Some(mut cur) = self.open_index_range_cursor(table_id, col_indices, range)? else {
            return Ok((None, src_schema));
        };
        Ok((cur.drain_chunk(usize::MAX).filter(|b| b.count > 0), src_schema))
    }

    /// Open an un-gated streaming cursor over the secondary-index range `range`
    /// on `col_indices` of `table_id`. `Ok(None)` = the range is provably empty
    /// (a `+∞` start, or an inverted / zero-width interval); `Err` = no such
    /// table or index, or a descriptor that pins every column with no range
    /// column left. No selectivity gate and no residual — the byte-exact OPK walk
    /// yields exactly the in-range source rows, so callers needing every match
    /// (the point/range seek, an `exact` ScanSpec index bound) drive this directly.
    pub(crate) fn open_index_range_cursor(
        &self,
        table_id: i64,
        col_indices: &[u32],
        range: &gnitz_wire::RangeDescriptor,
    ) -> Result<Option<Box<BoundedIndexCursor>>, String> {
        match self.open_index_range(table_id, col_indices, range, false) {
            IndexScan::Cursor(c) => Ok(Some(c)),
            IndexScan::Empty => Ok(None),
            IndexScan::Decline(e) => Err(e),
        }
    }

    /// Flush one relation's memtable and its index tables. The flush compacts
    /// too: publishing a shard is what makes compaction due, so L0 cannot
    /// accumulate across a DDL-heavy session without the flush that grew it also
    /// bounding it. Registry-uniform — a system family's `Borrowed` handle
    /// reaches the same `Table` its `sys_stores` box holds. Production flushes
    /// the whole catalog at once through `flush_all_system_tables`.
    pub fn flush_family(&mut self, table_id: i64) -> Result<(), String> {
        self.dag
            .flush(table_id)
            .map_err(|e| format!("flush failed for table_id={table_id}: {e}"))
    }

    /// Worker DDL sync: apply a master-broadcast system-table delta. Workers
    /// update their registry from these; durability is master-side (fsynced
    /// SAL + the master's own system-table flush). The worker's inherited copy
    /// lives in RAM (memtable + the RAM tier) and is never flushed by the
    /// worker — only the master writes `_sys/` shards.
    ///
    /// Delegates to `apply_local` — the one ingest tail every path shares —
    /// so hooks run AFTER the storage write here too; `hook_index_register`'s
    /// DROP branch depends on the −1 row being applied before its remaining-
    /// uniqueness rescan. Errors propagate into the worker's DdlSync-fatal
    /// path (dispatch treats a DdlSync error as fatal: STATUS_ERROR + shutdown
    /// and _exit, which the master's watchdog turns into a cluster abort) — a
    /// swallowed failure here diverges this worker's catalog from the master.
    pub fn ddl_sync(&mut self, table_id: i64, mut batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id).ok_or_else(|| "ddl_sync only for system tables".to_string())?;
        self.apply_local(family, &mut batch, None)
    }

    /// Drop `relation_id` from a catalog this process owns alone — [`Self::ddl_sync`]
    /// in reverse, for the mirror. A production DROP arrives as a wire delta the
    /// executor prechecks and broadcasts, and never comes through here.
    ///
    /// Builds only the relation's own row: its `-1` fires `hook_relation_register`,
    /// whose cascade retracts the columns, indices and circuit rows and queues the
    /// directory. Retracting a child here too would leave it at net `-1`, where the
    /// next registration's `+1` sums to zero. The cascade's broadcast queue has no
    /// worker to reach, so it is discarded rather than left to grow.
    pub fn retract_relation_registration(&mut self, relation_id: i64) -> Result<(), String> {
        let family = match self.table_entry(relation_id)?.kind {
            RelationKind::View => SysFamily::View,
            RelationKind::BaseTable | RelationKind::Stream => SysFamily::Table,
            RelationKind::SystemCatalog => {
                return Err(format!("table_id {relation_id} is a system table"));
            }
        };
        let schema = family.schema();
        let batch = retract_single_row(self.sys_store(family), &schema, relation_id as u128);
        if batch.count > 0 {
            self.ddl_sync(family.id(), batch)?;
        }
        self.drain_pending_broadcasts();
        Ok(())
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

    /// The whole-relation source cursor for `source` — what every non-`Bounded`
    /// verdict below falls back to. `None` iff `source` is unregistered.
    fn full_source(&self, source: i64) -> Option<SourceCursor> {
        Some(SourceCursor::Full(Box::new(self.open_store_cursor(source)?)))
    }

    /// The source cursor for driving `source` through `view_id`'s circuit: an
    /// index-bounded cursor when the compiled plan pushed a bound down and
    /// [`Self::open_bounded_source`] takes it, else the full-scan cursor. The
    /// circuit's `Filter` is authoritative either way, so the choice only decides
    /// how many rows are read. The open may COMPILE the view.
    ///
    /// `Ok(None)` iff the source table is unregistered, which callers treat as
    /// "skip this source". A registered-but-empty table yields `Some`, and a
    /// provably empty range yields `Some(SourceCursor::Empty)` — collapsing that
    /// into `None` would skip the source rather than feed it one empty epoch.
    /// `Err` is a view that does not compile.
    pub fn open_source_cursor(&mut self, view_id: i64, source: i64) -> Result<Option<SourceCursor>, String> {
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
            return Ok(self.full_source(source));
        };
        // A bounded cursor is only sound in a process that owns its base store: an
        // index circuit is a local shadow of the local base slice, and where no
        // slice is owned (the master) a bounded cursor returns zero rows rather
        // than an error — the fallbacks below would not catch it and the view would
        // silently fill empty. Hard, not `debug_assert!`: release is a supported
        // deployment, and this costs one compare per bounded backfill, not per row.
        assert!(
            self.owns_stores,
            "bounded source cursor in a process owning no base store (view {view_id}, source {source})",
        );

        Ok(self.open_bounded_source(source, bound.idx_cols.as_slice(), &bound.desc))
    }

    /// The index-bounded source cursor for the range `desc` on `idx_cols` of
    /// `source`, gated by the cost model on [`Self::open_index_range`]; the
    /// full-scan cursor when the gate declines, and `SourceCursor::Empty` for a
    /// provably-empty range. Every non-`Bounded` outcome is a performance choice,
    /// never a correctness one — the caller's authoritative filter (a circuit's
    /// `Filter`, a ScanSpec's residual predicate) re-imposes the range — so the
    /// non-`exact` ScanSpec index bound and the circuit backfill share this one
    /// gate.
    ///
    /// `None` iff `source` is unregistered, which callers treat as "skip this
    /// source".
    pub(crate) fn open_bounded_source(
        &self,
        source: i64,
        idx_cols: &[u32],
        desc: &gnitz_wire::RangeDescriptor,
    ) -> Option<SourceCursor> {
        match self.open_index_range(source, idx_cols, desc, true) {
            IndexScan::Cursor(c) => Some(SourceCursor::Bounded(c)),
            IndexScan::Empty => Some(SourceCursor::Empty),
            IndexScan::Decline(_) => self.full_source(source),
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
    /// `count_range_raw` measures it exactly in O(log N) off the index cursor
    /// already open, and repositions nothing. N comes from `estimated_rows` —
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
        let (entry, ic) = match self.table_and_index(table_id, col_indices) {
            Ok(pair) => pair,
            // Unknown table, or the index was dropped since the plan compiled.
            Err(e) => return IndexScan::Decline(e),
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
    /// `gate` — an unselective range or an unowned base store. The gated caller
    /// answers this with a full scan; the ungated ones surface the message.
    Decline(String),
}

/// A chunked source of `Batch`es over one relation, in every shape a bound can
/// take. Interchangeable by construction for the circuit backfill: the circuit's
/// `Filter` decides what the view contains, so which variant is chosen only
/// decides how many rows the scan reads. The ad-hoc `ReadSpec` scan adds the
/// `PkSet` shape and drives the same enum.
///
/// Every variant is boxed (a `ReadCursor` is ~560 bytes; clippy's
/// `large_enum_variant`) — one allocation per scan, never per chunk.
pub enum SourceCursor {
    Full(Box<ReadCursor>),
    Bounded(Box<BoundedIndexCursor>),
    /// `pk IN (…)` gather over a listed key set.
    PkSet(Box<PkSetGather>),
    /// A provably-empty index range. Distinct from `Full` so nothing is scanned,
    /// and distinct from `open_source_cursor -> None` so the source still feeds
    /// one empty epoch (which is what mints a global aggregate's ground row).
    Empty,
}

impl SourceCursor {
    /// The next source rows, or `None` once the source is exhausted. A returned
    /// batch may be empty — `Bounded` yields one for a window of index entries
    /// whose base rows all resolved away — so only `None` means exhausted.
    ///
    /// `max_rows` bounds every variant exactly except `PkSet`, which tests it
    /// before each key and then drains that key's whole group, so it can
    /// overshoot to `max_rows - 1 + |largest group|`. Callers read `chunk.count`.
    pub fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_chunk(max_rows),
            SourceCursor::Bounded(c) => c.drain_chunk(max_rows),
            SourceCursor::PkSet(g) => g.next_chunk(max_rows),
            SourceCursor::Empty => None,
        }
    }
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
