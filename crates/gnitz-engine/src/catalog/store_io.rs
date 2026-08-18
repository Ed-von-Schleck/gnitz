//! Server-facing I/O on table families — ingest, scan, point/range seek
//! (including secondary-index lookup), and the multi-phase flush/replay
//! paths. System tables route through the catalog write path; user tables
//! delegate to `DagEngine`.

use super::*;
use crate::schema::project_schema;
use crate::storage::{BoundedIndexCursor, PkSetGather};

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
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: Batch) -> Result<Batch, String> {
        if table_id < FIRST_USER_TABLE_ID {
            return Err("ingest_returning_effective not supported for system tables".to_string());
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
                return Err(format!(
                    "push for table_id={table_id} carries {} payload columns, table schema has {}",
                    batch.num_payload_cols(),
                    schema.num_payload_cols()
                ));
            }
        }
        self.dag
            .ingest_returning_effective(table_id, batch)
            .ok_or_else(|| format!("ingest failed for table_id={table_id}: not registered"))
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

        let mut cursor = entry.open_cursor();
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
        let mut cursor = entry.open_cursor();
        let mut batch = Batch::empty_with_schema(&entry.schema);
        cursor.copy_live_pk_group_into(pk, &mut batch);
        (batch.count > 0).then_some(batch)
    }

    /// Batched point lookup for the FK parent probe. Seek each PK in `pks`
    /// (verbatim OPK bytes) in this worker's store, appending the stored row at
    /// weight 1 for every present, live key into a result batch projected to
    /// `project` (parent column indices, all non-PK scalar). Absent / retracted
    /// keys contribute nothing, and passing `pks` ascending keeps the cursor's
    /// probes monotonic.
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
        project: &[u8],
    ) -> Result<Batch, String> {
        let entry = self.table_entry(table_id)?;
        let schema = entry.schema;
        let result_schema = project_schema(&schema, project);
        // Resolve the fixed projection once — `(col_idx, payload_slot, size)`
        // per projected column — instead of re-deriving payload index and
        // column size per row per column inside the seek loop. The projection
        // is master-built and excludes PK columns (the FK rules gather only a
        // non-PK referenced column; `project_schema` asserts it one frame up),
        // so every projected column has a payload slot.
        let proj: Vec<(usize, usize, usize)> = project
            .iter()
            .map(|&p| {
                let ci = p as usize;
                let pi = schema.try_payload_idx(ci).expect("FK projection excludes PK columns");
                (ci, pi, schema.columns[ci].size() as usize)
            })
            .collect();
        let mut out = Batch::with_capacity(result_schema, pks.len());
        // The cursor also drops the keys this process holds no row for — the
        // master broadcasts the list, so most of it belongs elsewhere.
        let mut cursor = entry.open_cursor();
        for pk in pks {
            if cursor.advance_to_exact_live(pk) {
                copy_cursor_cols_to_batch(&cursor, &mut out, &proj);
            }
        }
        Ok(out)
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
        let Some(mut cur) = self.open_index_range_cursor(table_id, col_indices, range, 0)? else {
            return Ok((None, src_schema));
        };
        Ok((cur.drain_chunk(usize::MAX).filter(|b| b.count > 0), src_schema))
    }

    /// Open an un-gated streaming cursor over the secondary-index range `range`
    /// on `col_indices` of `table_id`. `Ok(None)` = the range is provably empty
    /// (a `+∞` start, or an inverted / zero-width interval); `Err` = the
    /// descriptor pins every column with no range column left (a trust-boundary
    /// rejection). No selectivity gate and no residual — the byte-exact OPK walk
    /// yields exactly the in-range source rows, so callers needing every match
    /// (the point/range seek, an `exact` ScanSpec index bound) drive this directly.
    ///
    /// `pk_capacity` sizes the walk's per-chunk PK scratch; `0` lets it grow.
    ///
    /// The one place an index-bounded cursor is built, so the write-ordering
    /// guarantee of the non-atomic base-then-index write path lives here alone:
    /// the index cursor snapshots first and the base cursor after, which means
    /// every entry the walk yields already had its base row written.
    pub(crate) fn open_index_range_cursor(
        &mut self,
        table_id: i64,
        col_indices: &[u32],
        range: &gnitz_wire::RangeDescriptor,
        pk_capacity: usize,
    ) -> Result<Option<BoundedIndexCursor>, String> {
        let (entry, ic) = self.table_and_index(table_id, col_indices)?;
        let Some((start, end)) = index_range_keys(ic, range)? else {
            return Ok(None);
        };
        let idx = ic.table_mut().open_cursor();
        let src = entry.open_cursor();
        Ok(Some(BoundedIndexCursor::new(
            idx,
            src,
            start,
            end,
            ic.key_spec,
            pk_capacity,
        )))
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

    /// Cursor-returning sibling of `scan_family` for callers that stream the
    /// relation chunk-wise (`drain_chunk`) instead of materializing it whole.
    /// `None` when the table is unregistered — callers treat that as empty.
    /// User tables only; system tables keep `scan_family`.
    ///
    /// The handle owns its sources via `Rc`, so it stays valid while the
    /// caller mutates OTHER relations (index table, view family) between
    /// chunks; the scanned relation itself must not be written mid-loop.
    pub(crate) fn open_store_cursor(&self, table_id: i64) -> Option<ReadCursor> {
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
    /// `None` iff the source table is unregistered, which callers treat as "skip
    /// this source". A registered-but-empty table yields `Some`, and a provably
    /// empty range yields `Some(SourceCursor::Empty)` — collapsing that into
    /// `None` would skip the source rather than feed it one empty epoch.
    pub(crate) fn open_source_cursor(&mut self, view_id: i64, source: i64) -> Option<SourceCursor> {
        // Must precede `source_scan_bound`: `handle_backfill` reaches here before
        // anything compiles the view, and an uncached plan would silently report
        // "no bound" — the motivating GROUP BY case would full-scan invisibly.
        // Cache-first and idempotent.
        let bound = self
            .dag
            .ensure_compiled(view_id)
            .then(|| self.dag.source_scan_bound(view_id, source))
            .flatten();
        let Some(bound) = bound else {
            return self.full_source(source);
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

        self.open_bounded_source(source, bound.idx_cols.as_slice(), &bound.desc)
    }

    /// The index-bounded source cursor for the range `desc` on `idx_cols` of
    /// `source`, gated by [`Self::index_scan_verdict`]; the full-scan cursor
    /// when the gate declines, and `SourceCursor::Empty` for a provably-empty
    /// range. Every non-`Bounded` outcome is a performance choice, never a
    /// correctness one — the caller's authoritative filter (a circuit's `Filter`,
    /// a ScanSpec's residual predicate) re-imposes the range — so the non-`exact`
    /// ScanSpec index bound and the circuit backfill share this one gate.
    ///
    /// `None` iff `source` is unregistered, which callers treat as "skip this
    /// source".
    pub(crate) fn open_bounded_source(
        &mut self,
        source: i64,
        idx_cols: &[u32],
        desc: &gnitz_wire::RangeDescriptor,
    ) -> Option<SourceCursor> {
        let m = match self.index_scan_verdict(source, idx_cols, desc) {
            IndexScan::Use(m) => m.min(self.ddl_scan_chunk_rows),
            IndexScan::Empty => return Some(SourceCursor::Empty),
            IndexScan::Decline => return self.full_source(source),
        };
        match self.open_index_range_cursor(source, idx_cols, desc, m) {
            Ok(Some(c)) => Some(SourceCursor::Bounded(Box::new(c))),
            Ok(None) => Some(SourceCursor::Empty),
            Err(_) => self.full_source(source),
        }
    }

    /// The only cost model. A bounded scan is not unconditionally cheaper: for a
    /// range matching M of N rows it costs an index walk of M, an M log M sort,
    /// and M galloping base probes, where a full scan is one sequential columnar
    /// drain of N — so it loses badly as M → N (`WHERE indexed > 0` matches
    /// everything). M is not estimated: it is measured exactly in O(log N) before
    /// the first row is read (`count_range_raw` is `&self` and repositions
    /// nothing). N comes from `estimated_rows` — arithmetic over the children's
    /// run and shard counts — rather than a cursor's `estimated_length`, so the
    /// base cursor is never opened speculatively.
    fn index_scan_verdict(&self, source: i64, idx_cols: &[u32], desc: &gnitz_wire::RangeDescriptor) -> IndexScan {
        // The index was dropped since the plan compiled, or `n_eq` pins every
        // column with no range column left.
        let Ok((entry, ic)) = self.table_and_index(source, idx_cols) else {
            return IndexScan::Decline;
        };
        let keys = match index_range_keys(ic, desc) {
            Ok(Some(keys)) => keys,
            Ok(None) => return IndexScan::Empty,
            Err(_) => return IndexScan::Decline,
        };
        // Only user base tables own index circuits, so a resolved index implies an
        // owned base store; a borrowed system table degrades to the full scan.
        let Some(store) = entry.handle.as_owned_mut() else {
            return IndexScan::Decline;
        };
        let (start, end) = keys;
        let m = ic
            .table_mut()
            .open_cursor()
            .count_range_raw(start.pk_bytes(), end.as_ref().map(|e| e.pk_bytes()));
        if m > store.estimated_rows() / INDEX_SCAN_RATIO {
            return IndexScan::Decline;
        }
        IndexScan::Use(m)
    }
}

/// Use the index only when its range covers at most `1/INDEX_SCAN_RATIO` of the
/// local base slice.
const INDEX_SCAN_RATIO: usize = 16;

/// What the cost model says about an index-bounded scan of one range.
enum IndexScan {
    /// Walk the index; the value is the exactly measured range size.
    Use(usize),
    /// The range is provably empty — there is nothing to read either way.
    Empty,
    /// Full-scan instead: no usable index, no owned base store, or the range is
    /// not selective enough to pay for the walk.
    Decline,
}

/// A chunked source of `Batch`es over one relation, in every shape a bound can
/// take. Interchangeable by construction for the circuit backfill: the circuit's
/// `Filter` decides what the view contains, so which variant is chosen only
/// decides how many rows the scan reads. The ad-hoc `ReadSpec` scan adds the
/// `PkSet` shape and drives the same enum.
///
/// Every variant is boxed (a `ReadCursor` is ~560 bytes; clippy's
/// `large_enum_variant`) — one allocation per scan, never per chunk.
pub(crate) enum SourceCursor {
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
    pub(crate) fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
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
/// current row to `out` (which has the `project_schema` layout) with weight 1,
/// copying only the columns in `proj` — the caller-resolved
/// `(col_idx, payload_slot, size)` triple per projected column. The projected
/// payload column at position `k` corresponds to `proj[k]`; the projected null
/// bit `k` mirrors the source row's null bit for that column. Projected
/// columns are scalar, so no blob relocation is required.
fn copy_cursor_cols_to_batch(cursor: &ReadCursor, out: &mut Batch, proj: &[(usize, usize, usize)]) {
    // `current_pk_bytes()` is the verbatim OPK PK region for any width, and the
    // read cursor always tracks it regardless of stride. For narrow PKs it
    // equals `widen_pk_be(current_pk_bytes) == current_key_narrow()`; for wide
    // PKs it is the only PK form, so one path serves both.
    out.extend_pk_bytes(cursor.current_pk_bytes());
    out.extend_weight(&1i64.to_le_bytes());

    // One pass over the projection: the regions are independent append
    // buffers, so the null word can be appended after the column data.
    let src_null = cursor.current_null_word;
    let mut proj_null = 0u64;
    for (k, &(ci, pi, col_size)) in proj.iter().enumerate() {
        if gnitz_wire::null_word_get(src_null, pi) {
            gnitz_wire::null_word_set(&mut proj_null, k, true);
        }
        match cursor.col_bytes(ci, col_size) {
            Some(data) => out.extend_col(k, data),
            None => out.fill_col_zero(k, col_size),
        }
    }
    out.extend_null_bmp(&proj_null.to_le_bytes());
    out.count += 1;
}
