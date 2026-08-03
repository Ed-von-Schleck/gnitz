//! Server-facing I/O on table families — ingest, scan, point/range seek
//! (including secondary-index lookup), and the multi-phase flush/replay
//! paths. System tables route through the catalog write path; user tables
//! delegate to `DagEngine`.

use super::*;
use crate::schema::project_schema;
use crate::storage::{BoundedIndexCursor, PartitionProbe};

impl CatalogEngine {
    /// The registry entry for `table_id`, or the shared "Unknown table_id"
    /// error every hard-resolving store path reports.
    pub(crate) fn table_entry(&self, table_id: i64) -> Result<&crate::query::TableEntry, String> {
        self.dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))
    }

    /// The partitioned store behind `table_id`, or `None` when it is
    /// unregistered or a borrowed system table. Every driver of a chunked
    /// [`SourceCursor`] re-reads it per chunk — the cursor cannot hold the
    /// borrow across a `&mut CatalogEngine` use.
    pub(crate) fn partitioned_store(&self, table_id: i64) -> Option<&PartitionedTable> {
        self.dag.tables.get(&table_id).and_then(|e| e.handle.as_partitioned())
    }

    /// Ingest a user-table batch and return the effective delta (after PK
    /// enforcement).  Used by multi-worker push where the worker needs the effective
    /// batch for later DAG evaluation but does NOT evaluate immediately.
    /// System tables are NOT supported (use `ingest_to_family` for those).
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: Batch) -> Result<Batch, String> {
        if table_id < FIRST_USER_TABLE_ID {
            return Err("ingest_returning_effective not supported for system tables".to_string());
        }
        self.dag
            .ingest_returning_effective(table_id, batch)
            .ok_or_else(|| format!("ingest failed for table_id={table_id}: not registered"))
    }

    /// Scan all positive-weight rows from a table. Registry-uniform: system
    /// tables are pre-registered `Borrowed` handles (whose `full_scan`
    /// preserves the `Rc` snapshot cache), so one lookup serves every id —
    /// the CIRCUIT_* tables are SQL-introspectable through it like any other.
    /// Returns the scan plus the table's schema descriptor — the entry is
    /// already resolved here, so the reply path never re-resolves (and
    /// re-copies) the descriptor.
    pub fn scan_family(&mut self, table_id: i64) -> Result<(Rc<Batch>, SchemaDescriptor), String> {
        let entry = self.table_entry(table_id)?;
        Ok((entry.handle.full_scan(), entry.schema))
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
        let opk = crate::schema::key::seek_opk_bytes(&entry.schema, seek_pk, seek_pk_extra)?;
        Ok((Self::seek_entry_bytes(entry, opk.pk_bytes()), entry.schema))
    }

    /// Byte-keyed sibling of [`seek_family`] for callers that already hold the
    /// OPK bytes — only the wide-PK tests seek through it directly.
    /// Registry-uniform: system tables are pre-registered `Borrowed` handles,
    /// so one lookup serves every id.
    #[cfg(test)]
    pub(crate) fn seek_family_bytes(&mut self, table_id: i64, pk: &[u8]) -> Result<Option<Batch>, String> {
        let entry = self.table_entry(table_id)?;
        Ok(Self::seek_entry_bytes(entry, pk))
    }

    /// The seek+materialise primitive: open a cursor over the one partition `pk`
    /// can live in and copy every live row of its PK group. Correct at any PK
    /// width. A base table's PK is unique (`enforce_unique_pk` on ingest) so this
    /// emits one row; a view output store enforces nothing, and a synthetic view
    /// key (`_join_pk`) names one row per row the join produced for it — walking
    /// the group is what makes a seek answer the same rows a point-range read of
    /// that key does. A borrowed system table is one unpartitioned `Table` with
    /// nothing to route among, so it opens whole.
    fn seek_entry_bytes(entry: &crate::query::TableEntry, pk: &[u8]) -> Option<Batch> {
        let mut cursor = entry.handle.open_cursor_for_key(pk)?;
        let mut batch = Batch::empty_with_schema(&entry.schema);
        cursor.copy_live_pk_group_into(pk, &mut batch);
        (batch.count > 0).then_some(batch)
    }

    /// Batched point lookup. Route each PK in `pks` (verbatim OPK bytes) to the
    /// one partition that can hold it and seek there, appending the stored row
    /// (weight 1) for every present, live key into a result batch projected to
    /// `project`. Each `seek` re-probes every source independently, so order is
    /// not required for correctness; passing `pks` ascending keeps each routed
    /// cursor's binary-search probes monotonic for better cache locality.
    /// Absent / retracted keys are skipped — identical to `seek_family`'s
    /// single-key `None` — so a removed PK with no committed row contributes
    /// nothing. `project` lists the parent column indices to return (all
    /// non-PK scalar columns); an empty `project` returns PK-only rows.
    /// Each PK resolves to its group's FIRST live row, which is also its only
    /// one: an FK parent is always a base table (`gnitz-sql` resolves
    /// `fk_table_id` through TABLE_TAB), whose PK `enforce_unique_pk` keeps
    /// unique. The consumer requires that — it indexes the result by PK, so a
    /// second row of a group would overwrite the first rather than join it.
    /// The seek and `pk IN (…)` readers, whose consumers take whole groups, walk
    /// instead.
    ///
    /// Reuses one cursor per touched partition across all keys (cheaper than N
    /// `seek_family` calls, each of which re-opens a cursor). Projection keeps
    /// the result scalar-only — FK-referenced columns are never STRING/BLOB — so
    /// the blob arena is never touched. Works for both narrow and wide PKs: the
    /// OPK bytes are seeked verbatim, with no native→OPK re-encode.
    pub fn gather_family_bytes(
        &mut self,
        table_id: i64,
        pks: &[crate::schema::key::PkBuf],
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
        // The probe also drops the keys this process holds no partition for —
        // the master broadcasts the list, so most of it belongs elsewhere.
        let store = entry.handle.as_partitioned();
        let mut probe = entry.handle.open_probe();
        for pk in pks {
            if let Some(cursor) = probe.advance_to_exact_live(store, pk.pk_bytes()) {
                copy_cursor_cols_to_batch(cursor, &mut out, &proj);
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
            .index_circuits
            .iter()
            .find(|ic| ic.col_indices.as_slice() == cols)
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
    /// worker and merges (the index is partitioned by source PK, so a range's
    /// matches scatter across workers).
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
        // Resolved again rather than threaded out of the opener: the cursor holds
        // no borrow, so the base store is a per-drain argument.
        let store = self
            .partitioned_store(table_id)
            .expect("open_index_range_cursor already required a partitioned store");
        Ok((cur.drain_chunk(store, usize::MAX).filter(|b| b.count > 0), src_schema))
    }

    /// Open an un-gated streaming cursor over the secondary-index range `range`
    /// on `col_indices` of `table_id`. `Ok(None)` = the range is provably empty
    /// (a `+∞` start, or an inverted / zero-width interval); `Err` = the
    /// descriptor pins every column with no range column left (a trust-boundary
    /// rejection). No selectivity gate and no residual — the byte-exact OPK walk
    /// yields exactly the in-range source rows, so callers needing every match
    /// (the point/range seek, an `exact` ScanSpec index bound) drive this directly.
    ///
    /// Preserves the write-ordering guarantee of the non-atomic base-then-index
    /// write path: the index cursor snapshots first and each base partition is
    /// opened later, so every entry the walk yields already had its base row
    /// written. `pk_capacity` pre-sizes the per-chunk PK scratch (the measured
    /// range size capped at the chunk size, or 0 to grow).
    ///
    /// Only user base tables own index circuits, so a resolved index implies a
    /// partitioned base; the `Err` is a registry corruption, not a shape
    /// production can build.
    pub(crate) fn open_index_range_cursor(
        &mut self,
        table_id: i64,
        col_indices: &[u32],
        range: &gnitz_wire::RangeDescriptor,
        pk_capacity: usize,
    ) -> Result<Option<BoundedIndexCursor>, String> {
        let (entry, ic) = self.table_and_index(table_id, col_indices)?;
        let src_schema = entry.schema;
        let store = entry
            .handle
            .as_partitioned()
            .ok_or_else(|| format!("index range on non-partitioned table {table_id}"))?;
        let Some((start, end)) = index_range_keys(ic, range)? else {
            return Ok(None);
        };
        Ok(Some(BoundedIndexCursor::new(
            ic.table_mut().open_cursor(),
            PartitionProbe::new(store),
            start,
            end,
            ic.key_spec,
            src_schema,
            pk_capacity,
        )))
    }

    /// Flush a table's WAL.
    pub fn flush_family(&mut self, table_id: i64) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            if let Some(table) = self.sys_table_mut(table_id) {
                table.flush().map_err(|e| format!("flush error: {e}"))?;
                // Compact so L0 shards don't accumulate without bound across
                // DDL-heavy sessions (system catalog tables are scanned on every
                // boot and DDL op).
                table
                    .compact_if_needed()
                    .map_err(|e| format!("compaction error: {e:?}"))?;
            }
            Ok(())
        } else {
            self.dag
                .flush(table_id)
                .map_err(|e| format!("flush failed for table_id={table_id}: {e}"))
        }
    }

    /// Worker DDL sync: apply a master-broadcast system-table delta. Workers
    /// update their registry from these; durability is master-side (fsynced
    /// SAL + the master's own system-table flush). The worker's inherited copy
    /// lives in RAM (memtable + `in_memory_l0`) and is never flushed by the
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
        self.dag.tables.get(&table_id).map(|e| e.handle.open_cursor())
    }

    /// The whole-relation source cursor for `source` — what every non-`Bounded`
    /// verdict below falls back to. `None` iff `source` is unregistered.
    fn full_source(&self, source: i64) -> Option<SourceCursor> {
        Some(SourceCursor::Full(Box::new(self.open_store_cursor(source)?)))
    }

    /// The source cursor for driving `source` through `view_id`'s circuit: an
    /// index-bounded cursor when the compiled plan pushed a bound down, the index
    /// circuit resolves, and the range measures selective; else the full-scan
    /// cursor. Every fallback is a **performance** choice, never a correctness one
    /// — the circuit's `Filter` is authoritative and unchanged, so `Full` and
    /// `Bounded` yield the same view. Note the open may COMPILE the view (see the
    /// `ensure_compiled` below) — plan-cache side effects included.
    ///
    /// `None` iff the source table is **unregistered** — byte-identical to
    /// `open_store_cursor`'s contract, which callers treat as "skip this source".
    /// A registered-but-empty table yields `Some`, and a *provably empty* range
    /// yields `Some(SourceCursor::Empty)`: collapsing that into `None` would make a
    /// driver skip the source entirely rather than feed it one empty epoch.
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
        // A bounded cursor is only sound in a process that owns base partitions: an
        // index circuit is a local shadow of the local base slice, and where no
        // slice is owned (the master) a bounded cursor returns zero rows rather
        // than an error — the fallbacks below would not catch it and the view would
        // silently fill empty. Hard, not `debug_assert!`: release is a supported
        // deployment, and this costs one compare per bounded backfill, not per row.
        assert!(
            self.owns_partitions(),
            "bounded source cursor in a process owning no base partitions (view {view_id}, source {source})",
        );

        self.open_bounded_source(source, bound.idx_cols.as_slice(), &bound.desc)
    }

    /// The index-bounded source cursor for the range `desc` on `idx_cols` of
    /// `source`, gated by selectivity: a `BoundedIndexCursor` when the range
    /// covers at most 1/`INDEX_SCAN_RATIO` of the local base slice, else the
    /// full-scan cursor — built only on that verdict, never speculatively — plus
    /// `SourceCursor::Empty` for a provably-empty range. Every non-`Bounded`
    /// outcome is a PERFORMANCE choice,
    /// never a correctness one — a caller's authoritative filter (a circuit's
    /// `Filter`, a ScanSpec's residual predicate) re-imposes the range — so the
    /// non-`exact` ScanSpec index bound and the circuit backfill share this one gate.
    ///
    /// `None` iff `source` is unregistered (byte-identical to
    /// `open_store_cursor`'s contract, which callers treat as "skip this source").
    pub(crate) fn open_bounded_source(
        &mut self,
        source: i64,
        idx_cols: &[u32],
        desc: &gnitz_wire::RangeDescriptor,
    ) -> Option<SourceCursor> {
        let Ok((entry, ic)) = self.table_and_index(source, idx_cols) else {
            // The index was dropped since the plan compiled.
            return self.full_source(source);
        };
        let (start, end) = match index_range_keys(ic, desc) {
            // Provably empty — decided before any cursor is built.
            Ok(Some(keys)) => keys,
            Ok(None) => return Some(SourceCursor::Empty),
            // Malformed: `n_eq` pins every column with no range column left.
            Err(_) => return self.full_source(source),
        };

        // Only user base tables own index circuits, so a resolved index implies a
        // partitioned base; a borrowed system table degrades to the full scan
        // like every other non-`Bounded` outcome here.
        let Some(store) = entry.handle.as_partitioned() else {
            return self.full_source(source);
        };
        // `ingest_store_and_indices` writes base-then-index non-atomically, so the
        // index must be snapshotted no later than the base: this opens the index
        // cursor now and the probe opens each base partition later, which is the
        // safe order (every entry the walk yields had its base row written first).
        let idx = ic.table_mut().open_cursor();
        // The only cost model. A bounded scan is not unconditionally cheaper: for a
        // range matching M of N rows it costs an index walk of M, an M log M sort,
        // and M galloping base probes, where a full scan is one sequential columnar
        // drain of N — so it loses badly as M → N (`WHERE indexed > 0` matches
        // everything). M is not estimated: it is measured exactly in O(log N)
        // before the first row is read (`count_range_raw` is `&self` and
        // repositions nothing). N comes from `estimated_rows` — arithmetic over
        // the children's run and shard counts — rather than a cursor's
        // `estimated_length`, so the whole-store cursor is built inside the
        // `Full` arm alone instead of speculatively on every open.
        let m = idx.count_range_raw(start.pk_bytes(), end.as_ref().map(|e| e.pk_bytes()));
        if m > store.estimated_rows() / INDEX_SCAN_RATIO {
            return self.full_source(source);
        }
        Some(SourceCursor::Bounded(Box::new(BoundedIndexCursor::new(
            idx,
            PartitionProbe::new(store),
            start,
            end,
            ic.key_spec,
            entry.schema,
            // The PK scratch's exact per-chunk bound: the measured range size,
            // capped at the drivers' chunk size.
            m.min(self.ddl_scan_chunk_rows),
        ))))
    }
}

/// Use the index only when its range covers at most 1/16 of the local base slice.
const INDEX_SCAN_RATIO: usize = 16;

/// The source cursor a circuit backfill drives, in the two shapes the drive can
/// take plus the provably-empty one. Interchangeable by construction: the
/// circuit's `Filter` decides what the view contains, so which variant is chosen
/// only decides how many rows the scan reads.
///
/// Both cursor variants are boxed (a `ReadCursor` is ~560 bytes; clippy's
/// `large_enum_variant`) — one allocation per backfill, never per chunk.
pub(crate) enum SourceCursor {
    Full(Box<ReadCursor>),
    Bounded(Box<BoundedIndexCursor>),
    /// A provably-empty index range. Distinct from `Full` so nothing is scanned,
    /// and distinct from `open_source_cursor -> None` so the source still feeds
    /// one empty epoch (which is what mints a global aggregate's ground row).
    Empty,
}

impl SourceCursor {
    /// The next up-to-`max_rows` source rows. `store` is the scanned relation's
    /// partitioned store — read per chunk by the driver, since a cursor cannot
    /// hold that borrow across the `&mut CatalogEngine` uses between chunks.
    /// `None` for a borrowed system table, which only ever reaches the `Full`
    /// arm (a `Bounded` cursor requires an index, and only user base tables own
    /// index circuits).
    pub(crate) fn drain_chunk(&mut self, store: Option<&PartitionedTable>, max_rows: usize) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_chunk(max_rows),
            SourceCursor::Bounded(c) => c.drain_chunk(
                store.expect("a bounded source cursor is only built over a partitioned base"),
                max_rows,
            ),
            SourceCursor::Empty => None,
        }
    }
}

/// The half-open OPK key range `[start, end)` for `range` over `ic`'s index, each
/// key exactly `ic.index_schema.pk_stride()` bytes. The cut → key mapping and the
/// provably-empty verdicts are the shared `range_keys_from_cuts` (§ its doc);
/// this function contributes only the index-specific group-prefix encoder and
/// the arity guard.
///
/// `Ok(None)` = provably empty (a `+∞` saturated start, or an inverted range like
/// `x > 5 AND x < 3` the planner does not pre-reject). `Err` = the descriptor pins
/// `n_eq` columns with no range column left within the index's arity — a trust
/// boundary the `pub` seek path must reject and a backfill bound merely degrades on.
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
    let cols = ic.col_indices.as_slice();
    // Precondition: the range column sits right after the equality prefix, so
    // `n_eq + 1` leading columns must exist. Guard *before* the `natives[..=n_eq]` /
    // `leading_key_size(n_eq + 1)` indexing below would panic. (Written
    // `n_eq >= len`, never a `+ 1` that could overflow on an adversarial length.)
    // It also keeps `prefix_len < idx_pk_stride` strict, so `pad` always extends
    // the group key.
    let eq_natives = range.eq_vals();
    let n_eq = eq_natives.len();
    if n_eq >= cols.len() {
        return Err(format!(
            "index range: n_eq {n_eq} has no range column within index arity {} on cols {cols:?}",
            cols.len()
        ));
    }

    let idx_pk_stride = ic.index_schema.pk_stride() as usize; // leading + source PK

    // The group prefix of a cut value is the full (n_eq + 1)-column leading key,
    // encoded through the circuit's baked spec — the same path the write side
    // uses (`write_span`/`batch_project_index` — byte-identical by construction).
    // `seek_prefix` returns it as a `PkBuf` whose zero tail makes `group(v)` IS
    // `pad(group(v))`.
    let mut natives = [0u128; gnitz_wire::PK_LIST_MAX_COLS];
    natives[..n_eq].copy_from_slice(eq_natives);
    Ok(crate::storage::range_keys_from_cuts(range, idx_pk_stride, |v| {
        natives[n_eq] = v;
        ic.key_spec.seek_prefix(&natives[..=n_eq])
    }))
}

/// Projecting sibling of `copy_cursor_row_with_weight`: append the cursor's
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
        let ptr = cursor.col_ptr(ci, col_size);
        if !ptr.is_null() {
            let data = unsafe { std::slice::from_raw_parts(ptr, col_size) };
            out.extend_col(k, data);
        } else {
            out.fill_col_zero(k, col_size);
        }
    }
    out.extend_null_bmp(&proj_null.to_le_bytes());
    out.count += 1;
}
