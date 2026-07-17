//! Server-facing I/O on table families — ingest, scan, point/range seek
//! (including secondary-index lookup), and the multi-phase flush/replay
//! paths. System tables route through the catalog write path; user tables
//! delegate to `DagEngine`.

use super::*;
use crate::schema::project_schema;
use crate::storage::{gather_source_rows, BoundedIndexCursor};

impl CatalogEngine {
    /// The registry entry for `table_id`, or the shared "Unknown table_id"
    /// error every hard-resolving store path reports.
    pub(crate) fn table_entry(&self, table_id: i64) -> Result<&crate::query::TableEntry, String> {
        self.dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))
    }

    /// Ingest a user-table batch and return the effective delta (after unique_pk
    /// dedup).  Used by multi-worker push where the worker needs the effective
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
        let (opk, stride) = crate::schema::seek_opk_bytes(&entry.schema, seek_pk, seek_pk_extra)?;
        Ok((Self::seek_entry_bytes(entry, &opk[..stride]), entry.schema))
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

    /// The seek+materialise primitive: open the merged cursor, `seek_exact_live`
    /// the OPK `pk` bytes, copy the row. Correct at any PK width.
    fn seek_entry_bytes(entry: &crate::query::TableEntry, pk: &[u8]) -> Option<Batch> {
        let mut cursor = entry.handle.open_cursor();
        if !cursor.seek_exact_live(pk) {
            return None;
        }
        let mut batch = Batch::with_schema(entry.schema, 1);
        cursor.copy_current_row_into(&mut batch, cursor.current_weight);
        Some(batch)
    }

    /// Batched point lookup. Open one cursor on `table_id` and seek each PK in
    /// `pks` (verbatim OPK bytes), appending the stored row (weight 1) for every
    /// present, live key into a result batch projected to `project`. Each `seek`
    /// re-probes every source independently, so order is not required for
    /// correctness; passing `pks` ascending keeps the per-source binary-search
    /// probes monotonic for better cache locality.
    /// Absent / retracted keys are skipped — identical to `seek_family`'s
    /// single-key `None` — so a removed PK with no committed row contributes
    /// nothing. `project` lists the parent column indices to return (all
    /// non-PK scalar columns); an empty `project` returns PK-only rows.
    ///
    /// Reuses one cursor across all keys (cheaper than N `seek_family` calls,
    /// each of which re-opens a cursor). Projection keeps the result scalar-
    /// only — FK-referenced columns are never STRING/BLOB — so the blob arena
    /// is never touched. Works for both narrow and wide PKs: the OPK bytes are
    /// seeked verbatim, with no native→OPK re-encode.
    pub fn gather_family_bytes(
        &mut self,
        table_id: i64,
        pks: &[crate::storage::PkBuf],
        project: &[u8],
    ) -> Result<Batch, String> {
        let entry = self.table_entry(table_id)?;
        let schema = entry.schema;
        let result_schema = project_schema(&schema, project);
        // Resolve the fixed projection once — `(col_idx, payload_slot, size)`
        // per projected column — instead of re-deriving payload index and
        // column size per row per column inside the seek loop. The projection
        // is master-built and excludes PK columns (`collect_fk_projection`
        // skips `is_pk_col`; `project_schema` asserts it one frame up), so
        // every projected column has a payload slot.
        let proj: Vec<(usize, usize, usize)> = project
            .iter()
            .map(|&p| {
                let ci = p as usize;
                let pi = schema.try_payload_idx(ci).expect("FK projection excludes PK columns");
                (ci, pi, schema.columns[ci].size() as usize)
            })
            .collect();
        let mut out = Batch::with_schema(result_schema, pks.len());
        let mut cursor = entry.handle.open_cursor();
        for pk in pks {
            // Order is not required for correctness; `advance_to` is
            // backward-capable, so it matches `seek_exact_live` on any input and
            // additionally fast-paths the (common) ascending-`pks` caller.
            if cursor.advance_to_exact_live(pk.pk_bytes()) {
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
            .index_circuits
            .iter()
            .find(|ic| ic.col_indices.as_slice() == cols)
            .ok_or_else(|| format!("No index on cols {cols:?} for table {table_id}"))?;
        Ok((entry, ic))
    }

    /// Index-assisted lookup: prefix-scan the index by `natives` — the native
    /// key values of the leading `natives.len()` indexed columns
    /// (`natives.len()` may be `< col_indices.len()` for a leading-prefix
    /// scan) — reconstruct the source PK from the index PK suffix, and resolve
    /// to the source rows.
    ///
    /// Rows with a NULL in ANY indexed column are absent from the index
    /// (`batch_project_index` skips them), so a prefix scan returns only rows
    /// whose trailing indexed columns are all non-NULL — the SQL planner must
    /// not serve a prefix predicate from an index whose uncovered trailing
    /// columns are nullable.
    pub fn seek_by_index(
        &mut self,
        table_id: i64,
        col_indices: &[u32],
        natives: &[u128],
    ) -> Result<(Option<Batch>, SchemaDescriptor), String> {
        let (entry, ic) = self.table_and_index(table_id, col_indices)?;

        let src_schema = entry.schema;
        let src_pk_stride = src_schema.pk_stride() as usize;
        let idx_key_size = ic.index_schema.leading_key_size(col_indices.len());

        // OPK-encode the supplied natives into a leading-key prefix through the
        // circuit's baked spec, cut to the supplied arity. The spec pairs each
        // source column (the sign-extension width) with its promoted index
        // column; the index PK region is OPK-at-rest, so the prefix is
        // order-preserving and matches stored entries (`batch_project_index`
        // encodes through the same spec).
        let spec = ic.key_spec.prefix(natives.len());
        let (opk, prefix_len) = spec.seek_prefix(natives);
        let opk_prefix = &opk[..prefix_len];

        // One index cursor for the walk; collect each positive match's source-PK
        // suffix. The walk yields only positive-weight entries
        // (`walk_to_positive_with_prefix`), so every yielded entry is live — push
        // its source PK unconditionally.
        //
        // Seek to the first positive-weight match, then walk forward with
        // `walk_to_positive_with_prefix` after each consumed entry. Re-calling
        // `seek_first_positive_with_prefix` inside the loop would re-seek and
        // re-find the same entry forever — an orphaned index entry (positive
        // weight, no source row) would spin.
        // A non-unique indexed value matches multiple rows; accumulate ALL of
        // them on this worker, not just the first. (A unique index yields one
        // match, so this is equivalent there.) Index entries co-locate with
        // their source rows (partitioned by source PK), so a value's matches can
        // be spread across workers: this returns one worker's partial set and
        // the master (`fan_out_seek_by_index_collect_async`) merges across all.
        let mut idx_cursor = ic.table_mut().open_cursor();
        let mut pks: Vec<crate::storage::PkBuf> = Vec::new();

        let mut hit = idx_cursor.seek_first_positive_with_prefix(opk_prefix);
        while hit {
            let cur_pk = idx_cursor.current_pk_bytes();
            pks.push(crate::storage::PkBuf::from_bytes(
                &cur_pk[idx_key_size..idx_key_size + src_pk_stride],
            ));
            idx_cursor.advance();
            hit = idx_cursor.walk_to_positive_with_prefix(opk_prefix);
        }
        // Free the index merge tree and its shard snapshots before the base cursor
        // opens. An early free, not an invariant: a cursor pins the mmap rather
        // than the file, so holding both would be correct too.
        drop(idx_cursor);

        // Full-arity equality (`natives.len() == col_indices.len()`) pins every
        // indexed column, so the entries vary only in their trailing source-PK
        // suffix and the walk already yields them ascending — skip the sort. A
        // leading-prefix seek leaves trailing indexed columns free, interleaving
        // source PKs across groups, so it must sort to recover storage order.
        if natives.len() < col_indices.len() {
            pks.sort_unstable();
        }
        // An empty `pks` short-circuits before the base cursor's snapshot clone.
        let batch = (!pks.is_empty())
            .then(|| gather_source_rows(&mut entry.handle.open_cursor(), src_schema, &pks))
            .flatten();
        Ok((batch, src_schema))
    }

    /// Ordered range scan over a secondary index: the leading
    /// `range.eq_vals().len()` columns are equality-pinned, and the next index
    /// column is bounded by the descriptor's half-open cut interval
    /// `[start, end)`. Each `Cut` maps to one byte key in the index PK space —
    ///
    /// | cut         | byte key                                                |
    /// |-------------|---------------------------------------------------------|
    /// | `Before(v)` | `pad(group(v))` — below every duplicate of `v`          |
    /// | `After(v)`  | `pad(succ(group(v)))` — above every duplicate of `v`;   |
    /// |             | `succ` overflow ⇒ no key space above the group (`+∞`)   |
    ///
    /// where `group(v)` is the `prefix_len`-byte `[eq OPK ‖ promoted slot OPK]`
    /// group key, `pad` zero-extends to the index PK stride, and `succ` is the
    /// fixed-width byte successor. The walk is then uniform — seek to `start`,
    /// advance while `key < end` — with `start ≥ end` (or a `+∞` start)
    /// provably empty. SQL bound semantics (inclusivity, unboundedness,
    /// out-of-range saturation) are resolved to cuts in the planner; none of
    /// them reach this layer.
    ///
    /// Correctness rests on the post-`cff7c58` OPK ordering invariant: the index
    /// PK region is `[promoted leading-key OPK ‖ source-PK OPK]` and memcmp order
    /// on those bytes equals typed order (signed and composite included). For any
    /// `prefix_len`-byte group key `p`, every full key `k` with
    /// `k[..prefix_len] == p` satisfies `pad(p) ≤ k < pad(succ(p))`, so a cut key
    /// includes or excludes whole duplicate groups with no per-row inclusivity
    /// test.
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
        let (entry, ic) = self.table_and_index(table_id, col_indices)?;
        let src_schema = entry.schema;

        // A `+∞` start, or an inverted/zero-width interval, is provably empty:
        // short-circuit before constructing any cursor or running the O(log N) seek.
        let Some((start, end)) = index_range_keys(ic, range)? else {
            return Ok((None, src_schema));
        };

        // The wire seek IS one unchunked drain of the bounded cursor — the same
        // walk/gather the backfill scan drives chunk-wise, so the two paths
        // cannot diverge on the weight-consolidation subtleties. The `.filter`
        // maps the cursor's `Some(empty)` ("in-range entries, none resolved")
        // back to this API's `None`.
        let mut cur = BoundedIndexCursor::new(
            ic.table_mut().open_cursor(),
            entry.handle.open_cursor(),
            &start,
            end,
            ic.index_schema.leading_key_size(col_indices.len()),
            src_schema,
            0,
        );
        Ok((cur.drain_chunk(usize::MAX).filter(|b| b.count > 0), src_schema))
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
        // Cache-first and idempotent, so it is also correct for a transient.
        let bound = self
            .dag
            .ensure_compiled(view_id)
            .then(|| self.dag.source_scan_bound(view_id, source))
            .flatten();
        let Some(bound) = bound else {
            return Some(SourceCursor::Full(Box::new(self.open_store_cursor(source)?)));
        };
        // A bounded cursor is only sound in a process that owns base partitions: an
        // index circuit is a local shadow of the local base slice, and where no
        // slice is owned (the master) a bounded cursor returns zero rows rather
        // than an error — the fallbacks below would not catch it and the view would
        // silently fill empty. Hard, not `debug_assert!`: release is a supported
        // deployment, and this costs one compare per bounded backfill, not per row.
        assert!(
            self.active_part_start != self.active_part_end,
            "bounded source cursor in a process owning no base partitions (view {view_id}, source {source})",
        );

        let Ok((entry, ic)) = self.table_and_index(source, bound.idx_cols.as_slice()) else {
            // The index was dropped since the plan compiled, or the id is a
            // remapped transient with no index circuits.
            return Some(SourceCursor::Full(Box::new(self.open_store_cursor(source)?)));
        };
        // The gather resolves by source PK alone and emits one (PK, payload) group
        // per PK — wrong on a non-`unique_pk` base table, where one PK can carry
        // several live payloads differing in the indexed column. (That is a
        // pre-existing bug in the thin index path; do not promote it into durably
        // persisted, incrementally maintained views.) Every SQL-created table is
        // `unique_pk`, so this costs nothing on the SQL path.
        if !entry.unique_pk() {
            return Some(SourceCursor::Full(Box::new(self.open_store_cursor(source)?)));
        }
        let (start, end) = match index_range_keys(ic, &bound.desc) {
            // Provably empty — decided before any cursor is built.
            Ok(Some(keys)) => keys,
            Ok(None) => return Some(SourceCursor::Empty),
            // Malformed: `n_eq` pins every column with no range column left.
            Err(_) => return Some(SourceCursor::Full(Box::new(self.open_store_cursor(source)?))),
        };

        // Both cursors open in one synchronous stretch with no reactor yield: the
        // worker is single-threaded and `ingest_store_and_indices` writes
        // base-then-index non-atomically, so a yield between the two opens could
        // snapshot a base row whose index entry is not yet visible — losing a row a
        // full scan would have returned. (`entry` is registered — `table_and_index`
        // resolved it — so the `?` cannot fire here.)
        let idx = ic.table_mut().open_cursor();
        let src = Box::new(self.open_store_cursor(source)?);
        // The only cost model. A bounded scan is not unconditionally cheaper: for a
        // range matching M of N rows it costs an index walk of M, an M log M sort,
        // and M galloping base probes, where a full scan is one sequential columnar
        // drain of N — so it loses badly as M → N (`WHERE indexed > 0` matches
        // everything). M is not estimated: it is measured exactly in O(log N)
        // before the first row is read (`count_range_raw` is `&self` and
        // repositions nothing), so `Full` hands `src` over verbatim.
        let m = idx.count_range_raw(start.pk_bytes(), end.as_ref().map(|e| e.pk_bytes()));
        if m > src.estimated_length() / INDEX_SCAN_RATIO {
            return Some(SourceCursor::Full(src));
        }
        Some(SourceCursor::Bounded(Box::new(BoundedIndexCursor::new(
            idx,
            *src,
            &start,
            end,
            ic.index_schema.leading_key_size(bound.idx_cols.as_slice().len()),
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
/// Both cursor variants are boxed (a `ReadCursor` is ~560 bytes and a
/// `BoundedIndexCursor` holds two of them; clippy's `large_enum_variant`) —
/// one allocation per backfill, never per chunk.
pub(crate) enum SourceCursor {
    Full(Box<ReadCursor>),
    Bounded(Box<BoundedIndexCursor>),
    /// A provably-empty index range. Distinct from `Full` so nothing is scanned,
    /// and distinct from `open_source_cursor -> None` so the source still feeds
    /// one empty epoch (which is what mints a global aggregate's ground row).
    Empty,
}

impl SourceCursor {
    pub(crate) fn drain_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        match self {
            SourceCursor::Full(c) => c.drain_chunk(max_rows),
            SourceCursor::Bounded(c) => c.drain_chunk(max_rows),
            SourceCursor::Empty => None,
        }
    }
}

/// The half-open OPK key range `[start, end)` for `range` over `ic`'s index, each
/// key exactly `ic.index_schema.pk_stride()` bytes.
///
/// `Ok(None)` = provably empty (a `+∞` saturated start, or an inverted range like
/// `x > 5 AND x < 3` the planner does not pre-reject). `Err` = the descriptor pins
/// `n_eq` columns with no range column left within the index's arity — a trust
/// boundary the `pub` seek path must reject and a backfill bound merely degrades on.
///
/// Each `Cut` maps to one byte key in the index PK space —
///
/// | cut         | byte key                                                |
/// |-------------|---------------------------------------------------------|
/// | `Before(v)` | `pad(group(v))` — below every duplicate of `v`          |
/// | `After(v)`  | `pad(succ(group(v)))` — above every duplicate of `v`;   |
/// |             | `succ` overflow ⇒ no key space above the group (`+∞`)   |
///
/// where `group(v)` is the `prefix_len`-byte `[eq OPK ‖ promoted slot OPK]` group
/// key, `pad` zero-extends to the index PK stride, and `succ` is the fixed-width
/// byte successor. The walk is then uniform — seek to `start`, advance while
/// `key < end`. SQL bound semantics (inclusivity, unboundedness, out-of-range
/// saturation) are resolved to cuts in the planner; none reach this layer.
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
) -> Result<Option<(crate::storage::PkBuf, Option<crate::storage::PkBuf>)>, String> {
    use gnitz_wire::Cut;

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
    let prefix_len = ic.index_schema.leading_key_size(n_eq + 1); // eq prefix + range slot

    // Every cut key is `pad(group(v))` or its successor for a full (n_eq + 1)-column
    // group key, so the circuit's baked spec cut to the equality prefix PLUS the
    // range column encodes both cuts through the same path the write side uses
    // (`write_span`/`batch_project_index` — byte-identical by construction). Stack
    // scratch throughout: MAX_PK_BYTES bounds every index schema's pk_stride
    // (asserted in `SchemaDescriptor::new`), and `seek_prefix` leaves the bytes past
    // `prefix_len` zero, so `group(v)` IS `pad(group(v))`.
    let spec = ic.key_spec.prefix(n_eq + 1);
    let mut natives = [0u128; gnitz_wire::PK_LIST_MAX_COLS];
    natives[..n_eq].copy_from_slice(eq_natives);
    // Cut → byte key, `None` = `+∞`. `After` steps to the byte successor of the
    // whole group, so e.g. an exclusive lower bound seeks strictly past every
    // duplicate of `v` in O(log N) (no per-row skip); the carry may ripple into the
    // equality prefix, which is exactly the first key of the next equality group.
    let mut cut_key = |c: Cut| -> Option<crate::storage::PkBuf> {
        natives[n_eq] = c.value();
        let mut k = spec.seek_prefix(&natives[..=n_eq]).0;
        match c {
            Cut::Before(_) => Some(crate::storage::PkBuf::from_bytes(&k[..idx_pk_stride])),
            Cut::After(_) => crate::storage::increment_key_in_place(&mut k[..prefix_len])
                .then(|| crate::storage::PkBuf::from_bytes(&k[..idx_pk_stride])),
        }
    };
    let (start, end) = (cut_key(range.start), cut_key(range.end));

    // A `+∞` start, or `start ≥ end`, is provably empty (a zero-width saturated
    // interval, or an inverted range).
    let Some(start) = start else {
        return Ok(None);
    };
    if end.as_ref().is_some_and(|e| start.pk_bytes() >= e.pk_bytes()) {
        return Ok(None);
    }
    Ok(Some((start, end)))
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
        if src_null & (1u64 << pi) != 0 {
            proj_null |= 1u64 << k;
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

// `increment_key_in_place` and its unit coverage moved to
// `storage/range_key.rs` (the shared home for the byte successor and the
// range-join cut-point derivation).
