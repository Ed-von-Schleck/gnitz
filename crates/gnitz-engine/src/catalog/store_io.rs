//! Server-facing I/O on table families — ingest, scan, point/range seek
//! (including secondary-index lookup), and the multi-phase flush/replay
//! paths. System tables route through the catalog write path; user tables
//! delegate to `DagEngine`.

use super::*;
use crate::schema::project_schema;

impl CatalogEngine {
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
        let entry = self
            .dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))?;
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
        let entry = self
            .dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))?;
        let (opk, stride) = crate::schema::seek_opk_bytes(&entry.schema, seek_pk, seek_pk_extra)?;
        Ok((Self::seek_entry_bytes(entry, &opk[..stride]), entry.schema))
    }

    /// Byte-keyed sibling of [`seek_family`] for callers that already hold the
    /// OPK bytes — only the wide-PK tests seek through it directly.
    /// Registry-uniform: system tables are pre-registered `Borrowed` handles,
    /// so one lookup serves every id.
    #[cfg(test)]
    pub(crate) fn seek_family_bytes(&mut self, table_id: i64, pk: &[u8]) -> Result<Option<Batch>, String> {
        let entry = self
            .dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))?;
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
        let entry = self
            .dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))?;
        let schema = entry.schema;
        let result_schema = project_schema(&schema, project);
        let mut out = Batch::with_schema(result_schema, pks.len());
        let mut cursor = entry.handle.open_cursor();
        for pk in pks {
            // Order is not required for correctness; `advance_to` is
            // backward-capable, so it matches `seek_exact_live` on any input and
            // additionally fast-paths the (common) ascending-`pks` caller.
            if cursor.advance_to_exact_live(pk.pk_bytes()) {
                copy_cursor_cols_to_batch(&cursor, &mut out, &schema, project);
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
        let entry = self
            .dag
            .tables
            .get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {table_id}"))?;
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

        // OPK-encode the supplied natives into a leading-key prefix. The spec
        // pairs each source column (the sign-extension width) with its promoted
        // index column; the index PK region is OPK-at-rest, so the prefix is
        // order-preserving and matches stored entries (`batch_project_index`
        // encodes through the same spec).
        let spec = crate::schema::IndexKeySpec::new(&col_indices[..natives.len()], &src_schema, &ic.index_schema);
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
        // Free the index merge tree and its shard snapshots before the base
        // cursor opens, so the two never coexist.
        drop(idx_cursor);

        // Full-arity equality (`natives.len() == col_indices.len()`) pins every
        // indexed column, so the entries vary only in their trailing source-PK
        // suffix and the walk already yields them ascending — skip the sort. A
        // leading-prefix seek leaves trailing indexed columns free, interleaving
        // source PKs across groups, so it must sort to recover storage order.
        if natives.len() < col_indices.len() {
            pks.sort_unstable();
        }
        Ok((Self::resolve_source_pks(&entry.handle, src_schema, &pks), src_schema))
    }

    /// Resolve already-collected source PKs against the base table into a result
    /// batch — every present, live, exact-PK row at its net `current_weight`
    /// (never a hardcoded 1, so Z-Set multiplicity is preserved) — or `None` when
    /// nothing resolves.
    ///
    /// `pks` must be **ascending** in `compare_pk_bytes` (storage) order: each
    /// `seek_exact_live` then lower-bounds at or past the previous key, turning K
    /// scattered point-seeks into one monotone forward sweep that keeps shard
    /// pages and merge state hot. The PKs are index entries' source-PK OPK
    /// suffixes, whose memcmp order equals base storage order, so a byte sort *is*
    /// the seek order; `PkBuf` carries the exact `src_pk_stride` bytes inline (up
    /// to `MAX_PK_BYTES`), so wide sources resolve with no widen. `acc` is sized
    /// to `pks.len()` — a tight upper bound, since base PKs are unique and each
    /// carries one indexed value — so it never grows row by row; an empty `pks`
    /// short-circuits before the base cursor's snapshot clone + tree build.
    fn resolve_source_pks(
        handle: &StoreHandle,
        src_schema: SchemaDescriptor,
        pks: &[crate::storage::PkBuf],
    ) -> Option<Batch> {
        debug_assert!(
            pks.windows(2).all(|w| w[0] <= w[1]),
            "resolve_source_pks requires ascending PKs for the monotone sweep"
        );
        if pks.is_empty() {
            return None;
        }
        let mut src_cursor = handle.open_cursor();
        let mut acc = Batch::with_schema(src_schema, pks.len());
        for pk in pks {
            // PKs are asserted ascending above, so the galloping `advance_to`
            // seeds each probe at the prior position — one monotone forward sweep.
            if src_cursor.advance_to_exact_live(pk.pk_bytes()) {
                let w = src_cursor.current_weight;
                src_cursor.copy_current_row_into(&mut acc, w);
            }
        }
        (acc.count > 0).then_some(acc)
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
        use gnitz_wire::Cut;

        let (entry, ic) = self.table_and_index(table_id, col_indices)?;

        // Precondition: the range column sits right after the equality prefix, so
        // `n_eq + 1` leading columns must exist. The worker validates this at the
        // trust boundary, but this `pub` method is also reachable from unit tests —
        // guard here too, *before* the `col_indices[..=n_eq]` /
        // `leading_key_size(n_eq + 1)` indexing below would panic. (Written
        // `n_eq >= len`, never a `+ 1` that could overflow on an adversarial
        // length.) It also keeps `prefix_len < idx_pk_stride` strict, so `pad`
        // always extends the group key.
        let eq_natives = range.eq_vals();
        let n_eq = eq_natives.len();
        if n_eq >= col_indices.len() {
            return Err(format!(
                "seek_by_index_range: n_eq {n_eq} has no range column within index \
                 arity {} on cols {col_indices:?}",
                col_indices.len()
            ));
        }

        let src_schema = entry.schema;
        let src_pk_stride = src_schema.pk_stride() as usize;
        let idx_pk_stride = ic.index_schema.pk_stride() as usize; // leading + source PK
        let idx_key_size = ic.index_schema.leading_key_size(col_indices.len());
        let prefix_len = ic.index_schema.leading_key_size(n_eq + 1); // eq prefix + range slot

        // Every cut key is `pad(group(v))` or its successor for a full
        // (n_eq + 1)-column group key, so one IndexKeySpec over the equality
        // prefix PLUS the range column encodes both cuts through the same path
        // the write side uses (`write_span`/`batch_project_index` —
        // byte-identical by construction). Stack scratch throughout: MAX_PK_BYTES
        // bounds every index schema's pk_stride (asserted in
        // SchemaDescriptor::new), and `seek_prefix` leaves the bytes past
        // `prefix_len` zero, so `group(v)` IS `pad(group(v))`.
        let spec = crate::schema::IndexKeySpec::new(&col_indices[..=n_eq], &src_schema, &ic.index_schema);
        let mut natives = [0u128; gnitz_wire::PK_LIST_MAX_COLS];
        natives[..n_eq].copy_from_slice(eq_natives);
        // Cut → byte key, `None` = `+∞`. `After` steps to the byte successor of
        // the whole group, so e.g. an exclusive lower bound seeks strictly past
        // every duplicate of `v` in O(log N) (no per-row skip); the carry may
        // ripple into the equality prefix, which is exactly the first key of the
        // next equality group.
        let mut cut_key = |c: Cut| {
            natives[n_eq] = c.value();
            let mut k = spec.seek_prefix(&natives[..=n_eq]).0;
            match c {
                Cut::Before(_) => Some(k),
                Cut::After(_) => crate::storage::increment_key_in_place(&mut k[..prefix_len]).then_some(k),
            }
        };
        let (start, end) = (cut_key(range.start), cut_key(range.end));

        // A `+∞` start, or `start ≥ end`, is provably empty (a zero-width
        // saturated interval, or an inverted range like `x > 5 AND x < 3` the
        // planner does not pre-reject): short-circuit before constructing any
        // cursor or running the O(log N) seek.
        let Some(start) = start else {
            return Ok((None, src_schema));
        };
        let end = end.as_ref().map(|e| &e[..idx_pk_stride]);
        if end.is_some_and(|e| &start[..idx_pk_stride] >= e) {
            return Ok((None, src_schema));
        }

        // One index cursor for the walk; collect each positive match's source PK.
        let mut idx_cursor = ic.table_mut().open_cursor();
        let mut pks: Vec<crate::storage::PkBuf> = Vec::new();

        idx_cursor.seek_bytes(&start[..idx_pk_stride]);
        while idx_cursor.valid {
            let cur_pk = idx_cursor.current_pk_bytes();
            if end.is_some_and(|e| cur_pk >= e) {
                break;
            }
            if idx_cursor.current_weight > 0 {
                pks.push(crate::storage::PkBuf::from_bytes(
                    &cur_pk[idx_key_size..idx_key_size + src_pk_stride],
                ));
            }
            idx_cursor.advance();
        }
        // Free the index merge tree and its shard snapshots before the base
        // cursor opens, so the two never coexist and obsolete shards can be
        // reclaimed.
        drop(idx_cursor);

        // A range spans many duplicate groups, so the collected source PKs
        // interleave across the base table — sort to recover the ascending sweep.
        pks.sort_unstable();
        Ok((Self::resolve_source_pks(&entry.handle, src_schema, &pks), src_schema))
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
    pub(crate) fn open_store_cursor(&mut self, table_id: i64) -> Option<ReadCursor> {
        self.dag.tables.get(&table_id).map(|e| e.handle.open_cursor())
    }
}

/// Projecting sibling of `copy_cursor_row_with_weight`: append the cursor's
/// current row to `out` (which has the `project_schema` layout) with weight 1,
/// copying only the columns named in `project`. The projected payload column
/// at position `k` corresponds to source column `project[k]`; the projected
/// null bit `k` mirrors the source row's null bit for that column. Projected
/// columns are scalar, so no blob relocation is required.
fn copy_cursor_cols_to_batch(cursor: &ReadCursor, out: &mut Batch, src_schema: &SchemaDescriptor, project: &[u8]) {
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
    for (k, &p) in project.iter().enumerate() {
        // The projection is master-built and excludes PK columns
        // (`collect_fk_projection` skips `is_pk_col`; `project_schema` asserts
        // it one frame up), so every projected column has a payload slot.
        let pi = src_schema
            .try_payload_idx(p as usize)
            .expect("FK projection excludes PK columns");
        if src_null & (1u64 << pi) != 0 {
            proj_null |= 1u64 << k;
        }
        let col_size = src_schema.columns[p as usize].size() as usize;
        let ptr = cursor.col_ptr(p as usize, col_size);
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
