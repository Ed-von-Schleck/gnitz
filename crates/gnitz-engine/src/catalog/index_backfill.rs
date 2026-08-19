//! Secondary-index construction: opening an index table, the chunked base-table
//! scan that projects into it, its two entry points (a fresh CREATE INDEX and
//! the worker's boot rebuild), the UNIQUE promote, and FK auto-index creation.

use super::*;
use crate::schema::key::PkBuf;
use crate::schema::make_index_schema;

impl CatalogEngine {
    /// Locally retract an index registration whose +1 was applied but never
    /// broadcast. A failed rollback leaves the +1 in sys_indices while the
    /// client is handed an error — a permanently diverged catalog, and the
    /// next boot's replay would open a missing index directory — so it
    /// fail-stops, matching `compensate_stage_a`'s rollback abort.
    pub(super) fn rollback_index_registration(&mut self, undo: Batch, index_id: i64) {
        if let Err(undo_err) = self.submit_local(SysFamily::Index, undo) {
            gnitz_fatal_abort!(
                "catalog: index registration rollback failed (index_id={}): {} \
                 — catalog state permanently diverged; aborting",
                index_id,
                undo_err,
            );
        }
    }

    /// The recovery policy for a secondary-index table: resume from a manifest
    /// at the resume generation, and only while the topology still matches —
    /// the same two-part verdict `compute_invalid_views` reaches for a view, in
    /// the form `Table::new` reads.
    pub(crate) fn index_recovery_source(&self) -> RecoverySource {
        RecoverySource::Rederive {
            resume_at: self.topology_matches().then_some(self.resume_generation),
        }
    }

    /// Open this process's copy of a secondary-index table under `idx_dir` — the
    /// one recipe for the live CREATE INDEX hook and the worker-boot rebuild, so
    /// the two cannot diverge on where it is homed or on its resume gate.
    ///
    /// A forked worker homes at its own `w{rank}of{n}` child, like every other
    /// relation store. The master and standalone home at `idx_dir` itself: the
    /// master's copy stays permanently empty, and homing it at `w0of{n}` would
    /// put it on the directory worker 0's inherited handle already holds.
    pub(crate) fn new_index_table(
        &self,
        idx_dir: &str,
        index_id: i64,
        idx_schema: SchemaDescriptor,
    ) -> Result<Table, String> {
        let table_dir = if crate::foundation::worker_ctx::is_worker() {
            ChildAddr::this_worker(self.num_workers).dir(idx_dir)
        } else {
            idx_dir.to_string()
        };
        Table::new(&table_dir, idx_schema, index_id as u32, self.index_recovery_source())
            .map_err(|e| format!("Failed to create index table {index_id}: error {e}"))
    }

    // -- Index backfill (scan source, project into index table) ------------

    pub(crate) fn backfill_index(
        &mut self,
        owner_id: i64,
        owner_schema: &SchemaDescriptor,
        col_indices: &[u32],
        idx_table: *mut Table,
        idx_schema: &SchemaDescriptor,
        check_dups: bool,
    ) -> Result<(), String> {
        // The relation filled here is the *index table* (`owner_id` is the
        // indexed relation, a durable base table). It must be empty: an index
        // that resumed from its checkpoint already holds these rows, and
        // projecting them again would double every weight.
        debug_assert!(
            unsafe { &*idx_table }.estimated_rows() == 0,
            "backfill_index into a non-empty index (owner {owner_id}): would double-count",
        );
        // Built here, not read off a circuit: the fresh-index path runs before
        // `add_index_circuit` registers one.
        let target = IndexProjectionTarget {
            cols: PkColList::from_slice(col_indices),
            spec: crate::schema::IndexKeySpec::new(col_indices, owner_schema, idx_schema),
            idx_schema: *idx_schema,
            table: Some(idx_table),
        };
        self.stream_index_projection(owner_id, &[target], check_dups)
    }

    /// Worker-boot index rebuild: re-open every registered index circuit's Table
    /// at this worker's own child (replacing the fork-inherited parent-dir one)
    /// and, for each that did NOT resume from its checkpoint, backfill it from
    /// the trimmed/rehomed base slice — one scan per owner table, each chunk
    /// projected into the index tables that need filling. Must run after
    /// trim/rehome and BEFORE SAL replay —
    /// replay projects the unflushed committed tail into the index exactly once
    /// through `ingest_store_and_indices`, so a rebuild *after* replay would
    /// double-count every replayed row. Boot data was validated at original
    /// write time and a slice-local check cannot see a duplicate that
    /// legitimately straddles two workers' slices, so the rebuild skips the dup
    /// check entirely. Fail-fast: an error aborts worker boot via the startup
    /// ACK.
    ///
    /// Returns how many indexes were rebuilt rather than resumed — 0 on a clean
    /// restart at the same topology.
    ///
    /// In a Standalone process this is a legal idempotent re-create-and-rebuild
    /// at the parent dir — unit-testable without touching the global role.
    pub fn backfill_all_indexes(&mut self) -> Result<usize, String> {
        // Snapshot the worklist first: each owner's rebuild mutably borrows
        // self.dag (`replace_index_table`), so no borrow of `dag.tables` may be
        // held across the loop. Only base tables carry index circuits, so
        // views/system tables contribute nothing.
        struct IndexWork {
            cols: PkColList,
            index_id: i64,
            idx_schema: SchemaDescriptor,
            key_spec: crate::schema::IndexKeySpec,
        }
        let worklist: Vec<(i64, String, Vec<IndexWork>)> = self
            .dag
            .tables
            .iter()
            .filter(|(_, entry)| !entry.index_circuits.is_empty())
            .map(|(&owner_id, entry)| {
                let works = entry
                    .index_circuits
                    .iter()
                    .map(|ic| IndexWork {
                        cols: ic.col_indices,
                        index_id: ic.index_id,
                        idx_schema: ic.index_schema,
                        key_spec: ic.key_spec,
                    })
                    .collect();
                (owner_id, entry.directory.clone(), works)
            })
            .collect();

        let mut rebuilt = 0usize;
        for (owner_id, owner_dir, works) in worklist {
            // Re-create and install every index table before opening the scan;
            // one base-slice scan then feeds the ones that must be re-derived.
            let mut targets: Vec<IndexProjectionTarget> = Vec::new();
            for w in &works {
                let table = self
                    .new_index_table(&index_dir(&owner_dir, w.index_id), w.index_id, w.idx_schema)
                    .map_err(|e| format!("index table re-create failed (owner {owner_id}): {e}"))?;
                let resumed = table.resumed_from_checkpoint();
                let ptr = self
                    .dag
                    .replace_index_table(owner_id, w.cols.as_slice(), Box::new(table))
                    .ok_or_else(|| format!("index circuit vanished during rebuild (owner {owner_id})"))?;
                if !resumed {
                    targets.push(IndexProjectionTarget {
                        cols: w.cols,
                        spec: w.key_spec,
                        idx_schema: w.idx_schema,
                        table: Some(ptr),
                    });
                }
            }
            rebuilt += targets.len();
            self.stream_index_projection(owner_id, &targets, false)?;
        }
        Ok(rebuilt)
    }

    /// Shared streaming pass for `backfill_index`, `promote_index_to_unique`,
    /// and the boot rebuild (`backfill_all_indexes`): scan `owner_id`
    /// chunk-wise, project each chunk into every target's index layout, reject
    /// duplicate keys when `check_dups`, and ingest each projected chunk into
    /// the target's table when one is supplied. Peak memory is
    /// O(chunk × row_width) plus, when checking, the per-target cross-chunk
    /// `seen` set (one `PkBuf` per scanned key).
    ///
    /// `check_dups` is a caller policy (hoisted out of this function): the live
    /// CREATE-INDEX/promote paths pass `is_unique && ctx.is_live()` / `true`; the
    /// worker/standalone boot rebuild passes `false`. A boot-replayed IDX_TAB
    /// `+1` was validated at original write time and every later INSERT went
    /// through the unique filter, so skipping the check at boot cannot admit a
    /// duplicate; it only avoids carrying a `seen` set over every row. A slice-local
    /// rebuild also cannot false-positive (a global duplicate may legitimately
    /// straddle two workers' slices), so the boot skip is doubly justified. The
    /// ingest is unconditional — it IS the ephemeral index rebuild/backfill.
    ///
    /// On a duplicate found mid-stream the partially-ingested index table is
    /// discarded whole: it is ephemeral, registered nowhere, and
    /// `hook_index_register`'s staged directory is removed as this error unwinds.
    fn stream_index_projection(
        &mut self,
        owner_id: i64,
        targets: &[IndexProjectionTarget],
        check_dups: bool,
    ) -> Result<(), String> {
        if targets.is_empty() {
            return Ok(());
        }
        let chunk_rows = self.ddl_scan_chunk_rows;
        let mut seen: Vec<rustc_hash::FxHashSet<PkBuf>> = if check_dups {
            targets.iter().map(|_| rustc_hash::FxHashSet::default()).collect()
        } else {
            Vec::new()
        };

        let Some(mut handle) = self.open_store_cursor(owner_id) else {
            return Ok(());
        };
        while let Some(chunk) = handle.drain_chunk(chunk_rows) {
            for (ti, t) in targets.iter().enumerate() {
                let projected = DagEngine::batch_project_index(&chunk, &t.spec, &t.idx_schema);
                if projected.count == 0 {
                    continue;
                }
                // The duplicate check applies to the full composite leading span.
                if check_dups && projected_chunk_has_dup_keys(&projected, t.spec.key_size(), &mut seen[ti]) {
                    return Err(self.unique_create_dup_err(owner_id, t.cols.as_slice()));
                }
                if let Some(table) = t.table {
                    unsafe { &mut *table }
                        .ingest_owned_batch(projected)
                        .map_err(|e| format!("index backfill: ingest failed (owner {owner_id}): {e}"))?;
                }
            }
        }
        Ok(())
    }

    /// Promote the existing index circuit on `col_idx` to unique, after verifying
    /// the committed base rows contain no duplicate keys. Used when a UNIQUE index
    /// registers over a column that already has a circuit (an FK auto-index, or a
    /// prior non-unique index): the per-column dedup keeps one circuit, so the
    /// uniqueness is folded into the incumbent — no second index table is built
    /// (`make_index_schema` does not depend on `is_unique`; uniqueness is the flag
    /// plus the duplicate check, not a different storage layout). Empty base table
    /// → pure flag flip. Skips the scan outside the live phase (boot shard replay)
    /// because data was validated at original write time (mirrors the same guard
    /// in `hook_cascade_fk`). The flag flip below runs unconditionally.
    pub(crate) fn promote_index_to_unique(
        &mut self,
        owner_id: i64,
        owner_schema: &SchemaDescriptor,
        col_indices: &[u32],
    ) -> Result<(), String> {
        if self.ctx.is_live() {
            let idx_schema = make_index_schema(col_indices, owner_schema)?;
            let target = IndexProjectionTarget {
                cols: PkColList::from_slice(col_indices),
                spec: crate::schema::IndexKeySpec::new(col_indices, owner_schema, &idx_schema),
                idx_schema,
                table: None,
            };
            self.stream_index_projection(owner_id, &[target], true)?;
        }
        self.dag.set_index_circuit_uniqueness(owner_id, col_indices, true);
        Ok(())
    }

    // -- FK auto-index creation -------------------------------------------

    pub(crate) fn create_fk_indices(&mut self, table_id: i64) -> Result<(), String> {
        // An unregistered owner has no columns to auto-index; checked before the
        // COL_TAB read so the miss costs nothing. The registered schema is where
        // the PK list lives: `register_relation` builds it from the same
        // `TABLE_TAB.pk_col_idx` the row persisted.
        let Some(owner_schema) = self.dag.tables.get(&table_id).map(|e| e.schema) else {
            return Ok(());
        };
        let pk_cols = owner_schema.pk_indices();
        let col_defs = self.read_column_defs(table_id);

        let (schema_name, table_name) = self.caches.entity_by_id.get(&table_id).cloned().unwrap_or_default();

        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id == 0 {
                continue;
            }
            // Skip every PK column: the PK region already stores them, so an
            // FK whose column is part of the PK needs no separate auto-index.
            if pk_cols.contains(&(col_idx as u32)) {
                continue;
            }
            let index_name = make_fk_index_name(&schema_name, &table_name, &cd.name);
            if self.caches.index_by_name.contains_key(&index_name) {
                continue;
            }

            let index_id = self.allocate_index_id();

            // Write index record to sys_indices (FK indices are not unique).
            let packed_cols = gnitz_wire::pack_pk_cols(&[col_idx as u32]);
            let batch = idx_tab_batch(index_id, table_id, packed_cols, &index_name, false, 1);
            // hook_cascade_fk fires on master and every worker, so each side
            // creates its own FK indices locally; submit_local applies + fires
            // hooks without a broadcast. submit would broadcast IDX_TAB before
            // TABLE_TAB and duplicate the rows the worker already produced.
            if let Err(e) = self.submit_local(SysFamily::Index, batch) {
                // The +1 reached sys_indices but its directory/cache setup failed
                // and was never broadcast. Submit the matching -1 locally to
                // reverse the storage write and any partial cache updates;
                // otherwise the next boot's replay opens a missing index
                // directory and crashes.
                let undo = idx_tab_batch(index_id, table_id, packed_cols, &index_name, false, -1);
                self.rollback_index_registration(undo, index_id);
                return Err(e);
            }
        }
        Ok(())
    }
}

/// One index target of a `stream_index_projection` pass: the source column
/// list (for the duplicate-violation message), the span encode plan, the
/// index layout to project into, and — for the backfill/rebuild callers —
/// the index table each projected chunk is ingested into (`None` = check-only,
/// the promote path).
struct IndexProjectionTarget {
    cols: PkColList,
    spec: crate::schema::IndexKeySpec,
    idx_schema: SchemaDescriptor,
    table: Option<*mut Table>,
}

/// True if a positive-weight row in a projected index chunk shares its leading
/// index key (the first `key_size` bytes of the index PK) with another row of
/// this chunk or any earlier chunk recorded in `seen`.
///
/// Compound-PK index layout: index PK is `(indexed_key…, src_pk…)`. Uniqueness
/// applies to the leading `indexed_key` span only — two rows differing only in
/// their source-PK suffix represent two source rows sharing the indexed value.
/// For a composite `UNIQUE (a, b, …)` the span is the sum of every promoted
/// column's width and can exceed 16 bytes, so the dedup token is a `PkBuf`
/// holding the raw span (a stack key, no per-row heap allocation), not a
/// truncating `u128`.
///
/// `seen` is caller-owned because the scan is chunked: cross-chunk duplicates
/// are only visible through state carried across calls. Shared by
/// `backfill_index` (fresh unique index) and `promote_index_to_unique`
/// (UNIQUE folded into an existing circuit) so both gate on the same predicate.
fn projected_chunk_has_dup_keys(projected: &Batch, key_size: usize, seen: &mut rustc_hash::FxHashSet<PkBuf>) -> bool {
    for row in 0..projected.count {
        let weight = projected.get_weight(row);
        if weight <= 0 {
            continue;
        }
        // Base-table scan chunks are consolidated: weight ≥ 2 is the same
        // (PK, payload) row inserted multiple times — that many live
        // instances of the same index key. NULL-valued rows never reach
        // here (batch_project_index skips them).
        if weight > 1 {
            return true;
        }
        let pk_bytes = projected.get_pk_bytes(row);
        if !seen.insert(PkBuf::from_bytes(&pk_bytes[..key_size])) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod dup_key_tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

    /// Build a projected-index batch whose PK region is each supplied span
    /// (here the whole index PK; the dedup keys on the leading `key_size`).
    fn idx_batch(spans: &[[u8; 24]]) -> Batch {
        // Three U64 PK columns → a 24-byte composite span (> 16 bytes).
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0); 3], &[0, 1, 2]);
        let mut b = Batch::with_capacity(schema, spans.len().max(1));
        for s in spans {
            b.ensure_row_capacity();
            b.extend_pk_bytes(s);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.count += 1;
        }
        b
    }

    /// A >16-byte composite span is deduped on its FULL width: two spans sharing
    /// their leading 16 bytes but differing in the trailing 8 are distinct (the
    /// `u128` truncation this replaced would have falsely reported a duplicate),
    /// while an exact repeat is a duplicate.
    #[test]
    fn dup_keys_over_16_bytes_no_truncation() {
        let span = |tail: u64| {
            let mut s = [0u8; 24];
            s[..16].copy_from_slice(&[7u8; 16]); // shared leading 16 bytes
            s[16..].copy_from_slice(&tail.to_be_bytes());
            s
        };

        let mut seen = rustc_hash::FxHashSet::default();
        assert!(
            !projected_chunk_has_dup_keys(&idx_batch(&[span(1), span(2)]), 24, &mut seen),
            "distinct 24-byte spans sharing a 16-byte prefix are NOT duplicates",
        );

        let mut seen2 = rustc_hash::FxHashSet::default();
        assert!(
            projected_chunk_has_dup_keys(&idx_batch(&[span(1), span(1)]), 24, &mut seen2),
            "identical 24-byte spans are duplicates",
        );
    }

    /// `seen` carries cross-chunk state: a duplicate split across two chunk
    /// calls is caught on the second chunk.
    #[test]
    fn dup_keys_cross_chunk() {
        let span = |tail: u64| {
            let mut s = [0u8; 24];
            s[16..].copy_from_slice(&tail.to_be_bytes());
            s
        };
        let mut seen = rustc_hash::FxHashSet::default();
        assert!(!projected_chunk_has_dup_keys(&idx_batch(&[span(1)]), 24, &mut seen));
        assert!(
            projected_chunk_has_dup_keys(&idx_batch(&[span(1)]), 24, &mut seen),
            "the same span in a later chunk is a cross-chunk duplicate",
        );
    }
}
