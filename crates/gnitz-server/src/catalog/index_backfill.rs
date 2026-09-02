//! Secondary-index construction: opening an index table, the chunked base-table
//! scan that projects into it, its two entry points (a fresh CREATE INDEX and
//! the worker's boot rebuild), the UNIQUE promote, and FK auto-index creation.

use super::*;
use gnitz_store::schema::key::PkBuf;
use gnitz_store::schema::make_index_schema;

impl CatalogEngine {
    /// Locally retract an index registration whose +1 was applied but never
    /// broadcast. `Err` means the rollback itself failed, leaving the +1 in
    /// sys_indices while the client is handed an error — a permanently diverged
    /// catalog, and the next boot's replay would open a missing index directory.
    /// The caller that owns a watchdog fail-stops on it, matching
    /// `compensate_stage_a`'s rollback.
    pub(super) fn rollback_index_registration(&mut self, undo: Batch, index_id: i64) -> Result<(), String> {
        self.submit_local(SysFamily::Index, undo).map_err(|undo_err| {
            format!(
                "catalog: index registration rollback failed (index_id={index_id}): {undo_err} \
                 — catalog state permanently diverged"
            )
        })
    }

    /// Open this process's copy of a secondary-index table under `idx_dir` — the
    /// one recipe for the live CREATE INDEX hook and the worker-boot rebuild, so
    /// the two cannot diverge on where it is homed or on its resume gate.
    ///
    /// A forked worker homes at its own `w{rank}of{n}` child, like every other
    /// relation store. The master and standalone home at `idx_dir` itself: the
    /// master's copy stays permanently empty, and homing it at `w0of{n}` would
    /// put it on the directory worker 0's inherited handle already holds.
    pub(in crate::catalog) fn new_index_table(
        &self,
        idx_dir: &str,
        index_id: i64,
        idx_schema: SchemaDescriptor,
    ) -> Result<Table, String> {
        let table_dir = if gnitz_store::foundation::worker_ctx::is_worker() {
            ChildAddr::this_worker(self.registry.num_workers()).dir(idx_dir)
        } else {
            idx_dir.to_string()
        };
        Table::new(&table_dir, idx_schema, index_id as u32, self.registry.rederive_source())
            .map_err(|e| format!("Failed to create index table {index_id}: error {e}"))
    }

    // -- Index backfill (scan source, project into index table) ------------

    pub(in crate::catalog) fn backfill_index(
        &mut self,
        owner_id: i64,
        owner_schema: &SchemaDescriptor,
        col_indices: &[u32],
        idx_table: *mut Table,
        idx_schema: &SchemaDescriptor,
        check_dups: bool,
    ) -> Result<(), String> {
        // The relation filled here is the *index table* (`owner_id` is the
        // indexed relation, a durable base table). Refused unless empty, in every
        // build: an index that resumed from its checkpoint already holds these
        // rows, and projecting them again would double every weight — a wrong
        // answer no later check looks for.
        if unsafe { &*idx_table }.estimated_rows() != 0 {
            return Err(format!(
                "backfill_index into a non-empty index (owner {owner_id}): would double-count"
            ));
        }
        // Built here, not read off a circuit: the fresh-index path runs before
        // `add_index_circuit` registers one.
        let target = IndexProjectionTarget {
            cols: PkColList::from_slice(col_indices),
            spec: gnitz_store::schema::IndexKeySpec::new(col_indices, owner_schema, idx_schema),
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
    pub(crate) fn backfill_all_indexes(&mut self) -> Result<usize, String> {
        // Snapshot the worklist first: each owner's rebuild mutably borrows
        // self.dag (`replace_index_table`), so no borrow of the registry may be
        // held across the loop. Only base tables carry index circuits, so
        // views/system tables contribute nothing.
        struct IndexWork {
            cols: PkColList,
            index_id: i64,
            idx_schema: SchemaDescriptor,
            key_spec: gnitz_store::schema::IndexKeySpec,
        }
        let worklist: Vec<(i64, String, Vec<IndexWork>)> = self
            .registry
            .entries()
            .filter(|(_, entry)| !entry.index_circuits.is_empty())
            .map(|(owner_id, entry)| {
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
                    .registry
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
    /// CREATE-INDEX/promote paths pass `is_unique && mode == Live` / `true`; the
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
        let chunk_rows = self.registry.ddl_scan_chunk_rows();
        let mut seen: Vec<rustc_hash::FxHashSet<PkBuf>> = if check_dups {
            targets.iter().map(|_| rustc_hash::FxHashSet::default()).collect()
        } else {
            Vec::new()
        };

        let Some(mut handle) = self.registry.open_store_cursor(owner_id) else {
            return Ok(());
        };
        while let Some(chunk) = handle.drain_chunk(chunk_rows) {
            for (ti, t) in targets.iter().enumerate() {
                let projected = gnitz_store::storage::batch_project_index(&chunk, &t.spec, &t.idx_schema);
                if projected.is_empty() {
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
    /// → pure flag flip. The verification scan runs on a first apply only: replayed
    /// and compensated data passed its duplicate check when originally written,
    /// and under compensation a spurious `Err` is a fatal abort. The flag flip
    /// below runs unconditionally.
    pub(in crate::catalog) fn promote_index_to_unique(
        &mut self,
        owner_id: i64,
        owner_schema: &SchemaDescriptor,
        col_indices: &[u32],
    ) -> Result<(), String> {
        if self.ctx.mode() == ApplyMode::Live {
            let idx_schema = make_index_schema(col_indices, owner_schema)?;
            let target = IndexProjectionTarget {
                cols: PkColList::from_slice(col_indices),
                spec: gnitz_store::schema::IndexKeySpec::new(col_indices, owner_schema, &idx_schema),
                idx_schema,
                table: None,
            };
            self.stream_index_projection(owner_id, &[target], true)?;
        }
        self.registry.set_index_circuit_uniqueness(owner_id, col_indices, true);
        Ok(())
    }

    // -- FK auto-index creation -------------------------------------------

    pub(in crate::catalog) fn create_fk_indices(&mut self, table_id: i64) -> Result<(), String> {
        // An unregistered owner has no columns to auto-index; checked before the
        // COL_TAB read so the miss costs nothing. The registered schema is where
        // the PK list lives: `register_relation` builds it from the same
        // `TABLE_TAB.pk_col_idx` the row persisted.
        let Some(owner_schema) = self.registry.entry(table_id).map(|e| e.schema) else {
            return Ok(());
        };
        let pk_cols = owner_schema.pk_indices();
        let col_defs = self.read_column_defs(table_id);

        for (col_idx, cd) in col_defs.iter().enumerate() {
            if cd.fk_table_id == 0 {
                continue;
            }
            // Skip every PK column: the PK region already stores them, so an
            // FK whose column is part of the PK needs no separate auto-index.
            if pk_cols.contains(&(col_idx as u32)) {
                continue;
            }
            let index_name = make_fk_index_name(table_id, col_idx);
            if self.caches.index_by_name.contains_key(&index_name) {
                continue;
            }

            let index_id = self
                .allocate_index_id()
                .map_err(|e| format!("index id allocation failed: {e}"))?;

            // Write index record to sys_indices (FK indices are not unique).
            // `submit_local` bypasses the precheck, and is the only path allowed
            // to set the internal bit.
            let packed_cols = gnitz_wire::pack_pk_cols(&[col_idx as u32]);
            let props = gnitz_wire::IndexProps {
                is_unique: false,
                is_internal: true,
            };
            let batch = idx_tab_batch(index_id, table_id, packed_cols, &index_name, props, 1);
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
                let undo = idx_tab_batch(index_id, table_id, packed_cols, &index_name, props, -1);
                self.rollback_index_registration(undo, index_id)?;
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
    spec: gnitz_store::schema::IndexKeySpec,
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
    for row in 0..projected.len() {
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
#[path = "tests/index_backfill.rs"]
mod tests;
