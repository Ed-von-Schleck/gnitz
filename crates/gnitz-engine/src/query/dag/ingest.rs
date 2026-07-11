//! The ingestion pipeline: unique-PK enforcement, store + secondary-index
//! application, index projection, and the flush / checkpoint table collection.

use super::*;

/// Fault-injection seam for the push-apply / SAL-replay path; identity in
/// release builds. In a debug build with
/// `GNITZ_INJECT_INGEST_APPLY_ERROR=store|index`, substitutes
/// `Err(StorageError::Io)` for the matching ingest inside
/// `ingest_store_and_indices`, so the fail-stop abort fires deterministically
/// without a real disk fault. No one-shot latch — the process aborts on first
/// fire. The env is read once. Placing the seam at the dag layer keeps
/// `storage` seam-free and fires exactly on the push-apply/replay path (VM
/// integration bypasses it), so a seam-armed server still boots cleanly on a
/// pushless data dir and fails only on the first INSERT apply.
fn inject_ingest_apply_error(
    which: &str,
    r: Result<(), crate::storage::StorageError>,
) -> Result<(), crate::storage::StorageError> {
    #[cfg(debug_assertions)]
    {
        use std::sync::OnceLock;
        static MODE: OnceLock<Option<String>> = OnceLock::new();
        let armed = MODE.get_or_init(|| std::env::var("GNITZ_INJECT_INGEST_APPLY_ERROR").ok());
        if armed.as_deref() == Some(which) {
            return Err(crate::storage::StorageError::Io);
        }
    }
    #[cfg(not(debug_assertions))]
    let _ = which;
    r
}

/// The per-PK batch state of the unique-PK enforcement walk. `store_probed`
/// must stay sticky across a delete of the same PK (only the insert fact is
/// cleared): losing it would re-emit the stored-row retraction on a later
/// re-insert of the PK and drive base-table weights negative.
#[derive(Default, Clone, Copy)]
struct UniquePkRowState {
    /// Batch row index of the last `+1` insertion of this PK, if still live.
    /// A batch index (not an effective index): `append_batch_negated` reads
    /// from `batch`, and the effective batch carries extra store-retraction
    /// rows that break any 1:1 correspondence.
    last_insert: Option<usize>,
    /// The store was already probed (and any stored row retracted) for this PK.
    /// `retract_pk_bytes` is a pure lookup (it only arms the `found_*`
    /// accessors) and the store cannot change mid-batch, so one probe per PK is
    /// exact — and the stored-row retraction must be emitted at most once, or
    /// downstream weights go negative.
    store_probed: bool,
}

impl DagEngine {
    // ── Ingestion ───────────────────────────────────────────────────────

    /// Ingest a batch into a table's store + index projections.
    ///
    /// Stages:
    /// 1. unique_pk enforcement (retract existing, dedup intra-batch)
    /// 2. store.ingest_batch
    /// 3. index projection
    pub fn ingest_to_family(&mut self, table_id: i64, batch: Batch) -> i32 {
        if self.ingest_returning_effective(table_id, batch).is_some() {
            0
        } else {
            -1
        }
    }

    /// Ingest a borrowed batch (no clone) for the common non-unique-PK path.
    /// For unique_pk tables, falls back to cloning + `ingest_returning_effective`.
    pub fn ingest_by_ref(&mut self, table_id: i64, batch: &Batch) -> i32 {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => return -1,
        };

        if entry.unique_pk() {
            return self.ingest_to_family(table_id, batch.clone_batch());
        }

        if batch.count == 0 {
            return 0;
        }

        let schema = entry.schema;
        Self::ingest_store_and_indices(table_id, entry, &schema, batch);

        0
    }

    /// Ingest a batch and return the effective batch (after unique_pk
    /// enforcement) — what downstream views need to see. `None` means exactly
    /// "table not registered"; a storage-apply failure never returns
    /// (`ingest_store_and_indices` aborts).
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: Batch) -> Option<Batch> {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => {
                gnitz_warn!("dag: ingest_returning_effective — table_id={} not registered", table_id);
                return None;
            }
        };

        let schema = entry.schema;
        let unique_pk = entry.unique_pk();
        // unique_pk ⟹ Partitioned: every SQL-created base table is registered
        // Partitioned; system tables (the only Borrowed handles) are never
        // unique_pk, so a unique_pk relation always resolves to a PartitionedTable.
        // The debug_assert turns any future stray Borrowed+unique_pk registration
        // into a loud failure in debug; a release build passes the batch through
        // unenforced rather than skipping enforcement on a missing partitioned store.
        let effective_batch = if unique_pk {
            match entry.handle.as_partitioned_mut() {
                Some(ptable) => Self::enforce_unique_pk(ptable, &schema, batch),
                None => {
                    debug_assert!(false, "unique_pk relation {table_id} must be a PartitionedTable");
                    batch
                }
            }
        } else {
            batch
        };

        if effective_batch.count == 0 {
            return Some(effective_batch);
        }

        let entry = self.tables.get_mut(&table_id).unwrap();

        Self::ingest_store_and_indices(table_id, entry, &schema, &effective_batch);

        Some(effective_batch)
    }

    /// Project all index batches from `source`, ingest a clone into the store,
    /// then drain the projected index batches into their respective index tables.
    /// Shared by `ingest_by_ref` and `ingest_returning_effective`.
    ///
    /// A storage error here means committed (or SAL-replayed) data was not
    /// applied while the client already holds a durability ACK, so process state
    /// has diverged from the durable SAL. That is fatal at the point of
    /// detection: `gnitz_fatal_abort!` and let restart + SAL replay re-apply the
    /// batch (its WAL zone stays above the flushed-shard watermark, so it *will*
    /// be replayed). Silent swallowing is the one unsound response — it neither
    /// applies nor replays the entry, and the next checkpoint orphans it.
    fn ingest_store_and_indices(table_id: i64, entry: &mut TableEntry, schema: &SchemaDescriptor, source: &Batch) {
        let index_batches: Vec<Batch> = entry
            .index_circuits
            .iter()
            .map(|ic| Self::batch_project_index(source, &ic.key_spec, schema, &ic.index_schema))
            .collect();

        if let Err(e) = inject_ingest_apply_error("store", entry.handle.ingest_borrowed_batch(source)) {
            crate::gnitz_fatal_abort!(
                "dag: base-table ingest failed (table_id={}): {} — committed data \
                 not applied, state diverged from durable SAL; aborting for \
                 restart+replay",
                table_id,
                e,
            );
        }

        for (ic, idx_batch) in entry.index_circuits.iter_mut().zip(index_batches) {
            if idx_batch.count > 0 {
                let index_id = ic.index_id;
                if let Err(e) = inject_ingest_apply_error("index", ic.table_mut().ingest_owned_batch(idx_batch)) {
                    crate::gnitz_fatal_abort!(
                        "dag: secondary-index ingest failed (table_id={}, index_id={}): {} \
                         — index diverged from base table; aborting for restart+replay",
                        table_id,
                        index_id,
                        e,
                    );
                }
            }
        }
    }

    // ── Flush / checkpoint collection ───────────────────────────────────

    /// Flush a table's WAL through the store handle and every index circuit.
    /// An unregistered `table_id` is a caller bug (debug_assert), a no-op in
    /// release; `Err` is always a storage fault.
    pub fn flush(&mut self, table_id: i64) -> Result<(), StorageError> {
        let Some(entry) = self.tables.get_mut(&table_id) else {
            debug_assert!(false, "flush of unregistered table_id {table_id}");
            return Ok(());
        };
        entry.handle.flush()?;
        for ic in &mut entry.index_circuits {
            ic.table_mut().flush()?;
        }
        Ok(())
    }

    /// Flush `view_id`'s trace, aborting the process on a storage fault: the
    /// callers flush a view they just ingested or backfilled, so a failure is
    /// a RAM-tier spill fault — the trace can no longer be bounded, and
    /// continuing would grow memory unchecked under a sustained fault.
    /// Restart re-derives the view from its base tables (the same disk fault
    /// would already abort the base ingest via `ingest_store_and_indices`).
    pub fn flush_view_or_abort(&mut self, view_id: i64) {
        if let Err(e) = self.flush(view_id) {
            crate::gnitz_fatal_abort!(
                "dag: view trace flush failed (view_id={}): {} — view state \
                 cannot be bounded; aborting for restart+re-derive",
                view_id,
                e,
            );
        }
    }

    /// Collect the tables the base checkpoint round (`FLAG_FLUSH`) flushes:
    /// every registered user relation's partitions plus its index-circuit
    /// tables. System tables (`StoreHandle::Borrowed`) are skipped — workers
    /// never barrier-flush their inherited `_sys` copies. `SalReplay` partitions
    /// publish (`Pending`); rederived partitions and index tables fold to RAM
    /// (`DoneInline`), which the generic flush loop consumes.
    ///
    /// Same `*mut Table` validity argument as `collect_ephemeral_flush_tables`
    /// below.
    pub(crate) fn collect_base_flush_tables(&mut self) -> Vec<*mut Table> {
        let mut out: Vec<*mut Table> = Vec::new();
        for entry in self.tables.values_mut() {
            if let Some(pt) = entry.handle.as_partitioned_mut() {
                for t in pt.partitions_mut() {
                    out.push(t as *mut Table);
                }
            }
            for ic in &mut entry.index_circuits {
                out.push(ic.table_mut() as *mut Table);
            }
        }
        out
    }

    /// Collect the tables the ephemeral checkpoint round force-persists, in two
    /// disjoint sets: (1) every compiled view plan's operator-trace tables, and
    /// (2) every view's output-store partitions. The worker flushes set 1 fully
    /// durable before set 2 (the flush-ordering invariant: any output manifest at
    /// generation G implies that view's traces are durable at G).
    ///
    /// Returned as raw `*mut Table` — owned trace tables are not in `self.tables`
    /// so they cannot be keyed by `tid`, and the engine already passes
    /// `*mut Table`. Valid because the worker flush handler is a synchronous `fn`
    /// on a single-threaded process: no reactor yield and no concurrent
    /// `cache`/`tables` mutation, so the partition set is frozen for the flush.
    /// `cache` and `tables` are separate fields (clean disjoint borrow) and the
    /// two sets are disjoint allocations (scratch dirs vs the view dir). Index
    /// tables are excluded: they live in `TableEntry::index_circuits`, not the
    /// plan cache, and stay erase-at-boot.
    pub(crate) fn collect_ephemeral_flush_tables(&mut self) -> (Vec<*mut Table>, Vec<*mut Table>) {
        let mut traces: Vec<*mut Table> = Vec::new();
        for plan in self.cache.values_mut() {
            for sub in plan.sub_plans_mut() {
                // Null owned cursors before the fold so none holds a stale snapshot.
                sub.vm.null_owned_cursors();
                for owned in sub.vm.owned_tables.iter_mut() {
                    traces.push(&mut **owned as *mut Table);
                }
            }
        }

        let mut outputs: Vec<*mut Table> = Vec::new();
        for entry in self.tables.values() {
            if entry.kind != RelationKind::View {
                continue;
            }
            // Views are always `Partitioned`; a replicated view holds exactly its
            // one worker-owned partition (no `part_0`-only miss).
            if let Some(pt) = entry.handle.as_partitioned_mut() {
                for t in pt.partitions_mut() {
                    outputs.push(t as *mut Table);
                }
            }
        }
        (traces, outputs)
    }

    // ── unique-PK enforcement ───────────────────────────────────────────

    /// Enforce unique-PK semantics on an ingest batch: retract any stored row
    /// with the same PK before inserting the new one, and resolve duplicate PKs
    /// within the batch so each surviving PK nets to a single live row.
    ///
    /// Emits the stored-row retraction (`-1`, old payload) into the effective
    /// batch so downstream views see the old payload removed before the new one
    /// lands. Keys on `get_pk_bytes` (verbatim OPK) and dedups on `&[u8]` slices
    /// borrowed from the batch's PK region — correct for every PK width. Never
    /// round-trips through a native `u128`
    /// (which `opk_key` would re-encode, double-flipping a signed PK's sign bit,
    /// so the probe would match no stored row and the retraction would be
    /// silently dropped).
    pub(super) fn enforce_unique_pk(
        ptable: &mut PartitionedTable,
        schema: &SchemaDescriptor,
        mut batch: Batch,
    ) -> Batch {
        // Empty-batch guard: empty batches reach the engine via the
        // `CatalogStore` ingest wrappers, which — unlike the worker loop — do
        // not pre-filter `count == 0`.
        if batch.count == 0 {
            return batch;
        }
        // unique_pk contract: per-PK accumulated weight ∈ {0, 1}. A pushed row
        // at |w| > 1 is the row repeated; retract-before-insert collapses
        // repeats to one live instance (and a delete removes at most one), so
        // normalize weights to ±1 before the enforcement walk. Must run on the
        // input batch, not at append time: `append_batch_negated` re-reads the
        // original row, so clamping only the appended copy would emit `+1` then
        // `-w` for the same element and drive intra-batch dedup net-negative.
        batch.map_weights(|w| w.clamp(-1, 1));

        let mut effective = Batch::with_schema(*schema, batch.count * 2);
        let mut state: FxHashMap<&[u8], UniquePkRowState> =
            FxHashMap::with_capacity_and_hasher(batch.count, Default::default());

        for row in 0..batch.count {
            let w = batch.get_weight(row);
            if w == 0 {
                continue;
            }
            let pkb = batch.get_pk_bytes(row);
            let st = state.entry(pkb).or_default();

            // Stored-row retraction — shared by insert and delete. Probe the
            // store the first time this PK is seen and, if found, emit a
            // retraction of the stored (PK, payload) so downstream views drop
            // the old payload. Gating on `store_probed` skips the repeated LSM
            // point lookup for a PK the batch touches again.
            if !st.store_probed {
                st.store_probed = true;
                let (_existing_w, stored_row) = ptable.retract_pk_bytes(pkb);
                if let Some(stored_row) = stored_row {
                    // The located stored row is an owned `ColumnarSource` view;
                    // copy it in at weight -1 via the canonical source-append.
                    effective.append_row_from_source_bytes(pkb, -1, &stored_row, 0, None);
                }
            }

            if w > 0 {
                // Insert. If this PK was already inserted in this batch, retract
                // that earlier insertion (intra-batch upsert: last value wins).
                if let Some(prev_pos) = st.last_insert {
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                }
                st.last_insert = Some(row);
                effective.append_batch(&batch, row, row + 1);
            } else {
                // Delete (w < 0). The stored-row retraction above already emitted
                // the removal; here only cancel a prior intra-batch insertion and
                // clear the insert fact so a later re-insert of this PK is not
                // re-negated (`store_retracted` stays sticky — see its doc).
                // A retraction of a key that is neither stored nor seen has
                // nothing to cancel — passing it through would store a
                // negative-weight phantom row (violating base-table positivity),
                // and dropping it is idempotent under delete replay.
                if let Some(prev_pos) = st.last_insert.take() {
                    effective.append_batch_negated(&batch, prev_pos, prev_pos + 1);
                }
            }
        }

        effective
    }

    /// Batch-level index projection.
    ///
    /// Compound-PK index schema layout:
    ///   `(indexed_col [promoted], src_pk_0, src_pk_1, …)`
    /// every column is in the PK, no payload columns. We hand-pack the index
    /// PK bytes: the leading slot is the indexed column value (low bytes of
    /// its LE form, zero-padded out to the index column's width), followed
    /// by each source PK column laid out contiguously after it.
    pub(crate) fn batch_project_index(
        src: &Batch,
        spec: &crate::schema::IndexKeySpec,
        src_schema: &SchemaDescriptor,
        idx_schema: &SchemaDescriptor,
    ) -> Batch {
        let src_pk_stride = src_schema.pk_stride() as usize;
        let idx_stride = idx_schema.pk_stride() as usize;

        let mut out = Batch::with_schema(*idx_schema, src.count.max(1));
        // MAX_PK_BYTES bounds every index schema's pk_stride (asserted in
        // SchemaDescriptor::new), so the scratch PK buffer lives on the stack
        // with no per-batch heap allocation. The used [..idx_stride] prefix is
        // fully overwritten each row (the leading [..idx_key_size] OPK-encoded
        // indexed value(s) and trailing source PK suffix); the single zero-init
        // covers the (currently empty) tail.
        let mut idx_pk_buf = [0u8; crate::schema::MAX_PK_BYTES];

        let mb = src.as_mem_batch();

        // `spec` is the per-column read/encode plan — the registered circuit's
        // precomputed `key_spec` on the per-push path — so the row loop does no
        // per-column method call or schema indexing.
        let idx_key_size = spec.key_size();

        for row in 0..src.count {
            let weight = src.get_weight(row);
            if weight == 0 {
                continue;
            }
            // Leading index-key slots: each indexed value re-encoded OPK into
            // its promoted slot. The index table's PK region is
            // order-preserving like any other; seeks (has_pk / seek_by_index)
            // encode the same way. NULL in ANY indexed column ⇒ row not
            // indexed; retractions (weight < 0) DO project, so the index
            // entry retracts with its source row.
            if !spec.write_span(&mb, row, &mut idx_pk_buf) {
                continue;
            }
            // The source PK region is laid out in pk_indices order, so the
            // index's trailing PK suffix is byte-identical to the source's PK
            // (already OPK).
            idx_pk_buf[idx_key_size..idx_key_size + src_pk_stride].copy_from_slice(src.get_pk_bytes(row));
            out.extend_pk_bytes(&idx_pk_buf[..idx_stride]);
            out.extend_weight(&weight.to_le_bytes());
            // Index schema has zero payload columns, but the null_bmp region
            // is still part of the batch layout. Keep the per-row null-bmp
            // append so the batch's region cursors stay in lockstep with
            // `count` independent of `with_schema`'s zero-init.
            out.extend_null_bmp(&0u64.to_le_bytes());
            out.count += 1;
        }

        // `out` is `Raw` from `with_schema`; the `extend_*` loop above never raises
        // it, and the index-table ingest re-sorts/folds it.
        out
    }
}
