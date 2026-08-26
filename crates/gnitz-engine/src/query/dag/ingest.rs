//! The ingestion pipeline: unique-PK enforcement, store + secondary-index
//! application, index projection, and the flush / checkpoint table collection.

use super::*;

/// `GNITZ_INJECT_INGEST_APPLY_ERROR=store|index`: substitute `Err(Io)` for the
/// matching ingest below, so the fail-stop abort fires without a real disk
/// fault. No one-shot latch — the process aborts on first fire. Placing the seam
/// at the dag layer keeps `storage` seam-free and fires exactly on the
/// push-apply/replay path (VM integration bypasses it), so a seam-armed server
/// still boots cleanly on a pushless data dir and fails only on the first INSERT
/// apply.
static INGEST_APPLY_ERROR: crate::foundation::fault::Seam =
    crate::foundation::fault::Seam::new("GNITZ_INJECT_INGEST_APPLY_ERROR");

fn inject_ingest_apply_error(
    which: &str,
    r: Result<(), crate::storage::StorageError>,
) -> Result<(), crate::storage::StorageError> {
    if INGEST_APPLY_ERROR.at(which) {
        return Err(crate::storage::StorageError::Io(libc::EIO));
    }
    r
}

impl DagEngine {
    // ── Ingestion ───────────────────────────────────────────────────────

    /// Ingest a borrowed batch (no clone) for relations that run no PK
    /// enforcement. A base table falls back to cloning +
    /// [`Self::ingest_returning_effective`].
    pub fn ingest_by_ref(&mut self, table_id: i64, batch: &Batch) -> Result<(), StorageError> {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => {
                gnitz_warn!("dag: ingest_by_ref — table_id={} not registered", table_id);
                return Ok(());
            }
        };

        if entry.kind.is_base_table() {
            self.ingest_returning_effective(table_id, batch.clone_batch())?;
        } else if batch.count > 0 {
            Self::ingest_store_and_indices(table_id, entry, batch)?;
        }
        Ok(())
    }

    /// Ingest a view's tick output into its own store and — when the view carries
    /// a feed — a copy stamped with `tick_round` into its delta store.
    ///
    /// One entry lookup for both stores, as [`Self::ingest_store_and_indices`]
    /// already does for a relation and its index circuits. This is the only moment
    /// a view's delta exists as an addressable object: the store ingest below
    /// consolidates it into the memtable, whose fold drops net-zero (PK, payload)
    /// rows, so an insert in one round and its retraction in the next annihilate
    /// and no later reader could recover either. The capture and the ingest are
    /// the same batch on the same worker, so every batch this store absorbs on the
    /// tick path is captured byte-identically.
    ///
    /// **A replicated view stamps on worker 0 alone.** `execute_multi_worker_step`
    /// short-circuits one: every worker holds every source in full and computes
    /// the entire result locally, so `out_delta` is the full global delta on all
    /// W. A delta read of such a view is routed by `replicated_unicast` to worker
    /// 0, so the other W−1 delta stores would be written every tick and read
    /// never. Writer and reader take "which worker holds this feed" from the same
    /// replicated-placement fact, so they cannot come to different answers — and
    /// if they ever did, a broadcast gather over W identical stores would hand
    /// back every row W times, which no row-set comparison would show.
    ///
    /// **A fed view consolidates once, here, and both stores take the result.**
    /// The two ingests would otherwise sort and fold the same rows independently:
    /// `ingest_borrowed_batch` consolidates into the memtable's owned copy, and
    /// the stamp inherits its source's layout claim — which is `Raw` for every
    /// operator but the single-source exchange arm, `reduce` included — so
    /// `ingest_owned_batch` would re-sort it. Consolidating first makes the
    /// inherited `Consolidated` claim true (prepending one constant to every key
    /// preserves sortedness and distinctness), so the stamp is copied by value
    /// into a store that moves rather than re-folds it, and it carries the folded
    /// row count rather than the raw one.
    pub(crate) fn ingest_view_delta(
        &mut self,
        view_id: i64,
        batch: &Batch,
        tick_round: u64,
    ) -> Result<(), StorageError> {
        let Some(entry) = self.tables.get_mut(&view_id) else {
            gnitz_warn!("dag: ingest_view_delta — view_id={} not registered", view_id);
            return Ok(());
        };
        debug_assert!(
            entry.kind.is_view(),
            "ingest_view_delta drives the tick path, which only ever reaches a view; \
             a base table would silently skip enforce_unique_pk here",
        );
        if batch.count == 0 {
            return Ok(());
        }
        // Only a fed view that stamps on this worker pays the up-front fold; every
        // other view hands the batch straight to the store ingest it always did.
        // `delta.is_some()` leads, so a view with no feed — every view on a server
        // not using the feature — reads no placement and loads no worker rank.
        let stamps_here = entry.delta.is_some()
            && (!entry.schema.placement().is_replicated() || crate::foundation::worker_ctx::worker_rank() == 0);
        let folded = stamps_here
            .then(|| Batch::consolidate_if_needed(batch, &entry.schema))
            .flatten();
        let batch = folded.as_ref().unwrap_or(batch);
        Self::ingest_store_and_indices(view_id, entry, batch)?;

        let Some(feed) = entry.delta.as_ref().filter(|_| stamps_here) else {
            return Ok(());
        };
        let stamped = batch.stamped_with_pk_prefix(&entry.schema, &feed.schema, tick_round);
        if let Err(e) = feed.handle.ingest_owned_batch(stamped) {
            // Logged, not fatal, because **this round is not lost**. The batch is
            // moved into the memtable before anything fallible runs, and the only
            // error source above that is the spill: every one of its failure paths
            // leaves the run in the RAM tier (`persist_l0_run` writes, registers,
            // and only then clears the tier), and every error after the clear has
            // the rows registered on disk. So what failed is the spill of
            // accumulated data, which the next tick retries — not the capture.
            //
            // Aborting would also destroy the thing it claims to protect: unlike a
            // base table, whose abort is answered by SAL replay, a delta store is
            // opened `Rederive { resume_at: None }` and is **erased at open**, so a
            // restart drops every retained round of every fed view and forces every
            // subscriber to bootstrap again. The cost of continuing is back-pressure
            // — a RAM tier that grows while the disk stays broken — not a hole.
            gnitz_error!(
                "dag: delta-store spill failed (view_id={}, round={}): {} — the round is \
                 held in RAM and retried on the next tick; the feed is intact",
                view_id,
                tick_round,
                e,
            );
        }
        Ok(())
    }

    /// Ingest a batch into a relation's store + index projections and return the
    /// effective batch (after PK enforcement) — what downstream views need to
    /// see. `Ok(None)` means exactly "table not registered"; `Err` is a
    /// storage-apply failure.
    ///
    /// Named apart from `CatalogEngine::ingest_to_family` (which routes a
    /// *system* family through the precheck/hooks path) — the two are different
    /// operations that were one keystroke apart.
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: Batch) -> Result<Option<Batch>, StorageError> {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => {
                gnitz_warn!("dag: ingest_returning_effective — table_id={} not registered", table_id);
                return Ok(None);
            }
        };

        let effective_batch = if entry.kind.is_base_table() {
            entry.handle.enforce_unique_pk(&entry.schema, batch)
        } else {
            batch
        };

        if effective_batch.count > 0 {
            Self::ingest_store_and_indices(table_id, entry, &effective_batch)?;
        }
        Ok(Some(effective_batch))
    }

    /// Project all index batches from `source`, ingest a clone into the store,
    /// then drain the projected index batches into their respective index tables.
    /// Shared by `ingest_by_ref` and `ingest_returning_effective`.
    ///
    /// A storage error here means committed (or SAL-replayed) data was not
    /// applied while the client already holds a durability ACK, so process state
    /// has diverged from the durable SAL. Silent swallowing is the one unsound
    /// response — it neither applies nor replays the entry, and the next
    /// checkpoint orphans it. The error is returned instead: a caller that owns
    /// a watchdog aborts on it and lets restart + SAL replay re-apply the batch
    /// (its WAL zone stays above the flushed-shard watermark, so it *will* be
    /// replayed); one that does not, poisons its handle.
    fn ingest_store_and_indices(table_id: i64, entry: &mut TableEntry, source: &Batch) -> Result<(), StorageError> {
        let index_batches: Vec<Batch> = entry
            .index_circuits
            .iter()
            .map(|ic| Self::batch_project_index(source, &ic.key_spec, &ic.index_schema))
            .collect();

        inject_ingest_apply_error("store", entry.handle.ingest_borrowed_batch(source)).inspect_err(|e| {
            gnitz_error!(
                "dag: base-table ingest failed (table_id={}): {} — committed data \
                 not applied, state diverged from durable SAL",
                table_id,
                e,
            );
        })?;

        for (ic, idx_batch) in entry.index_circuits.iter_mut().zip(index_batches) {
            if idx_batch.count > 0 {
                let index_id = ic.index_id;
                inject_ingest_apply_error("index", ic.table_mut().ingest_owned_batch(idx_batch)).inspect_err(|e| {
                    gnitz_error!(
                        "dag: secondary-index ingest failed (table_id={}, index_id={}): {} \
                         — index diverged from base table",
                        table_id,
                        index_id,
                        e,
                    );
                })?;
            }
        }
        Ok(())
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

    /// Every store this process **owns and checkpoints**: each relation's `Owned`
    /// handle plus its index-circuit tables. `Borrowed` and `Detached` handles
    /// drop out — that is what keeps workers from barrier-flushing their inherited
    /// `_sys` copies.
    ///
    /// A fed view's delta store is `Owned` and is deliberately **not** here, which
    /// is what puts it in neither checkpoint round: it publishes no manifest and is
    /// erased at open, so there is nothing a restart could resume it from. That is
    /// also what makes its shard unlinking immediate rather than deferred to the
    /// post-publish drain this set feeds (`ShardIndex::evict_by_drop`).
    ///
    /// Both checkpoint rounds start from this one set and let `Table` decide:
    /// the base round is handed it whole (`flush_prepare` publishes the durable
    /// stores and folds the rederived ones to RAM), the ephemeral round takes
    /// the rederived half. Neither re-derives "which stores does this round
    /// touch" from the relation kind, so the two cannot drift apart.
    ///
    /// Returned as raw `*mut Table` — the engine already passes `*mut Table`,
    /// and owned trace tables are not in `self.tables` so they cannot be keyed
    /// by `tid`. Valid because the worker flush handler is a synchronous `fn` on
    /// a single-threaded process: no reactor yield and no concurrent
    /// `cache`/`tables` mutation, so the table set is frozen for the flush.
    pub fn collect_base_flush_tables(&mut self) -> Vec<*mut Table> {
        let mut out: Vec<*mut Table> = Vec::new();
        for entry in self.tables.values_mut() {
            if let Some(t) = entry.handle.as_owned_mut() {
                out.push(t as *mut Table);
            }
            for ic in &mut entry.index_circuits {
                out.push(ic.table_mut() as *mut Table);
            }
        }
        out
    }

    /// True when a base round would publish a cut newer than the last one.
    /// Asked of the same table set the round flushes, so the two cannot drift
    /// on which stores publish.
    pub fn base_advanced_since_publish(&mut self) -> bool {
        self.collect_base_flush_tables()
            .into_iter()
            .any(|t| unsafe { &*t }.base_round_advances_publish())
    }

    /// The tables the ephemeral checkpoint round force-persists, split into the
    /// two sets the worker flushes in order: every compiled view plan's
    /// operator-trace tables, then every rederived store (view outputs and
    /// secondary indexes). Set 1 goes fully durable before set 2, so any output
    /// manifest at generation G implies that view's traces are durable at G.
    ///
    /// The sets are disjoint allocations — scratch dirs versus the relation dir
    /// — and `cache` and `tables` are separate fields, so the borrows are clean.
    /// Same `*mut Table` validity argument as `collect_base_flush_tables`.
    pub fn collect_ephemeral_flush_tables(&mut self) -> (Vec<*mut Table>, Vec<*mut Table>) {
        // Iterate the (smaller) plan cache and consult `tables` — a disjoint
        // sibling field — for each plan's kind. Every `cache` entry has a
        // matching `tables` entry (`ensure_compiled` requires `tables.get`
        // first; `unregister_table` removes both), so this misses no view trace.
        let mut traces: Vec<*mut Table> = Vec::new();
        for (tid, plan) in self.cache.iter_mut() {
            if !self.tables.get(tid).is_some_and(|e| e.kind.is_view()) {
                continue;
            }
            for sub in plan.sub_plans_mut() {
                // Null owned cursors before the fold so none holds a stale snapshot.
                sub.vm.null_owned_cursors();
                for idx in sub.vm.program.table_indices() {
                    traces.push(sub.vm.program.table_mut(idx) as *mut Table);
                }
            }
        }

        let outputs = self
            .collect_base_flush_tables()
            .into_iter()
            .filter(|&t| unsafe { &*t }.is_rederived())
            .collect();
        (traces, outputs)
    }

    /// Batch-level index projection.
    ///
    /// Compound-PK index schema layout:
    ///   `(indexed_col [promoted], src_pk_0, src_pk_1, …)`
    /// every column is in the PK, no payload columns. We hand-pack the index
    /// PK bytes: the leading slot is the indexed column value (low bytes of
    /// its LE form, zero-padded out to the index column's width), followed
    /// by each source PK column laid out contiguously after it.
    pub fn batch_project_index(
        src: &Batch,
        spec: &crate::schema::IndexKeySpec,
        idx_schema: &SchemaDescriptor,
    ) -> Batch {
        let idx_stride = idx_schema.pk_stride() as usize;

        let mut out = Batch::with_capacity(*idx_schema, src.count.max(1));
        // MAX_PK_BYTES bounds every index schema's pk_stride (asserted in
        // SchemaDescriptor::new), so the scratch PK buffer lives on the stack
        // with no per-batch heap allocation. The used [..idx_stride] prefix is
        // fully overwritten each row (the leading [..idx_key_size] OPK-encoded
        // indexed value(s) and trailing source PK suffix); the single zero-init
        // covers the (currently empty) tail.
        let mut idx_pk_buf = [0u8; crate::schema::MAX_PK_BYTES];

        let mb = src.as_mem_batch();

        for row in 0..src.count {
            let weight = src.get_weight(row);
            if weight == 0 {
                continue;
            }
            // The row's entry key `[span ‖ src_pk]`: each indexed value
            // re-encoded OPK into its promoted slot, then the source PK region
            // verbatim (laid out in pk_indices order, already OPK). The index
            // table's PK region is order-preserving like any other; seeks
            // (has_pk / seek_by_index) encode the same way. NULL in ANY indexed
            // column ⇒ row not indexed; retractions (weight < 0) DO project, so
            // the index entry retracts with its source row.
            if !spec.write_entry(&mb, row, &mut idx_pk_buf) {
                continue;
            }
            out.extend_pk_bytes(&idx_pk_buf[..idx_stride]);
            out.extend_weight(&weight.to_le_bytes());
            // Index schema has zero payload columns, but the null_bmp region
            // is still part of the batch layout, and the arena is uninitialized
            // — so the per-row null-bmp append both keeps the region cursors in
            // lockstep with `count` and is what makes the word zero.
            out.extend_null_bmp(&0u64.to_le_bytes());
            out.count += 1;
        }

        // `out` is `Raw` from `with_capacity`; the `extend_*` loop above never raises
        // it, and the index-table ingest re-sorts/folds it.
        out
    }
}
