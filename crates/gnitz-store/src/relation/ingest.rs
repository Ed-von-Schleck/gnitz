//! The ingestion pipeline: unique-PK enforcement, store + secondary-index
//! application, index projection, and the flush / checkpoint table collection.

use super::{RelationRegistry, TableEntry, FIRST_USER_TABLE_ID};
use crate::storage::{Batch, StorageError, Table};

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

/// `GNITZ_INJECT_INGEST_APPLY_ERROR=store|index`: substitute `Err(Io)` for the
/// matching ingest below, so the fail-stop abort fires without a real disk
/// fault. No one-shot latch — the process aborts on first fire. Placing the seam
/// on the ingest path keeps `storage` seam-free and fires exactly on the
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

impl RelationRegistry {
    // ── Ingestion ───────────────────────────────────────────────────────

    /// Ingest a borrowed batch (no clone) for relations that run no PK
    /// enforcement. A base table falls back to cloning + [`Self::apply`], since
    /// enforcement rewrites the batch.
    ///
    /// An unregistered id is a warning and a no-op, not an error: the caller
    /// routes by an id its own catalog resolved, so a miss here means the two
    /// have diverged and the ingest has nowhere to land either way.
    pub fn ingest_by_ref(&mut self, table_id: i64, batch: &Batch) -> Result<(), StorageError> {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => {
                gnitz_warn!("relation: ingest_by_ref — table_id={} not registered", table_id);
                return Ok(());
            }
        };
        if entry.kind.is_base_table() {
            Self::apply(table_id, entry, batch.clone_batch()).map(drop)
        } else if batch.count > 0 {
            Self::ingest_store_and_indices(table_id, entry, batch)
        } else {
            Ok(())
        }
    }

    /// Enforce this relation's PK rule, apply the result to its store and index
    /// projections, and hand back the effective batch. The one body under both
    /// public ingest entries, so neither can drift on what applying a batch to a
    /// relation means.
    fn apply(table_id: i64, entry: &mut TableEntry, batch: Batch) -> Result<Batch, StorageError> {
        let effective = if entry.kind.is_base_table() {
            entry.handle.enforce_unique_pk(&entry.schema, batch)
        } else {
            batch
        };
        if effective.count > 0 {
            Self::ingest_store_and_indices(table_id, entry, &effective)?;
        }
        Ok(effective)
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
    pub fn ingest_view_delta(&mut self, view_id: i64, batch: &Batch, tick_round: u64) -> Result<(), StorageError> {
        let Some(entry) = self.tables.get_mut(&view_id) else {
            gnitz_warn!("relation: ingest_view_delta — view_id={} not registered", view_id);
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
                "relation: delta-store spill failed (view_id={}, round={}): {} — the round is \
                 held in RAM and retried on the next tick; the feed is intact",
                view_id,
                tick_round,
                e,
            );
        }
        Ok(())
    }

    /// Ingest a user-relation batch into its store + index projections and
    /// return the effective batch (after PK enforcement) — what downstream views
    /// need to see. System families are not supported (the catalog's
    /// `ingest_to_family` routes those through the precheck/hooks path).
    ///
    /// `IngestError::Rejected` means nothing was applied and the request is at
    /// fault; `IngestError::Storage` means committed data did not reach the
    /// store.
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: Batch) -> Result<Batch, IngestError> {
        if table_id < FIRST_USER_TABLE_ID {
            return Err(IngestError::Rejected(
                "ingest_returning_effective not supported for system tables".to_string(),
            ));
        }
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => {
                return Err(IngestError::Rejected(format!(
                    "ingest failed for table_id={table_id}: not registered"
                )))
            }
        };
        // Width guard, ahead of `enforce_unique_pk`. A worker parked mid-epoch
        // stashes an incoming `DdlSync` and replays it at the next top-level
        // drain while still serving pushes inline, so its `TableEntry.schema` can
        // lag a frame the master framed from its own widened catalog — and the
        // worker decodes with no hint, so nothing else compares the two. A real
        // check, not a `debug_assert`: the append path is driven by the
        // destination's region count, so a release build would silently drop the
        // extra column and ACK the push as success.
        let want = entry.schema.num_payload_cols();
        if batch.num_payload_cols() != want {
            return Err(IngestError::Rejected(format!(
                "push for table_id={table_id} carries {} payload columns, table schema has {}",
                batch.num_payload_cols(),
                want
            )));
        }

        Self::apply(table_id, entry, batch).map_err(IngestError::Storage)
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
            .map(|ic| crate::storage::batch_project_index(source, &ic.key_spec, &ic.index_schema))
            .collect();

        inject_ingest_apply_error("store", entry.handle.ingest_borrowed_batch(source)).inspect_err(|e| {
            gnitz_error!(
                "relation: base-table ingest failed (table_id={}): {} — committed data \
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
                        "relation: secondary-index ingest failed (table_id={}, index_id={}): {} \
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
    /// A `Vec` of `&mut Table` rather than a keyed lookup: an owned trace table is
    /// not in `self.tables` at all. Both sources (`StoreHandle::as_owned_mut`,
    /// `IndexCircuitEntry::table_mut`) hand a `&mut` out of an `UnsafeCell`, so
    /// they own the disjointness argument, not the borrow checker.
    pub fn collect_base_flush_tables(&mut self) -> Vec<&mut Table> {
        let mut out: Vec<&mut Table> = Vec::new();
        for entry in self.tables.values_mut() {
            if let Some(t) = entry.handle.as_owned_mut() {
                out.push(t);
            }
            for ic in &mut entry.index_circuits {
                out.push(ic.table_mut());
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
            .any(|t| t.base_round_advances_publish())
    }

    /// The rederived stores the ephemeral round force-persists — view output
    /// stores and secondary-index tables alike. A compiled view's operator-trace
    /// tables are the DBSP layer's half of the round, collected there; set 1
    /// (traces) goes fully durable before set 2 (these), so any output manifest
    /// at generation G implies that view's traces are durable at G.
    pub fn collect_ephemeral_output_tables(&mut self) -> Vec<&mut Table> {
        self.collect_base_flush_tables()
            .into_iter()
            .filter(|t| t.is_rederived())
            .collect()
    }

    /// Publish [`Self::collect_ephemeral_output_tables`] at `generation`, in one
    /// barrier. The second half of an ephemeral checkpoint round wherever one
    /// runs — behind a circuit layer that flushed its traces first, or in a
    /// mirror, which owns no traces to flush.
    pub fn flush_ephemeral_outputs(&mut self, generation: u64) -> Result<(), String> {
        let round = crate::storage::FlushRound::Ephemeral(generation);
        crate::storage::flush_barrier(self.collect_ephemeral_output_tables(), round)
            .map_err(|e| format!("ephemeral output flush: {e}"))
    }
}
