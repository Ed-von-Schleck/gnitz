//! The ingestion pipeline: unique-PK enforcement, store + secondary-index
//! application, index projection, and the flush / checkpoint table collection.

use super::{RelationKind, RelationRegistry, TableEntry};
use crate::storage::{Batch, StorageError, StoreError, Table};

/// `GNITZ_INJECT_INGEST_APPLY_ERROR=store|index`: report `Err(Io)` from the
/// matching ingest below, which has already run — so what fires is the error
/// *handling*, not a rolled-back write.
///
/// On the push-apply/replay path only (VM integration bypasses it), so a
/// seam-armed server still boots on a pushless data dir.
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

    /// Enforce this relation's PK rule, apply the result to its store and index
    /// projections, and hand back the effective batch — what applying a batch to
    /// a relation means.
    fn apply(table_id: i64, entry: &mut TableEntry, batch: Batch, needed: bool) -> Result<Option<Batch>, StorageError> {
        let effective = if entry.kind.is_base_table() {
            entry.handle.enforce_unique_pk(&entry.schema, batch)
        } else {
            batch
        };
        if effective.count == 0 {
            return Ok(needed.then_some(effective));
        }
        Self::ingest_store_and_indices(table_id, entry, effective, needed)
    }

    /// Ingest a view's epoch output into its own store and — when the view carries
    /// a feed — a copy stamped with `round` into its delta store. `round` is
    /// `None` for a backfill, whose rows a bootstrap read already carries.
    ///
    /// Captured here because this is the last moment the delta exists as a batch:
    /// the store's fold drops net-zero (PK, payload) rows, so an insert and its
    /// later retraction annihilate and no reader could recover either.
    ///
    /// **A replicated view stamps on worker 0 alone**: every worker computes the
    /// whole result and a delta read of one is routed to worker 0, so the other
    /// W−1 delta stores would be written every tick and read never.
    ///
    /// **A captured batch is consolidated once, here.** The stamp inherits its
    /// source's layout claim, so otherwise the delta store would re-sort what the
    /// output store just folded, and would carry the raw row count.
    ///
    /// The batch is **moved** into the output store unless `needed` — which is why
    /// the stamp is taken first, and why it is handed back rather than cloned: the
    /// store consolidates a borrowed `Raw` batch instead of copying it, so a
    /// caller-side clone would be pure addition.
    pub fn ingest_view_delta(
        &mut self,
        view_id: i64,
        batch: Batch,
        round: Option<u64>,
        needed: bool,
    ) -> Result<Option<Batch>, StorageError> {
        let rank = self.slot.rank;
        let Some(entry) = self.tables.get_mut(&view_id) else {
            gnitz_warn!("relation: ingest_view_delta — relation {} is not registered", view_id);
            return Ok(None);
        };
        debug_assert!(
            entry.kind.is_view(),
            "ingest_view_delta drives the tick path, which only ever reaches a view; \
             a base table would silently skip enforce_unique_pk here",
        );
        if batch.count == 0 {
            return Ok(needed.then_some(batch));
        }
        // The round this batch is captured under, or `None` for no capture — the
        // one decision both the fold and the stamp read. `round` leads, so a
        // backfill and every unfed view read no placement and no worker rank.
        let capture: Option<u64> =
            round.filter(|_| entry.delta.is_some() && (!entry.schema.placement().is_replicated() || rank == 0));
        // Only a captured batch pays the up-front fold; every other view hands the
        // batch straight to the store ingest it always did.
        let folded = capture.and_then(|_| Batch::consolidate_if_needed(&batch, &entry.schema));
        let src = folded.as_ref().unwrap_or(&batch);
        // Stamped before the store ingest moves the batch out from under it.
        let pending = entry
            .delta
            .as_ref()
            .zip(capture)
            .map(|(feed, r)| (feed, src.stamped_with_pk_prefix(&entry.schema, &feed.schema, r), r));

        debug_assert!(
            entry.index_circuits.is_empty(),
            "a view owns no index circuits, so only its own store is written here",
        );
        // The fold, when one ran, is the copy the feed already paid for — so the
        // store takes it by value either way and the caller keeps the original.
        let (res, echo) = match (folded, needed) {
            (Some(f), false) => (entry.handle.ingest_owned_batch(f), None),
            (Some(f), true) => (entry.handle.ingest_owned_batch(f), Some(batch)),
            (None, false) => (entry.handle.ingest_owned_batch(batch), None),
            (None, true) => (entry.handle.ingest_borrowed_batch(&batch), Some(batch)),
        };
        inject_ingest_apply_error("store", res).inspect_err(|e| {
            gnitz_error!(
                "relation: view output store ingest failed (view_id={}): {} — committed data \
                 not applied, state diverged from durable SAL",
                view_id,
                e,
            );
        })?;

        // A **bounded** view drains per epoch. Its capacity sweep runs off a
        // spill and budgets itself to one push-down per spill, so the spill
        // cadence is the sweep's granularity: draining only on the memtable's own
        // budget leaves so few, so large shards that the first sweep dehydrates
        // the whole view. No other view needs it — nothing else reads a spill
        // cadence, and a delta feed's budget hangs off the spill it takes anyway.
        if entry.budgets.capacity_bytes.is_some() {
            entry.handle.flush()?;
        }

        let Some((feed, stamped, round)) = pending else {
            return Ok(echo);
        };
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
                round,
                e,
            );
        }
        Ok(echo)
    }

    /// Ingest a user-relation batch into its store + index projections and
    /// return the effective batch (after PK enforcement) — what downstream views
    /// need to see. System families are not supported (the catalog's `submit`
    /// routes those through the precheck/hooks path).
    ///
    /// `StoreError::Rejected` means nothing was applied and the request is at
    /// fault; `StoreError::Storage` means committed data did not reach the
    /// store.
    pub fn ingest_returning_effective(
        &mut self,
        table_id: i64,
        batch: Batch,
        needed: bool,
    ) -> Result<Option<Batch>, StoreError> {
        let entry = match self.tables.get_mut(&table_id) {
            Some(e) => e,
            None => {
                return Err(StoreError::rejected(format!(
                    "ingest failed: relation {table_id} is not registered"
                )))
            }
        };
        // The registered kind, not an id band: `TableEntry.kind` is the fact this
        // crate owns, and every production caller is already gated on the band.
        if entry.kind == RelationKind::SystemCatalog {
            return Err(StoreError::rejected(
                "ingest_returning_effective not supported for system tables",
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
        let want = entry.schema.num_payload_cols();
        if batch.num_payload_cols() != want {
            return Err(StoreError::rejected(format!(
                "push for table_id={table_id} carries {} payload columns, table schema has {}",
                batch.num_payload_cols(),
                want
            )));
        }

        Self::apply(table_id, entry, batch, needed)
            .map_err(|e| StoreError::storage(format!("ingest into relation {table_id}"), e))
    }

    /// Ingest `source` into this relation's store, then project and ingest one
    /// index circuit at a time, so no index batch outlives its own ingest.
    ///
    /// A storage error here means committed (or SAL-replayed) data was not
    /// applied while the client already holds a durability ACK, so process state
    /// has diverged from the durable SAL. Silent swallowing is the one unsound
    /// response — it neither applies nor replays the entry, and the next
    /// checkpoint orphans it. The error is returned instead: a caller that owns
    /// a watchdog aborts on it and lets restart + SAL replay re-apply the batch
    /// (its WAL zone stays above the flushed-shard watermark, so it *will* be
    /// replayed); one that does not, poisons its handle.
    fn ingest_store_and_indices(
        table_id: i64,
        entry: &TableEntry,
        source: Batch,
        needed: bool,
    ) -> Result<Option<Batch>, StorageError> {
        for ic in entry.index_circuits.iter() {
            let idx_batch = crate::storage::batch_project_index(&source, &ic.key_spec, &ic.index_schema);
            if idx_batch.count > 0 {
                let index_id = ic.index_id;
                inject_ingest_apply_error("index", ic.ingest_owned_batch(idx_batch)).inspect_err(|e| {
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
        // Last, so a caller that does not want the batch back lets the store move
        // it instead of copying it.
        let (res, effective) = match needed {
            false => (entry.handle.ingest_owned_batch(source), None),
            true => (entry.handle.ingest_borrowed_batch(&source), Some(source)),
        };
        inject_ingest_apply_error("store", res).inspect_err(|e| {
            gnitz_error!(
                "relation: store ingest failed (table_id={}): {} — committed data \
                 not applied, state diverged from durable SAL",
                table_id,
                e,
            );
        })?;
        Ok(effective)
    }

    // ── Flush / checkpoint collection ───────────────────────────────────

    /// Flush a relation's store and every index circuit on it. Its one production
    /// caller ends a backfill, so it always names a view, which owns no circuit —
    /// only tests reach the loop. Unregistered is a caller bug; `Err` is storage.
    pub fn flush(&mut self, table_id: i64) -> Result<(), StorageError> {
        let Some(entry) = self.tables.get_mut(&table_id) else {
            debug_assert!(false, "flush of unregistered table_id {table_id}");
            return Ok(());
        };
        entry.handle.flush()?;
        for ic in &mut entry.index_circuits {
            ic.handle.flush()?;
        }
        Ok(())
    }

    /// Every **user** store this process owns and checkpoints: each relation's
    /// own `Table` plus its index-circuit tables. Excluding the system families
    /// by kind is what stops a forked worker flushing its inherited `_sys` copy.
    ///
    /// A fed view's delta store is deliberately **not** here, which is what puts
    /// it in neither checkpoint round: it publishes no manifest and is erased at
    /// open, so nothing could resume it. That is also what makes its shard
    /// unlinking immediate rather than deferred to the drain this set feeds.
    ///
    /// Both checkpoint rounds start from this one set and let `Table` decide:
    /// the base round is handed it whole (`flush_prepare` publishes the durable
    /// stores and folds the rederived ones to RAM), the ephemeral round takes
    /// the rederived half. Neither re-derives "which stores does this round
    /// touch" from the relation kind, so the two cannot drift apart.
    ///
    /// A `Vec` of `&mut Table` rather than a keyed lookup: an owned trace table is
    /// not in `self.tables` at all.
    pub fn collect_base_flush_tables(&mut self) -> Vec<&mut Table> {
        let mut out: Vec<&mut Table> = Vec::new();
        for entry in self.tables.values_mut() {
            if entry.kind == RelationKind::SystemCatalog {
                continue;
            }
            if let Some(t) = entry.handle.owned_mut() {
                out.push(t);
            }
            out.extend(entry.index_circuits.iter_mut().filter_map(|ic| ic.handle.owned_mut()));
        }
        out
    }

    /// The system families' stores, all borrowed at once — what one flush barrier
    /// needs and a one-at-a-time accessor cannot hand out. The complement of
    /// [`Self::collect_base_flush_tables`], on the same kind test.
    pub fn collect_system_flush_tables(&mut self) -> Vec<&mut Table> {
        self.tables
            .values_mut()
            .filter(|e| e.kind == RelationKind::SystemCatalog)
            .filter_map(TableEntry::owned_store_mut)
            .collect()
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
    pub fn flush_ephemeral_outputs(&mut self, generation: u64) -> Result<(), StoreError> {
        let round = crate::storage::FlushRound::Ephemeral(generation);
        crate::storage::flush_barrier(self.collect_ephemeral_output_tables(), round)
            .map_err(|e| StoreError::storage("ephemeral output flush", e))
    }
}
