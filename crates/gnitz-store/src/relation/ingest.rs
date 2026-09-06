//! The ingestion pipeline: unique-PK enforcement, store + secondary-index
//! application, index projection, and the flush / checkpoint table collection.

use super::{RelationKind, RelationRegistry, TableEntry};
use crate::schema::SchemaDescriptor;
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
    /// `None` for a backfill, which captures nothing.
    ///
    /// The capture must happen here: the store's fold drops net-zero rows, so
    /// after it an insert and its later retraction are both unrecoverable. A
    /// replicated view captures on worker 0 alone, where its delta reads are
    /// served. Returns the batch only when `needed`; otherwise it is moved into
    /// the store.
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
        // The feed schema and round this batch is captured under, or `None` for no
        // capture — the one decision both the fold and the stamp read. `round` and
        // the feed lead, so a backfill and every unfed view read no placement and
        // no worker rank.
        let capture: Option<(&SchemaDescriptor, u64)> = entry
            .delta
            .as_deref()
            .map(|feed| &feed.schema)
            .zip(round)
            .filter(|_| !entry.schema.placement().is_replicated() || rank == 0);
        // Only a captured batch pays the up-front fold; every other view hands the
        // batch straight to the store ingest it always did.
        let folded = capture.and_then(|_| Batch::consolidate_if_needed(&batch, &entry.schema));
        let src = folded.as_ref().unwrap_or(&batch);
        // Stamped before the store ingest moves the batch out from under it.
        let pending = capture.map(|(ds, r)| (src.stamped_with_pk_prefix(&entry.schema, ds, r), r));

        debug_assert!(
            entry.index_circuits.is_empty(),
            "a view owns no index circuits, so only its own store is written here",
        );
        // The fold, when one ran, is the copy the feed already paid for — so the
        // store takes it by value either way and the caller keeps the original.
        let (res, echo) = match folded {
            Some(f) => (entry.handle.ingest_owned_batch(f), needed.then_some(batch)),
            None if needed => (entry.handle.ingest_borrowed_batch(&batch), Some(batch)),
            None => (entry.handle.ingest_owned_batch(batch), None),
        };
        inject_ingest_apply_error("store", res).inspect_err(|e| {
            gnitz_error!(
                "relation: view output store ingest failed (view_id={}): {} — committed data \
                 not applied, state diverged from durable SAL",
                view_id,
                e,
            );
        })?;

        // A bounded view drains per epoch: its capacity sweep runs off a spill, so
        // the spill cadence is the sweep's granularity. On the memtable's own
        // budget the shards are too few and too large for the sweep to be gradual.
        if entry.budgets.capacity_bytes.is_some() {
            entry.handle.flush()?;
        }

        let Some((stamped, round)) = pending else {
            return Ok(echo);
        };
        let feed = entry.delta.as_deref_mut().expect("capture implies a feed");
        if let Err(e) = feed.handle.ingest_owned_batch(stamped) {
            // Logged, not fatal: the round is already captured, and what failed is
            // a spill the next tick retries. Aborting would lose more than it
            // saves — a delta store is erased at open, so a restart drops every
            // retained round and forces every subscriber to bootstrap.
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

    /// Ingest a user-relation batch into its store and index projections, moving
    /// it into the store. System families are rejected.
    ///
    /// `Rejected` means nothing was applied and the request is at fault;
    /// `Storage` means committed data did not reach the store.
    pub fn ingest(&mut self, table_id: i64, batch: Batch) -> Result<(), StoreError> {
        self.ingest_batch(table_id, batch, false).map(drop)
    }

    /// [`Self::ingest`], handing back the batch as the store saw it, after PK
    /// enforcement. Costs the one copy [`Self::ingest`] avoids.
    pub fn ingest_returning_effective(&mut self, table_id: i64, batch: Batch) -> Result<Batch, StoreError> {
        self.ingest_batch(table_id, batch, true)
            .map(|b| b.expect("`needed` is set, so the effective batch comes back"))
    }

    fn ingest_batch(&mut self, table_id: i64, batch: Batch, needed: bool) -> Result<Option<Batch>, StoreError> {
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
        // A pushed batch can carry a schema the registry has not caught up to, and
        // nothing else compares the two. Checked, not asserted: the append path
        // reads the destination's region count, so an unchecked mismatch drops the
        // extra column and ACKs the push as success.
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
    /// A storage error means committed data was not applied while the client
    /// already holds a durability ACK. It must not be swallowed: a caller with a
    /// watchdog aborts and lets restart + SAL replay re-apply the batch, and one
    /// without poisons its handle.
    fn ingest_store_and_indices(
        table_id: i64,
        entry: &mut TableEntry,
        source: Batch,
        needed: bool,
    ) -> Result<Option<Batch>, StorageError> {
        for ic in entry.index_circuits.iter_mut() {
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

    /// Flush a relation's store and every index circuit on it. Unregistered is a
    /// caller bug; `Err` is storage.
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

    /// Every **user** store this process owns and checkpoints: each relation's own
    /// `Table` plus its index-circuit tables. The system families are excluded, so
    /// a forked worker cannot flush its inherited copy.
    ///
    /// A fed view's delta store is deliberately absent, which is what keeps it out
    /// of both checkpoint rounds. Both rounds start from this one set.
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

    /// The rederived stores the ephemeral round force-persists. A compiled view's
    /// operator-trace tables are the DBSP layer's half of the round and go durable
    /// first, so an output manifest at a generation implies its traces are too.
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
