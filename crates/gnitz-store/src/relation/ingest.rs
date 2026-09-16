//! The ingestion pipeline: unique-PK enforcement, store + secondary-index
//! application, index projection, and the flush / checkpoint table collection.

use super::{Relation, RelationKind, RelationRegistry};
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, StorageError, StoreError, Table};

/// `GNITZ_INJECT_INGEST_APPLY_ERROR=store|index`: report `Err(Io)` from the
/// matching ingest below, which has already run — so what fires is the error
/// *handling*, not a rolled-back write.
///
/// On the push-apply/replay path only (VM integration bypasses it), so a
/// seam-armed server still boots on a pushless data dir.
static INGEST_APPLY_ERROR: gnitz_foundation::fault::Seam =
    gnitz_foundation::fault::Seam::new("GNITZ_INJECT_INGEST_APPLY_ERROR");

/// Inert for [`RelationKind::SystemCatalog`]: a system write happens at boot, and
/// the seam's contract is that an armed process still boots and then fails a
/// *push*.
fn inject_ingest_apply_error(
    which: &str,
    kind: RelationKind,
    r: Result<(), crate::storage::StorageError>,
) -> Result<(), crate::storage::StorageError> {
    if kind != RelationKind::SystemCatalog && INGEST_APPLY_ERROR.at(which) {
        return Err(crate::storage::StorageError::Io(libc::EIO));
    }
    r
}

impl RelationRegistry {
    // ── Ingestion ───────────────────────────────────────────────────────

    /// Enforce this relation's PK rule, apply the result to its store and index
    /// projections, and hand back the effective batch — what applying a batch to
    /// a relation means.
    fn apply(entry: &mut Relation, batch: Batch, needed: bool) -> Result<Option<Batch>, StorageError> {
        let effective = if entry.kind.is_base_table() {
            entry.store.enforce_unique_pk(batch)
        } else {
            batch
        };
        if effective.count == 0 {
            return Ok(needed.then_some(effective));
        }
        Self::ingest_store_and_indices(entry, effective, needed)
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
        let capture: Option<(SchemaDescriptor, u64)> = entry
            .delta
            .as_deref()
            .map(|feed| feed.schema())
            .zip(round)
            .filter(|_| !entry.store.schema().placement().is_replicated() || rank == 0);
        // Only a captured batch pays the up-front fold; every other view hands the
        // batch straight to the store ingest it always did.
        let folded = capture.and_then(|_| Batch::consolidate_if_needed(&batch, &entry.store.schema()));
        let src = folded.as_ref().unwrap_or(&batch);
        // Stamped before the store ingest moves the batch out from under it.
        let pending = capture.map(|(ds, r)| (src.stamped_with_pk_prefix(&ds, r), r));

        debug_assert!(
            entry.indexes.is_empty(),
            "a view owns no index circuits, so only its own store is written here",
        );
        // The fold, when one ran, is the copy the feed already paid for — so the
        // store takes it by value either way and the caller keeps the original.
        let (res, echo) = match folded {
            Some(f) => (entry.store.ingest_owned_batch(f), needed.then_some(batch)),
            None if needed => (entry.store.ingest_borrowed_batch(&batch), Some(batch)),
            None => (entry.store.ingest_owned_batch(batch), None),
        };
        inject_ingest_apply_error("store", entry.kind, res).inspect_err(|e| {
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
        // Not through the barrier: a view's store is rederived, so a base round
        // publishes nothing and folds to RAM anyway.
        if entry.is_bounded() {
            entry.store.flush_to_ram()?;
        }

        let Some((stamped, round)) = pending else {
            return Ok(echo);
        };
        let feed = entry.delta.as_deref_mut().expect("capture implies a feed");
        if let Err(e) = feed.ingest_owned_batch(stamped) {
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

    /// Apply `batch` to `id`'s store and its index projections, moving it in.
    /// Kind-uniform: a base table's PK rule runs, everything else is written as
    /// it stands.
    ///
    /// `Rejected` means nothing was applied and the request is at fault;
    /// `Storage` means committed data did not reach the store.
    pub fn ingest(&mut self, id: i64, batch: Batch) -> Result<(), StoreError> {
        self.ingest_batch(id, batch, false).map(drop)
    }

    /// [`Self::ingest`], handing back the batch as the store saw it, after PK
    /// enforcement — the **push** verb. `id` must be an ingestion point: a push
    /// naming a view or a system family would silently skip `enforce_unique_pk`
    /// and write a relation no client may write.
    pub fn ingest_returning_effective(&mut self, id: i64, batch: Batch) -> Result<Batch, StoreError> {
        match self.relation(id).map(Relation::kind) {
            Some(k) if k.is_ingestion_point() => {}
            Some(k) => {
                return Err(StoreError::rejected(format!(
                    "push into relation {id}: a {} is not an ingestion point",
                    k.noun()
                )))
            }
            None => {
                return Err(StoreError::rejected(format!(
                    "ingest failed: relation {id} is not registered"
                )))
            }
        }
        self.ingest_batch(id, batch, true)
            .map(|b| b.expect("`needed` is set, so the effective batch comes back"))
    }

    fn ingest_batch(&mut self, id: i64, batch: Batch, needed: bool) -> Result<Option<Batch>, StoreError> {
        let entry = match self.tables.get_mut(&id) {
            Some(e) => e,
            None => {
                return Err(StoreError::rejected(format!(
                    "ingest failed: relation {id} is not registered"
                )))
            }
        };
        // A pushed batch can carry a schema the registry has not caught up to, and
        // nothing else compares the two. Checked, not asserted: the append path
        // reads the destination's region count, so an unchecked mismatch drops the
        // extra column and ACKs the push as success.
        let want = entry.store.schema().num_payload_cols();
        if batch.num_payload_cols() != want {
            return Err(StoreError::rejected(format!(
                "push for table_id={id} carries {} payload columns, table schema has {}",
                batch.num_payload_cols(),
                want
            )));
        }

        Self::apply(entry, batch, needed).map_err(|e| StoreError::storage(format!("ingest into relation {id}"), e))
    }

    /// Ingest `source` into this relation's store, then project and ingest one
    /// index circuit at a time, so no index batch outlives its own ingest.
    ///
    /// A storage error means committed data was not applied while the client
    /// already holds a durability ACK. It must not be swallowed: a caller with a
    /// watchdog aborts and lets restart + SAL replay re-apply the batch, and one
    /// without poisons its handle.
    fn ingest_store_and_indices(
        entry: &mut Relation,
        source: Batch,
        needed: bool,
    ) -> Result<Option<Batch>, StorageError> {
        let (id, kind) = (entry.id(), entry.kind);
        for ix in entry.indexes.iter_mut() {
            let idx_batch = crate::storage::batch_project_index(&source, &ix.key_spec, &ix.store.schema());
            if idx_batch.count > 0 {
                let index_id = ix.index_id;
                inject_ingest_apply_error("index", kind, ix.ingest_owned_batch(idx_batch)).inspect_err(|e| {
                    gnitz_error!(
                        "relation: secondary-index ingest failed (table_id={}, index_id={}): {} \
                         — index diverged from base table",
                        id,
                        index_id,
                        e,
                    );
                })?;
            }
        }
        // Last, so a caller that does not want the batch back lets the store move
        // it instead of copying it.
        let (res, effective) = if needed {
            (entry.store.ingest_borrowed_batch(&source), Some(source))
        } else {
            (entry.store.ingest_owned_batch(source), None)
        };
        inject_ingest_apply_error("store", kind, res).inspect_err(|e| {
            gnitz_error!(
                "relation: store ingest failed (table_id={}): {} — committed data \
                 not applied, state diverged from durable SAL",
                id,
                e,
            );
        })?;
        Ok(effective)
    }

    // ── Flush / checkpoint collection ───────────────────────────────────

    /// Flush a relation's store and every index circuit on it. Unregistered is a
    /// caller bug; `Err` is storage.
    pub fn flush(&mut self, id: i64) -> Result<(), StorageError> {
        let Some(entry) = self.tables.get_mut(&id) else {
            debug_assert!(false, "flush of unregistered relation {id}");
            return Ok(());
        };
        entry.store.flush()?;
        for ix in &mut entry.indexes {
            ix.store.flush()?;
        }
        Ok(())
    }

    /// Every **user** store this process owns and checkpoints: each relation's own
    /// `Table` plus its index-circuit tables. The system families are excluded, so
    /// a forked worker cannot flush its inherited copy.
    ///
    /// A fed view's delta store is deliberately absent, which is what keeps it out
    /// of both checkpoint rounds. Both rounds start from this one set.
    fn collect_base_flush_tables(&mut self) -> Vec<&mut Table> {
        let mut out: Vec<&mut Table> = Vec::new();
        for entry in self.tables.values_mut() {
            if entry.kind == RelationKind::SystemCatalog {
                continue;
            }
            if let Some(t) = entry.store.table_mut() {
                out.push(t);
            }
            out.extend(entry.indexes.iter_mut().filter_map(|ix| ix.store.table_mut()));
        }
        out
    }

    /// The system families' stores, all borrowed at once — what one flush barrier
    /// needs and a one-at-a-time accessor cannot hand out. The complement of
    /// [`Self::collect_base_flush_tables`], on the same kind test.
    fn collect_system_flush_tables(&mut self) -> Vec<&mut Table> {
        self.tables
            .values_mut()
            .filter(|e| e.kind == RelationKind::SystemCatalog)
            .filter_map(|e| e.store.table_mut())
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
    fn collect_ephemeral_output_tables(&mut self) -> Vec<&mut Table> {
        self.collect_base_flush_tables()
            .into_iter()
            .filter(|t| t.is_rederived())
            .collect()
    }

    // ── The checkpoint rounds ───────────────────────────────────────────

    /// The base round: every user store this process owns and every secondary
    /// index beneath it, in **one** barrier — a per-table loop would build an
    /// io_uring and force a journal commit per table.
    pub fn checkpoint_base(&mut self) -> Result<(), StoreError> {
        crate::storage::flush_barrier(self.collect_base_flush_tables(), crate::storage::FlushRound::Base)
            .map_err(|e| StoreError::storage("base flush", e))
    }

    /// The system round: every system family's store, in one barrier.
    pub fn checkpoint_system(&mut self) -> Result<(), StoreError> {
        crate::storage::flush_barrier(self.collect_system_flush_tables(), crate::storage::FlushRound::Base)
            .map_err(|e| StoreError::storage("system catalog flush", e))
    }

    /// The ephemeral round at `generation`: every compiled circuit's operator
    /// state first, then this registry's rederived output stores, in two
    /// barriers. That order is why both halves are one call — an output manifest
    /// at `generation` must imply that view's traces already are, which is what
    /// the next boot's resume verdict reads. A host that maintains no circuit
    /// passes an empty iterator.
    pub fn checkpoint_ephemeral<'s>(
        &mut self,
        generation: u64,
        state: impl IntoIterator<Item = &'s mut crate::relation::CircuitState>,
    ) -> Result<(), StoreError> {
        let round = crate::storage::FlushRound::Ephemeral(generation);
        let traces: Vec<&mut Table> = state.into_iter().flat_map(|s| s.tables_mut()).collect();
        crate::storage::flush_barrier(traces, round).map_err(|e| StoreError::storage("ephemeral trace flush", e))?;
        crate::storage::flush_barrier(self.collect_ephemeral_output_tables(), round)
            .map_err(|e| StoreError::storage("ephemeral output flush", e))
    }

    /// Unlink the manifest of every store [`Self::checkpoint_ephemeral`]
    /// publishes, so the next open peeks `None` and erases those shards instead
    /// of resuming them. Its inverse, over the same two collections.
    pub fn unlink_ephemeral_manifests<'s>(
        &mut self,
        state: impl IntoIterator<Item = &'s mut crate::relation::CircuitState>,
    ) {
        for s in state {
            for t in s.tables_mut() {
                t.unlink_manifest();
            }
        }
        for t in self.collect_ephemeral_output_tables() {
            t.unlink_manifest();
        }
    }
}
