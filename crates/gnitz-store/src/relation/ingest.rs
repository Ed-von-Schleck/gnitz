//! The ingestion pipeline: unique-PK enforcement, store + secondary-index
//! application, delta capture, and the flush / checkpoint table collection.

use super::{Relation, RelationKind, RelationRegistry, SecondaryIndex, Store};
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, StorageError, StoreError, Table};

/// `GNITZ_INJECT_INGEST_APPLY_ERROR=store|index`: report `Err(Io)` from the
/// matching ingest below, which has already run — so what fires is the error
/// *handling*, not a rolled-back write.
static INGEST_APPLY_ERROR: gnitz_foundation::fault::Seam =
    gnitz_foundation::fault::Seam::new("GNITZ_INJECT_INGEST_APPLY_ERROR");

/// Inert for [`RelationKind::SystemCatalog`], whose writes happen at boot: an
/// armed process must reach the push it is meant to fail.
fn inject_ingest_apply_error(which: &str, kind: RelationKind, r: Result<(), StorageError>) -> Result<(), StorageError> {
    if kind != RelationKind::SystemCatalog && INGEST_APPLY_ERROR.at(which) {
        return Err(StorageError::Io(libc::EIO));
    }
    r
}

/// Fill each of `targets`, all empty, from one chunked scan of `owner`, `owner_id`'s store.
pub(super) fn fill_indexes(
    owner: &Store,
    chunk_rows: usize,
    owner_id: i64,
    targets: &mut [&mut SecondaryIndex],
) -> Result<(), StoreError> {
    if targets.is_empty() {
        return Ok(());
    }
    for ix in targets.iter() {
        assert_eq!(
            ix.store.estimated_rows(),
            0,
            "index {} of relation {owner_id} is already populated",
            ix.index_id
        );
    }
    let mut source = owner.cursor();
    while let Some(chunk) = source.drain_chunk(chunk_rows) {
        for ix in targets.iter_mut() {
            let index_id = ix.index_id;
            ix.project_and_ingest(&chunk)
                .map_err(|e| StoreError::storage(format!("fill index {index_id} of relation {owner_id}"), e))?;
        }
    }
    Ok(())
}

impl RelationRegistry {
    // ── Ingestion ───────────────────────────────────────────────────────

    /// Apply `batch` to `id`'s store and its index projections, moving it in.
    /// Kind-uniform: a base table's PK rule runs, everything else is written as
    /// it stands.
    ///
    /// `Rejected` means nothing was applied and the request is at fault;
    /// `Storage` means committed data did not reach the store.
    pub fn ingest(&mut self, id: i64, batch: Batch) -> Result<(), StoreError> {
        self.ingest_at(id, batch, None, false).map(drop)
    }

    /// [`Self::ingest`], handing back the batch as the store saw it, after PK
    /// enforcement — what a caller that must forward the applied rows takes.
    pub fn ingest_returning(&mut self, id: i64, batch: Batch) -> Result<Batch, StoreError> {
        self.ingest_at(id, batch, None, true)
            .map(|b| b.expect("`needed` is set, so the effective batch comes back"))
    }

    /// Ingest a view's epoch output into its own store, and — where this process
    /// serves the view's feed — a copy stamped with `round` into its delta store.
    /// `round` is `None` for a backfill, which captures nothing; the batch comes
    /// back only when `needed`.
    ///
    /// The capture belongs here rather than downstream: the store's fold drops
    /// net-zero rows, and a round has to keep both sides of one.
    pub fn ingest_view_delta(
        &mut self,
        view_id: i64,
        batch: Batch,
        round: Option<u64>,
        needed: bool,
    ) -> Result<Option<Batch>, StoreError> {
        self.ingest_at(view_id, batch, round, needed)
    }

    /// Resolve `id`, admit the batch's shape against the store's, and apply it.
    fn ingest_at(
        &mut self,
        id: i64,
        batch: Batch,
        round: Option<u64>,
        needed: bool,
    ) -> Result<Option<Batch>, StoreError> {
        let entry = self.relation_mut_or_err(id)?;
        // A pushed batch can carry a schema the registry has not caught up to.
        // Checked, not asserted: the append path sizes by the destination's region
        // count, so a mismatch would drop the extra column and ACK the push.
        let want = entry.store.schema().num_payload_cols();
        if batch.num_payload_cols() != want {
            return Err(StoreError::rejected(format!(
                "push for table_id={id} carries {} payload columns, table schema has {}",
                batch.num_payload_cols(),
                want
            )));
        }
        Self::ingest_into(entry, batch, round, needed)
    }

    /// Apply `batch` to one resolved relation: its PK rule, its index
    /// projections, its own store and its delta capture.
    ///
    /// `#[inline]`: it returns a 1 KiB `Batch` by value, so a call would cost an
    /// extra sret move at every site.
    #[inline]
    fn ingest_into(
        entry: &mut Relation,
        batch: Batch,
        round: Option<u64>,
        needed: bool,
    ) -> Result<Option<Batch>, StoreError> {
        let (id, kind) = (entry.id(), entry.kind);
        let effective = match kind.is_base_table() {
            true => entry.store.enforce_unique_pk(batch),
            false => batch,
        };
        if effective.count == 0 {
            return Ok(needed.then_some(effective));
        }

        let capture: Option<(SchemaDescriptor, u64)> = entry.delta.as_deref().map(|feed| feed.schema()).zip(round);
        // Folded once for both: a round keeps one net weight per element, and the
        // store then takes that fold by value instead of folding its own copy.
        let folded = capture.and_then(|_| Batch::consolidate_if_needed(&effective, &entry.store.schema()));
        let pending = capture.map(|(delta_schema, r)| {
            let src = folded.as_ref().unwrap_or(&effective);
            (src.stamped_with_pk_prefix(&delta_schema, r), r)
        });

        for ix in entry.indexes.iter_mut() {
            let index_id = ix.index_id;
            let res = match ix.project_and_ingest(&effective) {
                // The seam reports from a write that ran; an empty projection is none.
                Ok(false) => continue,
                other => other.map(drop),
            };
            inject_ingest_apply_error("index", kind, res)
                .map_err(|e| StoreError::storage(format!("ingest into index {index_id} of relation {id}"), e))?;
        }

        let (res, echo) = match folded {
            Some(f) => (entry.store.ingest_owned_batch(f), needed.then_some(effective)),
            None if needed => (entry.store.ingest_borrowed_batch(&effective), Some(effective)),
            None => (entry.store.ingest_owned_batch(effective), None),
        };
        inject_ingest_apply_error("store", kind, res)
            .map_err(|e| StoreError::storage(format!("ingest into relation {id}"), e))?;

        let Some((stamped, round)) = pending else {
            return Ok(echo);
        };
        let feed = entry.delta.as_deref_mut().expect("capture implies a feed");
        if let Err(e) = feed.ingest_owned_batch(stamped) {
            // Logged, not fatal: the round is captured and the spill retries next
            // tick, where a restart would erase every retained round instead.
            gnitz_error!(
                "relation: delta-store spill failed (view_id={}, round={}): {} — the round is \
                 held in RAM and retried on the next tick; the feed is intact",
                id,
                round,
                e,
            );
        }
        Ok(echo)
    }

    // ── Flush / checkpoint collection ───────────────────────────────────

    /// Fold `id`'s store memtable into its RAM tier: no manifest publish, no
    /// barrier, and nothing of the relation's indexes. Unregistered is an `Err`.
    pub fn fold_to_ram(&mut self, id: i64) -> Result<(), StoreError> {
        let entry = self.relation_mut_or_err(id)?;
        entry
            .store
            .flush_to_ram()
            .map_err(|e| StoreError::storage(format!("fold relation {id} to RAM"), e))
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

    /// The system families' stores, all borrowed at once. The complement of
    /// [`Self::collect_base_flush_tables`], on the same kind test.
    fn collect_system_flush_tables(&mut self) -> Vec<&mut Table> {
        self.tables
            .values_mut()
            .filter(|e| e.kind == RelationKind::SystemCatalog)
            .filter_map(|e| e.store.table_mut())
            .collect()
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

    /// The ephemeral round at the resume generation: `state`'s operator traces,
    /// then this registry's rederived output stores, in two barriers — so an
    /// output manifest implies its view's traces are already durable.
    pub fn checkpoint_ephemeral<'s>(
        &mut self,
        state: impl IntoIterator<Item = &'s mut crate::relation::CircuitState>,
    ) -> Result<(), StoreError> {
        let round = crate::storage::FlushRound::Ephemeral(self.resume_generation);
        let traces: Vec<&mut Table> = state.into_iter().flat_map(|s| s.tables_mut()).collect();
        crate::storage::flush_barrier(traces, round).map_err(|e| StoreError::storage("ephemeral trace flush", e))?;
        crate::storage::flush_barrier(self.collect_ephemeral_output_tables(), round)
            .map_err(|e| StoreError::storage("ephemeral output flush", e))
    }
}
