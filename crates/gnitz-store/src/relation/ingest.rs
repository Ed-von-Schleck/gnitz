//! The ingestion pipeline: unique-PK enforcement, store + secondary-index
//! application, delta capture, and the flush / checkpoint table collection.

use super::{Relation, RelationKind, RelationRegistry, SecondaryIndex, Store};
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ManifestStamp, StorageError, StoreError, Table};

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
            ix.store.held().estimated_rows(),
            0,
            "index {} of relation {owner_id} is already populated",
            ix.index_id
        );
    }
    let mut source = owner.held().open_cursor();
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
        let want = entry.schema().num_payload_cols();
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
        // A stream's rows exist only as the deltas they produce.
        if kind == RelationKind::Stream {
            return Ok(needed.then_some(batch));
        }
        let effective = match kind.is_base_table() {
            true => super::unique_pk::enforce_unique_pk(entry.store.held(), batch),
            false => batch,
        };
        if effective.count == 0 {
            return Ok(needed.then_some(effective));
        }

        let capture: Option<(SchemaDescriptor, u64)> = entry.delta.as_deref().map(|t| *t.schema()).zip(round);
        // Folded once for both: a round keeps one net weight per element, and the
        // store then takes that fold by value instead of folding its own copy.
        let folded = capture.and_then(|_| Batch::consolidate_if_needed(&effective, entry.store.schema()));
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

        let store = entry.store.held_mut();
        let (res, echo) = match folded {
            Some(f) => (store.ingest_owned_batch(f), needed.then_some(effective)),
            None if needed => (store.ingest_borrowed_batch(&effective), Some(effective)),
            None => (store.ingest_owned_batch(effective), None),
        };
        inject_ingest_apply_error("store", kind, res)
            .map_err(|e| StoreError::storage(format!("ingest into relation {id}"), e))?;

        let Some((stamped, round)) = pending else {
            return Ok(echo);
        };
        let feed = entry.delta.as_deref_mut().expect("capture implies a feed");
        if let Err(e) = feed.ingest_owned_batch(stamped) {
            // Logged, not fatal: the round is captured and the next spill retries,
            // where a restart would erase every retained round instead.
            gnitz_error!(
                "relation: delta-store spill failed (view_id={}, round={}): {} — the spill or \
                 its upkeep failed; the next spill retries it; the feed is intact",
                id,
                round,
                e,
            );
        }
        Ok(echo)
    }

    // ── Flush / checkpoint collection ───────────────────────────────────

    /// Fold `id`'s store memtable into its RAM tier, which past its ceiling also
    /// spills it, compacts and runs the capacity sweep: no manifest publish, no
    /// barrier, and nothing of the relation's indexes. Unregistered is an `Err`.
    pub fn fold_to_ram(&mut self, id: i64) -> Result<(), StoreError> {
        let entry = self.relation_mut_or_err(id)?;
        entry
            .store
            .held_mut()
            .fold_to_ram()
            .map_err(|e| StoreError::storage(format!("fold relation {id} to RAM"), e))
    }

    /// Each user relation's own `Table` plus its index tables. System families
    /// are excluded, so a forked worker cannot flush its inherited copy.
    fn collect_user_tables(&mut self) -> impl Iterator<Item = &mut Table> {
        self.tables
            .values_mut()
            .filter(|e| e.kind != RelationKind::SystemCatalog)
            .flat_map(|e| {
                e.store
                    .table_mut()
                    .into_iter()
                    .chain(e.indexes.iter_mut().filter_map(|ix| ix.store.table_mut()))
            })
    }

    /// The system families' stores: the complement of [`Self::collect_user_tables`].
    fn collect_system_tables(&mut self) -> impl Iterator<Item = &mut Table> {
        self.tables
            .values_mut()
            .filter(|e| e.kind == RelationKind::SystemCatalog)
            .filter_map(|e| e.store.table_mut())
    }

    // ── The checkpoint rounds ───────────────────────────────────────────

    /// The base round: every user store that is not rederived, in one barrier.
    pub fn checkpoint_base(&mut self) -> Result<(), StoreError> {
        crate::storage::flush_barrier(
            self.collect_user_tables().filter(|t| !t.is_rederived()),
            Default::default(),
        )
        .map_err(|e| StoreError::storage("base flush", e))
    }

    /// The system round: every system family's store, in one barrier.
    pub fn checkpoint_system(&mut self, replay_floor: u64) -> Result<(), StoreError> {
        let stamp = ManifestStamp { replay_floor, ..Default::default() };
        crate::storage::flush_barrier(self.collect_system_tables(), stamp)
            .map_err(|e| StoreError::storage("system catalog flush", e))
    }

    /// The ephemeral round at the resume generation: `state`'s operator traces,
    /// then the rederived stores, so an output manifest implies durable traces.
    pub fn checkpoint_ephemeral<'s>(
        &mut self,
        state: impl IntoIterator<Item = &'s mut crate::relation::CircuitState>,
    ) -> Result<(), StoreError> {
        let stamp = ManifestStamp {
            checkpoint_gen: self.resume_generation,
            ..Default::default()
        };
        let traces = state.into_iter().flat_map(|s| s.tables_mut());
        crate::storage::flush_barrier(traces, stamp).map_err(|e| StoreError::storage("ephemeral trace flush", e))?;
        crate::storage::flush_barrier(self.collect_user_tables().filter(|t| t.is_rederived()), stamp)
            .map_err(|e| StoreError::storage("ephemeral output flush", e))
    }
}
