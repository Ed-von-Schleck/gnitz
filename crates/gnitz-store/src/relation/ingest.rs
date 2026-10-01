//! The ingestion pipeline: unique-PK enforcement, store + secondary-index
//! application, delta capture, and the flush / checkpoint table collection.

use super::delta::delta_round_prefix;
use super::{Relation, RelationKind, RelationRegistry, SecondaryIndex, Store};
use crate::storage::Table;
use gnitz_zset::repr::{Batch, StorageError};

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
    owner_id: u64,
    targets: &mut [&mut SecondaryIndex],
) -> Result<(), String> {
    if targets.is_empty() {
        return Ok(());
    }
    for ix in targets.iter() {
        assert_eq!(
            ix.store.held().estimated_rows(),
            0,
            "index on columns {:?} of relation {owner_id} is already populated",
            ix.cols.as_slice()
        );
    }
    let mut source = owner.held().open_cursor();
    while let Some(chunk) = source.drain_chunk(chunk_rows) {
        for ix in targets.iter_mut() {
            let cols = ix.cols;
            ix.project_and_ingest(&chunk).map_err(|e| {
                format!(
                    "fill index on columns {:?} of relation {owner_id}: {e}",
                    cols.as_slice()
                )
            })?;
        }
    }
    Ok(())
}

impl RelationRegistry {
    // ── Ingestion ───────────────────────────────────────────────────────

    /// Apply `batch` to `id`'s store and its index projections, moving it in.
    /// Kind-uniform: a base table's PK rule runs, everything else is written as
    /// it stands.
    pub fn ingest(&mut self, id: u64, batch: Batch) -> Result<(), String> {
        self.ingest_at(id, batch, None, false).map(drop)
    }

    /// [`Self::ingest`], handing back the batch as the store saw it, after PK
    /// enforcement — what a caller that must forward the applied rows takes.
    pub fn ingest_returning(&mut self, id: u64, batch: Batch) -> Result<Batch, String> {
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
        view_id: u64,
        batch: Batch,
        round: Option<u64>,
        needed: bool,
    ) -> Result<Option<Batch>, String> {
        self.ingest_at(view_id, batch, round, needed)
    }

    /// Resolve `id`, admit the batch's shape against the store's, and apply it.
    fn ingest_at(&mut self, id: u64, batch: Batch, round: Option<u64>, needed: bool) -> Result<Option<Batch>, String> {
        let entry = self.relation_mut_or_err(id)?;
        // Checked, not asserted: the append path sizes by the destination's region
        // count, so a mismatch would silently drop the extra column.
        let want = entry.schema().num_payload_cols();
        if batch.num_payload_cols() != want {
            return Err(format!(
                "batch for relation {id} carries {} payload columns, its schema has {}",
                batch.num_payload_cols(),
                want
            ));
        }
        Self::ingest_into(entry, batch, round, needed)
    }

    /// Apply `batch` to one resolved relation: its PK rule, its index
    /// projections, its own store and its delta capture.
    ///
    /// `#[inline]`: it returns a `Batch` by value, so a call would cost an
    /// extra sret move at every site.
    #[inline]
    fn ingest_into(
        entry: &mut Relation,
        batch: Batch,
        round: Option<u64>,
        needed: bool,
    ) -> Result<Option<Batch>, String> {
        let (id, kind) = (entry.id(), entry.kind);
        let effective = match kind {
            // A stream's rows exist only as the deltas they produce.
            RelationKind::Stream => return Ok(needed.then_some(batch)),
            RelationKind::BaseTable => super::unique_pk::enforce_unique_pk(entry.store.held(), batch),
            // Folded once for the store, the delta capture and every reader of the
            // echo, which all read a view's output at net weights.
            RelationKind::View(_) => batch.into_consolidated(),
            RelationKind::SystemCatalog => batch,
        };
        if effective.is_empty() {
            return Ok(needed.then_some(effective));
        }

        let pending = entry
            .delta
            .as_deref()
            .zip(round)
            .map(|(feed, r)| (effective.with_key_prefix(feed.schema(), &delta_round_prefix(r)), r));

        for ix in entry.indexes.iter_mut() {
            let cols = ix.cols;
            let res = match ix.project_and_ingest(&effective) {
                // The seam reports from a write that ran; an empty projection is none.
                Ok(false) => continue,
                other => other.map(drop),
            };
            inject_ingest_apply_error("index", kind, res).map_err(|e| {
                format!(
                    "ingest into index on columns {:?} of relation {id}: {e}",
                    cols.as_slice()
                )
            })?;
        }

        let store = entry.store.held_mut();
        let (res, echo) = match needed {
            true => (store.ingest_borrowed_batch(&effective), Some(effective)),
            false => (store.ingest_owned_batch(effective), None),
        };
        inject_ingest_apply_error("store", kind, res).map_err(|e| format!("ingest into relation {id}: {e}"))?;

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
    pub fn fold_to_ram(&mut self, id: u64) -> Result<(), String> {
        let entry = self.relation_mut_or_err(id)?;
        entry
            .store
            .held_mut()
            .fold_to_ram()
            .map_err(|e| format!("fold relation {id} to RAM: {e}"))
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
    pub fn checkpoint_base(&mut self) -> Result<(), String> {
        crate::storage::flush_barrier(self.collect_user_tables().filter(|t| !t.is_rederived()), 0)
            .map_err(|e| format!("base flush: {e}"))
    }

    /// The system round: every system family's store, in one barrier.
    pub fn checkpoint_system(&mut self, replay_floor: u64) -> Result<(), String> {
        crate::storage::flush_barrier(self.collect_system_tables(), replay_floor)
            .map_err(|e| format!("system catalog flush: {e}"))
    }

    /// The ephemeral round at the resume generation: `state`'s operator traces and
    /// every rederived store, in one barrier.
    pub fn checkpoint_ephemeral<'s>(
        &mut self,
        state: impl IntoIterator<Item = &'s mut crate::relation::CircuitState>,
    ) -> Result<(), String> {
        let generation = self.resume_generation;
        let mut tables: Vec<&mut Table> = state.into_iter().flat_map(|s| s.tables_mut()).collect();
        tables.extend(self.collect_user_tables().filter(|t| t.is_rederived()));
        crate::storage::flush_barrier(tables, generation).map_err(|e| format!("ephemeral flush: {e}"))
    }
}
