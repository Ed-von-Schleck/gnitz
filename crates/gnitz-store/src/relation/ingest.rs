//! The write path: unique-PK enforcement, store + secondary-index application,
//! delta capture, the seal, and the checkpoint rounds.

use super::store::{not_held, Rows};
use super::{CircuitState, Relation, RelationKind, RelationRegistry};
use crate::storage::Table;
use gnitz_zset::repr::{Batch, StorageError};
use std::rc::Rc;

/// `GNITZ_INJECT_INGEST_APPLY_ERROR=store|index`: report `Err(Io)` from the
/// matching ingest below, which has already run — so what fires is the error
/// *handling*, not a rolled-back write.
static INGEST_APPLY_ERROR: gnitz_foundation::fault::Seam =
    gnitz_foundation::fault::Seam::new("GNITZ_INJECT_INGEST_APPLY_ERROR");

/// Inert for [`RelationKind::SystemCatalog`]: an armed process must reach the
/// push it is meant to fail.
fn inject_ingest_apply_error(which: &str, kind: RelationKind, r: Result<(), StorageError>) -> Result<(), StorageError> {
    if kind != RelationKind::SystemCatalog && INGEST_APPLY_ERROR.at(which) {
        return Err(StorageError::Io(libc::EIO));
    }
    r
}

/// Where a write lands in its relation's store.
#[derive(Clone, Copy)]
enum Landing {
    /// Above the cut, until a seal answers it: neither captured nor echoed.
    Pending,
    /// At the cut: captured as `round` by the delta feed this process serves, and
    /// the batch as applied echoed back iff `echo`.
    Cut { round: Option<u64>, echo: bool },
}

impl Relation {
    /// Whether rows sit above this relation's cut.
    fn has_pending(&self) -> bool {
        match &self.rows {
            Rows::Held { table, .. } => table.has_pending(),
            Rows::Absent(_) => false,
            Rows::Stream { unsealed, .. } => unsealed.is_some(),
        }
    }

    /// See [`RelationRegistry::seal`].
    #[inline]
    fn seal(&mut self) -> Result<Option<Batch>, String> {
        let id = self.spec.id;
        match &mut self.rows {
            Rows::Held { table, .. } => table.seal().map_err(|e| format!("seal relation {id}: {e}")),
            Rows::Absent(_) => Ok(None),
            Rows::Stream { unsealed, .. } => Ok(unsealed.take().filter(|delta| !delta.is_empty())),
        }
    }

    /// Apply `batch` to this relation's store and its index projections where
    /// `landing` puts it; the batch as applied comes back iff it asks for an echo
    /// and the batch holds a row.
    fn apply(&mut self, batch: Batch, landing: Landing) -> Result<Option<Batch>, String> {
        let Relation { spec, rows, indexes } = self;
        let id = spec.id;
        // Checked, not asserted: the append path sizes by the destination's region
        // count, so a mismatch would silently drop the extra column.
        let want = rows.schema().num_payload_cols();
        let got = batch.schema().num_payload_cols();
        if got != want {
            return Err(format!(
                "batch for relation {id} carries {got} payload columns, its schema has {want}"
            ));
        }
        let (table, feed) = match rows {
            // A stream's rows exist only as the deltas they produce.
            Rows::Stream { unsealed, .. } => {
                if let Landing::Pending = landing {
                    match unsealed {
                        Some(held) => held.append_batch(&batch),
                        held => *held = Some(batch),
                    }
                }
                return Ok(None);
            }
            Rows::Held { table, feed } => (&mut **table, feed),
            Rows::Absent(_) => not_held(),
        };
        let kind = spec.kind;
        let mut effective = match kind.is_base_table() {
            true => super::unique_pk::enforce_unique_pk(table, batch),
            false => batch,
        };
        // Folded once for every reader below — the indexes, the store, the delta
        // capture and the echo — which all read net weights. In place: a batch
        // moved through a by-value call is copied once more.
        effective.consolidate_in_place();
        if effective.is_empty() {
            return Ok(None);
        }

        for ix in indexes.iter_mut() {
            let res = ix.project_and_ingest(&effective);
            inject_ingest_apply_error("index", kind, res).map_err(|e| {
                format!(
                    "ingest into index on columns {:?} of relation {id}: {e}",
                    ix.cols.as_slice()
                )
            })?;
        }

        let Landing::Cut { round, echo } = landing else {
            table.ingest_pending(effective);
            return Ok(None);
        };
        // An echo hands back the caller's batch: its copy, tight, is the run.
        let (run, applied) = match echo {
            true => (effective.to_consolidated(), Some(effective)),
            false => (effective, None),
        };
        let capture = feed.as_mut().zip(round);
        // One allocation for the store's run and the feed's round, which is
        // charged every byte it holds: none it has no row in.
        let run = Rc::new(match capture {
            Some(_) => run.shrunk(),
            None => run.trimmed(),
        });
        let res = table.ingest_run(Rc::clone(&run));
        inject_ingest_apply_error("store", kind, res).map_err(|e| format!("ingest into relation {id}: {e}"))?;
        if let Some((feed, round)) = capture {
            feed.record(round, run);
        }
        Ok(applied)
    }
}

impl RelationRegistry {
    // ── Ingestion ───────────────────────────────────────────────────────
    //
    // Each verb is one lookup and one call, inlined into its caller: out of
    // line, a batch moving through it by value is copied once more.

    /// Apply `batch` to `id`'s store and its index projections, moving it in.
    /// Every batch is consolidated, a base table's after its PK rule; a stream
    /// writes nothing, holding no rows.
    #[inline]
    pub fn ingest(&mut self, id: u64, batch: Batch) -> Result<(), String> {
        let relation = self.relation_mut_or_err(id)?;
        // Rows left above the cut would stay there: nothing seals a relation no
        // view reads.
        if relation.has_pending() {
            relation.seal()?;
        }
        relation
            .apply(batch, Landing::Cut { round: None, echo: false })
            .map(drop)
    }

    /// [`Self::ingest`] above `id`'s cut: every reader sees the rows but one at
    /// [`Cut::Sealed`](super::Cut::Sealed), until [`Self::seal`] answers them.
    /// A view is refused.
    #[inline]
    pub fn ingest_pending(&mut self, id: u64, batch: Batch) -> Result<(), String> {
        let relation = self.relation_mut_or_err(id)?;
        if relation.kind().is_view() {
            return Err(format!("relation {id} is a view: its rows land only at the cut"));
        }
        relation.apply(batch, Landing::Pending).map(drop)
    }

    /// [`Self::ingest`], without its seal of pending rows (a view holds none:
    /// [`Self::ingest_pending`] refuses one), of one round of view `id`'s
    /// output, captured as round `round` by the delta feed this process serves
    /// for it; the batch as applied comes back iff `echo` and it holds a row.
    #[inline]
    pub fn ingest_round(&mut self, id: u64, batch: Batch, round: u64, echo: bool) -> Result<Option<Batch>, String> {
        let landing = Landing::Cut { round: Some(round), echo };
        self.relation_mut_or_err(id)?.apply(batch, landing)
    }

    /// Move `id`'s cut past every row ingested as pending since the last seal,
    /// and answer those rows: the relation's delta, consolidated for a table and
    /// as pushed for a stream. `None` when there is none.
    #[inline]
    pub fn seal(&mut self, id: u64) -> Result<Option<Batch>, String> {
        self.relation_mut_or_err(id)?.seal()
    }

    /// Drop every row view `id`'s store and its indexes hold; the next barrier
    /// publishes them empty, and until then a reopen finds what the last one
    /// published. For a view with no delta feed.
    pub fn clear_rows(&mut self, id: u64) -> Result<(), String> {
        let entry = self.relation_mut_or_err(id)?;
        debug_assert!(entry.kind().is_view() && entry.feed().is_none());
        entry.rows.held_mut().clear();
        entry
            .indexes
            .iter_mut()
            .filter_map(|ix| ix.table.as_deref_mut())
            .for_each(Table::clear);
        Ok(())
    }

    // ── Flush / checkpoint collection ───────────────────────────────────

    /// Fold `id`'s store memtable into its RAM tier, which past its ceiling also
    /// spills it, then run every fold its disk tier owes, the capacity sweep
    /// included: no manifest publish, no barrier, and nothing of the relation's
    /// indexes. Unregistered is an `Err`.
    pub fn fold_to_ram(&mut self, id: u64) -> Result<(), String> {
        let store = self.relation_mut_or_err(id)?.rows.held_mut();
        store
            .fold_to_ram()
            .and_then(|()| store.settle())
            .map_err(|e| format!("fold relation {id} to RAM: {e}"))
    }

    /// The stores this process holds of the relations whose kind `keep` accepts.
    pub(super) fn held_rows(&mut self, keep: impl Fn(RelationKind) -> bool) -> impl Iterator<Item = &mut Table> {
        self.relations
            .values_mut()
            .filter(move |e| keep(e.kind()))
            .filter_map(|e| e.rows.table_mut())
    }

    // ── The checkpoint rounds ───────────────────────────────────────────

    /// The bytes `id`'s next published manifest carries beside its rows.
    pub fn set_caller_record(&mut self, id: u64, record: Vec<u8>) -> Result<(), String> {
        self.relation_mut_or_err(id)?
            .table_mut_or_err()?
            .set_caller_record(record);
        Ok(())
    }

    /// The base round: every base table's rows — the one user store a boot does
    /// not rebuild — in one barrier.
    pub fn checkpoint_base(&mut self) -> Result<(), String> {
        crate::storage::flush_barrier(self.held_rows(RelationKind::is_base_table), 0)
            .map_err(|e| format!("base flush: {e}"))
    }

    /// The system round: every system family's store, in one barrier.
    pub fn checkpoint_system(&mut self, replay_floor: u64) -> Result<(), String> {
        assert!(
            self.residency.publishes(RelationKind::SystemCatalog),
            "a worker holds the system families in RAM only; the master writes their shards"
        );
        crate::storage::flush_barrier(self.held_rows(|k| k == RelationKind::SystemCatalog), replay_floor)
            .map_err(|e| format!("system catalog flush: {e}"))
    }

    /// The ephemeral round at `generation`: the stores a boot rebuilds — a view's
    /// rows and every index — of each user relation `resumes` picks, and `state`,
    /// those relations' operator traces, in one barrier. A relation no round ever
    /// picks has no manifest, so every open rebuilds it.
    pub fn checkpoint_ephemeral<'s>(
        &mut self,
        state: impl IntoIterator<Item = &'s mut CircuitState>,
        generation: u64,
        resumes: impl Fn(u64) -> bool,
    ) -> Result<(), String> {
        let mut tables: Vec<&mut Table> = state.into_iter().flat_map(CircuitState::tables_mut).collect();
        let picked = self.relations.values_mut().filter(|e| resumes(e.id()));
        for e in picked {
            if e.kind().is_view() {
                tables.extend(e.rows.table_mut());
            }
            tables.extend(e.indexes.iter_mut().filter_map(|ix| ix.table.as_deref_mut()));
        }
        crate::storage::flush_barrier(tables, generation).map_err(|e| format!("ephemeral flush: {e}"))
    }
}

#[cfg(test)]
#[path = "tests/ingest.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/capture.rs"]
mod bench;
