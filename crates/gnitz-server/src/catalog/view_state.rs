//! What the catalog owns above the registry because it needs both rungs: the
//! boot resume verdict, the source cursor a circuit backfill drives, and the read
//! wrappers that add what a caller cannot — the hydrator, or a delta read's expiry
//! status.

use super::*;
use gnitz_store::read::SourceCursor;
use gnitz_store::relation::Residency;
use gnitz_wire::{ReadBound, ReadSpec, WireFault};
use rustc_hash::FxHashSet;
use std::rc::Rc;

impl CatalogEngine {
    /// Every row of `table_id`, with this engine's own circuit layer as the hydrator.
    pub(crate) fn scan(&mut self, table_id: i64) -> Result<Rc<Batch>, String> {
        let schema = self.registry.relation_or_err(table_id)?.schema();
        Ok(self.registry.scan_spec(
            table_id,
            ReadSpec::all_rows(ReadBound::None),
            schema.layout_digest(),
            Some(&mut self.dag),
        )?)
    }

    /// [`RelationRegistry::scan_spec`], hydrating.
    pub(crate) fn scan_spec(
        &mut self,
        target_id: i64,
        spec: ReadSpec,
        reply_layout: u64,
    ) -> Result<Rc<Batch>, WireFault> {
        self.registry
            .scan_spec(target_id, spec, reply_layout, Some(&mut self.dag))
            .map_err(|e| WireFault::from(e.to_string()))
    }

    /// [`RelationRegistry::delta_read`]. The one site where a delta read's store
    /// error becomes a wire fault: an expired cursor keeps its own status,
    /// everything else is `WireStatus::Error`.
    pub(crate) fn delta_read(
        &self,
        target_id: i64,
        after_tick: u64,
        cut_tick: u64,
        reply_layout: u64,
    ) -> Result<Rc<Batch>, WireFault> {
        self.registry
            .delta_read(target_id, after_tick, cut_tick, reply_layout)
            .map_err(|e| match e {
                StoreError::DeltaExpired(text) => WireFault {
                    status: gnitz_wire::WireStatus::DeltaExpired,
                    text,
                },
                other => WireFault::from(other.to_string()),
            })
    }

    /// Mark non-resumable every view whose checkpointed state — output stores
    /// and operator traces alike — must be rebuilt rather than resumed.
    ///
    /// A view is **valid** (resumed) iff:
    ///   * the recorded topology matches the launched `(worker_count, STATE_FORMAT)`
    ///     — a different worker count re-shapes every keyed store's row placement;
    ///   * every child that carries its state is stamped with the committed
    ///     checkpoint generation — `resume_generation`, the in-memory recovered
    ///     `G`, NOT the recovery-start-bumped durable `G+1` — which
    ///     [`RelationRegistry::view_children_resumable`] answers; and
    ///   * every VIEW it scans is itself valid — else it could read a rebuilt
    ///     sibling's freshly-emptied output store; and
    ///   * none of its sources is a STREAM — a stream-fed view's manifests are
    ///     published normally, so without this it would resume at the committed
    ///     generation onto state whose stream inputs are gone.
    ///
    /// The topology word is decided once for the whole set. Then phase 1 decides
    /// each view's **local** validity (direct sources + child manifests), and
    /// phase 2 propagates invalidity to any view scanning an invalid source,
    /// following scan edges forward from every locally invalid view.
    pub(crate) fn compute_invalid_views(&mut self) {
        let topo_valid = self.topology_matches();

        let view_ids = self.registry.view_ids();
        // A worker-count or STATE_FORMAT change re-shapes every keyed store, so no
        // view resumes and nothing below need be read.
        if !topo_valid {
            self.dag.set_rebuild(view_ids.into_iter().collect());
            return;
        }

        // Phase 1: local validity (no direct stream source + every output child's
        // manifest at g). A *transitive* stream source needs no walk here: phase 2
        // propagates invalidity down every dependency chain. The source test comes
        // first, so it short-circuits the per-child manifest reads.
        // An unreadable manifest reads as a mismatch, which is the verdict a child
        // whose manifest a previous open erased must get: its siblings may still
        // be at `g`.
        let mut invalid: FxHashSet<i64> = FxHashSet::default();
        for &vid in &view_ids {
            let stream_fed = self
                .dag
                .sources_of(vid)
                .iter()
                .any(|s| self.registry.relation(*s).map(Relation::kind) == Some(RelationKind::Stream));
            if stream_fed {
                invalid.insert(vid);
                continue;
            }
            if !self.registry.view_children_resumable(vid) {
                invalid.insert(vid);
            }
        }

        // Phase 2: every registered view downstream of an invalid one.
        let view_set: FxHashSet<i64> = view_ids.iter().copied().collect();
        let seeds = invalid.iter().copied().collect();
        let reached = self.dag.dependent_closure(seeds);
        invalid.extend(reached.into_iter().filter(|v| view_set.contains(v)));
        self.dag.set_rebuild(invalid);
    }

    /// Open this process's stores under the boot's resume verdict: a relation's
    /// rederived state resumes iff the topology matches and the verdict kept it.
    pub(crate) fn open_stores(&mut self, rank: u32, residency: Residency) -> Result<usize, String> {
        let topology = self.topology_matches();
        let dag = &self.dag;
        Ok(self
            .registry
            .open_stores(rank, residency, |id| topology && !dag.awaits_rebuild(id))?)
    }

    /// The cursor driving `source` through `view_id`'s circuit, under the bound the
    /// circuit carries for it; the circuit's `Filter` applies the WHERE.
    pub(crate) fn open_source_cursor(&mut self, view_id: i64, source: i64) -> Result<SourceCursor, String> {
        let bound = self.dag.view_meta(view_id)?.source_bound(source);
        self.registry
            .open_bound(source, bound)
            .map(|(cursor, _unapplied)| cursor)
            .map_err(String::from)
    }
}
