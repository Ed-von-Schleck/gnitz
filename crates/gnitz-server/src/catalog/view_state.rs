//! What the catalog owns above the registry because it needs both rungs: the
//! boot resume verdict, the source cursor a circuit backfill drives, and the read
//! wrappers that add what a caller cannot — the hydrator.

use std::rc::Rc;

use gnitz_store::relation::{Relation, RelationKind, Residency};
use gnitz_wire::ReadSpec;
use gnitz_zset::repr::{Batch, SourceCursor};
use rustc_hash::FxHashSet;

use super::CatalogEngine;

impl CatalogEngine {
    /// [`RelationRegistry::scan_spec`], hydrating.
    pub(crate) fn scan_spec(&mut self, target_id: u64, spec: ReadSpec, reply_layout: u64) -> Result<Rc<Batch>, String> {
        self.registry
            .scan_spec(target_id, spec, reply_layout, Some(&mut self.dag))
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
    ///   * every view of its chain is valid — a segment's rows are dropped once
    ///     its chain is built, so a chain member's rebuild needs them rebuilt; and
    ///   * none of its sources is a STREAM — a stream-fed view's manifests are
    ///     published normally, so without this it would resume at the committed
    ///     generation onto state whose stream inputs are gone.
    ///
    /// The topology word is decided once for the whole set. Then phase 1 decides
    /// each view's **local** validity (direct sources + child manifests), and
    /// phase 2 propagates invalidity to any view scanning an invalid source,
    /// following scan edges forward from every locally invalid view, and to the
    /// whole chain of any invalid view.
    fn compute_invalid_views(&mut self) {
        let topo_valid = self.topology_matches();

        let view_ids: Vec<u64> = self.registry.view_ids().collect();
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
        let mut invalid: FxHashSet<u64> = FxHashSet::default();
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
            if !self.registry.view_children_resumable(vid, self.resume_generation) {
                invalid.insert(vid);
            }
        }

        // Phase 2: every view downstream of an invalid one, then every view of a
        // chain holding one.
        let downstream = self.dag.dependent_closure(invalid.iter().copied());
        invalid.extend(downstream);
        let chains: FxHashSet<u64> = invalid.iter().map(|&v| self.dag.chain_of(v)).collect();
        let whole_chains = view_ids.into_iter().filter(|&v| chains.contains(&self.dag.chain_of(v)));
        self.dag.set_rebuild(whole_chains.collect());
    }

    /// Relay each base table onto the launched worker count and drop the children
    /// no relation owns any more, then reach the resume verdict — which reads every
    /// launched rank's manifests, so it runs where no worker exists yet.
    pub(in crate::catalog) fn settle_derived_state(&mut self) -> Result<(), String> {
        self.registry
            .reconcile_child_dirs()
            .map_err(|e| format!("child-dir sweep failed: {e}"))?;
        self.compute_invalid_views();
        Ok(())
    }

    /// Open this process's stores under the boot's resume verdict: a relation's
    /// rederived state resumes iff the topology matches and the verdict kept it.
    pub(crate) fn open_stores(&mut self, rank: u32, residency: Residency) -> Result<usize, String> {
        let resume_at = self.topology_matches().then_some(self.resume_generation);
        let dag = &self.dag;
        self.registry
            .open_stores(rank, residency, |id| resume_at.filter(|_| !dag.awaits_rebuild(id)))
    }

    /// The cursor driving `source` through `view_id`'s circuit, under the bound the
    /// circuit carries for it; the circuit's `Filter` applies the WHERE.
    pub(crate) fn open_source_cursor(&mut self, view_id: u64, source: u64) -> Result<SourceCursor, String> {
        let bound = self.dag.view_meta(view_id)?.source_bound(source);
        self.registry
            .open_bound(source, bound)
            .map(|(cursor, _unapplied)| cursor)
    }
}
