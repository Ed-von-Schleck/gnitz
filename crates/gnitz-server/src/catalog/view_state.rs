//! What the catalog owns above the registry because it needs both rungs: the
//! boot resume verdict, and the read wrappers that add what a caller cannot —
//! the hydrator.

use std::rc::Rc;

use gnitz_store::relation::{Relation, Residency};
use gnitz_wire::ReadSpec;
use gnitz_zset::repr::Batch;
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
    ///     checkpoint generation, which
    ///     [`RelationRegistry::view_children_resumable`] answers. That is
    ///     `resume_generation`, the recovered `G`: the durable one is `G+1`
    ///     before any store opens when the boot replays a push; and
    ///   * every VIEW it scans is itself valid — else it could read a rebuilt
    ///     sibling's freshly-emptied output store; and
    ///   * every view of its chain is valid — a segment's rows are dropped once
    ///     its chain is built, so a chain member's rebuild needs them rebuilt; and
    ///   * none of its sources is a STREAM or a system family — its state
    ///     integrates stream rows that are gone, or catalog rows no boot replays
    ///     into it.
    ///
    /// The topology word is decided once for the whole set. Then each view's
    /// **local** validity is decided (direct sources + child manifests), and
    /// [`Self::rebuilt_with`] propagates invalidity to any view scanning an
    /// invalid source and to the whole chain of any invalid view.
    fn compute_invalid_views(&mut self) {
        let topo_valid = self.topology_matches();

        let view_ids: Vec<u64> = self.registry.view_ids().collect();
        // A worker-count or STATE_FORMAT change re-shapes every keyed store, so no
        // view resumes and nothing below need be read.
        if !topo_valid {
            self.dag.set_rebuild(view_ids.into_iter().collect());
            return;
        }

        // Local validity: every output child's manifest at `g`. A view over a
        // stream or a system family needs no manifest read.
        // An unreadable manifest reads as a mismatch, which is the verdict a child
        // whose manifest a previous open erased must get: its siblings may still
        // be at `g`.
        let mut invalid = self.views_over_unreplayed_sources();
        for &vid in &view_ids {
            if !invalid.contains(&vid) && !self.registry.view_children_resumable(vid, self.resume_generation) {
                invalid.insert(vid);
            }
        }
        let rebuild = self.rebuilt_with(invalid);
        self.dag.set_rebuild(rebuild);
    }

    /// The views with a direct source whose deltas no boot replays into a kept
    /// view. Boot replays a base table's tail alone: a stream's rows are gone,
    /// and a system family's tail is applied before any view exists.
    fn views_over_unreplayed_sources(&self) -> FxHashSet<u64> {
        let unreplayed = |s: &u64| {
            self.registry
                .relation(*s)
                .map(Relation::kind)
                .is_some_and(|k| !(k.is_view() || k.is_base_table()))
        };
        self.registry
            .view_ids()
            .filter(|&vid| self.dag.sources_of(vid).iter().any(unreplayed))
            .collect()
    }

    /// Every view a rebuild of `invalid` takes with it: each view downstream of
    /// one, then every view of a chain holding one.
    fn rebuilt_with(&self, mut invalid: FxHashSet<u64>) -> FxHashSet<u64> {
        let downstream = self.dag.dependent_closure(invalid.iter().copied());
        invalid.extend(downstream);
        let chains: FxHashSet<u64> = invalid.iter().map(|&v| self.dag.chain_of(v)).collect();
        self.registry
            .view_ids()
            .filter(|&v| chains.contains(&self.dag.chain_of(v)))
            .collect()
    }

    /// The views every boot rebuilds whatever their stores hold: those a stream
    /// or a system family reaches, and their chains. A subset of what
    /// [`Self::compute_invalid_views`] rejects, so a checkpoint round has no
    /// reason to publish one.
    pub(in crate::catalog) fn never_resumed_views(&self) -> FxHashSet<u64> {
        self.rebuilt_with(self.views_over_unreplayed_sources())
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
}
