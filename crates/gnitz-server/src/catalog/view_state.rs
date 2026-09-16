//! What the catalog owns above the registry because it needs both rungs: the
//! boot resume verdict, the source cursor a circuit backfill drives, and the read
//! wrappers that add what a caller cannot — the hydrator, or a delta read's expiry
//! status. Every other registry name is spelled `registry()` at its site.

use super::*;
use gnitz_store::storage::SourceCursor;
use gnitz_wire::{IndexWalk, ReadBound};
use gnitz_wire::{ReadSpec, WireFault};
use rustc_hash::FxHashSet;
use std::rc::Rc;

impl CatalogEngine {
    /// [`RelationRegistry::scan`] with this engine's own circuit layer as
    /// the hydrator.
    pub(crate) fn scan(&mut self, table_id: i64) -> Result<(Rc<Batch>, SchemaDescriptor), String> {
        let schema = self.registry.relation_or_err(table_id)?.schema();
        let (dag, registry) = self.dag_and_registry_mut();
        Ok((registry.scan(table_id, Some(dag))?, schema))
    }

    /// [`RelationRegistry::scan_spec`], hydrating.
    pub(crate) fn scan_spec(
        &mut self,
        target_id: i64,
        spec: ReadSpec,
        reply_schema: &SchemaDescriptor,
    ) -> Result<Rc<Batch>, WireFault> {
        let (dag, registry) = self.dag_and_registry_mut();
        registry
            .scan_spec(target_id, spec, reply_schema, Some(dag))
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
        reply_schema: &SchemaDescriptor,
    ) -> Result<Rc<Batch>, WireFault> {
        self.registry
            .delta_read(target_id, after_tick, cut_tick, reply_schema)
            .map_err(|e| match e {
                StoreError::DeltaExpired(text) => WireFault {
                    status: gnitz_wire::WireStatus::DeltaExpired,
                    text,
                },
                other => WireFault::from(other.to_string()),
            })
    }

    /// Compute the set of view ids whose checkpointed state — output stores and
    /// operator traces alike — must be rejected at boot and rebuilt, rather than
    /// resumed from its manifests.
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
            self.invalid_views = view_ids.into_iter().collect();
            return;
        }

        // Phase 1: local validity (no direct stream source + every output child's
        // manifest at g). A *transitive* stream source needs no walk here: phase 2
        // propagates invalidity down every dependency chain. The source test comes
        // first, so it short-circuits the per-child manifest reads.
        // An unpeekable manifest reads as a mismatch, which is the verdict a child
        // whose manifest a previous open erased must get: its siblings may still
        // be at `g`.
        let mut invalid: FxHashSet<i64> = FxHashSet::default();
        for &vid in &view_ids {
            let stream_fed = self
                .dag
                .get_source_ids(&self.registry, vid)
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
        let reached = self.dag.dependent_closure(&self.registry, seeds);
        invalid.extend(reached.into_iter().filter(|v| view_set.contains(v)));
        self.invalid_views = invalid;
    }

    /// The cursor driving `source` through `view_id`'s circuit, under the bound the
    /// circuit carries for it; its `Filter` stays authoritative.
    pub(crate) fn open_source_cursor(&mut self, view_id: i64, source: i64) -> Result<SourceCursor, String> {
        // A store-less handle reads empty rather than erroring: correct for a
        // stream, a wrong answer for a process whose store is elsewhere. Hard, not
        // `debug_assert!` — release is a supported deployment.
        assert!(
            self.registry.residency().owns_stores(),
            "source cursor in a process owning no base store (view {view_id}, source {source})",
        );
        let CatalogEngine { registry, dag, .. } = self;
        let bound = match dag.source_scan_bound(registry, view_id, source) {
            // A backfill hint: an index the source no longer has falls back to the full scan.
            ReadBound::IndexRange { bound, .. } => ReadBound::IndexRange { bound, walk: IndexWalk::Optional },
            bound => bound,
        };
        registry.open_bound(source, bound).map_err(String::from)
    }
}
