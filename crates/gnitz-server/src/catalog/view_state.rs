//! What the catalog owns above the registry because it needs both rungs: the
//! boot resume verdict, the source cursor a circuit backfill drives, and the
//! three read entries that can meet a skeleton row.
//!
//! Those three are the only wrappers the catalog keeps over the read rung.
//! Every other registry name a caller reaches is spelled `registry()` at its
//! site: a `CatalogEngine::relation_kind` would hide the rung its answer came
//! from, and a delegator layer would be a dozen functions that can drift from
//! what they forward to. These three earn their place by supplying something a
//! caller cannot — the hydrator, which is the catalog's *other* field.

use super::*;
use gnitz_store::read::IndexWalk;
use gnitz_store::storage::SourceCursor;
use gnitz_wire::{ReadSpec, WireFault};
use rustc_hash::FxHashSet;

impl CatalogEngine {
    /// [`RelationRegistry::scan_family`] with this engine's own circuit layer as
    /// the hydrator.
    pub(crate) fn scan_family(&mut self, table_id: i64) -> Result<(Rc<Batch>, SchemaDescriptor), String> {
        let (dag, registry) = self.dag_and_registry_mut();
        registry.scan_family(table_id, Some(dag))
    }

    /// [`RelationRegistry::seek_family`], hydrating.
    pub(crate) fn seek_family(
        &mut self,
        table_id: i64,
        seek_pk: u128,
        seek_pk_extra: &[u8],
    ) -> Result<(Option<Batch>, SchemaDescriptor), String> {
        let (dag, registry) = self.dag_and_registry_mut();
        registry.seek_family(table_id, seek_pk, seek_pk_extra, Some(dag))
    }

    /// [`RelationRegistry::scan_spec_family`], hydrating.
    pub(crate) fn scan_spec_family(
        &mut self,
        target_id: i64,
        spec: &ReadSpec,
        reply_schema: &SchemaDescriptor,
        cut_tick: u64,
    ) -> Result<Batch, WireFault> {
        let (dag, registry) = self.dag_and_registry_mut();
        registry.scan_spec_family(target_id, spec, reply_schema, cut_tick, Some(dag))
    }

    /// Compute the set of view ids whose checkpointed state — output stores and
    /// operator traces alike — must be rejected at boot and rebuilt, rather than
    /// resumed from its manifests.
    ///
    /// A view is **valid** (resumed) iff:
    ///   * the recorded topology matches the launched `(worker_count, STATE_FORMAT)`
    ///     — a different worker count re-shapes every keyed store's row placement;
    ///   * every child that carries its state — its output-store children and its
    ///     owned operator scratch — is stamped with the committed checkpoint
    ///     generation — `resume_generation`, the in-memory recovered `G`, NOT the
    ///     recovery-start-bumped durable `G+1` — matching what `Table::new`'s
    ///     conditional load peeks; and
    ///   * every VIEW it scans is itself valid — else it could read a rebuilt
    ///     sibling's freshly-emptied output store; and
    ///   * none of its sources is a STREAM — a stream-fed view's manifests are
    ///     published normally, so without this it would resume at the committed
    ///     generation onto state whose stream inputs are gone.
    ///
    /// The topology word is decided once for the whole set. Then phase 1 decides
    /// each view's **local** validity (direct sources + child manifests), and
    /// phase 2 propagates invalidity to any view scanning an invalid source,
    /// walking the views in dependency order so one pass reaches the whole cascade.
    ///
    /// Which children carry that state is [`children_at_generation`]'s to say.
    /// Reading the operator scratch alongside the output stores is what rejects an
    /// output store one generation ahead of the integral beneath it — the ephemeral
    /// round stamps every output store but only a *compiled* view's traces.
    pub(crate) fn compute_invalid_views(&mut self) {
        self.registry.assert_pre_fork("compute_invalid_views");
        let launched_workers = self.registry.num_workers();
        // The same value `rederive_source` reads for the store-open half of the
        // verdict, so both halves answer from one number.
        let g = self.registry.resume_generation();
        let topo_valid = self.registry.topology_matches();

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
                .any(|s| self.registry.relation_kind(*s) == Some(RelationKind::Stream));
            if stream_fed {
                invalid.insert(vid);
                continue;
            }
            let dir = self
                .registry
                .table_directory(vid)
                .expect("vid taken from the registry's own view list");
            if !children_at_generation(dir, launched_workers, g) {
                invalid.insert(vid);
            }
        }
        if invalid.is_empty() {
            // Clean restart: nothing to propagate, so skip the ordering walk below.
            self.invalid_views = invalid;
            return;
        }

        // Phase 2: propagate invalidity to any still-valid view that scans an
        // invalid source. Base and stream sources never enter `invalid`, so they
        // pass — a stream's own dependents were caught in phase 1 instead. A
        // source view precedes every view scanning it in `order_by_view_deps`,
        // so a single pass carries invalidity down the whole chain.
        for vid in self.dag.order_by_view_deps(&self.registry, &view_ids) {
            if !invalid.contains(&vid)
                && self
                    .dag
                    .get_source_ids(&self.registry, vid)
                    .iter()
                    .any(|s| invalid.contains(s))
            {
                invalid.insert(vid);
            }
        }
        self.invalid_views = invalid;
    }

    /// The source cursor for driving `source` through `view_id`'s circuit: an
    /// index-bounded cursor when the compiled plan pushed a bound down and
    /// `open_index_source`'s gate takes it, else the full-scan cursor.
    /// The circuit's `Filter` is authoritative either way, so the choice only
    /// decides how many rows are read. The open may COMPILE the view.
    ///
    /// A registered-but-empty table yields a cursor, and a provably empty range
    /// yields `SourceCursor::Empty` — collapsing that into an error would skip
    /// the source rather than feed it one empty epoch. `Err` is a view that does
    /// not compile, or an unregistered source: DDL_SYNC applies in SAL order, so
    /// a worker that cannot see the source has diverged from the catalog.
    pub(crate) fn open_source_cursor(&mut self, view_id: i64, source: i64) -> Result<SourceCursor, String> {
        // A store-less handle opens an EMPTY cursor, not an error — right for a
        // stream, silently wrong for the post-fork master. Hard, not
        // `debug_assert!`: release is a supported deployment, one compare per
        // backfill source.
        assert!(
            self.registry.owns_stores(),
            "source cursor in a process owning no base store (view {view_id}, source {source})",
        );
        // Must precede `source_scan_bound`: `handle_backfill` reaches here before
        // anything compiles the view, and an uncached plan would silently report
        // "no bound" — the motivating GROUP BY case would full-scan invisibly.
        // Cache-first and idempotent.
        let CatalogEngine { registry, dag, .. } = self;
        let bound = dag
            .ensure_compiled(registry, view_id)?
            .then(|| dag.source_scan_bound(view_id, source))
            .flatten();
        let Some(bound) = bound else {
            return Ok(SourceCursor::Full(Box::new(
                registry.table_entry(source)?.open_cursor(),
            )));
        };
        registry.open_index_source(source, bound.idx_cols.as_slice(), &bound.desc, IndexWalk::Optional)
    }
}
