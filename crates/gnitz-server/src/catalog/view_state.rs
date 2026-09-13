//! What the catalog owns above the registry because it needs both rungs: the
//! boot resume verdict, the source cursor a circuit backfill drives, and the
//! three read entries that can meet a skeleton row.
//!
//! Those three are the only wrappers the catalog keeps over the read rung.
//! Every other registry name a caller reaches is spelled `registry()` at its
//! site: a `CatalogEngine::kind` would hide the rung its answer came
//! from, and a delegator layer would be a dozen functions that can drift from
//! what they forward to. These three earn their place by supplying something a
//! caller cannot — the hydrator, which is the catalog's *other* field.

use super::*;
use gnitz_store::storage::SourceCursor;
use gnitz_wire::IndexWalk;
use gnitz_wire::{ReadSpec, WireFault};
use rustc_hash::FxHashSet;

impl CatalogEngine {
    /// [`RelationRegistry::scan`] with this engine's own circuit layer as
    /// the hydrator.
    pub(crate) fn scan(&mut self, table_id: i64) -> Result<(Rc<Batch>, SchemaDescriptor), String> {
        let (dag, registry) = self.dag_and_registry_mut();
        registry.scan(table_id, Some(dag)).map_err(String::from)
    }

    /// Point lookup by the wire seek pair, hydrating: the pair decoded to OPK
    /// bytes, then [`RelationRegistry::seek`]. The schema comes back with
    /// a miss too — its STATUS_OK reply block needs it.
    pub(crate) fn seek(
        &mut self,
        table_id: i64,
        seek_pk: u128,
        seek_pk_extra: &[u8],
    ) -> Result<(Option<Batch>, SchemaDescriptor), String> {
        let schema = self.registry.relation_or_err(table_id)?.schema();
        let opk = gnitz_store::schema::key::seek_opk_bytes(&schema, seek_pk, seek_pk_extra)
            .map_err(|e| format!("seek: table {table_id}: {e}"))?;
        let (dag, registry) = self.dag_and_registry_mut();
        Ok((registry.seek(table_id, opk.pk_bytes(), Some(dag))?, schema))
    }

    /// [`RelationRegistry::scan_spec`], hydrating. The one site where a
    /// store error becomes a wire fault: an expired delta cursor keeps its own
    /// status, everything else is `STATUS_ERROR`.
    pub(crate) fn scan_spec(
        &mut self,
        target_id: i64,
        spec: &ReadSpec,
        reply_schema: &SchemaDescriptor,
        cut_tick: u64,
    ) -> Result<Batch, WireFault> {
        let (dag, registry) = self.dag_and_registry_mut();
        registry
            .scan_spec(target_id, spec, reply_schema, cut_tick, Some(dag))
            .map_err(|e| match e {
                StoreError::DeltaExpired(text) => WireFault {
                    status: gnitz_wire::STATUS_DELTA_EXPIRED,
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
    /// walking the views in dependency order so one pass reaches the whole cascade.
    pub(crate) fn compute_invalid_views(&mut self) {
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
                .any(|s| self.registry.relation(*s).map(Relation::kind) == Some(RelationKind::Stream));
            if stream_fed {
                invalid.insert(vid);
                continue;
            }
            if !self.registry.view_children_resumable(vid) {
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
    /// a bounded walk that drains nothing — collapsing that into an error would skip
    /// the source rather than feed it one empty epoch. `Err` is a view that does
    /// not compile, or an unregistered source: DDL_SYNC applies in SAL order, so
    /// a worker that cannot see the source has diverged from the catalog.
    pub(crate) fn open_source_cursor(&mut self, view_id: i64, source: i64) -> Result<SourceCursor, String> {
        // A store-less handle reads empty rather than erroring: correct for a
        // stream, a wrong answer for a process whose store is elsewhere. Hard, not
        // `debug_assert!` — release is a supported deployment.
        assert!(
            self.registry.residency().owns_stores(),
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
            return Ok(SourceCursor::Full(Box::new(registry.relation_or_err(source)?.cursor())));
        };
        registry
            .open_index_source(source, bound.idx_cols.as_slice(), &bound.desc, IndexWalk::Optional)
            .map_err(String::from)
    }
}
