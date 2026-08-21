//! Epoch execution: the one pre → relay → consolidate → post pipeline every
//! compiled shape runs through, and the DAG evaluation driver.

use super::*;
use crate::query::compiler::PlanShape;
use std::collections::BTreeMap;

/// The DAG traversal's work queue: `(depth, view_id, source_id) → batch`.
/// Ordered by depth, so `pop_first` is the shallowest pending edge; keyed by the
/// edge, so merge-on-collision is one `get_mut`.
type Pending = BTreeMap<(i32, i64, i64), Batch>;

impl DagEngine {
    // ── Epoch execution ─────────────────────────────────────────────────

    /// Execute one view's epoch with no exchange IPC: every `Single`-shape
    /// dispatch (arms 5 and 6 of `execute_multi_worker_step`, which never invoke
    /// the relay at all — `PlanShape::Single` does not call it), and a replicated
    /// view of any shape (arm 1), where the identity relay does run because the
    /// shape is still `Exchanged` but every worker already holds every source in
    /// full.
    fn execute_epoch(&mut self, view_id: i64, input: Batch, source_id: i64) -> Option<Batch> {
        self.run_view_epoch(view_id, input, source_id, |pre, _| pre)
    }

    /// Execute one view epoch through its compiled shape.
    ///
    /// * `Single`: one sub-pipeline, run directly.
    /// * `Exchanged`: route the delta to the side(s) scanning its source, run
    ///   each side, hand its output through `relay`, consolidate, and seed the
    ///   post combine with every side's batch.
    ///
    /// `relay(pre, key)` is the repartition step: an exchanged view passes the
    /// exchange IPC (`key` is the side's round key — a two-sided set-op keys by
    /// the side's source so the two rounds don't collide in the master
    /// accumulator; a unary side keys 0), a replicated view passes identity
    /// (every worker holds every source in full, so there is nothing to
    /// repartition). A side that takes the delta always runs its
    /// relay — even on an empty pre output — so collective exchange rounds stay
    /// balanced across workers; an inactive side (no delta this epoch) skips its
    /// VM pass, its relay, and its consolidate, and seeds an empty placeholder
    /// (`a_needs`/`b_needs` derive only from the plan sources and `src_id`,
    /// both identical on every worker, so all workers skip the same side).
    ///
    /// The consolidate is mandatory before the post phase: the relay
    /// concatenates rows from all workers (and a HashRow reindex scrambles PK
    /// order), and the post phase's distinct/join operators assume sorted,
    /// weight-merged input.
    fn run_view_epoch(
        &mut self,
        view_id: i64,
        input: Batch,
        src_id: i64,
        mut relay: impl FnMut(Batch, i64) -> Batch,
    ) -> Option<Batch> {
        if !self.ensure_compiled(view_id) {
            gnitz_warn!("dag: run_view_epoch — no plan for view_id={}", view_id);
            return None;
        }
        let plan = self.cache.get_mut(&view_id).unwrap();
        match &mut plan.shape {
            PlanShape::Single(sub) => Self::execute_sub_plan(sub, input, src_id),
            PlanShape::Exchanged { sides, post } => {
                // A unary side takes every delta; a set-op side takes it iff it
                // scans the delta's source (`a UNION a` — both sides scan one
                // relation — clones so each side gets it).
                let unary = sides.len() == 1;
                // Nothing takes the delta: every seed comes up empty and the rows
                // are dropped. The epoch still runs — the post phase can still
                // mint the global-ground row — so this line is the only trace.
                if !unary && !sides.iter().any(|s| s.source_id == src_id) {
                    gnitz_warn!(
                        "dag: view {} — delta source {} matches no side; rows dropped",
                        view_id,
                        src_id
                    );
                    debug_assert!(false, "view {view_id}: delta source {src_id} matches no side");
                }
                let mut remaining = sides.iter().filter(|s| unary || s.source_id == src_id).count();
                let mut input = Some(input);
                let mut seeds: Vec<(u16, Batch)> = Vec::with_capacity(sides.len());
                for side in sides.iter_mut() {
                    let schema = side.exchange_schema();
                    let takes = unary || side.source_id == src_id;
                    let consolidated = if takes {
                        remaining -= 1;
                        let delta = if remaining == 0 {
                            input.take().unwrap()
                        } else {
                            input.as_ref().unwrap().clone_batch()
                        };
                        // `exchange_schema()` IS the side's out-register schema,
                        // which the VM stamps on what it returns, so the wire
                        // encode already sees the side's pre-exchange schema
                        // (never the view's combine-widened final one).
                        let pre = Self::execute_sub_plan(&mut side.plan, delta, src_id)
                            .unwrap_or_else(|| Batch::empty_with_schema(&schema));
                        let relay_key = if unary { 0 } else { side.source_id };
                        Self::consolidate_exchanged(relay(pre, relay_key), &schema)
                    } else {
                        Batch::empty_with_schema(&schema)
                    };
                    seeds.push((side.seed_reg, consolidated));
                }
                // A pad round whose every side produced nothing.
                if Self::latch_empty_epoch(post, seeds.iter().all(|(_, b)| b.count == 0)) {
                    return None;
                }
                Self::execute_sub_plan_multi(post, seeds)
            }
        }
    }

    /// Release the delta batches pinned in a view's compiled-plan regfiles.
    /// The VM only clears deltas at the *start* of an epoch, so after a
    /// backfill the last chunk's input and intermediate deltas stay resident
    /// until the next evaluation. Called at the end of a backfill so peak
    /// resident memory falls back to ~O(chunk) once it drains.
    pub fn clear_view_regfile_deltas(&mut self, view_id: i64) {
        if let Some(plan) = self.cache.get_mut(&view_id) {
            for sub in plan.sub_plans_mut() {
                sub.vm.clear_deltas();
            }
        }
    }

    /// Execute one sub-pipeline epoch, seeding one register per input. Takes the
    /// sub-plan by mutable reference, to reach the VM's regfile and owned-cursor
    /// state.
    fn execute_sub_plan_multi(sub: &mut SubPlan, inputs: impl IntoIterator<Item = (u16, Batch)>) -> Option<Batch> {
        let SubPlan { vm, out_reg, .. } = sub;
        vm.compact_owned_traces();
        vm.bind_trace_cursors();
        vm::execute_epoch_multi(&vm.program, &mut vm.regfile, inputs, *out_reg)
    }

    /// True ⇒ the caller must return `None`: an empty epoch this program cannot
    /// emit from. Skips the whole VM pass — cursor refresh, compaction checks,
    /// dispatch — for the empty placeholders multi-worker lockstep fans to every
    /// dependent edge every tick; the `clear_deltas` releases the previous real
    /// epoch's batches, which the skipped epoch-start clear would have.
    ///
    /// The latch is cleared BEFORE the program runs: a global-ground reduce mints
    /// its V₀ row at most once, and after this pass `trace_out` holds it whether
    /// this epoch wrote it or a previous one did.
    fn latch_empty_epoch(sub: &mut SubPlan, input_is_empty: bool) -> bool {
        if !input_is_empty {
            return false;
        }
        if !sub.can_emit_on_empty.get() {
            sub.vm.clear_deltas();
            return true;
        }
        sub.can_emit_on_empty.set(false);
        false
    }

    /// Single-input sub-pipeline epoch. `source_id > 0` selects the input
    /// register from the sub-plan's `source_reg_map`; pass `0` when the
    /// sub-plan has a single unambiguous input.
    fn execute_sub_plan(sub: &mut SubPlan, input: Batch, source_id: i64) -> Option<Batch> {
        if Self::latch_empty_epoch(sub, input.count == 0) {
            return None;
        }
        let in_reg = if source_id > 0 {
            sub.source_reg_map.get(&source_id).copied().unwrap_or(sub.in_reg)
        } else {
            sub.in_reg
        };
        Self::execute_sub_plan_multi(sub, std::iter::once((in_reg, input)))
    }

    /// Sort+weight-merge a post-exchange batch for the post phase's merge-walk
    /// join/distinct operators (the mandatory consolidation). Every relay leg
    /// stamps its layout claim truthfully, so `into_consolidated` can trust the
    /// flags: the scatter/repartition/broadcast ops certify only what they can
    /// prove (a multi-worker concatenation ships `Raw`), the wire encoders
    /// derive the `FLAG_BATCH_*` bits from the batch's own layout
    /// (`WireMsg::encode_impl`) and the decode re-certifies the claim against
    /// the data (debug-verified), and the identity/skip-exchange legs pass the
    /// VM's own claims through under the same `exchange_schema` descriptor they
    /// were certified with. A `Raw` claim is re-sorted here; a verified
    /// Sorted/Consolidated claim folds or passes through.
    fn consolidate_exchanged(batch: Batch, schema: &SchemaDescriptor) -> Batch {
        batch.into_consolidated(schema)
    }

    /// Stamp a delta headed for the exchange wire with its source table's schema
    /// when it carries none: a row-bearing batch with a `None` schema emits
    /// `FLAG_HAS_DATA` without `FLAG_HAS_SCHEMA` and panics the reactor decode.
    fn ensure_wire_schema(&self, mut input: Batch, src_id: i64) -> Batch {
        if input.schema.is_none() {
            if let Some(entry) = self.tables.get(&src_id) {
                input.set_schema(entry.schema);
            }
        }
        input
    }

    // ── Multi-worker dispatch ───────────────────────────────────────────

    /// Run one multi-worker DAG step: ensure the view's circuit is compiled,
    /// then dispatch on the compiled shape + routing annotations and run the
    /// view's epoch, returning the output delta (`None` when a phase produced
    /// nothing). The arms, in priority order:
    /// 1. Replicated-output intercept (the view's own schema bit, stamped at
    ///    registration from its source set): the view computes its full result
    ///    locally — no exchange IPC at all.
    /// 2. Range join — relay the source delta (eq-prefix scatter for a band
    ///    join, broadcast for a pure range join, decided master-side in
    ///    `prepare_relay`), then the exchanged pipeline. Checked before the
    ///    shape match: a range join is `Exchanged` too (its output
    ///    ExchangeShard) but needs its *input* relayed as well, and its output
    ///    exchange is unconditional (no skip — a pure range probe needs the
    ///    full delta even when the join key equals the source PK).
    /// 3. Two-sided set-op — each side scattered by its hash PK, then combined.
    /// 4. Unary exchange — the pipeline on the local delta, eliding the output
    ///    IPC when the shuffle is a proven no-op (`skips_exchange`).
    /// 5. Single + join-scatter source — scatter the delta by the join-shard
    ///    cols before the (single) pipeline, unless the source is already
    ///    co-partitioned on them. Per-(view, source), so it stays a per-source
    ///    test inside the arm.
    /// 6. Single — one-phase execute.
    fn execute_multi_worker_step<E: ExchangeCallback>(
        &mut self,
        view_id: i64,
        input: Batch,
        src_id: i64,
        exchange: &mut E,
    ) -> Option<Batch> {
        if !self.ensure_compiled(view_id) {
            gnitz_warn!("dag: execute_multi_worker_step — no plan for view_id={}", view_id);
            return None;
        }

        // Arm 1. A view stamped replicated holds every source in full and receives
        // the full (broadcast) delta on every worker, so it computes its entire
        // result locally and the worker-0 scan reads it whole. Every worker
        // evaluates this identically, so they skip the same exchange rounds and the
        // collective barrier stays balanced.
        if self.relation_is_replicated(view_id) {
            return self.execute_epoch(view_id, input, src_id);
        }

        // Routing comes off the one memoized `ViewMeta` the master relay also
        // reads; the plan supplies only its executable shape. Taken before the
        // plan borrow so the `&mut self` memo lookup and the `&self` cache read
        // do not overlap.
        let meta = self.view_meta(view_id);
        let plan = self.cache.get(&view_id).unwrap();
        let is_range_join = meta.range_join_n_eq.is_some();
        let sides = match &plan.shape {
            PlanShape::Exchanged { sides, .. } => sides.len(),
            PlanShape::Single(_) => 0,
        };
        let skip_output_exchange = sides == 1 && !is_range_join && meta.skips_exchange;
        let join_scatter = sides == 0 && meta.scatter_sources.contains(&src_id);

        if is_range_join {
            // Arm 2 — relay the input delta first, then the exchanged pipeline.
            let input = self.ensure_wire_schema(input, src_id);
            let bc = exchange.do_exchange(view_id, &input, src_id);
            self.run_view_epoch(view_id, bc, src_id, |pre, key| exchange.do_exchange(view_id, &pre, key))
        } else if sides > 0 {
            // Arms 3 + 4 — the exchanged pipeline; the relay elides the IPC when
            // the unary output shuffle is a proven no-op.
            self.run_view_epoch(view_id, input, src_id, |pre, key| {
                if skip_output_exchange {
                    pre
                } else {
                    exchange.do_exchange(view_id, &pre, key)
                }
            })
        } else if join_scatter {
            // Arm 5 — scatter the delta by the join-shard cols before the pipeline.
            let input = self.ensure_wire_schema(input, src_id);
            let exchanged = exchange.do_exchange(view_id, &input, src_id);
            self.execute_epoch(view_id, exchanged, src_id)
        } else {
            // Arm 6 — single-phase execute.
            self.execute_epoch(view_id, input, src_id)
        }
    }

    /// Drive ONE view's epoch for a distributed-backfill chunk and ingest its
    /// output into the view's family. Returns true iff the view produced rows
    /// (the caller flushes the view once after the final chunk).
    ///
    /// This is the **view-scoped** analogue of `evaluate_dag_multi_worker`,
    /// which drives `source_id`'s *whole* dependent closure. A live CREATE VIEW
    /// must drive only the new view: the source already has populated existing
    /// dependents that a closure re-drive would double-count. Boot has no such
    /// dependents (every view starts empty), so it keeps the closure driver.
    /// The new view has no dependents of its own yet, so there is nothing to
    /// fan downstream — just run its step and ingest.
    pub fn backfill_view_step_multi_worker<E: ExchangeCallback>(
        &mut self,
        view_id: i64,
        source_id: i64,
        delta: Batch,
        exchange: &mut E,
    ) -> bool {
        if !self.tables.contains_key(&view_id) {
            return false;
        }
        // A backfilled view must be ephemeral: a durable one loads its shards
        // from its manifest at open, which would double-count against the deltas
        // ingested below.
        debug_assert!(
            self.tables
                .get(&view_id)
                .is_none_or(|e| e.kind.recovery_source() != Some(RecoverySource::SalReplay)),
            "distributed backfill into durable relation {view_id}: \
             would double-count loaded shards",
        );
        match self.execute_multi_worker_step(view_id, delta, source_id, exchange) {
            Some(out) if out.count > 0 => {
                self.ingest_returning_effective(view_id, out);
                true
            }
            _ => false,
        }
    }

    // ── DAG traversal driver ────────────────────────────────────────────

    /// Multi-worker DAG evaluation with exchange IPC. Seeds the pending queue
    /// from `source_id`'s direct dependents, then repeatedly pops the shallowest
    /// pending edge, runs its view's multi-worker step, ingests the output, and
    /// fans that output — or, for a view that produced nothing, an empty
    /// placeholder so collective exchange rounds stay in lockstep across workers
    /// — onto each downstream edge, until the queue drains. Every modified view's
    /// output store is flushed exactly once after the DAG settles.
    ///
    /// `tick_round` is the strictly-increasing round the master allocated for the
    /// tick group that drove this evaluation. It stamps every fed view's captured
    /// delta, and is what makes "give me what changed since N" answerable.
    pub fn evaluate_dag_multi_worker<E: ExchangeCallback>(
        &mut self,
        source_id: i64,
        delta: Batch,
        tick_round: u64,
        exchange: &mut E,
    ) {
        self.get_dep_map();
        let Some(view_ids) = self.dep.forward.get(&source_id).filter(|v| !v.is_empty()) else {
            return;
        };

        let mut pending = self.build_pending(view_ids, source_id, delta);
        let mut dirty_views: FxHashSet<i64> = FxHashSet::default();
        let mut popped_depth = i32::MIN;

        while let Some(((depth, view_id, src_id), input)) = pending.pop_first() {
            // Registration stamps `depth = max(source depth) + 1`, so depth
            // strictly increases along every edge: a producer at depth d fans only
            // onto depth > d, and no edge can re-enter a depth already popped.
            // That is what makes ordering by depth alone a valid schedule.
            debug_assert!(
                depth >= popped_depth,
                "pending popped depth {depth} after {popped_depth}"
            );
            popped_depth = depth;

            // The table may have been dropped between queueing and now.
            if !self.tables.contains_key(&view_id) {
                continue;
            }

            let out_delta = self
                .execute_multi_worker_step(view_id, input, src_id, exchange)
                .filter(|b| b.count > 0);

            if let Some(out) = out_delta.as_ref() {
                dirty_views.insert(view_id);
                self.ingest_view_delta(view_id, out, tick_round);
            }

            // Fan the output onto each dependent edge. Both borrows are shared
            // and disjoint from each other; `map_or` yields an empty slice for a
            // terminal view, which `queue_dependents` no-ops on.
            let src_schema = self.tables[&view_id].schema;
            let dep_view_ids = self.dep.forward.get(&view_id).map_or(&[][..], Vec::as_slice);
            Self::queue_dependents(&mut pending, &self.tables, dep_view_ids, view_id, src_schema, out_delta);
        }

        for vid in dirty_views {
            self.flush_view_or_abort(vid);
        }
    }

    /// Seed the pending queue from `source_id`'s direct dependents. The last live
    /// one takes ownership of `delta`; the rest get clones.
    fn build_pending(&self, view_ids: &[i64], source_id: i64, delta: Batch) -> Pending {
        let mut pending = Pending::new();
        let Some(last_idx) = view_ids.iter().rposition(|&vid| self.tables.contains_key(&vid)) else {
            return pending;
        };
        let mut delta = Some(delta);
        for (i, &vid) in view_ids.iter().enumerate() {
            let Some(depth) = self.tables.get(&vid).map(|e| e.depth) else {
                continue;
            };
            let batch = if i == last_idx {
                delta.take().unwrap()
            } else {
                delta.as_ref().unwrap().clone_batch()
            };
            pending.insert((depth, vid, source_id), batch);
        }
        pending
    }

    /// Queue `view_id`'s output onto each dependent's pending edge.
    ///
    /// `delta` is the producer's output, or `None` when it fired with none —
    /// empty placeholders are still queued so exchange-dependent views run and
    /// collective rounds stay in lockstep.
    ///
    /// Every queued batch is labelled with `src_schema` — the PRODUCER's output
    /// schema, never the consumer's. A JOIN consumer's combine-widened final
    /// schema is a different width than the operand batch on this edge; tagging
    /// the operand with it would trip the vm seed guard.
    fn queue_dependents(
        pending: &mut Pending,
        tables: &FxHashMap<i64, TableEntry>,
        dep_view_ids: &[i64],
        view_id: i64,
        src_schema: SchemaDescriptor,
        mut delta: Option<Batch>,
    ) {
        let depth_of = |dep_id: i64| tables.get(&dep_id).map(|e| e.depth);
        // A dependent already holding rows takes a merge; one holding an empty
        // placeholder takes a fill, because `op_union` against an empty operand
        // clones the other one whole — the copy this split exists to avoid.
        let takes_fill = |pending: &Pending, dep_id: i64, depth: i32| {
            pending.get(&(depth, dep_id, view_id)).is_none_or(|b| b.count == 0)
        };

        // Merges first, so the fill pass below can MOVE the producer's batch into
        // the last dependent that needs one instead of cloning it for every one.
        if let Some(d) = delta.as_ref() {
            for &dep_id in dep_view_ids {
                let Some(depth) = depth_of(dep_id) else { continue };
                let Some(slot) = pending.get_mut(&(depth, dep_id, view_id)) else {
                    continue;
                };
                if slot.count == 0 {
                    continue;
                }
                let existing = slot.take();
                let schema = existing.schema.unwrap_or(src_schema);
                *slot = ops::op_union(existing, d, &schema);
            }
        }

        let Some(last_fill) = dep_view_ids
            .iter()
            .rposition(|&dep_id| depth_of(dep_id).is_some_and(|d| takes_fill(pending, dep_id, d)))
        else {
            return;
        };
        for (i, &dep_id) in dep_view_ids.iter().enumerate() {
            let Some(depth) = depth_of(dep_id) else { continue };
            if !takes_fill(pending, dep_id, depth) {
                continue;
            }
            let batch = match delta.as_ref() {
                None => Batch::empty_with_schema(&src_schema),
                Some(_) if i == last_fill => delta.take().expect("moved at most once"),
                Some(d) => d.clone_batch(),
            };
            pending.insert((depth, dep_id, view_id), batch);
        }
    }
}
