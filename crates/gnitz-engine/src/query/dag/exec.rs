//! Epoch execution: the one pre → relay → consolidate → post pipeline every
//! compiled shape runs through, and the DAG evaluation driver.

use super::*;
use crate::query::compiler::{ExtCursorScratch, PlanShape};

pub(super) struct PendingEntry {
    pub depth: i32,
    pub view_id: i64,
    pub source_id: i64,
    pub batch: Batch,
}

impl DagEngine {
    // ── Epoch execution ─────────────────────────────────────────────────

    /// Execute one view's epoch with no exchange IPC — the single-worker and
    /// backfill path, where one owner sees every shard (relay = identity).
    pub fn execute_epoch(&mut self, view_id: i64, input: Batch, source_id: i64) -> Option<Batch> {
        self.run_view_epoch(view_id, input, source_id, |pre, _| pre)
    }

    /// Execute one view epoch through its compiled shape.
    ///
    /// * `Single`: one sub-pipeline, run directly.
    /// * `Exchanged`: route the delta to the side(s) scanning its source, run
    ///   each side, hand its output through `relay`, consolidate, and seed the
    ///   post combine with every side's batch.
    ///
    /// `relay(pre, key)` is the repartition step: multi-worker passes the
    /// exchange IPC (`key` is the side's round key — a two-sided set-op keys by
    /// the side's source so the two rounds don't collide in the master
    /// accumulator; a unary side keys 0), single-worker passes identity (one
    /// worker owns all shards). A side that takes the delta always runs its
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
            PlanShape::Single(sub) => Self::execute_sub_plan(view_id, sub, &self.tables, input, src_id),
            PlanShape::Exchanged { sides, post } => {
                // A unary side takes every delta; a set-op side takes it iff it
                // scans the delta's source (`a UNION a` — both sides scan one
                // relation — clones so each side gets it).
                let unary = sides.len() == 1;
                debug_assert!(
                    unary || sides.iter().any(|s| s.source_id == src_id),
                    "run_view_epoch view {view_id}: delta source {src_id} matches no side",
                );
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
                        let mut pre = Self::execute_sub_plan(view_id, &mut side.plan, &self.tables, delta, src_id)
                            .unwrap_or_else(|| Batch::empty_with_schema(&schema));
                        // Label with the side's pre-exchange output schema for
                        // the wire encode (never the view's combine-widened
                        // final schema).
                        pre.set_schema(schema);
                        let relay_key = if unary { 0 } else { side.source_id };
                        Self::consolidate_exchanged(relay(pre, relay_key), &schema)
                    } else {
                        Batch::empty_with_schema(&schema)
                    };
                    seeds.push((side.seed_reg, consolidated));
                }
                // Same empty-epoch skip as `execute_sub_plan`: a pad round
                // whose every side produced nothing runs the post VM only if
                // it can emit from empty input (global-ground reduce).
                if !post.can_emit_on_empty && seeds.iter().all(|(_, b)| b.count == 0) {
                    post.vm.clear_deltas();
                    return None;
                }
                Self::execute_sub_plan_multi(view_id, post, &self.tables, seeds)
            }
        }
    }

    /// Release the delta batches pinned in a view's compiled-plan regfiles.
    /// The VM only clears deltas at the *start* of an epoch, so after a
    /// backfill the full scanned source dataset and intermediate deltas stay
    /// resident until the next evaluation. Call this after backfill to free
    /// them immediately.
    pub fn clear_view_regfile_deltas(&mut self, view_id: i64) {
        if let Some(plan) = self.cache.get_mut(&view_id) {
            for sub in plan.sub_plans_mut() {
                sub.vm.clear_deltas();
            }
        }
    }

    /// Distributed-backfill analogue of `backfill_view`'s post-loop
    /// `clear_view_regfile_deltas` (catalog/ddl.rs). After a worker's
    /// `handle_backfill(source_id)` loop, the last chunk's input + intermediate
    /// delta registers stay pinned in the touched views' regfiles. Release them
    /// across `source_id`'s dependent closure, so peak resident memory falls
    /// back to ~O(chunk) once the backfill drains.
    ///
    /// Also carries `backfill_view`'s Ephemeral guard: every view backfilled this
    /// way must be ephemeral, else its manifest-loaded shards would double-count
    /// against the deltas the backfill ingests.
    pub fn clear_regfile_deltas_from_source(&mut self, source_id: i64) {
        for view_id in self.dependent_closure(vec![source_id]) {
            debug_assert!(
                self.tables
                    .get(&view_id)
                    .is_none_or(|e| e.kind.recovery_source() != RecoverySource::SalReplay),
                "distributed backfill into durable relation {view_id}: \
                 would double-count loaded shards",
            );
            self.clear_view_regfile_deltas(view_id);
        }
    }

    /// Execute one sub-pipeline epoch, seeding one register per input. Takes
    /// the sub-plan by mutable reference (to reach the VM's regfile and
    /// owned-cursor state) and the engine's table map by shared reference (for
    /// external-trace cursor construction); both come from different fields of
    /// `DagEngine`, so callers hold them as independent borrows.
    fn execute_sub_plan_multi(
        view_id: i64,
        sub: &mut SubPlan,
        tables: &FxHashMap<i64, TableEntry>,
        inputs: impl IntoIterator<Item = (u16, Batch)>,
    ) -> Option<Batch> {
        let SubPlan {
            vm,
            out_reg,
            ext_trace_regs,
            ext_cursors,
            ..
        } = sub;
        Self::fill_ext_cursors(tables, ext_trace_regs, vm.program.reg_meta.len(), ext_cursors);
        vm.refresh_owned_cursors();
        let r = vm::execute_epoch_multi(&vm.program, &mut vm.regfile, inputs, *out_reg, &ext_cursors.ptrs);
        // Drop the external cursors at epoch end (buffer capacity is retained):
        // holding them across ticks would pin memtable snapshots and shard
        // mmaps of the scanned base tables. The registers' cursor pointers are
        // rebound from fresh handles at the next epoch's start, before any deref.
        ext_cursors.cursors.clear();
        ext_cursors.ptrs.clear();
        Self::vm_epoch_result(view_id, r)
    }

    /// Single-input sub-pipeline epoch. `source_id > 0` selects the input
    /// register from the sub-plan's `source_reg_map`; pass `0` when the
    /// sub-plan has a single unambiguous input.
    fn execute_sub_plan(
        view_id: i64,
        sub: &mut SubPlan,
        tables: &FxHashMap<i64, TableEntry>,
        input: Batch,
        source_id: i64,
    ) -> Option<Batch> {
        // Empty placeholder epoch (multi-worker lockstep fans one to every
        // dependent edge every tick): unless the program can emit from an
        // empty input, skip the whole VM pass — cursor refresh + compaction
        // checks + dispatch. Clearing the deltas releases the previous real
        // epoch's batches, which the skipped epoch-start clear would have.
        if input.count == 0 && !sub.can_emit_on_empty {
            sub.vm.clear_deltas();
            return None;
        }
        let in_reg = if source_id > 0 {
            sub.source_reg_map.get(&source_id).copied().unwrap_or(sub.in_reg)
        } else {
            sub.in_reg
        };
        Self::execute_sub_plan_multi(view_id, sub, tables, std::iter::once((in_reg, input)))
    }

    /// Fill the reusable ext-cursor buffers: one fresh cursor per external
    /// trace register, its raw pointer indexed by register ID in `ptrs`.
    fn fill_ext_cursors(
        tables: &FxHashMap<i64, TableEntry>,
        ext_trace_regs: &[(u16, i64)],
        num_regs: usize,
        scratch: &mut ExtCursorScratch,
    ) {
        debug_assert!(scratch.ptrs.is_empty() && scratch.cursors.is_empty());
        scratch.ptrs.resize(num_regs, std::ptr::null_mut());
        for &(reg_id, table_id) in ext_trace_regs {
            if let Some(entry) = tables.get(&table_id) {
                // External-trace reads are the operator-state read path:
                // compact first so L0 on intermediate/trace tables stays
                // bounded (no background compactor yet). A compaction Err
                // leaves the shard index unchanged; the cursor still opens
                // on a consistent snapshot.
                let _ = entry.handle.compact_if_needed();
                scratch.cursors.push(Box::new(entry.handle.open_cursor()));
                if (reg_id as usize) < num_regs {
                    // Derive the raw pointer AFTER the move, from the box's
                    // stable heap address inside the scratch. Deriving it from
                    // a local Box and then moving the box would invalidate it
                    // under Stacked/Tree Borrows. The ReadCursor heap
                    // allocation is stable across later Vec growth, so the
                    // pointer remains valid.
                    let p = scratch.cursors.last_mut().unwrap().as_mut() as *mut ReadCursor;
                    scratch.ptrs[reg_id as usize] = p;
                }
            }
        }
    }

    /// Sort+weight-merge a post-exchange batch for the post phase's merge-walk
    /// join/distinct operators (the mandatory consolidation). Every relay leg
    /// stamps its layout claim truthfully, so `into_consolidated` can trust the
    /// flags: the scatter/repartition/broadcast ops certify only what they can
    /// prove (a multi-worker concatenation ships `Raw`), the wire encoders
    /// derive the `FLAG_BATCH_*` bits from the batch's own layout
    /// (`encode_wire_into_impl`) and the decode re-certifies the claim against
    /// the data (debug-verified), and the identity/skip-exchange legs pass the
    /// VM's own claims through under the same `exchange_schema` descriptor they
    /// were certified with. A `Raw` claim is re-sorted here; a verified
    /// Sorted/Consolidated claim folds or passes through.
    fn consolidate_exchanged(batch: Batch, schema: &SchemaDescriptor) -> Batch {
        batch.into_consolidated(schema)
    }

    /// Normalize a VM epoch result into the DAG's `Option<Batch>` convention:
    /// a positive-count batch, or `None` for an empty epoch.
    ///
    /// A VM `Err` is an unrecoverable internal-invariant violation — every
    /// `VmError` means a Reduce trace cursor the compiler promised is unbound,
    /// i.e. the compiled circuit is malformed. Every VM operator is otherwise
    /// infallible, so there is no data- or query-level fault to surface.
    /// Continuing would drop the delta and permanently desync the view's
    /// integral (and in multi-worker, a skipped exchange round deadlocks the
    /// cluster), so we fail stop. In multi-worker the master's `watchdog` reaps
    /// the exited worker and tears the tick down; single-worker exits the
    /// process. If a recoverable (data/query-level) VM error is ever
    /// introduced, it must be a distinct variant routed to transaction-level
    /// failure — never funneled here.
    pub(super) fn vm_epoch_result(view_id: i64, r: Result<Option<Batch>, vm::VmError>) -> Option<Batch> {
        match r {
            Ok(Some(batch)) if batch.count > 0 => Some(batch),
            Ok(_) => None,
            Err(e @ (vm::VmError::TraceOutCursorUnbound | vm::VmError::TraceInCursorUnbound)) => gnitz_fatal_abort!(
                "dag: VM execution error {:?} for view_id={} — malformed circuit, \
                 cannot continue without producing inconsistent view state",
                e,
                view_id,
            ),
        }
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
    /// 1. All-sources-replicated intercept (live table flags, never baked): the
    ///    view computes its full result locally — no exchange IPC at all.
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

        // Arm 1. A view whose sources are all replicated holds the full copy of
        // every source and receives the full (broadcast) delta on every worker,
        // so it computes its entire result locally: the output is itself
        // replicated and the worker-0 scan reads it whole. Every worker
        // evaluates this identically, so they skip the same exchange rounds and
        // the collective barrier stays balanced.
        if self.view_all_sources_replicated(view_id) {
            return self.execute_epoch(view_id, input, src_id);
        }

        let plan = self.cache.get(&view_id).unwrap();
        let is_range_join = plan.range_join_n_eq.is_some();
        let sides = match &plan.shape {
            PlanShape::Exchanged { sides, .. } => sides.len(),
            PlanShape::Single(_) => 0,
        };
        let skip_output_exchange = sides == 1 && !is_range_join && plan.skips_exchange;
        let join_scatter = sides == 0
            && plan.join_shard_map.get(&src_id).is_some_and(|c| !c.is_empty())
            && !plan.co_partitioned.contains(&src_id);

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
        match self.execute_multi_worker_step(view_id, delta, source_id, exchange) {
            Some(out) if out.count > 0 => {
                self.ingest_to_family(view_id, out);
                true
            }
            _ => false,
        }
    }

    // ── DAG traversal driver ────────────────────────────────────────────

    /// Multi-worker DAG evaluation with exchange IPC. Seeds the pending queue
    /// from `source_id`'s direct dependents, then repeatedly pops the
    /// shallowest pending edge, runs its view's multi-worker step, ingests the
    /// output, and fans that output — or, for a view that produced nothing, an
    /// empty placeholder so collective exchange rounds stay in lockstep across
    /// workers — onto each downstream edge, until the queue drains. Every
    /// modified view trace is flushed exactly once after the DAG settles.
    pub fn evaluate_dag_multi_worker<E: ExchangeCallback>(&mut self, source_id: i64, delta: Batch, exchange: &mut E) {
        self.get_dep_map();
        let view_ids: Vec<i64> = self.dep.forward.get(&source_id).cloned().unwrap_or_default();
        if view_ids.is_empty() {
            return;
        }

        let (mut pending, mut pending_pos) = self.build_pending(&view_ids, source_id, delta);
        let mut dirty_views: FxHashSet<i64> = FxHashSet::default();

        while let Some(entry) = pending.pop() {
            let view_id = entry.view_id;
            let src_id = entry.source_id;
            let input = entry.batch;
            pending_pos.remove(&(view_id, src_id));

            // The table may have been dropped between queueing and now.
            if !self.tables.contains_key(&view_id) {
                continue;
            }

            let out_delta = self.execute_multi_worker_step(view_id, input, src_id, exchange);
            let has_output = out_delta.as_ref().is_some_and(|b| b.count > 0);

            if has_output {
                dirty_views.insert(view_id);
                if self.dep.forward.get(&view_id).is_none_or(|d| d.is_empty()) {
                    // Terminal view: move the batch into its family (no clone for
                    // unique_pk) — there is nothing downstream to fan onto.
                    self.ingest_to_family(view_id, out_delta.unwrap());
                    continue;
                }
                self.ingest_by_ref(view_id, out_delta.as_ref().unwrap());
            }

            // Fan the output — or, for a view that produced nothing, an empty
            // placeholder — onto each dependent edge. Borrow the dep list (disjoint
            // from `&self.tables`) rather than cloning; `map_or` yields an empty
            // slice for a view with no dependents, which queue_dependents no-ops.
            let src_schema = self.tables[&view_id].schema;
            let delta = if has_output { out_delta.as_ref() } else { None };
            let dep_view_ids = self.dep.forward.get(&view_id).map_or(&[][..], Vec::as_slice);
            Self::queue_dependents(
                &mut pending,
                &mut pending_pos,
                &self.tables,
                dep_view_ids,
                view_id,
                src_schema,
                delta,
            );
        }

        // Flush each modified view trace exactly once after the full DAG settles.
        for vid in dirty_views {
            self.flush_view_or_abort(vid);
        }
    }

    /// Seed the initial pending list for a DAG traversal.
    /// Returns entries sorted descending by depth (shallowest at tail for
    /// O(1) pop) plus a position index for merge-on-collision lookups.
    fn build_pending(
        &self,
        view_ids: &[i64],
        source_id: i64,
        delta: Batch,
    ) -> (Vec<PendingEntry>, FxHashMap<(i64, i64), usize>) {
        let last_valid_idx = view_ids.iter().rposition(|&vid| self.tables.contains_key(&vid));
        let mut delta_opt = Some(delta);
        let mut pending: Vec<PendingEntry> = Vec::new();
        if let Some(last_idx) = last_valid_idx {
            for (i, &vid) in view_ids.iter().enumerate() {
                let depth = match self.tables.get(&vid) {
                    Some(e) => e.depth,
                    None => continue,
                };
                let b = if i == last_idx {
                    delta_opt.take().unwrap()
                } else {
                    delta_opt.as_ref().unwrap().clone_batch()
                };
                pending.push(PendingEntry {
                    depth,
                    view_id: vid,
                    source_id,
                    batch: b,
                });
            }
        }
        let mut pending_pos: FxHashMap<(i64, i64), usize> = FxHashMap::default();
        Self::resort_pending(&mut pending, &mut pending_pos);
        (pending, pending_pos)
    }

    /// Restore the pending queue's descending-depth order and rebuild the
    /// `(view_id, source_id) → index` lookup, after new entries were pushed.
    fn resort_pending(pending: &mut [PendingEntry], pending_pos: &mut FxHashMap<(i64, i64), usize>) {
        pending.sort_by_key(|n| std::cmp::Reverse(n.depth));
        pending_pos.clear();
        for (i, pe) in pending.iter().enumerate() {
            pending_pos.insert((pe.view_id, pe.source_id), i);
        }
    }

    /// Queue `view_id`'s output onto each dependent's pending edge.
    ///
    /// `delta` is the producer's output, or `None` when the producer fired with
    /// no output (empty placeholders are queued so exchange-dependent views
    /// still run and collective rounds stay in lockstep). When present it is
    /// merged into an existing pending entry, or cloned into a new one; when
    /// absent a new entry gets a zero-allocation empty placeholder and existing
    /// entries are left untouched.
    ///
    /// Every queued batch is labelled with `src_schema` — the PRODUCER's output
    /// schema, never the consumer's. A JOIN consumer's combine-widened final
    /// schema is a different width than the operand batch on this edge; tagging
    /// the operand with it would trip the vm seed guard. `src_schema` must be
    /// `self.tables[&view_id].schema`; `tables` is read only for dependents'
    /// depth, so the immutable borrow does not conflict with the snapshot.
    fn queue_dependents(
        pending: &mut Vec<PendingEntry>,
        pending_pos: &mut FxHashMap<(i64, i64), usize>,
        tables: &FxHashMap<i64, TableEntry>,
        dep_view_ids: &[i64],
        view_id: i64,
        src_schema: SchemaDescriptor,
        delta: Option<&Batch>,
    ) {
        let mut pushed = false;
        for &dep_id in dep_view_ids {
            let dep_depth = match tables.get(&dep_id) {
                Some(e) => e.depth,
                None => continue,
            };

            if let Some(&existing_idx) = pending_pos.get(&(dep_id, view_id)) {
                if let (true, Some(d)) = (existing_idx < pending.len(), delta) {
                    let existing = pending[existing_idx].batch.take();
                    let schema = existing.schema.unwrap_or(src_schema);
                    let merged = ops::op_union(existing, d, &schema);
                    pending[existing_idx].batch = merged;
                }
            } else {
                let batch = match delta {
                    Some(d) => d.clone_batch(),
                    None => Batch::empty_with_schema(&src_schema),
                };
                pending.push(PendingEntry {
                    depth: dep_depth,
                    view_id: dep_id,
                    source_id: view_id,
                    batch,
                });
                pushed = true;
            }
        }
        // New entries are not recorded in `pending_pos` here: the end-of-loop
        // `resort_pending` rebuilds it wholesale, and nothing reads a new entry's
        // slot before then — `dep_view_ids` is deduped (get_dep_map), so no later
        // iteration probes a `(dep_id, view_id)` key an earlier one pushed. The
        // pre-existing slots the merge branch *does* probe stay valid because
        // `pending` is only appended to here (the merge mutates a batch in place,
        // never moves an entry). One stable resort at the end restores descending
        // depth order while preserving per-depth insertion order. A resort is
        // needed iff at least one edge was pushed; pure merges change neither
        // membership nor depth.
        if pushed {
            Self::resort_pending(pending, pending_pos);
        }
    }
}
