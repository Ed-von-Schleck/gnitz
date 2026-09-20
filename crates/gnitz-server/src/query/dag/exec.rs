//! Epoch execution: the one pre → relay → consolidate → post pipeline every
//! compiled shape runs through, and the DAG evaluation driver.

use super::*;
use crate::query::compiler::{Side, Sides, OUTPUT_RELAY};
use gnitz_store::relation::Relation;
use gnitz_store::storage::StorageError;

/// One edge of a tick's schedule: `producer`'s output feeds `view`. Field order
/// is the sort order, and sorting puts a view after every view it reads, because
/// ids ascend along scan edges.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Step {
    view: i64,
    producer: i64,
}

/// What one drive runs: a tick of `source`'s whole dependent closure, stamping
/// fed views' deltas with `round`; or one backfill chunk of `source` into `view`
/// alone, which captures no delta.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Drive {
    Tick { source: i64, round: u64 },
    Backfill { view: i64, source: i64 },
}

/// The exchange transport of one view's epoch.
struct Relay<'a> {
    exchange: &'a mut dyn ExchangeCallback,
    view_id: i64,
    /// This view's shuffle is a proven no-op, so a round hands its batch back.
    elide: bool,
    /// This epoch's source is replicated and another worker holds its counted
    /// copy, so that worker sends the single-sourced rounds.
    muted: bool,
}

impl<'a> Relay<'a> {
    fn new(
        exchange: &'a mut dyn ExchangeCallback,
        registry: &RelationRegistry,
        view_id: i64,
        src_id: i64,
        elide: bool,
    ) -> Relay<'a> {
        let muted = registry.slot().rank != Placement::REPLICA_OWNER
            && registry.relation(src_id).is_some_and(Relation::is_replicated);
        Relay { exchange, view_id, elide, muted }
    }

    /// One repartition round of a side's output. Run even for an empty batch, so
    /// the collective rounds stay balanced across workers.
    fn round(&mut self, batch: Batch, emits_replica: bool) -> Batch {
        match self.elide {
            true => batch,
            false => self.send(batch, OUTPUT_RELAY, emits_replica),
        }
    }

    /// Publish `batch` under `key` and take back what this worker owns. A muted
    /// round publishes an empty batch instead: under `replica` every worker holds
    /// the same rows, so sending them all would relay each one `W` times.
    fn send(&mut self, batch: Batch, key: i64, replica: bool) -> Batch {
        if self.muted && replica {
            let empty = Batch::empty_with_schema(batch.schema());
            drop(batch);
            return self.exchange.do_exchange(self.view_id, empty, key);
        }
        self.exchange.do_exchange(self.view_id, batch, key)
    }
}

impl DagEngine {
    // ── Epoch execution ─────────────────────────────────────────────────

    /// Run one view's epoch: compile it if it is not memoized, route the delta
    /// through the exchange where the view's routing metadata says it must, then
    /// execute the compiled plan.
    fn run_view_epoch(
        &mut self,
        registry: &RelationRegistry,
        view_id: i64,
        input: Batch,
        src_id: i64,
        exchange: &mut dyn ExchangeCallback,
    ) -> Result<Batch, String> {
        let (meta, plan) = self.ensure_compiled(registry, view_id)?;
        let elide = plan.self_contained || meta.skips_exchange;
        let mut relay = Relay::new(exchange, registry, view_id, src_id, elide);
        let input = match !plan.self_contained && meta.source_route(src_id).is_some() {
            true => relay.send(input, src_id, true),
            false => input,
        };
        Self::run_plan(plan, input, src_id, &mut relay).map_err(|e| e.to_string())
    }

    /// Seed every phase of a compiled plan and run its post combine.
    ///
    /// Which sides are active derives from the plan and `src_id`, both
    /// worker-identical — so the collective relay rounds stay balanced.
    fn run_plan(
        plan: &mut CompileOutput,
        input: Batch,
        src_id: i64,
        relay: &mut Relay<'_>,
    ) -> Result<Batch, StorageError> {
        let CompileOutput { sides, post, .. } = plan;
        // Each arm hands `execute_epoch_multi` a fixed-size array: the seed count
        // is the side count, known here, so no arm heap-allocates to carry it.
        match sides {
            Sides::Unexchanged { .. } => {
                let seed = sub_seed(post, input, src_id);
                vm::execute_epoch_multi(&mut post.vm, [seed])
            }
            Sides::Unary(side) => {
                let seed = Self::run_side(side, Some(input), src_id, relay)?;
                vm::execute_epoch_multi(&mut post.vm, [seed])
            }
            Sides::Pair([a, b]) => {
                // `a UNION a` scans the source on both sides, so both take the delta.
                let takes = |s: &Side| s.plan.source_reg_map.contains_key(&src_id);
                let (da, db) = match (takes(a), takes(b)) {
                    (true, true) => (Some(input.clone_batch()), Some(input)),
                    (true, false) => (Some(input), None),
                    (false, true) => (None, Some(input)),
                    (false, false) => (None, None),
                };
                let seeds = [
                    Self::run_side(a, da, src_id, relay)?,
                    Self::run_side(b, db, src_id, relay)?,
                ];
                vm::execute_epoch_multi(&mut post.vm, seeds)
            }
        }
    }

    /// One side's seed for the post phase, `None` where the delta does not reach
    /// it. The consolidate hands the post phase one folded seed per side, whether
    /// or not the exchange ran.
    fn run_side(
        side: &mut Side,
        delta: Option<Batch>,
        src_id: i64,
        relay: &mut Relay<'_>,
    ) -> Result<(vm::DeltaReg, Batch), StorageError> {
        // The pre-exchange schema, never the view's combine-widened one.
        let schema = *side.plan.vm.program.out_schema();
        let Some(delta) = delta else {
            return Ok((side.seed_reg, Batch::empty_with_schema(&schema)));
        };
        let seed = sub_seed(&side.plan, delta, src_id);
        let pre = vm::execute_epoch_multi(&mut side.plan.vm, [seed])?;
        Ok((
            side.seed_reg,
            relay.round(pre, side.emits_replica).into_consolidated(&schema),
        ))
    }

    // ── DAG traversal driver ────────────────────────────────────────────

    /// One [`Step`] per dependency edge out of `source_id`'s forward closure, in
    /// execution order. A pure function of the dep map, which is
    /// worker-identical — what keeps the workers in lockstep.
    fn tick_schedule(&self, source_id: i64) -> Vec<Step> {
        let mut schedule: Vec<Step> = Vec::new();
        for producer in std::iter::once(source_id).chain(self.dependent_closure(vec![source_id])) {
            for &view in self.dependents_of(producer) {
                schedule.push(Step { view, producer });
            }
        }
        schedule.sort_unstable();
        schedule
    }

    /// Run `what` over `delta` and ingest every view's output into its family.
    pub(crate) fn drive(
        &mut self,
        registry: &mut RelationRegistry,
        what: Drive,
        delta: Batch,
        exchange: &mut dyn ExchangeCallback,
    ) -> Result<(), String> {
        let (source, schedule, round) = match what {
            Drive::Tick { source, round } => (source, self.tick_schedule(source), Some(round)),
            Drive::Backfill { view, source } => (source, vec![Step { view, producer: source }], None),
        };
        // How many steps still read each producer's output.
        let mut readers: FxHashMap<i64, usize> = FxHashMap::default();
        for step in &schedule {
            *readers.entry(step.producer).or_default() += 1;
        }
        let mut outputs: FxHashMap<i64, Batch> = FxHashMap::default();
        outputs.insert(source, delta);
        for step in &schedule {
            let left = readers.get_mut(&step.producer).expect("counted above");
            *left -= 1;
            // The last reader takes the batch; earlier ones copy as they run, so at
            // most two copies are live.
            let input = match *left {
                0 => outputs.remove(&step.producer),
                _ => outputs.get(&step.producer).map(Batch::clone_batch),
            }
            .expect("the schedule runs every producer before the steps it feeds");
            let needed = readers.contains_key(&step.view);
            let out = self.run_view_epoch(registry, step.view, input, step.producer, exchange)?;
            let echo = registry.ingest_view_delta(step.view, out, round, needed)?;
            // A view with two producers runs one epoch per producer, and its
            // readers see the union. An empty output is kept too, so a consumer's
            // exchange rounds run on every worker.
            if let Some(out) = echo {
                let merged = match outputs.remove(&step.view) {
                    Some(held) if !held.is_empty() => {
                        // The held batch's schema, not the incoming one: it selects
                        // `payload_cmp` and is what the union's result is certified
                        // under.
                        let schema = *held.schema();
                        ops::op_union(held, &out, &schema)
                    }
                    _ => out,
                };
                outputs.insert(step.view, merged);
            }
        }
        Ok(())
    }

    /// End `view_id`'s backfill.
    ///
    /// Releases the last chunk's pinned registers and trace cursors, then folds
    /// the view's memtable into its RAM tier once — so the view's first reads open
    /// over fewer sources, at one fold per backfill rather than one per chunk.
    pub(crate) fn finish_backfill(&mut self, registry: &mut RelationRegistry, view_id: i64) -> Result<(), String> {
        if let Some(plan) = self.views.get_mut(&view_id).and_then(|e| e.plan.as_mut()) {
            for sub in plan.sub_plans_mut() {
                sub.vm.release();
            }
        }
        registry.fold_to_ram(view_id).map_err(|e| e.to_string())
    }
}

/// The `(register, batch)` seeding a sub-plan with `source_id`'s delta.
fn sub_seed(sub: &SubPlan, input: Batch, source_id: i64) -> (vm::DeltaReg, Batch) {
    let reg = sub.source_reg_map.get(&source_id).copied();
    (
        reg.expect("the dep map names only sources the view's circuit scans"),
        input,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;
