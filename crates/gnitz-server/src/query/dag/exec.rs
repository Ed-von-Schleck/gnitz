//! Epoch execution: the one pre → relay → consolidate → post pipeline every
//! compiled shape runs through, and the DAG evaluation driver.

use super::*;
use crate::query::compiler::{Side, Sides, OUTPUT_RELAY};
use gnitz_store::storage::StoreError;

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

/// How one view's epoch reaches the other workers.
struct Relay {
    view_id: i64,
    /// This view's shuffle is a proven no-op, so a round hands its batch back.
    elide: bool,
    /// This epoch's source is replicated and another worker holds its counted
    /// copy, so that worker sends the single-sourced rounds.
    muted: bool,
}

impl Relay {
    /// The relay of `view_id`'s epoch over `src_id`'s delta.
    fn new(registry: &RelationRegistry, view_id: i64, src_id: i64, elide: bool) -> Relay {
        let muted = registry.slot().rank != Placement::REPLICA_OWNER
            && registry.relation(src_id).is_some_and(Relation::is_replicated);
        Relay { view_id, elide, muted }
    }

    /// One repartition round of a side's output. Run even for an empty batch, so
    /// the collective rounds stay balanced across workers.
    fn round(&self, host: &mut impl DriveHost, batch: Batch, emits_replica: bool) -> Batch {
        match self.elide {
            true => batch,
            false => self.send(host, batch, OUTPUT_RELAY, emits_replica),
        }
    }

    /// Publish `batch` under `key` and take back what this worker owns. A muted
    /// round publishes an empty batch instead: under `replica` every worker holds
    /// the same rows, so sending them all would relay each one `W` times.
    fn send(&self, host: &mut impl DriveHost, batch: Batch, key: i64, replica: bool) -> Batch {
        if self.muted && replica {
            let empty = Batch::empty_with_schema(batch.schema());
            drop(batch);
            return host.exchange(self.view_id, empty, key);
        }
        host.exchange(self.view_id, batch, key)
    }
}

/// `view_id`'s compiled plan, which its epoch compiled on entry.
fn plan_of(host: &mut impl DriveHost, view_id: i64) -> Result<&mut ViewPlan, StoreError> {
    host.parts()
        .0
        .plans
        .get_mut(&view_id)
        .ok_or_else(|| StoreError::rejected(format!("view {view_id}: its plan was dropped mid-epoch")))
}

// ── Epoch execution ─────────────────────────────────────────────────────

/// Run one view's epoch over `src_id`'s delta.
fn run_view_epoch(host: &mut impl DriveHost, view_id: i64, input: Batch, src_id: i64) -> Result<Batch, String> {
    let (relay, routed) = {
        let (dag, registry) = host.parts();
        let (meta, plan) = dag.ensure_compiled(registry, view_id)?;
        let relay = Relay::new(
            registry,
            view_id,
            src_id,
            plan.code.self_contained || meta.skips_exchange,
        );
        (relay, !plan.code.self_contained && meta.source_route(src_id).is_some())
    };
    let input = match routed {
        true => relay.send(host, input, src_id, true),
        false => input,
    };
    run_plan(host, &relay, input, src_id).map_err(|e| e.to_string())
}

/// Seed every phase of a compiled plan and run its post combine. The sides run
/// follow from the plan and `src_id` alone, so every worker relays alike.
fn run_plan(host: &mut impl DriveHost, relay: &Relay, input: Batch, src_id: i64) -> Result<Batch, StoreError> {
    enum Shape {
        Unexchanged,
        Unary,
        /// Whether each side scans the source.
        Pair(bool, bool),
    }
    let takes = |s: &Side| s.plan.source_reg_map.contains_key(&src_id);
    let shape = match &plan_of(host, relay.view_id)?.code.sides {
        Sides::Unexchanged { .. } => Shape::Unexchanged,
        Sides::Unary(_) => Shape::Unary,
        Sides::Pair([a, b]) => Shape::Pair(takes(a), takes(b)),
    };
    match shape {
        Shape::Unexchanged => {
            let seed = sub_seed(&plan_of(host, relay.view_id)?.code.post, input, src_id);
            run_post(host, relay.view_id, [seed])
        }
        Shape::Unary => {
            let seed = run_side(host, relay, 0, Some(input), src_id)?;
            run_post(host, relay.view_id, [seed])
        }
        Shape::Pair(ta, tb) => {
            // `a UNION a` scans the source on both sides, so both take the delta.
            let (da, db) = match (ta, tb) {
                (true, true) => (Some(input.clone_batch()), Some(input)),
                (true, false) => (Some(input), None),
                (false, true) => (None, Some(input)),
                (false, false) => (None, None),
            };
            let seeds = [
                run_side(host, relay, 0, da, src_id)?,
                run_side(host, relay, 1, db, src_id)?,
            ];
            run_post(host, relay.view_id, seeds)
        }
    }
}

/// The post phase over the seeds every side produced.
fn run_post<const N: usize>(
    host: &mut impl DriveHost,
    view_id: i64,
    seeds: [(vm::DeltaReg, Batch); N],
) -> Result<Batch, StoreError> {
    let ViewPlan { code, state } = plan_of(host, view_id)?;
    vm::execute_epoch_multi(&mut code.post.vm, state, seeds)
}

/// Side `i`'s seed for the post phase, folded: the relay delivers rows in an
/// order that varies with the worker count, and a float SUM follows it.
fn run_side(
    host: &mut impl DriveHost,
    relay: &Relay,
    i: usize,
    delta: Option<Batch>,
    src_id: i64,
) -> Result<(vm::DeltaReg, Batch), StoreError> {
    let (pre, seed_reg, emits_replica, schema) = {
        let ViewPlan { code, state } = plan_of(host, relay.view_id)?;
        let side = &mut code.sides.sides_mut()[i];
        // The pre-exchange schema, never the view's combine-widened one.
        let schema = *side.plan.vm.program.out_schema();
        let Some(delta) = delta else {
            return Ok((side.seed_reg, Batch::empty_with_schema(&schema)));
        };
        let seed = sub_seed(&side.plan, delta, src_id);
        let pre = vm::execute_epoch_multi(&mut side.plan.vm, state, [seed])?;
        (pre, side.seed_reg, side.emits_replica, schema)
    };
    Ok((
        seed_reg,
        relay.round(host, pre, emits_replica).into_consolidated(&schema),
    ))
}

// ── DAG traversal driver ────────────────────────────────────────────────

impl DagEngine {
    /// One [`Step`] per dependency edge out of `source_id`'s forward closure, in
    /// execution order, skipping non-resumable views: their backfill fills them.
    /// Worker-identical, which keeps the workers in lockstep.
    fn tick_schedule(&self, registry: &RelationRegistry, source_id: i64) -> Vec<Step> {
        let mut schedule: Vec<Step> = Vec::new();
        for producer in std::iter::once(source_id).chain(self.dependent_closure(vec![source_id])) {
            for &view in self.dependents_of(producer) {
                if !registry.is_non_resumable(view) {
                    schedule.push(Step { view, producer });
                }
            }
        }
        schedule.sort_unstable();
        schedule
    }

    /// End `view_id`'s backfill.
    ///
    /// Releases the last chunk's pinned registers and trace cursors, then folds
    /// the view's memtable into its RAM tier once — so the view's first reads open
    /// over fewer sources, at one fold per backfill rather than one per chunk.
    pub(crate) fn finish_backfill(&mut self, registry: &mut RelationRegistry, view_id: i64) -> Result<(), String> {
        if let Some(plan) = self.plans.get_mut(&view_id) {
            for sub in plan.code.sub_plans_mut() {
                sub.vm.release();
            }
        }
        registry.fold_to_ram(view_id).map_err(|e| e.to_string())
    }
}

/// Run `what` over `delta` and ingest every view's output into its family.
pub(crate) fn drive(host: &mut impl DriveHost, what: Drive, delta: Batch) -> Result<(), String> {
    let (source, schedule, round) = match what {
        Drive::Tick { source, round } => {
            let (dag, registry) = host.parts();
            (source, dag.tick_schedule(registry, source), Some(round))
        }
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
        let out = run_view_epoch(host, step.view, input, step.producer)?;
        let echo = host.parts().1.ingest_view_delta(step.view, out, round, needed)?;
        // Kept even when empty, so a reader's exchange rounds run on every worker.
        if let Some(out) = echo {
            let merged = match outputs.remove(&step.view) {
                Some(held) if !held.is_empty() => {
                    // The held batch's schema: the union is certified under it.
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
