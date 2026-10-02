//! Epoch execution: the one pre → relay → post pipeline every compiled shape
//! runs through, and the DAG evaluation driver.

use super::*;

/// One edge of a tick's schedule: `producer`'s output feeds `view`. Field order
/// is the sort order, and sorting puts a view after every view it reads, because
/// ids ascend along scan edges.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Step {
    view: u64,
    producer: u64,
}

/// What one drive runs: a tick of `source`'s whole dependent closure, stamping
/// fed views' deltas with `round`; or one backfill chunk of `source` into `view`
/// alone, which captures no delta.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Drive {
    Tick {
        source: u64,
        round: u64,
    },
    /// **View-scoped.** Drives `view` alone, never `source`'s whole dependent
    /// closure: the source may already have populated dependents (a live CREATE
    /// VIEW over a source with prior views, a boot rebuild beside resumed
    /// siblings) that a closure re-drive would double-count.
    Backfill {
        view: u64,
        source: u64,
    },
}

/// Hand `batch` to the workers that consume it.
fn relay(host: &mut impl DriveHost, view_id: u64, batch: Cow<'_, Batch>, how: Option<&Relay>, fold: bool) -> Batch {
    match how {
        None => batch.into_owned(),
        Some(Relay::Broadcast) => host.exchange(view_id, batch, &ScatterPlan::broadcast(), fold),
        Some(Relay::Round(p)) => host.exchange(view_id, batch, p, fold),
        Some(Relay::Share(p)) => {
            let slot = host.parts().1.slot();
            p.share(&batch, slot)
        }
    }
}

/// `view_id`'s compiled plan, which its epoch compiled on entry.
fn plan_of(host: &mut impl DriveHost, view_id: u64) -> &mut ViewPlan {
    host.parts().0.plan_mut(view_id).expect("compiled on the epoch's entry")
}

// ── Epoch execution ─────────────────────────────────────────────────────

/// `view_id`'s plan and state, beside what its epoch reads of its sources.
fn plan_and_reads<'a>(
    host: &'a mut impl DriveHost,
    view_id: u64,
    unfed: &'a [u64],
) -> (&'a mut ViewPlan, vm::SourceReads<'a>) {
    let (dag, registry) = host.parts();
    let DagEngine { views, .. } = dag;
    let plan = views
        .get_mut(&view_id)
        .and_then(|v| v.plan.as_mut())
        .expect("compiled on the epoch's entry");
    (plan, vm::SourceReads { registry, view: view_id, unfed })
}

/// Run one view's epoch over `src_id`'s delta. `unfed`: the sources the view
/// has been fed no row of.
fn run_view_epoch(
    host: &mut impl DriveHost,
    view_id: u64,
    input: Cow<'_, Batch>,
    src_id: u64,
    unfed: &[u64],
) -> Result<Batch, String> {
    let (route, fold) = {
        let (dag, registry) = host.parts();
        let (meta, plan) = ensure_compiled(&mut dag.views, registry, view_id)?;
        let code = &plan.code;
        let route = match code.self_contained {
            true => None,
            false => meta.source_route(src_id).cloned(),
        };
        let fold = match code.sides.is_empty() {
            true => code.post.seed_folds(src_id),
            false => code.sides.iter().any(|s| s.plan.seed_folds(src_id)),
        };
        (route, fold)
    };
    let input = relay(host, view_id, input, route.as_ref(), fold);
    run_plan(host, view_id, input, src_id, unfed)
}

/// Run every side that scans `src_id` over its delta, then the post combine.
fn run_plan(
    host: &mut impl DriveHost,
    view_id: u64,
    input: Batch,
    src_id: u64,
    unfed: &[u64],
) -> Result<Batch, String> {
    let code = &plan_of(host, view_id).code;
    if code.sides.is_empty() {
        let seed = sub_seed(&code.post, input, src_id);
        return run_post(host, view_id, [seed], unfed);
    }
    // `a UNION a` scans the source on more than one side.
    let scanning: Vec<usize> = (0..code.sides.len())
        .filter(|&i| code.sides[i].plan.source_reg_map.contains_key(&src_id))
        .collect();
    let mut seeds = Vec::with_capacity(scanning.len());
    if let Some((&last, rest)) = scanning.split_last() {
        for &i in rest {
            seeds.push(run_side(host, view_id, i, Batch::clone(&input), src_id, unfed)?);
        }
        seeds.push(run_side(host, view_id, last, input, src_id, unfed)?);
    }
    run_post(host, view_id, seeds, unfed)
}

/// The post phase over the seeds the sides produced.
fn run_post(
    host: &mut impl DriveHost,
    view_id: u64,
    seeds: impl IntoIterator<Item = (vm::DeltaReg, Batch)>,
    unfed: &[u64],
) -> Result<Batch, String> {
    let (ViewPlan { code, state }, reads) = plan_and_reads(host, view_id, unfed);
    vm::execute_epoch_multi(&mut code.post.vm, state, &reads, seeds)
}

/// Side `i`'s seed for the post phase: its relayed output.
fn run_side(
    host: &mut impl DriveHost,
    view_id: u64,
    i: usize,
    delta: Batch,
    src_id: u64,
    unfed: &[u64],
) -> Result<(vm::DeltaReg, Batch), String> {
    let (pre, seed_reg, how, fold) = {
        let (ViewPlan { code, state }, reads) = plan_and_reads(host, view_id, unfed);
        let fold = code.post.vm.program.folds(code.sides[i].seed_reg);
        let side = &mut code.sides[i];
        let seed = sub_seed(&side.plan, delta, src_id);
        let pre = vm::execute_epoch_multi(&mut side.plan.vm, state, &reads, [seed])?;
        (pre, side.seed_reg, side.relay.clone(), fold)
    };
    Ok((seed_reg, relay(host, view_id, Cow::Owned(pre), how.as_ref(), fold)))
}

// ── DAG traversal driver ────────────────────────────────────────────────

impl DagEngine {
    /// One [`Step`] per dependency edge out of `source_id`'s forward closure, in
    /// execution order, skipping non-resumable views: their backfill fills them.
    /// The skipped set is closed under dependents, so no step reads a skipped
    /// producer. Worker-identical, which keeps the workers in lockstep.
    fn tick_schedule(&self, source_id: u64) -> Vec<Step> {
        let mut schedule: Vec<Step> = Vec::new();
        for producer in std::iter::once(source_id).chain(self.dependent_closure(vec![source_id])) {
            for &view in self.dependents_of(producer) {
                if !self.awaits_rebuild(view) {
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
    pub(crate) fn finish_backfill(&mut self, registry: &mut RelationRegistry, view_id: u64) -> Result<(), String> {
        if let Some(plan) = self.plan_mut(view_id) {
            for sub in plan.code.sub_plans_mut() {
                sub.vm.release();
            }
        }
        registry.fold_to_ram(view_id)
    }
}

/// Run `what` over `delta` and ingest every view's output into its family.
/// `None`: this process has no rows of the source this time, and drives an empty
/// delta, so its exchange rounds run all the same.
pub(crate) fn drive(host: &mut impl DriveHost, what: Drive, delta: Option<Batch>) -> Result<(), String> {
    let (source, schedule, round, unfed) = match what {
        Drive::Tick { source, round } => {
            let (dag, _) = host.parts();
            (source, dag.tick_schedule(source), Some(round), Vec::new())
        }
        Drive::Backfill { view, source } => {
            let dag = host.parts().0;
            // Its ticks run from its first chunk on.
            dag.rebuild.remove(&view);
            // A backfill feeds the view its sources in this order, one after the
            // other: the ones behind `source` have reached it with nothing yet.
            let sources = dag.sources_of(view);
            let behind = sources.iter().position(|&s| s == source).map_or(0, |at| at + 1);
            let unfed = sources[behind..].to_vec();
            (source, vec![Step { view, producer: source }], None, unfed)
        }
    };
    // How many steps still read each producer's output.
    let mut readers: FxHashMap<u64, usize> = FxHashMap::default();
    for step in &schedule {
        *readers.entry(step.producer).or_default() += 1;
    }
    let delta = match delta {
        Some(delta) => delta,
        None => Batch::empty_with_schema(&host.parts().1.relation_or_err(source)?.schema()),
    };
    let mut outputs: FxHashMap<u64, Batch> = FxHashMap::default();
    outputs.insert(source, delta);
    for step in &schedule {
        let left = readers.get_mut(&step.producer).expect("counted above");
        *left -= 1;
        // The last reader takes the batch; an earlier one borrows it, and copies
        // it only when its view is unrouted, so at most two copies are live.
        let input = match *left {
            0 => outputs.remove(&step.producer).map(Cow::Owned),
            _ => outputs.get(&step.producer).map(Cow::Borrowed),
        }
        .expect("the schedule runs every producer before the steps it feeds");
        let needed = readers.contains_key(&step.view);
        let out = run_view_epoch(host, step.view, input, step.producer, &unfed)?;
        let echo = host.parts().1.ingest_at(step.view, out, round, needed)?;
        // Kept even when empty, so a reader's exchange rounds run on every worker.
        if let Some(out) = echo {
            let merged = match outputs.remove(&step.view) {
                Some(held) if !held.is_empty() => {
                    // The held batch's schema: the union is certified under it.
                    let schema = *held.schema();
                    algebra::op_union(Cow::Owned(held), &out, &schema)
                }
                _ => out,
            };
            outputs.insert(step.view, merged);
        }
    }
    Ok(())
}

/// The `(register, batch)` seeding a sub-plan with `source_id`'s delta.
fn sub_seed(sub: &SubPlan, input: Batch, source_id: u64) -> (vm::DeltaReg, Batch) {
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
