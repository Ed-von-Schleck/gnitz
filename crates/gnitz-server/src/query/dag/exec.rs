//! Epoch execution: the one pre → relay → post pipeline every compiled shape
//! runs through, and the DAG evaluation driver.

use super::*;

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

// ── Epoch execution ─────────────────────────────────────────────────────

/// `view_id`'s plan, which its epoch compiled on entry, beside the stores an
/// epoch of it runs over.
fn plan_and_stores<'a>(
    host: &'a mut impl DriveHost,
    view_id: u64,
    unfed: &'a [u64],
) -> (&'a mut CompileOutput, vm::Stores<'a>) {
    let (dag, registry) = host.parts();
    let ViewPlan { code, state } = dag.plan_mut(view_id).expect("compiled on the epoch's entry");
    let stores = vm::Stores {
        own: state,
        registry,
        view: view_id,
        unfed,
    };
    (code, stores)
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
        // An empty delta into a plan that runs no exchange round and owes no
        // ground row is an epoch every sub-plan would answer empty.
        let sub_plans = || code.sides.iter().map(|s| &s.plan).chain([&code.post]);
        if input.is_empty()
            && route.is_none()
            && code.sides.iter().all(|s| s.relay.is_none())
            && sub_plans().all(|p| !p.vm.pending_ground_row)
        {
            return Ok(Batch::empty_with_schema(code.post.vm.out_schema()));
        }
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
    let (code, _) = plan_and_stores(host, view_id, unfed);
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
    let (code, mut stores) = plan_and_stores(host, view_id, unfed);
    vm::execute_epoch(&mut code.post.vm, &mut stores, seeds)
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
        let (code, mut stores) = plan_and_stores(host, view_id, unfed);
        let fold = code.post.vm.folds(code.sides[i].seed_reg);
        let side = &mut code.sides[i];
        let seed = sub_seed(&side.plan, delta, src_id);
        let pre = vm::execute_epoch(&mut side.plan.vm, &mut stores, [seed])?;
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
    fn tick_schedule(&mut self, source_id: u64) -> Rc<[Step]> {
        let steps = self.dep.tick_steps(source_id);
        match self.rebuild.is_empty() {
            true => steps,
            false => steps.iter().filter(|s| !self.awaits_rebuild(s.view)).copied().collect(),
        }
    }

    /// End `view_id`'s backfill from `source`.
    ///
    /// Folds the view's memtable into its RAM tier once — so the view's first
    /// reads open over fewer sources, at one fold per backfill rather than one per
    /// chunk.
    ///
    /// A source that [passes through](Self::passes_through) held its rows for the
    /// backfills of the views scanning it. Those run in ascending id order, so the
    /// highest of them ends the last, and the rows are dropped.
    pub(crate) fn finish_backfill(
        &mut self,
        registry: &mut RelationRegistry,
        view_id: u64,
        source: u64,
    ) -> Result<(), String> {
        registry.fold_to_ram(view_id)?;
        if self.passes_through(source) && self.dependents_of(source).iter().max() == Some(&view_id) {
            registry.clear_rows(source)?;
        }
        Ok(())
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
            (source, Rc::from([Step { view, producer: source }]), None, unfed)
        }
    };
    // How many steps still read each producer's output. Sized for the schedule,
    // as `outputs` is: growing either rehashes it once per doubling.
    let mut readers: FxHashMap<u64, usize> = FxHashMap::with_capacity_and_hasher(schedule.len(), Default::default());
    for step in schedule.iter() {
        *readers.entry(step.producer).or_default() += 1;
    }
    let delta = match delta {
        Some(delta) => delta,
        None => Batch::empty_with_schema(&host.parts().1.relation_or_err(source)?.schema()),
    };
    let mut outputs: FxHashMap<u64, Batch> = FxHashMap::with_capacity_and_hasher(schedule.len(), Default::default());
    outputs.insert(source, delta);
    for step in schedule.iter() {
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
        let (dag, registry) = host.parts();
        let echo = match round {
            // A tick's delta reaches its readers in this schedule, and no tick
            // runs while a chain is being built.
            Some(_) if dag.passes_through(step.view) => needed.then(|| out.into_consolidated()),
            _ => registry.ingest_at(step.view, out, round, needed)?,
        };
        // Kept even when empty, so a reader's exchange rounds run on every worker.
        if let Some(out) = echo {
            let merged = match outputs.remove(&step.view) {
                Some(held) => {
                    // The held batch's schema: the union is certified under it.
                    let schema = *held.schema();
                    algebra::op_union(Cow::Owned(held), Cow::Owned(out), &schema)
                }
                None => out,
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

#[cfg(test)]
#[path = "benches/exec.rs"]
mod bench;
