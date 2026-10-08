//! Epoch execution: a view's program run round by round, and the DAG
//! evaluation driver.

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
    let stores = vm::Stores { own: state, registry, unfed };
    (code, stores)
}

/// Run one view's epoch over `input`, `src_id`'s delta, which it may `take`.
/// `unfed`: the sources the view has been fed no row of.
fn run_view_epoch(
    host: &mut impl DriveHost,
    view_id: u64,
    input: &mut Batch,
    take: bool,
    src_id: u64,
    unfed: &[u64],
) -> Result<Batch, String> {
    let reg = {
        let (dag, registry) = host.parts();
        let (_, plan) = ensure_compiled(&mut dag.views, registry, view_id)?;
        let reg = plan.code.source_reg_map.get(&src_id).copied();
        let reg = reg.expect("the dep map names only sources the view's circuit scans");
        if input.is_empty() && plan.code.vm.idles_on_empty(reg) {
            return Ok(Batch::empty_with_schema(plan.code.vm.out_schema()));
        }
        reg
    };
    let mut epoch = vm::Epoch::tick(reg, input, take);
    let mut gathered = None;
    loop {
        let (code, mut stores) = plan_and_stores(host, view_id, unfed);
        match vm::run(&mut code.vm, &mut stores, &mut epoch, gathered.take())? {
            vm::Ran::Done(out) => return Ok(out),
            vm::Ran::Round { plan, batch, fold } => gathered = Some(host.exchange(view_id, batch, &plan, fold)),
        }
    }
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
        let last = *left == 0;
        let needed = readers.contains_key(&step.view);
        // Lent to every reader, the last of which may take it.
        let input = outputs.get_mut(&step.producer);
        let input = input.expect("the schedule runs every producer before the steps it feeds");
        let out = run_view_epoch(host, step.view, input, last, step.producer, &unfed)?;
        if last {
            outputs.remove(&step.producer);
        }
        let (dag, registry) = host.parts();
        let echo = match round {
            // A tick's delta reaches its readers in this schedule, and no tick
            // runs while a chain is being built.
            Some(_) if dag.passes_through(step.view) => needed.then_some(out),
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/exec.rs"]
mod bench;
