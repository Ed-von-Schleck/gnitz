//! Epoch execution: a view's program run round by round, and the DAG
//! evaluation driver.

use super::*;

// ── Epoch execution ─────────────────────────────────────────────────────

/// `view_id`'s program, which its epoch compiled on entry, beside the stores an
/// epoch of it runs over.
fn plan_and_stores<'a>(
    host: &'a mut impl DriveHost,
    view_id: u64,
    unfed: &'a [u64],
) -> (&'a mut vm::Vm, vm::Stores<'a>) {
    let (dag, registry) = host.parts();
    let ViewPlan { code, state } = dag.plan_mut(view_id).expect("compiled on the epoch's entry");
    let stores = vm::Stores { own: state, registry, unfed };
    (&mut code.vm, stores)
}

/// Run one view's epoch over `input`, `src_id`'s delta, which it may `take`.
/// `unfed`: the sources the view has been fed no row of. `drained`: what each
/// round it runs passes to [`DriveHost::exchange`], and takes from it.
// Inlined: a call copies the output batch whole, once per step of a tick.
#[inline(always)]
fn run_view_epoch(
    host: &mut impl DriveHost,
    view_id: u64,
    input: &mut Batch,
    take: bool,
    src_id: u64,
    unfed: &[u64],
    drained: &mut bool,
) -> Result<Batch, String> {
    let reg = {
        let (dag, registry) = host.parts();
        let plan = ensure_compiled(&mut dag.views, registry, view_id)?;
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
        let (vm, mut stores) = plan_and_stores(host, view_id, unfed);
        match vm::run(vm, &mut stores, &mut epoch, gathered.take())? {
            vm::Ran::Done(out) => return Ok(out),
            vm::Ran::Round { plan, batch, fold } => {
                let rows;
                (rows, *drained) = host.exchange(view_id, batch, &plan, fold, *drained);
                gathered = Some(rows);
            }
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
}

/// Seal `source` and run its whole dependent closure over what the seal answers,
/// stamping fed views' deltas with `round`.
pub(crate) fn tick(host: &mut impl DriveHost, source: u64, round: u64) -> Result<(), String> {
    let (dag, registry) = host.parts();
    let delta = match registry.seal(source)? {
        Some(delta) => delta,
        // Driven all the same: the other workers' exchange rounds wait on this one.
        None => Batch::empty_with_schema(&registry.relation_or_err(source)?.schema()),
    };
    let schedule = dag.tick_schedule(source);
    run_schedule(host, source, &schedule, round, delta)
}

/// Fill `view`, and it alone, from each relation it scans, in scan order.
pub(crate) fn backfill(host: &mut impl DriveHost, view: u64) -> Result<(), String> {
    let (dag, registry) = host.parts();
    // Its ticks run from here on.
    dag.rebuild.remove(&view);
    let sources = dag.sources_of(view).to_vec();
    let chunk_rows = registry.scan_chunk_rows();
    for (at, &source) in sources.iter().enumerate() {
        let (dag, registry) = host.parts();
        let bound = dag.view_meta(view)?.source_bound(source);
        let (mut cursor, _unapplied) = registry.open_bound(source, bound, Cut::Sealed)?;
        let schema = registry.relation_or_err(source)?.schema();
        let unfed = &sources[at + 1..];
        loop {
            let chunk = cursor.drain_chunk(chunk_rows);
            let mut drained = chunk.is_none();
            // Empty once drained, until every worker is: all run the same rounds.
            let mut input = chunk.unwrap_or_else(|| Batch::empty_with_schema(&schema));
            let out = run_view_epoch(host, view, &mut input, true, source, unfed, &mut drained)?;
            host.parts().1.ingest_at(view, out, None, false)?;
            if drained {
                break;
            }
        }
        let (dag, registry) = host.parts();
        // Views fill in id order, so the last to scan a source is its last reader.
        let last_reader = dag.dependents_of(source).iter().max() == Some(&view);
        if last_reader && dag.passes_through(source) {
            registry.clear_rows(source)?;
        }
    }
    // Once, not per chunk: the view's first reads open over fewer runs.
    host.parts().1.fold_to_ram(view)
}

/// Run `schedule`, a tick of `source`, over `delta`, and ingest every view's
/// output into its family.
// Inlined: `delta` is taken by value, and at a call it is copied whole.
#[inline(always)]
fn run_schedule(
    host: &mut impl DriveHost,
    source: u64,
    schedule: &[Step],
    round: u64,
    delta: Batch,
) -> Result<(), String> {
    // How many steps still read each producer's output. Sized for the schedule,
    // as `outputs` is: growing either rehashes it once per doubling.
    let mut readers: FxHashMap<u64, usize> = FxHashMap::with_capacity_and_hasher(schedule.len(), Default::default());
    for step in schedule.iter() {
        *readers.entry(step.producer).or_default() += 1;
    }
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
        let out = run_view_epoch(host, step.view, input, last, step.producer, &[], &mut false)?;
        if last {
            outputs.remove(&step.producer);
        }
        let (dag, registry) = host.parts();
        let echo = match dag.passes_through(step.view) {
            // A tick's delta reaches its readers in this schedule.
            true => needed.then_some(out),
            false => registry.ingest_at(step.view, out, Some(round), needed)?,
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
