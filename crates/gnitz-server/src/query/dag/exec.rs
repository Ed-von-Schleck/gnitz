//! Epoch execution: a view's program run round by round, and the DAG
//! evaluation driver.

use super::*;

// ── Epoch execution ─────────────────────────────────────────────────────

/// `view_id`'s program, beside the stores an epoch of it runs over, which its
/// epoch opened on entry.
fn plan_and_stores<'a>(
    host: &'a mut impl DriveHost,
    view_id: u64,
    unfed: &'a [u64],
) -> (&'a mut vm::Vm, vm::Stores<'a>) {
    let (dag, registry) = host.parts();
    let view = dag.views.get_mut(&view_id).expect("registered on the epoch's entry");
    let state = view.state.as_mut().expect("opened on the epoch's entry");
    let stores = vm::Stores { own: state, registry, unfed };
    (&mut view.code.vm, stores)
}

/// Run one view's epoch over `input`, `src_id`'s delta, which it may `take`.
/// `unfed`: the sources the view has been fed no row of — none, past its
/// backfill. `drained`: what each round it runs passes to
/// [`DriveHost::exchange`], and takes from it.
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
        let (code, _) = opened(&mut dag.views, registry, view_id)?;
        let scan = code.sources.get(&src_id);
        let reg = scan.expect("the dep map names only sources the view's circuit scans").0;
        if input.is_empty() && code.vm.idles_on_empty(reg) {
            return Ok(Batch::empty_with_schema(code.vm.out_schema()));
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

/// Whether this process mints the row a global aggregate owes over an empty input: each
/// process holding the view's whole input does, else the one worker the empty group's
/// exchange sends every row to.
fn mints_ground_row(slot: Slot, view: Placement) -> bool {
    slot.of <= 1 || view.is_replicated() || slot.rank as usize == algebra::ground_owner(slot.of as usize)
}

/// Seal `source` and run its whole dependent closure over what the seal answers,
/// fed views' deltas captured as `round`.
pub(crate) fn tick(host: &mut impl DriveHost, source: u64, round: u64) -> Result<(), String> {
    let delta = host.parts().1.seal(source)?;
    run_tick(host, source, round, delta)
}

/// Fill `view`, and it alone, from each relation it scans, in scan order.
pub(crate) fn backfill(host: &mut impl DriveHost, view: u64) -> Result<(), String> {
    let (dag, registry) = host.parts();
    // Its ticks run from here on.
    dag.rebuild.remove(&view);
    let sources = dag.sources_of(view).to_vec();
    let chunk_rows = registry.scan_chunk_rows();
    if mints_ground_row(registry.slot(), registry.relation_or_err(view)?.placement()) {
        opened(&mut dag.views, registry, view)?;
        // No source has fed the view yet.
        let (vm, mut stores) = plan_and_stores(host, view, &sources);
        let minted = vm::mint_ground_rows(vm, &mut stores)?;
        host.parts().1.ingest_at(view, minted, None, false)?;
    }
    for (at, &source) in sources.iter().enumerate() {
        let (dag, registry) = host.parts();
        // `sources_of` names only registered views, and of each only the
        // relations its circuit scans.
        let bound = dag.views[&view].code.sources[&source].1.clone();
        let relation = registry.relation_or_err(source)?;
        let (mut cursor, _unapplied) = relation.open_bound(bound, Cut::Sealed)?;
        let schema = relation.schema();
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

/// Run `source`'s tick schedule over `delta`, `None` where it brings this process
/// no row, and ingest every view's output into its family.
// Inlined: `delta` is taken by value, and at a call it is copied whole.
#[inline(always)]
fn run_tick(host: &mut impl DriveHost, source: u64, round: u64, delta: Option<Batch>) -> Result<(), String> {
    let (dag, registry) = host.parts();
    let delta = match delta {
        Some(delta) => delta,
        // Driven all the same: the other workers' exchange rounds wait on this one.
        None => Batch::empty_with_schema(&registry.relation_or_err(source)?.schema()),
    };
    let schedule = dag.tick_schedule(source);
    // Sized for the schedule: growing it rehashes it once per doubling.
    let mut outputs: FxHashMap<u64, Batch> = FxHashMap::with_capacity_and_hasher(schedule.len(), Default::default());
    outputs.insert(source, delta);
    for &Step { view, producer, last, needed } in schedule.iter() {
        // Lent to every reader, the last of which may take it.
        let input = outputs.get_mut(&producer);
        let input = input.expect("the schedule runs every producer before the steps it feeds");
        let out = run_view_epoch(host, view, input, last, producer, &[], &mut false)?;
        if last {
            outputs.remove(&producer);
        }
        let (dag, registry) = host.parts();
        let echo = match out.is_empty() || dag.passes_through(view) {
            // A tick's delta reaches its readers in this schedule, and an empty one
            // changes no store.
            true => needed.then_some(out),
            false => registry.ingest_at(view, out, Some(round), needed)?,
        };
        // Kept even when empty, so a reader's exchange rounds run on every worker.
        if let Some(out) = echo {
            let merged = match outputs.remove(&view) {
                Some(held) => {
                    // The held batch's schema: the union is certified under it.
                    let schema = *held.schema();
                    algebra::op_union(Cow::Owned(held), Cow::Owned(out), &schema)
                }
                None => out,
            };
            outputs.insert(view, merged);
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
