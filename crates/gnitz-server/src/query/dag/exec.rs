//! Epoch execution: a view's program run round by round, and the DAG
//! evaluation driver.

use super::*;
use std::collections::hash_map::Entry;

// ── Epoch execution ─────────────────────────────────────────────────────

/// Run one view's epoch over `input`, `src_id`'s delta — what [`relay_delta`]
/// gathered of it where the view's scan of `src_id` relays — which it may
/// `take`; `None` is an empty delta. Answers `None` where the epoch produced no row.
/// `unfed`: the sources the view has been fed no row of — none, past its
/// backfill. `drained`: what each round it runs passes to
/// [`DriveHost::exchange`], and takes from it.
// Inlined: a call copies the output batch whole, once per step of a tick.
#[inline(always)]
fn run_view_epoch(
    host: &mut impl DriveHost,
    view_id: u64,
    input: Option<&mut Batch>,
    take: bool,
    src_id: u64,
    unfed: &[u64],
    drained: &mut bool,
) -> Result<Option<Batch>, String> {
    let mut empty;
    let (dag, registry) = host.parts();
    let (mut code, mut stores) = opened(&mut dag.views, registry, view_id, unfed)?;
    // A tick reaches a view only from a source its circuit scans.
    let reg = code.scan(src_id).seed;
    let input = match input.filter(|input| !input.is_empty()) {
        Some(input) => input,
        None if code.vm.idles_on_empty(reg) => return Ok(None),
        // Its round runs on every worker, whatever this one holds.
        None => {
            empty = Batch::empty_with_schema(code.vm.schema_of(reg));
            &mut empty
        }
    };
    let mut epoch = vm::Epoch::tick(reg, input, take);
    let mut gathered = None;
    loop {
        match vm::run(&mut code.vm, &mut stores, &mut epoch, gathered.take())? {
            vm::Ran::Done(out) => return Ok((!out.is_empty()).then_some(out)),
            vm::Ran::Round { plan, batch, fold } => {
                let rows;
                (rows, *drained) = host.exchange(view_id, batch, &plan, fold, *drained);
                gathered = Some(rows);
                // The exchange held the host: the plan and its stores are looked up again.
                let (dag, registry) = host.parts();
                (code, stores) = opened(&mut dag.views, registry, view_id, unfed)?;
            }
        }
    }
}

/// `input`, `src_id`'s delta, relayed into view `view_id` by the round its scan
/// of `src_id` relays by: this worker's share of every worker's, gathered by
/// merge iff `fold`. The round runs on every worker, whatever this one holds;
/// `None` is an empty delta. `take` and `drained` as [`run_view_epoch`]'s.
fn relay_delta(
    host: &mut impl DriveHost,
    view_id: u64,
    src_id: u64,
    input: Option<&mut Batch>,
    take: bool,
    fold: bool,
    drained: &mut bool,
) -> Batch {
    let code = &host.parts().0.views[&view_id].code;
    let scan = code.scan(src_id);
    let plan = Rc::clone(&scan.relay.as_ref().expect("a relayed scan has a relay").plan);
    let schema = *code.vm.schema_of(scan.seed);
    let mut empty;
    let input = match input {
        Some(input) => input,
        None => {
            empty = Batch::empty_with_schema(&schema);
            &mut empty
        }
    };
    // Every worker sends its rows, and decodes theirs, under the seed's label.
    assert!(
        input.schema().same_region_types(&schema),
        "relay of {src_id} into view {view_id}: schema/batch region mismatch"
    );
    input.set_schema(&schema);
    // Every worker gathers every row of a broadcast: one fold at the sender
    // serves them all.
    if fold && plan.is_broadcast() {
        input.consolidate_in_place();
    }
    let operand = match take {
        true => Cow::Owned(input.take()),
        false => Cow::Borrowed(&*input),
    };
    let rows;
    (rows, *drained) = host.exchange(view_id, operand, &plan, fold, *drained);
    rows
}

// ── DAG traversal driver ────────────────────────────────────────────────

/// Whether this process mints the row a global aggregate owes over an empty input: each
/// process holding the view's whole input does, else the one worker the empty group's
/// exchange sends every row to.
fn mints_ground_row(slot: Slot, self_contained: bool) -> bool {
    self_contained || slot.rank as usize == algebra::ground_owner(slot.of as usize)
}

/// Seal `source` and run its whole dependent closure over what the seal answers,
/// fed views' deltas captured as `round`.
pub(crate) fn tick(host: &mut impl DriveHost, source: u64, round: u64) -> Result<(), String> {
    let delta = host.parts().1.seal(source)?;
    run_tick(host, source, round, delta)
}

/// Fill `view`, and it alone, from each relation it scans, in scan order. Views
/// fill in id order: one whose source still awaits its own fill is refused, which
/// keeps the rebuild set closed under dependents.
pub(crate) fn backfill(host: &mut impl DriveHost, view: u64) -> Result<(), String> {
    let (dag, registry) = host.parts();
    let sources: Vec<u64> = dag.sources_of(view).collect();
    if let Some(source) = sources.iter().find(|&&s| dag.awaits_rebuild(s)) {
        return Err(format!("view {view} filled before its source {source}"));
    }
    // Its ticks run from here on.
    dag.rebuild.remove(&view);
    let chunk_rows = registry.scan_chunk_rows();
    // No source has fed the view yet.
    let (code, mut stores) = opened(&mut dag.views, registry, view, &sources)?;
    if mints_ground_row(registry.slot(), code.self_contained) {
        let minted = vm::mint_ground_rows(&mut code.vm, &mut stores)?;
        registry.ingest(view, minted)?;
    }
    for (at, &source) in sources.iter().enumerate() {
        let (dag, registry) = host.parts();
        // `sources` came from this view's own plan.
        let bound = dag.views[&view].code.scan(source).bound.clone();
        let code = &dag.views[&view].code;
        let seed = code.scan(source).seed;
        // Whether a round relays the source's rows, and whether it folds them.
        let relay_fold = code.scan(source).relay.is_some().then(|| code.vm.folds(seed));
        let relation = registry.relation_or_err(source)?;
        let (mut cursor, _unapplied) = relation.open_bound(bound, Cut::Sealed)?;
        let unfed = &sources[at + 1..];
        loop {
            let mut chunk = cursor.drain_chunk(chunk_rows);
            // `None` once drained, until every worker is: all run the same rounds.
            let mut drained = chunk.is_none();
            if let Some(fold) = relay_fold {
                chunk = Some(relay_delta(
                    host,
                    view,
                    source,
                    chunk.as_mut(),
                    true,
                    fold,
                    &mut drained,
                ));
            }
            if let Some(out) = run_view_epoch(host, view, chunk.as_mut(), true, source, unfed, &mut drained)? {
                host.parts().1.ingest(view, out)?;
            }
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
    let schedule = host.parts().0.tick_schedule(source);
    // The engine's map, empty between ticks, with room for the schedule:
    // growing it rehashes it once per doubling. A producer with no entry
    // produced nothing.
    let mut outputs = std::mem::take(&mut host.parts().0.tick_outputs);
    outputs.reserve(schedule.len());
    outputs.extend(delta.map(|delta| (source, delta)));
    let mut relays = std::mem::take(&mut host.parts().0.tick_relays);
    for &Step { view, producer, last, needed, relay } in schedule.iter() {
        // Lent to every reader, the last of which may take it.
        let mut input = outputs.get_mut(&producer);
        let mut take = last;
        // A relayed step reads its round's rows: the round's first reader runs it.
        if let Some(slot) = relay {
            if slot.at == relays.len() {
                relays.push(relay_delta(host, view, producer, input, last, slot.fold, &mut false));
            } else if last {
                // No later step reads the producer's own rows.
                outputs.remove(&producer);
            }
            (input, take) = (Some(&mut relays[slot.at]), slot.last);
        }
        let Some(out) = run_view_epoch(host, view, input, take, producer, &[], &mut false)? else {
            continue;
        };
        let (dag, registry) = host.parts();
        let echo = match dag.passes_through(view) {
            // A tick's delta reaches its readers in this schedule.
            true => needed.then_some(out),
            false => registry.ingest_round(view, out, round, needed)?,
        };
        if let Some(out) = echo {
            match outputs.entry(view) {
                Entry::Vacant(e) => {
                    e.insert(out);
                }
                Entry::Occupied(mut e) => {
                    let held = e.get_mut();
                    // The held batch's schema: the union is certified under it.
                    let schema = *held.schema();
                    *held = algebra::op_union(Cow::Owned(held.take()), Cow::Owned(out), &schema);
                }
            }
        }
    }
    outputs.clear();
    relays.clear();
    let dag = host.parts().0;
    dag.tick_outputs = outputs;
    dag.tick_relays = relays;
    Ok(())
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/exec.rs"]
mod bench;
