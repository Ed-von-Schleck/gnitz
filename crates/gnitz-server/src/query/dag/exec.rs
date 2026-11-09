//! Epoch execution: a view's program run round by round, and the DAG
//! evaluation driver.

use super::*;
use std::collections::hash_map::Entry;

// ── Epoch execution ─────────────────────────────────────────────────────

/// Run one view's backfill epoch over `input`, a chunk of `src_id`'s rows —
/// what a relay round gathered of it where the view's scan of `src_id` relays —
/// `None` once drained. Answers `None` where the epoch produced no row.
/// `unfed`: the sources the view has been fed no row of. `drained`: what each
/// round it runs passes to [`DriveHost::exchange`], and takes from it.
// Inlined: a call copies the output batch whole, once per chunk.
#[inline(always)]
fn run_view_epoch(
    host: &mut impl DriveHost,
    view_id: u64,
    input: Option<Batch>,
    src_id: u64,
    unfed: &[u64],
    drained: &mut bool,
) -> Result<Option<Batch>, String> {
    let (dag, registry) = host.parts();
    let (mut code, mut stores) = opened(&mut dag.views, registry, view_id, unfed)?;
    let reg = code.scan(src_id).seed;
    let seed = match input.filter(|input| !input.is_empty()) {
        Some(input) => input,
        None if code.vm.idles_on_empty(reg) => return Ok(None),
        // Its round runs on every worker, whatever this one holds.
        None => Batch::empty_with_schema(code.vm.schema_of(reg)),
    };
    let mut epoch = vm::Epoch::tick(reg, seed, true);
    let (mut gathered, mut rows) = (Vec::new(), None);
    loop {
        match vm::run(&mut code.vm, &mut stores, &mut epoch, rows.take())? {
            vm::Ran::Done(out) => return Ok((!out.is_empty()).then_some(out)),
            vm::Ran::Round { plan, fold } => {
                let lane = Lane {
                    view: view_id,
                    batch: epoch.operand(),
                    plan: &plan,
                    fold,
                };
                *drained = host.exchange(std::iter::once(lane), *drained, &mut gathered);
                rows = gathered.pop();
                // The exchange held the host: the plan and its stores are looked up again.
                let (dag, registry) = host.parts();
                (code, stores) = opened(&mut dag.views, registry, view_id, unfed)?;
            }
        }
    }
}

/// What the round relaying `input`, `src_id`'s delta, into view `view_id` sends
/// of it — given where `take`, else a copy — and the plan it sends it by. The
/// round runs on every worker, whatever this one holds; `None` is an empty
/// delta. `fold` as [`Lane::fold`].
fn relay_operand(
    host: &mut impl DriveHost,
    view_id: u64,
    src_id: u64,
    input: Option<&mut Batch>,
    take: bool,
    fold: bool,
) -> (Batch, Rc<ScatterPlan>) {
    let code = &host.parts().0.views[&view_id].code;
    let scan = code.scan(src_id);
    let plan = Rc::clone(&scan.relay.as_ref().expect("a relayed scan has a relay").plan);
    let schema = *code.vm.schema_of(scan.seed);
    let Some(input) = input else {
        return (Batch::empty_with_schema(&schema), plan);
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
    let rows = match take {
        true => input.take(),
        false => input.clone(),
    };
    (rows, plan)
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
    let mut gathered = Vec::new();
    // No source has fed the view yet.
    let (code, mut stores) = opened(&mut dag.views, registry, view, &sources)?;
    if mints_ground_row(registry.slot(), code.self_contained) {
        let minted = vm::mint_ground_rows(&mut code.vm, &mut stores)?;
        registry.ingest(view, minted)?;
    }
    for (at, &source) in sources.iter().enumerate() {
        let (dag, registry) = host.parts();
        // `sources` came from this view's own plan.
        let code = &dag.views[&view].code;
        let scan = code.scan(source);
        // Whether a round relays the source's rows, and whether it folds them.
        let relay_fold = scan.relay.is_some().then(|| code.vm.folds(scan.seed));
        let relation = registry.relation_or_err(source)?;
        let (mut cursor, _unapplied) = relation.open_bound(scan.bound.clone(), Cut::Sealed, scan.read)?;
        let unfed = &sources[at + 1..];
        loop {
            let mut chunk = cursor.drain_chunk(chunk_rows);
            // `None` once drained, until every worker is: all run the same rounds.
            let mut drained = chunk.is_none();
            if let Some(fold) = relay_fold {
                let (rows, plan) = relay_operand(host, view, source, chunk.as_mut(), true, fold);
                let lane = Lane {
                    view,
                    batch: Cow::Owned(rows),
                    plan: &plan,
                    fold,
                };
                drained = host.exchange(std::iter::once(lane), drained, &mut gathered);
                chunk = gathered.pop();
            }
            if let Some(out) = run_view_epoch(host, view, chunk, source, unfed, &mut drained)? {
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

/// Where a step of the running tick stands.
#[derive(Default)]
enum Stage {
    /// Not at a round: not started, or done.
    #[default]
    Idle,
    /// At its relay round, which sends `rows` under `plan`.
    Relay {
        rows: Batch,
        plan: Rc<ScatterPlan>,
        fold: bool,
    },
    /// Its epoch, at a round under `plan`.
    Epoch {
        epoch: vm::Epoch,
        plan: Rc<ScatterPlan>,
        fold: bool,
    },
}

/// One tick's run of its schedule, in waves: every step that can start runs to
/// its first round or its end, then the rounds every step stands at run as one
/// [`DriveHost::exchange`]. Each epoch sends and gathers exactly the rows a
/// round of its own would; only the waits are shared.
///
/// Steps start in schedule order, and one starts only once no step standing at
/// a round is of its view or of a relation its view scans: a view then reads
/// each source's store and output as a run one step at a time leaves them. A
/// round's lanes follow the schedule, the same on every worker. An epoch at a
/// round holds its registers and its own copy of its delta until the round
/// gathers, so a tick holds the state of every epoch of a wave at once.
///
/// The engine keeps one between ticks, empty, for its capacity.
#[derive(Default)]
pub(super) struct TickRun {
    schedule: Rc<[Step]>,
    round: u64,
    /// The batch each producer of the tick has emitted, keyed by producer. A
    /// producer with no entry produced nothing.
    outputs: FxHashMap<u64, Batch>,
    /// What each relay round gathered, indexed by [`meta::RelaySlot`], from
    /// the round's run to its last reader; `None` while its round is out.
    relays: Vec<Option<Batch>>,
    /// Each step's [`Stage`], by its index in the schedule.
    stages: Vec<Stage>,
    /// What a wave's rounds gathered, one batch per step at a round.
    gathered: Vec<Batch>,
}

impl TickRun {
    /// Step `i`'s delta, and whether it may take it: what its relay round
    /// gathered, or its producer's output.
    fn feed(&mut self, i: usize) -> (Option<&mut Batch>, bool) {
        let Step { producer, last, relay, .. } = self.schedule[i];
        match relay {
            Some(slot) => (self.relays[slot.at].as_mut(), slot.last),
            None => (self.outputs.get_mut(&producer), last),
        }
    }

    /// Whether step `i` can start: a relay round another step runs for it has
    /// gathered, and no step at a round holds it back.
    fn ready(&self, host: &mut impl DriveHost, i: usize) -> bool {
        let Step { view, relay, .. } = self.schedule[i];
        if relay.is_some_and(|slot| self.relays.get(slot.at).is_some_and(Option::is_none)) {
            return false;
        }
        let dag = host.parts().0;
        let mut standing = (self.stages.iter().zip(self.schedule.iter()))
            .filter(|(stage, _)| !matches!(stage, Stage::Idle))
            .map(|(_, step)| step.view);
        !standing.any(|held| held == view || dag.sources_of(view).any(|s| s == held))
    }

    /// Start step `i`: run it to its first round, or its end.
    fn start(&mut self, host: &mut impl DriveHost, i: usize) -> Result<(), String> {
        let Step { view, producer, last, relay, .. } = self.schedule[i];
        // A relayed step reads its round's rows: the round's first reader runs it.
        if let Some(slot) = relay {
            if slot.at == self.relays.len() {
                let (rows, plan) =
                    relay_operand(host, view, producer, self.outputs.get_mut(&producer), last, slot.fold);
                self.relays.push(None);
                self.stages[i] = Stage::Relay { rows, plan, fold: slot.fold };
                return Ok(());
            }
            if last {
                // No later step reads the producer's own rows.
                self.outputs.remove(&producer);
            }
        }
        self.start_epoch(host, i)
    }

    /// Start step `i`'s epoch over its [`Self::feed`].
    fn start_epoch(&mut self, host: &mut impl DriveHost, i: usize) -> Result<(), String> {
        let Step { view, producer, .. } = self.schedule[i];
        let code = &host.parts().0.views[&view].code;
        // A tick reaches a view only from a source its circuit scans.
        let reg = code.scan(producer).seed;
        let idles = code.vm.idles_on_empty(reg);
        let epoch = match self.feed(i) {
            (Some(input), take) if !input.is_empty() => match (take, idles) {
                (true, _) => vm::Epoch::tick(reg, input.take(), true),
                // It runs no round, so it ends before any later reader starts:
                // lent the delta, it hands it back.
                (false, true) => vm::Epoch::tick(reg, input.take(), false),
                // It may stand at a round beside a later reader, so it holds a
                // copy, of the delta as an epoch makes it: every later copy
                // shares its fold.
                (false, false) => {
                    code.vm.label_seed(reg, input);
                    vm::Epoch::tick(reg, input.clone(), true)
                }
            },
            _ if idles => return Ok(()),
            // Its round runs on every worker, whatever this one holds.
            _ => vm::Epoch::tick(reg, Batch::empty_with_schema(code.vm.schema_of(reg)), true),
        };
        self.run_epoch(host, i, epoch, None)
    }

    /// Run step `i`'s `epoch` on, `gathered` what the round it stood at
    /// gathered: to the next round, where it stands, or to its end.
    fn run_epoch(
        &mut self,
        host: &mut impl DriveHost,
        i: usize,
        mut epoch: vm::Epoch,
        gathered: Option<Batch>,
    ) -> Result<(), String> {
        let view = self.schedule[i].view;
        let (dag, registry) = host.parts();
        let (code, mut stores) = opened(&mut dag.views, registry, view, &[])?;
        match vm::run(&mut code.vm, &mut stores, &mut epoch, gathered)? {
            vm::Ran::Round { plan, fold } => {
                self.stages[i] = Stage::Epoch { epoch, plan, fold };
                Ok(())
            }
            vm::Ran::Done(out) => {
                if let Some(lent) = epoch.into_lent() {
                    *self.feed(i).0.expect("a lent delta has its slot") = lent;
                }
                self.complete(host, i, out)
            }
        }
    }

    /// Step `i` ran to its end with `out`: ingest it, and hold it for its readers.
    fn complete(&mut self, host: &mut impl DriveHost, i: usize, out: Batch) -> Result<(), String> {
        if out.is_empty() {
            return Ok(());
        }
        let Step { view, needed, .. } = self.schedule[i];
        let (dag, registry) = host.parts();
        let echo = match dag.passes_through(view) {
            // A tick's delta reaches its readers in this schedule.
            true => needed.then_some(out),
            false => registry.ingest_round(view, out, self.round, needed)?,
        };
        if let Some(out) = echo {
            match self.outputs.entry(view) {
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
        Ok(())
    }

    /// Run every step: start what can start, then the rounds it stands at as one.
    fn run(&mut self, host: &mut impl DriveHost) -> Result<(), String> {
        let mut next = 0;
        loop {
            while next < self.schedule.len() && self.ready(host, next) {
                self.start(host, next)?;
                next += 1;
            }
            // A relay's rows are its round's to free once sent.
            let lanes: Vec<Lane> = (self.stages.iter_mut().zip(self.schedule.iter()))
                .filter_map(|(stage, step)| {
                    let (batch, plan, fold) = match stage {
                        Stage::Idle => return None,
                        Stage::Relay { rows, plan, fold } => (Cow::Owned(rows.take()), plan, fold),
                        Stage::Epoch { epoch, plan, fold } => (epoch.operand(), plan, fold),
                    };
                    Some(Lane {
                        view: step.view,
                        batch,
                        plan,
                        fold: *fold,
                    })
                })
                .collect();
            if lanes.is_empty() {
                debug_assert_eq!(next, self.schedule.len(), "a step waits on no round");
                return Ok(());
            }
            host.exchange(lanes.into_iter(), false, &mut self.gathered);
            let mut gathered = std::mem::take(&mut self.gathered);
            let mut rows = gathered.drain(..).map(Some);
            for i in 0..self.stages.len() {
                match std::mem::take(&mut self.stages[i]) {
                    Stage::Idle => continue,
                    Stage::Relay { .. } => {
                        let slot = self.schedule[i].relay.expect("a relay step");
                        self.relays[slot.at] = rows.next().expect("a lane per round");
                        self.start_epoch(host, i)?;
                    }
                    Stage::Epoch { epoch, .. } => {
                        self.run_epoch(host, i, epoch, rows.next().expect("a lane per round"))?;
                    }
                }
            }
            drop(rows);
            self.gathered = gathered;
        }
    }
}

/// Run `source`'s tick schedule over `delta`, `None` where it brings this process
/// no row, and ingest every view's output into its family.
// Inlined: `delta` is taken by value, and at a call it is copied whole.
#[inline(always)]
fn run_tick(host: &mut impl DriveHost, source: u64, round: u64, delta: Option<Batch>) -> Result<(), String> {
    let dag = host.parts().0;
    let mut run = std::mem::take(&mut dag.tick);
    run.schedule = dag.tick_schedule(source);
    run.round = round;
    // Room for the schedule: growing the map rehashes it once per doubling.
    run.outputs.reserve(run.schedule.len());
    run.outputs.extend(delta.map(|delta| (source, delta)));
    run.stages.resize_with(run.schedule.len(), Stage::default);
    let ran = run.run(host);
    // A failed step leaves the others' epochs at their rounds: dropped here, and
    // each program frees its registers when it next runs.
    run.outputs.clear();
    run.relays.clear();
    run.stages.clear();
    run.gathered.clear();
    host.parts().0.tick = run;
    ran
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/exec.rs"]
mod bench;
