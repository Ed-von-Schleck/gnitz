//! Epoch execution: a view's program run round by round, and the DAG
//! evaluation driver.

use super::*;
use std::collections::hash_map::Entry;
use std::collections::VecDeque;

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
    mut input: Option<Batch>,
    src_id: u64,
    unfed: &[u64],
    drained: &mut bool,
) -> Result<Option<Batch>, String> {
    let mut ran = start_epoch(host, view_id, src_id, input.as_mut(), true, unfed)?;
    let mut gathered = Vec::new();
    loop {
        let mut epoch = match ran {
            None => return Ok(None),
            Some(vm::Ran::Done(out)) => return Ok((!out.is_empty()).then_some(out)),
            Some(vm::Ran::Round(epoch)) => epoch,
        };
        let (batch, plan, fold) = epoch.round();
        let lane = Lane { view: view_id, batch, plan, fold };
        *drained = host.exchange(std::iter::once(lane), *drained, &mut gathered);
        let rows = gathered.pop().expect("a lane per round");
        ran = Some(resume_epoch(host, view_id, epoch, rows, unfed)?);
    }
}

/// Start view `view_id`'s epoch over `input`, `src_id`'s delta, given where
/// `take` and lent otherwise, as [`vm::start`] runs it. `None` where the epoch
/// idles: its delta is empty and reaches no round. `unfed` as [`RegisteredView::opened`]'s.
fn start_epoch(
    host: &mut impl DriveHost,
    view_id: u64,
    src_id: u64,
    input: Option<&mut Batch>,
    take: bool,
    unfed: &[u64],
) -> Result<Option<vm::Ran>, String> {
    let (dag, registry) = host.parts();
    // A view's run reaches it only from a source its circuit scans.
    let view = dag.registered(view_id)?;
    let reg = view.code.scan(src_id).seed;
    let seed = input.filter(|input| !input.is_empty());
    // An empty delta still runs its round, on every worker, whatever this one
    // holds. One that runs none is skipped before its view's stores are opened.
    if seed.is_none() && view.code.vm.idles_on_empty(reg) {
        return Ok(None);
    }
    let (code, mut stores) = view.opened(registry, view_id, unfed)?;
    vm::start(&mut code.vm, &mut stores, reg, seed, take).map(Some)
}

/// Run view `view_id`'s `epoch` on over `rows`, what its round gathered. The
/// exchange held the host: the plan and its stores are looked up again.
fn resume_epoch(
    host: &mut impl DriveHost,
    view_id: u64,
    epoch: vm::Epoch,
    rows: Batch,
    unfed: &[u64],
) -> Result<vm::Ran, String> {
    let (dag, registry) = host.parts();
    let (code, mut stores) = dag.registered(view_id)?.opened(registry, view_id, unfed)?;
    vm::resume(&mut code.vm, &mut stores, epoch, rows)
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
    let Some(input) = input else {
        return (Batch::empty_with_schema(code.vm.schema_of(scan.seed)), plan);
    };
    // Every worker sends its rows, and decodes theirs, under the seed's label.
    code.vm.label(scan.seed, input);
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
/// process of a replicated view does, holding its whole input, else the one worker the
/// empty group's exchange sends every row to.
fn mints_ground_row(slot: Slot, replicated: bool) -> bool {
    replicated || slot.rank as usize == algebra::ground_owner(slot.of as usize)
}

/// Seal `source` and run its whole dependent closure over what the seal answers,
/// fed views' deltas captured as `round`.
pub(crate) fn tick(host: &mut impl DriveHost, source: u64, round: u64) -> Result<(), String> {
    let delta = host.parts().1.seal(source)?;
    run_tick(host, source, round, delta)
}

/// Fill `view`, and it alone, from each relation it scans, in scan order, each
/// read at its cut: rows above it reach the view with the tick that seals them.
/// The caller fills views in id order, a view after every view it scans.
pub(crate) fn backfill(host: &mut impl DriveHost, view: u64) -> Result<(), String> {
    let (dag, registry) = host.parts();
    let sources: Vec<u64> = dag.sources_of(view).collect();
    let chunk_rows = registry.scan_chunk_rows();
    let mut gathered = Vec::new();
    // No source has fed the view yet.
    let replicated = registry.relation_or_err(view)?.placement().is_replicated();
    let (code, mut stores) = dag.registered(view)?.opened(registry, view, &sources)?;
    if mints_ground_row(registry.slot(), replicated) {
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
        // A stream holds no rows: every worker skips it alike, so no round is owed.
        if relation.kind() == RelationKind::Stream {
            continue;
        }
        let mut cursor = relation.open_sealed(scan.bound.clone(), scan.read)?;
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
    host.parts().1.settle(view)
}

/// The round a step of the running tick stands at.
#[expect(clippy::large_enum_variant, reason = "held once per round, and boxing allocates")]
enum Stage {
    /// Its relay round, which sends `rows` under `plan`.
    Relay {
        rows: Batch,
        plan: Rc<ScatterPlan>,
        fold: bool,
    },
    /// A round of its epoch.
    Epoch(vm::Epoch),
}

/// One tick's run of its schedule, in waves: steps start in schedule order, each
/// run to its first round or its end, up to the first that cannot start yet;
/// then the rounds every step stands at run as one [`DriveHost::exchange`]. Each
/// epoch sends and gathers exactly the rows a round of its own would; only the
/// waits are shared.
///
/// A step starts only once no step standing at a round is of its view or of its
/// producer. Besides its producer's output, a view's epoch reads only its own
/// operator traces and store and base tables' stores at their sealed cut, none
/// of which another view's step moves: a view's store is never another view's
/// integral. A round's lanes follow the schedule, the same on every worker. An
/// epoch at a round holds its registers, and with them its own copy of a delta
/// it reads again, until the round gathers, so a tick holds the state of every
/// epoch of a wave at once.
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
    /// The steps standing at a round, each by its index in the schedule, in
    /// schedule order: the order their lanes go out. A step run on from a
    /// gathered round that stands again goes behind the rest of its wave.
    standing: VecDeque<(usize, Stage)>,
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
    /// gathered, and no step at a round is of its view or of its producer.
    fn ready(&self, i: usize) -> bool {
        // A relay round is out only while the step that runs it stands.
        if self.standing.is_empty() {
            return true;
        }
        let step = &self.schedule[i];
        if step
            .relay
            .is_some_and(|slot| self.relays.get(slot.at).is_some_and(Option::is_none))
        {
            return false;
        }
        !self.standing.iter().any(|&(at, _)| {
            let held = self.schedule[at].view;
            held == step.view || held == step.producer
        })
    }

    /// Start step `i`: run it to its first round, or its end.
    fn start(&mut self, host: &mut impl DriveHost, i: usize) -> Result<(), String> {
        let Step { view, producer, last, relay, .. } = self.schedule[i];
        // A relayed step reads its round's rows: the round's first reader runs it.
        if let Some(slot) = relay.filter(|slot| slot.first) {
            debug_assert_eq!(slot.at, self.relays.len(), "rounds start in slot order");
            let (rows, plan) = relay_operand(host, view, producer, self.outputs.get_mut(&producer), last, slot.fold);
            self.relays.push(None);
            let fold = slot.fold;
            self.standing.push_back((i, Stage::Relay { rows, plan, fold }));
            return Ok(());
        }
        self.start_epoch(host, i)
    }

    /// Start step `i`'s epoch over its [`Self::feed`], lent where a later step
    /// reads it too.
    fn start_epoch(&mut self, host: &mut impl DriveHost, i: usize) -> Result<(), String> {
        let Step { view, producer, .. } = self.schedule[i];
        let (input, take) = self.feed(i);
        match start_epoch(host, view, producer, input, take, &[])? {
            Some(ran) => self.ran(host, i, ran),
            None => Ok(()),
        }
    }

    /// Step `i`'s epoch ran to `ran`: a round, where it stands, or its end.
    fn ran(&mut self, host: &mut impl DriveHost, i: usize, ran: vm::Ran) -> Result<(), String> {
        match ran {
            vm::Ran::Round(epoch) => {
                self.standing.push_back((i, Stage::Epoch(epoch)));
                Ok(())
            }
            vm::Ran::Done(out) => self.complete(host, i, out),
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
            while next < self.schedule.len() && self.ready(next) {
                self.start(host, next)?;
                next += 1;
            }
            if self.standing.is_empty() {
                debug_assert_eq!(next, self.schedule.len(), "a step waits on no round");
                return Ok(());
            }
            let schedule = &self.schedule;
            let lanes = self.standing.iter_mut().map(|(i, stage)| {
                let (batch, plan, fold) = match stage {
                    // A relay's rows are its round's to free once sent.
                    Stage::Relay { rows, plan, fold } => (Cow::Owned(rows.take()), &**plan, *fold),
                    Stage::Epoch(epoch) => epoch.round(),
                };
                Lane {
                    view: schedule[*i].view,
                    batch,
                    plan,
                    fold,
                }
            });
            host.exchange(lanes, false, &mut self.gathered);
            let mut gathered = std::mem::take(&mut self.gathered);
            debug_assert_eq!(self.standing.len(), gathered.len(), "a lane per round");
            for rows in gathered.drain(..) {
                let (i, stage) = self.standing.pop_front().expect("a lane per round");
                match stage {
                    Stage::Relay { .. } => {
                        let slot = self.schedule[i].relay.expect("a relay step");
                        self.relays[slot.at] = Some(rows);
                        self.start_epoch(host, i)?;
                    }
                    Stage::Epoch(epoch) => {
                        let ran = resume_epoch(host, self.schedule[i].view, epoch, rows, &[])?;
                        self.ran(host, i, ran)?;
                    }
                }
            }
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
    let ran = run.run(host);
    // A failed step leaves the others' epochs at their rounds: dropped here,
    // registers and all.
    run.outputs.clear();
    run.relays.clear();
    run.standing.clear();
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
