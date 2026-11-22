//! Epoch execution: a view's program run round by round, and the DAG
//! evaluation driver.

use super::*;
use std::collections::VecDeque;

use gnitz_store::relation::{Landing, PkRule};

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

/// Fill `views`, sorted and closed under dependents, from each relation outside
/// the set that a member scans, read at its cut: rows above it reach the views
/// with the tick that seals them. Every global aggregate's ground row is minted
/// first. Then each member in id order drains its sources outside the set, in
/// scan order, each chunk run through one schedule: the member's epoch over it
/// and, in the same run, every member downstream of it over the deltas it
/// emits. A member's sources inside the set reach it only as
/// those deltas, so a chain segment's rows go to its readers as a tick's do, and
/// its store holds none of them.
pub(crate) fn backfill(host: &mut impl DriveHost, views: &[u64]) -> Result<(), String> {
    let dag = host.parts().0;
    debug_assert!(views.is_sorted(), "a backfill fills its views in id order");
    debug_assert!(
        views
            .iter()
            .all(|&v| dag.dependents_of(v).iter().all(|d| views.binary_search(d).is_ok())),
        "a backfilled view's readers are backfilled with it: each would otherwise read its deltas twice"
    );
    let mut run = std::mem::take(&mut dag.tick);
    let ran = run.backfill(host, views);
    // A failed ground-row landing leaves its rows held.
    run.clear();
    host.parts().0.tick = run;
    ran
}

/// What a run feeds the views it steps.
#[derive(Clone, Copy)]
enum Pass {
    /// A tick, its views' outputs captured as `round` by the delta feeds.
    Tick { round: u64 },
    /// A backfill feeding `member`, which captures nothing. A fill run steps
    /// only `member` and the views above it: `member` has been fed its first
    /// `fed` sources, in scan order, and every other view the run steps none.
    Fill { member: u64, fed: usize },
}

impl Default for Pass {
    fn default() -> Self {
        Pass::Tick { round: 0 }
    }
}

impl Pass {
    /// How many of `view`'s sources, in scan order, it has been fed: every
    /// later one reads empty.
    fn fed(self, view: u64) -> usize {
        match self {
            Pass::Tick { .. } => usize::MAX,
            Pass::Fill { member, fed } => {
                debug_assert!(view >= member, "a fill run steps no view below its member");
                if view == member { fed } else { 0 }
            }
        }
    }
}

/// The round a step of the running tick stands at.
enum Stage {
    /// Its relay round, which sends what the round's slot holds under this plan.
    Relay(Rc<ScatterPlan>),
    /// A round of its epoch.
    Epoch(vm::Epoch),
}

/// One run of a schedule, in waves: steps start in schedule order, each run to
/// its first round or its end, up to the first that cannot start yet; then the
/// rounds every step stands at run as one [`DriveHost::exchange`]. Each epoch
/// sends and gathers exactly the rows a round of its own would; only the waits
/// are shared.
///
/// A step starts only once no step standing at a round is of its view or of its
/// producer. Besides its producer's output, a view's epoch reads only its own
/// operator traces and store and base tables' stores at their sealed cut, none
/// of which another view's step moves: a view's store is never another view's
/// integral. A round's lanes follow the schedule, the same on every worker. An
/// epoch at a round holds its registers, and with them its own copy of a delta
/// it reads again, until the round gathers, so a run holds the state of every
/// epoch of a wave at once.
///
/// A tick runs its source's schedule once. A backfill runs one over its
/// ground rows, and one per chunk it feeds a member.
///
/// The engine keeps one between runs, empty, for its capacity.
#[derive(Default)]
pub(super) struct TickRun {
    schedule: Rc<[Step]>,
    pass: Pass,
    /// The batch each producer of the run has emitted, keyed by producer. A
    /// producer with no entry produced nothing.
    outputs: FxHashMap<u64, Batch>,
    /// Each relay round's rows, indexed by [`schedule::RelaySlot`]: what it sends
    /// from its start until it is sent, then nothing until it gathers, then
    /// what it gathered until its last reader. A round is out while the step
    /// that runs it stands at it.
    relays: Vec<Batch>,
    /// The steps standing at a round, each by its index in the schedule, in
    /// schedule order: the order their lanes go out. A step run on from a
    /// gathered round that stands again goes behind the rest of its wave.
    standing: VecDeque<(usize, Stage)>,
    /// What a wave's rounds gathered, one batch per step at a round.
    gathered: Vec<Batch>,
}

impl TickRun {
    /// Whether step `i` can start: no step at a round is of its view or of its
    /// producer, or runs the relay round it reads.
    fn ready(&self, i: usize) -> bool {
        if self.standing.is_empty() {
            return true;
        }
        let step = &self.schedule[i];
        let relay = step.relay.map(|slot| slot.at);
        !self.standing.iter().any(|(j, stage)| {
            let held = &self.schedule[*j];
            held.view == step.view
                || held.view == step.producer
                || matches!(stage, Stage::Relay(_)) && held.relay.map(|slot| slot.at) == relay
        })
    }

    /// Start step `i`, to its first round or its end, over its delta: what its
    /// relay round gathered, or its producer's output, lent where a later step
    /// reads it too. The first step to reach a relay round stands at the round
    /// instead, and starts again once it has gathered.
    fn start(&mut self, host: &mut impl DriveHost, i: usize) -> Result<(), String> {
        let Step { view, producer, last, relay } = self.schedule[i];
        let fed = self.pass.fed(view);
        let (dag, registry) = host.parts();
        // A view's run reaches it only from a source its circuit scans.
        let registered = dag.registered(view)?;
        let (input, take) = match relay {
            None => (self.outputs.get_mut(&producer), last),
            Some(slot) if slot.at == self.relays.len() => {
                self.start_relay(&registered.code, i, slot);
                return Ok(());
            }
            Some(slot) => (Some(&mut self.relays[slot.at]), slot.last),
        };
        let reg = registered.code.scan(producer).seed;
        let seed = input.filter(|input| !input.is_empty());
        // An empty delta still runs its round, on every worker, whatever this one
        // holds. One that runs none is skipped before its view's stores are opened.
        if seed.is_none() && registered.code.vm.idles_on_empty(reg) {
            return Ok(());
        }
        let (vm, mut stores) = registered.opened(registry, view, fed)?;
        let ran = vm::start(vm, &mut stores, reg, seed, take)?;
        self.ran(host, i, ran)
    }

    /// Run step `i`'s `epoch` on from the round it stood at, over what the round
    /// gathered.
    fn resume(&mut self, host: &mut impl DriveHost, i: usize, epoch: vm::Epoch, rows: Batch) -> Result<(), String> {
        let view = self.schedule[i].view;
        // The exchange held the host: the plan and its stores are looked up again.
        let (dag, registry) = host.parts();
        let (vm, mut stores) = dag.registered(view)?.opened(registry, view, self.pass.fed(view))?;
        let ran = vm::resume(vm, &mut stores, epoch, rows)?;
        self.ran(host, i, ran)
    }

    /// Step `i`'s epoch ran to `ran`: a round, where it stands, or its end.
    fn ran(&mut self, host: &mut impl DriveHost, i: usize, ran: vm::Ran) -> Result<(), String> {
        match ran {
            vm::Ran::Round(epoch) => {
                self.standing.push_back((i, Stage::Epoch(epoch)));
                Ok(())
            }
            vm::Ran::Done(out) => self.land(host, self.schedule[i].view, out),
        }
    }

    /// Stand step `i`, the first reader of relay round `slot`, at the round,
    /// which runs on every worker, whatever this one holds: it sends the
    /// producer's output, taken where no later step reads it. `code`: the
    /// step's view's plan.
    fn start_relay(&mut self, code: &CompileOutput, i: usize, slot: schedule::RelaySlot) {
        let Step { producer, last, .. } = self.schedule[i];
        let scan = code.scan(producer);
        let plan = Rc::clone(&scan.relay.as_ref().expect("a relayed scan has a relay").plan);
        let rows = match self.outputs.get_mut(&producer) {
            None => Batch::empty_with_schema(code.vm.schema_of(scan.seed)),
            Some(input) => {
                // Every worker sends its rows, and decodes theirs, under the
                // seed's label.
                code.vm.label(scan.seed, input);
                // Every worker gathers every row of a broadcast: one fold at
                // the sender serves them all.
                if slot.fold && plan.is_broadcast() {
                    input.consolidate_in_place();
                }
                if last { input.take() } else { input.clone() }
            }
        };
        self.relays.push(rows);
        self.standing.push_back((i, Stage::Relay(plan)));
    }

    /// Ingest `out`, `view`'s output, unless the view passes it through, and hold
    /// it for its readers where a view scans it: each is a later step of the run.
    fn land(&mut self, host: &mut impl DriveHost, view: u64, out: Batch) -> Result<(), String> {
        if out.is_empty() {
            return Ok(());
        }
        let (dag, registry) = host.parts();
        let needed = dag.is_scanned(view);
        let round = match self.pass {
            Pass::Tick { round } => Some(round),
            Pass::Fill { .. } => None,
        };
        let echo = match dag.views[&view].passes_through {
            // Its delta reaches its readers in this run.
            true => needed.then_some(out),
            false => registry.land(view, out, Landing::Cut { round, echo: needed }, PkRule::Enforce)?,
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

    /// Run `schedule` as `pass` over `seed`, a producer's output, beside what
    /// the run already holds, and drop what it leaves. `drained`: what the
    /// run's first round passes to [`DriveHost::exchange`]; each round passes
    /// what the last answered. Answers what the last answered, `drained` where
    /// no round ran.
    fn run(
        &mut self,
        host: &mut impl DriveHost,
        schedule: Rc<[Step]>,
        pass: Pass,
        seed: Option<(u64, Batch)>,
        drained: bool,
    ) -> Result<bool, String> {
        self.schedule = schedule;
        self.pass = pass;
        // Room for the schedule: growing the map rehashes it once per doubling.
        self.outputs.reserve(self.schedule.len());
        self.outputs.extend(seed);
        let ran = self.waves(host, drained);
        self.clear();
        ran
    }

    /// Run every step: start what can start, then the rounds it stands at as one.
    fn waves(&mut self, host: &mut impl DriveHost, mut drained: bool) -> Result<bool, String> {
        let mut next = 0;
        loop {
            while next < self.schedule.len() && self.ready(next) {
                self.start(host, next)?;
                next += 1;
            }
            if self.standing.is_empty() {
                debug_assert_eq!(next, self.schedule.len(), "a step waits on no round");
                return Ok(drained);
            }
            let (schedule, relays) = (&self.schedule, &mut self.relays);
            let lanes = self.standing.iter_mut().map(|(i, stage)| {
                let step = &schedule[*i];
                let (batch, plan, fold) = match stage {
                    // A relay's rows are its round's to free once sent.
                    Stage::Relay(plan) => {
                        let slot = step.relay.expect("a relay step");
                        (Cow::Owned(relays[slot.at].take()), &**plan, slot.fold)
                    }
                    Stage::Epoch(epoch) => epoch.round(),
                };
                Lane { view: step.view, batch, plan, fold }
            });
            drained = host.exchange(lanes, drained, &mut self.gathered);
            let mut gathered = std::mem::take(&mut self.gathered);
            debug_assert_eq!(self.standing.len(), gathered.len(), "a lane per round");
            for rows in gathered.drain(..) {
                let (i, stage) = self.standing.pop_front().expect("a lane per round");
                match stage {
                    Stage::Relay(_) => {
                        let slot = self.schedule[i].relay.expect("a relay step");
                        self.relays[slot.at] = rows;
                        self.start(host, i)?;
                    }
                    Stage::Epoch(epoch) => self.resume(host, i, epoch, rows)?,
                }
            }
            self.gathered = gathered;
        }
    }

    /// Drop what a run leaves: a failed step leaves the others' epochs at their
    /// rounds, dropped here, registers and all.
    fn clear(&mut self) {
        self.outputs.clear();
        self.relays.clear();
        self.standing.clear();
        self.gathered.clear();
    }

    /// [`backfill`] of `views`.
    fn backfill(&mut self, host: &mut impl DriveHost, views: &[u64]) -> Result<(), String> {
        let Some(&first) = views.first() else {
            return Ok(());
        };
        // No member has been fed a row yet.
        let pass = Pass::Fill { member: first, fed: 0 };
        self.pass = pass;
        // Every ground row lands in its view before any row reaches the view: a
        // reduce retracts its group's stored row, and a ground row minted over
        // rows it already holds would stand beside them.
        let mut minting = Vec::new();
        for &view in views {
            let (dag, registry) = host.parts();
            let registered = dag.registered(view)?;
            // Worker-identical: every worker runs the schedule that feeds it on.
            if !registered.code.vm.mints_ground_rows() {
                continue;
            }
            minting.push(view);
            let replicated = registry.relation_or_err(view)?.placement().is_replicated();
            if mints_ground_row(registry.slot(), replicated) {
                let (vm, mut stores) = registered.opened(registry, view, 0)?;
                let rows = vm::mint_ground_rows(vm, &mut stores)?;
                self.land(host, view, rows)?;
            }
        }
        if !minting.is_empty() {
            let schedule = host.parts().0.schedule_from(&minting, Vec::new());
            self.run(host, schedule, pass, None, false)?;
        }
        for &view in views {
            self.drain_into(host, view, views)?;
            // Once, not per chunk: the view's first reads open over fewer runs.
            // Nothing lands in it after its own sources: only lower ids feed it.
            host.parts().1.settle(view)?;
        }
        Ok(())
    }

    /// Feed member `view` of the backfilled `views` each source outside the set,
    /// in scan order, a chunk a run, against the sources fed before it.
    fn drain_into(&mut self, host: &mut impl DriveHost, view: u64, views: &[u64]) -> Result<(), String> {
        let (dag, registry) = host.parts();
        let sources: Vec<u64> = dag.sources_of(view).collect();
        let chunk_rows = registry.scan_chunk_rows();
        for (at, &source) in sources.iter().enumerate() {
            if views.binary_search(&source).is_ok() {
                continue;
            }
            let (dag, registry) = host.parts();
            debug_assert!(
                !dag.views.get(&source).is_some_and(|v| v.passes_through),
                "a passthrough view holds no rows: it is backfilled with its readers"
            );
            let relation = registry.relation_or_err(source)?;
            // A stream holds no rows: every worker skips it alike, so no round is owed.
            if relation.kind() == RelationKind::Stream {
                continue;
            }
            // `sources` came from this view's own plan.
            let scan = dag.views[&view].code.scan(source);
            let mut cursor = relation.open_sealed(scan.bound.clone(), scan.read)?;
            let schedule = dag.schedule_from(&[view], vec![(view, source)]);
            // The source joins those fed before it as it drains.
            let pass = Pass::Fill { member: view, fed: at + 1 };
            loop {
                let chunk = cursor.drain_chunk(chunk_rows);
                // Until every worker is drained: all run the same rounds.
                let drained = chunk.is_none();
                let seed = chunk.map(|chunk| (source, chunk));
                if self.run(host, Rc::clone(&schedule), pass, seed, drained)? {
                    break;
                }
            }
        }
        Ok(())
    }
}

/// Run `source`'s tick schedule over `delta`, `None` where it brings this process
/// no row, and ingest every view's output into its family.
// Inlined: `delta` is taken by value, and at a call it is copied whole.
#[inline(always)]
fn run_tick(host: &mut impl DriveHost, source: u64, round: u64, delta: Option<Batch>) -> Result<(), String> {
    let dag = host.parts().0;
    let mut run = std::mem::take(&mut dag.tick);
    let schedule = dag.tick_schedule(source);
    let seed = delta.map(|delta| (source, delta));
    let ran = run.run(host, schedule, Pass::Tick { round }, seed, false);
    host.parts().0.tick = run;
    ran.map(drop)
}

#[cfg(test)]
#[path = "tests/exec.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/exec.rs"]
mod bench;
