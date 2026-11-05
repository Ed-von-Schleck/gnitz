//! The group-commit task: every user write's SAL groups, batched from whatever
//! is queued behind the first request (see [`drain_ready_batch`]). A write's
//! table locks are dropped once it is laid out, when its writer is answered;
//! the writer then waits for the SAL's fdatasync to cover it. Checkpoints,
//! backups and DDL pauses end a batch. Once serving, it is the one task that
//! runs a checkpoint.

use super::validate::{FamilyRows, TxnFamily};
use super::write::HeldTables;
use super::Shared;
use crate::runtime::signals;
use crate::runtime::sync::{chan, oneshot};
use gnitz_store::relation::PkRule;
use gnitz_wire::{WireFault, WireStatus};
use gnitz_zset::repr::Batch;
use rustc_hash::FxHashMap;
use smallvec::{smallvec, SmallVec};
use std::rc::Rc;

/// Row ceiling on one committer batch. Tested before the receive, so a batch is
/// capped at this plus one whole request — and one request is itself bounded
/// only by `MAX_FRAME_PAYLOAD`.
const MAX_PENDING_ROWS: usize = 100_000;

/// One request to the committer.
enum CommitRequest {
    /// Commit one write.
    Write(PendingWrite),
    /// Wake the committer so it re-tests `needs_checkpoint`.
    Reclaim,
    /// End the batch of what is queued ahead, and run `BatchEnd` once it has
    /// committed.
    End(BatchEnd),
}

/// A request that ends a committer batch, run once the batch has committed.
enum BatchEnd {
    /// The checkpoint sequence over everything committed. A failed round
    /// aborts, so it has no error to report.
    Checkpoint(oneshot::Sender<()>),
    /// The checkpoint sequence, then every published store put into `dir`,
    /// with nothing committed in between.
    Backup {
        dir: String,
        done: oneshot::Sender<Result<(), WireFault>>,
    },
    /// Signal `parked`, then run nothing until `resume`.
    Pause {
        parked: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    },
}

/// The committer paused: it runs no batch, and so no checkpoint, until this is
/// dropped.
pub(super) struct Paused(Option<oneshot::Sender<()>>);

impl Drop for Paused {
    fn drop(&mut self) {
        if let Some(resume) = self.0.take() {
            resume.send(());
        }
    }
}

/// One write awaiting commit, all-or-nothing: each family is one `Push` group,
/// in frame order.
struct PendingWrite {
    families: Vec<TxnFamily>,
    waiter: Waiter,
}

/// The client of one write: the rows it wrote, and the table locks it holds
/// until the write is laid out in the SAL, when `done` resolves to the write's
/// commit LSN or to the error that rolled the whole write back.
struct Waiter {
    done: oneshot::Sender<Result<u64, WireFault>>,
    rows: usize,
    held: HeldTables,
}

/// The way to the committer task. A write and a backup are refused once a
/// graceful shutdown has been requested; the pause, the reclaim poke and the
/// final checkpoint itself are always queued.
///
/// Everything a refusing request admits is queued ahead of the final
/// checkpoint: the check and the send share no await, and the signal loop
/// sends that checkpoint only after it has itself seen the flag, which it
/// cannot do until the admitting task yields.
pub(super) struct Committer {
    tx: chan::Sender<CommitRequest>,
}

/// The requests [`run`] receives.
pub(super) struct CommitRequests(chan::Receiver<CommitRequest>);

impl Committer {
    /// The committer's handle, and the requests [`run`] is to be spawned on.
    pub(super) fn new() -> (Committer, CommitRequests) {
        let (tx, rx) = chan::unbounded();
        (Committer { tx }, CommitRequests(rx))
    }

    /// Lay out `families` as one write, dropping `held` once it is; resolves
    /// then to the write's commit LSN, which is not yet durable.
    pub(super) async fn write(&self, families: Vec<TxnFamily>, held: HeldTables) -> Result<u64, WireFault> {
        let rows = families.iter().map(|f| f.rows.view().len()).sum();
        self.admit(|done| {
            let waiter = Waiter { done, rows, held };
            CommitRequest::Write(PendingWrite { families, waiter })
        })?
        .await
    }

    /// Checkpoint, then copy every published store into `dir`.
    pub(super) async fn backup(&self, dir: String) -> Result<(), WireFault> {
        self.admit(|done| CommitRequest::End(BatchEnd::Backup { dir, done }))?
            .await
    }

    /// Resolves once the batch queued ahead is laid out and the committer has
    /// parked. A checkpoint the SAL needs runs ahead of the park.
    pub(super) async fn pause(&self) -> Paused {
        let (resume, on_resume) = oneshot::channel();
        // Built before the await, so a dropped caller still resumes the committer.
        let paused = Paused(Some(resume));
        let parked = self.queue(|parked| CommitRequest::End(BatchEnd::Pause { parked, resume: on_resume }));
        parked.await;
        paused
    }

    /// Whether a request waits for the committer to take it.
    pub(super) fn has_queued(&self) -> bool {
        !self.tx.is_empty()
    }

    /// Resolves once every write queued ahead has committed and a checkpoint
    /// sequence has run over them.
    pub(super) fn final_checkpoint(&self) -> oneshot::Receiver<()> {
        self.queue(|done| CommitRequest::End(BatchEnd::Checkpoint(done)))
    }

    /// Wake the committer so it re-tests `needs_checkpoint`.
    pub(super) fn reclaim(&self) {
        self.tx.send(CommitRequest::Reclaim);
    }

    fn admit<T>(
        &self,
        req: impl FnOnce(oneshot::Sender<T>) -> CommitRequest,
    ) -> Result<oneshot::Receiver<T>, WireFault> {
        if signals::shutdown_requested() {
            return Err("server shutting down".to_string().into());
        }
        Ok(self.queue(req))
    }

    fn queue<T>(&self, req: impl FnOnce(oneshot::Sender<T>) -> CommitRequest) -> oneshot::Receiver<T> {
        let (done, rx) = oneshot::channel();
        self.tx.send(req(done));
        rx
    }
}

/// The buffers one committer batch is built in, kept for the boot: the
/// committer is one serial task, so no two batches use them at once.
#[derive(Default)]
struct CommitScratch {
    /// The batch's writes, and after [`commit_writes`] the ones to commit again.
    writes: Vec<PendingWrite>,
    /// The batch's commit units; empty between batches.
    units: Vec<CommitUnit>,
    /// Per table, the last of `units` that writes it; empty between batches.
    last_unit: FxHashMap<u64, usize>,
    /// Per unit, the batches merged behind its one family's; empty between
    /// batches.
    tails: Vec<Vec<FamilyRows>>,
}

/// The committer task loop. Never returns.
pub(super) async fn run(CommitRequests(mut rx): CommitRequests, shared: Rc<Shared>) {
    let mut s = CommitScratch::default();
    loop {
        let first = rx.recv().await;

        let mut end = drain_ready_batch(&mut rx, first, &mut s.writes);
        // While the fdatasync in flight covers everything published, this
        // batch is durable at the next either way. Laid out now, it releases
        // its table locks; unless a write is parked on one, it waits that
        // fdatasync out and takes in what queued meanwhile.
        let sal = shared.dispatcher.sal();
        if end.is_none() && sal.syncing_all() && !s.writes.iter().any(|w| w.waiter.held.contended()) {
            sal.durable_through(sal.watermark()).await;
            if let Some(next) = rx.try_recv() {
                end = drain_ready_batch(&mut rx, next, &mut s.writes);
            }
        }

        // Before this batch's groups: the checkpoint makes room for them.
        if shared.dispatcher.sal().needs_checkpoint() {
            run_checkpoint_sequence(&shared).await;
        }

        // A write the SAL refused for want of room goes again behind a
        // checkpoint, merged with none, until each has committed or been refused
        // on an empty log.
        let mut merge = true;
        while !s.writes.is_empty() {
            commit_writes(&shared, &mut s, merge).await;
            if !s.writes.is_empty() {
                run_checkpoint_sequence(&shared).await;
                merge = false;
            }
        }

        match end {
            None => {}
            Some(BatchEnd::Checkpoint(done)) => {
                run_checkpoint_sequence(&shared).await;
                done.send(());
            }
            Some(BatchEnd::Backup { dir, done }) => {
                run_checkpoint_sequence(&shared).await;
                done.send(shared.dispatcher.backup_round(&dir).await);
            }
            Some(BatchEnd::Pause { parked, resume }) => {
                parked.send(());
                resume.await;
            }
        }
    }
}

/// Push `first` onto `writes`, then keep draining requests already queued
/// behind it — without ever waiting — until the row cap, counted over all of
/// `writes`, is reached, an `End` request arrives, or the channel runs dry.
/// Returns the request that ended the batch, if one did.
///
/// An `End` request ends the batch wherever it lands, `first` included: what is
/// queued behind it is younger than whatever issued it, so folding that in would
/// make it wait on a commit it has no ordering interest in. It rides the
/// next `rx.recv()`, which this loop reaches immediately.
fn drain_ready_batch(
    rx: &mut chan::Receiver<CommitRequest>,
    first: CommitRequest,
    writes: &mut Vec<PendingWrite>,
) -> Option<BatchEnd> {
    let mut row_count = writes.iter().map(|w| w.waiter.rows).sum::<usize>();
    let mut next = Some(first);
    while let Some(req) = next {
        match req {
            // A write is one indivisible entry: its whole family set rides this
            // batch, and counts every family's rows.
            CommitRequest::Write(w) => {
                row_count += w.waiter.rows;
                writes.push(w);
            }
            CommitRequest::End(end) => return Some(end),
            CommitRequest::Reclaim => {}
        }
        next = (row_count < MAX_PENDING_ROWS).then(|| rx.try_recv()).flatten();
    }
    None
}

/// The checkpoint sequence: base round, drain, ephemeral round. A failed round
/// aborts: the workers are re-epoched against an un-reset SAL, which only a
/// restart's replay repairs.
async fn run_checkpoint_sequence(shared: &Shared) {
    shared
        .dispatcher
        .checkpoint_base()
        .await
        .unwrap_or_else(|e| gnitz_fatal_abort!("{e}"));

    // One drain suffices: no write is committed meanwhile, so the pending-tick
    // counts cannot grow.
    // Stamping after a failed drain would make the views' missing deltas
    // generation-valid. Unstamped, they stay behind the generation the base
    // round that flushed those deltas' rows advanced to, and are rebuilt at the
    // next boot. Not an abort: this path also serves the shutdown checkpoint.
    if let Err(e) = shared.ticks.drain(shared.dispatcher.reactor().now()).await {
        gnitz_warn!("checkpoint drain failed, skipping the ephemeral round: {}", e);
        return;
    }

    // The ephemeral round runs with no tick in flight.
    let _park = shared.ticks.gate.write().await;
    shared
        .dispatcher
        .checkpoint_ephemeral()
        .await
        .unwrap_or_else(|e| gnitz_fatal_abort!("{e}"));
}

/// One client-visible commit unit, all-or-nothing: the SAL groups it emits and
/// the clients waiting on their shared verdict.
struct CommitUnit {
    outcome: Outcome,
    /// The unit's clients in arrival order: a write's one, or a merged run's
    /// several.
    waiters: SmallVec<[Waiter; 1]>,
}

/// Where a [`CommitUnit`] stands.
enum Outcome {
    /// One group each, in frame order. A run of one-family writes to one table
    /// is one unit of one family, their batches merged. `in_zone` once laid out:
    /// whether one of its groups rode inside the zone, which a restart recovers.
    Laid { families: Vec<TxnFamily>, in_zone: bool },
    /// Why none of the unit's writes committed.
    Failed(WireFault),
}

impl CommitUnit {
    /// The families the unit laid out; none once it failed.
    fn laid(&self) -> &[TxnFamily] {
        match &self.outcome {
            Outcome::Laid { families, .. } => families,
            Outcome::Failed(_) => &[],
        }
    }

    /// A run of several clients' writes merged into one.
    fn is_merged(&self) -> bool {
        self.waiters.len() > 1
    }

    /// The commit LSN of a laid unit: the zone LSN when one of its groups rode
    /// inside the zone, which a restart recovers, else `0`.
    fn lsn(&self, zone_lsn: u64) -> u64 {
        match self.outcome {
            Outcome::Laid { in_zone: true, .. } => zone_lsn,
            _ => 0,
        }
    }

    /// The unit as the writes it was built from: a merged run split back into
    /// its clients' pushes, by the rows each wrote.
    fn into_writes(self) -> Vec<PendingWrite> {
        let Outcome::Laid { mut families, .. } = self.outcome else {
            unreachable!("only a unit not yet laid out is taken back")
        };
        if self.waiters.len() == 1 {
            let waiter = self.waiters.into_iter().next().expect("a unit answers one client");
            return vec![PendingWrite { families, waiter }];
        }
        let TxnFamily { tid, mode, rows, pk_rule } = families.pop().expect("a merged run is one family");
        let batch = rows.into_batch();
        let mut start = 0;
        (self.waiters.into_iter())
            .map(|waiter| {
                let range = (start, start + waiter.rows);
                start += waiter.rows;
                let rows = FamilyRows::Built(Batch::from_ranges(&batch, &[range], 0));
                PendingWrite {
                    families: vec![TxnFamily { tid, mode, rows, pk_rule }],
                    waiter,
                }
            })
            .collect()
    }

    /// Drop every client's table locks and answer it: [`CommitUnit::lsn`] for
    /// a laid unit, the error for a failed one.
    fn resolve(self, zone_lsn: u64) {
        let result = match self.outcome {
            Outcome::Laid { .. } => Ok(self.lsn(zone_lsn)),
            Outcome::Failed(e) => Err(e),
        };
        for Waiter { done, held, .. } in self.waiters {
            drop(held);
            done.send(result.clone());
        }
    }
}

/// Build `s.units` from `s.writes`, in arrival order: one unit per write, but
/// under `merge` a one-family write joins the unit of the last write before it
/// to its table when that is a one-family write too, so it moves past only
/// writes to other tables. A unit's batches stay in arrival order, which is
/// what makes intra-batch last-insert-wins mean last *inserted*. No unit a
/// write merges into is held to [`PkRule::Fresh`]: such a write holds its
/// table's exclusive lock until its unit resolves, after this, so no other
/// write to the table is in the batch.
fn build_units(s: &mut CommitScratch, merge: bool) {
    s.units.reserve(s.writes.len());
    for PendingWrite { families, waiter } in s.writes.drain(..) {
        if let ([f], true) = (&families[..], merge) {
            if let Some(&at) = s.last_unit.get(&f.tid) {
                if matches!(s.units[at].laid(), [g] if g.tid == f.tid) {
                    debug_assert!([f, &s.units[at].laid()[0]].iter().all(|g| g.pk_rule == PkRule::Enforce));
                    s.units[at].waiters.push(waiter);
                    s.tails[at].extend(families.into_iter().map(|f| f.rows));
                    continue;
                }
            }
        }
        for f in &families {
            s.last_unit.insert(f.tid, s.units.len());
        }
        s.units.push(CommitUnit {
            outcome: Outcome::Laid { families, in_zone: false },
            waiters: smallvec![waiter],
        });
        // A unit nothing merges into builds no `Vec`.
        s.tails.push(Vec::new());
    }
    s.last_unit.clear();
    for (unit, tail) in s.units.iter_mut().zip(s.tails.drain(..)) {
        if tail.is_empty() {
            continue;
        }
        let Outcome::Laid { families, .. } = &mut unit.outcome else {
            unreachable!("no unit has been laid out yet")
        };
        let head = &mut families[0];
        let first = head.rows.view();
        let merged = Batch::concat(
            first.schema(),
            std::iter::once(first).chain(tail.iter().map(FamilyRows::view)),
        );
        head.rows = FamilyRows::Built(merged);
    }
}

/// Commit the batch of writes in `s.writes`. Lays out every group and queues
/// their tids for the tick under one SAL hold, then raises the validation
/// caches and commit LSNs, and drops the writers' table locks as it answers
/// them.
///
/// Leaves in `s.writes` the writes to commit again behind a checkpoint: the
/// first unit the SAL had no room for, and every unit after it. A unit of one
/// write too large for a log with nothing else in its epoch fails instead. A
/// merged run comes back as the writes it merged, which a batch that does not
/// `merge` commits one by one. `s.units` is empty again on return.
async fn commit_writes(shared: &Shared, s: &mut CommitScratch, merge: bool) {
    // ------------------------------------------------------------------
    // Phase A (no lock): the units.
    // ------------------------------------------------------------------
    build_units(s, merge);

    // ------------------------------------------------------------------
    // Phase B (under the SAL writer): emit SAL groups, queue their tids for the
    // tick. The batch's recoverable groups form one zone.
    // ------------------------------------------------------------------
    let (zone_lsn, again) = {
        let disp = &shared.dispatcher;
        let mut excl = disp.sal().lock().await;

        // Nothing laid out inside the scope is visible until it commits, so a
        // write that runs out of SAL space part-way can take its earlier
        // families back. Every write and the commit are in this one synchronous
        // block, so no reader ever observes the gap.
        let mut scope = excl.begin("commit");
        let zone_lsn = scope.lsn();

        // A unit is all-or-nothing: a family that does not fit takes back the
        // unit's earlier families, which were never published.
        let mut refused_at = None;
        for (at, unit) in s.units.iter_mut().enumerate() {
            let mut unit_in_zone = false;
            let written = scope.all_or_nothing(|scope| {
                for f in unit.laid() {
                    unit_in_zone |= disp.write_commit_group(scope, f.tid, f.pk_rule, f.rows.view())?;
                }
                Ok(())
            });
            match written {
                Ok(()) => {
                    if let Outcome::Laid { in_zone, .. } = &mut unit.outcome {
                        *in_zone = unit_in_zone;
                    }
                }
                // A merged run goes again as the writes it merged; a unit of one
                // write, if a checkpoint can make room for it.
                Err(e)
                    if e.status == WireStatus::SalFull
                        && (unit.is_merged()
                            || disp
                                .commit_fits_empty_log(&scope, unit.laid().iter().map(|f| (f.tid, f.rows.view())))) =>
                {
                    refused_at = Some(at);
                    break;
                }
                Err(e) => unit.outcome = Outcome::Failed(e),
            }
        }
        // Allocates only on the refusal path; `s.units` keeps the units that
        // committed or failed.
        let again = refused_at.map(|at| s.units.split_off(at));

        // Started now rather than at the first writer's wait for it.
        if scope.commit() {
            disp.sal().want_durable(zone_lsn);
        }
        // The auto-tick this may fire overlaps the fdatasync.
        shared.note_commit_rows(
            &excl,
            s.units
                .iter()
                .flat_map(CommitUnit::laid)
                .map(|f| (f.tid, f.rows.view().len())),
        );
        (zone_lsn, again)
    };

    // The caches and commit LSNs, then the table locks. No OCC check of a
    // later write to one of the tables runs before the raise. Such a write is
    // laid out behind this batch, so the fdatasync that covers it covers this
    // batch too.
    for unit in s.units.drain(..) {
        let lsn = unit.lsn(zone_lsn);
        for f in unit.laid() {
            shared.writes.committed(&shared.cat(), f.tid, f.rows.view(), lsn);
            // After the raise, which is what a woken poll reads as staleness.
            shared.feeds.wake(f.tid);
        }
        unit.resolve(zone_lsn);
    }
    s.writes
        .extend(again.into_iter().flatten().flat_map(CommitUnit::into_writes));
}

#[cfg(test)]
#[path = "tests/committer.rs"]
mod tests;
