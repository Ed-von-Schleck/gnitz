//! The group-commit task: every user write's SAL groups, batched from whatever
//! is queued behind the first request (see [`drain_ready_batch`]), and the
//! fdatasync task beside it, which keeps one fdatasync in flight over every
//! batch laid out meanwhile (see [`sync_loop`]). A write's table locks are
//! dropped once it is laid out, and its writer is answered at the fdatasync
//! that covers it. Checkpoints, backups and DDL pauses end a batch.
//! Once serving, it is the one task that runs a checkpoint.

use super::validate::{FamilyRows, TxnFamily};
use super::write::HeldTables;
use super::Shared;
use crate::runtime::signals;
use crate::runtime::sync::{chan, oneshot};
use gnitz_wire::{WireFault, WireStatus};
use gnitz_zset::repr::Batch;
use rustc_hash::FxHashMap;
use smallvec::{smallvec, SmallVec};
use std::cell::Cell;
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

/// Where a write's commit LSN goes, or the error that rolled the whole write
/// back.
type Reply = oneshot::Sender<Result<u64, WireFault>>;

/// The client of one write: the rows it wrote, and the table locks it holds
/// until the write is laid out in the SAL. Its reply is sent at the fdatasync
/// that covers the write, once every write laid out ahead of it is answered.
///
/// Workers apply a group once it is published, so between the layout and the
/// reply a read, or a later write's validation, can see the write before a
/// restart would recover it. Nothing laid out is taken back: a failed
/// fdatasync aborts the process.
struct Waiter {
    done: Reply,
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

    /// Commit `families` as one write, dropping `held` once it is laid out;
    /// resolves to the write's commit LSN at the fdatasync that covers it.
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
    /// parked. A checkpoint the SAL needs runs ahead of the park. The batch's
    /// writers are answered at their fdatasync meanwhile.
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

/// One batch's replies, held for the fdatasync that covers it, or a barrier
/// answered once every job sent ahead of it has been.
enum SyncJob {
    Batch {
        /// Whether the batch opened a zone, which only an fdatasync makes
        /// durable.
        zoned: bool,
        /// The batch's writers in batch order, each with its commit LSN: a
        /// batch with no zone is answered behind the ones ahead of it.
        replies: Vec<(Reply, u64)>,
        /// The tables the batch wrote, woken for their feeds once it is durable.
        woken: SmallVec<[u64; 4]>,
    },
    Barrier(oneshot::Sender<()>),
}

/// The way to [`sync_loop`]: jobs are answered in the order sent.
struct Syncer {
    tx: chan::Sender<SyncJob>,
    /// Set while [`sync_loop`]'s fdatasync is in flight. Every job it took is
    /// answered as soon as that resolves, so with this clear and no job queued,
    /// every job sent is answered.
    in_flight: Rc<Cell<bool>>,
}

impl Syncer {
    fn send(&self, zoned: bool, replies: Vec<(Reply, u64)>, woken: SmallVec<[u64; 4]>) {
        self.tx.send(SyncJob::Batch { zoned, replies, woken });
    }

    /// Whether the fdatasync in flight covers every batch sent.
    fn covered(&self) -> bool {
        self.in_flight.get() && self.tx.is_empty()
    }

    /// Resolves once every job sent ahead is answered.
    async fn idle(&self) {
        if !self.in_flight.get() && self.tx.is_empty() {
            return;
        }
        let (done, rx) = oneshot::channel();
        self.tx.send(SyncJob::Barrier(done));
        rx.await
    }
}

/// The fdatasync task. An fdatasync covers every batch laid out before it is
/// submitted, so one in flight at a time serves them all: the batches laid out
/// while one is in flight share the next. Never returns.
async fn sync_loop(mut rx: chan::Receiver<SyncJob>, shared: Rc<Shared>, in_flight: Rc<Cell<bool>>) {
    let mut jobs = Vec::new();
    loop {
        jobs.push(rx.recv().await);
        while let Some(job) = rx.try_recv() {
            jobs.push(job);
        }
        if jobs.iter().any(|j| matches!(j, SyncJob::Batch { zoned: true, .. })) {
            // Submitted after every job's layout, so it covers all of them.
            in_flight.set(true);
            shared.dispatcher.sal().sync(shared.dispatcher.reactor()).await;
            in_flight.set(false);
        }
        for job in jobs.drain(..) {
            match job {
                SyncJob::Batch { replies, woken, .. } => {
                    for tid in woken {
                        // After the raise, which is what a woken poll reads as staleness.
                        shared.feeds.wake(tid);
                    }
                    for (done, lsn) in replies {
                        done.send(Ok(lsn));
                    }
                }
                SyncJob::Barrier(done) => done.send(()),
            }
        }
    }
}

/// The committer task loop. Never returns.
pub(super) async fn run(CommitRequests(mut rx): CommitRequests, shared: Rc<Shared>) {
    let mut s = CommitScratch::default();
    let (tx, sync_rx) = chan::unbounded();
    let in_flight = Rc::new(Cell::new(false));
    shared
        .dispatcher
        .reactor()
        .spawn(sync_loop(sync_rx, Rc::clone(&shared), Rc::clone(&in_flight)));
    let syncer = Syncer { tx, in_flight };
    loop {
        let first = rx.recv().await;

        let mut end = drain_ready_batch(&mut rx, first, &mut s.writes);
        // While the fdatasync in flight covers every batch sent, this one is
        // answered at the next either way. Laid out now, it releases its table
        // locks; unless a write is parked on one, it waits that fdatasync out
        // and takes in what queued meanwhile.
        if end.is_none() && syncer.covered() && !s.writes.iter().any(|w| w.waiter.held.contended()) {
            syncer.idle().await;
            if let Some(next) = rx.try_recv() {
                end = drain_ready_batch(&mut rx, next, &mut s.writes);
            }
        }

        // Before this batch's groups: the checkpoint makes room for them.
        if shared.dispatcher.sal().needs_checkpoint() {
            run_checkpoint_sequence(&shared, &syncer).await;
        }

        // A write the SAL refused for want of room goes again behind a
        // checkpoint, merged with none, until each has committed or been refused
        // on an empty log.
        let mut merge = true;
        while !s.writes.is_empty() {
            commit_writes(&shared, &mut s, merge, &syncer).await;
            if !s.writes.is_empty() {
                run_checkpoint_sequence(&shared, &syncer).await;
                merge = false;
            }
        }

        match end {
            None => {}
            Some(BatchEnd::Checkpoint(done)) => {
                run_checkpoint_sequence(&shared, &syncer).await;
                done.send(());
            }
            Some(BatchEnd::Backup { dir, done }) => {
                run_checkpoint_sequence(&shared, &syncer).await;
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

/// The checkpoint sequence, over nothing still awaiting its fdatasync: base
/// round, drain, ephemeral round. A failed round aborts: the workers are
/// re-epoched against an un-reset SAL, which only a restart's replay repairs.
async fn run_checkpoint_sequence(shared: &Shared, syncer: &Syncer) {
    syncer.idle().await;
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
        let TxnFamily { tid, mode, rows } = families.pop().expect("a merged run is one family");
        let batch = rows.into_batch();
        let mut start = 0;
        (self.waiters.into_iter())
            .map(|waiter| {
                let range = (start, start + waiter.rows);
                start += waiter.rows;
                let rows = FamilyRows::Built(Batch::from_ranges(&batch, &[range], 0));
                PendingWrite {
                    families: vec![TxnFamily { tid, mode, rows }],
                    waiter,
                }
            })
            .collect()
    }

    /// Drop every client's table locks, then answer a failed unit's clients
    /// with its error, and push a laid unit's onto `replies` with its
    /// [`CommitUnit::lsn`], for the fdatasync that covers it.
    fn resolve(self, zone_lsn: u64, replies: &mut Vec<(Reply, u64)>) {
        let lsn = self.lsn(zone_lsn);
        for Waiter { done, held, .. } in self.waiters {
            drop(held);
            match &self.outcome {
                Outcome::Laid { .. } => replies.push((done, lsn)),
                Outcome::Failed(e) => done.send(Err(e.clone())),
            }
        }
    }
}

/// Build `s.units` from `s.writes`, in arrival order: one unit per write, but
/// under `merge` a one-family write joins the unit of the last write before it
/// to its table when that is a one-family write too, so it moves past only
/// writes to other tables. A unit's batches stay in arrival order, which is
/// what makes intra-batch last-insert-wins mean last *inserted*.
fn build_units(s: &mut CommitScratch, merge: bool) {
    s.units.reserve(s.writes.len());
    for PendingWrite { families, waiter } in s.writes.drain(..) {
        if let ([f], true) = (&families[..], merge) {
            if let Some(&at) = s.last_unit.get(&f.tid) {
                if matches!(s.units[at].laid(), [g] if g.tid == f.tid) {
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
/// caches and commit LSNs, drops the writers' table locks, and hands their
/// replies to the syncer, which sends them once the batch is durable.
///
/// Leaves in `s.writes` the writes to commit again behind a checkpoint: the
/// first unit the SAL had no room for, and every unit after it. A unit of one
/// write too large for a log with nothing else in its epoch fails instead. A
/// merged run comes back as the writes it merged, which a batch that does not
/// `merge` commits one by one. `s.units` is empty again on return.
async fn commit_writes(shared: &Shared, s: &mut CommitScratch, merge: bool, syncer: &Syncer) {
    // ------------------------------------------------------------------
    // Phase A (no lock): the units.
    // ------------------------------------------------------------------
    build_units(s, merge);

    // ------------------------------------------------------------------
    // Phase B (under the SAL writer): emit SAL groups, queue their tids for the
    // tick. The batch's recoverable groups form one zone.
    // ------------------------------------------------------------------
    let (zone_lsn, zoned, again) = {
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
                    unit_in_zone |= disp.write_commit_group(scope, f.tid, f.rows.view())?;
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

        let zoned = scope.commit();
        // The auto-tick this may fire overlaps the fdatasync.
        shared.note_commit_rows(
            &excl,
            s.units
                .iter()
                .flat_map(CommitUnit::laid)
                .map(|f| (f.tid, f.rows.view().len())),
        );
        (zone_lsn, zoned, again)
    };

    // The caches and commit LSNs, then the table locks. No OCC check of a
    // later write to one of the tables runs before the raise. Such a write is
    // laid out behind this batch, so the fdatasync that covers it covers this
    // batch too.
    let mut woken = SmallVec::new();
    for unit in &s.units {
        let lsn = unit.lsn(zone_lsn);
        for f in unit.laid() {
            shared.writes.committed(&shared.cat(), f.tid, f.rows.view(), lsn);
            woken.push(f.tid);
        }
    }
    let mut replies = Vec::new();
    for unit in s.units.drain(..) {
        unit.resolve(zone_lsn, &mut replies);
    }
    s.writes
        .extend(again.into_iter().flatten().flat_map(CommitUnit::into_writes));

    // ------------------------------------------------------------------
    // Phase C: the batch's fdatasync, on the syncer. A batch of nothing but
    // stream groups opened no zone, so needs none, and is still answered in
    // order behind the batches ahead of it.
    // ------------------------------------------------------------------
    syncer.send(zoned, replies, woken);
}

#[cfg(test)]
#[path = "tests/committer.rs"]
mod tests;
