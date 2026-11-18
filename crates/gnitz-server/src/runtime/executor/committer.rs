//! The group-commit task: every user write's SAL groups, batched from whatever
//! is queued behind the first request (see [`drain_ready_batch`]). A write's
//! table locks are dropped once it is laid out, when its writer is answered;
//! the writer then waits for the SAL's fdatasync to cover it. Checkpoints,
//! backups and a DDL's settles end a batch. Once serving, it is the one task
//! that runs a checkpoint.

use super::Shared;
use super::family::{FamilyRows, TxnFamily};
use super::write::HeldTables;
use crate::runtime::sync::{Either, chan, oneshot, select2};
use gnitz_foundation::gnitz_warn;
use gnitz_store::relation::PkRule;
use gnitz_wire::WireFault;
use gnitz_zset::repr::Batch;
use rustc_hash::FxHashMap;
use smallvec::{SmallVec, smallvec};
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
    /// Nothing: its answer says the `needs_checkpoint` test of its batch has
    /// run, ahead of the batch's writes.
    Settle(oneshot::Sender<()>),
}

/// One write awaiting commit, all-or-nothing: its families in frame order. Its
/// client holds the table locks `held` until the write is laid out in the SAL,
/// when `done` resolves to the write's commit LSN or to the error that rolled
/// the whole write back.
struct PendingWrite {
    families: Vec<TxnFamily>,
    /// The rows of every family.
    rows: usize,
    held: HeldTables,
    done: oneshot::Sender<Result<u64, WireFault>>,
}

/// The way to the committer task. A write and a backup are refused once it is
/// closed; a settle is always queued.
pub(super) struct Committer {
    tx: chan::Sender<CommitRequest>,
    /// Set by [`Committer::close`], in the step that queues the final checkpoint.
    closed: Cell<bool>,
}

/// The requests [`run`] receives.
pub(super) struct CommitRequests(chan::Receiver<CommitRequest>);

impl Committer {
    /// The committer's handle, and the requests [`run`] is to be spawned on.
    pub(super) fn new() -> (Committer, CommitRequests) {
        let (tx, rx) = chan::unbounded();
        (Committer { tx, closed: Cell::new(false) }, CommitRequests(rx))
    }

    /// Lay out `families` as one write, dropping `held` once it is; resolves
    /// then to the write's commit LSN, which is not yet durable.
    pub(super) async fn write(&self, families: Vec<TxnFamily>, held: HeldTables) -> Result<u64, WireFault> {
        let rows = families.iter().map(|f| f.rows.view().len()).sum();
        self.admit(|done| CommitRequest::Write(PendingWrite { families, rows, held, done }))?
            .await
    }

    /// Checkpoint, then copy every published store into `dir`.
    pub(super) async fn backup(&self, dir: String) -> Result<(), WireFault> {
        self.admit(|done| CommitRequest::End(BatchEnd::Backup { dir, done }))?
            .await
    }

    /// Resolves once every write queued ahead has committed or failed. The
    /// committer tests `needs_checkpoint` before it lays out the batch the
    /// settle ends, so only with no write queued ahead is the log then below
    /// its checkpoint threshold.
    pub(super) fn settle(&self) -> oneshot::Receiver<()> {
        self.queue(|done| CommitRequest::End(BatchEnd::Settle(done)))
    }

    /// Refuse every write and backup from here on. Resolves once every write
    /// admitted before has committed and a checkpoint sequence has run over them.
    pub(super) fn close(&self) -> oneshot::Receiver<()> {
        self.closed.set(true);
        self.queue(|done| CommitRequest::End(BatchEnd::Checkpoint(done)))
    }

    fn admit<T>(
        &self,
        req: impl FnOnce(oneshot::Sender<T>) -> CommitRequest,
    ) -> Result<oneshot::Receiver<T>, WireFault> {
        if self.closed.get() {
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
}

/// The committer task loop. Never returns. With no request queued it runs a
/// checkpoint as soon as the SAL wants one: a workload with no writes sends it
/// nothing, yet its reads still write SAL groups.
pub(super) async fn run(CommitRequests(mut rx): CommitRequests, shared: Rc<Shared>) {
    let mut s = CommitScratch::default();
    loop {
        let first = match select2(rx.recv(), shared.dispatcher.sal().checkpoint_due()).await {
            Either::A(req) => req,
            Either::B(()) => {
                run_checkpoint_sequence(&shared).await;
                continue;
            }
        };

        let mut end = drain_ready_batch(&mut rx, first, &mut s.writes);
        // While the fdatasync in flight covers everything published, this
        // batch is durable at the next either way. Laid out now, it releases
        // its table locks; unless a write is parked on one, it waits that
        // fdatasync out and takes in what queued meanwhile.
        let sal = shared.dispatcher.sal();
        if end.is_none() && sal.syncing_all() && !s.writes.iter().any(|w| w.held.contended()) {
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
        // checkpoint, merged with none, until each has committed or failed.
        // Every pass ends: behind a checkpoint the first unit starts at the
        // checkpoint's floor unless another task's groups — reads, ticks —
        // were written in between, and a unit refused where it started at the
        // floor fails, so a pass with no such group commits or fails a unit.
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
            Some(BatchEnd::Settle(done)) => done.send(()),
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
    let mut row_count = writes.iter().map(|w| w.rows).sum::<usize>();
    let mut next = Some(first);
    while let Some(req) = next {
        match req {
            // A write is one indivisible entry: its whole family set rides this
            // batch, and counts every family's rows.
            CommitRequest::Write(w) => {
                row_count += w.rows;
                writes.push(w);
            }
            CommitRequest::End(end) => return Some(end),
        }
        next = (row_count < MAX_PENDING_ROWS).then(|| rx.try_recv()).flatten();
    }
    None
}

/// The checkpoint sequence: base round, a tick of everything pending, ephemeral
/// round, under one hold of the apply lock. A failed ephemeral round aborts:
/// its views are stamped and the master's system rows are not durable.
async fn run_checkpoint_sequence(shared: &Shared) {
    // Held through the ephemeral round, so neither round has a tick in flight
    // (a worker meets no `Flush` inside a drive) and no DDL body, which runs
    // under the lock too, lands between them. A tick in flight is waited out
    // before the base round takes the SAL writer: a checkpoint a refusal
    // called for frees no room until then.
    let alone = shared.dispatcher.apply_lock().await;
    shared.dispatcher.checkpoint_base(&alone).await;

    // One tick takes everything pending: the committer is the task running
    // this, so no write is committed meanwhile and the pending-tick counts
    // cannot grow.
    // Stamping after a failed tick would make the views' missing deltas
    // generation-valid. Unstamped, they stay behind the generation the base
    // round that flushed those deltas' rows advanced to, and are rebuilt at the
    // next boot. Not an abort: this path also serves the shutdown checkpoint.
    match shared.dispatcher.emit_tick(&alone).await {
        Ok(Some(wait)) => wait.acks().await,
        Ok(None) => {}
        Err(e) => {
            gnitz_warn!("checkpoint drain failed, skipping the ephemeral round: {}", e);
            return;
        }
    }

    shared
        .dispatcher
        .checkpoint_ephemeral(&alone)
        .await
        .unwrap_or_else(|e| gnitz_fatal_abort!("{e}"));
}

/// One client-visible commit unit, all-or-nothing: the SAL groups it emits and
/// the writes waiting on their shared verdict.
struct CommitUnit {
    /// The unit's writes in arrival order: one, or a merged run's several.
    writes: SmallVec<[PendingWrite; 1]>,
    /// A merged run's one family: its writes' batches concatenated. The writes
    /// keep their own rows, which a refused run goes again from.
    merged: Option<Box<TxnFamily>>,
    /// Whether one of the unit's groups rode inside the zone, which a restart
    /// recovers, `false` until it is laid out; or why none of its writes
    /// committed.
    outcome: Result<bool, WireFault>,
}

impl CommitUnit {
    /// The families the unit lays out, one group each in frame order: a merged
    /// run's one, else its write's. None once it failed.
    fn laid(&self) -> &[TxnFamily] {
        match (&self.outcome, &self.merged) {
            (Err(_), _) => &[],
            (Ok(_), Some(merged)) => std::slice::from_ref(&**merged),
            (Ok(_), None) => &self.writes[0].families,
        }
    }

    /// A run of several clients' writes merged into one.
    fn is_merged(&self) -> bool {
        self.merged.is_some()
    }

    /// The commit LSN of a laid unit: the zone LSN when one of its groups rode
    /// inside the zone, else `0`.
    fn lsn(&self, zone_lsn: u64) -> u64 {
        if matches!(self.outcome, Ok(true)) { zone_lsn } else { 0 }
    }

    /// Drop every client's table locks and answer it: [`CommitUnit::lsn`] for
    /// a laid unit, the error for a failed one.
    fn resolve(self, zone_lsn: u64) {
        let lsn = self.lsn(zone_lsn);
        let result = self.outcome.map(|_| lsn);
        for PendingWrite { held, done, .. } in self.writes {
            drop(held);
            done.send(result.clone());
        }
    }
}

/// Build `s.units` from `s.writes`, in arrival order: one unit per write, but
/// under `merge` a one-family write joins the unit of the last write before it
/// to its table when that is a one-family write too, so it moves past only
/// writes to other tables. A merged run's family holds its writes' batches in
/// arrival order, which is what makes intra-batch last-insert-wins mean last
/// *inserted*. No unit a write merges into is held to [`PkRule::Fresh`]: such
/// a write holds its table's exclusive lock until its unit resolves, after
/// this, so no other write to the table is in the batch.
fn build_units(s: &mut CommitScratch, merge: bool) {
    s.units.reserve(s.writes.len());
    for write in s.writes.drain(..) {
        if merge
            && let [f] = &write.families[..]
            && let Some(&at) = s.last_unit.get(&f.tid)
            && let [g] = &s.units[at].writes[0].families[..]
            && g.tid == f.tid
        {
            debug_assert!([f, g].iter().all(|g| g.pk_rule == PkRule::Enforce));
            s.units[at].writes.push(write);
            continue;
        }
        for f in &write.families {
            s.last_unit.insert(f.tid, s.units.len());
        }
        s.units.push(CommitUnit {
            writes: smallvec![write],
            merged: None,
            outcome: Ok(false),
        });
    }
    s.last_unit.clear();
    for unit in &mut s.units {
        let [head, _, ..] = &unit.writes[..] else { continue };
        let head = &head.families[0];
        let rows = Batch::concat(
            head.rows.view().schema(),
            unit.writes.iter().map(|w| w.families[0].rows.view()),
        );
        unit.merged = Some(Box::new(TxnFamily::new(head.tid, head.mode, FamilyRows::Built(rows))));
    }
}

/// Commit the batch of writes in `s.writes`. Lays out every group and queues
/// their tids for the tick under one SAL hold, then raises the FK presence
/// caches and commit LSNs, and drops the writers' table locks as it answers
/// them.
///
/// Leaves in `s.writes` the writes to commit again behind a checkpoint: the
/// first unit the SAL had no room for, and every unit after it. A unit of one
/// write fails instead when no checkpoint can make room for it: it is over the
/// limit of a push, or it was refused starting directly behind a checkpoint. A
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
        let mut scope = excl.begin();

        // A unit is all-or-nothing: a family that does not fit takes back the
        // unit's earlier families, which were never published.
        let mut refused_at = None;
        for (at, unit) in s.units.iter_mut().enumerate() {
            let mut unit_in_zone = false;
            let written = scope.unit(|scope| {
                for f in unit.laid() {
                    unit_in_zone |= disp.write_commit_group(scope, f.tid, f.pk_rule, f.rows.view())?;
                }
                Ok(())
            });
            match written {
                Ok(()) => unit.outcome = Ok(unit_in_zone),
                // A merged run goes again as the writes it merged; a unit of one
                // write, if a checkpoint may make room for it. The unit was taken
                // back, so the scope's next group starts where the unit did.
                Err(full) if unit.is_merged() || full.checkpoint_may_admit() => {
                    refused_at = Some(at);
                    break;
                }
                Err(full) => unit.outcome = Err(full.into()),
            }
        }
        // Allocates only on the refusal path; `s.units` keeps the units that
        // committed or failed.
        let again = refused_at.map(|at| s.units.split_off(at));

        // Started now rather than at the first writer's wait for it.
        let zone_lsn = scope.commit();
        if let Some(lsn) = zone_lsn {
            disp.sal().want_durable(lsn);
        }
        // The auto-tick this may fire overlaps the fdatasync.
        for f in s.units.iter().flat_map(CommitUnit::laid) {
            disp.credit(&excl, f.tid, f.rows.view().len());
        }
        (zone_lsn.unwrap_or(0), again)
    };

    // The FK presence caches and commit LSNs, then the table locks. No OCC
    // check of a later write to one of the tables runs before the raise. Such a
    // write is laid out behind this batch, so the fdatasync that covers it
    // covers this batch too.
    for unit in s.units.drain(..) {
        let lsn = unit.lsn(zone_lsn);
        for f in unit.laid() {
            shared.writes.committed(f.tid, f.rows.view(), lsn);
        }
        unit.resolve(zone_lsn);
    }
    s.writes
        .extend(again.into_iter().flatten().flat_map(|unit| unit.writes));
}

#[cfg(test)]
#[path = "tests/committer.rs"]
mod tests;
