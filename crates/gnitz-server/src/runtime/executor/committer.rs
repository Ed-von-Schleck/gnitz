//! The group-commit task: every user write's SAL groups, its fdatasync and its
//! worker ACKs, batched from whatever is queued behind the first request (see
//! [`drain_ready_batch`]). Checkpoints, backups and DDL pauses end a batch. Once serving, it is the one task that runs a checkpoint.

use super::validate::TxnFamily;
use super::Shared;
use crate::runtime::reactor::AckLease;
use crate::runtime::signals;
use crate::runtime::sync::{chan, oneshot};
use gnitz_wire::WireFault;
use gnitz_zset::repr::Batch;
use rustc_hash::FxHashSet;
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
/// in frame order, and `done` resolves `Ok(zone_lsn)` or the error that rolled
/// the whole write back.
struct PendingWrite {
    families: Vec<TxnFamily>,
    done: oneshot::Sender<Result<u64, WireFault>>,
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

    /// Commit `families` as one write; resolves to the write's zone LSN.
    pub(super) async fn write(&self, families: Vec<TxnFamily>) -> Result<u64, WireFault> {
        self.admit(|done| CommitRequest::Write(PendingWrite { families, done }))?
            .await
    }

    /// Checkpoint, then copy every published store into `dir`.
    pub(super) async fn backup(&self, dir: String) -> Result<(), WireFault> {
        self.admit(|done| CommitRequest::End(BatchEnd::Backup { dir, done }))?
            .await
    }

    /// Resolves once the batch queued ahead has committed and the committer has
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

/// The committer task loop. Never returns.
pub(super) async fn run(CommitRequests(mut rx): CommitRequests, shared: Rc<Shared>) {
    loop {
        let first = rx.recv().await;

        let PendingBatch { writes, end } = drain_ready_batch(&mut rx, first);

        // Before this batch's groups: the checkpoint makes room for them.
        if shared.dispatcher.sal().needs_checkpoint() {
            run_checkpoint_sequence(&shared).await;
        }

        if !writes.is_empty() {
            commit_writes(&shared, writes).await;
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

/// One committer batch: the writes to commit together.
#[derive(Default)]
struct PendingBatch {
    writes: Vec<PendingWrite>,
    /// The request that ended this batch, if one did.
    end: Option<BatchEnd>,
}

/// Sort `first` into the batch, then keep draining requests already queued
/// behind it — without ever waiting — until the row cap is reached, an `End`
/// request arrives, or the channel runs dry. There is no debounce timer: one
/// would add tail latency to every commit to help only serial single-request
/// workloads, while pipelined clients batch without one.
///
/// An `End` request ends the batch wherever it lands, `first` included: what is
/// queued behind it is younger than whatever issued it, so folding that in would
/// make it wait on a commit it has no ordering interest in. It rides the
/// next `rx.recv()`, which this loop reaches immediately.
fn drain_ready_batch(rx: &mut chan::Receiver<CommitRequest>, first: CommitRequest) -> PendingBatch {
    let mut b = PendingBatch::default();
    let mut row_count = 0usize;
    let mut next = Some(first);
    while let Some(req) = next {
        match req {
            // A write is one indivisible entry: its whole family set rides this
            // batch, and counts every family's rows.
            CommitRequest::Write(w) => {
                row_count += w.families.iter().map(|f| f.batch.len()).sum::<usize>();
                b.writes.push(w);
            }
            CommitRequest::End(end) => {
                b.end = Some(end);
                break;
            }
            CommitRequest::Reclaim => {}
        }
        next = (row_count < MAX_PENDING_ROWS).then(|| rx.try_recv()).flatten();
    }
    b
}

/// The checkpoint sequence: base round, drain, ephemeral round. A failed round
/// aborts: the workers are re-epoched against an un-reset SAL, which only a
/// restart's replay repairs.
async fn run_checkpoint_sequence(shared: &Rc<Shared>) {
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
    /// The coalesced clients of a merged run, or a write's one client.
    dones: Vec<oneshot::Sender<Result<u64, WireFault>>>,
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

    /// Answer every client: the zone LSN for a unit a restart recovers, `0` for
    /// one it does not.
    fn resolve(self, zone_lsn: u64) {
        let result = match self.outcome {
            Outcome::Laid { in_zone, .. } => Ok(if in_zone { zone_lsn } else { 0 }),
            Outcome::Failed(e) => Err(e),
        };
        for done in self.dones {
            done.send(result.clone());
        }
    }
}

/// Whether no table is written by two of `units`.
fn write_disjoint_tables(units: &[CommitUnit]) -> bool {
    let mut seen = FxHashSet::default();
    units.iter().all(|unit| {
        let mut tids: Vec<u64> = unit.laid().iter().map(|f| f.tid).collect();
        tids.sort_unstable();
        tids.dedup();
        tids.into_iter().all(|tid| seen.insert(tid))
    })
}

/// Commit one batch of writes. Lays out every group, queues their tids for the
/// tick and submits the fsync SQE under one SAL hold, then awaits the worker
/// ACKs and, when the batch opened a zone, the fsync. Only then are the
/// validation caches updated and the writers answered.
async fn commit_writes(shared: &Rc<Shared>, mut writes: Vec<PendingWrite>) {
    // One-family writes in tid runs. Stable: arrival order within a run is what
    // makes intra-batch last-insert-wins mean last *inserted*.
    writes.sort_by_key(|w| (w.families.len() != 1, w.families[0].tid));

    // ------------------------------------------------------------------
    // Phase A (no lock): one unit per write, a run of one-family writes to one
    // table merged into one.
    // ------------------------------------------------------------------
    let mut units: Vec<CommitUnit> = Vec::with_capacity(writes.len());
    let mut remaining = writes.into_iter().peekable();
    while let Some(PendingWrite { mut families, done }) = remaining.next() {
        let mut dones = vec![done];
        if let [head] = &mut families[..] {
            // The run's batches behind its first. A single push ships the
            // client's batch as it stands, so the dominant case builds no `Vec`.
            let mut tail: Vec<Batch> = Vec::new();
            while let Some(next) = remaining.next_if(|w| matches!(&w.families[..], [f] if f.tid == head.tid)) {
                tail.extend(next.families.into_iter().map(|f| f.batch));
                dones.push(next.done);
            }
            if !tail.is_empty() {
                head.batch = Batch::concat(
                    head.batch.schema(),
                    std::iter::once(&head.batch).chain(&tail).map(Batch::as_mem_batch),
                );
            }
        }
        units.push(CommitUnit {
            outcome: Outcome::Laid { families, in_zone: false },
            dones,
        });
    }
    debug_assert!(
        write_disjoint_tables(&units),
        "two units of one batch write one table, so the sort reordered them"
    );

    // ------------------------------------------------------------------
    // Phase B (under the SAL writer): emit SAL groups, queue their tids for the
    // tick, submit fsync SQE. The batch's recoverable groups form one zone.
    // ------------------------------------------------------------------
    let (zone_lsn, synced, leases) = {
        let disp = &shared.dispatcher;
        let mut excl = disp.sal().lock().await;

        // Nothing laid out inside the scope is visible until it commits, so a
        // write that runs out of SAL space part-way can take its earlier
        // families back. Every write, the commit and the fsync submit are in
        // this one synchronous block, so no reader ever observes the gap.
        let mut scope = excl.begin("commit");
        let zone_lsn = scope.lsn();

        // The ACKs of every group laid out and kept, in unit order.
        let mut leases: Vec<AckLease> = Vec::with_capacity(units.len());
        // A unit is all-or-nothing: a family that does not fit takes back the
        // unit's earlier families, which were never published.
        for unit in &mut units {
            let written = scope.all_or_nothing(|scope| {
                (unit.laid().iter())
                    .map(|f| disp.write_commit_group(scope, f.tid, &f.batch))
                    .collect::<Result<Vec<_>, _>>()
            });
            match written {
                Ok(groups) => {
                    if let Outcome::Laid { in_zone, .. } = &mut unit.outcome {
                        *in_zone = groups.iter().any(|&(_, in_zone)| in_zone);
                    }
                    leases.extend(groups.into_iter().map(|(lease, _)| lease));
                }
                Err(e) => unit.outcome = Outcome::Failed(e),
            }
        }

        let synced = scope.commit().then(|| excl.sync(disp.reactor()));
        // The auto-tick this may fire overlaps the ACKs and the fsync.
        shared.note_commit_rows(
            &excl,
            units.iter().flat_map(CommitUnit::laid).map(|f| (f.tid, f.batch.len())),
        );
        (zone_lsn, synced, leases)
    };

    // ------------------------------------------------------------------
    // Phase C (no lock): await push ACKs, per group in unit order. Replies for
    // later groups wait in their routes meanwhile.
    // ------------------------------------------------------------------
    for lease in &leases {
        lease.acks().await;
    }

    // ------------------------------------------------------------------
    // Phase D (no lock): await the fsync. A batch of nothing but stream groups
    // opened no zone, so has none. Writes batched together share one zone LSN.
    // ------------------------------------------------------------------
    if let Some(synced) = synced {
        synced.await;
    }

    // The caches, then the answers.
    for f in units.iter().flat_map(CommitUnit::laid) {
        shared.writes.checks.committed(&shared.cat(), f.tid, &f.batch);
    }
    for unit in units {
        unit.resolve(zone_lsn);
    }
}

#[cfg(test)]
#[path = "tests/committer.rs"]
mod tests;
