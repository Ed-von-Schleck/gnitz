//! Group commit task.
//!
//! Owns the `write_commit_group` → `SalScope::commit` → `fdatasync` → per-worker push
//! ACK sequence for every user-table INSERT/UPSERT. Receives commit requests via
//! `chan` and batches whatever is already queued behind the first — see
//! **Batching** below; there is no debounce timer.
//!
//! Design notes:
//!
//! - **One committer per master process.**  Single-threaded: everything
//!   lives on the reactor, sharing the `MasterDispatcher` by reference with
//!   the executor.
//! - **Batching.**  Pipelined clients batch naturally: after `rx.recv()`
//!   returns the first request, `try_recv` drains anything already
//!   queued (capped at `MAX_PENDING_ROWS`). There is no debounce timer —
//!   a timer would add tail latency to every commit to help only serial
//!   single-request workloads. See `drain_ready_batch`.
//! - **Checkpoint.**  When the batch warrants one the committer runs the full
//!   sequence (`run_checkpoint_sequence`: base round → drain → tick gate →
//!   ephemeral round), then proceeds with the push batch.
//!   `checkpoint_warranted` is the one statement of when.
//! - **Barrier.**  A `Barrier` request flushes any in-flight batch and
//!   signals via a oneshot — used by DDL to drain the committer before
//!   catalog mutation, and by graceful shutdown (see `BarrierKind`).

use super::executor::{request_drain, Shared};
use super::TxnFamily;
use crate::runtime::reactor::{chan, oneshot, AckLease};
use gnitz_wire::WireFault;
use gnitz_zset::repr::Batch;
use rustc_hash::FxHashSet;
use std::rc::Rc;

/// Row ceiling on one committer batch. Tested before the receive, so a batch is
/// capped at this plus one whole request — and one request is itself bounded
/// only by `MAX_FRAME_PAYLOAD`.
const MAX_PENDING_ROWS: usize = 100_000;

/// One request to the committer.
pub enum CommitRequest {
    /// Commit one write.
    Write(PendingWrite),
    /// Drain any in-flight batch and signal via `done`. `kind` decides whether
    /// the batch runs a checkpoint sequence (see `BarrierKind`).
    Barrier {
        kind: BarrierKind,
        done: oneshot::Sender<()>,
    },
    /// Wake the committer so it re-tests `checkpoint_warranted`.
    Reclaim,
}

/// One write awaiting commit, all-or-nothing: each family is one `Push` group,
/// in frame order, and `done` resolves `Ok(zone_lsn)` or the error that rolled
/// the whole write back.
pub struct PendingWrite {
    pub families: Vec<TxnFamily>,
    pub done: oneshot::Sender<Result<u64, WireFault>>,
}

/// Who issued a `CommitRequest::Barrier`, and therefore whether its batch runs a
/// checkpoint sequence.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BarrierKind {
    /// DDL drain from `handle_ddl_txn`.
    Ddl,
    /// Graceful shutdown: forces the full checkpoint sequence on the batch it
    /// arrives in regardless of SAL fullness, so its `done` implies the base +
    /// drain + ephemeral rounds were all attempted. The drain's own failure path
    /// skips the ephemeral round rather than aborting, precisely because Shutdown
    /// runs here.
    Shutdown,
}

/// The committer task loop. Never returns.
///
/// For checkpoint flush rounds the lock is held across the ENTIRE round
/// (write + ACK wait + reset), but released across the
/// sequence's drain step so the tick loop can acquire it per tick.
pub async fn run(mut rx: chan::Receiver<CommitRequest>, shared: Rc<Shared>) {
    loop {
        let first = rx.recv().await;

        // Drain any additional requests already queued — no timer wait.
        // Pipelined clients still get batched; serial clients don't pay
        // a latency tax.
        let batch = drain_ready_batch(&mut rx, first);

        // Before this batch's groups, never after: workers bump their
        // expected_epoch on Flush, so a group written behind one in the same
        // epoch is silently skipped.
        if checkpoint_warranted(&shared, &batch) {
            run_checkpoint_sequence(&shared).await;
        }

        let PendingBatch { writes, barrier } = batch;
        if !writes.is_empty() {
            commit_writes(&shared, writes).await;
        }

        if let Some((_, b)) = barrier {
            b.send(());
        }
    }
}

/// Whether this batch warrants the full checkpoint sequence.
fn checkpoint_warranted(shared: &Shared, batch: &PendingBatch) -> bool {
    matches!(batch.barrier, Some((BarrierKind::Shutdown, _))) || shared.disp().sal().needs_checkpoint()
}

/// One committer batch: the writes to commit together.
#[derive(Default)]
struct PendingBatch {
    writes: Vec<PendingWrite>,
    /// The barrier that ended this batch, if one did: signalled once the batch —
    /// and any checkpoint sequence — has committed.
    barrier: Option<(BarrierKind, oneshot::Sender<()>)>,
}

/// Sort `first` into the batch, then keep draining requests already queued
/// behind it — without ever waiting — until the row cap is reached, a barrier
/// arrives, or the channel runs dry.
///
/// A barrier ends the batch wherever it lands, `first` included: what is queued
/// behind it is younger than whatever issued it, so folding that in would make
/// the barrier wait on a commit it has no ordering interest in. It rides the
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
            CommitRequest::Barrier { kind, done } => {
                b.barrier = Some((kind, done));
                break;
            }
            CommitRequest::Reclaim => {}
        }
        next = (row_count < MAX_PENDING_ROWS).then(|| rx.try_recv()).flatten();
    }
    b
}

/// The full steady-state checkpoint sequence: base round → drain → tick gate →
/// ephemeral round. The base round advances the generation when it has pushes
/// to flush. Requests arriving meanwhile wait in the channel, so the pushes
/// `batch` holds are the only ones the sequence runs ahead of.
///
/// A failed round aborts: the workers are re-epoched against an un-reset SAL,
/// which only a restart's replay repairs.
async fn run_checkpoint_sequence(shared: &Rc<Shared>) {
    // A checkpoint sequence starts after a DDL that holds the tick gate.
    if shared.tick_gate.is_write_held() {
        drop(shared.tick_gate.read().await);
    }
    shared
        .disp()
        .checkpoint_base()
        .await
        .unwrap_or_else(|e| gnitz_fatal_abort!("{e}"));

    // Step 2 — DRAIN (lock released). One Drain suffices: pushes are held, so the
    // pending-tick counts cannot grow.
    // Stamping after a failed drain would make the views' missing deltas
    // generation-valid. Unstamped, they stay behind the generation the base
    // round that flushed those deltas' rows advanced to, and are rebuilt at the
    // next boot. Not an abort: this path also serves the Shutdown barrier.
    if let Err(e) = request_drain(shared, false).await {
        gnitz_warn!("checkpoint drain failed, skipping the ephemeral round: {}", e);
        return;
    }

    // Step 3 — EPHEMERAL ROUND, with no tick running.
    let _park = shared.tick_gate.write().await;
    shared
        .disp()
        .checkpoint_ephemeral()
        .await
        .unwrap_or_else(|e| gnitz_fatal_abort!("{e}"));
}

/// One client-visible commit unit, all-or-nothing: the SAL groups it emits and
/// the clients waiting on their shared verdict.
struct CommitUnit {
    /// One group each, in frame order. A run of one-family writes to one table
    /// is one unit of one family, their batches merged.
    families: Vec<TxnFamily>,
    /// The coalesced clients of a merged run, or a write's one client.
    dones: Vec<oneshot::Sender<Result<u64, WireFault>>>,
    /// Why none of `families` committed.
    failed: Option<WireFault>,
}

impl CommitUnit {
    /// This unit's families if it has not failed: what the SAL admitted, and
    /// past Phase C what every worker also took.
    fn live(&self) -> &[TxnFamily] {
        if self.failed.is_some() {
            &[]
        } else {
            &self.families
        }
    }

    fn resolve(self, zone_lsn: u64) {
        let result = self.failed.map_or(Ok(zone_lsn), Err);
        for done in self.dones {
            done.send(result.clone());
        }
    }
}

/// Whether no table is written by two of `units`.
fn write_disjoint_tables(units: &[CommitUnit]) -> bool {
    let mut seen = FxHashSet::default();
    units.iter().all(|unit| {
        let mut tids: Vec<u64> = unit.families.iter().map(|f| f.tid).collect();
        tids.sort_unstable();
        tids.dedup();
        tids.into_iter().all(|tid| seen.insert(tid))
    })
}

/// Commit one batch of writes. Emits every group's SAL writes, queues their tids
/// for the tick and submits the fsync SQE under one SAL hold, THEN awaits worker
/// ACKs (Phase C) and the fsync CQE (Phase D). `done.send` and the update of the
/// master's caches of committed rows happen after fsync.
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
        units.push(CommitUnit { families, dones, failed: None });
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
        let disp = shared.disp();
        let mut excl = disp.sal().lock().await;

        // Nothing laid out inside the scope is visible until it commits, so a
        // write that runs out of SAL space part-way can take its earlier
        // families back. Every write, the commit and the fsync submit are in
        // this one synchronous block, so no reader ever observes the gap.
        let scope = excl.begin("commit");
        let zone_lsn = scope.lsn();

        // The ACKs of every group laid out and kept, in unit order.
        let mut leases: Vec<AckLease> = Vec::with_capacity(units.len());
        // A unit is all-or-nothing: a family that does not fit rolls the unit
        // back to where it started, and the families before it were never
        // published.
        for unit in &mut units {
            let (savepoint, kept) = (scope.savepoint(), leases.len());
            unit.failed = unit
                .families
                .iter()
                .find_map(|f| match disp.write_commit_group(&scope, f.tid, &f.batch) {
                    Ok(lease) => {
                        leases.push(lease);
                        None
                    }
                    Err(e) => Some(e),
                });
            if unit.failed.is_some() {
                scope.roll_back(savepoint);
                leases.truncate(kept);
            }
        }

        let synced = scope.commit().then(|| excl.sync(disp.reactor(), "committer"));
        // The auto-tick this may fire overlaps the ACKs and the fsync.
        shared.note_commit_rows(
            &excl,
            units.iter().flat_map(|u| u.live()).map(|f| (f.tid, f.batch.len())),
        );
        (zone_lsn, synced, leases)
    };

    // ------------------------------------------------------------------
    // Phase C (no lock): await push ACKs, per group in unit order. Replies for
    // later groups wait in their routes meanwhile.
    // The master's caches of committed rows are NOT updated here: they hold
    // only what is durable, so that runs after Phase D's fsync.
    // ------------------------------------------------------------------
    for lease in &leases {
        lease.acks().await;
    }

    // ------------------------------------------------------------------
    // Phase D (no lock): await fsync CQE. Client response is held until after
    // fsync so the client sees only durable data. A batch of nothing but stream
    // groups opened no zone, so has no fsync to await. Writes batched together
    // share one zone LSN.
    // ------------------------------------------------------------------
    if let Some(synced) = synced {
        synced.await;
    }

    // The master's caches of committed rows follow, now that fsync confirms
    // durability and before any writer is answered.
    for f in units.iter().flat_map(|u| u.live()) {
        shared.disp().committed(f.tid, &f.batch);
    }

    // Send responses: every client of a unit gets the unit's verdict.
    for unit in units {
        unit.resolve(zone_lsn);
    }
}

#[cfg(test)]
#[path = "tests/committer.rs"]
mod tests;
