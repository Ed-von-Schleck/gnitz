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
//!   three-step sequence (`run_checkpoint_sequence`: gen bump → base round →
//!   drain → tick gate → ephemeral round), then proceeds with the push batch.
//!   `checkpoint_warranted` is the one statement of when.
//! - **Barrier.**  A `Barrier` request flushes any in-flight batch and
//!   signals via a oneshot — used by DDL to drain the committer before
//!   catalog mutation, and by graceful shutdown (see `BarrierKind`).

use super::executor::{request_drain, Shared};
use super::guard_panic;
use super::TxnFamily;
use crate::runtime::master::FlushRound;
use crate::runtime::reactor::{chan, oneshot, AckLease};
use crate::runtime::sal::SalScope;
use gnitz_wire::WireFault;
use gnitz_zset::repr::Batch;
use std::rc::Rc;

/// Row ceiling on one committer batch. Tested before the receive, so a batch is
/// capped at this plus one whole request — and one request is itself bounded
/// only by `MAX_FRAME_PAYLOAD`.
const MAX_PENDING_ROWS: usize = 100_000;

/// One request to the committer.
#[allow(clippy::large_enum_variant)]
pub enum CommitRequest {
    /// Buffer one batch for group commit.
    Push(PendingPush),
    /// An atomic user-table transaction: N families emitted as N `Push`
    /// groups inside one zone, with a single `done` for the
    /// whole bundle. Validated and lock-guarded by the executor before it
    /// reaches here.
    Txn(PendingTxn),
    /// Drain any in-flight batch and signal via `done`. `kind` decides whether
    /// the batch runs a checkpoint sequence (see `BarrierKind`).
    Barrier {
        kind: BarrierKind,
        done: oneshot::Sender<()>,
    },
    /// Wake the committer so it re-tests `checkpoint_warranted`.
    Reclaim,
}

/// One buffered atomic transaction awaiting commit: its families in frame order
/// and one `done` resolving `Ok(zone_lsn)` for the whole bundle, or the error that
/// rolled it back. Every family is `recoverable` — the executor
/// refuses a stream target — so a transaction always opens a zone.
pub struct PendingTxn {
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

/// One buffered single push awaiting commit. `done` resolves to `Ok(zone_lsn)` or
/// `Err(error_message)`.
pub struct PendingPush {
    pub tid: u64,
    pub batch: Batch,
    /// Whether these rows are something a restart must recover, i.e. whether this
    /// group may join the zone the commit and fdatasync close. False only for a
    /// stream. Decided by the executor, which has already resolved the target's kind,
    /// so the committer never asks the catalog what a relation *is*.
    pub recoverable: bool,
    pub done: oneshot::Sender<Result<u64, WireFault>>,
}

/// The committer task loop. Never returns.
///
/// For checkpoint flush rounds the lock is held across the ENTIRE round
/// (write + ACK wait + reset; see `MasterDispatcher::flush`), but released across the
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

        let PendingBatch { pushes, txns, barrier } = batch;
        if !pushes.is_empty() || !txns.is_empty() {
            commit_pushes(&shared, pushes, txns).await;
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

/// One committer batch: the single pushes to group-commit and the atomic
/// transactions to commit.
#[derive(Default)]
struct PendingBatch {
    pushes: Vec<PendingPush>,
    txns: Vec<PendingTxn>,
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
            CommitRequest::Push(p) => {
                row_count += p.batch.len();
                b.pushes.push(p);
            }
            // A transaction is one indivisible entry — its whole family set rides
            // this batch; its row count is the sum of every family's rows.
            CommitRequest::Txn(t) => {
                row_count += t.families.iter().map(|f| f.batch.len()).sum::<usize>();
                b.txns.push(t);
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

/// The full steady-state checkpoint sequence: gen bump → base round → drain →
/// tick gate → ephemeral round. Requests arriving meanwhile wait in the channel,
/// so the pushes `batch` holds are the only ones the sequence runs ahead of.
///
/// A failed round aborts: the workers are re-epoched against an un-reset SAL,
/// which only a restart's replay repairs.
async fn run_checkpoint_sequence(shared: &Rc<Shared>) {
    // A DDL holding the tick gate has uncommitted families step 1 must not flush.
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
    // generation-valid; left behind step 1's generation, they are rebuilt at the
    // next boot. Not an abort: this path also serves the Shutdown barrier.
    if let Err(e) = request_drain(shared).await {
        gnitz_warn!("checkpoint drain failed, skipping the ephemeral round: {}", e);
        return;
    }

    // Step 3 — EPHEMERAL ROUND, with no tick running.
    let _park = shared.tick_gate.write().await;
    shared
        .disp()
        .flush(FlushRound::Ephemeral)
        .await
        .unwrap_or_else(|e| gnitz_fatal_abort!("{e}"));
}

/// One single-tid SAL group: a merged run of single pushes, or one transaction
/// family.
struct GroupInfo {
    tid: u64,
    /// See `PendingPush::recoverable`. A merged run is homogeneous in
    /// `tid`, so one flag per group is exact.
    recoverable: bool,
    /// The ACKs of the workers the group was written to; `Some` once it is laid out.
    lease: Option<AckLease>,
    merged: Batch,
}

/// One client-visible commit unit, all-or-nothing: the SAL groups it emits and
/// the clients waiting on their shared verdict.
struct CommitUnit {
    /// One group for a merged single-push run; one per family, in frame order,
    /// for a transaction.
    groups: Vec<GroupInfo>,
    /// The coalesced clients of a merged run, or a transaction's one client.
    dones: Vec<oneshot::Sender<Result<u64, WireFault>>>,
    /// Why none of `groups` committed.
    failed: Option<WireFault>,
}

impl CommitUnit {
    /// This unit's groups if it has not failed: what the SAL admitted, and past
    /// Phase C what every worker also took. The one spelling: a group awaited
    /// that no worker was sent would park forever.
    fn live(&self) -> &[GroupInfo] {
        if self.failed.is_some() {
            &[]
        } else {
            &self.groups
        }
    }

    fn resolve(self, zone_lsn: u64) {
        let result = self.failed.map_or(Ok(zone_lsn), Err);
        for done in self.dones {
            done.send(result.clone());
        }
    }
}

/// Commit one batch of pushes. Emits every group's SAL writes, queues their tids
/// for the tick and submits the fsync SQE under one SAL hold, THEN awaits worker
/// ACKs (Phase C) and the fsync CQE (Phase D). `done.send` and the unique-index
/// filter update happen after fsync.
async fn commit_pushes(shared: &Rc<Shared>, mut pushes: Vec<PendingPush>, txns: Vec<PendingTxn>) {
    // Sort by tid so runs are homogeneous. Stable: arrival order within a run is
    // what makes intra-batch last-insert-wins mean last *inserted*.
    pushes.sort_by_key(|p| p.tid);

    let mut units: Vec<CommitUnit> = Vec::with_capacity(txns.len());

    // ------------------------------------------------------------------
    // Phase A (no lock): build merged batches.
    // Transaction units come FIRST (transactions-first emission), each family its
    // own group in frame order; then the merged single-push units.
    // ------------------------------------------------------------------
    for PendingTxn { families, done } in txns {
        units.push(CommitUnit {
            groups: families
                .into_iter()
                .map(|fam| GroupInfo {
                    tid: fam.tid,
                    recoverable: true,
                    lease: None,
                    merged: fam.batch,
                })
                .collect(),
            dones: vec![done],
            failed: None,
        });
    }

    // Single-push runs: walk the sorted pushes, draining each maximal tid run
    // into one merged group.
    let mut remaining = pushes.into_iter().peekable();
    while let Some(first) = remaining.next() {
        let (tid, recoverable) = (first.tid, first.recoverable);
        // Split the run into its two independent halves as it is drained: the
        // batches the merge consumes, and the `done` senders the unit resolves.
        // The run's first batch is held apart, so the dominant single-push case
        // allocates no `Vec` and enters no `catch_unwind` frame.
        let head = first.batch;
        let mut tail: Vec<Batch> = Vec::new();
        let mut dones = vec![first.done];
        while remaining.peek().is_some_and(|p| p.tid == tid) {
            let p = remaining.next().unwrap();
            tail.push(p.batch);
            dones.push(p.done);
        }

        // A single push ships the client's batch as it stands; a coalesced run
        // concatenates into one. The concat reads client-supplied batches, so it
        // is guarded.
        // A failed concat leaves a failed unit with no group to lay out.
        let merged = if tail.is_empty() {
            Ok(head)
        } else {
            guard_panic("commit_merge", || {
                Ok::<_, String>(Batch::concat(
                    &shared.disp().schema_desc_for(tid),
                    std::iter::once(&head).chain(tail.iter()).map(Batch::as_mem_batch),
                ))
            })
        };
        units.push(match merged {
            Ok(merged) => CommitUnit {
                groups: vec![GroupInfo { tid, recoverable, lease: None, merged }],
                dones,
                failed: None,
            },
            Err(panic_msg) => CommitUnit {
                groups: Vec::new(),
                dones,
                failed: Some(WireFault::from(panic_msg)),
            },
        });
    }

    // ------------------------------------------------------------------
    // Phase B (under the SAL writer): emit SAL groups, queue their tids for the
    // tick, submit fsync SQE. The batch's recoverable groups form one zone.
    // ------------------------------------------------------------------
    let (zone_lsn, synced) = {
        let disp = shared.disp();
        let mut excl = disp.sal().lock().await;

        // Nothing laid out inside the scope is visible until it commits, so a
        // transaction that runs out of SAL space part-way can take its earlier
        // families back. Every write, the commit and the fsync submit are in
        // this one synchronous block, so no reader ever observes the gap.
        let scope = excl.begin("commit");
        let zone_lsn = scope.lsn();

        // Emit every unit into the zone, in unit order (transactions first). The
        // first recoverable group the scope admits opens the zone.
        //
        // A unit is all-or-nothing: a family that does not fit rolls the bundle
        // back to where it started, and the families before it were never
        // published. A single-push unit has one group, so the same rule refuses
        // that push alone.
        for unit in units.iter_mut().filter(|u| u.failed.is_none()) {
            let savepoint = scope.savepoint();
            unit.failed = unit
                .groups
                .iter_mut()
                .find_map(|g| match lay_out_group(shared, &scope, g) {
                    Ok(lease) => {
                        g.lease = Some(lease);
                        None
                    }
                    Err(e) => Some(e),
                });
            if unit.failed.is_some() {
                scope.roll_back(savepoint);
            }
        }

        let synced = scope.commit().then(|| excl.sync(disp.reactor(), "committer"));
        // Queued in the block that laid the groups out, so a tick whose snapshot
        // of the SAL watermark covers `zone_lsn` also takes these tids — what
        // `read_is_fresh` relies on. The tick's group follows these in the log,
        // and the auto-tick it may fire overlaps the ACKs and the fsync.
        shared.note_commit_rows(units.iter().flat_map(|u| u.live()).map(|g| (g.tid, g.merged.len())));
        (zone_lsn, synced)
    };

    // ------------------------------------------------------------------
    // Phase C (no lock): await push ACKs, per live group in unit order. Replies
    // for later groups wait in their routes meanwhile.
    // unique_filter_ingest_batch is NOT called here: a filter entry for rows a
    // crash would discard makes the next INSERT of the same key fail a
    // uniqueness check nothing durable backs. It runs after Phase D's fsync.
    // ------------------------------------------------------------------
    for g in units.iter().flat_map(|u| u.live()) {
        let lease = g.lease.as_ref().expect("a live group was laid out");
        if let Err(e) = lease.acks().await {
            // The group is durable and the other workers applied it: answering it
            // would leave the SAL and this worker's partition disagreeing.
            gnitz_fatal_abort!("worker rejected a committed group (tid={}): {}", g.tid, e);
        }
    }

    // ------------------------------------------------------------------
    // Phase D (no lock): await fsync CQE. Client response is held until after
    // fsync so the client sees only durable data. A batch of nothing but stream
    // groups opened no zone, so has no fsync to await. Pushes batched together
    // share one zone LSN.
    // ------------------------------------------------------------------
    if let Some(synced) = synced {
        synced.await;
    }

    // Update unique-index filters now that fsync confirms durability.
    // Wrapped for task liveness: a panic here must not fail the commit —
    // the data is already durable. Invalidate on panic so the next
    // constrained INSERT re-validates from scratch.
    for g in units.iter().flat_map(|u| u.live()) {
        if let Err(e) = guard_panic("unique_filter_ingest", || {
            shared.disp().unique_filter_ingest_batch(g.tid, &g.merged);
            Ok::<_, String>(())
        }) {
            shared.disp().unique_filter_invalidate_table(g.tid);
            gnitz_warn!("{}", e);
        }
    }

    // Send responses: every client of a unit gets the unit's verdict.
    for unit in units {
        unit.resolve(zone_lsn);
    }
}

/// Lay one group out in `scope`, inside its zone when the group is
/// `recoverable`, and answer the lease its workers ACK on. `guard_panic` covers
/// the encode of a client-supplied batch.
fn lay_out_group(shared: &Rc<Shared>, scope: &SalScope, g: &GroupInfo) -> Result<AckLease, WireFault> {
    guard_panic("commit_write", || {
        shared.disp().write_commit_group(scope, g.tid, &g.merged, g.recoverable)
    })
}

#[cfg(test)]
#[path = "tests/committer.rs"]
mod tests;
