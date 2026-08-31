//! Group commit task.
//!
//! Owns the `sal.write_ingest + fdatasync + per-worker push ACK`
//! sequence for every user-table INSERT/UPSERT. Receives commit
//! requests via `chan` and batches whatever is already queued behind the
//! first — see **Batching** below; there is no debounce timer.
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
//! - **Checkpoint.**  If `sal.needs_checkpoint()` (or a Shutdown barrier
//!   forces it) the committer runs the full three-step sequence
//!   (`run_checkpoint_sequence`: gen bump → base round → drain/quiesce →
//!   ephemeral round), then proceeds with the push batch.
//! - **Barrier.**  A `Barrier` request flushes any in-flight batch and
//!   signals via a oneshot — used by DDL to drain the committer before
//!   catalog mutation, by `relay_loop` to reclaim SAL space, and by the
//!   graceful-shutdown watchdog (see `BarrierKind`).

use super::executor::{Shared, TickTrigger};
use super::guard_panic;
use crate::runtime::master::{await_worker_acks, first_worker_error_opt, TxnFamily};
use crate::runtime::reactor::{chan, join_into, oneshot, select2, Either, ReplyFuture, ReplyLease};
use crate::runtime::sal::{GroupTargets, SalFit, SalMessageKind, ZoneMark};
use crate::runtime::wire::DecodedWire;
use gnitz_engine::foundation::fault::Seam;
use gnitz_engine::storage::Batch;
use gnitz_wire::{WireConflictMode, WireFault};
use rustc_hash::FxHashMap;
use std::rc::Rc;

const MAX_PENDING_ROWS: usize = 100_000;

/// `GNITZ_INJECT_PUSH_ABORT=after_groups`: crash the master between the push
/// groups and the commit sentinel.
static PUSH_ABORT: Seam = Seam::new("GNITZ_INJECT_PUSH_ABORT");

/// One request to the committer.
#[allow(clippy::large_enum_variant)]
pub enum CommitRequest {
    /// Buffer one batch for group commit.
    Push(PendingPush),
    /// An atomic user-table transaction: N families emitted as N `Push`
    /// groups inside one zone under one sentinel, with a single `done` for the
    /// whole bundle. Validated and lock-guarded by the executor before it
    /// reaches here.
    Txn(PendingTxn),
    /// Drain any in-flight batch and signal via `done`. `kind` decides how
    /// the barrier interacts with a checkpoint sequence (see `BarrierKind`).
    Barrier {
        kind: BarrierKind,
        done: oneshot::Sender<()>,
    },
}

/// One buffered atomic transaction awaiting commit: its families in frame order
/// and one `done` resolving `Ok(zone_lsn)` for the whole bundle (or the first family
/// error on a pre-sentinel abort). Every family is `recoverable` — the executor
/// refuses a stream target — so a transaction always opens a zone.
pub struct PendingTxn {
    pub families: Vec<TxnFamily>,
    pub done: oneshot::Sender<Result<u64, WireFault>>,
}

/// Who issued a `CommitRequest::Barrier`, and therefore how the committer
/// services it while a checkpoint sequence is in flight.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BarrierKind {
    /// Relay-space barrier: serviced mid-sequence by a reclaim-only base round
    /// (no gen re-bump) and signaled right after step 1, so `relay_loop` unparks
    /// before the drain and can refill space during it.
    ///
    /// `forced` means the requester has a concrete byte requirement the
    /// committer's own space test cannot see — `relay_loop` sends it when the
    /// relay it holds does not fit. The watchdog sends `false`: it fires on a
    /// timer off the ambient watermark, which the committer re-reads itself.
    Reclaim { forced: bool },
    /// DDL drain from `handle_ddl_txn`: deferred to the end of a checkpoint
    /// sequence (servicing it mid-sequence would interleave a CREATE VIEW's
    /// reactor-parked backfill).
    Ddl,
    /// Graceful shutdown: forces the full checkpoint sequence on the batch it
    /// arrives in regardless of SAL fullness, and resolves only at sequence
    /// end (deferred like `Ddl`) — so its `done` implies the base + drain +
    /// ephemeral rounds all completed.
    Shutdown,
}

/// One buffered single push awaiting commit. `done` resolves to `Ok(zone_lsn)` or
/// `Err(error_message)`.
pub struct PendingPush {
    pub tid: i64,
    pub batch: Batch,
    pub mode: WireConflictMode,
    /// Whether these rows are something a restart must recover, i.e. whether this
    /// group may open the zone the sentinel and fdatasync close. False only for a
    /// stream. Decided by the executor, which has already resolved the target's kind,
    /// so the committer never asks the catalog what a relation *is*.
    pub recoverable: bool,
    pub done: oneshot::Sender<Result<u64, WireFault>>,
}

/// The committer task loop. Returns when all senders drop (shutdown).
///
/// For normal commit groups `sal_writer_excl` is held only for the
/// synchronous SAL write + signal + fsync-SQE-submit triple, then
/// released before awaiting ACKs or the fsync CQE, so tick/relay tasks
/// can make progress during the wait.
///
/// For checkpoint flush rounds the lock is held across the ENTIRE round
/// (write + ACK wait + reset; see `flush_round`), but released across the
/// sequence's drain step so the tick loop can acquire it per tick.
pub async fn run(mut rx: chan::Receiver<CommitRequest>, shared: Rc<Shared>) {
    // `commit_pushes`' ACK scratch, reused across every commit to avoid a
    // per-commit Vec<ReplyFuture> + Vec<Option<DecodedWire>> pair. Sized for one
    // group's ACKs; commit_pushes grows them on the first multi-group batch and
    // reuses the capacity thereafter.
    let mut fut_slots: Vec<ReplyFuture> = Vec::with_capacity(shared.disp().num_workers());
    let mut ack_slots: Vec<Option<DecodedWire>> = Vec::with_capacity(shared.disp().num_workers());
    // Per-(tid, mode) merged-batch pool. `Batch::clear()` resets count and
    // blob without freeing the data buffer, so subsequent multi-push runs
    // skip the `strides_from_schema + zero-fill via buf.resize` cost.
    // Capacity sized for typical hot-table fan-out; grows on demand.
    const EXPECTED_HOT_TABLES: usize = 16;
    let mut merge_pool: FxHashMap<(i64, u8), Batch> =
        FxHashMap::with_capacity_and_hasher(EXPECTED_HOT_TABLES, Default::default());
    loop {
        // Block for the first request, exit if no senders remain.
        let first = match rx.recv().await {
            Some(req) => req,
            None => return,
        };

        // Drain any additional requests already queued — no timer wait.
        // Pipelined clients still get batched; serial clients don't pay
        // a latency tax.
        let mut batch = drain_ready_batch(&mut rx, first);

        // Checkpoint decision for the whole batch, barrier-only batches
        // included: relay_loop's low-space barrier arrives precisely to
        // reclaim SAL space. Barrier batches also honor the relay-space
        // threshold directly, covering GNITZ_CHECKPOINT_BYTES configs above
        // it. A Shutdown barrier forces the full sequence regardless of SAL
        // fullness. Must complete fully before commit groups go out: workers
        // bump their expected_epoch on Flush, so commit groups written
        // AFTER Flush in the same epoch would be silently skipped by
        // workers.
        let has_barriers = !batch.barriers.is_empty();
        // A Shutdown barrier, or a relay that reported its own group does not
        // fit — neither is decidable from the ambient space test below.
        let forced = batch
            .barriers
            .iter()
            .any(|(k, _)| matches!(k, BarrierKind::Shutdown | BarrierKind::Reclaim { forced: true }));
        // No checkpoint round inside a DDL window: `run_checkpoint_sequence`'s
        // drain would never complete against the parked tick loop, and the DDL's
        // own W2M collectors would eat the round's ACKs (see `TickGate`).
        // Refusing rather than waiting is safe — `relay_loop` cannot be mid-retry
        // once the Quiesce is acked (a worker ACKs its tick only after its relay
        // was written), the watchdog's barrier re-fires in 100 ms, and the
        // backfill reclaims through its own `checkpoint_before_backfill`.
        let ddl_window = shared.ddl_window.get() != 0;
        // Consume the one-shot force_checkpoint (a transaction that didn't fit
        // the SAL's remaining space set it), so the retry finds a reclaimed SAL
        // even when `sal_needs_checkpoint()` is still false. Left armed inside a
        // DDL window, where it could not be honoured anyway.
        let force_ckpt = !ddl_window && shared.force_checkpoint.take();
        let checkpoint = !ddl_window
            && (forced
                || force_ckpt
                || shared.disp().sal_needs_checkpoint()
                || (has_barriers && shared.disp().relay_fit(0) != SalFit::Fits));

        if checkpoint {
            // The full three-step sequence: gen bump → base round → drain →
            // ephemeral round. It signals the reclaim barriers right after
            // step 1 (so relay_loop unparks before the drain), defers the
            // DDL/Shutdown barriers to the end, and holds pushes/txns through the
            // sequence — folding any that arrive mid-drain into `batch`.
            run_checkpoint_sequence(&mut rx, &shared, &mut batch).await;
        }

        let PendingBatch { pushes, txns, barriers } = batch;
        if !pushes.is_empty() || !txns.is_empty() {
            commit_pushes(&shared, pushes, txns, &mut fut_slots, &mut ack_slots, &mut merge_pool).await;
        }

        // A DDL or shutdown barrier can change a schema out from under the pooled
        // per-(tid, mode) merge batches. A Reclaim barrier cannot, and the
        // watchdog fires those on a timer, so clearing on those too would drop
        // the pool every 100 ms while the SAL sits above its line.
        if barriers.iter().any(|(k, _)| !matches!(k, BarrierKind::Reclaim { .. })) {
            merge_pool.clear();
        }
        for (_, b) in barriers {
            b.send(());
        }
    }
}

/// One debounced committer batch: the single pushes to group-commit, the atomic
/// transactions to commit, plus the barrier senders to signal once the batch
/// (and any checkpoint sequence) completes.
struct PendingBatch {
    pushes: Vec<PendingPush>,
    txns: Vec<PendingTxn>,
    /// The batch's barriers on the way in. `run_checkpoint_sequence` partitions
    /// them — it signals the reclaim ones itself right after the base round —
    /// so from that point on this holds only the barriers still owed a signal,
    /// which the caller sends once the batch has committed.
    barriers: Vec<(BarrierKind, oneshot::Sender<()>)>,
}

/// Sort `first` into `pushes`/`barriers`, then drain additional requests
/// without waiting: if the channel has items ready, pull them all;
/// otherwise return immediately. The 1ms debounce timer is a worst-case
/// cost paid only by serial single-request clients, so we skip it
/// entirely and rely on pipelined clients to enqueue fast enough that
/// `try_recv` sees a non-empty queue.
fn drain_ready_batch(rx: &mut chan::Receiver<CommitRequest>, first: CommitRequest) -> PendingBatch {
    let mut pushes = Vec::new();
    let mut txns: Vec<PendingTxn> = Vec::new();
    let mut barriers = Vec::new();
    let mut row_count: usize = 0;
    match first {
        CommitRequest::Push(p) => {
            row_count = p.batch.count;
            pushes.push(p);
        }
        // A transaction is one indivisible entry — its whole family set rides
        // this batch; its row count is the sum of every family's rows.
        CommitRequest::Txn(txn) => {
            row_count = txn.families.iter().map(|f| f.batch.count).sum();
            txns.push(txn);
        }
        CommitRequest::Barrier { kind, done } => barriers.push((kind, done)),
    }
    while row_count < MAX_PENDING_ROWS {
        match rx.try_recv() {
            Some(CommitRequest::Push(p)) => {
                row_count += p.batch.count;
                pushes.push(p);
            }
            Some(CommitRequest::Txn(txn)) => {
                row_count += txn.families.iter().map(|f| f.batch.count).sum::<usize>();
                txns.push(txn);
            }
            Some(CommitRequest::Barrier { kind, done }) => {
                // Stop at the first barrier: requests queued after it are
                // logically younger than whatever issued the barrier (a DDL, or a
                // low-SAL-space relay reclaim), so folding them into this batch
                // would make the barrier wait on their commit for no ordering
                // benefit — and, for the reclaim barrier, would keep consuming the
                // SAL space it is trying to free. They ride the next batch.
                barriers.push((kind, done));
                break;
            }
            None => break,
        }
    }
    PendingBatch { pushes, txns, barriers }
}

/// Emit one broadcast flush group and reset the SAL, holding `sal_writer_excl`
/// for the ENTIRE window (write + ACK wait + reset).
///
/// Releasing the lock before the await would let concurrent tick/relay/DDL/
/// fan-out tasks write SAL groups with the old epoch. Workers bump
/// `expected_epoch` when they process a flush, so those groups would be silently
/// skipped, and the reset then orphans them permanently while their writers wait
/// for ACKs that never arrive.
///
/// `ephemeral_gen: None` — base/reclaim round: `Flush` (lsn 0, unused).
/// `Some(gen)` — ephemeral round: `FlushEph` carrying the generation in
/// the group header's `lsn` field (workers read it to stamp view manifests).
/// Both rounds finalize identically via `checkpoint_post_ack` (system-table
/// flush + gated-deletion drain + reset): a `commit_serial_range_durable`
/// advance can land in the `sys_sequences` MemTable during the drain window
/// after the base round's reset, so the ephemeral reset must flush the system
/// tables first or that advance is discarded on a crash.
async fn flush_round(shared: &Rc<Shared>, ephemeral_gen: Option<u64>) -> Result<(), String> {
    let nw = shared.disp().num_workers();
    // A round already costs a broadcast, an `nw`-way ACK wait, a system-table
    // flush and a SAL reset, so it allocates its own scratch rather than
    // threading `run`'s through three frames; `req_ids` below is allocated per
    // round on the same grounds.
    let req_ids = shared.reactor.alloc_replies(nw);
    let mut fut_slots: Vec<ReplyFuture> = Vec::with_capacity(nw);
    let mut ack_slots: Vec<Option<DecodedWire>> = Vec::with_capacity(nw);

    let _sal_excl = shared.disp().sal_excl().lock().await;

    {
        let disp = shared.disp();
        // FlushEph's lsn IS the checkpoint generation — workers latch it via
        // `set_resume_generation`. The base round's lsn is unread; pass 0.
        let (lsn, kind) = match ephemeral_gen {
            Some(gen) => (gen, SalMessageKind::FlushEph),
            None => (0, SalMessageKind::Flush),
        };
        disp.write_checkpoint_group(lsn, kind, GroupTargets::All(&req_ids))
            .map_err(|f| f.text)?;
        disp.signal_all();
    }

    await_worker_acks(&shared.reactor, &req_ids, "checkpoint", &mut fut_slots, &mut ack_slots).await?;
    // Both rounds finalize the same way: flush system tables, then reset the SAL.
    guard_panic("checkpoint_post_ack", || shared.disp().checkpoint_post_ack())
}

/// The full steady-state checkpoint sequence: gen bump → base round → drain →
/// quiesce → ephemeral round. Signals the reclaim barriers right after step 1,
/// and leaves `batch` holding the pushes it held across the sequence — folded
/// with any that arrived mid-drain — plus the deferred DDL/Shutdown barriers,
/// for the caller to commit and signal.
///
/// This sequence and boot's pre-reactor `boot_checkpoint` are the only SAL
/// checkpoint drivers; `MasterDispatcher::reclaim_base` states the exclusivity a
/// third one would have to own. Inside a round `flush_round` holds
/// `sal_writer_excl` across write + ACK wait + reset, and that span cannot be
/// shortened: a writer admitted before the reset emits its group in the old
/// epoch, which every worker skips, and the reset then orphans that group and
/// hangs its writer. The lock is released across the step-2 drain because the
/// tick task re-acquires it per tick.
async fn run_checkpoint_sequence(
    rx: &mut chan::Receiver<CommitRequest>,
    shared: &Rc<Shared>,
    batch: &mut PendingBatch,
) {
    // Step 0: gen bump. From this instant every existing rederived manifest is
    // stale; a crash below rebuilds views instead of silently staleifying them.
    let gen = match shared.disp().bump_checkpoint_generation() {
        Ok(g) => g,
        // The generation bump is what makes every existing rederived manifest
        // stale before the rounds below overwrite the base. Failing it leaves
        // the SAL un-reset, so restart replays it — but continuing the sequence
        // would publish a base cut that no view manifest is invalidated against.
        Err(e) => gnitz_fatal_abort!("checkpoint generation bump failed: {}", e),
    };

    // Step 1: base round. A failure is unrecoverable in-process (workers
    // already bumped their read epoch on Flush but the master did not
    // reset the SAL, so the cluster is epoch-desynced). Abort so restart
    // replays the un-reset SAL and re-derives views from the durable base
    // tables.
    if let Err(e) = flush_round(shared, None).await {
        gnitz_fatal_abort!("checkpoint failed, cluster epoch-desynced: {}", e);
    }

    // Unpark relay_loop now: step 1's reset already reclaimed space, so the
    // triggering reclaim barriers can be released before the drain (the drain's
    // own relays refill space, and relay_loop must be live to service them).
    // Deferring them past the drain would deadlock it. DDL/Shutdown barriers
    // stay deferred to sequence end.
    let (reclaim, deferred): (Vec<_>, Vec<_>) = std::mem::take(&mut batch.barriers)
        .into_iter()
        .partition(|(k, _)| matches!(k, BarrierKind::Reclaim { .. }));
    batch.barriers = deferred;
    for (_, b) in reclaim {
        b.send(());
    }

    // Step 2 — DRAIN (lock released). One Drain suffices: the tick loop walks the
    // source's full dependent closure with inline exchange rounds, and pushes are
    // held so `tick_rows` cannot grow. Mirrors the SCAN drain.
    let (done_tx, done_rx) = oneshot::channel::<Result<(), String>>();
    shared.tick_tx.send(TickTrigger::Drain { done: done_tx });
    let drained = await_servicing(done_rx, rx, batch, shared).await;
    // Step 3 would stamp every view manifest at `gen` while the views are missing
    // the deltas this drain failed to tick — durable, generation-valid loss. Skip
    // it: step 0's bump is already durable and the manifests are still at
    // `gen - 1`, so `compute_invalid_views` rebuilds them from the base tables on
    // the next boot. Aborting would be equally safe, but this path also runs on
    // the Shutdown barrier, so it would turn any tick error during the final
    // drain into `_exit(134)`.
    match drained {
        Some(Ok(())) => {}
        Some(Err(e)) => {
            gnitz_warn!("checkpoint drain failed, skipping the ephemeral round: {}", e);
            return;
        }
        // Cancelled, or the request channel closed.
        None => return,
    }

    // Quiesce so no tick runs during the ephemeral flush.
    let (acked_tx, acked_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    shared.tick_tx.send(TickTrigger::Quiesce {
        acked: acked_tx,
        release: release_rx,
    });
    let _ = await_servicing(acked_rx, rx, batch, shared).await;

    // Step 3 — EPHEMERAL ROUND. Stamp the step-0 generation (no re-bump happens
    // mid-drain, so no re-read is needed).
    if let Err(e) = flush_round(shared, Some(gen)).await {
        gnitz_fatal_abort!("ephemeral checkpoint round failed, cluster epoch-desynced: {}", e);
    }
    release_tx.send(()); // resume the tick loop
}

/// Await `target_rx` (a Drain `done` or Quiesce `acked`) while keeping the
/// committer responsive: a reclaim barrier is serviced by a reclaim-only base
/// round (so relay_loop refills SAL space during the drain), DDL/Shutdown
/// barriers are deferred to sequence end, and pushes are held for the folded
/// commit. The target is re-polled after each serviced request, so no wakeup
/// is lost.
///
/// Returns the target's payload — `None` if it was cancelled or the request
/// channel closed. The Drain target carries the tick's verdict, which decides
/// whether the sequence's ephemeral round may run.
async fn await_servicing<T>(
    target_rx: oneshot::Receiver<T>,
    rx: &mut chan::Receiver<CommitRequest>,
    batch: &mut PendingBatch,
    shared: &Rc<Shared>,
) -> Option<T> {
    // `oneshot::Receiver` is `Unpin`, so `&mut target` is itself a Future.
    let mut target = target_rx;
    loop {
        match select2(&mut target, rx.recv()).await {
            // Target fired (drain done / quiesce acked), or the channel closed.
            Either::A(v) => return v,
            Either::B(None) => return None,
            Either::B(Some(CommitRequest::Barrier {
                kind: BarrierKind::Reclaim { forced },
                done,
            })) => {
                // Reclaim-only base round: no gen re-bump, no re-staleing of
                // view manifests. Gated on the same predicate `run` uses — step 1
                // of the sequence in progress has already reset the SAL, and the
                // watchdog fires these on a timer without waiting for the last
                // one, so an ungated round here would repeat a full
                // broadcast + per-worker-ACK + system-table flush for nothing.
                // A `forced` request states a requirement that predicate cannot
                // see, so it runs the round regardless.
                if forced || shared.disp().relay_fit(0) != SalFit::Fits {
                    if let Err(e) = flush_round(shared, None).await {
                        gnitz_fatal_abort!("reclaim checkpoint failed, cluster epoch-desynced: {}", e);
                    }
                }
                done.send(());
            }
            Either::B(Some(CommitRequest::Barrier { kind, done })) => batch.barriers.push((kind, done)),
            Either::B(Some(CommitRequest::Push(p))) => batch.pushes.push(p),
            Either::B(Some(CommitRequest::Txn(txn))) => batch.txns.push(txn),
        }
    }
}

/// One homogeneous (tid, mode) SAL group: a merged run of single pushes, or one
/// transaction family.
struct GroupInfo {
    tid: i64,
    mode: WireConflictMode,
    /// See `CommitRequest::Push::recoverable`. A merged run is homogeneous in
    /// `(tid, mode)`, so one flag per group is exact.
    recoverable: bool,
    req_ids: ReplyLease,
    merged: Batch,
    write_err: Option<WireFault>,
}

/// One client-visible commit unit — the span of `groups` it emits and the `done`
/// senders that share its verdict. A merged single-push run is one group with N
/// dones (one per coalesced client request); a transaction is N groups (one per
/// family, in frame order) with one done. Units tile `groups` contiguously in
/// emission order, so iterating units visits groups in group order.
struct CommitUnit {
    groups: std::ops::Range<usize>,
    dones: Vec<oneshot::Sender<Result<u64, WireFault>>>,
    /// Whether a worker-ACK error may downgrade this unit's verdict. Single
    /// pushes degrade gracefully per group, so they do. A transaction's contract
    /// is "Err ⇒ nothing committed", so once its sentinel is durable a worker
    /// error must never turn its `Ok` into an `Err`. Dropping the error instead
    /// is sound only while no *graceful* worker push error can reach here: a real
    /// apply error fail-stops the worker, and "table not registered" is excluded
    /// by the DDL tick quiesce (`TickGate`) plus the catalog read lock held
    /// through the ACK. `commit_pushes` aborts rather than trusting that in prose.
    downgrade_on_worker_err: bool,
}

impl CommitUnit {
    /// Resolve every `done` of this unit exactly once: `Ok(zone_lsn)` iff all of
    /// its groups committed, else the first group error.
    fn resolve(self, groups: &[GroupInfo], zone_lsn: u64) {
        let result = match groups[self.groups].iter().find_map(|g| g.write_err.clone()) {
            Some(e) => Err(e),
            None => Ok(zone_lsn),
        };
        for done in self.dones {
            done.send(result.clone());
        }
    }
}

/// Commit one debounced batch of pushes. Emits every group's SAL writes under
/// `sal_writer_excl` (alongside the signal + fsync SQE submit), releases the
/// lock, THEN awaits worker ACKs (Phase C) and the fsync CQE (Phase D). LSN
/// assignment and `done.send` happen after worker ACKs; the unique-index filter
/// update happens after fsync.
async fn commit_pushes(
    shared: &Rc<Shared>,
    mut pushes: Vec<PendingPush>,
    txns: Vec<PendingTxn>,
    fut_slots: &mut Vec<ReplyFuture>,
    ack_slots: &mut Vec<Option<DecodedWire>>,
    merge_pool: &mut FxHashMap<(i64, u8), Batch>,
) {
    // Sort by (tid, mode) so runs are homogeneous.
    pushes.sort_by_key(|p| (p.tid, p.mode.as_wire()));

    let nw = shared.disp().num_workers();
    let mut groups: Vec<GroupInfo> = Vec::new();
    let mut units: Vec<CommitUnit> = Vec::with_capacity(txns.len());
    let alloc_req_ids = || shared.reactor.alloc_replies(nw);

    // ------------------------------------------------------------------
    // Phase A (no lock): build merged batches + req_id allocations.
    // Transaction units come FIRST (transactions-first emission), each family its
    // own group in frame order; then the merged single-push units.
    // ------------------------------------------------------------------
    for txn in txns {
        let gstart = groups.len();
        for fam in txn.families {
            groups.push(GroupInfo {
                tid: fam.tid,
                mode: fam.mode,
                recoverable: true,
                req_ids: alloc_req_ids(),
                merged: fam.batch,
                write_err: None,
            });
        }
        units.push(CommitUnit {
            groups: gstart..groups.len(),
            dones: vec![txn.done],
            downgrade_on_worker_err: false,
        });
    }

    // Single-push runs: walk the sorted pushes, draining each maximal (tid, mode)
    // run into one merged group.
    let mut remaining = pushes.into_iter().peekable();
    while let Some(first) = remaining.next() {
        let (tid, mode, recoverable) = (first.tid, first.mode, first.recoverable);
        // Split the run into its two independent halves as it is drained: the
        // batches the merge consumes, and the `done` senders the unit resolves.
        let mut batches: Vec<Batch> = vec![first.batch];
        let mut dones = vec![first.done];
        while remaining.peek().is_some_and(|p| p.tid == tid && p.mode == mode) {
            let p = remaining.next().unwrap();
            batches.push(p.batch);
            dones.push(p.done);
        }
        let total_rows: usize = batches.iter().map(|b| b.count).sum();

        // Single-push: take ownership of the client's batch (no merge).
        // Multi-push: pop a pooled merged batch (or alloc fresh) and
        // refill it. DDL between bursts can change the schema, so
        // mismatched pool entries are dropped and reallocated — without
        // this, append_batch writes rows under the wrong column layout.
        let built = guard_panic("commit_merge", || {
            if batches.len() == 1 {
                return Ok(batches.pop().expect("one batch in a single-push run"));
            }
            let schema = shared.disp().schema_desc_for(tid);
            let mut m = match merge_pool.remove(&(tid, mode.as_wire())) {
                Some(pooled) if pooled.schema == schema => pooled,
                _ => Batch::with_capacity(schema, total_rows.max(1)),
            };
            m.clear();
            for pb in batches.iter() {
                m.append_batch(pb, 0, pb.count);
            }
            Ok(m)
        });
        let (merged, write_err) = match built {
            Ok(m) => (m, None),
            Err(panic_msg) => {
                let placeholder = guard_panic("commit_fallback_schema", || {
                    Ok(Batch::empty_with_schema(&shared.disp().schema_desc_for(tid)))
                })
                .unwrap_or_else(|_| Batch::empty_with_schema(&gnitz_engine::schema::SchemaDescriptor::minimal_u64()));
                (placeholder, Some(WireFault::from(panic_msg)))
            }
        };

        let gstart = groups.len();
        groups.push(GroupInfo {
            tid,
            mode,
            recoverable,
            req_ids: alloc_req_ids(),
            merged,
            write_err,
        });
        units.push(CommitUnit {
            groups: gstart..groups.len(),
            dones,
            downgrade_on_worker_err: true,
        });
    }

    // ------------------------------------------------------------------
    // Phase B (under lock): emit SAL groups, signal, submit fsync SQE.
    // Lock dropped immediately after; ACKs and fsync CQE are awaited
    // outside so tick/relay/DDL tasks can make progress.
    //
    // All groups in this batch share one zone_lsn. A batch with a recoverable group
    // opens a zone and closes it with the commit sentinel after all groups, which
    // lets recovery treat it atomically: either every group applies or none do. Its
    // zone LSN is published once, after fsync (Phase D), so clients only see durable
    // LSNs. A batch of nothing but stream groups writes no zone at all.
    // ------------------------------------------------------------------
    let (zone_lsn, fsync_fut) = {
        let _sal_excl = shared.disp().sal_excl().lock().await;

        // Reserve under sal_writer_excl so reservation order == SAL write order
        // across every durable allocator (this committer, DDL, SERIAL). A
        // user-table push pins no system-family counter, so the floor is 0.
        let zone_lsn = shared.lsn_alloc.reserve(0);

        // Nothing laid out below is visible until `publish_pending`, so a
        // transaction that runs out of SAL space part-way can take its earlier
        // families back. Every write, the sentinel and the fsync submit are in
        // this one synchronous block, so no reader ever observes the gap.
        shared.disp().defer_publication();

        // Opened by the first recoverable group that reaches the SAL.
        let mut zone_opened = false;

        // Emit every unit into the zone, in unit order (transactions first).
        for unit in &units {
            let span = unit.groups.clone();
            if unit.downgrade_on_worker_err {
                // A single-push group degrades gracefully: a refused group wrote
                // zero bytes, and the batch simply skips it.
                let g = &mut groups[span.start];
                if g.write_err.is_none() {
                    g.write_err = lay_out_group(shared, g, zone_lsn, &mut zone_opened).err();
                }
                continue;
            }

            // A transaction is all-or-nothing: a family that does not fit rolls
            // the whole bundle back to where it started, and the families before
            // it were never published.
            let savepoint = shared.disp().savepoint();
            let zone_before = zone_opened;
            let mut fail = None;
            for gi in span.clone() {
                let g = &mut groups[gi];
                if let Err(e) = lay_out_group(shared, g, zone_lsn, &mut zone_opened) {
                    fail = Some(e);
                    break;
                }
            }
            if let Some(e) = fail {
                shared.disp().roll_back(savepoint);
                zone_opened = zone_before;
                for g in groups[span].iter_mut() {
                    g.write_err.get_or_insert_with(|| e.clone());
                }
            }
        }

        let any_written = groups.iter().any(|g| g.write_err.is_none());
        if !any_written {
            // No group wrote → no sentinel; every unit resolves to its first error.
            shared.disp().publish_pending();
            for unit in units {
                unit.resolve(&groups, zone_lsn);
            }
            return;
        }

        // Every laid-out group becomes visible here, before the sentinel — which
        // is what the crash seam below cuts between.
        shared.disp().publish_pending();

        // Abort after push groups but BEFORE the commit sentinel. SAL recovery
        // skips any zone whose sentinel is absent; workers that already flushed
        // their shard files before the crash retain the data via the shard path,
        // so this seam is for targeted debugging rather than asserting
        // invisibility after restart.
        if PUSH_ABORT.at("after_groups") {
            unsafe {
                libc::abort();
            }
        }

        // Close the zone with the commit sentinel before fsync.  If this
        // fails the zone has no commit sentinel and recovery would silently
        // drop all groups in the zone — that is unrecoverable data loss
        // after a restart while the client already received Ok.  Abort.
        // `commit_zone` signals the workers itself once the sentinel is published.
        let fsync_fut = if zone_opened {
            if let Some(e) = shared.disp().commit_zone(zone_lsn).err() {
                gnitz_fatal_abort!("commit_zone failed, durability lost: {}", e);
            }
            // Submit fsync SQE (synchronous — returns a future). The
            // ReplyFutures are built into `fut_slots` outside the lock scope;
            // the reply slots themselves were opened by `alloc_replies` in
            // Phase A, so nothing is missed in between.
            Some(shared.reactor.fsync(shared.disp().sal_fd()))
        } else {
            // The signal `commit_zone` would have sent. Phase C awaits its ACKs.
            shared.disp().signal_all();
            None
        };
        (zone_lsn, fsync_fut)
    };

    // Build per-worker reply futures into the caller-supplied scratch
    // buffer. Holds one block of `nw` entries for each group with
    // `write_err.is_none()`, in iteration order — failed groups have no
    // slot, so the cursor in Phase C must skip them without advancing.
    fut_slots.clear();
    for g in groups.iter().filter(|g| g.write_err.is_none()) {
        fut_slots.extend(g.req_ids.iter().map(|&id| shared.reactor.await_reply(id)));
    }

    // ------------------------------------------------------------------
    // Phase C (no lock): await push ACKs, fire tick.
    // fsync is awaited separately in Phase D so DAG evaluation overlaps
    // with fdatasync (~5 ms gap eliminated). LSN publish is deferred to
    // after fsync so clients only see a durable LSN.
    // unique_filter_ingest_batch is NOT called here: a filter entry for rows a
    // crash would discard makes the next INSERT of the same key fail a
    // uniqueness check nothing durable backs. It runs after Phase D's fsync.
    // ------------------------------------------------------------------
    {
        join_into(fut_slots, ack_slots).await;

        // Units tile `groups` in emission order, so this visits groups in the same
        // order `fut_slots` was built in and the ACK cursor stays aligned.
        let mut cursor = 0usize;
        for unit in &units {
            for gi in unit.groups.clone() {
                if groups[gi].write_err.is_some() {
                    continue;
                }
                let worker_err = first_worker_error_opt("commit", &ack_slots[cursor..cursor + nw]);
                cursor += nw;
                match (unit.downgrade_on_worker_err, worker_err) {
                    (true, e) => groups[gi].write_err = e,
                    // Discarding it would leave the transaction durable in the SAL
                    // but missing from this worker's partition, with the client
                    // told Ok — silent divergence. Nothing may reach here (see
                    // `downgrade_on_worker_err`), so treat it as unrecoverable
                    // rather than diverging quietly.
                    (false, Some(e)) => gnitz_fatal_abort!(
                        "worker rejected a committed transaction group (tid={}): {}",
                        groups[gi].tid,
                        e
                    ),
                    (false, None) => {}
                }
            }
        }
        // Drop DecodedWire heap fields (data_batch, error_msg) before the
        // committer parks waiting for the next request. Capacity stays
        // resident so the next commit reuses it.
        ack_slots.clear();

        // Invalidate filters for groups whose worker ACK reported an error.
        // LSN publish + filter ingest are deferred to Phase D after fsync.
        for g in groups.iter().filter(|g| g.write_err.is_some()) {
            invalidate_filters(shared, g.tid);
        }

        // Bump tick counters and maybe fire the auto-tick BEFORE awaiting
        // the fsync CQE so DAG evaluation overlaps with fdatasync. We
        // bump tick_rows on writes that succeeded at the worker level —
        // the LSN publish is deferred but tick batching can proceed.
        {
            let mut tr = shared.tick_rows.borrow_mut();
            for g in &groups {
                if g.write_err.is_none() {
                    *tr.entry(g.tid).or_insert(0) += g.merged.count;
                }
            }
        }
        if shared.any_threshold_crossed() {
            shared.tick_tx.send(TickTrigger::Auto);
        }
    }

    // ------------------------------------------------------------------
    // Phase D (no lock): await fsync CQE.  Client response is held until
    // after fsync so the client sees only durable data. A batch of nothing but
    // stream groups opened no zone: no fsync to await, and no LSN to publish.
    // ------------------------------------------------------------------
    if let Some(fsync_fut) = fsync_fut {
        let fsync_rc = fsync_fut.await;
        if fsync_rc < 0 {
            gnitz_fatal_abort!("SAL fdatasync (committer) failed rc={}", fsync_rc);
        }
        // Publish the zone LSN exactly once, after fsync confirms durability.
        // Pipelined pushes batched together share one zone_lsn, so clients may see
        // duplicate LSNs — only non-decreasing monotonicity is guaranteed.
        // `write_err` can still be set in Phase C.
        if groups.iter().any(|g| g.write_err.is_none()) {
            shared.lsn_alloc.publish(zone_lsn);
        }
    }

    // Update unique-index filters now that fsync confirms durability.
    // Wrapped for task liveness: a panic here must not fail the commit —
    // the data is already durable. Invalidate on panic so the next
    // constrained INSERT re-validates from scratch.
    for g in groups.iter().filter(|g| g.write_err.is_none()) {
        if let Err(e) = guard_panic("unique_filter_ingest", || {
            shared.disp().unique_filter_ingest_batch(g.tid, &g.merged);
            Ok(())
        }) {
            invalidate_filters(shared, g.tid);
            gnitz_warn!("{}", e);
        }
    }

    // Send responses. A single-push unit resolves each coalesced client's `done`
    // from its one group; a transaction's single `done` resolves `Ok` iff every
    // family group committed.
    for unit in units {
        unit.resolve(&groups, zone_lsn);
    }

    // Return the batches to the (tid, mode) pool so the next burst skips the
    // strides_from_schema + zero-fill cost. Single-push batches (originally the
    // client's) are pooled too — the schema-staleness guard above discards them
    // on a DDL change.
    for g in groups {
        merge_pool.insert((g.tid, g.mode.as_wire()), g.merged);
    }
}

/// Drop a table's unique-index filters so the next constrained INSERT
/// re-validates from scratch. Guarded: this runs on paths where the data is
/// already durable, so a panic in the filter map must not fail the commit.
fn invalidate_filters(shared: &Rc<Shared>, tid: i64) {
    let _ = guard_panic("unique_filter_invalidate", || {
        shared.disp().unique_filter_invalidate_table(tid);
        Ok(())
    });
}

/// Lay one group out into the open zone. Wrapped in `guard_panic` so a malformed
/// batch fails the group instead of the node.
///
/// The zone is opened by the first `recoverable` group that actually writes bytes —
/// a refused group writes none. A [`SalFit::Transient`] refusal arms the forced
/// checkpoint here, where the verdict is observed, so a client's retry finds a
/// reclaimed SAL whether the refusal cost it a single push or a transaction.
fn lay_out_group(shared: &Rc<Shared>, g: &GroupInfo, zone_lsn: u64, zone_opened: &mut bool) -> Result<(), WireFault> {
    let zone_start = !*zone_opened && g.recoverable;
    let mark = if zone_start { ZoneMark::Start } else { ZoneMark::Plain };
    // The guard covers the encode, which reads a client-supplied batch; the SAL's
    // own verdict comes back out of it typed.
    let refused = guard_panic("commit_write", || {
        Ok(shared
            .disp()
            .write_commit_group(g.tid, zone_lsn, &g.merged, g.mode, &g.req_ids, mark)
            .err())
    })?;
    if let Some(fit) = refused {
        if fit == SalFit::Transient {
            shared.force_checkpoint.set(true);
        }
        return Err(fit.refusal("commit group"));
    }
    if zone_start {
        *zone_opened = true;
    }
    Ok(())
}
