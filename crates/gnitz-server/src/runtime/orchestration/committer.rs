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
//!   drain/quiesce → ephemeral round), then proceeds with the push batch.
//!   `checkpoint_warranted` is the one statement of when.
//! - **Barrier.**  A `Barrier` request flushes any in-flight batch and
//!   signals via a oneshot — used by DDL to drain the committer before
//!   catalog mutation, by a tick's relay to reclaim SAL space, and by the
//!   graceful-shutdown watchdog (see `BarrierKind`).

use super::executor::{request_drain, request_quiesce, Shared};
use super::guard_panic;
use super::TxnFamily;
use crate::runtime::master::worker_error;
use crate::runtime::reactor::{chan, oneshot, select2, Either, Lease};
use crate::runtime::sal::{GroupTargets, SalMessageKind, SalScope};
use gnitz_store::storage::Batch;
use gnitz_wire::WireFault;
use std::rc::Rc;

/// Row ceiling on one committer batch. Tested before the receive, so a batch is
/// capped at this plus one whole request — and one request is itself bounded
/// only by `MAX_FRAME_PAYLOAD_SERVER`.
const MAX_PENDING_ROWS: usize = 100_000;

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
    /// (no gen re-bump) and signaled right after step 1, so the tick waiting on it
    /// resumes before the drain it must serve.
    ///
    /// `forced` means the requester has a concrete byte requirement the
    /// committer's own space test cannot see — a tick's relay sends it when the
    /// relay it holds does not fit. The watchdog sends `false`: it fires on a
    /// timer off the ambient watermark, which the committer re-reads itself.
    Reclaim { forced: bool },
    /// DDL drain from `handle_ddl_txn`: deferred to the end of a checkpoint
    /// sequence (servicing it mid-sequence would interleave a CREATE VIEW's
    /// exclusive backfill rounds).
    Ddl,
    /// Graceful shutdown: forces the full checkpoint sequence on the batch it
    /// arrives in regardless of SAL fullness, and resolves only at sequence
    /// end (deferred like `Ddl`) — so its `done` implies the base + drain +
    /// ephemeral rounds were all attempted. The drain's own failure path skips
    /// the ephemeral round rather than aborting, precisely because Shutdown runs
    /// here.
    Shutdown,
}

/// One buffered single push awaiting commit. `done` resolves to `Ok(zone_lsn)` or
/// `Err(error_message)`.
pub struct PendingPush {
    pub tid: i64,
    pub batch: Batch,
    /// Whether these rows are something a restart must recover, i.e. whether this
    /// group may open the zone the sentinel and fdatasync close. False only for a
    /// stream. Decided by the executor, which has already resolved the target's kind,
    /// so the committer never asks the catalog what a relation *is*.
    pub recoverable: bool,
    pub done: oneshot::Sender<Result<u64, WireFault>>,
}

/// The committer task loop. Returns when the reactor shutdown drops this task.
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
    loop {
        let first = rx.recv().await;

        // Drain any additional requests already queued — no timer wait.
        // Pipelined clients still get batched; serial clients don't pay
        // a latency tax.
        let mut batch = drain_ready_batch(&mut rx, first);

        // Before this batch's groups, never after: workers bump their
        // expected_epoch on Flush, so a group written behind one in the same
        // epoch is silently skipped.
        if checkpoint_warranted(&shared, &batch) {
            run_checkpoint_sequence(&mut rx, &shared, &mut batch).await;
        }

        let PendingBatch { pushes, txns, barriers } = batch;
        if !pushes.is_empty() || !txns.is_empty() {
            commit_pushes(&shared, pushes, txns).await;
        }

        for (_, b) in barriers {
            b.send(());
        }
    }
}

/// Whether this batch warrants the full checkpoint sequence — barrier-only
/// batches included, since a tick relay's low-space barrier arrives precisely to
/// reclaim SAL space.
///
/// Never inside a DDL window, where the sequence cannot run. Refusing rather
/// than waiting is safe: no tick relay can be mid-retry once the Quiesce is
/// acked (the tick loop is serial and relays inside its tick), the
/// watchdog's barrier re-fires in 100 ms, and a DDL's own `Ddl` barrier reaches
/// this decision before its window opens.
fn checkpoint_warranted(shared: &Shared, batch: &PendingBatch) -> bool {
    if shared.ddl_window.get() != 0 {
        return false;
    }
    let forced = batch
        .barriers
        .iter()
        .any(|(k, _)| matches!(k, BarrierKind::Shutdown | BarrierKind::Reclaim { forced: true }));
    let disp = shared.disp();
    forced || disp.sal_needs_checkpoint() || disp.sal_space_low()
}

/// One committer batch: the single pushes to group-commit, the atomic
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

/// Sort `first` into the batch, then keep draining requests already queued
/// behind it — without ever waiting — until the row cap is reached, a barrier
/// arrives, or the channel runs dry.
///
/// A barrier ends the batch wherever it lands, `first` included: what is queued
/// behind it is younger than whatever issued it, so folding that in would make
/// the barrier wait on a commit it has no ordering interest in. It rides the
/// next `rx.recv()`, which this loop reaches immediately.
fn drain_ready_batch(rx: &mut chan::Receiver<CommitRequest>, first: CommitRequest) -> PendingBatch {
    let mut b = PendingBatch {
        pushes: Vec::new(),
        txns: Vec::new(),
        barriers: Vec::new(),
    };
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
                b.barriers.push((kind, done));
                break;
            }
        }
        next = (row_count < MAX_PENDING_ROWS).then(|| rx.try_recv()).flatten();
    }
    b
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
/// Every step aborts on failure rather than reporting one: a failed round leaves
/// the workers re-epoched and the SAL un-reset, so the cluster is epoch-desynced
/// and nothing in-process can put it back. Aborting makes restart replay the
/// un-reset SAL and re-derive views from the durable base tables.
///
/// `ephemeral_gen: None` — base/reclaim round: `Flush` (lsn 0, unused).
/// `Some(gen)` — ephemeral round: `FlushEph` carrying the generation in
/// the group header's `lsn` field (workers latch it via `set_resume_generation`
/// to stamp view manifests).
/// Both rounds finalize identically via `checkpoint_post_ack` (system-table
/// flush + gated-deletion drain + reset): a `commit_serial_range_durable`
/// advance can land in the `sys_sequences` MemTable during the drain window
/// after the base round's reset, so the ephemeral reset must flush the system
/// tables first or that advance is discarded on a crash.
async fn flush_round(shared: &Rc<Shared>, ephemeral_gen: Option<u64>) {
    let nw = shared.disp().num_workers();
    let req_ids = shared.reactor.lease_acks(nw);

    // `ephemeral_gen` already discriminates the two rounds, so it names the one a
    // failure is reported against too.
    let (lsn, kind, round) = match ephemeral_gen {
        Some(gen) => (gen, SalMessageKind::FlushEph, "ephemeral"),
        None => (0, SalMessageKind::Flush, "base"),
    };

    let _sal_excl = shared.disp().sal_excl().lock().await;

    {
        let disp = shared.disp();
        if let Err(e) = disp.write_checkpoint_group(lsn, kind, GroupTargets::All(req_ids.base())) {
            gnitz_fatal_abort!("checkpoint {} round: flush group write failed: {}", round, e);
        }
        disp.signal_all();
    }

    if let Err(e) = req_ids.acks(nw, |w, c| worker_error(w, "checkpoint", c)).await {
        gnitz_fatal_abort!("checkpoint {} round: worker ACK failed: {}", round, e);
    }
    // Both rounds finalize the same way: flush system tables, then reset the SAL.
    if let Err(e) = shared.disp().checkpoint_post_ack() {
        gnitz_fatal_abort!("checkpoint {} round: post-ACK finalize failed: {}", round, e);
    }
}

/// The full steady-state checkpoint sequence: gen bump → base round → drain →
/// quiesce → ephemeral round. Signals the reclaim barriers right after step 1,
/// and leaves `batch` holding the pushes it held across the sequence — folded
/// with any that arrived mid-drain — plus the deferred DDL/Shutdown barriers,
/// for the caller to commit and signal.
///
/// Must not run inside a DDL window: the drain below would never complete
/// against the parked tick loop (see `TickGate`).
///
/// `MasterDispatcher::reclaim_base` states the exclusivity every SAL checkpoint
/// driver must own. The rounds hold `sal_writer_excl` (see `flush_round`); the
/// step-2 drain does not, because the tick task re-acquires it per tick.
async fn run_checkpoint_sequence(
    rx: &mut chan::Receiver<CommitRequest>,
    shared: &Rc<Shared>,
    batch: &mut PendingBatch,
) {
    // Step 0: gen bump. From this instant every existing rederived manifest is
    // stale; a crash below rebuilds views instead of silently staleifying them.
    let gen = match shared.disp().cat().bump_checkpoint_generation() {
        Ok(g) => g,
        // Continuing would publish a base cut no view manifest is invalidated
        // against. Aborting leaves the SAL un-reset, so restart replays it.
        Err(e) => gnitz_fatal_abort!("checkpoint generation bump failed: {}", e),
    };

    // Step 1: base round. A failure inside it is unrecoverable in-process and
    // aborts there; see `flush_round`.
    flush_round(shared, None).await;

    // Release the reclaim barriers now: step 1's reset already reclaimed space, and
    // a tick waiting on one must finish before the tick loop can take the drain
    // below. Deferring them past the drain would deadlock it. DDL/Shutdown
    // barriers stay deferred to sequence end.
    let (reclaim, deferred): (Vec<_>, Vec<_>) = std::mem::take(&mut batch.barriers)
        .into_iter()
        .partition(|(k, _)| matches!(k, BarrierKind::Reclaim { .. }));
    batch.barriers = deferred;
    for (_, b) in reclaim {
        b.send(());
    }

    // Step 2 — DRAIN (lock released). One Drain suffices: the tick loop walks the
    // source's full dependent closure with inline exchange rounds, and pushes are
    // held so the pending-tick counts cannot grow. Mirrors the SCAN drain.
    let drained = await_servicing(request_drain(shared), rx, batch, shared).await;
    // Step 3 would stamp every view manifest at `gen` while the views are missing
    // the deltas this drain failed to tick — durable, generation-valid loss. Skip
    // it: step 0's bump is already durable and the manifests are still at
    // `gen - 1`, so `compute_invalid_views` rebuilds them from the base tables on
    // the next boot. Aborting would be equally safe, but this path also runs on
    // the Shutdown barrier, so it would turn any tick error during the final
    // drain into `_exit(134)`.
    if let Err(e) = drained {
        gnitz_warn!("checkpoint drain failed, skipping the ephemeral round: {}", e);
        return;
    }

    // Quiesce so no tick runs during the ephemeral flush. The park guard lives to
    // the end of this function, so the tick loop resumes only past the round.
    let _park = await_servicing(request_quiesce(shared), rx, batch, shared).await;

    // Step 3 — EPHEMERAL ROUND. Stamp the step-0 generation (no re-bump happens
    // mid-drain, so no re-read is needed).
    flush_round(shared, Some(gen)).await;
}

/// Await `target_rx` (a Drain `done` or a Quiesce ack) while keeping the
/// committer responsive: a reclaim barrier is serviced by a reclaim-only base
/// round (so a draining tick's relay gets SAL space), DDL/Shutdown barriers are
/// deferred to sequence end, and pushes are held for the folded commit. The
/// target is re-polled after each serviced request, so no wakeup is lost.
///
/// Returns the target's payload. The Drain target carries the tick's verdict,
/// which decides whether the sequence's ephemeral round may run.
async fn await_servicing<T>(
    target_rx: oneshot::Receiver<T>,
    rx: &mut chan::Receiver<CommitRequest>,
    batch: &mut PendingBatch,
    shared: &Rc<Shared>,
) -> T {
    // `oneshot::Receiver` is `Unpin`, so `&mut target` is itself a Future.
    let mut target = target_rx;
    loop {
        match select2(&mut target, rx.recv()).await {
            // Target fired: drain done, or the quiesce ack carrying its park token.
            Either::A(v) => return v,
            Either::B(CommitRequest::Barrier {
                kind: BarrierKind::Reclaim { forced },
                done,
            }) => {
                // Reclaim-only base round: no gen re-bump, no re-staleing of view
                // manifests. Gated on the margin test alone — a reclaim barrier
                // asks for space, not for a checkpoint — because step 1 already
                // reset the SAL and the watchdog fires these on a timer, so an
                // ungated round would cost a full broadcast for nothing.
                if forced || shared.disp().sal_space_low() {
                    flush_round(shared, None).await;
                }
                done.send(());
            }
            Either::B(CommitRequest::Barrier { kind, done }) => batch.barriers.push((kind, done)),
            Either::B(CommitRequest::Push(p)) => batch.pushes.push(p),
            Either::B(CommitRequest::Txn(txn)) => batch.txns.push(txn),
        }
    }
}

/// One single-tid SAL group: a merged run of single pushes, or one transaction
/// family.
struct GroupInfo {
    tid: i64,
    /// See `CommitRequest::Push::recoverable`. A merged run is homogeneous in
    /// `tid`, so one flag per group is exact.
    recoverable: bool,
    req_ids: Lease,
    merged: Batch,
    write_err: Option<WireFault>,
}

/// One client-visible commit unit: the SAL groups it emits and the clients
/// waiting on their shared verdict.
struct CommitUnit {
    /// One group for a merged single-push run; one per family, in frame order,
    /// for a transaction.
    groups: Vec<GroupInfo>,
    outcome: Outcome,
}

/// Who is owed this unit's verdict, and therefore what a worker-ACK error may do
/// to it.
enum Outcome {
    /// The coalesced clients of one merged run. A push degrades gracefully, so a
    /// worker error downgrades that group's verdict.
    Pushes(Vec<oneshot::Sender<Result<u64, WireFault>>>),
    /// One transaction. Its contract is "Err ⇒ nothing committed", so once the
    /// sentinel is durable a worker error must never turn `Ok` into `Err`.
    /// `commit_pushes` aborts instead — the DDL tick quiesce and the catalog read
    /// lock held through the ACK are what make that arm unreachable, and it
    /// fail-stops rather than diverge if they ever do not.
    Txn(oneshot::Sender<Result<u64, WireFault>>),
}

impl CommitUnit {
    /// This unit's groups with no error against them: what the SAL admitted,
    /// and past Phase C what every worker also took. The one spelling: a group
    /// awaited that no worker was sent would park forever.
    fn live(&self) -> impl Iterator<Item = &GroupInfo> {
        self.groups.iter().filter(|g| g.write_err.is_none())
    }

    fn live_mut(&mut self) -> impl Iterator<Item = &mut GroupInfo> {
        self.groups.iter_mut().filter(|g| g.write_err.is_none())
    }

    /// Resolve every `done` of this unit exactly once: `Ok(zone_lsn)` iff all of
    /// its groups committed, else the first group error.
    fn resolve(self, zone_lsn: u64) {
        let result = match self.groups.iter().find_map(|g| g.write_err.clone()) {
            Some(e) => Err(e),
            None => Ok(zone_lsn),
        };
        match self.outcome {
            Outcome::Pushes(dones) => {
                for done in dones {
                    done.send(result.clone());
                }
            }
            Outcome::Txn(done) => done.send(result),
        }
    }
}

/// Commit one batch of pushes. Emits every group's SAL writes under
/// `sal_writer_excl` (alongside the signal + fsync SQE submit), releases the
/// lock, THEN awaits worker ACKs (Phase C) and the fsync CQE (Phase D). LSN
/// assignment and `done.send` happen after worker ACKs; the unique-index filter
/// update happens after fsync.
async fn commit_pushes(shared: &Rc<Shared>, mut pushes: Vec<PendingPush>, txns: Vec<PendingTxn>) {
    // Sort by tid so runs are homogeneous. Stable: arrival order within a run is
    // what makes intra-batch last-insert-wins mean last *inserted*.
    pushes.sort_by_key(|p| p.tid);

    let nw = shared.disp().num_workers();
    let mut units: Vec<CommitUnit> = Vec::with_capacity(txns.len());
    let alloc_req_ids = || shared.reactor.lease_acks(nw);

    // ------------------------------------------------------------------
    // Phase A (no lock): build merged batches + req_id allocations.
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
                    req_ids: alloc_req_ids(),
                    merged: fam.batch,
                    write_err: None,
                })
                .collect(),
            outcome: Outcome::Txn(done),
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

        // A single push ships the client's batch as it stands. A coalesced run
        // merges into one batch sized for the whole run in rows *and* blob bytes:
        // `append_batch` otherwise regrows the heap once per source. The merge
        // reads client-supplied batches, so it is guarded.
        let merged = if tail.is_empty() {
            head
        } else {
            let total_rows = head.len() + tail.iter().map(|b| b.len()).sum::<usize>();
            let total_blob = head.blob.len() + tail.iter().map(|b| b.blob.len()).sum::<usize>();
            match guard_panic("commit_merge", || {
                let mut m = Batch::with_capacity_blob(&shared.disp().schema_desc_for(tid), total_rows, total_blob);
                m.append_batch(&head, 0, head.len());
                for pb in &tail {
                    m.append_batch(pb, 0, pb.len());
                }
                Ok(m)
            }) {
                Ok(m) => m,
                Err(panic_msg) => {
                    // Phase A runs before the lock and before any SAL write: nothing
                    // was written and no worker was told anything, so answer the
                    // coalesced clients and emit no group.
                    let e = WireFault::from(panic_msg);
                    for done in dones {
                        done.send(Err(e.clone()));
                    }
                    continue;
                }
            }
        };

        units.push(CommitUnit {
            groups: vec![GroupInfo {
                tid,
                recoverable,
                req_ids: alloc_req_ids(),
                merged,
                write_err: None,
            }],
            outcome: Outcome::Pushes(dones),
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

        // Floor 0: a user-table push pins no system-family counter.
        let zone_lsn = shared.lsn_alloc.reserve(0);

        // Nothing laid out inside the scope is visible until it commits, so a
        // transaction that runs out of SAL space part-way can take its earlier
        // families back. Every write, the sentinel and the fsync submit are in
        // this one synchronous block, so no reader ever observes the gap.
        let scope = shared.disp().begin(zone_lsn, "commit");

        // Emit every unit into the zone, in unit order (transactions first). The
        // scope opens the zone on the first recoverable group it admits.
        //
        // A unit is all-or-nothing: a family that does not fit rolls the bundle
        // back to where it started, and the families before it were never
        // published. A single-push unit has one group, so that same rule is what
        // "a refused push degrades gracefully" means for it.
        for unit in &mut units {
            let savepoint = scope.savepoint();
            let failure = unit.groups.iter().find_map(|g| lay_out_group(shared, &scope, g).err());
            if let Some(e) = failure {
                scope.roll_back(savepoint);
                for g in &mut unit.groups {
                    g.write_err = Some(e.clone());
                }
            }
        }

        // Publishes every laid-out group, then closes the zone. A failure here
        // is fatal: the alternative is telling a client `Ok` about a zone
        // recovery will silently drop.
        let closed = scope
            .commit()
            .unwrap_or_else(|e| gnitz_fatal_abort!("commit zone failed, durability lost: {}", e.text));
        // The wake goes out whatever was written; Phase C awaits the ACKs.
        shared.disp().signal_all();
        // A stream-only batch — or one every group of which was refused — opened
        // no zone: nothing to sync.
        let fsync_fut = closed.then(|| shared.reactor.fsync(shared.disp().sal_fd()));
        (zone_lsn, fsync_fut)
    };

    // ------------------------------------------------------------------
    // Phase C (no lock): await push ACKs, per live group in unit order, then
    // fire tick. Replies for later groups wait in their routes meanwhile.
    // fsync is awaited separately in Phase D so DAG evaluation overlaps
    // with fdatasync (~5 ms gap eliminated). LSN publish is deferred to
    // after fsync so clients only see a durable LSN.
    // unique_filter_ingest_batch is NOT called here: a filter entry for rows a
    // crash would discard makes the next INSERT of the same key fail a
    // uniqueness check nothing durable backs. It runs after Phase D's fsync.
    // ------------------------------------------------------------------
    {
        for unit in &mut units {
            let downgrade = matches!(unit.outcome, Outcome::Pushes(_));
            for g in unit.live_mut() {
                let Err(e) = g.req_ids.acks(nw, |w, c| worker_error(w, "commit", c)).await else {
                    continue;
                };
                if downgrade {
                    // Only a *worker* error invalidates: it means this worker took
                    // none of a group the others did, so the filter may now
                    // disagree with the cluster. A SAL-refused group reached no
                    // worker at all.
                    g.write_err = Some(e);
                    shared.disp().unique_filter_invalidate_table(g.tid);
                } else {
                    // Discarding it would leave the transaction durable in the SAL
                    // but missing from this worker's partition, with the client
                    // told Ok — silent divergence.
                    gnitz_fatal_abort!("worker rejected a committed transaction group (tid={}): {}", g.tid, e);
                }
            }
        }
        // Bump tick counters and maybe fire the auto-tick BEFORE awaiting
        // the fsync CQE so DAG evaluation overlaps with fdatasync. We
        // bump on writes that succeeded at the worker level — the LSN publish
        // is deferred but tick batching can proceed.
        //
        // CONTRACT for `read_is_fresh`: this mark must precede the Phase-D
        // `publish`, or a tick whose snapshot already reached that LSN can miss
        // the tid and the freshness test serves the view without its delta.
        shared.note_commit_rows(units.iter().flat_map(|u| u.live()).map(|g| (g.tid, g.merged.len())));
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
        if units.iter().flat_map(|u| u.live()).next().is_some() {
            shared.lsn_alloc.publish(zone_lsn);
        }
    }

    // Update unique-index filters now that fsync confirms durability.
    // Wrapped for task liveness: a panic here must not fail the commit —
    // the data is already durable. Invalidate on panic so the next
    // constrained INSERT re-validates from scratch.
    for g in units.iter().flat_map(|u| u.live()) {
        if let Err(e) = guard_panic("unique_filter_ingest", || {
            shared.disp().unique_filter_ingest_batch(g.tid, &g.merged);
            Ok(())
        }) {
            shared.disp().unique_filter_invalidate_table(g.tid);
            gnitz_warn!("{}", e);
        }
    }

    // Send responses. A single-push unit resolves each coalesced client's `done`
    // from its one group; a transaction's single `done` resolves `Ok` iff every
    // family group committed.
    for unit in units {
        unit.resolve(zone_lsn);
    }
}

/// Lay one group out in `scope`, inside its zone when the group is
/// `recoverable`. `guard_panic` covers the encode of a client-supplied batch;
/// the SAL's own refusal rides the `Ok` side, where it keeps its
/// `STATUS_SAL_FULL` instead of flattening to the guard's `String`.
fn lay_out_group(shared: &Rc<Shared>, scope: &SalScope, g: &GroupInfo) -> Result<(), WireFault> {
    guard_panic("commit_write", || {
        Ok(shared
            .disp()
            .write_commit_group(scope, g.tid, &g.merged, g.req_ids.base(), g.recoverable)
            .err())
    })?
    .map_or(Ok(()), Err)
}

#[cfg(test)]
#[path = "tests/committer.rs"]
mod tests;
