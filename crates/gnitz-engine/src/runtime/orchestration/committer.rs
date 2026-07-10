//! Group commit task.
//!
//! Owns the `sal.write_ingest + fdatasync + per-worker push ACK`
//! sequence for every user-table INSERT/UPSERT. Receives commit
//! requests via `mpsc` and batches them with a debounce timer.
//!
//! Design notes:
//!
//! - **One committer per master process.**  Single-threaded: everything
//!   lives on the reactor. Borrows `*mut MasterDispatcher` to call into
//!   the dispatcher's SAL writer without an exclusive `&mut Self` borrow
//!   across `.await`.
//! - **Batching.**  Pipelined clients batch naturally: after `rx.recv()`
//!   returns the first request, `try_recv` drains anything already
//!   queued (capped at `MAX_PENDING_ROWS`). There is no debounce timer —
//!   a timer would add tail latency to every commit to help only serial
//!   single-request workloads. See `debounce_drain`.
//! - **Checkpoint.**  If `sal.needs_checkpoint()` (or a Shutdown barrier
//!   forces it) the committer runs the full three-step sequence
//!   (`run_checkpoint_sequence`: gen bump → base round → drain/quiesce →
//!   ephemeral round), then proceeds with the push batch.
//! - **Barrier.**  A `Barrier` request flushes any in-flight batch and
//!   signals via a oneshot — used by DDL to drain the committer before
//!   catalog mutation, by `relay_loop` to reclaim SAL space, and by the
//!   graceful-shutdown watchdog (see `BarrierKind`).

use super::executor::{TickTrigger, TICK_COALESCE_ROWS};
use super::guard_panic;
use crate::runtime::lsn::ZoneLsnAllocator;
use crate::runtime::master::{first_worker_error_opt, MasterDispatcher};
use crate::runtime::reactor::{join_into, mpsc, oneshot, select2, AsyncMutex, Either, Reactor, ReplyFuture};
use crate::runtime::sal::{FLAG_FLUSH, FLAG_FLUSH_EPH};
use crate::runtime::wire::{DecodedWire, WireConflictMode};
use crate::storage::Batch;
use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::rc::Rc;

const MAX_PENDING_ROWS: usize = 100_000;

/// One request to the committer.
#[allow(clippy::large_enum_variant)]
pub enum CommitRequest {
    /// Buffer the batch for group commit. On completion, `done` resolves
    /// to `Ok(lsn)` or `Err(error_message)`.
    Push {
        tid: i64,
        batch: Batch,
        mode: WireConflictMode,
        done: oneshot::Sender<Result<u64, String>>,
    },
    /// Drain any in-flight batch and signal via `done`. `kind` decides how
    /// the barrier interacts with a checkpoint sequence (see `BarrierKind`).
    Barrier {
        kind: BarrierKind,
        done: oneshot::Sender<()>,
    },
}

/// Who issued a `CommitRequest::Barrier`, and therefore how the committer
/// services it while a checkpoint sequence is in flight.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BarrierKind {
    /// Relay-space barrier from `relay_loop`: serviced mid-sequence by a
    /// reclaim-only base round (no gen re-bump) and signaled right after
    /// step 1, so `relay_loop` unparks before the drain and can refill space
    /// during it.
    Reclaim,
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

/// Pending entry within the committer's current batch.
///
/// `batch` is an `Option` so the single-push commit fast path can move
/// ownership into `GroupInfo.merged` via `Option::take` without copying
/// (see Phase A below). After that, the slot is `None` and only metadata
/// (`tid`, `mode`, `done`) remains addressable.
struct PendingPush {
    tid: i64,
    batch: Option<Batch>,
    mode: WireConflictMode,
    done: oneshot::Sender<Result<u64, String>>,
}

/// Shared state between the committer task and the executor.
pub struct Shared {
    pub reactor: Rc<Reactor>,
    pub disp_ptr: *mut MasterDispatcher,
    pub sal_fd: i32,
    /// SAL-writer exclusivity (III.3b). The committer holds this for
    /// the entire checkpoint + commit emission window so a concurrent
    /// tick task or DDL broadcast cannot interleave a SAL group.
    pub sal_writer_excl: Rc<AsyncMutex<()>>,
    /// Zone-LSN allocation high-water + durability watermark, shared with the
    /// executor so SCAN/SEEK responses report the same LSN commits publish.
    pub lsn_alloc: Rc<ZoneLsnAllocator>,
    pub num_workers: usize,
    pub tick_rows: Rc<RefCell<FxHashMap<i64, usize>>>,
    pub tick_tids: Rc<RefCell<Vec<i64>>>,
    /// Tick-trigger sender: fires the auto-tick after large commits, and drives
    /// the checkpoint sequence's drain (`Drain`) and quiesce (`Quiesce`)
    /// between its base and ephemeral rounds.
    pub tick_tx: mpsc::Sender<TickTrigger>,
}

impl Shared {
    #[allow(clippy::mut_from_ref)]
    fn disp(&self) -> &mut MasterDispatcher {
        unsafe { &mut *self.disp_ptr }
    }
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
pub async fn run(mut rx: mpsc::Receiver<CommitRequest>, shared: Rc<Shared>) {
    // Reused across every commit/checkpoint cycle to avoid the per-iteration
    // Vec<ReplyFuture> + Vec<Option<DecodedWire>> allocations that the old
    // join_all path implied. Sized for one group's ACKs; commit_pushes grows
    // them on the first multi-group batch and reuses the capacity thereafter.
    let mut fut_slots: Vec<ReplyFuture> = Vec::with_capacity(shared.num_workers);
    let mut ack_slots: Vec<Option<DecodedWire>> = Vec::with_capacity(shared.num_workers);
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
        let (pushes, barriers) = debounce_drain(&mut rx, first);

        // Checkpoint decision for the whole batch, barrier-only batches
        // included: relay_loop's low-space barrier arrives precisely to
        // reclaim SAL space. Barrier batches also honor the relay-space
        // threshold directly, covering GNITZ_CHECKPOINT_BYTES configs above
        // it. A Shutdown barrier forces the full sequence regardless of SAL
        // fullness. Must complete fully before commit groups go out: workers
        // bump their expected_epoch on FLAG_FLUSH, so commit groups written
        // AFTER FLAG_FLUSH in the same epoch would be silently skipped by
        // workers.
        let has_barriers = !barriers.is_empty();
        let forced = barriers.iter().any(|(k, _)| *k == BarrierKind::Shutdown);
        let checkpoint =
            forced || shared.disp().sal_needs_checkpoint() || (has_barriers && !shared.disp().sal_has_relay_space());

        let (pushes, barriers) = if checkpoint {
            // The full three-step sequence: gen bump → base round → drain →
            // ephemeral round. It signals the reclaim barriers right after
            // step 1 (so relay_loop unparks before the drain), defers the
            // DDL/Shutdown barriers to the end, and holds pushes through the
            // sequence — folding any that arrive mid-drain into the returned
            // batch.
            run_checkpoint_sequence(&mut rx, &shared, &mut fut_slots, &mut ack_slots, barriers, pushes).await
        } else {
            (pushes, barriers)
        };

        if !pushes.is_empty() {
            commit_pushes(&shared, pushes, &mut fut_slots, &mut ack_slots, &mut merge_pool).await;
        }

        if has_barriers {
            merge_pool.clear();
        }
        for (_, b) in barriers {
            let _ = b.send(());
        }
    }
}

/// One debounced committer batch: the pushes to group-commit plus the barrier
/// senders to signal once the batch (and any checkpoint sequence) completes.
type PendingBatch = (Vec<PendingPush>, Vec<(BarrierKind, oneshot::Sender<()>)>);

/// Sort `first` into `pushes`/`barriers`, then drain additional requests
/// without waiting: if the channel has items ready, pull them all;
/// otherwise return immediately. The 1ms debounce timer is a worst-case
/// cost paid only by serial single-request clients, so we skip it
/// entirely and rely on pipelined clients to enqueue fast enough that
/// `try_recv` sees a non-empty queue.
fn debounce_drain(rx: &mut mpsc::Receiver<CommitRequest>, first: CommitRequest) -> PendingBatch {
    let mut pushes = Vec::new();
    let mut barriers = Vec::new();
    let mut row_count: usize = 0;
    match first {
        CommitRequest::Push { tid, batch, mode, done } => {
            row_count = batch.count;
            pushes.push(PendingPush {
                tid,
                batch: Some(batch),
                mode,
                done,
            });
        }
        CommitRequest::Barrier { kind, done } => barriers.push((kind, done)),
    }
    while row_count < MAX_PENDING_ROWS {
        match rx.try_recv() {
            Some(CommitRequest::Push { tid, batch, mode, done }) => {
                row_count += batch.count;
                pushes.push(PendingPush {
                    tid,
                    batch: Some(batch),
                    mode,
                    done,
                });
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
    (pushes, barriers)
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
/// `ephemeral_gen: None` — base/reclaim round: `FLAG_FLUSH` (lsn 0, unused),
/// then the full `checkpoint_post_ack` (system-table flush +
/// gated-deletion drain + reset). `Some(gen)` — ephemeral round:
/// `FLAG_FLUSH_EPH` carrying the generation in the group header's `lsn` field
/// (workers read it to stamp view manifests), then a bare reset — only command
/// groups sit in the SAL since the base round's reset, and gated deletions
/// already drained there.
async fn flush_round(
    shared: &Rc<Shared>,
    ephemeral_gen: Option<u64>,
    fut_slots: &mut Vec<ReplyFuture>,
    ack_slots: &mut Vec<Option<DecodedWire>>,
) -> Result<(), String> {
    let nw = shared.num_workers;
    let req_ids: Vec<u64> = (0..nw).map(|_| shared.reactor.alloc_request_id()).collect();

    let _sal_excl = shared.sal_writer_excl.lock().await;

    {
        let disp = shared.disp();
        // FlushEph's lsn IS the checkpoint generation — workers latch it via
        // `set_committed_generation`. The base round's lsn is unread; pass 0.
        let (lsn, flags) = match ephemeral_gen {
            Some(gen) => (gen, FLAG_FLUSH_EPH),
            None => (0, FLAG_FLUSH),
        };
        disp.write_checkpoint_group(lsn, flags, &req_ids)?;
        disp.signal_all();
    }

    fut_slots.clear();
    fut_slots.extend(req_ids.iter().map(|&id| shared.reactor.await_reply(id)));
    join_into(fut_slots, ack_slots).await;
    let err = first_worker_error_opt("checkpoint", ack_slots);
    ack_slots.clear();
    if let Some(e) = err {
        return Err(e);
    }
    match ephemeral_gen {
        None => guard_panic("checkpoint_post_ack", || shared.disp().checkpoint_post_ack()),
        Some(_) => {
            shared.disp().checkpoint_reset_only();
            Ok(())
        }
    }
}

/// The full steady-state checkpoint sequence: gen bump → base round → drain →
/// quiesce → ephemeral round. Signals the reclaim barriers right after step 1
/// and returns the held pushes (folded with any that arrived mid-drain) plus
/// the deferred DDL/Shutdown barriers for the caller to commit and signal.
async fn run_checkpoint_sequence(
    rx: &mut mpsc::Receiver<CommitRequest>,
    shared: &Rc<Shared>,
    fut_slots: &mut Vec<ReplyFuture>,
    ack_slots: &mut Vec<Option<DecodedWire>>,
    barriers: Vec<(BarrierKind, oneshot::Sender<()>)>,
    mut held_pushes: Vec<PendingPush>,
) -> PendingBatch {
    // Step 0: gen bump. From this instant every existing rederived manifest is
    // stale; a crash below rebuilds views instead of silently staleifying them.
    let gen = shared.disp().bump_checkpoint_generation();

    // Step 1: base round. A failure is unrecoverable in-process (workers
    // already bumped their read epoch on FLAG_FLUSH but the master did not
    // reset the SAL, so the cluster is epoch-desynced). Abort so restart
    // replays the un-reset SAL and re-derives views from the durable base
    // tables.
    if let Err(e) = flush_round(shared, None, fut_slots, ack_slots).await {
        crate::gnitz_fatal_abort!("checkpoint failed, cluster epoch-desynced: {}", e);
    }

    // CRITICAL deadlock fix: unpark relay_loop NOW — step-1's reset already
    // reclaimed space, so the triggering reclaim barriers can be released before
    // the drain (the drain's own relays refill space, and relay_loop must be live
    // to service them). DDL/Shutdown barriers stay deferred to sequence end.
    let (reclaim, mut deferred): (Vec<_>, Vec<_>) = barriers.into_iter().partition(|(k, _)| *k == BarrierKind::Reclaim);
    for (_, b) in reclaim {
        let _ = b.send(());
    }

    // Step 2 — DRAIN (lock released). One Drain suffices: the tick loop walks the
    // source's full dependent closure with inline exchange rounds, and pushes are
    // held so `tick_tids` cannot grow. Mirrors the SCAN drain.
    let tids = shared.tick_tids.borrow().clone();
    let (done_tx, done_rx) = oneshot::channel();
    shared.tick_tx.send(TickTrigger::Drain { tids, done: done_tx });
    await_servicing(
        done_rx,
        rx,
        &mut held_pushes,
        &mut deferred,
        shared,
        fut_slots,
        ack_slots,
    )
    .await;

    // Quiesce so no tick runs during the ephemeral flush.
    let (acked_tx, acked_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    shared.tick_tx.send(TickTrigger::Quiesce {
        acked: acked_tx,
        release: release_rx,
    });
    await_servicing(
        acked_rx,
        rx,
        &mut held_pushes,
        &mut deferred,
        shared,
        fut_slots,
        ack_slots,
    )
    .await;

    // Step 3 — EPHEMERAL ROUND. Stamp the step-0 generation (no re-bump happens
    // mid-drain, so no re-read is needed).
    if let Err(e) = flush_round(shared, Some(gen), fut_slots, ack_slots).await {
        crate::gnitz_fatal_abort!("ephemeral checkpoint round failed, cluster epoch-desynced: {}", e);
    }
    let _ = release_tx.send(()); // resume the tick loop

    (held_pushes, deferred)
}

/// Await `target_rx` (a Drain `done` or Quiesce `acked`) while keeping the
/// committer responsive: a reclaim barrier is serviced by a reclaim-only base
/// round (so relay_loop refills SAL space during the drain), DDL/Shutdown
/// barriers are deferred to sequence end, and pushes are held for the folded
/// commit. The target is re-polled after each serviced request, so no wakeup
/// is lost.
async fn await_servicing(
    target_rx: oneshot::Receiver<()>,
    rx: &mut mpsc::Receiver<CommitRequest>,
    held_pushes: &mut Vec<PendingPush>,
    deferred: &mut Vec<(BarrierKind, oneshot::Sender<()>)>,
    shared: &Rc<Shared>,
    fut_slots: &mut Vec<ReplyFuture>,
    ack_slots: &mut Vec<Option<DecodedWire>>,
) {
    // `oneshot::Receiver` is `Unpin`, so `&mut target` is itself a Future.
    let mut target = target_rx;
    loop {
        match select2(&mut target, rx.recv()).await {
            // Target fired (drain done / quiesce acked), or the channel closed.
            Either::A(_) => return,
            Either::B(None) => return,
            Either::B(Some(CommitRequest::Barrier {
                kind: BarrierKind::Reclaim,
                done,
            })) => {
                // Reclaim-only base round: no gen re-bump, no re-staleing of
                // view manifests.
                if let Err(e) = flush_round(shared, None, fut_slots, ack_slots).await {
                    crate::gnitz_fatal_abort!("reclaim checkpoint failed, cluster epoch-desynced: {}", e);
                }
                let _ = done.send(());
            }
            Either::B(Some(CommitRequest::Barrier { kind, done })) => deferred.push((kind, done)),
            Either::B(Some(CommitRequest::Push { tid, batch, mode, done })) => {
                held_pushes.push(PendingPush {
                    tid,
                    batch: Some(batch),
                    mode,
                    done,
                });
            }
        }
    }
}

/// Commit one debounced batch of pushes. Emits every group's SAL writes
/// under `sal_writer_excl` (alongside the signal + fsync SQE submit),
/// releases the lock, THEN awaits worker ACKs (Phase C) and the fsync
/// CQE (Phase D). LSN assignment and `done.send` happen after worker
/// ACKs; unique-index filter update happens after fsync.
async fn commit_pushes(
    shared: &Rc<Shared>,
    mut pushes: Vec<PendingPush>,
    fut_slots: &mut Vec<ReplyFuture>,
    ack_slots: &mut Vec<Option<DecodedWire>>,
    merge_pool: &mut FxHashMap<(i64, u8), Batch>,
) {
    // Sort by (tid, mode) so runs are homogeneous.
    pushes.sort_by_key(|p| (p.tid, p.mode.as_u8()));

    /// One homogeneous (tid, mode) run of `pushes`. Groups tile `pushes`
    /// contiguously in order, `len` pushes each.
    struct GroupInfo {
        len: usize,
        tid: i64,
        mode: WireConflictMode,
        req_ids: Vec<u64>,
        merged: Option<Batch>,
        write_err: Option<String>,
    }

    let nw = shared.num_workers;
    let n = pushes.len();
    let mut groups: Vec<GroupInfo> = Vec::new();

    // ------------------------------------------------------------------
    // Phase A (no lock): build merged batches + req_id allocations.
    // ------------------------------------------------------------------
    {
        let mut run_start = 0;
        while run_start < n {
            let tid = pushes[run_start].tid;
            let mode = pushes[run_start].mode;
            let mut run_end = run_start + 1;
            while run_end < n && pushes[run_end].tid == tid && pushes[run_end].mode == mode {
                run_end += 1;
            }

            let total_rows: usize = pushes[run_start..run_end]
                .iter()
                .map(|p| p.batch.as_ref().map(|b| b.count).unwrap_or(0))
                .sum();
            let req_ids: Vec<u64> = (0..nw).map(|_| shared.reactor.alloc_request_id()).collect();
            let single_push = run_end - run_start == 1;

            // Single-push: take ownership of the client's batch (no merge).
            // Multi-push: pop a pooled merged batch (or alloc fresh) and
            // refill it. DDL between bursts can change the schema, so
            // mismatched pool entries are dropped and reallocated — without
            // this, append_batch writes rows under the wrong column layout.
            let merged = match guard_panic("commit_merge", || {
                if single_push {
                    Ok(pushes[run_start].batch.take().expect("PendingPush.batch already taken"))
                } else {
                    let schema = shared.disp().schema_desc_for(tid);
                    let mut m = match merge_pool.remove(&(tid, mode.as_u8())) {
                        Some(pooled) if pooled.schema.as_ref() == Some(&schema) => pooled,
                        _ => Batch::with_schema(schema, total_rows.max(1)),
                    };
                    m.clear();
                    for p in pushes[run_start..run_end].iter() {
                        let pb = p
                            .batch
                            .as_ref()
                            .expect("PendingPush.batch is missing in multi-push run");
                        m.append_batch(pb, 0, pb.count);
                    }
                    Ok(m)
                }
            }) {
                Ok(m) => m,
                Err(panic_msg) => {
                    let placeholder = guard_panic("commit_fallback_schema", || {
                        Ok(Batch::empty_with_schema(&shared.disp().schema_desc_for(tid)))
                    })
                    .unwrap_or_else(|_| Batch::empty_with_schema(&crate::schema::SchemaDescriptor::minimal_u64()));
                    groups.push(GroupInfo {
                        len: run_end - run_start,
                        tid,
                        mode,
                        req_ids,
                        merged: Some(placeholder),
                        write_err: Some(panic_msg),
                    });
                    run_start = run_end;
                    continue;
                }
            };

            groups.push(GroupInfo {
                len: run_end - run_start,
                tid,
                mode,
                req_ids,
                merged: Some(merged),
                write_err: None,
            });
            run_start = run_end;
        }
    }

    // ------------------------------------------------------------------
    // Phase B (under lock): emit SAL groups, signal, submit fsync SQE.
    // Lock dropped immediately after; ACKs and fsync CQE are awaited
    // outside so tick/relay/DDL tasks can make progress.
    //
    // All groups in this batch share one zone_lsn. The commit sentinel
    // written after all groups lets recovery treat the batch atomically:
    // either every group applies or none do. The zone LSN is published once,
    // after fsync (Phase D), so clients only see durable LSNs.
    // ------------------------------------------------------------------
    let (zone_lsn, fsync_fut) = {
        let _sal_excl = shared.sal_writer_excl.lock().await;

        // Reserve under sal_writer_excl so reservation order == SAL write order
        // across every durable allocator (this committer, DDL, SERIAL). A
        // user-table push pins no system-family counter, so the floor is 0.
        let zone_lsn = shared.lsn_alloc.reserve(0);

        for g in groups.iter_mut() {
            if g.write_err.is_some() {
                continue;
            }
            let merged = g.merged.as_ref().expect("merged set in Phase A");
            let err = guard_panic("commit_write", || {
                Ok(shared
                    .disp()
                    .write_commit_group(g.tid, zone_lsn, merged, g.mode, &g.req_ids)
                    .err())
            })
            .unwrap_or_else(Some);
            g.write_err = err;
        }

        let any_written = groups.iter().any(|g| g.write_err.is_none());
        if !any_written {
            let mut pushes = pushes.into_iter();
            for g in &groups {
                let e = g.write_err.as_ref().unwrap();
                for p in pushes.by_ref().take(g.len) {
                    let _ = p.done.send(Err(e.clone()));
                }
            }
            return;
        }

        // Crash-injection seam: abort after push groups but BEFORE the
        // commit sentinel (GNITZ_INJECT_PUSH_ABORT=after_groups). SAL
        // recovery skips any zone whose sentinel is absent; workers that
        // already flushed their shard files before the crash retain the
        // data via the shard path, so this seam is useful for targeted
        // debugging rather than asserting invisibility after restart.
        #[cfg(debug_assertions)]
        if std::env::var("GNITZ_INJECT_PUSH_ABORT").as_deref() == Ok("after_groups") {
            unsafe {
                libc::abort();
            }
        }

        // Close the zone with the commit sentinel before fsync.  If this
        // fails the zone has no FLAG_TXN_COMMIT and recovery would silently
        // drop all groups in the zone — that is unrecoverable data loss
        // after a restart while the client already received Ok.  Abort.
        let commit_zone_err = shared.disp().commit_zone(zone_lsn).err();
        if let Some(e) = commit_zone_err {
            crate::gnitz_fatal_abort!("commit_zone failed, durability lost: {}", e);
        }
        shared.disp().signal_all();

        // Submit fsync SQE (synchronous — returns a future). The
        // ReplyFutures are built into `fut_slots` outside the lock scope:
        // await_reply only borrows reactor state, never the SAL writer,
        // so populating the slots after dropping the lock is correct
        // and lets concurrent tick/relay tasks make progress sooner.
        (zone_lsn, shared.reactor.fsync(shared.sal_fd))
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
    // unique_filter_ingest_batch is NOT called here — per the invariant in
    // async-invariants.md it must run only after fsync confirms durability.
    // ------------------------------------------------------------------
    {
        join_into(fut_slots, ack_slots).await;

        let mut cursor = 0usize;
        for g in groups.iter_mut() {
            if g.write_err.is_some() {
                continue;
            }
            if let Some(e) = first_worker_error_opt("commit", &ack_slots[cursor..cursor + nw]) {
                g.write_err = Some(e);
            }
            cursor += nw;
        }
        // Drop DecodedWire heap fields (data_batch, error_msg) before the
        // committer parks waiting for the next request. Capacity stays
        // resident so the next commit reuses it.
        ack_slots.clear();

        // Invalidate filters for groups whose worker ACK reported an error.
        // LSN publish + filter ingest are deferred to Phase D after fsync.
        for g in groups.iter_mut() {
            if g.write_err.is_some() {
                let _ = guard_panic("unique_filter_invalidate", || {
                    shared.disp().unique_filter_invalidate_table(g.tid);
                    Ok(())
                });
            }
        }

        // Bump tick counters and maybe fire the auto-tick BEFORE awaiting
        // the fsync CQE so DAG evaluation overlaps with fdatasync. We
        // bump tick_rows on writes that succeeded at the worker level —
        // the LSN publish is deferred but tick batching can proceed.
        {
            let mut tr = shared.tick_rows.borrow_mut();
            let mut tids = shared.tick_tids.borrow_mut();
            for g in &groups {
                if g.write_err.is_none() {
                    let entry = tr.entry(g.tid).or_insert(0);
                    if *entry == 0 {
                        tids.push(g.tid);
                    }
                    *entry += g.merged.as_ref().expect("merged set in Phase A").count;
                }
            }
        }
        if shared
            .tick_rows
            .borrow()
            .values()
            .any(|&rows| rows >= TICK_COALESCE_ROWS)
        {
            shared.tick_tx.send(TickTrigger::Auto);
        }
    }

    // ------------------------------------------------------------------
    // Phase D (no lock): await fsync CQE.  Client response is held until
    // after fsync so the client sees only durable data.
    // ------------------------------------------------------------------
    {
        let fsync_rc = fsync_fut.await;
        if fsync_rc < 0 {
            crate::gnitz_fatal_abort!("SAL fdatasync (committer) failed rc={}", fsync_rc);
        }
    }

    // Publish the zone LSN exactly once, after fsync confirms durability.
    // Pipelined pushes batched together share one zone_lsn, so clients
    // may see duplicate LSNs — only non-decreasing monotonicity is guaranteed.
    if groups.iter().any(|g| g.write_err.is_none()) {
        shared.lsn_alloc.publish(zone_lsn);
    }

    // Update unique-index filters now that fsync confirms durability.
    // Wrap per V.7: a panic in the filter update must not fail the commit —
    // the data is already durable. Invalidate on panic so the next
    // constrained INSERT re-validates from scratch.
    {
        for g in groups.iter_mut() {
            if g.write_err.is_some() {
                continue;
            }
            let merged = g.merged.as_ref().expect("merged set in Phase A");
            if let Err(e) = guard_panic("unique_filter_ingest", || {
                shared.disp().unique_filter_ingest_batch(g.tid, merged);
                Ok(())
            }) {
                let _ = guard_panic("unique_filter_invalidate", || {
                    shared.disp().unique_filter_invalidate_table(g.tid);
                    Ok(())
                });
                crate::gnitz_warn!("{}", e);
            }
        }
    }

    // Return merged batches to the (tid, mode) pool so the next burst
    // skips the strides_from_schema + zero-fill cost. Single-push batches
    // (originally the client's, taken via `Option::take`) are pooled too —
    // the schema-staleness guard above will discard them on a DDL change.
    for g in groups.iter_mut() {
        if let Some(b) = g.merged.take() {
            merge_pool.insert((g.tid, g.mode.as_u8()), b);
        }
    }

    // Now send responses in original order.
    let mut pushes = pushes.into_iter();
    for g in &groups {
        for p in pushes.by_ref().take(g.len) {
            let result = match &g.write_err {
                Some(e) => Err(e.clone()),
                None => Ok(zone_lsn),
            };
            let _ = p.done.send(result);
        }
    }
}
