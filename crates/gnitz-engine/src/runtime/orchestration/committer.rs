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
//! - **Checkpoint.**  If `sal.needs_checkpoint()` the committer emits a
//!   FLAG_FLUSH group with per-worker req_ids, awaits ACKs, runs the
//!   post-ACK bookkeeping (flush system tables + reset epoch), then
//!   proceeds with the push batch.
//! - **Barrier.**  A `Barrier` request flushes any in-flight batch and
//!   signals via a oneshot — used by DDL to drain the committer before
//!   catalog mutation.

use crate::foundation::posix_io::guard_panic;
use crate::runtime::master::{first_worker_error_opt, MasterDispatcher};
use crate::runtime::reactor::{join_into, mpsc, oneshot, AsyncMutex, Reactor, ReplyFuture};
use crate::runtime::wire::{DecodedWire, WireConflictMode};
use crate::storage::Batch;
use rustc_hash::FxHashMap;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Instant;

const MAX_PENDING_ROWS: usize = 100_000;
const TICK_COALESCE_ROWS: usize = 10_000;

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
    /// Drain any in-flight batch and signal via `done`. DDL uses this
    /// to ensure no push is mid-commit before applying catalog changes.
    Barrier { done: oneshot::Sender<()> },
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
    /// Monotonic LSN incremented on each successful commit; exposed to
    /// handlers via the `done` oneshot and shared with the executor so
    /// SCAN/SEEK responses report the same value.
    pub ingest_lsn: Rc<Cell<u64>>,
    pub num_workers: usize,
    pub tick_rows: Rc<RefCell<FxHashMap<i64, usize>>>,
    pub tick_tids: Rc<RefCell<Vec<i64>>>,
    pub fire_auto_tick: Rc<dyn Fn()>,
    pub t_last_push: Rc<Cell<Option<Instant>>>,
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
/// For checkpoint groups the lock is held across the ENTIRE sequence
/// (write + ACK wait + checkpoint_post_ack). This is required: workers
/// bump `expected_epoch` when they process FLAG_FLUSH, so any SAL group
/// written between the FLAG_FLUSH and `checkpoint_post_ack` carries the
/// old epoch and is silently skipped. `checkpoint_post_ack` then resets
/// `write_cursor` to 0, permanently orphaning those groups.
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

        let (pushes, barrier_senders) = start_batch(first);

        // Drain any additional requests already queued — no timer wait.
        // Pipelined clients still get batched; serial clients don't pay
        // a latency tax.
        let (pushes, barrier_senders) = debounce_drain(&mut rx, pushes, barrier_senders);

        // Checkpoint decision for the whole batch, barrier-only batches
        // included: relay_loop's low-space barrier arrives precisely to
        // reclaim SAL space. Barrier batches also honor the relay-space
        // threshold directly, covering GNITZ_CHECKPOINT_BYTES configs above
        // it. Must complete fully (emit + ACKs + post_ack) before commit
        // groups go out: workers bump their expected_epoch on FLAG_FLUSH,
        // so commit groups written AFTER FLAG_FLUSH in the same epoch
        // would be silently skipped by workers.
        let has_barriers = !barrier_senders.is_empty();
        if shared.disp().sal_needs_checkpoint() || (has_barriers && !shared.disp().sal_has_relay_space()) {
            if let Err(e) = run_checkpoint_phase(&shared, &mut fut_slots, &mut ack_slots).await {
                // Barrier semantics are "committer reached this point", not
                // "checkpoint succeeded": signal anyway. relay_loop rechecks
                // space and aborts if reclaim failed.
                crate::gnitz_warn!("checkpoint failed: {}", e);
                for p in pushes {
                    let _ = p.done.send(Err(e.clone()));
                }
                if has_barriers {
                    merge_pool.clear();
                }
                for b in barrier_senders {
                    let _ = b.send(());
                }
                continue;
            }
        }

        if !pushes.is_empty() {
            commit_pushes(&shared, pushes, &mut fut_slots, &mut ack_slots, &mut merge_pool).await;
        }

        if has_barriers {
            merge_pool.clear();
        }
        for b in barrier_senders {
            let _ = b.send(());
        }
    }
}

/// Sort one incoming request into either `pushes` or `barrier_senders`.
fn start_batch(req: CommitRequest) -> (Vec<PendingPush>, Vec<oneshot::Sender<()>>) {
    let mut pushes = Vec::new();
    let mut barriers = Vec::new();
    match req {
        CommitRequest::Push { tid, batch, mode, done } => {
            pushes.push(PendingPush {
                tid,
                batch: Some(batch),
                mode,
                done,
            });
        }
        CommitRequest::Barrier { done } => barriers.push(done),
    }
    (pushes, barriers)
}

/// Drain additional requests without waiting: if the channel has items
/// ready, pull them all; otherwise return immediately. The 1ms debounce
/// timer is a worst-case cost paid only by serial single-request
/// clients, so we skip it entirely and rely on pipelined clients to
/// enqueue fast enough that `try_recv` sees a non-empty queue.
fn debounce_drain(
    rx: &mut mpsc::Receiver<CommitRequest>,
    mut pushes: Vec<PendingPush>,
    mut barrier_senders: Vec<oneshot::Sender<()>>,
) -> (Vec<PendingPush>, Vec<oneshot::Sender<()>>) {
    let mut row_count: usize = pushes
        .iter()
        .map(|p| p.batch.as_ref().expect("batch present in debounce_drain").count)
        .sum();
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
            Some(CommitRequest::Barrier { done }) => {
                barrier_senders.push(done);
            }
            None => break,
        }
    }
    (pushes, barrier_senders)
}

/// Emit a FLAG_FLUSH checkpoint group and run the full post-ACK cleanup.
///
/// `sal_writer_excl` is held for the ENTIRE sequence: write + ACK wait +
/// `checkpoint_post_ack`. Releasing the lock before the await would let
/// concurrent tick/relay/DDL/fan-out tasks write SAL groups with the old
/// epoch. Workers bump `expected_epoch` when they process FLAG_FLUSH, so
/// those groups would be silently skipped. `checkpoint_post_ack` then
/// resets `write_cursor` to 0, permanently orphaning the groups and
/// leaving the writers waiting for ACKs that never arrive.
async fn run_checkpoint_phase(
    shared: &Rc<Shared>,
    fut_slots: &mut Vec<ReplyFuture>,
    ack_slots: &mut Vec<Option<DecodedWire>>,
) -> Result<(), String> {
    let nw = shared.num_workers;
    let req_ids: Vec<u64> = (0..nw).map(|_| shared.reactor.alloc_request_id()).collect();

    let _sal_excl = shared.sal_writer_excl.lock().await;

    {
        let disp = shared.disp();
        let lsn = disp.next_lsn();
        disp.write_checkpoint_group(lsn, &req_ids)?;
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
    guard_panic("checkpoint_post_ack", || shared.disp().checkpoint_post_ack())
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

    struct GroupInfo {
        start: usize,
        end: usize,
        tid: i64,
        mode_u8: u8,
        req_ids: Vec<u64>,
        merged: Option<Batch>,
        write_err: Option<String>,
        lsn: u64,
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
            let mode_u8 = mode.as_u8();
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
                    let mut m = match merge_pool.remove(&(tid, mode_u8)) {
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
                        start: run_start,
                        end: run_end,
                        tid,
                        mode_u8,
                        req_ids,
                        merged: Some(placeholder),
                        write_err: Some(panic_msg),
                        lsn: 0,
                    });
                    run_start = run_end;
                    continue;
                }
            };

            groups.push(GroupInfo {
                start: run_start,
                end: run_end,
                tid,
                mode_u8,
                req_ids,
                merged: Some(merged),
                write_err: None,
                lsn: 0,
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
    // either every group applies or none do. ingest_lsn is bumped once,
    // after fsync (Phase D), so clients only see durable LSNs.
    // ------------------------------------------------------------------
    let zone_lsn = shared.ingest_lsn.get() + 1;
    let fsync_fut = {
        let _sal_excl = shared.sal_writer_excl.lock().await;

        for g in groups.iter_mut() {
            if g.write_err.is_some() {
                continue;
            }
            let mode = pushes[g.start].mode;
            let merged = g.merged.as_ref().expect("merged set in Phase A");
            let err = guard_panic("commit_write", || {
                Ok(shared
                    .disp()
                    .write_commit_group(g.tid, zone_lsn, merged, mode, &g.req_ids)
                    .err())
            })
            .unwrap_or_else(Some);
            g.write_err = err;
            g.lsn = zone_lsn;
        }

        let any_written = groups.iter().any(|g| g.write_err.is_none());
        if !any_written {
            for g in &groups {
                let e = g.write_err.as_ref().unwrap();
                for p in pushes.drain(..g.end - g.start) {
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
        shared.reactor.fsync(shared.sal_fd)
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
        // ingest_lsn publish + filter ingest are deferred to Phase D after fsync.
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
        // ingest_lsn publish is deferred but tick batching can proceed.
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
            shared.t_last_push.set(Some(Instant::now()));
        }
        if shared
            .tick_rows
            .borrow()
            .values()
            .any(|&rows| rows >= TICK_COALESCE_ROWS)
        {
            (shared.fire_auto_tick)();
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
        shared.ingest_lsn.set(zone_lsn);
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
            merge_pool.insert((g.tid, g.mode_u8), b);
        }
    }

    // Now send responses in original order.
    for g in &groups {
        for p in pushes.drain(..g.end - g.start) {
            let result = match &g.write_err {
                Some(e) => Err(e.clone()),
                None => Ok(g.lsn),
            };
            let _ = p.done.send(result);
        }
    }
}
