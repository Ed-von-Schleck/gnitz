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

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Instant;
use rustc_hash::FxHashMap;
use crate::runtime::wire::WireConflictMode;
use crate::runtime::master::{MasterDispatcher, first_worker_error};
use crate::runtime::reactor::{self, AsyncMutex, Reactor, mpsc, oneshot};
use crate::storage::Batch;
use crate::util::guard_panic;

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
struct PendingPush {
    tid: i64,
    batch: Batch,
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
    fn disp(&self) -> &mut MasterDispatcher { unsafe { &mut *self.disp_ptr } }
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

        // Barrier-only tick: nothing to commit, just signal.
        if pushes.is_empty() {
            for b in barrier_senders { let _ = b.send(()); }
            continue;
        }

        // Phase 1 — checkpoint (if the SAL has crossed its threshold).
        // Must complete fully (emit + ACKs + post_ack) before commit
        // groups go out: workers bump their expected_epoch on FLAG_FLUSH,
        // so commit groups written AFTER FLAG_FLUSH in the same epoch
        // would be silently skipped by workers.
        if shared.disp().sal_needs_checkpoint() {
            if let Err(e) = run_checkpoint_phase(&shared).await {
                for p in pushes { let _ = p.done.send(Err(e.clone())); }
                for b in barrier_senders { let _ = b.send(()); }
                continue;
            }
        }

        // Phase 2 — commit the batched pushes.  Its own lock scope.
        commit_pushes(&shared, pushes).await;

        for b in barrier_senders { let _ = b.send(()); }
    }
}

/// Sort one incoming request into either `pushes` or `barrier_senders`.
fn start_batch(req: CommitRequest) -> (Vec<PendingPush>, Vec<oneshot::Sender<()>>) {
    let mut pushes = Vec::new();
    let mut barriers = Vec::new();
    match req {
        CommitRequest::Push { tid, batch, mode, done } => {
            pushes.push(PendingPush { tid, batch, mode, done });
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
    let mut row_count: usize = pushes.iter().map(|p| p.batch.count).sum();
    while row_count < MAX_PENDING_ROWS {
        match rx.try_recv() {
            Some(CommitRequest::Push { tid, batch, mode, done }) => {
                row_count += batch.count;
                pushes.push(PendingPush { tid, batch, mode, done });
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
async fn run_checkpoint_phase(shared: &Rc<Shared>) -> Result<(), String> {
    let nw = shared.num_workers;
    let req_ids: Vec<u64> = (0..nw).map(|_| shared.reactor.alloc_request_id()).collect();

    let _sal_excl = shared.sal_writer_excl.lock().await;

    let reply_futs = {
        let disp = shared.disp();
        let lsn = disp.next_lsn();
        disp.write_checkpoint_group(lsn, &req_ids)?;
        disp.signal_all();
        req_ids.iter().map(|&id| shared.reactor.await_reply(id)).collect::<Vec<_>>()
    };

    let decoded_vec = reactor::join_all(reply_futs).await;
    if let Some(e) = first_worker_error("checkpoint", &decoded_vec) {
        return Err(e);
    }
    guard_panic("checkpoint_post_ack", || {
        shared.disp().checkpoint_post_ack();
        Ok(())
    })
}

/// Commit one debounced batch of pushes. Emits every group's SAL writes
/// under `sal_writer_excl` (alongside the signal + fsync SQE submit),
/// releases the lock, THEN awaits worker ACKs (Phase C) and the fsync
/// CQE (Phase D). LSN assignment and `done.send` happen after worker
/// ACKs; unique-index filter update happens after fsync.
async fn commit_pushes(shared: &Rc<Shared>, mut pushes: Vec<PendingPush>) {
    // Sort by (tid, mode) so runs are homogeneous.
    pushes.sort_by_key(|p| (p.tid, p.mode.as_u8()));

    struct GroupInfo {
        start: usize,
        end: usize,
        tid: i64,
        req_ids: Vec<u64>,
        merged: Batch,
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
            let mut run_end = run_start + 1;
            while run_end < n && pushes[run_end].tid == tid && pushes[run_end].mode == mode {
                run_end += 1;
            }

            let total_rows: usize = pushes[run_start..run_end]
                .iter()
                .map(|p| p.batch.count)
                .sum();
            let req_ids: Vec<u64> = (0..nw).map(|_| shared.reactor.alloc_request_id()).collect();

            let merged = match guard_panic("commit_merge", || Ok({
                let schema = shared.disp().schema_desc_for(tid);
                let mut m = Batch::with_schema(schema, total_rows.max(1));
                for p in pushes[run_start..run_end].iter() {
                    m.append_batch(&p.batch, 0, p.batch.count);
                }
                m
            })) {
                Ok(m) => m,
                Err(panic_msg) => {
                    let placeholder = guard_panic("commit_fallback_schema", || Ok(
                        Batch::with_schema(shared.disp().schema_desc_for(tid), 1)
                    ))
                    .unwrap_or_else(|_| Batch::with_schema(
                        crate::schema::SchemaDescriptor::minimal_u64(), 1));
                    groups.push(GroupInfo {
                        start: run_start, end: run_end, tid, req_ids,
                        merged: placeholder,
                        write_err: Some(panic_msg), lsn: 0,
                    });
                    run_start = run_end;
                    continue;
                }
            };

            groups.push(GroupInfo {
                start: run_start, end: run_end, tid, req_ids, merged,
                write_err: None, lsn: 0,
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
    let (fsync_fut, reply_futs) = {
        let _sal_excl = shared.sal_writer_excl.lock().await;

        for g in groups.iter_mut() {
            if g.write_err.is_some() { continue; }
            let mode = pushes[g.start].mode;
            let err = guard_panic("commit_write", || Ok(
                shared.disp().write_commit_group(g.tid, zone_lsn, &g.merged, mode, &g.req_ids).err()
            )).unwrap_or_else(Some);
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
            unsafe { libc::abort(); }
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

        // Submit fsync SQE (synchronous — returns a future). Return the
        // awaitables; the .await happens after the lock is dropped.
        let fsync_fut = shared.reactor.fsync(shared.sal_fd);
        let all_req_ids: Vec<u64> = groups.iter()
            .filter(|g| g.write_err.is_none())
            .flat_map(|g| g.req_ids.iter().copied())
            .collect();
        let reply_futs: Vec<_> = all_req_ids.iter()
            .map(|&id| shared.reactor.await_reply(id))
            .collect();
        (fsync_fut, reply_futs)
    };

    // ------------------------------------------------------------------
    // Phase C (no lock): await push ACKs, fire tick.
    // fsync is awaited separately in Phase D so DAG evaluation overlaps
    // with fdatasync (~5 ms gap eliminated). LSN publish is deferred to
    // after fsync so clients only see a durable LSN.
    // unique_filter_ingest_batch is NOT called here — per the invariant in
    // async-invariants.md it must run only after fsync confirms durability.
    // ------------------------------------------------------------------
    {
        let decoded_vec = reactor::join_all(reply_futs).await;

        let mut cursor = 0usize;
        for g in groups.iter_mut() {
            if g.write_err.is_some() { continue; }
            if let Some(e) = first_worker_error("commit", &decoded_vec[cursor..cursor + nw]) {
                g.write_err = Some(e);
            }
            cursor += nw;
        }

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
                    if *entry == 0 { tids.push(g.tid); }
                    *entry += g.merged.count;
                }
            }
            shared.t_last_push.set(Some(Instant::now()));
        }
        if shared.tick_rows.borrow().values().any(|&rows| rows >= TICK_COALESCE_ROWS) {
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
            crate::gnitz_fatal_abort!(
                "SAL fdatasync (committer) failed rc={}", fsync_rc
            );
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
            if g.write_err.is_some() { continue; }
            if let Err(e) = guard_panic("unique_filter_ingest", || {
                shared.disp().unique_filter_ingest_batch(g.tid, &g.merged);
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
