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
//! - **Debounce.**  The first request starts a `FLUSH_DEADLINE` timer;
//!   subsequent requests join the batch. The loop flushes when the
//!   batch crosses `MAX_PENDING_ROWS` rows or the timer fires.
//! - **Checkpoint.**  If `sal.needs_checkpoint()` the committer emits a
//!   FLAG_FLUSH group with per-worker req_ids, awaits ACKs, runs the
//!   post-ACK bookkeeping (flush system tables + reset epoch), then
//!   proceeds with the push batch.
//! - **Barrier.**  A `Barrier` request flushes any in-flight batch and
//!   signals via a oneshot — used by DDL to drain the committer before
//!   catalog mutation.

use std::rc::Rc;
use crate::ipc::WireConflictMode;
use crate::master::{self, MasterDispatcher, first_worker_error};
use crate::reactor::{self, AsyncMutex, Reactor, mpsc, oneshot};
use crate::storage::Batch;
use crate::util::guard_panic;

const MAX_PENDING_ROWS: usize = 100_000;

/// One request to the committer.
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
    pub ingest_lsn: Rc<std::cell::Cell<u64>>,
    pub num_workers: usize,
}

/// The committer task loop. Returns when all senders drop (shutdown).
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

        // Hold SAL-writer exclusivity for the checkpoint + commit emission
        // window. Released before `done.send` so handlers that proceed on
        // the LSN can race with the next batch's emission.  Tick task,
        // relay task, and DDL all gate on the same mutex (III.3b).
        let _sal_excl = shared.sal_writer_excl.lock().await;

        // Checkpoint first (may flush system tables + reset SAL epoch).
        if unsafe { (*shared.disp_ptr).sal_needs_checkpoint() } {
            if let Err(e) = run_checkpoint(&shared).await {
                // Fail every push in the batch with the checkpoint error.
                for p in pushes { let _ = p.done.send(Err(e.clone())); }
                for b in barrier_senders { let _ = b.send(()); }
                continue;
            }
        }

        // Commit the batched pushes.
        commit_pushes(&shared, pushes).await;
        drop(_sal_excl);

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

/// Emit a FLAG_FLUSH checkpoint group, await ACKs, run post-ACK cleanup.
async fn run_checkpoint(shared: &Rc<Shared>) -> Result<(), String> {
    let disp_ptr = shared.disp_ptr;
    let decoded_vec = master::dispatch_fanout(disp_ptr, &shared.reactor, |disp, ids| {
        disp.write_checkpoint_group(ids)
    }).await?;
    if let Some(e) = first_worker_error("checkpoint", &decoded_vec) {
        return Err(e);
    }
    guard_panic("checkpoint_post_ack", || {
        unsafe { (*disp_ptr).checkpoint_post_ack(); }
        Ok(())
    })
}

/// Commit one debounced batch of pushes. Writes each group to SAL,
/// submits a single fdatasync + per-group ACK await, then responds
/// to each pending push with its LSN.
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

        // Per V.7 the committer task must never die. Wrap the SAL-mutating
        // closure (and the placeholder-schema fallback) so a panic on any
        // malformed-but-decoded input fails just this group.
        let disp_ptr = shared.disp_ptr;
        let (merged, write_err) = match guard_panic("commit", || Ok(unsafe {
            let disp = &mut *disp_ptr;
            let schema = disp.schema_desc_for(tid);
            let mut m = Batch::with_schema(schema, total_rows.max(1));
            for k in run_start..run_end {
                let b = &pushes[k].batch;
                m.append_batch(b, 0, b.count);
            }
            let err = disp.write_commit_group(tid, &m, mode, &req_ids).err();
            (m, err)
        })) {
            Ok(pair) => pair,
            Err(panic_msg) => {
                let placeholder = guard_panic("commit_fallback_schema", || Ok(unsafe {
                    Batch::with_schema((*disp_ptr).schema_desc_for(tid), 1)
                }))
                .unwrap_or_else(|_| Batch::with_schema(
                    crate::schema::SchemaDescriptor::minimal_u64(), 1));
                (placeholder, Some(panic_msg))
            }
        };

        if write_err.is_none() {
            for w in 0..nw { shared.reactor.increment_in_flight(w); }
        }

        groups.push(GroupInfo {
            start: run_start, end: run_end, tid, req_ids, merged, write_err,
            lsn: 0,
        });
        run_start = run_end;
    }

    let any_written = groups.iter().any(|g| g.write_err.is_none());

    if any_written {
        unsafe { (*shared.disp_ptr).signal_all(); }

        let all_req_ids: Vec<u64> = groups.iter()
            .filter(|g| g.write_err.is_none())
            .flat_map(|g| g.req_ids.iter().copied())
            .collect();
        let fsync = shared.reactor.fsync(shared.sal_fd);
        let reply_futs: Vec<_> = all_req_ids.iter()
            .map(|&id| shared.reactor.await_reply(id))
            .collect();
        let acks = reactor::join_all(reply_futs);
        let (fsync_rc, decoded_vec) = reactor::join2(fsync, acks).await;

        if fsync_rc < 0 {
            crate::gnitz_fatal_abort!(
                "SAL fdatasync (committer) failed rc={}", fsync_rc
            );
        }

        let mut cursor = 0usize;
        for g in groups.iter_mut() {
            if g.write_err.is_some() { continue; }
            if let Some(e) = first_worker_error("commit", &decoded_vec[cursor..cursor + nw]) {
                g.write_err = Some(e);
            }
            cursor += nw;
        }

        // Successful groups: bump ingest_lsn (sequentially per group) and
        // feed the unique-index filter. Wrap the filter calls per V.7.
        let disp_ptr = shared.disp_ptr;
        for g in groups.iter_mut() {
            if g.write_err.is_some() {
                let _ = guard_panic("unique_filter_invalidate", || {
                    unsafe { (*disp_ptr).unique_filter_invalidate_table(g.tid); }
                    Ok(())
                });
                continue;
            }
            let new_lsn = shared.ingest_lsn.get() + 1;
            shared.ingest_lsn.set(new_lsn);
            g.lsn = new_lsn;
            if let Err(e) = guard_panic("unique_filter_ingest", || {
                unsafe { (*disp_ptr).unique_filter_ingest_batch(g.tid, &g.merged); }
                Ok(())
            }) {
                // Don't fail the commit — the data is durable. Invalidate so
                // the next constrained INSERT re-validates from scratch.
                let _ = guard_panic("unique_filter_invalidate", || {
                    unsafe { (*disp_ptr).unique_filter_invalidate_table(g.tid); }
                    Ok(())
                });
                crate::gnitz_warn!("{}", e);
            }
        }
    }

    // Now send responses in original order.
    for g in &groups {
        let drained: Vec<PendingPush> = pushes.drain(..g.end - g.start).collect();
        for p in drained {
            let result = match &g.write_err {
                Some(e) => Err(e.clone()),
                None => Ok(g.lsn),
            };
            let _ = p.done.send(result);
        }
    }
}
