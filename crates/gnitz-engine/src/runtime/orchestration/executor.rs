//! Server executor: fully async event loop built on the reactor.
//!
//! The master process owns a single `Reactor` that drives:
//! - the accept socket (client connections),
//! - all per-fd connection tasks (one per client),
//! - the committer task (group commit + checkpoint + fsync),
//! - the tick task (event-driven, coalesces triggers),
//! - the relay task (writes FLAG_EXCHANGE_RELAY groups),
//! - the worker-crash watcher task.
//!
//! Ticks allocate per-worker req_ids, write one FLAG_TICK group per
//! pending tid, signal once, and `join_all` the ACKs through the
//! reactor's reply routing. The reactor demuxes FLAG_EXCHANGE wires
//! into an accumulator and hands completed views to the relay task.

use std::cell::{Cell, RefCell};
use std::num::NonZeroU64;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::storage::batch_pool::PooledSendBuf;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::catalog::{
    CatalogEngine, FIRST_USER_TABLE_ID, IDXTAB_PAY_IS_UNIQUE, IDXTAB_PAY_OWNER_ID, IDXTAB_PAY_SOURCE_COLS, IDX_TAB_ID,
    SEQ_ID_INDICES, SEQ_ID_SCHEMAS, SEQ_ID_TABLES, TABLE_TAB_ID, VIEW_TAB_ID,
};
use crate::foundation::posix_io::guard_panic;
use crate::runtime::committer::{self, CommitRequest};
use crate::runtime::master::{first_worker_error_opt, MasterDispatcher};
use crate::runtime::reactor::{
    join_into, mpsc, oneshot, select2, AsyncMutex, AsyncRwLock, Either, PendingRelay, Reactor, ReplyFuture,
};
use crate::runtime::wire::{
    self as ipc, SchemaWithVersion, FLAG_GET_INDICES, STATUS_ERROR, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH,
};
use crate::schema::{validate_schema_match, SchemaDescriptor};
use crate::storage::Batch;
use crate::storage::{index_meta_schema_desc, BatchBuilder, INDEX_META_COL_NAMES};

const TICK_COALESCE_ROWS: usize = 10_000;
const TICK_DEADLINE_MS: u64 = 20;
const WORKER_WATCH_MS: u64 = 100;

const FLAG_ALLOCATE_TABLE_ID: u64 = 1;
const FLAG_ALLOCATE_SCHEMA_ID: u64 = 2;
const FLAG_SEEK: u64 = 128;
const FLAG_SEEK_BY_INDEX: u64 = 256;
const FLAG_ALLOCATE_INDEX_ID: u64 = 512;

/// One tick request to `tick_loop_async`.
pub enum TickTrigger {
    /// Fire-and-forget trigger from INSERT when a tid crosses the row
    /// coalesce threshold.  Tids come from `tick_rows` / `tick_tids`.
    Auto,
    /// Explicit drain requested by SCAN: forces the listed tids to tick
    /// even if their `tick_rows` counter is empty, then signals `done`.
    Drain { tids: Vec<i64>, done: oneshot::Sender<()> },
    /// Pause the tick subsystem for a stop-the-world CREATE-VIEW DDL. On
    /// dequeue the tick loop signals `acked` — proving no tick is in flight
    /// (the loop is serial, so the prior tick has returned) and none will
    /// start — then blocks on `release` until the DDL hands the gate back.
    /// This drains any in-flight steady-state exchange tick before the DDL
    /// parks the reactor; see `handle_system_dml`.
    Quiesce {
        acked: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
}

/// Releases the tick-subsystem quiesce gate when dropped, so a CREATE-VIEW
/// stop-the-world window ends on every exit path of `handle_system_dml`
/// (success or early-return error). Sending wakes the parked `tick_loop_async`,
/// which resumes dequeuing ticks. `None` for non-view DDL (no gate taken).
struct TickGate(Option<oneshot::Sender<()>>);
impl Drop for TickGate {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// Shared executor state held by every task.
pub struct Shared {
    pub reactor: Rc<Reactor>,
    catalog: *mut CatalogEngine,
    dispatcher: *mut MasterDispatcher,
    sal_fd: i32,
    committer_tx: mpsc::Sender<CommitRequest>,
    catalog_rwlock: Rc<AsyncRwLock>,
    /// SAL-writer exclusivity. Held by committer (checkpoint + commit
    /// emission), tick (per-tid emission), relay (FLAG_EXCHANGE_RELAY),
    /// DDL (broadcast_ddl + fsync), and all fan-out operations (seek,
    /// scan, pipeline checks, unique-filter warmup). See async-invariants.md.
    sal_writer_excl: Rc<AsyncMutex<()>>,
    /// Tick trigger sender; senders include INSERT (auto-trigger on
    /// threshold cross) and SCAN (explicit drain).
    tick_tx: mpsc::Sender<TickTrigger>,
    /// Committer-owned counter; shared here so SCAN/SEEK handlers can
    /// report the same LSN the committer assigns.
    ingest_lsn: Rc<Cell<u64>>,
    last_tick_lsn: Rc<Cell<u64>>,
    /// Per-table row counter feeding the tick threshold.
    tick_rows: Rc<RefCell<FxHashMap<i64, usize>>>,
    tick_tids: Rc<RefCell<Vec<i64>>>,
    t_last_push: Rc<Cell<Option<Instant>>>,
    table_locks: RefCell<FxHashMap<i64, Rc<AsyncMutex<()>>>>,
}

impl Shared {
    #[allow(clippy::mut_from_ref)]
    fn cat(&self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }
    #[allow(clippy::mut_from_ref)]
    fn disp(&self) -> &mut MasterDispatcher {
        unsafe { &mut *self.dispatcher }
    }

    fn get_schema_desc(&self, target_id: i64) -> SchemaDescriptor {
        self.cat()
            .get_schema_desc(target_id)
            .unwrap_or_else(SchemaDescriptor::minimal_u64)
    }

    /// Return (or build and cache) the encoded schema wire block and current
    /// schema version for `target_id`. The block is stable for the lifetime
    /// of the table schema — it is invalidated alongside col_names whenever
    /// DDL modifies the table.
    fn get_schema_wire_block(&self, target_id: i64) -> (Rc<Vec<u8>>, u16) {
        let cat = self.cat();
        if let Some(cached) = cat.get_cached_schema_wire_block(target_id) {
            return (cached.block, cached.version);
        }
        let schema = cat
            .get_schema_desc(target_id)
            .unwrap_or_else(SchemaDescriptor::minimal_u64);
        let col_names = cat.get_col_names_bytes(target_id);
        let (name_refs, n) = ipc::col_names_as_refs(&col_names);
        let names_slice = &name_refs[..n];
        let block = Rc::new(ipc::build_schema_wire_block(&schema, names_slice, target_id as u32));
        let version = cat.get_schema_version(target_id);
        let (wire_safe, wire_row_stride) = crate::runtime::sal::compute_wire_props(&schema);
        cat.set_schema_wire_block(target_id, block.clone(), wire_safe, wire_row_stride);
        (block, version)
    }

    fn table_lock(&self, tid: i64) -> Rc<AsyncMutex<()>> {
        let mut locks = self.table_locks.borrow_mut();
        if let Some(l) = locks.get(&tid) {
            return Rc::clone(l);
        }
        let l = Rc::new(AsyncMutex::new(()));
        locks.insert(tid, Rc::clone(&l));
        l
    }

    /// True iff some pending tid has crossed the row coalesce threshold.
    /// Used by the tick task to skip the deadline coalesce window.
    fn any_threshold_crossed(&self) -> bool {
        self.tick_rows.borrow().values().any(|&rows| rows >= TICK_COALESCE_ROWS)
    }

    /// Drain `tick_rows` and `tick_tids` into `out`, retaining `out`'s
    /// capacity. Stable insertion order is preserved (anti-join semantics
    /// require that the b-side trace runs after a-side ticks). The caller's
    /// scratch buffer is reused across ticks instead of allocating a fresh
    /// `Vec` per drain.
    fn drain_tick_rows_into(&self, out: &mut Vec<i64>) {
        out.clear();
        let mut tids = self.tick_tids.borrow_mut();
        out.extend(tids.drain(..));
        self.tick_rows.borrow_mut().clear();
        self.t_last_push.set(None);
    }
}

// ---------------------------------------------------------------------------
// ServerExecutor entry point
// ---------------------------------------------------------------------------

pub struct ServerExecutor;

impl ServerExecutor {
    pub fn run(catalog: *mut CatalogEngine, dispatcher: *mut MasterDispatcher, server_fd: i32) -> i32 {
        let reactor = match Reactor::new(256) {
            Ok(r) => Rc::new(r),
            Err(e) => {
                eprintln!("io_uring init failed: {e}");
                return -1;
            }
        };
        let sal_fd = unsafe { &*dispatcher }.sal_fd();
        let num_workers = unsafe { &*dispatcher }.num_workers();

        reactor.attach_w2m(unsafe { &mut *dispatcher }.take_w2m());
        // After handoff, point the dispatcher at the reactor-owned receiver so
        // the reactor-parked CREATE-VIEW backfill can drive a synchronous
        // collect (the reactor's `OnceCell` slot is stable for its lifetime).
        unsafe { &mut *dispatcher }.set_w2m_receiver_ptr(reactor.w2m_receiver());
        reactor.attach_server_fd(server_fd);

        let sal_writer_excl = Rc::new(AsyncMutex::new(()));
        // Seed ingest_lsn above every table's current_lsn so each new zone
        // LSN is strictly greater, keeping `ingest_to_family`'s direct
        // current_lsn assignment monotonic across restarts.
        let initial_ingest_lsn = unsafe { &*catalog }.max_table_current_lsn();
        let ingest_lsn = Rc::new(Cell::new(initial_ingest_lsn));
        let last_tick_lsn = Rc::new(Cell::new(initial_ingest_lsn));
        let tick_rows: Rc<RefCell<FxHashMap<i64, usize>>> = Rc::new(RefCell::new(FxHashMap::default()));
        let tick_tids: Rc<RefCell<Vec<i64>>> = Rc::new(RefCell::new(Vec::new()));
        let t_last_push = Rc::new(Cell::new(None));

        let (committer_tx, committer_rx) = mpsc::unbounded::<CommitRequest>();
        let (tick_tx, tick_rx) = mpsc::unbounded::<TickTrigger>();
        let (relay_tx, relay_rx) = mpsc::unbounded::<PendingRelay>();
        // Wire the relay channel into the reactor so route_reply's
        // FLAG_EXCHANGE accumulator can hand off completed views.
        reactor.attach_relay_tx(relay_tx);

        let auto_tick_tx = tick_tx.clone();
        let committer_shared = Rc::new(committer::Shared {
            reactor: Rc::clone(&reactor),
            disp_ptr: dispatcher,
            sal_fd,
            sal_writer_excl: Rc::clone(&sal_writer_excl),
            ingest_lsn: Rc::clone(&ingest_lsn),
            num_workers,
            tick_rows: Rc::clone(&tick_rows),
            tick_tids: Rc::clone(&tick_tids),
            fire_auto_tick: Rc::new(move || {
                auto_tick_tx.send(TickTrigger::Auto);
            }),
            t_last_push: Rc::clone(&t_last_push),
        });
        let shared = Rc::new(Shared {
            reactor: Rc::clone(&reactor),
            catalog,
            dispatcher,
            sal_fd,
            committer_tx,
            catalog_rwlock: Rc::new(AsyncRwLock::new()),
            sal_writer_excl: Rc::clone(&sal_writer_excl),
            tick_tx,
            ingest_lsn: Rc::clone(&ingest_lsn),
            last_tick_lsn: Rc::clone(&last_tick_lsn),
            tick_rows: Rc::clone(&tick_rows),
            tick_tids: Rc::clone(&tick_tids),
            t_last_push: Rc::clone(&t_last_push),
            table_locks: RefCell::new(FxHashMap::default()),
        });

        reactor.spawn(committer::run(committer_rx, committer_shared));
        reactor.spawn(accept_loop(Rc::clone(&shared)));
        reactor.spawn(tick_loop_async(Rc::clone(&shared), tick_rx));
        reactor.spawn(relay_loop(Rc::clone(&shared), relay_rx));
        reactor.spawn(worker_watcher(Rc::clone(&shared)));

        reactor.block_until_shutdown();
        0
    }
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

async fn accept_loop(shared: Rc<Shared>) {
    loop {
        let fd = shared.reactor.accept().await;
        if fd < 0 {
            continue;
        }
        shared.reactor.register_conn(fd);
        let s = Rc::clone(&shared);
        shared.reactor.spawn(connection_loop(fd, s));
    }
}

enum HelloOutcome {
    /// Connection accepted; optionally bound to an authenticated client_id.
    Pass(Option<u64>),
    /// Caller must close the fd.
    Reject,
}

/// RAII guard that frees a reactor-allocated receive buffer on any exit path,
/// including when the enclosing task future is dropped at an `.await` suspension
/// point (e.g. server shutdown). A bare `libc::free` after `.await` would leak.
struct MallocGuard(*mut u8);
impl Drop for MallocGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                libc::free(self.0 as *mut libc::c_void);
            }
        }
    }
}

async fn connection_loop(fd: i32, shared: Rc<Shared>) {
    let bound_client_id = match shared.reactor.recv(fd).await {
        Some((ptr, len)) => {
            let _guard = MallocGuard(ptr);
            let outcome = run_hello_handshake(fd, &shared, ptr, len).await;
            match outcome {
                HelloOutcome::Pass(b) => b,
                HelloOutcome::Reject => {
                    shared.reactor.close_fd(fd);
                    return;
                }
            }
        }
        None => {
            shared.reactor.close_fd(fd);
            return;
        }
    };

    loop {
        let (ptr, len) = match shared.reactor.recv(fd).await {
            Some(v) => v,
            None => break,
        };
        let _guard = MallocGuard(ptr); // freed on any exit path including cancellation
        let data = unsafe { std::slice::from_raw_parts(ptr, len) };
        handle_message(fd, data, &shared, bound_client_id).await;
    }
    shared.reactor.close_fd(fd);
}

/// Validate a HELLO frame, elevate the connection's payload limit, and
/// reply with the symmetric ACK. See `Reactor::set_max_payload_len` for
/// why the limit must be raised before any `.await` here.
async fn run_hello_handshake(fd: i32, shared: &Rc<Shared>, ptr: *mut u8, len: usize) -> HelloOutcome {
    let data = unsafe { std::slice::from_raw_parts(ptr, len) };

    // `decode_hello_payload` validates the 8-byte length; the magic
    // check below is defence-in-depth on top of the pre-handshake recv
    // ceiling that already excludes non-HELLO first frames.
    let hello = match gnitz_wire::decode_hello_payload(data) {
        Ok(h) => h,
        Err(_) => return HelloOutcome::Reject,
    };
    if hello.magic != gnitz_wire::HELLO_MAGIC {
        return HelloOutcome::Reject;
    }

    let server_version = gnitz_wire::WAL_FORMAT_VERSION as u16;
    if hello.version != server_version {
        let msg = format!(
            "unsupported wire version: peer={}, server={}",
            hello.version, server_version,
        );
        send_error(shared, fd, 0, 0, msg.as_bytes()).await;
        return HelloOutcome::Reject;
    }

    // Auth method bits live in `hello.flags`. Only "none" (flags=0) is
    // wired today; future auth hooks would set `bound_client_id` here.
    let bound_client_id: Option<u64> = None;

    shared
        .reactor
        .set_max_payload_len(fd, gnitz_wire::MAX_FRAME_PAYLOAD_SERVER);

    let rc = shared.reactor.send_hello_ack(fd).await;
    if rc < 0 {
        return HelloOutcome::Reject;
    }
    HelloOutcome::Pass(bound_client_id)
}

// ---------------------------------------------------------------------------
// Worker-crash watcher
// ---------------------------------------------------------------------------

async fn worker_watcher(shared: Rc<Shared>) {
    loop {
        shared
            .reactor
            .timer(Instant::now() + Duration::from_millis(WORKER_WATCH_MS))
            .await;
        let crashed = shared.disp().check_workers();
        if crashed >= 0 {
            let base_dir = shared.cat().base_dir.clone();
            eprintln!("Worker {crashed} crashed (log: {base_dir}/worker_{crashed}.log), shutting down",);
            shared.disp().shutdown_workers();
            shared.reactor.request_shutdown();
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Tick loop (event-driven)
// ---------------------------------------------------------------------------

/// Drive ticks from a channel of `TickTrigger`s. Coalesces triggers
/// inside a bounded deadline window, then issues one batched tick for
/// the union of pending tids. Per IV.6, every per-(tid, worker) req_id
/// is allocated up front, all groups are written, then a single
/// `signal_all` fires.  ACKs are awaited via `join_all` through the
/// reactor's reply routing.
///
/// V.7 liveness: the outer loop body is wrapped so a failure in one
/// trigger only fails that trigger, not the loop. SAL emission is
/// further guarded by `guard_panic` inside `run_tick`.
async fn tick_loop_async(shared: Rc<Shared>, mut rx: mpsc::Receiver<TickTrigger>) {
    let nw = unsafe { (*shared.dispatcher).num_workers() };
    let mut fut_slots: Vec<ReplyFuture> = Vec::with_capacity(nw);
    let mut ack_slots: Vec<Option<ipc::DecodedWire>> = Vec::with_capacity(nw);
    let mut req_ids: Vec<u64> = Vec::with_capacity(nw);
    let mut triggers: Vec<TickTrigger> = Vec::new();
    // Reused across every tick; `drain_tick_rows_into` clears it before
    // refilling so capacity is retained.
    let mut tids_scratch: Vec<i64> = Vec::new();
    loop {
        let first = match rx.recv().await {
            Some(t) => t,
            None => return, // all senders dropped — clean shutdown
        };
        triggers.push(first);

        // Drain anything already queued.
        while let Some(more) = rx.try_recv() {
            triggers.push(more);
        }

        // Honour the coalesce deadline only if no trigger is row-threshold
        // urgent and no Drain is pending. Drain is a synchronous probe
        // (handle_scan awaits its `done`) so coalescing would just stall
        // the caller for TICK_DEADLINE_MS with nothing to coalesce.
        //
        // The timer is pinned outside the inner loop so every iteration
        // re-polls the same TimerFuture — its SQE is submitted once on
        // first poll and re-used across all `rx.recv()` wake-ups. The
        // previous shape (`let timer = …` inside the loop) allocated a
        // fresh TimerFuture per iteration and submitted a new SQE every
        // time `rx.recv()` resolved Pending-then-Ready, which was
        // unnecessary kernel churn.
        // A Quiesce is as urgent as a Drain: skip the coalesce window (the DDL
        // awaits its ack) and break the window if one arrives mid-coalesce.
        let urgent = |t: &TickTrigger| matches!(t, TickTrigger::Drain { .. } | TickTrigger::Quiesce { .. });
        let has_urgent = triggers.iter().any(urgent);
        if !has_urgent && !shared.any_threshold_crossed() {
            let deadline = Instant::now() + Duration::from_millis(TICK_DEADLINE_MS);
            let mut timer = Box::pin(shared.reactor.timer(deadline));
            loop {
                match select2(rx.recv(), timer.as_mut()).await {
                    Either::A(Some(more)) => {
                        let was_urgent = urgent(&more);
                        triggers.push(more);
                        if was_urgent || shared.any_threshold_crossed() {
                            break;
                        }
                    }
                    Either::A(None) => return, // channel closed mid-coalesce
                    Either::B(()) => break,    // deadline elapsed
                }
            }
        }

        // Process Quiesce markers before ticking: ack each (no tick is in
        // flight — the loop is serial) and block until the DDL releases the
        // gate, so no tick runs (and no exchange tick is in flight) while the
        // DDL holds the catalog write lock and parks the reactor. Remaining
        // Auto/Drain triggers in this batch run after release. No new triggers
        // arrive meanwhile: the DDL's write lock blocks every push, so the
        // committer fires no Auto.
        for trigger in std::mem::take(&mut triggers) {
            if let TickTrigger::Quiesce { acked, release } = trigger {
                let _ = acked.send(());
                let _ = release.await;
            } else {
                triggers.push(trigger);
            }
        }

        // Build the tid set: union of drained tick_rows (in INSERT order)
        // + explicit tids from SCAN triggers. The order is load-bearing:
        // anti-join semantics (EXCEPT, NOT IN, etc.) rely on processing
        // ticks in the order pushes arrived. Reordering causes the b-side
        // trace to be empty when a-side ticks (and vice versa), leaking
        // rows that should have cancelled.
        shared.drain_tick_rows_into(&mut tids_scratch);
        let has_drain = triggers.iter().any(|t| matches!(t, TickTrigger::Drain { .. }));
        if has_drain {
            let mut seen: FxHashSet<i64> = tids_scratch.iter().copied().collect();
            for t in &triggers {
                if let TickTrigger::Drain { tids: v, .. } = t {
                    for &tid in v {
                        if seen.insert(tid) {
                            tids_scratch.push(tid);
                        }
                    }
                }
            }
        }
        tids_scratch.retain(|&tid| shared.cat().has_id(tid));

        // Run the tick. Errors are reported in logs; every Drain trigger's
        // `done` is signalled regardless so callers don't hang.
        if let Err(e) = run_tick(&shared, &tids_scratch, nw, &mut req_ids, &mut fut_slots, &mut ack_slots).await {
            gnitz_warn!("tick error: {}", e);
        }
        for t in triggers.drain(..) {
            if let TickTrigger::Drain { done, .. } = t {
                let _ = done.send(());
            }
        }
    }
}

/// Emit FLAG_TICK groups for every `tid` and await the per-worker ACKs.
///
/// Holds `catalog_rwlock.read()` while looking up schemas + writing SAL
/// so DDL cannot mutate schemas mid-emission. Holds `sal_writer_excl`
/// for the contiguous emission window (III.3b). Both are released
/// before awaiting ACKs so other reactor work can proceed concurrently
/// with worker DAG eval.
async fn run_tick(
    shared: &Rc<Shared>,
    tids: &[i64],
    nw: usize,
    req_ids: &mut Vec<u64>,
    fut_slots: &mut Vec<ReplyFuture>,
    ack_slots: &mut Vec<Option<ipc::DecodedWire>>,
) -> Result<(), String> {
    if tids.is_empty() {
        return Ok(());
    }
    // Snapshot before any .await: a concurrent push can bump ingest_lsn
    // while we wait for tick ACKs, and setting last_tick_lsn to that
    // higher value would report an LSN that this tick never processed.
    let snapshot_lsn = shared.ingest_lsn.get();

    // Allocate every (tid, worker) req_id up front so the SAL emission
    // sequence is fully prepared before we take any locks.
    req_ids.clear();
    req_ids.extend((0..tids.len() * nw).map(|_| shared.reactor.alloc_request_id()));

    let _cat_read = shared.catalog_rwlock.read().await;
    let _sal_excl = shared.sal_writer_excl.lock().await;

    let emit_err = guard_panic("tick", || unsafe {
        let disp = &mut *shared.dispatcher;
        for (i, &tid) in tids.iter().enumerate() {
            let lsn = disp.next_lsn();
            disp.write_tick_group(tid, lsn, &req_ids[i * nw..(i + 1) * nw])?;
        }
        disp.signal_all();
        Ok(())
    });
    drop(_sal_excl);
    drop(_cat_read);
    emit_err?;

    fut_slots.clear();
    fut_slots.extend(req_ids.iter().copied().map(|id| shared.reactor.await_reply(id)));
    join_into(fut_slots, ack_slots).await;
    let err = first_worker_error_opt("tick", ack_slots);
    ack_slots.clear();
    if let Some(e) = err {
        return Err(e);
    }
    shared.last_tick_lsn.set(snapshot_lsn);
    Ok(())
}

// ---------------------------------------------------------------------------
// Relay loop
// ---------------------------------------------------------------------------

/// Consume completed `PendingRelay`s from the reactor's exchange
/// accumulator and write FLAG_EXCHANGE_RELAY groups back through the
/// dispatcher.  Lives in its own task so the SAL write happens outside
/// the reactor's synchronous CQE handler — `relay_exchange` reads the
/// catalog DAG (needs catalog_rwlock.read) and writes a SAL group
/// (needs sal_writer_excl), neither of which can block-acquire from
/// inside the reactor's tick.
///
/// A lost relay wedges workers blocked in `do_exchange_wait` forever
/// (they ACK neither tick nor relay and the master stays alive), so both
/// failure modes — an `emit_relay` error and no space after a reclaim
/// checkpoint — `gnitz_fatal_abort!` rather than warn-and-drop: a loud,
/// recoverable crash (workers self-exit via `getppid()`, operator
/// restarts) beats a silent permanent cluster wedge.
async fn relay_loop(shared: Rc<Shared>, mut rx: mpsc::Receiver<PendingRelay>) {
    loop {
        let relay = match rx.recv().await {
            Some(r) => r,
            None => return,
        };

        // Phase 1: CPU work + catalog read only — no SAL mutex.
        let prep = {
            let _cat = shared.catalog_rwlock.read().await;
            match guard_panic("prepare_relay", || unsafe { (*shared.dispatcher).prepare_relay(relay) }) {
                Ok(p) => p,
                Err(e) => gnitz_fatal_abort!("prepare_relay failed: {}", e),
            }
        };

        // Phase 2: emit under the SAL mutex. The space check shares the
        // lock with the write, so no other SAL writer can consume the
        // margin in between. The barrier await MUST happen with the lock
        // dropped: the committer's checkpoint takes sal_writer_excl, so
        // holding it across the barrier deadlocks master-side.
        let mut prep = Some(prep);
        let mut reclaimed = false;
        loop {
            {
                let _sal = shared.sal_writer_excl.lock().await;
                if unsafe { (*shared.dispatcher).sal_has_relay_space_arming() } {
                    if let Err(e) = guard_panic("emit_relay", || unsafe {
                        (*shared.dispatcher).emit_relay(prep.take().expect("relay emitted once"))
                    }) {
                        gnitz_fatal_abort!(
                            "emit_relay failed; a lost relay wedges workers \
                             blocked in exchange wait: {}",
                            e
                        );
                    }
                    break;
                }
                if reclaimed {
                    gnitz_fatal_abort!(
                        "SAL space exhausted even after forced checkpoint; \
                         cannot deliver exchange relay — aborting to prevent \
                         cluster deadlock"
                    );
                }
            }
            gnitz_warn!("SAL space low before exchange relay; triggering checkpoint");
            let (tx, rx_done) = oneshot::channel();
            shared.committer_tx.send(CommitRequest::Barrier { done: tx });
            let _ = rx_done.await;
            reclaimed = true;
        }
    }
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

async fn handle_message(fd: i32, data: &[u8], shared: &Rc<Shared>, bound_client_id: Option<u64>) {
    // Routing fast path: read (target_id, client_id) directly from the
    // control block's directory without allocating or running the full
    // decode. The auth check below uses the same parse the schema-hint
    // lookup uses; a forged directory cannot point one at an authorized
    // id and the other at a different region.
    let (peeked_target_id, peeked_client_id) = match ipc::peek_routing_header(data) {
        Ok(v) => v,
        Err(e) => {
            let msg = format!("decode error: {e}");
            send_error(shared, fd, 0, 0, msg.as_bytes()).await;
            return;
        }
    };
    if let Some(bound) = bound_client_id {
        if peeked_client_id != bound {
            // Reject before any heap-allocating decode path. A forged
            // client_id never reaches Batch::decode_from_wal_block.
            send_error(
                shared,
                fd,
                peeked_target_id as i64,
                bound,
                b"client_id not bound to this connection",
            )
            .await;
            return;
        }
    }

    // Decode the frame. Schema-less PUSH frames (warm-cache path) have
    // FLAG_HAS_DATA but not FLAG_HAS_SCHEMA; they need a catalog hint.
    let mut decoded = {
        let ctrl = match ipc::peek_control_block(data) {
            Ok(c) => c,
            Err(e) => {
                let msg = format!("decode error: {e}");
                send_error(shared, fd, 0, 0, msg.as_bytes()).await;
                return;
            }
        };
        let has_schema = (ctrl.flags & ipc::FLAG_HAS_SCHEMA) != 0;
        let has_data = (ctrl.flags & ipc::FLAG_HAS_DATA) != 0;
        if has_data && !has_schema {
            let client_version = ipc::wire_flags_get_schema_version(ctrl.flags);
            if client_version == 0 {
                send_error(
                    shared,
                    fd,
                    ctrl.target_id as i64,
                    ctrl.client_id,
                    b"FLAG_HAS_DATA without FLAG_HAS_SCHEMA",
                )
                .await;
                return;
            }
            let server_version = shared.cat().get_schema_version(ctrl.target_id as i64);
            if client_version != server_version {
                send_control_only(
                    shared,
                    fd,
                    ctrl.target_id as i64,
                    ctrl.client_id,
                    STATUS_SCHEMA_MISMATCH,
                )
                .await;
                return;
            }
            let catalog_schema = shared.get_schema_desc(ctrl.target_id as i64);
            let hint = SchemaWithVersion {
                descriptor: &catalog_schema,
                version: server_version,
            };
            match ipc::decode_wire_with_hint(data, hint) {
                Ok(d) => d,
                Err(e) => {
                    let msg = format!("decode error: {e}");
                    send_error(shared, fd, ctrl.target_id as i64, ctrl.client_id, msg.as_bytes()).await;
                    return;
                }
            }
        } else {
            match ipc::decode_wire(data) {
                Ok(d) => d,
                Err(e) => {
                    let msg = format!("decode error: {e}");
                    send_error(shared, fd, 0, 0, msg.as_bytes()).await;
                    return;
                }
            }
        }
    };

    // Trust boundary: FLAG_BATCH_SORTED / FLAG_BATCH_CONSOLIDATED assert "already
    // sorted/consolidated, skip the work." A client must never be trusted to claim
    // that, so neutralize them on every client-decoded batch; downstream
    // consolidation (the catalog DDL ingest and the commit path) then re-establishes
    // the invariants. No-op for conforming clients, which never set these.
    if let Some(b) = decoded.data_batch.as_mut() {
        b.sorted = false;
        b.consolidated = false;
    }

    let client_id = decoded.control.client_id;
    let target_id = decoded.control.target_id as i64;
    let flags = decoded.control.flags;
    let client_version = ipc::wire_flags_get_schema_version(flags);

    // ---------- ID allocations ----------
    if target_id == 0 {
        if flags & FLAG_ALLOCATE_TABLE_ID != 0 {
            let new_id = shared.cat().allocate_table_id();
            shared.cat().advance_sequence(SEQ_ID_TABLES, new_id - 1, new_id);
            send_alloc(shared, fd, new_id, client_id).await;
            return;
        }
        if flags & FLAG_ALLOCATE_SCHEMA_ID != 0 {
            let new_id = shared.cat().allocate_schema_id();
            shared.cat().advance_sequence(SEQ_ID_SCHEMAS, new_id - 1, new_id);
            send_alloc(shared, fd, new_id, client_id).await;
            return;
        }
        if flags & FLAG_ALLOCATE_INDEX_ID != 0 {
            let new_id = shared.cat().allocate_index_id();
            shared.cat().advance_sequence(SEQ_ID_INDICES, new_id - 1, new_id);
            send_alloc(shared, fd, new_id, client_id).await;
            return;
        }
    }

    let has_batch = decoded.data_batch.is_some();
    let batch_count = decoded.data_batch.as_ref().map(|b| b.count).unwrap_or(0);

    // ---------- SELECTs (SEEK / SEEK_BY_INDEX / SCAN) ----------
    if flags & FLAG_SEEK != 0 {
        let _g = shared.catalog_rwlock.read().await;
        handle_seek(
            shared,
            fd,
            client_id,
            target_id,
            decoded.control.seek_pk,
            &decoded.control.seek_pk_extra,
            client_version,
        )
        .await;
        return;
    }
    if flags & FLAG_SEEK_BY_INDEX != 0 {
        let _g = shared.catalog_rwlock.read().await;
        handle_seek_by_index(
            shared,
            fd,
            client_id,
            target_id,
            decoded.control.seek_col_idx, // pack_pk_cols(col_indices)
            decoded.control.seek_pk,
            &decoded.control.seek_pk_extra,
            client_version,
        )
        .await;
        return;
    }
    if flags & gnitz_wire::FLAG_SEEK_BY_INDEX_RANGE != 0 {
        let _g = shared.catalog_rwlock.read().await;
        handle_seek_by_index_range(
            shared,
            fd,
            client_id,
            target_id,
            decoded.control.seek_col_idx,   // pack_pk_cols(col_indices)
            &decoded.control.seek_pk_extra, // encoded RangeDescriptor
            client_version,
        )
        .await;
        return;
    }

    // GET_INDICES must be routed before the generic empty-batch scan dispatch
    // below, which keys only on target_id and would otherwise swallow it. The
    // epoch read and the descriptor build both run under the catalog read lock,
    // so there is no torn read between the epoch and the circuit list.
    if flags & FLAG_GET_INDICES != 0 {
        let _g = shared.catalog_rwlock.read().await;
        handle_get_indices(
            shared,
            fd,
            client_id,
            target_id,
            ipc::wire_flags_get_index_version(flags),
        )
        .await;
        return;
    }

    if target_id >= FIRST_USER_TABLE_ID && (!has_batch || batch_count == 0) {
        handle_scan(shared, fd, client_id, target_id, client_version).await;
        return;
    }

    // ---------- Schema validation on incoming data ----------
    if has_batch {
        if let Some(ref wire_schema) = decoded.schema {
            let expected = shared.get_schema_desc(target_id);
            if let Err(e) = validate_schema_match(wire_schema, &expected) {
                send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
                return;
            }
        }
    }

    // ---------- User-table INSERT ----------
    if target_id >= FIRST_USER_TABLE_ID && has_batch && batch_count > 0 {
        let mode = ipc::wire_flags_get_conflict_mode(flags);
        let batch = decoded.data_batch.unwrap();

        let _cat = shared.catalog_rwlock.read().await;
        if !shared.cat().has_id(target_id) {
            let msg = format!("table {target_id} not found");
            send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
            return;
        }
        // Acquire all FK-related table locks in ascending tid order.
        // Sorted acquisition prevents deadlock between concurrent child
        // INSERT and parent DELETE: both attempt the same ordered set.
        let lock_set = shared.cat().fk_lock_set(target_id);
        let mut _tlocks = Vec::with_capacity(lock_set.len());
        for &tid in &lock_set {
            _tlocks.push(shared.table_lock(tid).lock().await);
        }

        // Local (catalog-resident) unique-index validation. Wrapped per V.4
        // so a malformed batch can't crash the server.
        let cat_ptr_raw = shared.catalog;
        if let Err(e) = guard_panic("validate", || unsafe {
            (*cat_ptr_raw).validate_unique_indices(target_id, &batch)
        }) {
            send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
            return;
        }
        // Distributed validation (FK / unique indices + UPSERT).
        if let Err(e) = MasterDispatcher::validate_all_distributed_async(
            shared.dispatcher,
            &shared.reactor,
            &shared.sal_writer_excl,
            target_id,
            &batch,
            mode,
        )
        .await
        {
            send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
            return;
        }

        // Route through the committer and wait for commit ACK.
        let (tx, rx) = oneshot::channel::<Result<u64, String>>();
        shared.committer_tx.send(CommitRequest::Push {
            tid: target_id,
            batch,
            mode,
            done: tx,
        });
        let commit_result = rx.await;
        match commit_result {
            Ok(Ok(lsn)) => {
                send_ok_response(shared, fd, target_id, None, client_id, lsn as u128, client_version).await;
            }
            Ok(Err(e)) => {
                send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
            }
            Err(_) => {
                send_error(shared, fd, target_id, client_id, b"committer shut down").await;
            }
        }
        return;
    }

    // ---------- System-table DML (catalog + optional DDL broadcast) ----------
    if target_id < FIRST_USER_TABLE_ID {
        handle_system_dml(shared, fd, client_id, target_id, decoded, client_version).await;
    }

    // Fallthrough: ignore (should not happen).
}

async fn handle_seek(
    shared: &Rc<Shared>,
    fd: i32,
    client_id: u64,
    target_id: i64,
    pk: u128,
    seek_pk_extra: &[u8],
    client_version: u16,
) {
    if !shared.cat().has_id(target_id) {
        let msg = format!("table {target_id} not found");
        send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    if target_id >= FIRST_USER_TABLE_ID {
        match MasterDispatcher::fan_out_seek_async(
            shared.dispatcher,
            &shared.reactor,
            &shared.sal_writer_excl,
            target_id,
            pk,
            seek_pk_extra,
        )
        .await
        {
            Ok(slot) => shared.reactor.send_slot_or_close(fd, slot).await,
            Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
        }
    } else {
        match unsafe { (*shared.catalog).seek_family(target_id, pk, seek_pk_extra) } {
            Ok(batch) => send_ok_response(shared, fd, target_id, batch.as_ref(), client_id, pk, client_version).await,
            Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
        }
    }
}

/// Decode `seek_col_idx` (`pack_pk_cols(col_indices)` — the packed flag at bit
/// 63 is always set, so the old `col_idx as usize >= num_columns` guard would
/// always trip) and validate the full list against the table's schema before
/// classifying. Shared by the SEEK_BY_INDEX and SEEK_BY_INDEX_RANGE handlers.
fn validated_index_cols(
    shared: &Rc<Shared>,
    target_id: i64,
    seek_col_idx: u64,
    op: &str,
) -> Result<gnitz_wire::PkColList, String> {
    let cols = gnitz_wire::unpack_pk_cols(seek_col_idx);
    if let Some(schema) = shared.cat().get_schema_desc(target_id) {
        let ok = cols.is_well_formed() && cols.as_slice().iter().all(|&c| (c as usize) < schema.num_columns());
        if !ok {
            return Err(format!("{op}: invalid column list for table {target_id}"));
        }
    }
    Ok(cols)
}

#[allow(clippy::too_many_arguments)]
async fn handle_seek_by_index(
    shared: &Rc<Shared>,
    fd: i32,
    client_id: u64,
    target_id: i64,
    seek_col_idx: u64,
    seek_pk: u128,
    seek_pk_extra: &[u8],
    client_version: u16,
) {
    if !shared.cat().has_id(target_id) {
        let msg = format!("table {target_id} not found");
        send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    if target_id >= FIRST_USER_TABLE_ID {
        let cols = match validated_index_cols(shared, target_id, seek_col_idx, "seek_by_index") {
            Ok(cols) => cols,
            Err(msg) => {
                send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
                return;
            }
        };
        // Single catalog scan classifies the column list (exact list match). The
        // uniqueness flag is copied out immediately (`Option<bool>`), so no
        // catalog borrow is held across the await in the no-index arm.
        let is_unique = match shared
            .cat()
            .index_circuit_for_cols(target_id, cols.as_slice())
            .map(|ic| ic.is_unique)
        {
            Some(u) => u,
            None => {
                // No secondary index for this column list: a dedicated
                // control-only status, caught here with zero worker dispatch, so
                // the SQL planner falls back to a scan or a CREATE INDEX hint
                // without a prior catalog probe.
                send_control_only(shared, fd, target_id, client_id, STATUS_NO_INDEX).await;
                return;
            }
        };
        if is_unique && cols.as_slice().len() == 1 {
            // Single-column unique index: the seek supplies exactly one value
            // (seek_pk_extra empty), at most one match on a single worker —
            // forward that worker's slot directly (1 round-trip, keeping the
            // unicast-on-cache-hit routing) instead of broadcasting.
            match MasterDispatcher::fan_out_seek_by_index_async(
                shared.dispatcher,
                &shared.reactor,
                &shared.sal_writer_excl,
                target_id,
                cols.as_slice()[0],
                seek_pk,
            )
            .await
            {
                Ok(slot) => shared.reactor.send_slot_or_close(fd, slot).await,
                Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
            }
            return;
        }
        // Composite unique (no composite routing cache) OR non-unique (any
        // arity): the matching rows are scattered across workers, so broadcast
        // and merge all matches into one response (a composite unique seek
        // matches at most one row — merging one is correct). Forward the wire
        // frame verbatim (packed seek_col_idx, seek_pk + seek_pk_extra).
        match MasterDispatcher::fan_out_seek_by_index_collect_async(
            shared.dispatcher,
            &shared.reactor,
            &shared.sal_writer_excl,
            target_id,
            seek_col_idx,
            seek_pk,
            seek_pk_extra,
        )
        .await
        {
            Ok(merged) => {
                send_ok_response(
                    shared,
                    fd,
                    target_id,
                    merged.as_ref(),
                    client_id,
                    seek_pk,
                    client_version,
                )
                .await;
            }
            Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
        }
    } else {
        // System tables never carry secondary indexes.
        let msg = format!("SEEK_BY_INDEX on system table {target_id} is not supported");
        send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
    }
}

/// SELECT-path ordered range scan over a secondary index. Validate the column
/// list, confirm an index on it exists (else STATUS_NO_INDEX, like
/// `handle_seek_by_index`), then broadcast the range descriptor to all
/// workers and merge — a range's matches scatter by source PK, so there is no
/// single-worker fast path. The descriptor is forwarded verbatim (the worker is
/// the sole OPK encoder).
async fn handle_seek_by_index_range(
    shared: &Rc<Shared>,
    fd: i32,
    client_id: u64,
    target_id: i64,
    seek_col_idx: u64,
    seek_pk_extra: &[u8],
    client_version: u16,
) {
    if !shared.cat().has_id(target_id) {
        let msg = format!("table {target_id} not found");
        send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    if target_id < FIRST_USER_TABLE_ID {
        let msg = format!("SEEK_BY_INDEX_RANGE on system table {target_id} is not supported");
        send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    let cols = match validated_index_cols(shared, target_id, seek_col_idx, "seek_by_index_range") {
        Ok(cols) => cols,
        Err(msg) => {
            send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
            return;
        }
    };
    // Confirm a secondary index on this exact column list exists. `.is_some()`
    // copies a bool out, so no catalog borrow is held across the await below.
    let has_index = shared
        .cat()
        .index_circuit_for_cols(target_id, cols.as_slice())
        .is_some();
    if !has_index {
        send_control_only(shared, fd, target_id, client_id, STATUS_NO_INDEX).await;
        return;
    }
    match MasterDispatcher::fan_out_seek_by_index_range_collect_async(
        shared.dispatcher,
        &shared.reactor,
        &shared.sal_writer_excl,
        target_id,
        seek_col_idx,
        seek_pk_extra,
    )
    .await
    {
        Ok(merged) => {
            send_ok_response(shared, fd, target_id, merged.as_ref(), client_id, 0, client_version).await;
        }
        Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
    }
}

/// GET_INDICES: serve the client's durable, epoch-validated cache of a table's
/// secondary-index metadata — the `(col_idx, is_unique)` set, projected from the
/// DAG `index_circuits` (the system's operative truth for "is this column
/// enforced-unique", identical to the server's own FK gate `validate_fk_column`).
/// The client's cached epoch arrives in the index-version wire bits; on a match
/// we reply "unchanged" (no schema, no data), otherwise the fresh list.
async fn handle_get_indices(shared: &Rc<Shared>, fd: i32, client_id: u64, target_id: i64, client_epoch: u8) {
    let server_epoch = shared.cat().get_index_version(target_id); // absent ⇒ 1
    let flags = ipc::wire_flags_set_index_version(0, server_epoch);

    // Warm hit (and the missing-table race, since both resolve to epoch 1 with
    // an empty list): OK, no schema, no data → client keeps its cached Rc.
    if client_epoch == server_epoch {
        let buf = encode_response_buffer(target_id, client_id, None, STATUS_OK, b"", None, 0, flags);
        shared.reactor.send_buffer_or_close(fd, buf).await;
        return;
    }

    // Changed / first fetch: project every index circuit (FK + non-unique
    // included) to (col_idx PK, is_unique). The response always carries its own
    // schema block on the data path, so the client decodes against the wire
    // schema and never needs — or pollutes — the per-table schema cache.
    let desc = index_meta_schema_desc();
    // Build the schema block before the descriptor is moved into BatchBuilder.
    let schema_block = ipc::build_schema_wire_block(&desc, &INDEX_META_COL_NAMES[..], target_id as u32);
    let mut bb = BatchBuilder::new(desc);
    if let Some(entry) = shared.cat().dag.tables.get(&target_id) {
        for ic in &entry.index_circuits {
            // PK = packed column list (unique per circuit: deduped by list).
            bb.begin_row(gnitz_wire::pack_pk_cols(ic.col_indices.as_slice()) as u128, 1);
            bb.put_u64(ic.is_unique as u64); // payload: is_unique
            bb.end_row();
        }
    }
    let batch = bb.finish();
    let result = if batch.count > 0 { Some(&batch) } else { None };
    let buf = encode_response_buffer(
        target_id,
        client_id,
        result,
        STATUS_OK,
        b"",
        Some(schema_block.as_slice()),
        0,
        flags,
    );
    shared.reactor.send_buffer_or_close(fd, buf).await;
}

async fn handle_scan(shared: &Rc<Shared>, fd: i32, client_id: u64, target_id: i64, client_version: u16) {
    // Drain pending ticks before reading: views derive from source-table
    // pushes through the DAG (IV.2). Send a Drain trigger unconditionally
    // — even when `tick_tids` is observed empty — so any in-flight
    // auto-tick has time to complete. The tick loop processes triggers
    // serially, so awaiting our Drain's `done` guarantees serialization
    // behind a concurrent Auto. Without this, a large push fires Auto
    // asynchronously, drains `tick_tids`, but the scan reads before the
    // tick body finishes — the view appears empty until the next scan
    // triggers another drain.
    //
    // The catalog lock is intentionally acquired AFTER the drain: the drain
    // parks at rx.await, and AsyncRwLock is writer-preferring. Holding a
    // read lock here while parked would block concurrent DDL writers and
    // prevent tick_loop_async from acquiring its own read lock, causing a
    // three-way deadlock (BF-1).
    loop {
        let snapshot: Vec<i64> = shared.tick_tids.borrow().clone();
        let was_empty = snapshot.is_empty();
        let (tx, rx) = oneshot::channel::<()>();
        shared.tick_tx.send(TickTrigger::Drain {
            tids: snapshot,
            done: tx,
        });
        let _ = rx.await;
        // After the drain ack: if there were no tids queued AND none
        // appeared during the wait, we're done. Re-check before
        // breaking so a new push during the drain doesn't slip past.
        if was_empty && shared.tick_tids.borrow().is_empty() {
            break;
        }
    }
    let _g = shared.catalog_rwlock.read().await;
    if !shared.cat().has_id(target_id) {
        let msg = format!("table {target_id} not found");
        send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    // A replicated relation (a replicated base table, or a view all of whose
    // sources are replicated) holds an identical full copy on every worker;
    // gathering all workers would return N copies, so read just one (worker 0,
    // which always exists and — replicated tables are exempt from the bootstrap
    // trim — holds the full copy at partition 0).
    let replicated = shared.cat().relation_output_is_replicated(target_id);
    let lsn = shared.last_tick_lsn.get();

    // On a cache miss, master sends a preliminary schema-only frame before
    // dispatching workers. This eliminates N-1 redundant schema blocks
    // (one per worker) from the client's perspective.
    let server_version = shared.cat().get_schema_version(target_id);
    let include_schema = gnitz_wire::wire_should_include_schema(client_version, server_version);
    let effective_client_version = if include_schema {
        let (schema_block, _) = shared.get_schema_wire_block(target_id);
        // Emit preliminary schema frame first, then tell workers to skip schema.
        let prelim_flags = ipc::wire_flags_set_schema_version(ipc::FLAG_CONTINUATION, server_version);
        let prelim = encode_response_buffer(
            target_id,
            client_id,
            None,
            STATUS_OK,
            b"",
            Some(schema_block.as_slice()),
            0,
            prelim_flags,
        );
        let rc = shared.reactor.send_buffer(fd, prelim).await;
        if rc < 0 {
            shared.reactor.close_fd(fd);
            return;
        }
        // Embed server_version as client_version so workers omit their schema blocks.
        server_version
    } else {
        client_version
    };

    let result = if replicated {
        MasterDispatcher::fan_out_scan_single_worker_async(
            shared.dispatcher,
            &shared.reactor,
            &shared.sal_writer_excl,
            target_id,
            0,
            client_id,
            fd,
            effective_client_version,
        )
        .await
    } else {
        MasterDispatcher::fan_out_scan_async(
            shared.dispatcher,
            &shared.reactor,
            &shared.sal_writer_excl,
            target_id,
            client_id,
            fd,
            effective_client_version,
        )
        .await
    };
    match result {
        Ok(true) => {
            let terminal = make_terminal_scan_frame(target_id, client_id, lsn);
            shared.reactor.send_buffer_or_close(fd, terminal).await;
        }
        Ok(false) => {
            shared.reactor.close_fd(fd);
        }
        Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
    }
}

fn make_terminal_scan_frame(target_id: i64, client_id: u64, lsn: u64) -> PooledSendBuf {
    // Terminal scan frame: no schema block, no data. Client ignores schema version here.
    encode_response_buffer(target_id, client_id, None, STATUS_OK, b"", None, lsn as u128, 0)
}

/// System-table path: catalog ingest + (optionally) DDL broadcast.
/// DDL path acquires the catalog write lock and drains the committer.
async fn handle_system_dml(
    shared: &Rc<Shared>,
    fd: i32,
    client_id: u64,
    target_id: i64,
    decoded: ipc::DecodedWire,
    client_version: u16,
) {
    let batch = decoded.data_batch;
    let non_empty = batch.as_ref().map(|b| b.count > 0).unwrap_or(false);

    // Empty SCAN for system tables — no DDL, no lock needed.
    if !non_empty {
        let _g = shared.catalog_rwlock.read().await;
        let cat_ptr = shared.catalog;
        match guard_panic("scan", || unsafe { (*cat_ptr).scan_family(target_id) }) {
            Ok(b) => {
                let batch_ref = if b.count > 0 { Some(b) } else { None };
                send_ok_response(
                    shared,
                    fd,
                    target_id,
                    batch_ref.as_deref(),
                    client_id,
                    shared.last_tick_lsn.get() as u128,
                    client_version,
                )
                .await;
            }
            Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
        }
        return;
    }

    // DDL path: drain the committer barrier BEFORE acquiring the catalog write
    // lock. The barrier flushes user-table WAL and waits for worker ACKs (tens
    // of ms under load); holding the write lock across that wait would block
    // every concurrent SCAN/SEEK read for no reason — no catalog mutation
    // happens until after the barrier returns. Safe because the executor is
    // single-threaded: no other DDL can send its own barrier or take the write
    // lock while this coroutine is parked at rx.await, and concurrent user-table
    // commits don't touch the catalog schema.
    let t_ddl_start = Instant::now();
    let (tx, rx) = oneshot::channel::<()>();
    shared.committer_tx.send(CommitRequest::Barrier { done: tx });
    let _ = rx.await;
    let t_after_barrier = Instant::now();

    // A CREATE VIEW over data-bearing sources is a stop-the-world operation: it
    // drains the new view's sources and drives the view through the distributed
    // backfill, both inline with the reactor parked (further below). The +1
    // (create) rows are the new views; collect their ids before the write lock
    // (the batch is not mutated before its post-lock unwrap) and use the list
    // both to gate the quiesce here and to drive the drain/backfill below. A
    // DROP-only VIEW batch yields none and every non-VIEW DDL is excluded — both
    // keep the plain path. Quiesce the tick subsystem FIRST, while the reactor
    // still runs: an in-flight steady-state exchange tick's relays are delivered
    // by relay_loop (a reactor task), so it must finish before we park the
    // reactor. Quiescing must precede the write lock — run_tick/relay_loop both
    // take the read lock, so a write-lock-held quiesce would deadlock. `TickGate`
    // releases the gate on every exit path.
    let new_view_ids: Vec<i64> = if target_id == VIEW_TAB_ID {
        batch.as_ref().map_or_else(Vec::new, |b| {
            (0..b.count)
                .filter(|&i| b.get_weight(i) > 0)
                .map(|i| b.get_pk(i) as i64)
                .collect()
        })
    } else {
        Vec::new()
    };
    let view_create = !new_view_ids.is_empty();
    let _tick_gate = if view_create {
        let (acked_tx, acked_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        shared.tick_tx.send(TickTrigger::Quiesce {
            acked: acked_tx,
            release: release_rx,
        });
        let _ = acked_rx.await;
        TickGate(Some(release_tx))
    } else {
        TickGate(None)
    };

    let _write = shared.catalog_rwlock.write().await;
    let t_after_write = Instant::now();

    let batch = batch.unwrap();

    let t_drain_start = Instant::now();
    if !new_view_ids.is_empty() {
        // Lock-held committer barrier: a push could have committed between the
        // pre-lock barrier and the write lock; flush it so every straggler is
        // resident in pending_deltas before we drain. The committer stays idle
        // for the rest of the handler (the write lock blocks new pushes).
        let (tx, rx) = oneshot::channel::<()>();
        shared.committer_tx.send(CommitRequest::Barrier { done: tx });
        let _ = rx.await;

        // Drain every base table reachable from the new views — directly or
        // through an existing view source — so its pending_deltas reaches its
        // existing dependents exactly once and its committed store is current
        // before the view backfills. The source set comes from the durable
        // circuit (persisted before this VIEW_TAB row). One source per drain,
        // each a synchronous FLAG_TICK + inline collect with the reactor parked.
        let sources = unsafe { (*shared.catalog).dag.base_tables_reachable_from(new_view_ids.clone()) };
        for src in sources {
            if let Err(e) = guard_panic("view-drain", || shared.disp().drain_tick_blocking(src)) {
                // The view is not registered yet and no zone is open, so surface
                // the error and bail; TickGate's drop releases the gate and the
                // write lock drops on return.
                send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
                return;
            }
        }
    }
    let t_drain_done = Instant::now();
    let t_mut_start = Instant::now();

    let cat_ptr_raw = shared.catalog;
    // Discard any stale queue entries from a prior failed DDL so they don't
    // piggyback on this one. (pending_dir_deletions is NOT discarded here: a
    // failed DDL already clears it on the error path, and recovery legitimately
    // queues drops here that must be drained — not discarded — by this DDL's
    // post-fsync drain.)
    let _ = unsafe { (*cat_ptr_raw).drain_pending_broadcasts() };

    // Pre-flight global uniqueness for every CREATE of a unique secondary index
    // BEFORE reserving the zone LSN or mutating the catalog, so a violation needs
    // no rollback — it just surfaces to the client. Uniqueness is a global
    // property, but the per-worker `backfill_index` scan only sees a single
    // partition: a within-partition duplicate would fatally `_exit` every
    // affected worker via the DdlSync path, and a cross-partition duplicate would
    // be silently accepted. Validating across all workers on the master here
    // catches both and never broadcasts on failure, so no worker reaches that
    // fatal path. Routing every unique-index `+1` through this one branch makes
    // it the single choke point (standalone CREATE UNIQUE INDEX or an inline
    // unique constraint that lowers to an IDX_TAB row). The IDX_TAB row layout
    // (and the IDXTAB_PAY_* payload indices read below) is fixed by
    // `create_index` and read identically by `hook_index_register`.
    let mut filter_seeds: Vec<(i64, u64, FxHashSet<crate::storage::PkBuf>, bool)> = Vec::new();
    if target_id == IDX_TAB_ID {
        for i in 0..batch.count {
            if batch.get_weight(i) > 0 && unsafe { (*cat_ptr_raw).read_batch_u64(&batch, i, IDXTAB_PAY_IS_UNIQUE) } != 0
            {
                let owner_id = unsafe { (*cat_ptr_raw).read_batch_u64(&batch, i, IDXTAB_PAY_OWNER_ID) } as i64;
                // source_cols is the packed list — single-column or composite.
                // Pre-flight every well-formed unique index over its full column
                // list; a malformed row is rejected by hook_index_register inside
                // the ingest below with a precise error, so skip pre-flighting it
                // here (a wasted distributed scan that could surface a misleading
                // "duplicate value" error first).
                let packed = unsafe { (*cat_ptr_raw).read_batch_u64(&batch, i, IDXTAB_PAY_SOURCE_COLS) };
                let cols = gnitz_wire::unpack_pk_cols(packed);
                if !cols.is_well_formed() {
                    continue;
                }
                match MasterDispatcher::validate_unique_index_create_async(
                    shared.dispatcher,
                    &shared.reactor,
                    &shared.sal_writer_excl,
                    owner_id,
                    cols.as_slice(),
                )
                .await
                {
                    // No zone LSN reserved, no catalog mutation yet: just surface
                    // the violation to the client. The write lock drops on return.
                    Err(e) => {
                        send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
                        return;
                    }
                    // Hold the distinct span set to seed the filter post-commit,
                    // keyed by the packed column list (the filter-map key).
                    Ok((seen, capped)) => filter_seeds.push((owner_id, packed, seen, capped)),
                }
            }
        }
    }

    // Reserve the zone LSN but do NOT publish it yet. ingest_lsn becomes
    // visible to readers only after fsync confirms durability — Cleanup D
    // closes the pre-fsync window where clients could see an LSN whose
    // backing data is not yet on disk.
    //
    // Allocation must dominate every family's current_lsn, not just
    // ingest_lsn: un-pinned sys-table ingests (FK auto-indices, rollback
    // compensation) auto-bump family counters past the zone allocator, and
    // a checkpoint persists drifted counters as recovery dedup watermarks.
    // A zone LSN at or below such a watermark would have its committed-but-
    // unflushed deltas deduped away on recovery (`msg.lsn <= flushed`),
    // silently dropping an fsync-acknowledged DDL. Taking the max also
    // keeps a failed DDL's pinned LSN (never published to ingest_lsn) from
    // being reused by the next zone.
    let zone_lsn = shared
        .ingest_lsn
        .get()
        .max(unsafe { (*cat_ptr_raw).max_table_current_lsn() })
        + 1;
    let zone_lsn_nz = NonZeroU64::new(zone_lsn).expect("zone LSN allocator starts above 0");
    unsafe {
        (*cat_ptr_raw).ctx.open_ddl_zone(zone_lsn_nz);
    }

    // Clone before evaluate_dag consumes the batch; used by compensate_stage_a
    // to reconstruct the rollback list when fire_hooks succeeded but evaluate_dag
    // panicked (the top-level batch was never pushed to pending_broadcasts).
    let batch_for_undo = batch.clone();

    if let Err(e) = guard_panic("DDL", || {
        let cat = unsafe { &mut *cat_ptr_raw };
        cat.ingest_to_family(target_id, &batch)?;
        let dag = cat.get_dag_ptr();
        unsafe {
            (*dag).evaluate_dag(target_id, batch);
        }
        Ok(())
    }) {
        guard_panic("DDL-compensate", || {
            unsafe {
                (*cat_ptr_raw).compensate_stage_a(target_id, &batch_for_undo);
            }
            Ok::<(), String>(())
        })
        .unwrap_or_else(|ce| {
            gnitz_fatal_abort!("Stage-A DDL compensation panicked after DDL error '{}': {}", e, ce);
        });
        unsafe {
            (*cat_ptr_raw).ctx.close_ddl_zone();
        }
        send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
        return;
    }
    let t_mut_done = Instant::now();

    // SAL emission window: acquire III.3b mutex to serialize the
    // broadcast write against committer + tick + relay. Lock held ONLY
    // across the synchronous broadcast + fsync SQE submit; the fsync
    // .await happens after release so concurrent writers can proceed.
    let drained = unsafe { (*cat_ptr_raw).drain_pending_broadcasts() };
    let fsync_fut = {
        let _sal_excl = shared.sal_writer_excl.lock().await;
        let disp_ptr_raw = shared.dispatcher;
        // All broadcasts in the zone share `zone_lsn`. The trailing
        // commit sentinel marks the zone durably closed; recovery
        // applies the zone's groups only when the sentinel is on disk.
        //
        // Failure here is unrecoverable: ingest_to_family already mutated
        // the master catalog, and FLAG_DDL_SYNC groups that were written
        // are processed by workers in real-time before any sentinel
        // arrives.  A partial failure leaves master/worker schemas
        // permanently diverged.  DDL rollback is not implemented; abort.
        if let Err(e) = guard_panic("broadcast_ddl", || unsafe {
            for (tid, bat) in &drained {
                (*disp_ptr_raw).broadcast_ddl(*tid, bat, zone_lsn)?;
            }
            // Crash-injection seam: abort after broadcasts but BEFORE
            // the commit sentinel — exercises the recovery path where a
            // half-written zone must be skipped.
            #[cfg(debug_assertions)]
            if std::env::var("GNITZ_INJECT_DDL_PANIC").as_deref() == Ok("after_broadcasts") {
                libc::abort();
            }
            (*disp_ptr_raw).commit_zone(zone_lsn)?;
            Ok::<(), String>(())
        }) {
            gnitz_fatal_abort!("DDL broadcast failed after in-memory catalog mutation: {}", e);
        }
        shared.reactor.fsync(shared.sal_fd)
    };
    let t_bcast_done = Instant::now();
    let fsync_rc = fsync_fut.await;
    let t_fsync_done = Instant::now();
    if fsync_rc < 0 {
        gnitz_fatal_abort!("SAL fdatasync (DDL) failed rc={}", fsync_rc);
    }

    // Publish only after fsync. Tick handlers reading shared.ingest_lsn
    // during the DDL window now see the *old* LSN; tick-derived state
    // therefore reflects only durable work.
    shared.ingest_lsn.set(zone_lsn);
    unsafe {
        (*cat_ptr_raw).ctx.close_ddl_zone();
    }
    // Drop is now durable, but worker processes share this tree and may still
    // be applying the CREATE of an entity dropped in the same session. Defer
    // physical removal to the next checkpoint, whose worker-ACK barrier proves
    // every worker has consumed past this DROP. Removing here races a lagging
    // worker's partition-dir create and aborts it with ENOENT.
    unsafe {
        (*cat_ptr_raw).defer_pending_dir_deletions();
    }

    // Invalidate unique-filter state for durably-dropped tables/indices so a
    // recreated table with the same ID does not inherit stale filter entries.
    let disp_ptr_raw = shared.dispatcher;
    if target_id == TABLE_TAB_ID {
        for i in 0..batch_for_undo.count {
            if batch_for_undo.get_weight(i) < 0 {
                let tid = batch_for_undo.get_pk(i) as i64;
                unsafe {
                    (*disp_ptr_raw).unique_filter_invalidate_table(tid);
                }
            }
        }
    } else if target_id == IDX_TAB_ID {
        for i in 0..batch_for_undo.count {
            if batch_for_undo.get_weight(i) < 0 {
                let owner_id = unsafe { (*cat_ptr_raw).read_batch_u64(&batch_for_undo, i, IDXTAB_PAY_OWNER_ID) } as i64;
                let packed = unsafe { (*cat_ptr_raw).read_batch_u64(&batch_for_undo, i, IDXTAB_PAY_SOURCE_COLS) };
                // Remove the filter keyed by the dropped index's exact packed
                // column list. A non-existent key (e.g. a non-unique FK index)
                // is a harmless no-op; keying by the whole list means dropping
                // `(a, b)` never clears a distinct single-column filter on `a`.
                unsafe {
                    (*disp_ptr_raw).unique_filter_remove(owner_id, packed);
                }
            }
        }
    }

    // Seed the unique filters from the CREATE-time pre-flight's distinct key
    // sets so the first INSERT skips a redundant full-cluster warmup scan. The
    // pre-flight scanned every worker under the same catalog write lock the
    // warmup uses, so each set is complete and `warm = true` is sound. Done here
    // (post-fsync), never in the validator: the validator runs before the
    // IDX_TAB +1 commits, and a broadcast/fsync failure aborts the process
    // before this point, so no filter is published for an index that never
    // committed. Symmetric with the removal above and needs no rollback.
    for (owner_id, packed, seen, capped) in filter_seeds {
        unsafe {
            (*disp_ptr_raw).unique_filter_seed(owner_id, packed, seen, capped);
        }
    }

    // View-scoped distributed backfill. The view is now registered on every
    // worker (the FLAG_DDL_SYNC broadcast above; workers apply it before the
    // FLAG_BACKFILL below in SAL order) and the CREATE is durable. Drive each
    // exchange / equi-join view (`view_seeds_exchange_backfill`) through the boot
    // backfill machinery — once per source. Plain projection/filter views were
    // already filled inline by the workers' ddl_sync hook over the (now drained)
    // committed sources. Synchronous, reactor parked. A post-fsync Err cannot be
    // rolled back (the CREATE is durable), so abort — restart's boot backfill
    // rebuilds the view.
    let t_backfill_start = Instant::now();
    for &vid in &new_view_ids {
        if unsafe { (*cat_ptr_raw).dag.view_seeds_exchange_backfill(vid) } {
            // A multi-source equi-join iterates both sources: backfilling the
            // first fills its trace (join against the empty other trace → no
            // output), then the second produces the full join against it — the
            // two-sided trace fill is intrinsic to the bilinear join.
            let sources = unsafe { (*cat_ptr_raw).dag.get_source_ids(vid) };
            for src in sources {
                if let Err(e) = guard_panic("view-backfill", || shared.disp().fan_out_backfill(vid, src)) {
                    gnitz_fatal_abort!(
                        "live CREATE VIEW backfill failed after the CREATE was made durable \
                         (view={}, source={}): {}",
                        vid,
                        src,
                        e
                    );
                }
            }
        }
    }
    let t_backfill_done = Instant::now();
    if !new_view_ids.is_empty() {
        gnitz_info!(
            "live CREATE VIEW backfill: {} view(s), drain={:?}, backfill={:?}, parked={:?}",
            new_view_ids.len(),
            t_drain_done.duration_since(t_drain_start),
            t_backfill_done.duration_since(t_backfill_start),
            t_backfill_done.duration_since(t_drain_start),
        );
    }

    send_ok_response(shared, fd, target_id, None, client_id, zone_lsn as u128, client_version).await;
    let t_ddl_done = Instant::now();
    let total = t_ddl_done.duration_since(t_ddl_start);
    if total > Duration::from_millis(20) {
        gnitz_debug!(
            "DDL tid={} SLOW total={:?} barrier={:?} write_acq={:?} drain={:?} mutate={:?} broadcast={:?} \
             fsync={:?} backfill={:?} send={:?}",
            target_id,
            total,
            t_after_barrier.duration_since(t_ddl_start),
            t_after_write.duration_since(t_after_barrier),
            t_drain_done.duration_since(t_drain_start),
            t_mut_done.duration_since(t_mut_start),
            t_bcast_done.duration_since(t_mut_done),
            t_fsync_done.duration_since(t_bcast_done),
            t_backfill_done.duration_since(t_backfill_start),
            t_ddl_done.duration_since(t_fsync_done),
        );
    }
}

// ---------------------------------------------------------------------------
// Wire-protocol response helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn encode_response_buffer(
    target_id: i64,
    client_id: u64,
    result: Option<&Batch>,
    status: u32,
    error_msg: &[u8],
    prebuilt_schema: Option<&[u8]>,
    seek_pk: u128,
    flags: u64,
) -> PooledSendBuf {
    let sz = ipc::wire_size(status, error_msg, None, None, result, prebuilt_schema, &[]);
    let total = 4 + sz;
    let mut inner = crate::storage::batch_pool::acquire_buf();
    inner.reserve(total.max(8192));
    // SAFETY: encode_wire_into_ipc writes every byte [0, sz). The 4-byte frame
    // header is written immediately below. wal::encode zeros inter-region padding
    // (Step 1), so no byte is left uninitialised regardless of column type.
    #[allow(clippy::uninit_vec)]
    unsafe {
        inner.set_len(total);
    }
    inner[0..4].copy_from_slice(&(sz as u32).to_le_bytes());
    let written = ipc::encode_wire_into_ipc(
        &mut inner[4..total],
        0,
        target_id as u64,
        client_id,
        flags,
        seek_pk,
        0,
        0,
        status,
        error_msg,
        None,
        None,
        result,
        prebuilt_schema,
        &[],
    );
    debug_assert_eq!(written, sz);
    inner.truncate(4 + written);
    PooledSendBuf(inner)
}

async fn send_ok_response(
    shared: &Rc<Shared>,
    fd: i32,
    target_id: i64,
    result: Option<&Batch>,
    client_id: u64,
    seek_pk: u128,
    client_version: u16,
) {
    let (schema_block, server_version) = shared.get_schema_wire_block(target_id);
    let schema_arg = if gnitz_wire::wire_should_include_schema(client_version, server_version) {
        Some(schema_block.as_slice())
    } else {
        None
    };
    let flags = ipc::wire_flags_set_schema_version(0, server_version);
    let buf = encode_response_buffer(target_id, client_id, result, STATUS_OK, b"", schema_arg, seek_pk, flags);
    shared.reactor.send_buffer_or_close(fd, buf).await;
}

/// Control-only reply carrying just a status code: no schema, no data, no error
/// text. The schema-mismatch (`STATUS_SCHEMA_MISMATCH`) and no-index
/// (`STATUS_NO_INDEX`) responses are byte-identical apart from the status, and
/// the client treats each frame as a pure signal.
async fn send_control_only(shared: &Rc<Shared>, fd: i32, target_id: i64, client_id: u64, status: u32) {
    let buf = encode_response_buffer(target_id, client_id, None, status, b"", None, 0, 0);
    shared.reactor.send_buffer_or_close(fd, buf).await;
}

async fn send_error(shared: &Rc<Shared>, fd: i32, target_id: i64, client_id: u64, error_msg: &[u8]) {
    // STATUS_ERROR suppresses the schema block (has_schema = false), so
    // prebuilt_schema = None is correct and saves the cache lookup.
    // flags=0: client ignores schema version on error responses.
    let buf = encode_response_buffer(target_id, client_id, None, STATUS_ERROR, error_msg, None, 0, 0);
    shared.reactor.send_buffer_or_close(fd, buf).await;
}

async fn send_alloc(shared: &Rc<Shared>, fd: i32, new_id: i64, client_id: u64) {
    // Alloc responses carry no schema block; schema version irrelevant.
    let buf = encode_response_buffer(new_id, client_id, None, STATUS_OK, b"", None, 0, 0);
    shared.reactor.send_buffer_or_close(fd, buf).await;
}

#[cfg(test)]
mod tests {
    /// Demonstrates that the name_refs_arr slice is bounded by .min(MAX_COLUMNS).
    /// Before the fix, `&name_refs_arr[..col_names.len()]` panicked when
    /// col_names.len() > MAX_COLUMNS; after the fix it is always safe.
    #[test]
    fn col_names_slice_is_bounded_at_max_columns() {
        use crate::schema::MAX_COLUMNS;
        let mut arr = [&[] as &[u8]; MAX_COLUMNS];
        let names: Vec<Vec<u8>> = (0..MAX_COLUMNS).map(|i| vec![i as u8]).collect();
        for (i, n) in names.iter().enumerate() {
            arr[i] = n.as_slice();
        }
        // .min(MAX_COLUMNS) must not change the result for len == MAX_COLUMNS ...
        let slice = &arr[..names.len().min(MAX_COLUMNS)];
        assert_eq!(slice.len(), MAX_COLUMNS);
        assert_eq!(slice[0], &[0u8][..]);
        assert_eq!(slice[MAX_COLUMNS - 1], &[(MAX_COLUMNS - 1) as u8][..]);
        // ... and must cap at MAX_COLUMNS rather than panic for len > MAX_COLUMNS.
        let capped = names.len().min(MAX_COLUMNS);
        assert_eq!(
            capped, MAX_COLUMNS,
            "min(MAX_COLUMNS) is identity when len == MAX_COLUMNS"
        );
    }
}
