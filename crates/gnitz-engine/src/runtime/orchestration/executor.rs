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

use super::guard_panic;
use crate::foundation::posix_io;
use crate::runtime::tls::{ConnCountGuard, TlsShared};

use crate::catalog::{
    CatalogEngine, FIRST_USER_TABLE_ID, IDXTAB_PAY_IS_UNIQUE, IDXTAB_PAY_OWNER_ID, IDXTAB_PAY_SOURCE_COLS, IDX_TAB_ID,
    SEQ_TAB_ID, TABLE_TAB_ID, VIEW_TAB_ID,
};
use crate::runtime::committer::{self, BarrierKind, CommitRequest, PendingTxn};
use crate::runtime::lsn::ZoneLsnAllocator;
use crate::runtime::master::{first_worker_error_opt, MasterDispatcher, TxnFamily};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{
    join_into, mpsc, oneshot, select2, AsyncMutex, AsyncRwLock, Either, FsyncFuture, PendingRelay, Reactor, ReplyFuture,
};
use crate::runtime::wire::{
    self as ipc, SchemaWithVersion, FLAG_GET_INDICES, STATUS_ERROR, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH,
};
use crate::schema::{index_meta_schema_desc, validate_schema_match, SchemaDescriptor, INDEX_META_COL_NAMES};
use crate::storage::{Batch, BatchBuilder};

pub(crate) const TICK_COALESCE_ROWS: usize = 10_000;
const TICK_DEADLINE_MS: u64 = 20;
const WORKER_WATCH_MS: u64 = 100;

use gnitz_wire::{
    FLAG_ALLOCATE_INDEX_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_TABLE_ID, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
};

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
    /// parks the reactor; see `handle_ddl_txn`.
    Quiesce {
        acked: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
}

/// Releases the tick-subsystem quiesce gate when dropped, so a CREATE-VIEW
/// stop-the-world window ends on every exit path of `handle_ddl_txn`
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
    /// Zone-LSN allocation high-water + durability watermark, shared with the
    /// committer so SCAN/SEEK handlers report the same LSN it assigns.
    lsn_alloc: Rc<ZoneLsnAllocator>,
    last_tick_lsn: Rc<Cell<u64>>,
    /// Per-table row counter feeding the tick threshold.
    tick_rows: Rc<RefCell<FxHashMap<i64, usize>>>,
    tick_tids: Rc<RefCell<Vec<i64>>>,
    table_locks: RefCell<FxHashMap<i64, Rc<AsyncMutex<()>>>>,
    /// Set true by the graceful-shutdown watcher before it sends the final
    /// Shutdown barrier, so `handle_message`'s push path rejects new pushes
    /// — none may commit after the final checkpoint's view flush.
    draining: Rc<Cell<bool>>,
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
        let schema = cat
            .get_schema_desc(target_id)
            .unwrap_or_else(SchemaDescriptor::minimal_u64);
        let e = ipc::get_or_build_schema_wire_block(cat, target_id, &schema);
        (e.block, e.version)
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
    }
}

// ---------------------------------------------------------------------------
// ServerExecutor entry point
// ---------------------------------------------------------------------------

pub struct ServerExecutor;

impl ServerExecutor {
    /// `tls` is the optional TLS listener bootstrap from `server_main`:
    /// the bound TCP listen fd, the rustls server configuration, and the
    /// global live-connection cap.
    pub fn run(
        catalog: *mut CatalogEngine,
        dispatcher: *mut MasterDispatcher,
        server_fd: i32,
        tls: Option<TlsListener>,
    ) -> i32 {
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
        reactor.attach_listener(server_fd);
        if let Some(tl) = &tls {
            reactor.attach_listener(tl.fd);
        }
        // Reactor-thread live-TLS-connection counter, incremented by an RAII
        // guard stored in each session's `TlsShared` and decremented on its
        // drop. Single-threaded, so no atomics.
        let tls_conn_count = Rc::new(Cell::new(0u32));
        let accept_ctx = AcceptCtx {
            unix_fd: server_fd,
            tls,
            tls_conn_count,
        };

        let sal_writer_excl = Rc::new(AsyncMutex::new(()));
        // Seed the zone-LSN allocator above every table's current_lsn so each
        // new zone LSN is strictly greater, keeping `ingest_to_family`'s direct
        // current_lsn assignment monotonic across restarts.
        let initial_lsn = unsafe { &*catalog }.max_table_current_lsn();
        let lsn_alloc = Rc::new(ZoneLsnAllocator::new(initial_lsn));
        let last_tick_lsn = Rc::new(Cell::new(initial_lsn));
        let tick_rows: Rc<RefCell<FxHashMap<i64, usize>>> = Rc::new(RefCell::new(FxHashMap::default()));
        let tick_tids: Rc<RefCell<Vec<i64>>> = Rc::new(RefCell::new(Vec::new()));

        let (committer_tx, committer_rx) = mpsc::unbounded::<CommitRequest>();
        let (tick_tx, tick_rx) = mpsc::unbounded::<TickTrigger>();
        let (relay_tx, relay_rx) = mpsc::unbounded::<PendingRelay>();
        // Wire the relay channel into the reactor so route_reply's
        // FLAG_EXCHANGE accumulator can hand off completed views.
        reactor.attach_relay_tx(relay_tx);

        // Graceful-shutdown push gate (reactor-thread-only).
        let draining = Rc::new(Cell::new(false));

        let committer_shared = Rc::new(committer::Shared {
            reactor: Rc::clone(&reactor),
            disp_ptr: dispatcher,
            sal_fd,
            sal_writer_excl: Rc::clone(&sal_writer_excl),
            lsn_alloc: Rc::clone(&lsn_alloc),
            num_workers,
            force_checkpoint: Cell::new(false),
            tick_rows: Rc::clone(&tick_rows),
            tick_tids: Rc::clone(&tick_tids),
            tick_tx: tick_tx.clone(),
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
            lsn_alloc: Rc::clone(&lsn_alloc),
            last_tick_lsn: Rc::clone(&last_tick_lsn),
            tick_rows: Rc::clone(&tick_rows),
            tick_tids: Rc::clone(&tick_tids),
            table_locks: RefCell::new(FxHashMap::default()),
            draining: Rc::clone(&draining),
        });

        // Catch SIGTERM/SIGINT so the watchdog can drive a final checkpoint
        // before exiting.
        install_shutdown_signal_handlers();

        reactor.spawn(committer::run(committer_rx, committer_shared));
        reactor.spawn(accept_loop(Rc::clone(&shared), accept_ctx));
        reactor.spawn(tick_loop_async(Rc::clone(&shared), tick_rx));
        reactor.spawn(relay_loop(Rc::clone(&shared), relay_rx));
        reactor.spawn(watchdog(Rc::clone(&shared)));

        reactor.block_until_shutdown();
        0
    }
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

/// TLS listener runtime inputs, produced by `bootstrap::setup_tls_listener`
/// and threaded into `ServerExecutor::run` (hence `pub(crate)`): the bound
/// listen fd, the rustls config, and the global live-connection cap.
pub(crate) struct TlsListener {
    pub fd: i32,
    pub cfg: std::sync::Arc<rustls::ServerConfig>,
    pub max_conns: u32,
}

/// Accept-routing inputs: which listener fd is which, the TLS listener, and
/// the reactor-thread live-TLS-connection counter. Carried explicitly — the
/// reactor no longer records a listener fd (the udata round-trip replaced it).
struct AcceptCtx {
    unix_fd: i32,
    tls: Option<TlsListener>,
    tls_conn_count: Rc<Cell<u32>>,
}

async fn accept_loop(shared: Rc<Shared>, ctx: AcceptCtx) {
    loop {
        let (fd, listener) = shared.reactor.accept().await;
        if fd < 0 {
            continue;
        }
        if listener == ctx.unix_fd {
            shared.reactor.register_conn(fd);
            let peer = Peer::unix(fd, Rc::clone(&shared.reactor));
            let s = Rc::clone(&shared);
            // AF_UNIX (loopback) has no pre-auth deadline: the mature
            // local path is behaviourally unchanged.
            shared.reactor.spawn(connection_loop(peer, s, None));
            continue;
        }
        match &ctx.tls {
            Some(tl) if listener == tl.fd => {
                // Global connection cap: close the freshly-accepted fd before
                // any TLS work when the live count is at the cap. No TOCTOU —
                // on the single-threaded reactor there is no `.await` between
                // this check and `ConnCountGuard::new`, only synchronous
                // socket-option/`start` calls, so the count cannot go stale.
                if ctx.tls_conn_count.get() >= tl.max_conns {
                    gnitz_warn!("tls: connection cap {} reached; closing fd={fd}", tl.max_conns);
                    // SAFETY: freshly-accepted fd we own; no SQE references it.
                    unsafe { libc::close(fd) };
                    continue;
                }
                posix_io::set_nodelay(fd);
                posix_io::set_keepalive(fd);
                let guard = ConnCountGuard::new(Rc::clone(&ctx.tls_conn_count));
                match TlsShared::start(Rc::clone(&shared.reactor), fd, std::sync::Arc::clone(&tl.cfg), guard) {
                    Ok(conn) => {
                        let peer = Peer::tls(conn);
                        let s = Rc::clone(&shared);
                        // Pre-auth first-frame deadline: HELLO must arrive
                        // within this window of accept, else the connection
                        // is torn down (covers a stalled handshake and a
                        // completed-handshake-no-HELLO squat alike).
                        let deadline = Instant::now() + tls_hello_timeout();
                        shared.reactor.spawn(connection_loop(peer, s, Some(deadline)));
                    }
                    Err(e) => {
                        // `guard` was moved into `start`; on the error path it
                        // already dropped (decrementing) inside `start`'s frame.
                        gnitz_warn!("tls: session init failed for fd={fd}: {e}");
                        // SAFETY: freshly-accepted fd we own; no SQE references it.
                        unsafe { libc::close(fd) };
                    }
                }
            }
            _ => {
                gnitz_warn!("accept from unknown listener fd={listener}; closing conn fd={fd}");
                // SAFETY: freshly-accepted fd we own; no SQE references it.
                unsafe { libc::close(fd) };
            }
        }
    }
}

/// Pre-auth first-frame deadline (`GNITZ_TLS_HELLO_TIMEOUT_MS`, default
/// 15 000 ms). Comfortably exceeds the client's ~10 s post-connect
/// handshake+HELLO budget, so legitimate slow-link clients are not reaped.
fn tls_hello_timeout() -> std::time::Duration {
    static T: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *T.get_or_init(|| {
        std::time::Duration::from_millis(
            std::env::var("GNITZ_TLS_HELLO_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(15_000),
        )
    })
}

enum HelloOutcome {
    /// Connection accepted.
    Pass,
    /// Caller must close the connection.
    Reject,
}

/// `first_frame_deadline` bounds the arrival of the first (HELLO) frame:
/// `Some` for TLS (pre-auth reap), `None` for AF_UNIX (unchanged). Only the
/// first recv is raced against the deadline; everything after HELLO uses a
/// plain `peer.recv().await`.
async fn connection_loop(peer: Peer, shared: Rc<Shared>, first_frame_deadline: Option<Instant>) {
    // No HELLO in time (`Either::B`) → `None`, funnelling into the single close
    // site below. `select2` drops the losing recv (clears its waker) and the
    // losing timer (cancels its SQE), so the happy path leaves no timer behind.
    let first = match first_frame_deadline {
        Some(deadline) => match select2(peer.recv(), shared.reactor.timer(deadline)).await {
            Either::A(opt) => opt,
            Either::B(()) => None,
        },
        None => peer.recv().await,
    };
    let Some(buf) = first else {
        peer.close();
        return;
    };
    if let HelloOutcome::Reject = run_hello_handshake(&peer, buf.as_slice()).await {
        peer.close();
        return;
    }

    loop {
        let Some(buf) = peer.recv().await else { break };
        handle_message(&peer, buf.as_slice(), &shared).await;
    }
    peer.close();
}

/// Validate a HELLO frame, elevate the connection's payload limit, and
/// reply with the symmetric ACK. See `Reactor::set_max_payload_len` for
/// why the limit must be raised before any `.await` here.
async fn run_hello_handshake(peer: &Peer, data: &[u8]) -> HelloOutcome {
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
        send_error(peer, 0, 0, msg.as_bytes()).await;
        return HelloOutcome::Reject;
    }

    peer.set_max_payload_len(gnitz_wire::MAX_FRAME_PAYLOAD_SERVER);

    let rc = peer.send_hello_ack().await;
    if rc < 0 {
        return HelloOutcome::Reject;
    }
    HelloOutcome::Pass
}

// ---------------------------------------------------------------------------
// Watchdog: worker crashes + graceful shutdown (SIGTERM / SIGINT)
// ---------------------------------------------------------------------------

/// Set by the SIGTERM/SIGINT handler; polled by `watchdog`. A plain
/// `AtomicBool` store is async-signal-safe (unlike touching the reactor-thread
/// `Cell` flags), so the handler does nothing but flip this.
static SHUTDOWN_REQUESTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

extern "C" fn handle_shutdown_signal(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Install async-signal-safe handlers for SIGTERM and SIGINT. The handler only
/// flips `SHUTDOWN_REQUESTED`; all real work happens on the reactor thread in
/// `watchdog`. `SA_RESTART` lets an interrupted `io_uring_enter` restart
/// itself, so the signal never surfaces an EINTR error to the reactor — the
/// watchdog's 100 ms timer picks up the flag.
fn install_shutdown_signal_handlers() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handle_shutdown_signal as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = libc::SA_RESTART;
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
    }
}

/// One 100 ms reactor-timer poll loop with two terminal duties: worker-crash
/// detection (broadcast FLAG_SHUTDOWN, stop the reactor) and graceful shutdown
/// on SIGTERM/SIGINT — stop admitting pushes, run one final full checkpoint
/// through the committer (drain + persist while the reactor is still live),
/// then broadcast FLAG_SHUTDOWN and request reactor shutdown so `server_main`
/// exits cleanly. A signalfd fd-await would need new reactor machinery; the
/// timer poll is the established pattern.
async fn watchdog(shared: Rc<Shared>) {
    loop {
        shared
            .reactor
            .timer(Instant::now() + Duration::from_millis(WORKER_WATCH_MS))
            .await;

        if SHUTDOWN_REQUESTED.load(std::sync::atomic::Ordering::Relaxed) {
            gnitz_info!("shutdown signal received; draining, checkpointing, and stopping");

            // 1. Stop admitting new pushes (none may commit after the final
            //    flush).
            shared.draining.set(true);

            // 2. One final full checkpoint through the committer. The Shutdown
            //    barrier forces the whole sequence and is deferred to its end,
            //    so `done` resolves only after the base + drain + ephemeral
            //    rounds complete. A just-pushed delta may still sit in
            //    `pending_deltas` (the tick-coalesce window not yet fired), so
            //    the drain inside the sequence is load-bearing.
            let (tx, rx) = oneshot::channel::<()>();
            shared.committer_tx.send(CommitRequest::Barrier {
                kind: BarrierKind::Shutdown,
                done: tx,
            });
            let _ = rx.await;

            // 3. Workers flush + _exit, then stop the reactor
            //    (block_until_shutdown returns and server_main exits 0). The
            //    reactor/W2M receiver stays live throughout, so no `w2m()`
            //    handle dangles.
            shared.disp().shutdown_workers();
            shared.reactor.request_shutdown();
            return;
        }

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
    // Snapshot before any .await: a concurrent push can advance the published LSN
    // while we wait for tick ACKs, and setting last_tick_lsn to that
    // higher value would report an LSN that this tick never processed.
    let snapshot_lsn = shared.lsn_alloc.published();

    // Allocate every (tid, worker) req_id up front so the SAL emission
    // sequence is fully prepared before we take any locks.
    req_ids.clear();
    req_ids.extend((0..tids.len() * nw).map(|_| shared.reactor.alloc_request_id()));

    let _cat_read = shared.catalog_rwlock.read().await;
    let _sal_excl = shared.sal_writer_excl.lock().await;

    let emit_err = guard_panic("tick", || unsafe {
        let disp = &mut *shared.dispatcher;
        for (i, &tid) in tids.iter().enumerate() {
            disp.write_tick_group(tid, &req_ids[i * nw..(i + 1) * nw])?;
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
            shared.committer_tx.send(CommitRequest::Barrier {
                kind: BarrierKind::Reclaim,
                done: tx,
            });
            let _ = rx_done.await;
            reclaimed = true;
        }
    }
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

async fn handle_message(peer: &Peer, data: &[u8], shared: &Rc<Shared>) {
    // ONE control-block parse for the whole request: the schema-hint decision
    // and the full decode below both read this same parse, so a malicious
    // client cannot forge a directory that points one at one region and the
    // other at another.
    let ctrl = match ipc::peek_client_control(data) {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("decode error: {e}");
            send_error(peer, 0, 0, msg.as_bytes()).await;
            return;
        }
    };

    // Decode the frame. Schema-less PUSH frames (warm-cache path) have
    // FLAG_HAS_DATA but not FLAG_HAS_SCHEMA; they need a catalog hint.
    let decoded = {
        let has_schema = (ctrl.flags & ipc::FLAG_HAS_SCHEMA) != 0;
        let has_data = (ctrl.flags & ipc::FLAG_HAS_DATA) != 0;
        let (ctrl_target_id, ctrl_client_id) = (ctrl.target_id as i64, ctrl.client_id);
        if has_data && !has_schema {
            let client_version = ipc::wire_flags_get_schema_version(ctrl.flags);
            if client_version == 0 {
                send_error(
                    peer,
                    ctrl_target_id,
                    ctrl_client_id,
                    b"FLAG_HAS_DATA without FLAG_HAS_SCHEMA",
                )
                .await;
                return;
            }
            let server_version = shared.cat().get_schema_version(ctrl_target_id);
            if client_version != server_version {
                send_control_only(peer, ctrl_target_id, ctrl_client_id, STATUS_SCHEMA_MISMATCH).await;
                return;
            }
            let catalog_schema = shared.get_schema_desc(ctrl_target_id);
            let hint = SchemaWithVersion {
                descriptor: &catalog_schema,
                version: server_version,
            };
            match decode_client_wire(data, ctrl, Some(hint)) {
                Ok(d) => d,
                Err(e) => {
                    let msg = format!("decode error: {e}");
                    send_error(peer, ctrl_target_id, ctrl_client_id, msg.as_bytes()).await;
                    return;
                }
            }
        } else {
            match decode_client_wire(data, ctrl, None) {
                Ok(d) => d,
                Err(e) => {
                    let msg = format!("decode error: {e}");
                    send_error(peer, 0, 0, msg.as_bytes()).await;
                    return;
                }
            }
        }
    };

    let client_id = decoded.control.client_id;
    let target_id = decoded.control.target_id as i64;
    let flags = decoded.control.flags;
    let client_version = ipc::wire_flags_get_schema_version(flags);

    // ---------- Atomic DDL transaction (the system-write frame) ----------
    // Every system-table write — a CREATE's N family batches or a
    // DROP/CREATE INDEX/CREATE SCHEMA's single batch — arrives as one
    // FLAG_DDL_TXN frame and is ingested under one durable SAL zone. It shares
    // the `target_id == 0` sentinel with the alloc RPCs but carries a disjoint
    // flag, so branch here before the alloc block. `handle_ddl_txn` re-decodes the
    // bundle from the raw frame (the generic `decode_wire` above sees no data
    // block and yields control-only, which is unused for this route).
    if flags & gnitz_wire::FLAG_DDL_TXN != 0 {
        handle_ddl_txn(shared, peer, client_id, data, client_version).await;
        return;
    }

    // ---------- Atomic user-table transaction ----------
    // Placed after FLAG_DDL_TXN and before the `target_id == 0` alloc block so it
    // cannot collide with alloc RPCs or empty-batch scans; it carries
    // `target_id = 0`. Like DDL_TXN it re-decodes the bundle from the raw frame.
    if flags & gnitz_wire::FLAG_PUSH_TXN != 0 {
        handle_push_txn(shared, peer, client_id, data).await;
        return;
    }

    // ---------- SERIAL range reservation ----------
    // Carries `target_id = seq_id (= table_id) ≠ 0` and the range `count` in
    // `seek_col_idx`, so it precedes the `target_id == 0` catalog-id block.
    if flags & gnitz_wire::FLAG_ALLOCATE_SERIAL_RANGE != 0 {
        let seq_id = target_id; // = table_id
        let count = decoded.control.seek_col_idx.max(1) as i64;
        let base = commit_serial_range_durable(shared, seq_id, count).await;
        send_alloc(peer, base, client_id).await;
        return;
    }

    // ---------- ID allocations ----------
    if target_id == 0 {
        let alloc = if flags & FLAG_ALLOCATE_TABLE_ID != 0 {
            Some(shared.cat().allocate_table_id())
        } else if flags & FLAG_ALLOCATE_SCHEMA_ID != 0 {
            Some(shared.cat().allocate_schema_id())
        } else if flags & FLAG_ALLOCATE_INDEX_ID != 0 {
            Some(shared.cat().allocate_index_id())
        } else {
            None
        };
        if let Some(new_id) = alloc {
            send_alloc(peer, new_id, client_id).await;
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
            peer,
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
            peer,
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
            peer,
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
            peer,
            client_id,
            target_id,
            ipc::wire_flags_get_index_version(flags),
        )
        .await;
        return;
    }

    // ---------- Empty push ----------
    // A push of an empty batch is a legitimate empty Z-set delta: it commits
    // nothing, so ACK immediately with the "nothing written" LSN 0 (trivially
    // satisfied by any later freshness check). FLAG_PUSH is what separates it
    // from the data-less scan request below — without the flag an empty push
    // would be routed to handle_scan, whose streamed table dump desyncs push
    // reply readers (they read exactly one frame).
    if flags & gnitz_wire::FLAG_PUSH != 0 && (!has_batch || batch_count == 0) {
        // Same existence + writability gate as the INSERT path below; system
        // tids are fixed and always present, so only user tables are probed.
        // An empty push commits nothing, but a view target is rejected here
        // too so a client bug that happens to produce an empty batch (e.g.
        // `delete` with an empty pk list) fails the same way a non-empty one
        // does instead of being masked by a no-op ACK.
        if target_id >= FIRST_USER_TABLE_ID {
            let _cat = shared.catalog_rwlock.read().await;
            if push_target_rejected(shared, peer, target_id, client_id).await {
                return;
            }
        }
        send_ok_response(shared, peer, target_id, None, client_id, 0, client_version).await;
        return;
    }

    if target_id >= FIRST_USER_TABLE_ID && (!has_batch || batch_count == 0) {
        handle_scan(shared, peer, client_id, target_id, client_version).await;
        return;
    }

    // ---------- Schema validation on incoming data ----------
    // Only when the client actually shipped a schema block (cold push): on
    // the warm schema-less path the decode hint substituted the catalog's
    // own descriptor, so `decoded.schema` would just validate the catalog
    // schema against itself — pure overhead on every warm INSERT.
    if has_batch && flags & ipc::FLAG_HAS_SCHEMA != 0 {
        if let Some(ref wire_schema) = decoded.schema {
            let expected = shared.get_schema_desc(target_id);
            if let Err(e) = validate_schema_match(wire_schema, &expected) {
                send_error(peer, target_id, client_id, e.as_bytes()).await;
                return;
            }
        }
    }

    // ---------- User-table INSERT ----------
    if target_id >= FIRST_USER_TABLE_ID && has_batch && batch_count > 0 {
        let mode = ipc::wire_flags_get_conflict_mode(flags);
        let batch = decoded.data_batch.unwrap();

        let _cat = shared.catalog_rwlock.read().await;
        if push_target_rejected(shared, peer, target_id, client_id).await {
            return;
        }
        // Acquire all FK-related table locks in ascending tid order.
        // Sorted acquisition prevents deadlock between concurrent child
        // INSERT and parent DELETE: both attempt the same ordered set.
        let lock_set = shared.cat().fk_lock_set(target_id);
        let mut _tlocks = Vec::with_capacity(lock_set.len());
        for &tid in lock_set {
            _tlocks.push(shared.table_lock(tid).lock().await);
        }

        // Local (catalog-resident) unique-index validation. Wrapped per V.4
        // so a malformed batch can't crash the server.
        let cat_ptr_raw = shared.catalog;
        if let Err(e) = guard_panic("validate", || unsafe {
            (*cat_ptr_raw).validate_unique_indices(target_id, &batch)
        }) {
            send_error(peer, target_id, client_id, e.as_bytes()).await;
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
            send_error(peer, target_id, client_id, e.as_bytes()).await;
            return;
        }

        // Graceful shutdown in flight: reject so no push commits after the final
        // checkpoint's view flush. The client sees a clean error and can retry
        // against the restarted server.
        if shared.draining.get() {
            send_error(peer, target_id, client_id, b"server shutting down").await;
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
                send_ok_response(shared, peer, target_id, None, client_id, lsn as u128, client_version).await;
            }
            Ok(Err(e)) => {
                send_error(peer, target_id, client_id, e.as_bytes()).await;
            }
            Err(_) => {
                send_error(peer, target_id, client_id, b"committer shut down").await;
            }
        }
        return;
    }

    // ---------- System-table DML (catalog + optional DDL broadcast) ----------
    if target_id < FIRST_USER_TABLE_ID {
        handle_system_scan(shared, peer, client_id, target_id, decoded, client_version).await;
    }

    // Fallthrough: ignore (should not happen).
}

async fn handle_seek(
    shared: &Rc<Shared>,
    peer: &Peer,
    client_id: u64,
    target_id: i64,
    pk: u128,
    seek_pk_extra: &[u8],
    client_version: u16,
) {
    if reject_unknown_table(shared, peer, client_id, target_id).await {
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
            Ok(slot) => peer.send_slot_or_close(slot).await,
            Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
        }
    } else {
        match unsafe { (*shared.catalog).seek_family(target_id, pk, seek_pk_extra) } {
            Ok((batch, _)) => {
                send_ok_response(shared, peer, target_id, batch.as_ref(), client_id, pk, client_version).await
            }
            Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
        }
    }
}

/// Handle an atomic user-table transaction (`FLAG_PUSH_TXN`): decode + validate
/// the bundle as a unit under the union of the involved table locks, then emit
/// it as N `FLAG_PUSH` groups inside one zone under one sentinel. Mirrors the
/// plain-push arm's lock order (catalog read lock, then the per-table lock union
/// ascending) and its late `draining` check; both locks are held through the
/// committer ACK. Every rejection is pre-SAL, so an `Err` reply means "nothing
/// committed".
async fn handle_push_txn(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8]) {
    match push_txn_body(shared, data).await {
        // Standard single-frame ACK, seek_pk = zone LSN (uncorrelated, as
        // push_ddl_txn's reply is).
        Ok(lsn) => {
            let buf = encode_response_buffer(0, client_id, None, STATUS_OK, b"", None, lsn as u128, 0);
            peer.send_buffer_or_close(buf).await;
        }
        Err(e) => send_error(peer, 0, client_id, e.as_bytes()).await,
    }
}

/// The body of `handle_push_txn`: every rejection is a plain `Err`, so the one
/// caller above owns the single reply path. Returns the durable zone LSN.
async fn push_txn_body(shared: &Rc<Shared>, data: &[u8]) -> Result<u64, String> {
    // 1. Decode + frame-local shape rules (no catalog access).
    let raw = ipc::decode_push_txn(data).map_err(|e| format!("decode error: {e}"))?;
    if raw.is_empty() {
        return Err("TXN: empty family bundle".to_string());
    }

    // 2. Catalog read lock (excludes a concurrent DROP/DDL), then the
    //    catalog-dependent shape rules + per-family batch decode.
    let _cat = shared.catalog_rwlock.read().await;
    let mut families: Vec<TxnFamily> = Vec::with_capacity(raw.len());
    for fam in &raw {
        let tid = fam.tid;
        if tid < FIRST_USER_TABLE_ID {
            return Err(format!("TXN: {tid} is not a user table"));
        }
        // Same existence + writability gate the plain-push arm applies, so a view
        // target is rejected identically.
        if let Some(e) = push_target_error(shared, tid) {
            return Err(e);
        }
        if !shared.cat().table_has_unique_pk(tid) {
            return Err(format!("TXN: table {tid} is not unique_pk"));
        }
        let catalog_schema = shared.get_schema_desc(tid);
        // The schema block is always present; validate it against the catalog
        // per family (a concurrent DDL between buffer time and commit surfaces as
        // a clean error the application re-runs).
        let wire_schema = ipc::decode_schema_block(fam.schema_block, false)
            .map_err(|e| format!("TXN family {tid} schema decode error: {e}"))?;
        validate_schema_match(&wire_schema, &catalog_schema)?;
        let batch = decode_client_batch(fam.wal_block, &catalog_schema)
            .map_err(|e| format!("TXN family {tid} decode error: {e}"))?;
        if batch.count == 0 {
            return Err(format!("TXN: empty batch for table {tid}"));
        }
        families.push(TxnFamily {
            tid,
            mode: ipc::WireConflictMode::from_u8(fam.mode),
            batch,
        });
    }

    // 3. Acquire the per-table lock union ⋃ fk_lock_set(tid), sorted ascending
    //    and DEDUPED — a repeated tid would re-lock a non-reentrant mutex the
    //    same task already holds and hang forever.
    let mut union: Vec<i64> = Vec::new();
    for fam in &families {
        union.extend_from_slice(shared.cat().fk_lock_set(fam.tid));
    }
    union.sort_unstable();
    union.dedup();
    let mut _tlocks = Vec::with_capacity(union.len());
    for tid in union {
        _tlocks.push(shared.table_lock(tid).lock().await);
    }

    // 4. Distributed bundle validation (the four rules).
    MasterDispatcher::validate_txn_distributed_async(
        shared.dispatcher,
        &shared.reactor,
        &shared.sal_writer_excl,
        &families,
    )
    .await?;

    // 5. Drain check immediately before the committer send. INVARIANT: there must
    //    be NO `.await` between this check and `committer_tx.send` — on the
    //    single-threaded reactor that gap is atomic, which guarantees a
    //    transaction that observed `draining == false` enqueues before the
    //    watchdog's Shutdown barrier.
    if shared.draining.get() {
        return Err("server shutting down".to_string());
    }

    // 6. Route through the committer and wait for the zone ACK.
    let (tx, rx) = oneshot::channel::<Result<u64, String>>();
    shared
        .committer_tx
        .send(CommitRequest::Txn(PendingTxn { families, done: tx }));
    rx.await.map_err(|_| "committer shut down".to_string())?
}

/// Decode a CLIENT-supplied frame and neutralize any client-claimed batch
/// layout flags. FLAG_BATCH_SORTED / FLAG_BATCH_CONSOLIDATED assert "already
/// sorted/consolidated, skip the work" — a client must never be trusted to
/// claim that, so every client-boundary decode goes through this (or its
/// sibling `decode_client_batch`); downstream consolidation (the catalog DDL
/// ingest and the commit path) re-establishes the invariants. No-op for
/// conforming clients, which never set these.
fn decode_client_wire(
    data: &[u8],
    ctrl: ipc::DecodedControl,
    hint: Option<SchemaWithVersion<'_>>,
) -> Result<ipc::DecodedWire, &'static str> {
    let mut decoded = ipc::decode_wire_with_ctrl(data, ctrl, hint)?;
    if let Some(b) = decoded.data_batch.as_mut() {
        b.downgrade();
    }
    Ok(decoded)
}

/// `decode_client_wire`'s sibling for a raw WAL-block family batch inside a
/// client FLAG_DDL_TXN bundle: decode + neutralize the layout claim.
fn decode_client_batch(slice: &[u8], schema: &SchemaDescriptor) -> Result<Batch, &'static str> {
    let (mut b, _) = Batch::decode_from_wal_block(slice, schema, false)?;
    b.downgrade();
    Ok(b)
}

/// Reject a request addressed to an unknown table id. Returns `true` when
/// the error reply was sent and the caller must return.
async fn reject_unknown_table(shared: &Rc<Shared>, peer: &Peer, client_id: u64, target_id: i64) -> bool {
    if shared.cat().has_id(target_id) {
        return false;
    }
    let msg = format!("table {target_id} not found");
    send_error(peer, target_id, client_id, msg.as_bytes()).await;
    true
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
    peer: &Peer,
    client_id: u64,
    target_id: i64,
    seek_col_idx: u64,
    seek_pk: u128,
    seek_pk_extra: &[u8],
    client_version: u16,
) {
    if reject_unknown_table(shared, peer, client_id, target_id).await {
        return;
    }
    if target_id >= FIRST_USER_TABLE_ID {
        let cols = match validated_index_cols(shared, target_id, seek_col_idx, "seek_by_index") {
            Ok(cols) => cols,
            Err(msg) => {
                send_error(peer, target_id, client_id, msg.as_bytes()).await;
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
                send_control_only(peer, target_id, client_id, STATUS_NO_INDEX).await;
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
                Ok(slot) => peer.send_slot_or_close(slot).await,
                Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
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
                    peer,
                    target_id,
                    merged.as_ref(),
                    client_id,
                    seek_pk,
                    client_version,
                )
                .await;
            }
            Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
        }
    } else {
        // System tables never carry secondary indexes.
        let msg = format!("SEEK_BY_INDEX on system table {target_id} is not supported");
        send_error(peer, target_id, client_id, msg.as_bytes()).await;
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
    peer: &Peer,
    client_id: u64,
    target_id: i64,
    seek_col_idx: u64,
    seek_pk_extra: &[u8],
    client_version: u16,
) {
    if reject_unknown_table(shared, peer, client_id, target_id).await {
        return;
    }
    if target_id < FIRST_USER_TABLE_ID {
        let msg = format!("SEEK_BY_INDEX_RANGE on system table {target_id} is not supported");
        send_error(peer, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    let cols = match validated_index_cols(shared, target_id, seek_col_idx, "seek_by_index_range") {
        Ok(cols) => cols,
        Err(msg) => {
            send_error(peer, target_id, client_id, msg.as_bytes()).await;
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
        send_control_only(peer, target_id, client_id, STATUS_NO_INDEX).await;
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
            send_ok_response(shared, peer, target_id, merged.as_ref(), client_id, 0, client_version).await;
        }
        Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
    }
}

/// GET_INDICES: serve the client's durable, epoch-validated cache of a table's
/// secondary-index metadata — the `(col_idx, is_unique)` set, projected from the
/// DAG `index_circuits` (the system's operative truth for "is this column
/// enforced-unique", identical to the server's own FK gate `validate_fk_column`).
/// The client's cached epoch arrives in the index-version wire bits; on a match
/// we reply "unchanged" (no schema, no data), otherwise the fresh list.
async fn handle_get_indices(shared: &Rc<Shared>, peer: &Peer, client_id: u64, target_id: i64, client_epoch: u8) {
    let server_epoch = shared.cat().get_index_version(target_id); // absent ⇒ 1
    let flags = ipc::wire_flags_set_index_version(0, server_epoch);

    // Warm hit (and the missing-table race, since both resolve to epoch 1 with
    // an empty list): OK, no schema, no data → client keeps its cached Rc.
    if client_epoch == server_epoch {
        let buf = encode_response_buffer(target_id, client_id, None, STATUS_OK, b"", None, 0, flags);
        peer.send_buffer_or_close(buf).await;
        return;
    }

    // Changed / first fetch: project every index circuit (FK + non-unique
    // included) to (col_idx PK, is_unique). The response always carries its own
    // schema block on the data path, so the client decodes against the wire
    // schema and never needs — or pollutes — the per-table schema cache.
    let desc = index_meta_schema_desc();
    // Build the schema block before the descriptor is moved into BatchBuilder.
    let schema_block = ipc::build_schema_wire_block(&desc, &INDEX_META_COL_NAMES[..], 0, target_id as u32);
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
    peer.send_buffer_or_close(buf).await;
}

async fn handle_scan(shared: &Rc<Shared>, peer: &Peer, client_id: u64, target_id: i64, client_version: u16) {
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
    if reject_unknown_table(shared, peer, client_id, target_id).await {
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
        let rc = peer.send_buffer(prelim).await;
        if rc < 0 {
            peer.close();
            return;
        }
        // Embed server_version as client_version so workers omit their schema blocks.
        server_version
    } else {
        client_version
    };

    // A replicated relation single-sources from worker 0 (its full copy
    // lives on every worker; an all-worker fan-out would concatenate W
    // identical copies).
    let unicast = if replicated { 0 } else { -1 };
    let result = MasterDispatcher::fan_out_scan_async(
        shared.dispatcher,
        &shared.reactor,
        &shared.sal_writer_excl,
        unicast,
        target_id,
        client_id,
        peer,
        effective_client_version,
    )
    .await;
    match result {
        Ok(true) => {
            let terminal = make_terminal_scan_frame(target_id, client_id, lsn);
            peer.send_buffer_or_close(terminal).await;
        }
        Ok(false) => {
            peer.close();
        }
        Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
    }
}

fn make_terminal_scan_frame(target_id: i64, client_id: u64, lsn: u64) -> PooledSendBuf {
    // Terminal scan frame: no schema block, no data. Client ignores schema version here.
    encode_response_buffer(target_id, client_id, None, STATUS_OK, b"", None, lsn as u128, 0)
}

/// System-table read path: an empty-batch SCAN of a catalog family. Every
/// catalog WRITE now arrives as a `FLAG_DDL_TXN` frame (`handle_ddl_txn`), so a
/// non-empty batch on the plain system-table frame is a protocol error.
async fn handle_system_scan(
    shared: &Rc<Shared>,
    peer: &Peer,
    client_id: u64,
    target_id: i64,
    decoded: ipc::DecodedWire,
    client_version: u16,
) {
    let batch = decoded.data_batch;
    if batch.as_ref().map(|b| b.count > 0).unwrap_or(false) {
        send_error(
            peer,
            target_id,
            client_id,
            b"system-table writes must use the DDL_TXN frame",
        )
        .await;
        return;
    }

    // Empty SCAN for system tables — no DDL, no lock needed.
    let _g = shared.catalog_rwlock.read().await;
    let cat_ptr = shared.catalog;
    match guard_panic("scan", || unsafe { (*cat_ptr).scan_family(target_id) }) {
        Ok((b, _)) => {
            let batch_ref = if b.count > 0 { Some(b) } else { None };
            send_ok_response(
                shared,
                peer,
                target_id,
                batch_ref.as_deref(),
                client_id,
                shared.last_tick_lsn.get() as u128,
                client_version,
            )
            .await;
        }
        Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
    }
}

/// Collect the PKs of the bundle's `tid` family whose weight matches the
/// requested sign — the CREATE `+1` rows (`positive`) or the DROP `-1` rows.
/// Empty if the bundle carries no such family. A DDL bundle is weight-homogeneous
/// per family, so this cleanly selects the create rows or the drop rows.
fn family_pks_by_sign(families: &[(i64, Batch)], tid: i64, positive: bool) -> Vec<i64> {
    families
        .iter()
        .find(|(t, _)| *t == tid)
        .map(|(_, b)| {
            (0..b.count)
                .filter(|&i| {
                    if positive {
                        b.get_weight(i) > 0
                    } else {
                        b.get_weight(i) < 0
                    }
                })
                .map(|i| b.get_pk(i) as i64)
                .collect()
        })
        .unwrap_or_default()
}

/// Atomic DDL transaction: ingest a bundle of system-table family batches under
/// one durable SAL zone. Reached only via the `FLAG_DDL_TXN` route. Every
/// catalog write — a CREATE's N families or a DROP/CREATE INDEX/CREATE SCHEMA's
/// single family — flows here, so there is one system-write code path end to
/// end. Families are ingested in ascending topo order (so every register/index
/// hook sees its dependencies already in the memtable); on any failure the
/// applied families are negated in master memory before broadcast, so a crash
/// *or* a precheck failure can never strand an orphan catalog row.
async fn handle_ddl_txn(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8], client_version: u16) {
    // Decode the bundle and materialise each family's wal-block slice into an
    // owned Batch up front (before any lock), resolving its system schema from
    // the catalog. `sys_family_schema` rejects a bogus family tid without the
    // panic `sys_tab_schema` would hit on an unknown id in the system range.
    let raw_families = match ipc::decode_ddl_txn(data) {
        Ok(d) => d,
        Err(e) => {
            let msg = format!("decode error: {e}");
            send_error(peer, 0, client_id, msg.as_bytes()).await;
            return;
        }
    };
    if raw_families.is_empty() {
        send_error(peer, 0, client_id, b"DDL_TXN: empty family bundle").await;
        return;
    }
    let family_count = raw_families.len();
    let mut families: Vec<(i64, Batch)> = Vec::with_capacity(family_count);
    for &(tid, slice) in &raw_families {
        let schema = match shared.cat().sys_family_schema(tid) {
            Some(s) => s,
            None => {
                let msg = format!("DDL_TXN: {tid} is not a system family");
                send_error(peer, 0, client_id, msg.as_bytes()).await;
                return;
            }
        };
        match decode_client_batch(slice, &schema) {
            Ok(b) => families.push((tid, b)),
            Err(e) => {
                let msg = format!("DDL_TXN family {tid} decode error: {e}");
                send_error(peer, 0, client_id, msg.as_bytes()).await;
                return;
            }
        }
    }

    // A CREATE VIEW is a stop-the-world op (source drain + distributed backfill,
    // reactor parked). The VIEW_TAB family's +1 rows, if any, are the new views;
    // a DROP-only or non-VIEW bundle yields none and keeps the plain path.
    let new_view_ids: Vec<i64> = family_pks_by_sign(&families, VIEW_TAB_ID, true);
    let view_create = !new_view_ids.is_empty();

    // Drain the committer barrier BEFORE acquiring the catalog write
    // lock. The barrier flushes user-table WAL and waits for worker ACKs (tens
    // of ms under load); holding the write lock across that wait would block
    // every concurrent SCAN/SEEK read for no reason — no catalog mutation
    // happens until after the barrier returns. Quiesce the tick subsystem for a
    // CREATE VIEW while the reactor still runs and before the write lock
    // (run_tick/relay_loop take the read lock, so a write-lock-held quiesce would
    // deadlock). `TickGate` releases the gate on every exit path.
    let t_ddl_start = Instant::now();
    let (tx, rx) = oneshot::channel::<()>();
    shared.committer_tx.send(CommitRequest::Barrier {
        kind: BarrierKind::Ddl,
        done: tx,
    });
    let _ = rx.await;

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

    if view_create {
        // Lock-held committer barrier: a push could have committed between the
        // pre-lock barrier and the write lock; flush it so every straggler is
        // resident in pending_deltas before the in-loop source drain. The
        // committer stays idle for the rest of the handler (the write lock blocks
        // new pushes).
        let (tx, rx) = oneshot::channel::<()>();
        shared.committer_tx.send(CommitRequest::Barrier {
            kind: BarrierKind::Ddl,
            done: tx,
        });
        let _ = rx.await;
    }

    let cat_ptr_raw = shared.catalog;
    // Discard any stale queue entries from a prior failed DDL so they don't
    // piggyback on this one. (pending_dir_deletions is NOT discarded here: a
    // failed DDL already clears it on the error path, and recovery legitimately
    // queues drops here that must be drained — not discarded — by the post-fsync
    // drain.)
    let _ = unsafe { (*cat_ptr_raw).drain_pending_broadcasts() };

    // Pre-flight global uniqueness for every unique secondary index in this
    // bundle BEFORE reserving the zone LSN or mutating the catalog, so a
    // violation needs no rollback — it just surfaces to the client. This runs
    // before the ingest loop, so for a table created in the same bundle the owner
    // is not yet in `dag.tables` and `validate_unique_index_create_async`
    // short-circuits to an empty set (sound: the new table is empty, and
    // hook_index_register's own owner-check still succeeds later in the loop). The
    // IDX_TAB row layout (and the IDXTAB_PAY_* payload indices) is fixed by
    // `create_index` and read identically by `hook_index_register`.
    let mut filter_seeds: Vec<(i64, u64, FxHashSet<crate::storage::PkBuf>, bool)> = Vec::new();
    if let Some((_, idx_batch)) = families.iter().find(|(tid, _)| *tid == IDX_TAB_ID) {
        for i in 0..idx_batch.count {
            if idx_batch.get_weight(i) > 0 && idx_batch.read_payload_u64(i, IDXTAB_PAY_IS_UNIQUE) != 0 {
                let owner_id = idx_batch.read_payload_u64(i, IDXTAB_PAY_OWNER_ID) as i64;
                let packed = idx_batch.read_payload_u64(i, IDXTAB_PAY_SOURCE_COLS);
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
                        send_error(peer, 0, client_id, e.as_bytes()).await;
                        return;
                    }
                    // Hold the distinct span set to seed the filter post-commit,
                    // keyed by the packed column list (the filter-map key).
                    Ok((seen, capped)) => filter_seeds.push((owner_id, packed, seen, capped)),
                }
            }
        }
    }

    // Reserve the zone LSN but do NOT publish it until fsync confirms
    // durability. A DDL bundle writes arbitrary system families, so the floor is
    // `max_table_current_lsn` — the zone must dominate EVERY family's counter
    // (see `ZoneLsnAllocator::reserve` for why a drifted counter would dedup-drop
    // the zone on recovery).
    let zone_lsn = shared
        .lsn_alloc
        .reserve(unsafe { (*cat_ptr_raw).max_table_current_lsn() });
    let zone_lsn_nz = NonZeroU64::new(zone_lsn).expect("zone LSN allocator starts above 0");
    unsafe {
        (*cat_ptr_raw).ctx.open_ddl_zone(zone_lsn_nz);
    }

    // The post-fsync unique-filter maintenance needs the durably-dropped tids and
    // (owner, packed-cols) pairs (the -1 rows); the ingest loop consumes
    // `families`, so extract those minimal lists now instead of cloning the whole
    // TABLE_TAB / IDX_TAB batches. A bundle is one DDL, so at most one family
    // carries -1 rows; a CREATE bundle yields empty lists.
    let dropped_tids: Vec<i64> = family_pks_by_sign(&families, TABLE_TAB_ID, false);
    let dropped_indices: Vec<(i64, u64)> = families
        .iter()
        .find(|(tid, _)| *tid == IDX_TAB_ID)
        .map(|(_, b)| {
            (0..b.count)
                .filter(|&i| b.get_weight(i) < 0)
                .map(|i| {
                    let owner_id = b.read_payload_u64(i, IDXTAB_PAY_OWNER_ID) as i64;
                    let packed = b.read_payload_u64(i, IDXTAB_PAY_SOURCE_COLS);
                    (owner_id, packed)
                })
                .collect()
        })
        .unwrap_or_default();

    // Ingest the families in ascending topo order so every register/index hook
    // sees its dependencies already in the memtable. For a CREATE VIEW, drain the
    // new view's base sources once the circuit/dep families are in the memtable
    // (so get_source_ids resolves) but before the VIEW_TAB register hook's inline
    // backfill scans them — VIEW_TAB is the first family at or past view priority.
    // The between-precheck-and-apply marker holds the single family that was
    // applied but not yet enqueued (a hook/panic failure), which compensation must
    // negate; a precheck failure leaves the marker None, so no ghost -1 is written.
    // The ingest loop writes nothing to the SAL (broadcasts are queued and emitted
    // only in the tail below), so the in-loop drain's tick precedes the zone's
    // broadcasts in SAL order exactly as before.
    families.sort_by_key(|(tid, _)| CatalogEngine::catalog_topo_priority(*tid));
    let view_prio = CatalogEngine::catalog_topo_priority(VIEW_TAB_ID);
    let mut applied_not_enqueued: Option<(i64, Batch)> = None;
    let mut drained_sources = false;
    let ingest_res = guard_panic("DDL", || {
        let cat = unsafe { &mut *cat_ptr_raw };
        for (fid, fbatch) in families {
            if view_create && !drained_sources && CatalogEngine::catalog_topo_priority(fid) >= view_prio {
                for src in cat.dag.base_tables_reachable_from(new_view_ids.clone()) {
                    shared.disp().drain_tick_blocking(src)?;
                }
                drained_sources = true;
            }
            cat.precheck_family(fid, &fbatch)?;
            applied_not_enqueued = Some((fid, fbatch.clone()));
            cat.apply_and_enqueue_family(fid, fbatch)?;
            applied_not_enqueued = None;
        }
        Ok(())
    });
    if let Err(e) = ingest_res {
        guard_panic("DDL-compensate", || {
            unsafe {
                (*cat_ptr_raw).compensate_stage_a(applied_not_enqueued.take());
            }
            Ok::<(), String>(())
        })
        .unwrap_or_else(|ce| {
            gnitz_fatal_abort!("Stage-A DDL compensation panicked after DDL error '{}': {}", e, ce);
        });
        unsafe {
            (*cat_ptr_raw).ctx.close_ddl_zone();
        }
        send_error(peer, 0, client_id, e.as_bytes()).await;
        return;
    }

    // SAL emission window (byte-identical to the single-family DDL): broadcast
    // each drained family under the shared zone_lsn, close the zone with the
    // commit sentinel, then fsync. A failure here is unrecoverable — workers
    // already applied the FLAG_DDL_SYNC groups in real time — so abort.
    let drained = unsafe { (*cat_ptr_raw).drain_pending_broadcasts() };
    let fsync_fut = {
        let _sal_excl = shared.sal_writer_excl.lock().await;
        emit_zone_to_sal(shared, "DDL", &drained, zone_lsn)
    };
    let fsync_rc = fsync_fut.await;
    if fsync_rc < 0 {
        gnitz_fatal_abort!("SAL fdatasync (DDL) failed rc={}", fsync_rc);
    }

    // Publish only after fsync, then close the zone and defer dir removals to the
    // next checkpoint (whose worker-ACK barrier proves every worker consumed past
    // this DROP; removing here races a lagging worker's partition-dir create).
    shared.lsn_alloc.publish(zone_lsn);
    unsafe {
        (*cat_ptr_raw).ctx.close_ddl_zone();
        (*cat_ptr_raw).defer_pending_dir_deletions();
    }

    // Invalidate unique-filter state for durably-dropped tables/indices so a
    // recreated table with the same ID does not inherit stale filter entries.
    let disp_ptr_raw = shared.dispatcher;
    for &tid in &dropped_tids {
        unsafe {
            (*disp_ptr_raw).unique_filter_invalidate_table(tid);
        }
    }
    for &(owner_id, packed) in &dropped_indices {
        // Keying by the whole packed list means dropping `(a, b)` never clears a
        // distinct single-column filter on `a`.
        unsafe {
            (*disp_ptr_raw).unique_filter_remove(owner_id, packed);
        }
    }

    // Seed the unique filters from the pre-flight's distinct key sets so the first
    // INSERT skips a redundant full-cluster warmup scan. Post-fsync only: a
    // broadcast/fsync failure aborts the process before this point, so no filter
    // is published for an index that never committed.
    for (owner_id, packed, seen, capped) in filter_seeds {
        unsafe {
            (*disp_ptr_raw).unique_filter_seed(owner_id, packed, seen, capped);
        }
    }

    // Order the bundle's new views by intra-bundle dependency before backfilling:
    // a chain requires an upstream hidden view to be materialized before a
    // downstream one scans it. The order is re-derived from the dep-map (populated
    // during the ingest loop, DEP_TAB applying before VIEW_TAB) rather than from
    // VIEW_TAB row order.
    let ordered_view_ids: Vec<i64> = unsafe { (*cat_ptr_raw).dag.order_by_intra_bundle_deps(&new_view_ids) };

    // View-scoped distributed backfill for every exchange / equi-join view; plain
    // projection/filter views were already filled inline by hook_view_register
    // over the (now drained) committed sources. A post-fsync Err cannot be rolled
    // back (the CREATE is durable), so abort — restart's boot backfill rebuilds it.
    for &vid in &ordered_view_ids {
        if unsafe { (*cat_ptr_raw).dag.view_seeds_exchange_backfill(vid) } {
            // A multi-source equi-join iterates both sources: backfilling the
            // first fills its trace (join against the empty other trace → no
            // output), then the second produces the full join against it.
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

    send_ok_response(shared, peer, 0, None, client_id, zone_lsn as u128, client_version).await;
    let total = t_ddl_start.elapsed();
    if total > Duration::from_millis(20) {
        gnitz_debug!("DDL_TXN SLOW total={:?} families={}", total, family_count);
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
    peer: &Peer,
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
    peer.send_buffer_or_close(buf).await;
}

/// Control-only reply carrying just a status code: no schema, no data, no error
/// text. The schema-mismatch (`STATUS_SCHEMA_MISMATCH`) and no-index
/// (`STATUS_NO_INDEX`) responses are byte-identical apart from the status, and
/// the client treats each frame as a pure signal.
async fn send_control_only(peer: &Peer, target_id: i64, client_id: u64, status: u32) {
    let buf = encode_response_buffer(target_id, client_id, None, status, b"", None, 0, 0);
    peer.send_buffer_or_close(buf).await;
}

async fn send_error(peer: &Peer, target_id: i64, client_id: u64, error_msg: &[u8]) {
    // STATUS_ERROR suppresses the schema block (has_schema = false), so
    // prebuilt_schema = None is correct and saves the cache lookup.
    // flags=0: client ignores schema version on error responses.
    let buf = encode_response_buffer(target_id, client_id, None, STATUS_ERROR, error_msg, None, 0, 0);
    peer.send_buffer_or_close(buf).await;
}

/// Existence + writability gate shared by the empty-push and INSERT arms.
/// A view registers into the same id space as base tables (shared
/// `next_table_id`), so a raw client push addressed to a view tid would
/// otherwise commit rows into the view's output store that its circuit
/// never produced — permanently divergent derived state. Only base tables
/// are push targets; system-catalog tids never reach this (both arms gate
/// on `FIRST_USER_TABLE_ID`). Caller holds the catalog read lock. Returns
/// `true` iff an error frame was sent and the caller must return.
async fn push_target_rejected(shared: &Shared, peer: &Peer, target_id: i64, client_id: u64) -> bool {
    match push_target_error(shared, target_id) {
        Some(msg) => {
            send_error(peer, target_id, client_id, msg.as_bytes()).await;
            true
        }
        None => false,
    }
}

/// The reason `target_id` cannot receive a push (absent, or not a base table),
/// or `None` if it can. The shared existence + writability gate behind both the
/// plain-push arm (via `push_target_rejected`) and the per-family check in
/// `push_txn_body`, which owns its own reply path.
fn push_target_error(shared: &Shared, target_id: i64) -> Option<String> {
    // `kind` is `Copy`; `.map` ends the `dag.tables` borrow before the caller's
    // awaits.
    match shared.cat().dag.tables.get(&target_id).map(|e| e.kind) {
        None => Some(format!("table {target_id} not found")),
        Some(kind) if !kind.is_base_table() => Some(format!(
            "table {target_id} is not writable: pushes must target a base table"
        )),
        _ => None,
    }
}

/// Emit a closed catalog zone to the SAL: broadcast each drained family batch
/// under `zone_lsn`, write the commit sentinel (`commit_zone`, which also
/// signals all workers), and submit the fdatasync SQE, returning its future.
/// The caller must hold `sal_writer_excl` across the call so reservation order
/// == SAL write order. A failure here comes after the in-memory catalog
/// mutation and would permanently diverge master/worker state — unrecoverable,
/// so abort.
fn emit_zone_to_sal(shared: &Shared, op: &'static str, drained: &[(i64, Batch)], zone_lsn: u64) -> FsyncFuture {
    let disp = shared.dispatcher;
    if let Err(e) = guard_panic(op, || unsafe {
        for (tid, bat) in drained {
            (*disp).broadcast_ddl(*tid, bat, zone_lsn)?;
        }
        // Crash-injection seam: abort after broadcasts but BEFORE the commit
        // sentinel — exercises the recovery skip of a half-written zone.
        #[cfg(debug_assertions)]
        if std::env::var("GNITZ_INJECT_DDL_PANIC").as_deref() == Ok("after_broadcasts") {
            libc::abort();
        }
        (*disp).commit_zone(zone_lsn)?;
        Ok::<(), String>(())
    }) {
        gnitz_fatal_abort!("{} broadcast failed after in-memory catalog mutation: {}", op, e);
    }
    shared.reactor.fsync(shared.sal_fd)
}

/// Durably reserve a SERIAL id range for `seq_id` and return the range base.
///
/// The high-water must be persisted *at allocation time*: `recover_sequences`
/// runs pre-fork and the master holds no user-table rows, so a lost advance
/// cannot be re-derived. This routes the `sys_sequences` delta through the DDL
/// SAL commit path — the same path `CREATE` uses, which
/// `recover_system_tables_from_sal` replays via `hook_sequence_register`.
///
/// **Reserve + mutate + emit under both locks, release both BEFORE the fsync.**
/// The whole reserve/mutate/emit span is synchronous (the only `.await`s are the
/// two lock acquisitions), so catalog readers — SEEK / SEEK_BY_INDEX* /
/// GET_INDICES / tick emission, all of which take `catalog_rwlock.read()` — block
/// only for that brief span, never across the `fdatasync`. Distinctness and
/// publish-after-fsync are the `ZoneLsnAllocator` contract; the reservation
/// floor is `sys_sequences`' own counter, computed by `reserve_user_sequence`
/// (a SERIAL zone writes that one family, so recovery's per-family dedup needs
/// no other counter dominated). The pin (`current_lsn = zone_lsn`) runs
/// synchronously under the locks, so recovery's dedup matches the SAL group LSN.
///
/// The full `open_ddl_zone … ingest … close_ddl_zone` lifecycle is contained in
/// the one await-free write-lock section, so the single `ctx.ddl_zone_lsn` slot
/// is never observed by another allocator once the write lock drops. It needs
/// none of `handle_ddl`'s VIEW-only prelude (TickGate quiesce, committer barrier,
/// base-table drain): a `sys_sequences` advance has no DAG evaluation and no
/// rollback path.
async fn commit_serial_range_durable(shared: &Rc<Shared>, seq_id: i64, count: i64) -> i64 {
    let (base, zone_lsn, fsync_fut) = {
        // Lock order catalog -> SAL, matching INSERT/SEEK, so acquiring SAL under
        // catalog.write cannot deadlock. Both guards drop at the end of this block.
        let _write = shared.catalog_rwlock.write().await;
        let _sal_excl = shared.sal_writer_excl.lock().await;

        // Raw-pointer derefs (as handle_ddl) so no `&mut CatalogEngine` borrow is
        // held across a later `.await`; the write lock guarantees no other
        // coroutine touches the catalog while this block runs.
        let cat_ptr = shared.catalog;
        let (base, delta, zone_floor) = unsafe { (*cat_ptr).reserve_user_sequence(seq_id, count) };
        let zone_lsn = shared.lsn_alloc.reserve(zone_floor);
        let zone_lsn_nz = NonZeroU64::new(zone_lsn).expect("zone LSN allocator starts above 0");

        // A sys_sequences advance is a pure system-table write (no evaluate_dag,
        // no rollback); a hook failure on a well-formed 2-row delta is an
        // invariant violation — abort rather than compensate.
        // `gnitz_fatal_abort!` expands to an `unsafe` block, so keep it out of the
        // raw-deref `unsafe`.
        let ingest_res = unsafe {
            (*cat_ptr).ctx.open_ddl_zone(zone_lsn_nz);
            (*cat_ptr).ingest_to_family(SEQ_TAB_ID, &delta)
        };
        if let Err(e) = ingest_res {
            gnitz_fatal_abort!("sys_sequences ingest (serial range) failed: {}", e);
        }
        unsafe {
            (*cat_ptr).ctx.close_ddl_zone();
        }

        // SAL emission under the still-held sal_writer_excl; the fdatasync SQE is
        // submitted synchronously. Both guards drop as this block ends, before
        // the await below.
        let drained = unsafe { (*cat_ptr).drain_pending_broadcasts() };
        (
            base,
            zone_lsn,
            emit_zone_to_sal(shared, "serial-range", &drained, zone_lsn),
        )
    };

    if fsync_fut.await < 0 {
        gnitz_fatal_abort!("SAL fdatasync (serial range) failed");
    }

    // Publish only after fsync: readers never see an LSN whose backing
    // sys_sequences delta is not yet on disk.
    shared.lsn_alloc.publish(zone_lsn);
    base
}

async fn send_alloc(peer: &Peer, new_id: i64, client_id: u64) {
    // Alloc responses carry no schema block; schema version irrelevant.
    let buf = encode_response_buffer(new_id, client_id, None, STATUS_OK, b"", None, 0, 0);
    peer.send_buffer_or_close(buf).await;
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
