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
use rustc_hash::FxHashMap;

use super::guard_panic;
use crate::foundation::fault::Seam;
use crate::foundation::posix_io;
use crate::runtime::tls::{ConnCountGuard, TlsShared};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::catalog::{
    family_pks_by_sign, idx_tab_drops, idx_tab_unique_creates, CatalogEngine, SysFamily, FIRST_USER_TABLE_ID,
    SEQ_TAB_ID,
};
use crate::query::RelationKind;
use crate::runtime::committer::{self, BarrierKind, CommitRequest, PendingTxn};
use crate::runtime::lsn::ZoneLsnAllocator;
use crate::runtime::master::{
    dispatch_scan_multi_fanout, first_worker_error_opt, replicated_unicast, scan_spec_route, Fanout, MasterDispatcher,
    TxnFamily, UniqueFilter,
};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{
    join_into, mpsc, oneshot, select2, AsyncMutex, AsyncRwLock, Either, FsyncFuture, PendingRelay, Reactor, ReadGuard,
    ReplyFuture, WriteGuard,
};
use crate::runtime::sal::{SalFit, BACKFILL_DECISION_CONTINUE, FLAG_SCAN_SPEC};
use crate::runtime::wire::{
    self as ipc, SchemaWithVersion, FLAG_RESOLVE, STATUS_ERROR, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH,
};
use crate::schema::{validate_schema_match, SchemaDescriptor};
use crate::storage::Batch;

pub(crate) const TICK_COALESCE_ROWS: usize = 10_000;
const TICK_DEADLINE_MS: u64 = 20;
const WORKER_WATCH_MS: u64 = 100;

/// `GNITZ_INJECT_DDL_PANIC=after_broadcasts`: crash the master between a DDL
/// zone's broadcasts and its commit sentinel.
static DDL_PANIC: Seam = Seam::new("GNITZ_INJECT_DDL_PANIC");

/// `GNITZ_INJECT_RELAY_HOLD_FOR_DDL`: see `hold_relay_for_ddl`.
static RELAY_HOLD_FOR_DDL: Seam = Seam::new("GNITZ_INJECT_RELAY_HOLD_FOR_DDL");

/// Count of DDL tick-quiesce requests, bumped as each is sent. Read only by
/// `hold_relay_for_ddl`, which needs to observe the request rather than the
/// window it opens: the window is entered only after the tick loop acks, which
/// this very seam is holding up.
static DDL_QUIESCE_REQUESTS: AtomicU64 = AtomicU64::new(0);

use gnitz_wire::{
    FLAG_ALLOCATE_INDEX_ID, FLAG_ALLOCATE_SCHEMA_ID, FLAG_ALLOCATE_TABLE_ID, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
};

/// One tick request to `tick_loop`.
pub enum TickTrigger {
    /// Fire-and-forget trigger from INSERT when a tid crosses the row
    /// coalesce threshold.  Tids come from `tick_rows`.
    Auto,
    /// Explicit drain requested by a read or by the checkpoint: tick whatever is
    /// pending — even nothing — and report the tick's verdict on `done`. A reader
    /// that waited on a failed tick must be told: its view is stale, and reporting
    /// success would serve stale rows under `STATUS_OK`.
    Drain { done: oneshot::Sender<Result<(), String>> },
    /// Pause the tick subsystem for a DDL bundle. On dequeue the tick loop
    /// signals `acked` — proving no tick is in flight (the loop is serial, so
    /// the prior tick has returned) and none will start — then blocks on
    /// `release` until the DDL hands the gate back. This drains any in-flight
    /// steady-state exchange tick before the DDL broadcasts its `DdlSync`, so no
    /// worker is mid-epoch (and thus deferring that broadcast) when the client
    /// is ACKed; see `handle_ddl_txn`.
    Quiesce {
        acked: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
}

/// The window every DDL bundle runs inside: the tick loop is parked and the
/// DDL-window depth is raised for exactly as long as the gate lives. Dropping it
/// releases both, so the window ends on every exit path of `handle_ddl_txn`
/// (success or early-return error).
///
/// While the depth is non-zero no checkpoint round may run: its drain would
/// never complete against a parked tick loop, and the DDL's own synchronous W2M
/// collectors read the rings by position, so they would eat the round's ACKs and
/// park the committer forever holding `sal_writer_excl`. A depth, not a flag —
/// `handle_ddl_txn` awaits before taking the catalog write lock, so a second DDL
/// enters its own window while the first is still in its.
struct TickGate {
    /// Dropped by the field glue right after `Drop::drop` lowers the depth. The
    /// tick loop's `release.await` resolves `Err(Cancelled)` on that drop, which
    /// is the release signal — no explicit send needed.
    _release: oneshot::Sender<()>,
    depth: Rc<Cell<usize>>,
}

impl TickGate {
    /// Park the tick loop and enter a DDL window. Returns once the loop has
    /// acked — no tick is in flight and none will start until this gate drops.
    async fn enter(shared: &Rc<Shared>) -> Self {
        let (acked_tx, acked_rx) = oneshot::channel::<()>();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        shared.tick_tx.send(TickTrigger::Quiesce {
            acked: acked_tx,
            release: release_rx,
        });
        DDL_QUIESCE_REQUESTS.fetch_add(1, Ordering::Relaxed);
        let _ = acked_rx.await;
        let depth = Rc::clone(&shared.ddl_window);
        depth.set(depth.get() + 1);
        TickGate {
            _release: release_tx,
            depth,
        }
    }
}

impl Drop for TickGate {
    fn drop(&mut self) {
        self.depth.set(self.depth.get() - 1);
    }
}

/// Run `body` inside a DDL window: the tick loop is parked before it starts and
/// released once it finishes, with the depth raised for exactly that span.
///
/// A scope rather than a guard the caller binds, because the depth is not a lock
/// this DDL holds — it is the count of DDLs in flight that the committer reads to
/// decide whether a checkpoint sequence may run. Lowering it early does not
/// "release" anything: it reports that no DDL is running while this one still is,
/// and the sequence that then starts collides with whatever the handler does
/// next. Owning the gate here leaves no binding for a caller to drop, so that
/// cannot be written. `body` is a future, which is inert until awaited, so it
/// cannot run ahead of the gate either.
async fn with_ddl_window<T>(shared: &Rc<Shared>, body: impl std::future::Future<Output = T>) -> T {
    let _gate = TickGate::enter(shared).await;
    body.await
}

/// Send a committer barrier of `kind` and wait for it to resolve.
async fn await_barrier(shared: &Shared, kind: BarrierKind) {
    let (tx, rx) = oneshot::channel::<()>();
    shared.committer_tx.send(CommitRequest::Barrier { kind, done: tx });
    let _ = rx.await;
}

/// Shared executor state held by every task.
pub struct Shared {
    pub reactor: Rc<Reactor>,
    catalog: *mut CatalogEngine,
    dispatcher: Rc<MasterDispatcher>,
    committer_tx: mpsc::Sender<CommitRequest>,
    catalog_rwlock: Rc<AsyncRwLock>,
    /// SAL-writer exclusivity. Held by committer (checkpoint + commit
    /// emission), tick (per-tid emission), relay (FLAG_EXCHANGE_RELAY),
    /// DDL (broadcast_ddl + fsync), and all fan-out operations (seek,
    /// scan, pipeline checks, unique-filter warmup). See async-invariants.md.
    sal_writer_excl: Rc<AsyncMutex>,
    /// Tick trigger sender; senders include INSERT (auto-trigger on
    /// threshold cross) and SCAN (explicit drain).
    tick_tx: mpsc::Sender<TickTrigger>,
    /// Zone-LSN allocation high-water + durability watermark, shared with the
    /// committer so SCAN/SEEK handlers report the same LSN it assigns.
    lsn_alloc: Rc<ZoneLsnAllocator>,
    last_tick_lsn: Rc<Cell<u64>>,
    /// Tables with a pending delta, each with the row count feeding the tick
    /// threshold. `run_tick` writes one `FLAG_TICK` group per tid inside one
    /// `sal_writer_excl` window before awaiting any ACK, so the order the map
    /// yields them in only changes the order the workers see the groups in.
    tick_rows: Rc<RefCell<FxHashMap<i64, usize>>>,
    /// Per-table write serialization. A push whose validation reads committed
    /// state (`push_reads_committed_state`) and every transaction take the write
    /// guard; a push that reads no committed state takes the read guard, so
    /// same-table pushes reach the committer concurrently and share one fsync.
    table_locks: RefCell<FxHashMap<i64, Rc<AsyncRwLock>>>,
    /// Set true by the graceful-shutdown watcher before it sends the final
    /// Shutdown barrier, so `handle_message`'s push path rejects new pushes
    /// — none may commit after the final checkpoint's view flush.
    draining: Rc<Cell<bool>>,
    /// Nesting depth of the DDL windows in which the tick loop is parked; see
    /// `TickGate`. Read by the committer (no checkpoint round while non-zero)
    /// and by the watchdog (a SIGTERM waits the window out).
    ddl_window: Rc<Cell<usize>>,
    /// OCC per-table commit-LSN map: `tid → zone LSN of its last committed
    /// write this boot`. Bumped under the writer's table-lock guard immediately
    /// after a successful commit ACK (push arm and `push_txn_body`, `Ok` path
    /// only), and read by `push_txn_body`'s precondition check under the *write*
    /// guard on that lock, which excludes every bumper. A missing entry reads as
    /// `boot_seed`. Single-threaded reactor — a plain `RefCell`, and no borrow
    /// is ever held across an `.await`.
    table_commit_lsn: RefCell<FxHashMap<i64, u64>>,
    /// The default for a `table_commit_lsn` miss (a table not written this boot),
    /// seeded to `max_table_current_lsn()` — the same value `lsn_alloc.published()`
    /// starts at, so `boot_seed == published()` at boot. Soundness of the miss
    /// default does NOT rest on `boot_seed` dominating every pre-crash durable zone
    /// (a per-table counter can lag a global zone LSN across a crash-with-tail).
    /// It rests on OCC bases never surviving a restart: `last_seen_lsn` is
    /// per-connection, seeded from the HELLO ACK's `published()` and never
    /// persisted, so a restart severs every client and each re-seeds from the new
    /// `published()` (≥ boot_seed). Within a boot every live basis is ≥ boot_seed
    /// and every commit's zone strictly exceeds it, so a miss reading boot_seed can
    /// never false-pass.
    boot_seed: u64,
}

impl Shared {
    #[allow(clippy::mut_from_ref)]
    fn cat(&self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }
    fn disp(&self) -> &MasterDispatcher {
        &self.dispatcher
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

    fn table_lock(&self, tid: i64) -> Rc<AsyncRwLock> {
        let mut locks = self.table_locks.borrow_mut();
        if let Some(l) = locks.get(&tid) {
            return Rc::clone(l);
        }
        let l = Rc::new(AsyncRwLock::new());
        locks.insert(tid, Rc::clone(&l));
        l
    }

    /// Take the write guard on every table in `tids`, which must be sorted
    /// ascending and deduped. Sorted acquisition prevents deadlock between
    /// concurrent writers — a child INSERT and a parent DELETE attempt the same
    /// ordered set — and a repeated tid would take a second guard on a lock
    /// this task already holds and hang forever. Takes the tids owned:
    /// `fk_lock_set` borrows the catalog, and this loop awaits, so a slice
    /// would hold a catalog-derived reference across a suspension point during
    /// which another task's `cat()` mints a second `&mut`.
    async fn lock_tables_exclusive(&self, tids: Vec<i64>) -> Vec<WriteGuard> {
        let mut guards = Vec::with_capacity(tids.len());
        for tid in tids {
            guards.push(self.table_lock(tid).write().await);
        }
        guards
    }

    /// OCC: record `lsn` as the last-committed-write watermark for each of `tids`,
    /// under the caller's already-held table lock(s). The single writer of
    /// `table_commit_lsn` — both commit paths (the plain-push arm and
    /// `push_txn_body`) funnel through here, so a third write path can't silently
    /// omit the bump. Call on the commit `Ok` path only.
    ///
    /// `max`, not overwrite: shared-guard pushes to one table resume from their
    /// commit awaits in an order the guard does not enforce, so the watermark
    /// must never regress — a precondition check would then false-pass.
    fn record_commit_lsn(&self, tids: impl IntoIterator<Item = i64>, lsn: u64) {
        let mut map = self.table_commit_lsn.borrow_mut();
        for tid in tids {
            let e = map.entry(tid).or_default();
            *e = (*e).max(lsn);
        }
    }

    /// The zone LSN of `tid`'s last committed write this boot, or `boot_seed` for
    /// a table not written this boot. The miss default is sound for both readers:
    /// OCC (`push_txn_body`'s precondition check, under that table's write lock)
    /// because every live basis is ≥ `boot_seed` — see the field; read freshness
    /// (`read_is_fresh`) because `boot_seed` is the same `initial_lsn`
    /// `last_tick_lsn` is seeded to, so an unwritten table compares as absorbed,
    /// which it is — boot finishes its recovery tick sweep first.
    fn commit_lsn_of(&self, tid: i64) -> u64 {
        self.table_commit_lsn
            .borrow()
            .get(&tid)
            .copied()
            .unwrap_or(self.boot_seed)
    }

    /// Drop the per-relation state of a relation whose DROP is durable. Takes the
    /// catalog write guard because every `table_lock` acquisition is enclosed by a
    /// catalog *read* guard, so holding the write guard is what proves no task
    /// holds or awaits the lock being removed.
    fn forget_relation(&self, _catalog_write: &WriteGuard, id: i64) {
        self.table_locks.borrow_mut().remove(&id);
        self.table_commit_lsn.borrow_mut().remove(&id);
    }

    /// True iff some pending tid has crossed the row coalesce threshold.
    /// Used by the tick task to skip the deadline coalesce window.
    fn any_threshold_crossed(&self) -> bool {
        self.tick_rows.borrow().values().any(|&rows| rows >= TICK_COALESCE_ROWS)
    }

    /// Drain the pending tids into `out`, retaining `out`'s capacity — the
    /// caller's scratch buffer is reused across ticks instead of allocating a
    /// fresh `Vec` per drain.
    fn drain_tick_rows_into(&self, out: &mut Vec<i64>) {
        out.clear();
        out.extend(self.tick_rows.borrow_mut().drain().map(|(tid, _)| tid));
    }

    /// Put `tids` back after a tick failed to emit them, so their deltas are
    /// ticked again instead of stranded. Their true row counts are gone; 1 only
    /// understates the coalesce threshold, which honours the window instead of
    /// skipping it. A tid a mid-tick push already re-queued keeps that push's
    /// real count.
    fn requeue_tick_tids(&self, tids: &[i64]) {
        let mut rows = self.tick_rows.borrow_mut();
        for &tid in tids {
            rows.entry(tid).or_insert(1);
        }
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
        dispatcher: Rc<MasterDispatcher>,
        server_fd: i32,
        tls: Option<TlsListener>,
    ) -> i32 {
        let reactor = match Reactor::new(256) {
            Ok(r) => Rc::new(r),
            Err(e) => {
                gnitz_error!("io_uring init failed: {e}");
                return 1;
            }
        };
        reactor.attach_w2m(dispatcher.w2m_receiver());
        // After handoff, point the dispatcher at the reactor-owned receiver so
        // the reactor-parked CREATE-VIEW backfill can drive a synchronous
        // collect (the reactor's `OnceCell` slot is stable for its lifetime).
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

        let sal_writer_excl = Rc::new(AsyncMutex::new());
        // Seed the zone-LSN allocator above every table's current_lsn so each
        // new zone LSN is strictly greater, keeping `ingest_to_family`'s direct
        // current_lsn assignment monotonic across restarts.
        let initial_lsn = unsafe { &*catalog }.max_table_current_lsn();
        let lsn_alloc = Rc::new(ZoneLsnAllocator::new(initial_lsn));
        let last_tick_lsn = Rc::new(Cell::new(initial_lsn));
        let tick_rows: Rc<RefCell<FxHashMap<i64, usize>>> = Rc::new(RefCell::new(FxHashMap::default()));

        let (committer_tx, committer_rx) = mpsc::unbounded::<CommitRequest>();
        let (tick_tx, tick_rx) = mpsc::unbounded::<TickTrigger>();
        let (relay_tx, relay_rx) = mpsc::unbounded::<PendingRelay>();
        // Wire the relay channel into the reactor so route_reply's
        // FLAG_EXCHANGE accumulator can hand off completed views.
        reactor.attach_relay_tx(relay_tx);

        // Graceful-shutdown push gate (reactor-thread-only).
        let draining = Rc::new(Cell::new(false));
        // Nesting depth of the quiescing-DDL windows (reactor-thread-only).
        let ddl_window = Rc::new(Cell::new(0usize));
        let committer_shared = Rc::new(committer::Shared {
            reactor: Rc::clone(&reactor),
            disp: Rc::clone(&dispatcher),
            sal_writer_excl: Rc::clone(&sal_writer_excl),
            lsn_alloc: Rc::clone(&lsn_alloc),
            force_checkpoint: Cell::new(false),
            tick_rows: Rc::clone(&tick_rows),
            tick_tx: tick_tx.clone(),
            ddl_window: Rc::clone(&ddl_window),
        });
        let shared = Rc::new(Shared {
            reactor: Rc::clone(&reactor),
            catalog,
            dispatcher,
            committer_tx,
            catalog_rwlock: Rc::new(AsyncRwLock::new()),
            sal_writer_excl: Rc::clone(&sal_writer_excl),
            tick_tx,
            lsn_alloc: Rc::clone(&lsn_alloc),
            last_tick_lsn: Rc::clone(&last_tick_lsn),
            tick_rows: Rc::clone(&tick_rows),
            table_locks: RefCell::new(FxHashMap::default()),
            draining: Rc::clone(&draining),
            ddl_window: Rc::clone(&ddl_window),
            table_commit_lsn: RefCell::new(FxHashMap::default()),
            boot_seed: initial_lsn,
        });

        // Catch SIGTERM/SIGINT so the watchdog can drive a final checkpoint
        // before exiting.
        install_shutdown_signal_handlers();

        reactor.spawn(committer::run(committer_rx, committer_shared));
        reactor.spawn(accept_loop(Rc::clone(&shared), accept_ctx));
        reactor.spawn(tick_loop(Rc::clone(&shared), tick_rx));
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
        std::time::Duration::from_millis(crate::foundation::env::env_u64("GNITZ_TLS_HELLO_TIMEOUT_MS", 15_000))
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
    if let HelloOutcome::Reject = run_hello_handshake(&peer, &shared, buf.as_slice()).await {
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
async fn run_hello_handshake(peer: &Peer, shared: &Rc<Shared>, data: &[u8]) -> HelloOutcome {
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

    // Seed the client's OCC basis with the durability watermark now. This runs
    // before the connection message loop, so `published()` is `≤` any later read
    // the client issues — a sound (conservative) basis.
    let rc = peer.send_hello_ack(shared.lsn_alloc.published()).await;
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
            // Let a quiescing DDL finish first. Its tick loop is parked, so the
            // Shutdown barrier would resolve without a checkpoint and
            // `shutdown_workers()` would then kill the workers under a live
            // backfill, whose `fail_if_worker_dead` fatal-aborts.
            if shared.ddl_window.get() != 0 {
                continue;
            }
            gnitz_info!("shutdown signal received; draining, checkpointing, and stopping");

            // 1. Stop admitting new pushes (none may commit after the final
            //    flush).
            shared.draining.set(true);

            // 2. One final full checkpoint through the committer. The Shutdown
            //    barrier forces the whole sequence and is deferred to its end,
            //    so `done` resolves only after the base + drain + ephemeral
            //    rounds complete. A just-pushed delta may still sit in
            //    `pending_deltas` (the tick-coalesce window not yet fired), so
            //    the sequence's drain is what gets it into the views.
            await_barrier(&shared, BarrierKind::Shutdown).await;

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
            gnitz_error!("Worker {crashed} crashed (log: {base_dir}/worker_{crashed}.log), shutting down");
            shared.disp().shutdown_workers();
            shared.reactor.request_shutdown();
            return;
        }

        // The only reclaim trigger on a workload with no writes: every read verb
        // writes a SAL command group and nothing on a read path rewinds the
        // cursor. Deliberately not awaited — this loop is the sole worker-crash
        // detector and `flush_round`'s reply futures have no timeout, so awaiting
        // would let a worker dying mid-checkpoint hang the node silently instead
        // of aborting within one tick. The next tick re-checks and re-sends.
        // On a write workload it is inert: the committer already checkpoints at
        // 3/4 on every push, well before this 7/8 line. Skipped inside a DDL
        // window, where the committer refuses every checkpoint anyway.
        if shared.ddl_window.get() == 0 && !shared.disp().sal_relay_space_ok_raw() {
            let (tx, done) = oneshot::channel();
            shared.committer_tx.send(CommitRequest::Barrier {
                kind: BarrierKind::Reclaim { forced: false },
                done: tx,
            });
            // Fire-and-forget: dropping the receiver is the whole point, not an
            // RAII hold.
            drop(done);
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
async fn tick_loop(shared: Rc<Shared>, mut rx: mpsc::Receiver<TickTrigger>) {
    let nw = shared.disp().num_workers();
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

        shared.drain_tick_rows_into(&mut tids_scratch);
        tids_scratch.retain(|&tid| shared.cat().has_id(tid));

        // Run the tick. Errors are reported in logs AND handed to every Drain
        // trigger's `done`: the waiting reader's view is stale, so reporting
        // success would serve stale rows under STATUS_OK.
        let tick_result = run_tick(&shared, &tids_scratch, nw, &mut req_ids, &mut fut_slots, &mut ack_slots).await;
        if let Err(e) = &tick_result {
            gnitz_warn!("tick error: {}", e);
        }
        for t in triggers.drain(..) {
            if let TickTrigger::Drain { done, .. } = t {
                let _ = done.send(tick_result.clone());
            }
        }
    }
}

/// Emit FLAG_TICK groups for every `tid` and await the per-worker ACKs.
///
/// The emit-and-await lock shape: req_ids allocated before any lock,
/// `catalog_rwlock.read()` (so DDL cannot mutate schemas mid-emission) +
/// `sal_writer_excl` covering only the contiguous emission window (III.3b), one
/// `signal_all` inside it, both released before awaiting so other reactor work
/// proceeds concurrently with worker DAG eval.
async fn run_tick(
    shared: &Rc<Shared>,
    tids: &[i64],
    nw: usize,
    req_ids: &mut Vec<u64>,
    fut_slots: &mut Vec<ReplyFuture>,
    ack_slots: &mut Vec<Option<ipc::DecodedWire>>,
) -> Result<(), String> {
    // Snapshot before any .await: a concurrent push can advance the published LSN
    // while we wait for tick ACKs, and setting last_tick_lsn to that
    // higher value would report an LSN that this tick never processed.
    let snapshot_lsn = shared.lsn_alloc.published();
    if tids.is_empty() {
        // Nothing pending: an earlier completed tick already took every commit
        // published at or below the snapshot, because the committer queues a tid
        // before it publishes that commit's zone LSN. The watermark still
        // advances — `read_is_fresh` reads it, and a reader whose source
        // committed between a tick's dequeue and its publish would otherwise
        // never see its drain take effect. Skips two uncontended lock
        // acquisitions the rest of the body would take for a no-op.
        shared.last_tick_lsn.set(snapshot_lsn);
        return Ok(());
    }

    req_ids.clear();
    req_ids.extend((0..tids.len() * nw).map(|_| shared.reactor.alloc_request_id()));

    let _cat_read = shared.catalog_rwlock.read().await;
    let _sal_excl = shared.sal_writer_excl.lock().await;

    // Written by the closure as it goes, so the re-queue and reply-await below
    // are also correct on `guard_panic`'s panic arm, which discards the closure's
    // return value.
    let emitted = Cell::new(0usize);
    let emit = guard_panic("tick", || {
        let disp = shared.disp();
        let mut result = Ok(());
        for (i, &tid) in tids.iter().enumerate() {
            if let Err(e) = disp.write_tick_group(tid, &req_ids[i * nw..(i + 1) * nw]) {
                result = Err(e);
                break;
            }
            emitted.set(i + 1);
        }
        // Whatever was written is already published, so the workers consume it on
        // the next signal or their SAL wait timeout regardless. Signal it and
        // await its replies rather than returning while its evaluation is in
        // flight.
        if emitted.get() > 0 {
            disp.signal_all();
        }
        result
    });
    drop(_sal_excl);
    drop(_cat_read);

    let n = emitted.get();
    // The un-emitted tids never reached a worker, so they still need ticking. An
    // emitted tid is already being ticked by the workers (its group is
    // published), and `handle_tick` has taken its delta, so re-queueing it would
    // only produce a no-op tick that then reports success and masks this failure.
    shared.requeue_tick_tids(&tids[n..]);

    fut_slots.clear();
    fut_slots.extend(
        req_ids[..n * nw]
            .iter()
            .copied()
            .map(|id| shared.reactor.await_reply(id)),
    );
    join_into(fut_slots, ack_slots).await;
    let worker_err = first_worker_error_opt("tick", ack_slots);
    ack_slots.clear();
    if let Some(e) = emit.err().or(worker_err) {
        return Err(e);
    }
    shared.last_tick_lsn.set(snapshot_lsn);
    Ok(())
}

// ---------------------------------------------------------------------------
// Relay loop
// ---------------------------------------------------------------------------

/// Hold the FIRST steady-state exchange relay until a DDL has asked the tick
/// loop to quiesce, keeping every worker parked in `do_exchange_wait` across
/// that request. The DDL therefore reaches its catalog mutation while the
/// workers' catalogs are mid-epoch — the race is set up by ordering, not by a
/// sleep, so it does not depend on machine speed. One-shot: the rest of the run
/// relays at full speed.
///
/// Holds no catalog lock: taking one would queue the racing DDL's write lock
/// behind it and the window would never open.
async fn hold_relay_for_ddl(shared: &Shared) {
    let seen = DDL_QUIESCE_REQUESTS.load(Ordering::Relaxed);
    // Polling keeps the seam self-contained — nothing outside it needs a handle.
    // The bound releases the relay if no DDL ever comes, so a misarmed test fails
    // on its own assertion instead of wedging the node.
    for _ in 0..HOLD_RELAY_MAX_POLLS {
        if DDL_QUIESCE_REQUESTS.load(Ordering::Relaxed) != seen {
            return;
        }
        shared.reactor.timer(Instant::now() + Duration::from_millis(1)).await;
    }
    gnitz_warn!("relay hold seam armed but no DDL quiesce arrived; releasing the relay");
}

const HOLD_RELAY_MAX_POLLS: u32 = 10_000;

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
/// failure modes — an `emit_relay_with_decision` error and no space after a reclaim
/// checkpoint — `gnitz_fatal_abort!` rather than warn-and-drop: a loud,
/// recoverable crash (workers self-exit via `getppid()`, operator
/// restarts) beats a silent permanent cluster wedge.
async fn relay_loop(shared: Rc<Shared>, mut rx: mpsc::Receiver<PendingRelay>) {
    loop {
        let relay = match rx.recv().await {
            Some(r) => r,
            None => return,
        };

        if RELAY_HOLD_FOR_DDL.take_once() {
            hold_relay_for_ddl(&shared).await;
        }

        // Phase 1: CPU work + catalog read only — no SAL mutex.
        let prep = {
            let _cat = shared.catalog_rwlock.read().await;
            match guard_panic("prepare_relay", || shared.disp().prepare_relay(relay)) {
                Ok(p) => p,
                Err(e) => gnitz_fatal_abort!("prepare_relay failed: {}", e),
            }
        };

        // Phase 2: emit under the SAL mutex. The space check shares the
        // lock with the write, so no other SAL writer can consume the
        // margin in between. The barrier await MUST happen with the lock
        // dropped: the committer's checkpoint takes sal_writer_excl, so
        // holding it across the barrier deadlocks master-side.
        let mut reclaimed = false;
        loop {
            {
                let _sal = shared.sal_writer_excl.lock().await;
                let fit = shared.disp().sal_fit(prep.footprint);
                // The relay is written whole — there is no chunked form — so a
                // group over capacity is one no checkpoint can deliver.
                if fit == SalFit::Terminal {
                    gnitz_fatal_abort!("exchange relay exceeds the SAL outright; no checkpoint can deliver it");
                }
                // Both conditions: the group must fit at this cursor, and the
                // SAL must be above the proactive reclaim watermark.
                if fit == SalFit::Fits && shared.disp().sal_has_relay_space_arming() {
                    // Always CONTINUE: only steady-state tick exchanges reach this
                    // loop (both chunked-backfill drivers collect their relays
                    // synchronously in `collect_acks_and_relay`, the sole
                    // STOP/CHECKPOINT stamper), and a tick round never pads.
                    if let Err(e) = guard_panic("emit_relay", || {
                        shared
                            .disp()
                            .emit_relay_with_decision(&prep, BACKFILL_DECISION_CONTINUE)
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
            // `forced`: this relay's own byte count says it does not fit, which
            // the committer's ambient space test cannot see.
            await_barrier(&shared, BarrierKind::Reclaim { forced: true }).await;
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
        let (res, err_target, err_client) = if has_data && !has_schema {
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
            (
                decode_client_wire(data, ctrl, Some(hint)),
                ctrl_target_id,
                ctrl_client_id,
            )
        } else {
            // No schema hint means the frame carried no data block, so nothing
            // has been resolved yet — report the failure untargeted.
            (decode_client_wire(data, ctrl, None), 0, 0)
        };
        match res {
            Ok(d) => d,
            Err(e) => {
                let msg = format!("decode error: {e}");
                send_error(peer, err_target, err_client, msg.as_bytes()).await;
                return;
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

    // ---------- Consistent multi-relation scan ----------
    // Client→master frame naming N relations to snapshot at one SAL cut. Like
    // DDL_TXN / PUSH_TXN it carries `target_id = 0` and re-decodes its body from
    // the raw frame (the generic decode above sees no data block); placed in the
    // same pre-alloc-block run. Never written to the SAL.
    if flags & gnitz_wire::FLAG_SCAN_MULTI != 0 {
        handle_scan_multi(shared, peer, client_id, data).await;
        return;
    }

    // ---------- SERIAL range reservation ----------
    // Carries `target_id = seq_id (= table_id) ≠ 0` and the range `count` in
    // `seek_col_idx`, so it precedes the `target_id == 0` catalog-id block.
    if flags & gnitz_wire::FLAG_ALLOCATE_SERIAL_RANGE != 0 {
        let seq_id = target_id; // = table_id
        let count = decoded.control.seek_col_idx.max(1) as i64;
        match commit_serial_range_durable(shared, seq_id, count).await {
            Ok(base) => send_control_only(peer, base, client_id, STATUS_OK).await,
            Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
        }
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
            send_control_only(peer, new_id, client_id, STATUS_OK).await;
            return;
        }
    }

    let has_batch = decoded.data_batch.is_some();
    let batch_count = decoded.data_batch.as_ref().map(|b| b.count).unwrap_or(0);

    // ---------- SELECTs (SEEK / SEEK_BY_INDEX / SCAN) ----------
    if flags & FLAG_SEEK != 0 {
        // A base-table or system seek is the RMW hot path: base state is fresh at
        // push-apply time, so `read_lock` locks once and never drains. A view seek
        // drains inside it (BF-1), which is why the lock is taken there and not
        // at dispatch level.
        if let Some((_g, kind)) = read_lock(shared, peer, client_id, target_id).await {
            serve_seek(
                shared,
                peer,
                client_id,
                target_id,
                kind,
                decoded.control.seek_pk,
                &decoded.control.seek_pk_extra,
                client_version,
            )
            .await;
        }
        return;
    }
    if flags & FLAG_SEEK_BY_INDEX != 0 {
        // Only a base table can own a secondary index, so in practice `read_lock`
        // never drains here — but it is the same call every other read verb makes.
        let Some((_g, kind)) = read_lock(shared, peer, client_id, target_id).await else {
            return;
        };
        handle_seek_by_index(
            shared,
            peer,
            client_id,
            target_id,
            kind,
            decoded.control.seek_col_idx, // pack_pk_cols(col_indices)
            decoded.control.seek_pk,
            &decoded.control.seek_pk_extra,
            client_version,
        )
        .await;
        return;
    }
    // ScanSpec must be routed before the generic empty-batch scan dispatch below
    // (a `ReadSpec` request carries an empty batch, so target_id alone would route
    // it to the plain-scan fallthrough). Like `handle_scan` it locks (and drains a
    // view target) inside `read_lock`, so it takes no dispatch-level lock here.
    if flags & gnitz_wire::FLAG_SCAN_SPEC != 0 {
        handle_scan_spec(shared, peer, client_id, target_id, &decoded.control.seek_pk_extra).await;
        return;
    }

    // RESOLVE must be routed before the generic empty-batch scan dispatch below
    // and the `target_id < FIRST_USER_TABLE_ID` fallthrough, both of which key
    // on target_id alone: a by-name resolve carries `target_id = 0` and a by-id
    // resolve an arbitrary client-chosen tid, so either would swallow it.
    //
    // A plain read guard, not `read_lock`: a resolve answers catalog shape, and
    // a view tick moves a view's rows, never its shape — so the tick drain
    // `read_lock` waits for buys nothing here. The guard scope ends at the reply
    // buffer, so the lock is never held across the send.
    if flags & FLAG_RESOLVE != 0 {
        let reply = {
            let _g = shared.catalog_rwlock.read().await;
            build_resolve_reply(shared, client_id, target_id, &decoded.control.seek_pk_extra)
        };
        match reply {
            Ok(buf) => peer.send_buffer_or_close(buf).await,
            Err(msg) => send_error(peer, target_id, client_id, msg.as_bytes()).await,
        }
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
        // Same existence + writability gate as the INSERT path below. An empty
        // push commits nothing, but an unwritable target is rejected here too so
        // a client bug that happens to produce an empty batch (e.g. `delete` with
        // an empty pk list) fails the same way a non-empty one does instead of
        // being masked by a no-op ACK.
        {
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
        // The validator's own predicate decides the guard: a push that reads no
        // committed state cannot be invalidated by a concurrent one, so it may
        // share its table's lock and reach the committer alongside other pushes
        // to the same table, which fold into one SAL zone and one fsync. Every
        // other push takes all FK-related table locks exclusively.
        let reads_committed = shared.cat().push_reads_committed_state(target_id, mode);
        let _tlocks = if !reads_committed {
            (Some(shared.table_lock(target_id).read().await), Vec::new())
        } else {
            let lock_set = shared.cat().fk_lock_set(target_id).to_vec();
            (None, shared.lock_tables_exclusive(lock_set).await)
        };

        // Distributed validation (PK / FK / unique indices). A plain push is a
        // one-family bundle — the same four rules over the same fold — so the
        // batch moves into the family for the validation and back out for the
        // commit request. The validator itself skips a bundle no rule would
        // read a row for.
        let families = [TxnFamily {
            tid: target_id,
            mode,
            batch,
        }];
        if let Err(e) = MasterDispatcher::validate_txn_distributed(
            shared.disp(),
            &shared.reactor,
            &shared.sal_writer_excl,
            &families,
        )
        .await
        {
            send_error(peer, target_id, client_id, e.as_bytes()).await;
            return;
        }
        let [family] = families;
        let batch = family.batch;

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
                // Record the commit LSN for OCC while the table-lock guard is
                // still held (a concurrent precondition check reads it under the
                // write guard on the same lock, which excludes this one, so the
                // bump lands before any conflicting txn can pass).
                // Bump on the `Ok` path only: an `Err` reply is pre-SAL or
                // fail-stop, so no live-visible durable change to record.
                shared.record_commit_lsn([target_id], lsn);
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

/// Serve a point lookup with the catalog read lock already held: a catalog
/// family reads master-locally; a user relation (base table or view) fans out to
/// the owning worker by PK hash. SEEK unicasts to one worker, so no replicated
/// fork.
#[allow(clippy::too_many_arguments)]
async fn serve_seek(
    shared: &Rc<Shared>,
    peer: &Peer,
    client_id: u64,
    target_id: i64,
    kind: RelationKind,
    pk: u128,
    seek_pk_extra: &[u8],
    client_version: u16,
) {
    if kind == RelationKind::SystemCatalog {
        match unsafe { (*shared.catalog).seek_family(target_id, pk, seek_pk_extra) } {
            Ok((batch, _)) => {
                send_ok_response(shared, peer, target_id, batch.as_ref(), client_id, pk, client_version).await
            }
            Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
        }
    } else {
        match MasterDispatcher::fan_out_seek(
            shared.disp(),
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
    }
}

/// The two success shapes of `push_txn_body`. `Committed` carries the durable
/// zone LSN; `Conflict` carries a fresh basis (`published()`) the client adopts
/// for its retry. `push_txn_body` cannot send the reply itself (`peer` /
/// `client_id` are not in its scope), so it returns the outcome and
/// `handle_push_txn` renders it.
enum PushTxnOutcome {
    Committed(u64),
    Conflict(u64),
}

/// Handle an atomic user-table transaction (`FLAG_PUSH_TXN`): decode + validate
/// the bundle as a unit under the union of the involved table locks, run the OCC
/// precondition check under those locks, then emit it as N `FLAG_PUSH` groups
/// inside one zone under one sentinel. Mirrors the plain-push arm's lock order
/// (catalog read lock, then the per-table lock union ascending) and its late
/// `draining` check; both locks are held through the committer ACK. Every
/// rejection is pre-SAL, so an `Err` reply — and a `Conflict` outcome — mean
/// "nothing committed".
async fn handle_push_txn(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8]) {
    match push_txn_body(shared, data).await {
        // Standard single-frame ACK, seek_pk = zone LSN (uncorrelated, as
        // push_ddl_txn's reply is).
        Ok(PushTxnOutcome::Committed(lsn)) => {
            let buf = encode_response_buffer(ipc::WireMsg {
                client_id,
                seek_pk: lsn as u128,
                status: STATUS_OK,
                ..Default::default()
            });
            peer.send_buffer_or_close(buf).await;
        }
        // OCC precondition failed: a control-only STATUS_TXN_CONFLICT frame whose
        // `seek_pk` carries the fresh basis. Empty message — the client
        // synthesizes any human-readable text from the tid it sent.
        Ok(PushTxnOutcome::Conflict(fresh_basis)) => {
            let buf = encode_response_buffer(ipc::WireMsg {
                client_id,
                seek_pk: fresh_basis as u128,
                status: ipc::STATUS_TXN_CONFLICT,
                ..Default::default()
            });
            peer.send_buffer_or_close(buf).await;
        }
        Err(e) => send_error(peer, 0, client_id, e.as_bytes()).await,
    }
}

/// The body of `handle_push_txn`: every rejection is a plain `Err`, so the one
/// caller above owns the single reply path. Returns `Committed(zone_lsn)` on a
/// durable commit or `Conflict(fresh_basis)` when the OCC precondition check
/// fails.
async fn push_txn_body(shared: &Rc<Shared>, data: &[u8]) -> Result<PushTxnOutcome, String> {
    // 1. Decode + frame-local shape rules (no catalog access). The frame carries
    //    the families and the OCC preconditions (each `(tid, basis)`).
    let (raw, preconditions) = ipc::decode_push_txn(data).map_err(|e| format!("decode error: {e}"))?;
    if raw.is_empty() {
        return Err("TXN: empty family bundle".to_string());
    }

    // 2. Catalog read lock (excludes a concurrent DROP/DDL), then the
    //    catalog-dependent shape rules + per-family batch decode.
    let _cat = shared.catalog_rwlock.read().await;
    let mut families: Vec<TxnFamily> = Vec::with_capacity(raw.len());
    for fam in &raw {
        // The wire carries the tid as u32; the catalog addresses it as i64.
        let tid = fam.tid as i64;
        if tid < FIRST_USER_TABLE_ID {
            return Err(format!("TXN: {tid} is not a user table"));
        }
        // Same existence + writability gate the plain-push arm applies, so a view
        // target is rejected identically.
        if let Some(e) = push_target_error(shared, tid) {
            return Err(e);
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
    // Capture the family tids BEFORE `families` is moved into the commit request,
    // for the precondition-membership check and the post-commit map bump.
    let family_tids: Vec<i64> = families.iter().map(|f| f.tid).collect();

    // 3. Acquire the per-table lock union ⋃ fk_lock_set(tid) exclusively.
    let mut union: Vec<i64> = Vec::new();
    for fam in &families {
        union.extend_from_slice(shared.cat().fk_lock_set(fam.tid));
    }
    union.sort_unstable();
    union.dedup();
    let _tlocks = shared.lock_tables_exclusive(union).await;

    // 3b. OCC precondition check, under the just-acquired lock union and BEFORE
    //     validation. A precondition asserts "table `tid` has not been written
    //     since `basis`". The lock union already covers every precondition tid
    //     (preconditions ⊆ families, enforced here), so no lock-set extension.
    //     Every writer to a family table holds some guard on that same lock
    //     through its commit ACK and bumps the map before releasing; the write
    //     guard held here excludes both guard kinds, so a passing check + this
    //     commit are one atomic step. Reading the map borrow ends at each
    //     statement; no borrow crosses an `.await`.
    for &(tid, basis) in &preconditions {
        let tid = tid as i64;
        if !family_tids.contains(&tid) {
            return Err(format!("TXN: precondition on {tid}: not a written table"));
        }
        if shared.commit_lsn_of(tid) > basis {
            return Ok(PushTxnOutcome::Conflict(shared.lsn_alloc.published()));
        }
    }

    // 4. Distributed bundle validation (the four rules).
    MasterDispatcher::validate_txn_distributed(shared.disp(), &shared.reactor, &shared.sal_writer_excl, &families)
        .await?;

    // 5. Drain check immediately before the committer send. INVARIANT: there must
    //    be NO `.await` between this check and `committer_tx.send` — on the
    //    single-threaded reactor that gap is atomic, which guarantees a
    //    transaction that observed `draining == false` enqueues before the
    //    watchdog's Shutdown barrier.
    if shared.draining.get() {
        return Err("server shutting down".to_string());
    }

    // 6. Route through the committer and wait for the zone ACK. `families` is
    //    moved here; `family_tids` was captured above for the bump.
    let (tx, rx) = oneshot::channel::<Result<u64, String>>();
    shared
        .committer_tx
        .send(CommitRequest::Txn(PendingTxn { families, done: tx }));
    // Double `?`: the outer unwraps a channel cancel, the inner a committer
    // `Err` — so the bump below is reached ONLY on a successful commit.
    let lsn = rx.await.map_err(|_| "committer shut down".to_string())??;

    // 7. Record the commit LSN for every family tid while the table locks are
    //    still held (`_tlocks` in scope), so a later same-tid txn cannot pass its
    //    precondition against a pre-this-commit basis. Bump on `Ok` only. The
    //    reply is sent by `handle_push_txn` after the locks drop, which is fine:
    //    OCC needs only the bump under the lock, and a later same-tid txn cannot
    //    acquire the lock until this one releases (after the bump).
    shared.record_commit_lsn(family_tids.iter().copied(), lsn);
    Ok(PushTxnOutcome::Committed(lsn))
}

/// Decode a CLIENT-supplied frame. `decode_wire_with_ctrl` is the client-trust
/// entry: it leaves the batch `Raw`, dropping any FLAG_BATCH_SORTED /
/// FLAG_BATCH_CONSOLIDATED claim ("already sorted/consolidated, skip the work"),
/// which a client must never be trusted to make; downstream consolidation (the
/// catalog DDL ingest and the commit path) establishes those invariants. Every
/// client-boundary decode goes through this or its sibling
/// `decode_client_batch`.
fn decode_client_wire(
    data: &[u8],
    ctrl: ipc::DecodedControl,
    hint: Option<SchemaWithVersion<'_>>,
) -> Result<ipc::DecodedWire, &'static str> {
    let decoded = ipc::decode_wire_with_ctrl(data, ctrl, hint)?;
    // `decode_wire_body` builds a data batch only against a resolved schema, so
    // the two are present or absent together.
    if let (Some(b), Some(schema)) = (decoded.data_batch.as_ref(), decoded.schema.as_ref()) {
        reject_not_null_bits(b, schema)?;
    }
    Ok(decoded)
}

/// A raw WAL-block family batch inside a client FLAG_DDL_TXN bundle.
/// `decode_from_wal_block` builds every batch `Raw`, so like
/// `decode_client_wire` this carries no client layout claim.
fn decode_client_batch(slice: &[u8], schema: &SchemaDescriptor) -> Result<Batch, &'static str> {
    let (b, _) = Batch::decode_from_wal_block(slice, schema, false)?;
    reject_not_null_bits(&b, schema)?;
    Ok(b)
}

/// A client-supplied batch must not set a null bit on a payload column the
/// schema declares NOT NULL. `is_null` and `compare_by_group_cols` read such a
/// bit as a live NULL, while the evaluator's `nullable_slots`, a projection's
/// `NullPerm` and the `FixedIntNonnull` row comparator believe the schema
/// instead — a split that can turn a rejected UNIQUE duplicate into a committed,
/// durable row. Rejecting the bit here is what lets the rest of the engine pick
/// either camp freely.
///
/// Here rather than in `Batch::decode_from_wal_block`: this is a statement about
/// a CLIENT, and that decode also serves the worker's SAL consumption, the
/// master's W2M read and boot replay, where a rejection aborts the process
/// instead of answering the caller.
fn reject_not_null_bits(b: &Batch, schema: &SchemaDescriptor) -> Result<(), &'static str> {
    let not_null = gnitz_expr::SchemaFacts::not_null_payload_slots(schema);
    if not_null == 0 {
        return Ok(());
    }
    // OR-reduce, then one test: the conforming case walks every row either way,
    // so a per-row branch would only add work.
    let mb = b.as_mem_batch();
    let mut acc = 0u64;
    for row in 0..b.count {
        acc |= mb.get_null_word(row);
    }
    if acc & not_null != 0 {
        return Err("client batch sets a null bit on a NOT NULL column");
    }
    Ok(())
}

/// Resolve a read target's relation kind, rejecting an unknown table id.
/// Returns `None` when the error reply was sent and the caller must return.
/// The caller holds the catalog read lock.
async fn resolve_read_target(shared: &Rc<Shared>, peer: &Peer, client_id: u64, target_id: i64) -> Option<RelationKind> {
    let kind = shared.cat().dag.relation_kind(target_id);
    if kind.is_none() {
        let msg = format!("table {target_id} not found");
        send_error(peer, target_id, client_id, msg.as_bytes()).await;
    }
    kind
}

/// Decode `seek_col_idx` (`pack_pk_cols(col_indices)` — the packed flag at bit
/// 63 is always set, so the old `col_idx as usize >= num_columns` guard would
/// always trip) and validate the full list against the table's schema before
/// classifying. Used by the SEEK_BY_INDEX handler.
fn validated_index_cols(
    shared: &Rc<Shared>,
    target_id: i64,
    seek_col_idx: u64,
    op: &str,
) -> Result<gnitz_wire::PkColList, String> {
    let cols = gnitz_wire::unpack_pk_cols(seek_col_idx);
    match shared.cat().get_schema_desc(target_id) {
        Some(s) if s.cols_in_range(&cols) => Ok(cols),
        _ => Err(format!("{op}: invalid column list for table {target_id}")),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_seek_by_index(
    shared: &Rc<Shared>,
    peer: &Peer,
    client_id: u64,
    target_id: i64,
    kind: RelationKind,
    seek_col_idx: u64,
    seek_pk: u128,
    seek_pk_extra: &[u8],
    client_version: u16,
) {
    if kind != RelationKind::SystemCatalog {
        let cols = match validated_index_cols(shared, target_id, seek_col_idx, "seek_by_index") {
            Ok(cols) => cols,
            Err(msg) => {
                send_error(peer, target_id, client_id, msg.as_bytes()).await;
                return;
            }
        };
        // Single catalog scan (exact list match) answers "is there an index for
        // this column list"; the borrow ends with the condition, so none is held
        // across the await below.
        if shared
            .cat()
            .index_circuit_for_cols(target_id, cols.as_slice())
            .is_none()
        {
            // No secondary index for this column list: a dedicated control-only
            // status, caught here with zero worker dispatch, so the SQL planner
            // falls back to a scan or a CREATE INDEX hint without a prior catalog
            // probe.
            send_control_only(peer, target_id, client_id, STATUS_NO_INDEX).await;
            return;
        }
        // Forward the wire frame verbatim (packed seek_col_idx, seek_pk +
        // seek_pk_extra) to the broadcast-and-merge fan-out.
        match MasterDispatcher::fan_out_seek_by_index_collect(
            shared.disp(),
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

/// The relation a RESOLVE names, with its kind — or `None` when it names none.
/// `Err` is the one hard failure a resolve has: an unusable request blob, or a
/// schema that does not exist.
///
/// The `dag.relation_kind` lookup is the gate that makes the id safe to
/// describe: `read_column_defs` seeks `pack_column_id(owner_id, 0)`, whose range
/// check would abort the master on an out-of-range owner, and only ids
/// `allocate_table_id` issued reach `dag.tables`. Returning the kind is what
/// lets the caller consume that proof instead of re-asserting it.
fn resolve_request_target(
    shared: &Rc<Shared>,
    target_id: i64,
    name_blob: &[u8],
) -> Result<Option<(i64, RelationKind)>, String> {
    // By-id: the tid is the client's, unvalidated until the gate below. By-name:
    // the blob is the canonical `"schema_name.relation_name"`, which is exactly
    // the `entity_by_qname` key.
    let candidate = if name_blob.is_empty() {
        target_id
    } else {
        let qname = std::str::from_utf8(name_blob).map_err(|_| "RESOLVE: name is not valid UTF-8".to_string())?;
        match shared.cat().entity_id_by_qname(qname) {
            Some(tid) => tid,
            None => {
                // Distinguish "no such schema" from "no such relation in it"
                // only here — a qname hit already implies its schema exists.
                // Split at the first `.`: that is where `qualified_name` joined
                // the two halves, and a name a validating front end would have
                // rejected can only misreport which half was missing.
                let (schema_name, _) = qname
                    .split_once('.')
                    .ok_or_else(|| format!("RESOLVE: '{qname}' is not a qualified relation name"))?;
                if !shared.cat().has_schema(schema_name) {
                    return Err(format!("Schema '{schema_name}' not found"));
                }
                return Ok(None);
            }
        }
    };
    // A qname hit is not evidence of registration: `apply_entity_caches` inserts
    // on the raw row sign while `hook_table_register` registers only on net-live,
    // so the two maps are not maintained on one liveness rule.
    Ok(shared.cat().dag.relation_kind(candidate).map(|kind| (candidate, kind)))
}

/// Build the RESOLVE reply: the schema block plus the [`RelDescriptorBlob`]
/// carrying what the block cannot (kind, placement, foreign keys, secondary
/// indexes). Served entirely from the typed caches the master already
/// maintains; it writes no SAL group and wakes no worker.
fn build_resolve_reply(
    shared: &Rc<Shared>,
    client_id: u64,
    target_id: i64,
    name_blob: &[u8],
) -> Result<PooledSendBuf, String> {
    let Some((tid, kind)) = resolve_request_target(shared, target_id, name_blob)? else {
        // Relation absent is a successful answer, not an error: the client owes a
        // different wording per entry point ("Table …", "Table or view …",
        // `Ok(None)`), so it renders it itself. An empty descriptor blob says so,
        // and `target_id = 0` names no relation.
        return Ok(encode_response_buffer(ipc::WireMsg {
            client_id,
            status: STATUS_OK,
            ..Default::default()
        }));
    };

    // Only a base table reports its placement. A view and a system family are
    // both *stamped* `Replicated`, but this bit is a planner hint for a reduce
    // built directly over a source, and a view's locality is settled by the
    // compiler from the new view's own stamped placement instead. Reporting the
    // stamp here would re-plan an aggregate over a view on a second authority.
    let replicated = kind.is_base_table() && shared.cat().dag.relation_is_replicated(tid);

    let defs = shared.cat().read_column_defs(tid);
    let fks: Vec<gnitz_wire::RelFk> = defs
        .iter()
        .enumerate()
        .filter(|(_, d)| d.fk_table_id != 0)
        .map(|(ci, d)| gnitz_wire::RelFk {
            col_idx: ci as u32,
            fk_col_idx: d.fk_col_idx,
            fk_table_id: d.fk_table_id as u64,
        })
        .collect();
    let indexes: Vec<gnitz_wire::RelIndex> = shared
        .cat()
        .index_circuits(tid)
        .iter()
        .map(|ic| gnitz_wire::RelIndex {
            cols: ic.col_indices,
            is_unique: ic.is_unique,
        })
        .collect();
    let blob = gnitz_wire::RelDescriptorBlob {
        is_view: kind.is_view(),
        replicated,
        fks,
        indexes,
    }
    .encode();

    // Clone the `Rc` block out of the cache so no `cat()` borrow outlives it.
    // The reply always carries the block: a resolving client holds no descriptor
    // to validate a version against.
    let (schema_block, server_version) = shared.get_schema_wire_block(tid);
    Ok(encode_response_buffer(ipc::WireMsg {
        target_id: tid as u64,
        client_id,
        flags: ipc::wire_flags_set_schema_version(0, server_version),
        status: STATUS_OK,
        prebuilt_schema_block: Some(schema_block.as_slice()),
        seek_pk_extra: &blob,
        ..Default::default()
    }))
}

/// Drive one tick of everything pending, returning with NO catalog lock held.
/// Views derive from source-table pushes through the DAG (IV.2), so a read of a
/// stale view must first flush the pending — possibly in-flight — tick carrying
/// its sources' deltas.
///
/// One pass suffices for every commit the caller can have observed. A commit at
/// zone LSN `L` is published before its ACK, so `published() >= L` by the time
/// the drain is requested; `run_tick` snapshots `published()` at tick start,
/// later still, and stores it in `last_tick_lsn` on success. Whatever the tid set
/// was, the completed tick therefore leaves `last_tick_lsn >= L`.
///
/// The trigger is sent even when nothing looks pending: the tick loop processes
/// triggers serially, so awaiting `done` also serializes behind a concurrent
/// Auto. Without it a large push fires Auto asynchronously and the read could
/// observe the view mid-tick, apparently empty until the next read.
///
/// MUST be called with NO catalog read lock held: the drain parks at `rx.await`,
/// and the writer-preferring `AsyncRwLock` held across that park would block DDL
/// writers and `tick_loop`'s own read lock — a three-way deadlock (BF-1).
///
/// A failed tick is reported rather than swallowed: its views are stale, and
/// serving them under `STATUS_OK` would be a silent stale read.
async fn drain_pending_ticks(shared: &Rc<Shared>) -> Result<(), String> {
    let (tx, rx) = oneshot::channel::<Result<(), String>>();
    shared.tick_tx.send(TickTrigger::Drain { done: tx });
    // A cancelled receiver means the tick loop is gone; treat it as done.
    if let Ok(Err(e)) = rx.await {
        return Err(e);
    }
    Ok(())
}

/// True when no un-ticked commit can reach `target`: every source feeding it —
/// transitively, through view sources — committed at or below the last completed
/// tick's watermark, so a completed tick has absorbed all of them.
///
/// Soundness, for a commit `C` to base table `S ∈ source_closure(target)` at zone
/// LSN `L` that was ACKed to a client. `record_commit_lsn` precedes that ACK on
/// both commit paths and records a `max`, so `commit_lsn_of(S) >= L`. If the test
/// passes then `L <= last_tick_lsn`, which some completed tick `T` took from its
/// `published()` snapshot. The committer marks `S` in `tick_rows` before it
/// publishes `L`, and only after the workers ACKed the write; `T`'s dequeue and
/// its snapshot are one await-free span on the single-threaded reactor, so the
/// mark preceded the dequeue and `S` was in `T`'s tid set. `T` therefore emitted
/// `S`'s tick group, whose `handle_tick` took the `pending_deltas` holding `C`
/// and fanned it along the same `dep` edges this closure mirrors.
///
/// Under-reporting (one extra drain) is possible and harmless: the test is stated
/// over ACKed commits, so a commit published but whose connection task died
/// before recording its LSN is invisible to it — and no client can have observed
/// such a write.
///
/// A non-view target is vacuously fresh — it has no sources — and answers before
/// the closure walk, so a base-table read never pays for one. Caller holds the
/// catalog read lock; the whole call is synchronous, so `source_closure`'s `&mut`
/// rebuild crosses no await.
fn read_is_fresh(shared: &Rc<Shared>, target: i64) -> bool {
    if !shared.cat().dag.relation_kind(target).is_some_and(|k| k.is_view()) {
        return true;
    }
    let ticked = shared.last_tick_lsn.get();
    shared
        .cat()
        .dag
        .source_closure(vec![target])
        .into_iter()
        .all(|s| shared.commit_lsn_of(s) <= ticked)
}

/// Take the catalog read lock and resolve `target_id`'s kind from the same probe
/// that validated it, draining pending ticks first only when the target is a view
/// `read_is_fresh` reports stale. Returns `(guard, kind)`, or `None` if the target
/// was rejected (error already sent). The one read-lock entry point for every
/// single-target read verb: each one routes on the returned kind rather than
/// re-deciding the system/user split from the id.
///
/// A base table's rows AND its secondary indexes are written by the same ingest
/// apply, which is what makes skipping the drain safe for an `IndexRange` bound
/// too. A stale view drops the lock, drains with NO lock held (BF-1), then
/// re-locks and re-resolves — a DDL may have dropped it during the drain.
async fn read_lock(
    shared: &Rc<Shared>,
    peer: &Peer,
    client_id: u64,
    target_id: i64,
) -> Option<(ReadGuard, RelationKind)> {
    {
        let g = shared.catalog_rwlock.read().await;
        let kind = resolve_read_target(shared, peer, client_id, target_id).await?;
        if read_is_fresh(shared, target_id) {
            return Some((g, kind));
        }
    }
    // No preliminary frame has gone out yet (`negotiate_scan_schema` runs later),
    // so a failed drain is still reportable as a plain error.
    if let Err(e) = drain_pending_ticks(shared).await {
        send_error(peer, target_id, client_id, e.as_bytes()).await;
        return None;
    }
    let g = shared.catalog_rwlock.read().await;
    let kind = resolve_read_target(shared, peer, client_id, target_id).await?;
    Some((g, kind))
}

/// A scan's captured preliminary schema frame content: `(wire block,
/// server_version)`, present only on a schema-cache miss (the client's cached
/// version is stale, so the master emits the block once and the workers omit it).
type PrelimSchema = Option<(Rc<Vec<u8>>, u16)>;

/// Resolve a scan's per-relation schema negotiation: compare the client's cached
/// `client_version` against the server's. On a miss, capture the wire schema
/// block so the master can send ONE preliminary schema frame (via
/// [`build_prelim_schema_frame`]) instead of N per-worker copies, and bump the
/// effective client version to the server's so the workers omit their own block.
/// Returns `(prelim, effective_client_version)`: `prelim` is
/// `Some((block, server_version))` on a miss, `None` on a warm-cache hit. Shared
/// by `handle_scan` (emits inline) and `scan_multi_body` (captures, emits in the
/// deferred one-cut Phase 2).
fn negotiate_scan_schema(shared: &Rc<Shared>, tid: i64, client_version: u16) -> (PrelimSchema, u16) {
    let server_version = shared.cat().get_schema_version(tid);
    if gnitz_wire::wire_should_include_schema(client_version, server_version) {
        let (block, _) = shared.get_schema_wire_block(tid);
        (Some((block, server_version)), server_version)
    } else {
        (None, client_version)
    }
}

/// Build the preliminary schema-only frame — carrying `FLAG_CONTINUATION`, the
/// `server_version`, and the captured wire block — that precedes a scan's data
/// frames on a schema-cache miss. The caller chooses when to send it: inline for
/// a single scan, deferred to the one-cut Phase 2 for a multi-scan.
fn build_prelim_schema_frame(tid: i64, client_id: u64, server_version: u16, block: &[u8]) -> PooledSendBuf {
    let prelim_flags = ipc::wire_flags_set_schema_version(ipc::FLAG_CONTINUATION, server_version);
    encode_response_buffer(ipc::WireMsg {
        target_id: tid as u64,
        client_id,
        flags: prelim_flags,
        status: STATUS_OK,
        prebuilt_schema_block: Some(block),
        ..Default::default()
    })
}

async fn handle_scan(shared: &Rc<Shared>, peer: &Peer, client_id: u64, target_id: i64, client_version: u16) {
    let Some((_g, _kind)) = read_lock(shared, peer, client_id, target_id).await else {
        return;
    };
    let lsn = shared.last_tick_lsn.get();

    // On a schema-cache miss, master sends one preliminary schema-only frame
    // before dispatching workers, eliminating the N per-worker schema blocks.
    let (prelim, effective_client_version) = negotiate_scan_schema(shared, target_id, client_version);
    if let Some((block, server_version)) = prelim {
        let frame = build_prelim_schema_frame(target_id, client_id, server_version, block.as_slice());
        if peer.send_buffer(frame).await < 0 {
            peer.close();
            return;
        }
    }

    // `0` (worker-0 unicast) for a replicated relation — its full copy lives on
    // every worker, so a broadcast would concatenate W identical copies — else
    // `-1` (broadcast).
    let unicast = replicated_unicast(shared.disp(), target_id);
    // Embed the client's schema version in wire_flags so workers can decide
    // whether to include the schema block in their response.
    let result = MasterDispatcher::fan_out_scan(
        shared.disp(),
        &shared.reactor,
        &shared.sal_writer_excl,
        unicast,
        target_id,
        client_id,
        peer,
        0,
        gnitz_wire::wire_flags_set_schema_version(0, effective_client_version),
        &[],
    )
    .await;
    finish_scan_fanout(peer, target_id, client_id, lsn, result).await;
}

fn make_terminal_scan_frame(target_id: i64, client_id: u64, lsn: u64) -> PooledSendBuf {
    // Terminal scan frame: no schema block, no data. Client ignores schema version here.
    encode_response_buffer(ipc::WireMsg {
        target_id: target_id as u64,
        client_id,
        seek_pk: lsn as u128,
        status: STATUS_OK,
        ..Default::default()
    })
}

/// Finish one scan-shaped fan-out: `Ok(true)` → the terminal frame (stamped
/// with the pre-dispatch `lsn`), `Ok(false)` → the forward already failed
/// (close the peer), `Err` → the error frame. Shared by the plain scan and the
/// ScanSpec handler.
async fn finish_scan_fanout(peer: &Peer, target_id: i64, client_id: u64, lsn: u64, result: Result<bool, String>) {
    match result {
        Ok(true) => {
            let terminal = make_terminal_scan_frame(target_id, client_id, lsn);
            peer.send_buffer_or_close(terminal).await;
        }
        Ok(false) => peer.close(),
        Err(e) => send_error(peer, target_id, client_id, e.as_bytes()).await,
    }
}

/// Parameterized bounded read (`ReadSpec`). The scan pipeline, minus schema
/// negotiation: the client authors the reply schema and ships it in
/// `seek_pk_extra`, so the reply carries no schema block. `read_lock` still drains a
/// view target's pending ticks first (freshness), and terminal-frame/error
/// handling are identical to `handle_scan`; only the routing differs, since a
/// bound can confine the read to one worker.
async fn handle_scan_spec(shared: &Rc<Shared>, peer: &Peer, client_id: u64, target_id: i64, seek_pk_extra: &[u8]) {
    let Some((_g, kind)) = read_lock(shared, peer, client_id, target_id).await else {
        return;
    };
    // A `ReadSpec` has only a fan-out realization, and every worker holds a full
    // copy of a catalog family — so fanning one out would concatenate W identical
    // trains and inflate every row's weight W-fold, all replying STATUS_OK.
    if kind == RelationKind::SystemCatalog {
        let msg = format!("SCAN_SPEC: {target_id} is not a user relation");
        send_error(peer, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    let lsn = shared.last_tick_lsn.get();
    // A PK range confined to one partition unicasts: one SAL slot instead of W,
    // each of which would carry its own copy of the spec blob under the exclusive
    // SAL mutex.
    let unicast = scan_spec_route(shared.disp(), target_id, seek_pk_extra);
    let result = MasterDispatcher::fan_out_scan(
        shared.disp(),
        &shared.reactor,
        &shared.sal_writer_excl,
        unicast,
        target_id,
        client_id,
        peer,
        FLAG_SCAN_SPEC,
        0,
        seek_pk_extra,
    )
    .await;
    finish_scan_fanout(peer, target_id, client_id, lsn, result).await;
}

/// One relation's Phase-1 capture for `scan_multi_body`: its tid and the
/// preliminary schema frame to emit in Phase 2, as `(wire block, server_version)`
/// — `Some` iff the client's cached version missed, in which case the workers
/// were told (via `effective_client_version = server_version`) to omit their own
/// schema blocks. Exactly the `negotiate_scan_schema` `prelim` result, carried to
/// the deferred emit.
struct ScanMultiRelPlan {
    tid: i64,
    prelim: PrelimSchema,
}

/// SCAN_MULTI: snapshot N relations at one SAL cut and stream N reply trains in
/// request order. The read-side completion of the atomic multi-table write
/// story: an atomic commit is either wholly before the cut (visible in every
/// train) or wholly after (visible in none), never torn across the result set.
async fn handle_scan_multi(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8]) {
    match scan_multi_body(shared, peer, client_id, data).await {
        // Phase 2 already streamed every train and terminal.
        Ok(true) => {}
        // Client disconnected mid-stream: leases dropped in the body, close the peer.
        Ok(false) => peer.close(),
        // Shape/tid rejection (before any group is written) or a worker fault
        // mid-stream (leases already dropped in the body): one error frame. The
        // client discards any partial results it read.
        Err(e) => send_error(peer, 0, client_id, e.as_bytes()).await,
    }
}

/// Body of `handle_scan_multi`. `Ok(true)` once every relation's train and
/// terminal have been sent; `Ok(false)` on client disconnect; `Err(msg)` on a
/// shape/tid rejection or a mid-stream worker fault. All `ScanLease`s live in
/// the `dispatches` vec and drop on return, so any error/disconnect return
/// deregisters every id and discards undrained frames at the ring boundary.
async fn scan_multi_body(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8]) -> Result<bool, String> {
    // ── Phase 0: decode + frame-local shape rules ──────────────────────────
    // The count/duplicate shape rules are the shared client/server validator
    // (`gnitz_wire::validate_scan_multi_tids`); this is the authoritative check —
    // a client may skip its own copy. tid legality is resolved in Phase 1 under
    // the catalog lock.
    let relations = ipc::decode_scan_multi(data).map_err(|e| format!("decode error: {e}"))?;
    let tids: Vec<u64> = relations.iter().map(|(tid, _)| *tid).collect();
    gnitz_wire::validate_scan_multi_tids(&tids)?;

    // Drain once if any target is a stale view — the same test `read_lock` runs
    // for a single target — and with NO catalog
    // lock held (BF-1). The classifying lock is dropped before the drain; Phase 1
    // re-resolves every tid's kind under a fresh lock, so a DDL during the drain
    // is caught there, and an unknown tid is rejected there rather than here.
    let needs_drain = {
        let _cat = shared.catalog_rwlock.read().await;
        tids.iter().any(|&t| !read_is_fresh(shared, t as i64))
    };
    if needs_drain {
        drain_pending_ticks(shared).await?;
    }
    // The shared LSN stamped into every terminal.
    let lsn = shared.last_tick_lsn.get();

    // ── Phase 1: catalog lock — resolve shapes + schemas, dispatch one cut ──
    // Resolve every relation from the same catalog snapshot (so all N are
    // consistent with the cut even if a DDL commits during the later drain),
    // then write all N groups under one `sal_writer_excl` hold. The
    // catalog-read ⊃ `sal_writer_excl` order matches every other SAL writer, so
    // no lock inversion.
    let (dispatches, plans) = {
        let _cat = shared.catalog_rwlock.read().await;
        let mut plans: Vec<ScanMultiRelPlan> = Vec::with_capacity(relations.len());
        let mut fanout: Vec<(i64, Fanout, u16)> = Vec::with_capacity(relations.len());
        for &(tid_u, client_ver) in &relations {
            let tid = tid_u as i64;
            // Base tables AND views are legal; a catalog family stays on the
            // plain path, which serves it master-locally.
            match shared.cat().dag.relation_kind(tid) {
                None => return Err(format!("table {tid} not found")),
                Some(RelationKind::SystemCatalog) => return Err(format!("SCAN_MULTI: {tid} is not a user relation")),
                Some(_) => {}
            }
            // `0` (worker-0 unicast) for a replicated relation, `-1` (broadcast)
            // otherwise — the same policy `handle_scan` applies per relation.
            let unicast = replicated_unicast(shared.disp(), tid);
            // Capture (not emit) each relation's preliminary schema frame here so
            // Phase 2 can send it after the one-cut dispatch, in request order.
            let (prelim, effective_client_version) = negotiate_scan_schema(shared, tid, client_ver);
            plans.push(ScanMultiRelPlan { tid, prelim });
            fanout.push((tid, unicast, effective_client_version));
        }
        let dispatches = dispatch_scan_multi_fanout(
            shared.disp(),
            &shared.reactor,
            &shared.sal_writer_excl,
            client_id,
            &fanout,
        )
        .await?;
        // Release the catalog read lock here: Phase 2 touches no catalog state
        // (the snapshot is worker-frozen and the schemas are captured), so
        // holding it across the whole bulk read would needlessly block DDL.
        (dispatches, plans)
    };

    // ── Phase 2: sequential per-relation drain (no locks; holds all leases) ──
    let nw = shared.disp().num_workers();
    for (plan, d) in plans.iter().zip(&dispatches) {
        // Preliminary schema-only frame first, when captured in Phase 1.
        if let Some((block, server_version)) = plan.prelim.as_ref() {
            let frame = build_prelim_schema_frame(plan.tid, client_id, *server_version, block.as_slice());
            if peer.send_buffer(frame).await < 0 {
                return Ok(false);
            }
        }
        // Drain this relation's train (all workers, ascending) before the next —
        // the FIFO reply contract makes request order == ring order.
        match MasterDispatcher::await_and_drain_scan_relation(&shared.reactor, peer, d.unicast, &d.req_ids, nw).await {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(e) => return Err(e),
        }
        // Terminal frame for this relation (tid + the shared LSN).
        let terminal = make_terminal_scan_frame(plan.tid, client_id, lsn);
        if peer.send_buffer(terminal).await < 0 {
            return Ok(false);
        }
    }
    Ok(true)
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

/// The bundle's batch for system family `tid`, if it carries one. A DDL bundle
/// holds at most one family per tid.
fn bundle_family(families: &[(SysFamily, Batch)], family: SysFamily) -> Option<&Batch> {
    families.iter().find(|(f, _)| *f == family).map(|(_, b)| b)
}

/// Resolve `tid`'s system-family schema and decode a client wal-block slice
/// against it — the master's OWN registered layout, so a client cannot dictate
/// how its bytes are read. `sys_family_schema` rejects a bogus family tid
/// without the panic `sys_tab_schema` would hit on an unknown id in the system
/// range. Used by the DDL_TXN bundle decode.
fn decode_sys_family(tid: i64, slice: &[u8]) -> Result<(SysFamily, Batch), String> {
    let family = SysFamily::from_id(tid).ok_or_else(|| format!("{tid} is not a system family"))?;
    let batch = decode_client_batch(slice, &family.schema()).map_err(|e| format!("family {tid} decode error: {e}"))?;
    Ok((family, batch))
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
    let mut families: Vec<(SysFamily, Batch)> = Vec::with_capacity(family_count);
    for &(tid, slice) in &raw_families {
        match decode_sys_family(tid as i64, slice) {
            Ok(fb) => families.push(fb),
            Err(e) => {
                let msg = format!("DDL_TXN: {e}");
                send_error(peer, 0, client_id, msg.as_bytes()).await;
                return;
            }
        }
    }

    // A CREATE VIEW is a stop-the-world op (source drain + distributed backfill,
    // reactor parked). The VIEW_TAB family's +1 rows, if any, are the new views;
    // they alone need the lock-held barrier and the in-loop source drain below.
    let new_view_ids: Vec<i64> =
        bundle_family(&families, SysFamily::View).map_or_else(Vec::new, |b| family_pks_by_sign(b, true));
    let view_create = !new_view_ids.is_empty();

    // Drain the committer barrier BEFORE acquiring the catalog write lock. The
    // barrier flushes user-table WAL and waits for worker ACKs (tens of ms under
    // load); holding the write lock across that wait would block every concurrent
    // SCAN/SEEK read for no reason — no catalog mutation happens until after the
    // barrier returns.
    let t_ddl_start = Instant::now();
    await_barrier(shared, BarrierKind::Ddl).await;

    // Then quiesce the tick subsystem, while the reactor still runs and before
    // the write lock (run_tick/relay_loop take the read lock, so a
    // write-lock-held quiesce would deadlock). `TickGate` releases it on every
    // exit path.
    //
    // Every bundle, not just the stop-the-world ones: a worker parked mid-epoch
    // defers the broadcast DdlSync but keeps serving reads and pushes inline, so
    // a TABLE/COL/IDX mutation ACKed in that window leaves that worker answering
    // client traffic against a catalog the client was just told had changed.
    //
    // After the barrier, not concurrently with it: the checkpoint sequence sends
    // its own Drain then Quiesce, and a DDL Quiesce queued ahead of that Drain
    // parks the tick loop on a release this handler only sends once the barrier
    // returns — which needs the sequence to finish.
    with_ddl_window(shared, async {
        let catalog_write = shared.catalog_rwlock.write().await;

        if view_create {
            // Lock-held committer barrier: a push could have committed between the
            // pre-lock barrier and the write lock; flush it so every straggler is
            // resident in pending_deltas before the in-loop source drain. The
            // committer stays idle for the rest of the handler (the write lock blocks
            // new pushes).
            await_barrier(shared, BarrierKind::Ddl).await;
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
        // is not yet in `dag.tables` and `validate_unique_index_create`
        // short-circuits to an empty filter (sound: the new table is empty, and
        // hook_index_register's own owner-check still succeeds later in the loop). The
        // IDX_TAB row layout (and the IDXTAB_PAY_* payload indices) is fixed by
        // `create_index` and read identically by `hook_index_register`.
        let mut filter_seeds: Vec<(i64, u64, UniqueFilter)> = Vec::new();
        for (owner_id, packed, cols) in bundle_family(&families, SysFamily::Index)
            .map(idx_tab_unique_creates)
            .unwrap_or_default()
        {
            match MasterDispatcher::validate_unique_index_create(
                shared.disp(),
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
                // Hold the pre-flight's filter to publish post-commit, keyed by
                // the packed column list (the filter-map key).
                Ok(filter) => filter_seeds.push((owner_id, packed, filter)),
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

        // The post-fsync reclamation needs the durably-dropped relation ids and
        // (owner, packed-cols) pairs (the -1 rows); the ingest loop consumes
        // `families`, so extract those minimal lists now instead of cloning the whole
        // TABLE_TAB / VIEW_TAB / IDX_TAB batches. A bundle is one DDL, so at most one
        // family carries -1 rows; a CREATE bundle yields empty lists.
        let dropped_tids: Vec<i64> =
            bundle_family(&families, SysFamily::Table).map_or_else(Vec::new, |b| family_pks_by_sign(b, false));
        let dropped_view_ids: Vec<i64> =
            bundle_family(&families, SysFamily::View).map_or_else(Vec::new, |b| family_pks_by_sign(b, false));
        let dropped_indices: Vec<(i64, u64)> = bundle_family(&families, SysFamily::Index)
            .map(idx_tab_drops)
            .unwrap_or_default();

        // Ingest the families in ascending topo order so every register/index hook
        // sees its dependencies already in the memtable. For a CREATE VIEW, drain the
        // new view's base sources once the circuit families are in the memtable
        // (so get_source_ids resolves) but before VIEW_TAB registers the view — after
        // registration the view is a dependent of those bases, so an undrained pending
        // delta would tick it through `evaluate_dag` over rows the backfill below also
        // scans, counting them twice. VIEW_TAB is the first family at or past view
        // priority.
        // The between-precheck-and-apply marker holds the single family that was
        // applied but not yet enqueued (a hook/panic failure), which compensation must
        // negate; a precheck failure leaves the marker None, so no ghost -1 is written.
        // The ingest loop writes nothing to the SAL (broadcasts are queued and emitted
        // only in the tail below), so the in-loop drain's tick precedes the zone's
        // broadcasts in SAL order exactly as before.
        families.sort_by_key(|(f, _)| f.topo_priority());
        let view_prio = SysFamily::View.topo_priority();
        let mut applied_not_enqueued: Option<(i64, Batch)> = None;
        let mut drained_sources = false;
        let ingest_res = guard_panic("DDL", || {
            let cat = unsafe { &mut *cat_ptr_raw };
            for (family, fbatch) in families {
                if view_create && !drained_sources && family.topo_priority() >= view_prio {
                    for src in cat.dag.base_tables_reachable_from(new_view_ids.clone()) {
                        shared.disp().drain_tick_blocking(src)?;
                    }
                    drained_sources = true;
                }
                cat.precheck_family(family, &fbatch)?;
                applied_not_enqueued = Some((family.id(), fbatch.clone()));
                cat.apply_and_enqueue_family(family, fbatch)?;
                applied_not_enqueued = None;
            }
            // Compile every new view's circuit here, on the master, while the bundle
            // is still undoable. VIEW_TAB has been applied, so each view is registered
            // and every source resolves — and nothing has reached the SAL yet, so a
            // rejection leaves through the arm below with the view uncreated.
            // Compiling only on the workers, as the backfill does, puts the verdict
            // after the DDL is durable, where it can be nothing but a log line and a
            // view that returns no rows forever.
            for &vid in &new_view_ids {
                cat.preflight_view_compile(vid)?;
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
        // this DROP; removing here races a lagging worker's child-dir create).
        shared.lsn_alloc.publish(zone_lsn);
        unsafe {
            (*cat_ptr_raw).ctx.close_ddl_zone();
            (*cat_ptr_raw).defer_pending_dir_deletions();
        }

        // Invalidate unique-filter state for durably-dropped tables/indices so a
        // recreated table with the same ID does not inherit stale filter entries.
        for &tid in &dropped_tids {
            shared.disp().unique_filter_invalidate_table(tid);
        }
        // Relation ids are never reissued within a boot, so these entries are dead.
        for &id in dropped_tids.iter().chain(&dropped_view_ids) {
            shared.forget_relation(&catalog_write, id);
        }
        // Keying by the whole packed list means dropping `(a, b)` never clears a
        // distinct single-column filter on `a`.
        for &(owner_id, packed) in &dropped_indices {
            shared.disp().unique_filter_remove(owner_id, packed);
        }

        // Publish the pre-flight's filters so the first INSERT skips a redundant
        // full-cluster warmup scan. Post-fsync only: a broadcast/fsync failure
        // aborts the process before this point, so no filter is published for an
        // index that never committed.
        for (owner_id, packed, filter) in filter_seeds {
            shared.disp().unique_filter_seed(owner_id, packed, filter);
        }

        // Populate every new view. A post-fsync Err cannot be rolled back (the CREATE
        // is durable), so abort — restart's boot rebuild refills it.
        let gen_before = unsafe { (*cat_ptr_raw).durable_generation };
        guard_panic("view-backfill", || {
            shared.disp().backfill_views_in_dep_order(&new_view_ids)
        })
        .unwrap_or_else(|e| {
            gnitz_fatal_abort!(
                "live CREATE VIEW backfill failed after the CREATE was made durable: {}",
                e
            );
        });

        // `checkpoint_before_backfill` is the only thing above that bumps. If it
        // fired, every view and index is invalid on disk right now; finish the
        // checkpoint here, while the reactor is still parked and the tick loop
        // still quiesced, rather than leaving the database rebuild-on-boot until
        // something wakes the committer.
        if unsafe { (*cat_ptr_raw).durable_generation } != gen_before {
            let mut pending = Vec::new();
            shared.drain_tick_rows_into(&mut pending);
            // Mirrors the tick loop's own filter: a tid this very DDL dropped is
            // not ticked.
            pending.retain(|&tid| shared.cat().has_id(tid));
            guard_panic("view-restamp", || shared.disp().restamp_derived(&pending)).unwrap_or_else(|e| {
                gnitz_fatal_abort!("re-stamping derived state after a CREATE VIEW reclaim failed: {}", e);
            });
        }

        send_ok_response(shared, peer, 0, None, client_id, zone_lsn as u128, client_version).await;
        let total = t_ddl_start.elapsed();
        if total > Duration::from_millis(20) {
            gnitz_debug!("DDL_TXN SLOW total={:?} families={}", total, family_count);
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// Wire-protocol response helpers
// ---------------------------------------------------------------------------

/// Frame one reply into a pooled send buffer. Callers build the [`ipc::WireMsg`]
/// with the fields they actually set and leave the rest at `Default`.
fn encode_response_buffer(msg: ipc::WireMsg<'_>) -> PooledSendBuf {
    let sz = msg.size();
    let total = 4 + sz;
    let mut inner = crate::storage::batch_pool::acquire_buf();
    inner.reserve(total.max(8192));
    // SAFETY: `encode_ipc` writes every byte [0, sz). The 4-byte frame header is
    // written immediately below. wal::encode zeros inter-region padding (Step 1),
    // so no byte is left uninitialised regardless of column type.
    #[allow(clippy::uninit_vec)]
    unsafe {
        inner.set_len(total);
    }
    inner[0..4].copy_from_slice(&(sz as u32).to_le_bytes());
    let written = msg.encode_ipc(&mut inner[4..total], 0);
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
    let buf = encode_response_buffer(ipc::WireMsg {
        target_id: target_id as u64,
        client_id,
        flags: ipc::wire_flags_set_schema_version(0, server_version),
        seek_pk,
        status: STATUS_OK,
        data: ipc::WireData::Whole(result),
        prebuilt_schema_block: schema_arg,
        ..Default::default()
    });
    peer.send_buffer_or_close(buf).await;
}

/// Control-only reply carrying just a status code and a target id: no schema,
/// no data, no error text. Every reply whose whole content is the header — the
/// schema-mismatch and no-index signals, and an id allocation, whose answer *is*
/// the target id — goes out through here.
async fn send_control_only(peer: &Peer, target_id: i64, client_id: u64, status: u32) {
    let buf = encode_response_buffer(ipc::WireMsg {
        target_id: target_id as u64,
        client_id,
        status,
        ..Default::default()
    });
    peer.send_buffer_or_close(buf).await;
}

async fn send_error(peer: &Peer, target_id: i64, client_id: u64, error_msg: &[u8]) {
    // STATUS_ERROR suppresses the schema block (has_schema = false), so
    // prebuilt_schema = None is correct and saves the cache lookup.
    // flags=0: client ignores schema version on error responses.
    let buf = encode_response_buffer(ipc::WireMsg {
        target_id: target_id as u64,
        client_id,
        status: STATUS_ERROR,
        error_msg,
        ..Default::default()
    });
    peer.send_buffer_or_close(buf).await;
}

/// Existence + writability gate shared by the empty-push and INSERT arms.
/// A view registers into the same id space as base tables (shared
/// `next_table_id`), so a raw client push addressed to a view tid would
/// otherwise commit rows into the view's output store that its circuit
/// never produced — permanently divergent derived state. Only base tables
/// are push targets. Caller holds the catalog read lock. Returns `true` iff
/// an error frame was sent and the caller must return.
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
/// or `None` if it can. The shared existence + writability gate behind the
/// plain-push arms (via `push_target_rejected`), the per-family check in
/// `push_txn_body`, and the SERIAL range reservation — all of which address a
/// base table by id and own their own reply path.
fn push_target_error(shared: &Shared, target_id: i64) -> Option<String> {
    match shared.cat().dag.relation_kind(target_id) {
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
    let disp = shared.disp();
    if let Err(e) = guard_panic(op, || unsafe {
        // The first family opens the zone; a failure here aborts the loop, so no
        // later family can become the first one recovery sees.
        for (i, (tid, bat)) in drained.iter().enumerate() {
            disp.broadcast_ddl(*tid, bat, zone_lsn, i == 0)?;
        }
        // Abort after broadcasts but BEFORE the commit sentinel — exercises the
        // recovery skip of a half-written zone.
        if DDL_PANIC.at("after_broadcasts") {
            libc::abort();
        }
        // An empty zone has no groups for recovery to gate, so its sentinel
        // records nothing. Skipping it also makes "every sentinel follows an
        // ordinary group" true by construction, which is what keeps a run of
        // sentinels from reaching the checkpoint reserve.
        // `apply_and_enqueue_family` drops empty batches, so a DDL bundle whose
        // families all net to empty arrives here with `drained` empty.
        if !drained.is_empty() {
            disp.commit_zone(zone_lsn)?;
        }
        Ok::<(), String>(())
    }) {
        gnitz_fatal_abort!("{} broadcast failed after in-memory catalog mutation: {}", op, e);
    }
    shared.reactor.fsync(shared.disp().sal_fd())
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
/// RESOLVE / tick emission, all of which take `catalog_rwlock.read()` — block
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
/// none of `handle_ddl_txn`'s prelude (committer barrier, tick quiesce,
/// VIEW-only base-table drain): a `sys_sequences` advance has no DAG evaluation
/// and no rollback path, and the row it broadcasts is one no worker reads — a
/// worker that defers this `DdlSync` past a mid-epoch push answers every client
/// verb identically meanwhile.
async fn commit_serial_range_durable(shared: &Rc<Shared>, seq_id: i64, count: i64) -> Result<i64, String> {
    let (base, zone_lsn, fsync_fut) = {
        // Lock order catalog -> SAL, matching INSERT/SEEK, so acquiring SAL under
        // catalog.write cannot deadlock. Both guards drop at the end of this block.
        let _write = shared.catalog_rwlock.write().await;

        // A SERIAL sequence id IS the owning table's id, so the push-writability
        // gate answers here too. Checked under the write lock that guards the
        // reservation: an unvalidated id would durably write a `sys_sequences`
        // row that `recover_sequences` replays straight into the catalog's own
        // id counters at the next open.
        if let Some(e) = push_target_error(shared, seq_id) {
            return Err(e);
        }

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
    Ok(base)
}
