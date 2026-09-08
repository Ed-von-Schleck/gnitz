//! Server executor: the process lifecycle, the `Shared` state, the request
//! router and every read/push handler, and the reply-frame vocabulary. The
//! catalog-zone write path is the child `ddl`.
//!
//! The master owns one `Reactor` driving the accept socket, a task per
//! connection, the committer (group commit + checkpoint + fsync), the tick task,
//! the relay task and the worker-crash watchdog. The reactor demuxes
//! FLAG_EXCHANGE wires into an accumulator and hands completed views to the relay.
//!
//! A handler that splits into `handle_x` + `x_body` does so for one reason: every
//! rejection inside the body is a plain `Err`, so the handler above owns the
//! single reply path.

mod ddl;

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use gnitz_store::storage::batch_pool::PooledSendBuf;
use rustc_hash::FxHashMap;

use super::guard_panic;
use crate::runtime::tls::{TlsListener, TlsShared};
use gnitz_store::foundation::fault::Seam;

use self::ddl::{commit_serial_range_durable, handle_ddl_txn, hold_relay_for_ddl, RELAY_HOLD_FOR_DDL};
use super::TxnFamily;
use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::runtime::committer::{self, BarrierKind, CommitRequest, PendingPush, PendingTxn};
use crate::runtime::lsn::ZoneLsnAllocator;
use crate::runtime::master::{
    await_worker_acks, dispatch_scan_multi_fanout, exchange::ExchangeAccumulator, read_fanout, Fanout,
    MasterDispatcher, WorkerFault,
};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{
    chan, oneshot, select2, AsyncRwLock, Either, Reactor, ReadGuard, ReplyFuture, WriteGuard,
};
use crate::runtime::sal::{DirectGroup, GroupTargets, SalFit, SalMessageKind};
use crate::runtime::wire::{self as ipc, validate_schema_match, BACKFILL_DECISION_CONTINUE};
use gnitz_store::relation::RelationKind;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;
use gnitz_wire::txn_frame::validate_item_ids;
use gnitz_wire::{WireFault, STATUS_ERROR, STATUS_NOT_FOUND, STATUS_NO_INDEX, STATUS_OK, STATUS_SCHEMA_MISMATCH};

const TICK_COALESCE_ROWS: usize = 10_000;
const WORKER_WATCH_MS: u64 = 100;

/// `GNITZ_INJECT_RELAY_SPACE_LOW`: report one exchange relay's SAL space as low,
/// so tests drive the reclamation protocol (worker re-epoch, master
/// `checkpoint_reset`, epoch advancing) over a small table that would never
/// approach the 1 GiB mmap.
static RELAY_SPACE_LOW: Seam = Seam::new("GNITZ_INJECT_RELAY_SPACE_LOW");

/// `GNITZ_INJECT_PUSH_HOLD_FOR_DDL`: see `hold_push_for_ddl`.
static PUSH_HOLD_FOR_DDL: Seam = Seam::new("GNITZ_INJECT_PUSH_HOLD_FOR_DDL");

/// Poll bound for [`PUSH_HOLD_FOR_DDL`], in 1 ms ticks.
const PUSH_HOLD_MAX_POLLS: u32 = 2_000;

/// Park in 1 ms reactor ticks until `ready`, or until `polls` elapse. The shape
/// every hold-seam needs: it holds no lock while it waits, so the event it waits
/// for can actually happen, and the bound releases it if that event never comes —
/// a misarmed test then fails on its own assertion instead of wedging the node.
///
/// Polling rather than a handle keeps each seam self-contained: nothing outside
/// one needs to know it exists.
async fn park_until(shared: &Shared, polls: u32, what: &str, ready: impl Fn() -> bool) {
    for _ in 0..polls {
        if ready() {
            return;
        }
        shared.reactor.timer(Instant::now() + Duration::from_millis(1)).await;
    }
    gnitz_warn!("{}: seam armed but the event never arrived; releasing", what);
}

/// Hold one decoded push, before its catalog read lock, until a DDL has moved
/// `target_id`'s schema version — the window a warm push's decode-time descriptor
/// goes stale in, which production reaches when that read parks behind a queued
/// `ALTER TABLE` writer.
async fn hold_push_for_ddl(shared: &Shared, target_id: i64) {
    let seen = shared.cat().get_schema_version(target_id);
    park_until(shared, PUSH_HOLD_MAX_POLLS, "push hold", || {
        shared.cat().get_schema_version(target_id) != seen
    })
    .await
}

use gnitz_wire::ClientVerb;

/// One tick request to `tick_loop`. Minted only by `request_drain`,
/// `request_quiesce` and [`Shared::note_commit_rows`].
enum TickTrigger {
    /// Fire-and-forget trigger from INSERT when a tid crosses the row
    /// coalesce threshold.  Tids come from `tick_rows`.
    Auto,
    /// Explicit drain requested by a read or by the checkpoint: tick whatever is
    /// pending — even nothing — and report the tick's verdict on `done`. A reader
    /// that waited on a failed tick must be told: its view is stale, and reporting
    /// success would serve stale rows under `STATUS_OK`.
    Drain {
        done: oneshot::Sender<Result<(), WireFault>>,
    },
    /// Pause the tick subsystem. On dequeue the loop sends a [`TickPark`] on
    /// `acked` — proving no tick is in flight (the loop is serial) and none will
    /// start — then blocks until that token is dropped. The ack therefore also
    /// means any in-flight exchange tick has drained, which is what
    /// `handle_ddl_txn` waits on.
    Quiesce { acked: oneshot::Sender<TickPark> },
}

/// Holding one parks the tick loop; dropping it is the release, so no path can
/// forget to send one — the tick loop's `release.await` resolves `None` either
/// way.
pub(super) type TickPark = oneshot::Sender<()>;

/// Ask the tick loop to tick everything pending and report the tick's verdict.
pub(super) fn request_drain(shared: &Shared) -> oneshot::Receiver<Result<(), WireFault>> {
    let (done, rx) = oneshot::channel();
    shared.tick_tx.send(TickTrigger::Drain { done });
    rx
}

/// Ask the tick loop to park. The reply carries the token whose drop releases
/// it; `None` means the tick loop was already gone, so there is nothing parked.
pub(super) fn request_quiesce(shared: &Shared) -> oneshot::Receiver<TickPark> {
    let (acked, rx) = oneshot::channel();
    shared.tick_tx.send(TickTrigger::Quiesce { acked });
    rx
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
    dispatcher: Rc<MasterDispatcher>,
    committer_tx: chan::Sender<CommitRequest>,
    catalog_rwlock: Rc<AsyncRwLock>,
    /// Tick trigger sender. Reached only through `request_drain`,
    /// `request_quiesce` and [`Shared::note_commit_rows`], so every trigger this
    /// process sends is minted in one place.
    tick_tx: chan::Sender<TickTrigger>,
    /// Zone-LSN allocation high-water + durability watermark, read by the
    /// committer so SCAN/SEEK handlers report the same LSN it assigns.
    pub(super) lsn_alloc: ZoneLsnAllocator,
    last_tick_lsn: Cell<u64>,
    /// Tables with a pending delta, each with the row count feeding the tick
    /// threshold. `run_tick` writes one `Tick` group per tid inside one
    /// `sal_writer_excl` window before awaiting any ACK, so the order the map
    /// yields them in only changes the order the workers see the groups in.
    tick_rows: RefCell<FxHashMap<i64, usize>>,
    /// Per-table write serialization. A push whose validation reads committed
    /// state (`push_reads_committed_state`) and every transaction take the write
    /// guard; a push that reads no committed state takes the read guard, so
    /// same-table pushes reach the committer concurrently and share one fsync.
    table_locks: RefCell<FxHashMap<i64, Rc<AsyncRwLock>>>,
    /// Set true by the graceful-shutdown watcher before it sends the final
    /// Shutdown barrier. Read only by [`Shared::enqueue_commit`], which is what
    /// makes the test and the send one step.
    draining: Cell<bool>,
    /// Nesting depth of the DDL windows in which the tick loop is parked; see
    /// `TickGate`. Read by the committer (no checkpoint round while non-zero)
    /// and by the watchdog (a SIGTERM waits the window out).
    pub(super) ddl_window: Cell<usize>,
    /// OCC per-table commit-LSN map: `tid → the zone LSN its last accepted write
    /// this boot rode`. That LSN is published for a durable write; for a stream it is
    /// a reservation the batch never published, which is what makes
    /// `read_is_fresh` answer false and drain. Bumped under the writer's table-lock guard immediately
    /// after a successful commit ACK (push arm and `push_txn_body`, `Ok` path
    /// only), and read by `push_txn_body`'s precondition check under the *write*
    /// guard on that lock, which excludes every bumper. A missing entry reads as
    /// `boot_seed`. Single-threaded reactor — a plain `RefCell`, and no borrow
    /// is ever held across an `.await`.
    table_commit_lsn: RefCell<FxHashMap<i64, u64>>,
    /// The default for a `table_commit_lsn` miss (a table not written this boot),
    /// seeded to `max_table_current_lsn()` — the value `lsn_alloc.published()`
    /// also starts at. Within a boot every live OCC basis is ≥ it and every
    /// commit's zone exceeds it, so a miss cannot false-pass. That rests on no
    /// basis surviving a restart (`gnitz-core`'s `last_seen_lsn`), not on this
    /// dominating every pre-crash durable zone, which it need not.
    boot_seed: u64,
    /// Set by the watchdog when it tears the node down over a dead worker. The
    /// watchdog is detached and its `Output` discarded, so this is how the
    /// verdict reaches `ServerExecutor::run`.
    worker_crashed: Cell<bool>,
}

impl Shared {
    /// Shared access to the catalog, which is what the great majority of this
    /// file's catalog touches want. Split from [`Shared::cat_mut`] so the two are
    /// distinguishable at the call site: an accessor that hands out `&mut`
    /// unconditionally makes every read look like a mutation, and manufactures
    /// exclusive-borrow hazards at sites that only read.
    fn cat(&self) -> &CatalogEngine {
        self.dispatcher.cat()
    }

    /// Exclusive access, for the catalog methods that genuinely take `&mut self`.
    /// Reaches the catalog through the dispatcher's accessor, like [`Self::cat`].
    #[allow(clippy::mut_from_ref)]
    fn cat_mut(&self) -> &mut CatalogEngine {
        self.dispatcher.cat()
    }

    pub(super) fn disp(&self) -> &MasterDispatcher {
        &self.dispatcher
    }

    fn table_lock(&self, tid: i64) -> Rc<AsyncRwLock> {
        let mut locks = self.table_locks.borrow_mut();
        if let Some(l) = locks.get(&tid) {
            return Rc::clone(l);
        }
        let l = Rc::new(AsyncRwLock::default());
        locks.insert(tid, Rc::clone(&l));
        l
    }

    /// Take the write guard on every table in `tids`, sorting and deduping here
    /// so no caller can get it wrong: ascending order is what keeps a child
    /// INSERT and a parent DELETE from deadlocking on the same set, and a repeat
    /// would re-guard a lock this task holds and hang forever.
    ///
    /// Owned, not a slice: `fk_lock_set` borrows the catalog and this loop
    /// awaits, so a slice would hold that borrow across a suspension point.
    async fn lock_tables_exclusive(&self, mut tids: Vec<i64>) -> Vec<WriteGuard> {
        tids.sort_unstable();
        tids.dedup();
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

    /// Enqueue a commit request unless a graceful shutdown has begun. Synchronous,
    /// so the test and the send are one step: a request that saw a live server is
    /// queued ahead of the watchdog's Shutdown barrier, and none commits after the
    /// final checkpoint's view flush.
    fn enqueue_commit(&self, req: CommitRequest) -> Result<(), &'static str> {
        if self.draining.get() {
            return Err("server shutting down");
        }
        self.committer_tx.send(req);
        Ok(())
    }

    /// The zone LSN of `tid`'s last committed write this boot, or `boot_seed` for
    /// a table not written this boot. The miss default is sound for OCC because
    /// every live basis is ≥ `boot_seed` (the argument is on that field), and for
    /// read freshness because `boot_seed` is the same `initial_lsn` `last_tick_lsn`
    /// is seeded to, so an unwritten table compares as absorbed — which it is,
    /// boot finishing its recovery tick sweep first.
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
    ///
    /// The dispatcher's own per-relation entry — the idle-poll gate's last-round
    /// map — goes here too rather than through a second hook of the feed's own:
    /// this is already the one place a dropped relation's per-relation master
    /// state is cleared, and ids are never reused, so nothing else would ever
    /// reclaim it.
    fn forget_relation(&self, _catalog_write: &WriteGuard, id: i64) {
        self.table_locks.borrow_mut().remove(&id);
        self.table_commit_lsn.borrow_mut().remove(&id);
        self.disp().forget_delta_round(id);
    }

    /// Credit `rows` against each tid's pending-tick count and fire the auto-tick
    /// if any tid now stands at or above the coalesce threshold. Below it a push
    /// only accumulates, and nothing ticks until a read asks for a drain.
    pub(super) fn note_commit_rows(&self, rows: impl Iterator<Item = (i64, usize)>) {
        let crossed = {
            let mut pending = self.tick_rows.borrow_mut();
            for (tid, n) in rows {
                *pending.entry(tid).or_insert(0) += n;
            }
            pending.values().any(|&rows| rows >= TICK_COALESCE_ROWS)
        };
        if crossed {
            self.tick_tx.send(TickTrigger::Auto);
        }
    }

    /// Drain the pending tids into `out`, dropping any a DDL has since dropped —
    /// a tid no longer in the catalog must not be ticked. Retains `out`'s
    /// capacity, so the caller's scratch buffer is reused across ticks instead of
    /// allocating a fresh `Vec` per drain.
    ///
    /// The liveness filter is part of the drain rather than a separate step at
    /// each call site: both callers need it, and a third that forgot it would tick
    /// a dropped relation.
    fn drain_live_tick_rows_into(&self, out: &mut Vec<i64>) {
        out.clear();
        let mut rows = self.tick_rows.borrow_mut();
        out.extend(
            rows.drain()
                .map(|(tid, _)| tid)
                .filter(|&tid| self.cat().registry().has_id(tid)),
        );
    }

    /// Put `tids` back after a tick failed to emit them, so their deltas are
    /// ticked again instead of stranded. Their true row counts are gone; 1
    /// understates the coalesce threshold, so the committer fires no `Auto` off
    /// them alone and a repeatedly-refused emit (a full SAL) is retried only
    /// when the next push or read asks for a tick. A tid a mid-tick push
    /// already re-queued keeps that push's real count.
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
    pub fn run(dispatcher: Rc<MasterDispatcher>, server_fd: i32, tls: Option<TlsListener>) -> i32 {
        // 256 SQEs. Not a bound on outstanding work: `IoUringRing::push` flushes
        // a full SQ rather than refusing, so this sets submit batching, not depth.
        let reactor = match Reactor::new(256, crate::runtime::reactor::Limits::from_env()) {
            Ok(r) => Rc::new(r),
            Err(e) => {
                gnitz_error!("io_uring init failed: {e}");
                return 1;
            }
        };
        // Hand the W2M receiver over so the reactor-parked CREATE-VIEW backfill
        // can drive a synchronous collect (the reactor's `OnceCell` slot is
        // stable for its lifetime).
        reactor.attach_w2m(dispatcher.w2m_receiver());
        reactor.attach_listener(server_fd);
        if let Some(tl) = &tls {
            reactor.attach_listener(tl.fd());
        }
        let accept_ctx = AcceptCtx { unix_fd: server_fd, tls };

        // Seed the zone-LSN allocator above every table's current_lsn so each
        // new zone LSN is strictly greater, keeping the `submit` path's direct
        // current_lsn assignment monotonic across restarts.
        let initial_lsn = dispatcher.cat().registry().max_table_current_lsn();

        let (committer_tx, committer_rx) = chan::unbounded::<CommitRequest>();
        let (tick_tx, tick_rx) = chan::unbounded::<TickTrigger>();
        let shared = Rc::new(Shared {
            reactor: Rc::clone(&reactor),
            dispatcher,
            committer_tx,
            catalog_rwlock: Rc::new(AsyncRwLock::default()),
            tick_tx,
            lsn_alloc: ZoneLsnAllocator::new(initial_lsn),
            last_tick_lsn: Cell::new(initial_lsn),
            tick_rows: RefCell::new(FxHashMap::default()),
            table_locks: RefCell::new(FxHashMap::default()),
            draining: Cell::new(false),
            ddl_window: Cell::new(0),
            table_commit_lsn: RefCell::new(FxHashMap::default()),
            boot_seed: initial_lsn,
            worker_crashed: Cell::new(false),
        });

        // Catch SIGTERM/SIGINT so the watchdog can drive a final checkpoint
        // before exiting.
        install_shutdown_signal_handlers();

        reactor.spawn(committer::run(committer_rx, Rc::clone(&shared)));
        reactor.spawn(accept_loop(Rc::clone(&shared), accept_ctx));
        reactor.spawn(tick_loop(Rc::clone(&shared), tick_rx));
        reactor.spawn(relay_loop(Rc::clone(&shared)));
        reactor.spawn(watchdog(Rc::clone(&shared)));

        reactor.block_until_shutdown();
        // `2` separates a dead worker from the `1` above (a failure before or
        // instead of the event loop) and from `gnitz_fatal_abort!`'s `134`.
        if shared.worker_crashed.get() {
            2
        } else {
            0
        }
    }
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

/// Accept-routing inputs: which listener fd is which, and the TLS listener.
/// Carried explicitly, because the reactor records no listener fd — an accept
/// reports which listener it came from through the udata round-trip, and this
/// maps that back to a role.
struct AcceptCtx {
    unix_fd: i32,
    tls: Option<TlsListener>,
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
            // No pre-auth deadline: access here is gated by the socket path's
            // filesystem permissions, and whoever can open it already has full
            // DDL/DML authority, so squatting gains nothing.
            shared.reactor.spawn(connection_loop(peer, s, None));
            continue;
        }
        let Some(tl) = ctx.tls.as_ref().filter(|tl| listener == tl.fd()) else {
            gnitz_warn!("accept from unknown listener fd={listener}; closing conn fd={fd}");
            // SAFETY: freshly-accepted fd we own; no SQE references it.
            unsafe { libc::close(fd) };
            continue;
        };
        // Global connection cap: close the freshly-accepted fd before any TLS
        // work when the live count is at the cap.
        let Some(guard) = tl.admit() else {
            gnitz_warn!("tls: connection cap {} reached; closing fd={fd}", tl.max_conns);
            // SAFETY: freshly-accepted fd we own; no SQE references it.
            unsafe { libc::close(fd) };
            continue;
        };
        let conn = match TlsShared::start(Rc::clone(&shared.reactor), fd, std::sync::Arc::clone(&tl.cfg), guard) {
            Ok(conn) => conn,
            Err(e) => {
                // `guard` was moved into `start`; on the error path it already
                // dropped (decrementing) inside `start`'s frame.
                gnitz_warn!("tls: session init failed for fd={fd}: {e}");
                // SAFETY: freshly-accepted fd we own; no SQE references it.
                unsafe { libc::close(fd) };
                continue;
            }
        };
        let peer = Peer::tls(conn);
        let s = Rc::clone(&shared);
        // Pre-auth first-frame deadline: HELLO must arrive within this window of
        // accept, else the connection is torn down (covers a stalled handshake
        // and a completed-handshake-no-HELLO squat alike).
        let deadline = Instant::now() + tls_hello_timeout();
        shared.reactor.spawn(connection_loop(peer, s, Some(deadline)));
    }
}

/// Pre-auth first-frame deadline (`GNITZ_TLS_HELLO_TIMEOUT_MS`, default
/// 15 000 ms). Comfortably exceeds the client's ~10 s post-connect
/// handshake+HELLO budget, so legitimate slow-link clients are not reaped.
fn tls_hello_timeout() -> std::time::Duration {
    static T: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *T.get_or_init(|| {
        std::time::Duration::from_millis(gnitz_store::foundation::env::env_num(
            "GNITZ_TLS_HELLO_TIMEOUT_MS",
            15_000,
        ))
    })
}

enum HelloOutcome {
    /// Connection accepted.
    Pass,
    /// Caller must close the connection.
    Reject,
}

/// `first_frame_deadline` bounds the arrival of the first (HELLO) frame: `Some`
/// for TLS (pre-auth reap), `None` for AF_UNIX. Only the first recv is raced
/// against it.
async fn connection_loop(peer: Peer, shared: Rc<Shared>, first_frame_deadline: Option<Instant>) {
    serve_connection(&peer, &shared, first_frame_deadline).await;
    // The one exit: ship what is corked — a rejection is a corked reply like any
    // other — then retire the fd.
    peer.flush_egress().await;
    peer.close();
}

/// One message handled to completion before the next is received, so replies
/// leave in request order — which is how clients correlate them (`gnitz.aio`
/// gathers a mixed group onto one round-trip and rejects an out-of-order
/// `target_id`). Spawning `handle_message` to overlap requests would break that.
///
/// Returns when the peer is gone or refused; the caller closes.
async fn serve_connection(peer: &Peer, shared: &Rc<Shared>, first_frame_deadline: Option<Instant>) {
    // No HELLO in time (`Either::B`) → `None`. `select2` drops the losing recv
    // (clears its waker) and the losing timer (cancels its SQE), so the happy
    // path leaves no timer behind.
    let first = match first_frame_deadline {
        Some(deadline) => match select2(peer.recv(), shared.reactor.timer(deadline)).await {
            Either::A(opt) => opt,
            Either::B(()) => None,
        },
        None => peer.recv().await,
    };
    let Some(buf) = first else { return };
    if let HelloOutcome::Reject = run_hello_handshake(peer, shared, buf.as_slice()).await {
        return;
    }

    loop {
        // Never wait on the client holding corked bytes, and never hold more
        // than the budget: between them these are the whole shipping rule.
        let mut next = peer.try_recv();
        if next.is_none() {
            if peer.flush_egress().await < 0 {
                return;
            }
            next = peer.recv().await;
        }
        let Some(buf) = next else { return };
        handle_message(peer, buf.as_slice(), shared).await;
        if peer.flush_if_full().await < 0 {
            return;
        }
    }
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
        send_error(peer, 0, 0, msg.as_bytes());
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
/// detection (broadcast Shutdown, stop the reactor) and graceful shutdown
/// on SIGTERM/SIGINT — stop admitting pushes, run one final full checkpoint
/// through the committer (drain + persist while the reactor is still live),
/// then broadcast Shutdown and request reactor shutdown so `server_main`
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
            //    `pending_deltas` (below the row threshold, so no `Auto` fired),
            //    so the sequence's drain is what gets it into the views.
            await_barrier(&shared, BarrierKind::Shutdown).await;

            // 3. Workers flush + _exit, then stop the reactor. The reactor/W2M
            //    receiver stays live throughout, so no `w2m()` handle dangles.
            shared.disp().shutdown_workers();
            shared.reactor.request_shutdown();
            return;
        }

        if let Some(crashed) = shared.disp().check_workers() {
            let base_dir = shared.cat().base_dir().to_string();
            gnitz_error!("Worker {crashed} crashed (log: {base_dir}/worker_{crashed}.log), shutting down");
            shared.worker_crashed.set(true);
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
        if shared.ddl_window.get() == 0 && shared.disp().sal_space_low() {
            let (done_tx, done_rx) = oneshot::channel();
            shared.committer_tx.send(CommitRequest::Barrier {
                kind: BarrierKind::Reclaim { forced: false },
                done: done_tx,
            });
            // Fire-and-forget: dropping the receiver is the whole point, not an
            // RAII hold.
            drop(done_rx);
        }
    }
}

// ---------------------------------------------------------------------------
// Tick loop (event-driven)
// ---------------------------------------------------------------------------

/// Drive ticks from a channel of `TickTrigger`s: take every trigger already
/// queued, then issue one batched tick for the union of pending tids.
///
/// Coalescing is the *sender's* job — the committer sends `Auto` only once a tid
/// crosses `TICK_COALESCE_ROWS`, and `Drain`/`Quiesce` senders are parked on the
/// answer — so this loop never delays a tick to gather more.
///
/// A failure in one trigger fails only that trigger; SAL emission is further
/// guarded by `guard_panic` inside `run_tick`.
async fn tick_loop(shared: Rc<Shared>, mut rx: chan::Receiver<TickTrigger>) {
    let nw = shared.disp().num_workers();
    let mut fut_slots: Vec<ReplyFuture> = Vec::with_capacity(nw);
    let mut ack_slots: Vec<Option<ipc::DecodedWire>> = Vec::with_capacity(nw);
    let mut triggers: Vec<TickTrigger> = Vec::new();
    // The batch's `Drain` repliers, held across the tick they are waiting on.
    let mut dones: Vec<oneshot::Sender<Result<(), WireFault>>> = Vec::new();
    // Reused across every tick; `drain_live_tick_rows_into` clears it before
    // refilling so capacity is retained.
    let mut tids_scratch: Vec<i64> = Vec::new();
    loop {
        let first = match rx.recv().await {
            Some(t) => t,
            // Needs an arm but cannot arrive; see the committer's `rx.recv()`.
            None => return,
        };
        triggers.push(first);

        // Drain anything already queued.
        while let Some(more) = rx.try_recv() {
            triggers.push(more);
        }

        // A `Quiesce` is acked here — no tick is in flight, the loop being
        // serial — and blocks until the DDL releases the gate, so no tick runs
        // while the DDL holds the catalog write lock. The batch's `Drain`s are
        // answered after the tick below.
        for trigger in triggers.drain(..) {
            match trigger {
                TickTrigger::Quiesce { acked } => {
                    let (release_tx, release_rx) = oneshot::channel();
                    acked.send(release_tx);
                    let _ = release_rx.await;
                }
                TickTrigger::Drain { done } => dones.push(done),
                TickTrigger::Auto => {}
            }
        }

        shared.drain_live_tick_rows_into(&mut tids_scratch);

        // Run the tick. Errors are reported in logs AND handed to every Drain
        // trigger's `done`: the waiting reader's view is stale, so reporting
        // success would serve stale rows under STATUS_OK.
        let tick_result = run_tick(&shared, &tids_scratch, nw, &mut fut_slots, &mut ack_slots).await;
        if let Err(e) = &tick_result {
            gnitz_warn!("tick error: {}", e);
        }
        for done in dones.drain(..) {
            done.send(tick_result.clone());
        }
    }
}

/// Emit Tick groups for every `tid` and await the per-worker ACKs.
///
/// The emit-and-await lock shape: the reply lease taken before any lock,
/// `catalog_rwlock.read()` (so DDL cannot mutate schemas mid-emission) +
/// `sal_writer_excl` covering only the contiguous emission window, one
/// `signal_all` inside it, both released before awaiting so other reactor work
/// proceeds concurrently with worker DAG eval.
async fn run_tick(
    shared: &Rc<Shared>,
    tids: &[i64],
    nw: usize,
    fut_slots: &mut Vec<ReplyFuture>,
    ack_slots: &mut Vec<Option<ipc::DecodedWire>>,
) -> Result<(), WireFault> {
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

    let req_ids = shared.reactor.alloc_replies(tids.len() * nw);

    let _cat_read = shared.catalog_rwlock.read().await;
    let _sal_excl = shared.disp().sal_excl().lock().await;

    // Written by the closure as it goes, so the re-queue and reply-await below
    // are also correct on `guard_panic`'s panic arm, which discards the closure's
    // return value.
    let emitted = Cell::new(0usize);
    // The emit verdict rides out as the `Ok` value, so it keeps its typed status:
    // `guard_panic` is pinned to `Result<T, String>`, and a full SAL must reach a
    // waiting reader as the retryable `STATUS_SAL_FULL` rather than flattened.
    let emit = guard_panic("tick", || {
        let disp = shared.disp();
        let mut result = Ok(());
        for (i, &tid) in tids.iter().enumerate() {
            if let Err(e) = disp.write_tick_group(tid, GroupTargets::All(&req_ids[i * nw..(i + 1) * nw])) {
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
        Ok(result)
    });
    let emit = emit.map_err(WireFault::from).and_then(|r| r);
    drop(_sal_excl);
    drop(_cat_read);

    let n = emitted.get();
    // The un-emitted tids never reached a worker, so they still need ticking. An
    // emitted tid is already being ticked by the workers (its group is
    // published), and `handle_tick` has taken its delta, so re-queueing it would
    // only produce a no-op tick that then reports success and masks this failure.
    shared.requeue_tick_tids(&tids[n..]);

    let worker_err = await_worker_acks(&shared.reactor, &req_ids[..n * nw], "tick", fut_slots, ack_slots).await;
    if let Some(e) = emit.err().or(worker_err.err()) {
        return Err(e);
    }
    shared.last_tick_lsn.set(snapshot_lsn);
    Ok(())
}

// ---------------------------------------------------------------------------
// Relay loop
// ---------------------------------------------------------------------------

/// Accumulate the reactor's `FLAG_EXCHANGE` frames into rounds and write each
/// completed round back as an ExchangeRelay group. Its own task because the
/// write needs `catalog_rwlock.read` and `sal_writer_excl`, neither of which a
/// CQE handler can block-acquire.
///
/// A lost relay wedges workers blocked in `do_exchange_wait` forever
/// (they ACK neither tick nor relay and the master stays alive), so both
/// failure modes — an `emit_relay_with_decision` error and no space after a reclaim
/// checkpoint — `gnitz_fatal_abort!` rather than warn-and-drop: a loud,
/// recoverable crash (workers self-exit via `getppid()`, operator
/// restarts) beats a silent permanent cluster wedge.
async fn relay_loop(shared: Rc<Shared>) {
    let mut acc = ExchangeAccumulator::new(shared.disp().num_workers());
    loop {
        let (w, frame) = shared.reactor.next_exchange().await;
        let Some(relay) = acc.process(w, frame) else {
            continue;
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
        // Spent here, not inside the retry: a genuinely low first iteration must
        // not leave the latch to fire after the reclaim, where `reclaimed` turns
        // it into the fatal "exhausted even after a forced checkpoint".
        let mut inject_low = RELAY_SPACE_LOW.take_once();
        loop {
            {
                let _sal = shared.disp().sal_excl().lock().await;
                let mut fit = shared.disp().relay_fit(prep.footprint);
                if inject_low {
                    inject_low = false;
                    fit = SalFit::Transient;
                }
                match fit {
                    // Written whole. Chunking it as the W2M up-leg is chunked
                    // would not help: a parked worker cannot consume a partial
                    // train, so every chunk would be SAL-resident at once.
                    SalFit::Terminal => {
                        gnitz_fatal_abort!("exchange relay exceeds the SAL outright; no checkpoint can deliver it")
                    }
                    SalFit::Fits => {
                        // Always CONTINUE: only steady-state tick exchanges reach
                        // this loop (both chunked-backfill drivers collect their
                        // relays synchronously in `collect_acks_and_relay`, the
                        // sole STOP/CHECKPOINT stamper), and a tick round never pads.
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
                    SalFit::Transient if reclaimed => {
                        gnitz_fatal_abort!(
                            "SAL space exhausted even after forced checkpoint; \
                             cannot deliver exchange relay — aborting to prevent \
                             cluster deadlock"
                        )
                    }
                    SalFit::Transient => {}
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
    // ONE control-block parse for the whole request: routing, the schema-hint
    // decision and the push decode all read this same parse, so a malicious
    // client cannot forge a directory that points one at one region and another
    // at another.
    let ctrl = match ipc::peek_control_block(data) {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("decode error: {e}");
            send_error(peer, 0, 0, msg.as_bytes());
            return;
        }
    };
    let client_id = ctrl.client_id;
    let target_id = ctrl.target_id as i64;
    let client_version = gnitz_wire::wire_flags_get_schema_version(ctrl.flags);

    // The frame names exactly one verb, or it is malformed. A run of flag tests
    // would instead settle a two-verb frame by branch position, and would read a
    // frame that carries rows but names no verb as a scan — answering an INSERT
    // with a streamed table dump.
    let verb = match ClientVerb::from_flags(ctrl.flags) {
        Ok(v) => v,
        Err(e) => {
            send_error(peer, target_id, client_id, e.as_bytes());
            return;
        }
    };

    match verb {
        // The bundle frames name no single relation in `target_id`: each carries
        // its items after the control block and decodes its own body from `data`.
        ClientVerb::DdlTxn => handle_ddl_txn(shared, peer, client_id, data).await,
        ClientVerb::PushTxn => handle_push_txn(shared, peer, client_id, data).await,
        ClientVerb::ScanMulti => handle_scan_multi(shared, peer, client_id, data).await,
        ClientVerb::DeltaPoll => handle_delta_poll(shared, peer, client_id, data).await,

        // `target_id` is the sequence key (= the owning table's id); the range
        // `count` rides in `seek_col_idx`.
        ClientVerb::AllocSerialRange => {
            let count = ctrl.seek_col_idx.max(1) as i64;
            match commit_serial_range_durable(shared, target_id, count).await {
                Ok(base) => send_control_only(peer, base, client_id, STATUS_OK),
                Err(e) => send_error(peer, target_id, client_id, e.as_bytes()),
            }
        }

        // An id allocation names no relation, so `target_id` is not read — and a
        // frame that sets one is still allocated, rather than falling through to
        // a scan of that id. The run length rides in `seek_col_idx`, exactly as
        // `AllocSerialRange`'s does; the reply is the run's base.
        ClientVerb::AllocTableId => {
            let alloc = shared.cat_mut().allocate_table_ids(ctrl.seek_col_idx);
            reply_allocation(peer, client_id, alloc).await
        }
        ClientVerb::AllocSchemaId => {
            let alloc = shared.cat_mut().allocate_schema_id();
            reply_allocation(peer, client_id, alloc).await
        }
        ClientVerb::AllocIndexId => {
            let alloc = shared.cat_mut().allocate_index_ids(ctrl.seek_col_idx);
            reply_allocation(peer, client_id, alloc).await
        }

        ClientVerb::Seek => serve_seek(shared, peer, &ctrl, client_version).await,
        ClientVerb::SeekByIndex => handle_seek_by_index(shared, peer, &ctrl, client_version).await,
        ClientVerb::ScanSpec => handle_scan_spec(shared, peer, client_id, target_id, &ctrl.seek_pk_extra).await,

        // A plain read guard, not `read_lock`: a resolve answers catalog shape,
        // and a view tick moves a view's rows, never its shape — so the tick
        // drain `read_lock` waits for buys nothing here. The guard scope ends at
        // the reply buffer, so the lock is never held across the send.
        ClientVerb::Resolve => {
            let reply = {
                let _g = shared.catalog_rwlock.read().await;
                build_resolve_reply(shared, client_id, target_id, &ctrl.seek_pk_extra)
            };
            match reply {
                Ok(buf) => peer.send_or_close(buf).await,
                Err(msg) => send_error(peer, target_id, client_id, msg.as_bytes()),
            }
        }

        ClientVerb::Push => handle_push(shared, peer, data, ctrl).await,

        // A scan is the verb a frame names by naming none. The master-local /
        // fan-out split is `handle_scan`'s, taken off the kind `read_lock`
        // resolved — the one read-lock entry point every other read verb uses.
        ClientVerb::Scan => handle_scan(shared, peer, client_id, target_id, client_version).await,
    }
}

/// Reply to an id allocation. The new id rides back as the reply's *target* id —
/// that id is the whole answer, so the frame carries no schema and no data.
async fn reply_allocation<E: std::fmt::Display>(peer: &Peer, client_id: u64, alloc: Result<i64, E>) {
    match alloc {
        Ok(new_id) => send_control_only(peer, new_id, client_id, STATUS_OK),
        Err(e) => {
            let msg = format!("id allocation failed: {e}");
            send_error(peer, 0, client_id, msg.as_bytes());
        }
    }
}

/// Why a push frame could not be decoded.
enum PushReject {
    /// The client's cached schema does not describe the target. It must evict
    /// that cache entry and retry cold, where the existence and schema gates word
    /// whatever the real problem is.
    SchemaMismatch,
    Error(String),
}

/// Decode a push frame, resolving a warm frame's absent schema block against the
/// catalog. Runs before the catalog read guard, and off it: `AsyncRwLock` is
/// writer-preferring, so a bulk load's multi-megabyte decode under the guard
/// would stall every DDL writer behind it.
///
/// The version compare is a cheap-out, not the schema gate — it skips the decode
/// on an already-stale frame. `handle_push` owns the gate.
fn decode_push_frame(
    shared: &Shared,
    data: &[u8],
    ctrl: gnitz_wire::control::DecodedControl,
) -> Result<ipc::DecodedWire, PushReject> {
    let target_id = ctrl.target_id as i64;
    let client_version = gnitz_wire::wire_flags_get_schema_version(ctrl.flags);
    let has_schema = ctrl.flags & gnitz_wire::FLAG_HAS_SCHEMA != 0;
    let has_data = ctrl.flags & gnitz_wire::FLAG_HAS_DATA != 0;

    // A cold frame ships its own schema block and needs no hint.
    let catalog_schema = if has_data && !has_schema {
        if client_version == 0 {
            return Err(PushReject::Error("FLAG_HAS_DATA without FLAG_HAS_SCHEMA".to_string()));
        }
        if client_version != shared.cat().get_schema_version(target_id) {
            return Err(PushReject::SchemaMismatch);
        }
        Some(
            shared
                .cat()
                .registry()
                .get_schema_desc(target_id)
                .ok_or(PushReject::SchemaMismatch)?,
        )
    } else {
        None
    };
    decode_client_wire(data, ctrl, catalog_schema.as_ref()).map_err(|e| PushReject::Error(format!("decode error: {e}")))
}

/// Handle a client push: decode the frame, then commit it under the catalog read
/// lock and the target's table lock(s).
///
/// One handler for both batch shapes. An empty batch is a legitimate empty Z-Set
/// delta, so it is ACKed with the "nothing written" LSN `0` — but only after the
/// same existence + writability gate a non-empty push passes, so a client bug
/// that happens to produce an empty batch (a `delete` with an empty pk list)
/// fails the way a non-empty one would instead of being masked by a no-op ACK.
async fn handle_push(shared: &Rc<Shared>, peer: &Peer, data: &[u8], ctrl: gnitz_wire::control::DecodedControl) {
    let client_id = ctrl.client_id;
    let target_id = ctrl.target_id as i64;
    let flags = ctrl.flags;
    let client_version = gnitz_wire::wire_flags_get_schema_version(flags);
    let has_schema = flags & gnitz_wire::FLAG_HAS_SCHEMA != 0;

    // Decoding happens before the lock below, and cannot suspend: see
    // `decode_push_frame`.
    let decoded = match decode_push_frame(shared, data, ctrl) {
        Ok(d) => d,
        Err(PushReject::SchemaMismatch) => {
            send_control_only(peer, target_id, client_id, STATUS_SCHEMA_MISMATCH);
            return;
        }
        Err(PushReject::Error(msg)) => {
            send_error(peer, target_id, client_id, msg.as_bytes());
            return;
        }
    };
    if PUSH_HOLD_FOR_DDL.take_once() {
        hold_push_for_ddl(shared, target_id).await;
    }
    let _cat = shared.catalog_rwlock.read().await;
    let Some(kind) = target_kind_or_reject(shared, peer, client_id, target_id, Access::Write).await else {
        return;
    };

    // The guard drops before the empty-batch ACK: the reply is a socket write,
    // and this lock is writer-preferring.
    let batch = match decoded.data_batch {
        Some(b) if !b.is_empty() => b,
        _ => {
            drop(_cat);
            send_ok_response(shared, peer, target_id, None, client_id, 0, client_version);
            return;
        }
    };

    // Warm or cold alike: `decoded.schema` is the descriptor the batch was laid
    // out against, read before this guard, and a DDL queued in that gap has since
    // replaced it.
    if let Some(decoded_schema) = &decoded.schema {
        if let Err(e) = validate_client_schema(shared, target_id, decoded_schema) {
            drop(_cat);
            // A cold frame authored its schema, so it gets the mismatch in words;
            // a warm one is told to evict its cache entry and retry cold, where
            // that wording is reachable.
            if has_schema {
                send_error(peer, target_id, client_id, e.as_bytes());
            } else {
                send_control_only(peer, target_id, client_id, STATUS_SCHEMA_MISMATCH);
            }
            return;
        }
    }

    let Some(mode) = gnitz_wire::wire_flags_get_conflict_mode(flags) else {
        send_error(peer, target_id, client_id, b"push: unknown conflict mode");
        return;
    };

    // Not at the decode boundary: `reject_not_null_bits` runs there without a
    // relation kind, and must keep admitting a base table's retractions.
    if kind == RelationKind::Stream {
        if let Some(e) = stream_push_error(target_id, &batch, mode) {
            send_error(peer, target_id, client_id, e.as_bytes());
            return;
        }
    }

    // The validator's own predicate decides the guard: a push that reads no
    // committed state cannot be invalidated by a concurrent one, so it may share
    // its table's lock and reach the committer alongside other pushes to the same
    // table, which fold into one SAL zone and one fsync. Every other push takes
    // all FK-related table locks exclusively.
    let reads_committed = shared.cat().push_reads_committed_state(target_id, mode);
    let _tlocks = if !reads_committed {
        (Some(shared.table_lock(target_id).read().await), Vec::new())
    } else {
        let lock_set = shared.cat().fk_lock_set(target_id).to_vec();
        (None, shared.lock_tables_exclusive(lock_set).await)
    };

    // Distributed validation (PK / FK / unique indices). A plain push is a
    // one-family bundle — the same four rules over the same fold — so the batch
    // rides into the family for the validation and back out for the commit
    // request. The validator itself skips a bundle no rule would read a row for.
    let family = TxnFamily { tid: target_id, mode, batch };
    if let Err(e) = shared
        .disp()
        .validate_txn_distributed(&shared.reactor, std::slice::from_ref(&family))
        .await
    {
        send_fault(peer, target_id, client_id, &e);
        return;
    }
    let TxnFamily { batch, .. } = family;

    // Route through the committer and wait for commit ACK. A graceful shutdown
    // in flight refuses the enqueue; the client sees a clean error and can retry
    // against the restarted server.
    let (tx, rx) = oneshot::channel::<Result<u64, WireFault>>();
    let is_stream = kind == RelationKind::Stream;
    let queued = shared.enqueue_commit(CommitRequest::Push(PendingPush {
        tid: target_id,
        batch,
        recoverable: !is_stream,
        done: tx,
    }));
    if let Err(e) = queued {
        send_error(peer, target_id, client_id, e.as_bytes());
        return;
    }
    match rx.await {
        Some(Ok(zone_lsn)) => {
            // Record the commit LSN for OCC while the table-lock guard is still
            // held (a concurrent precondition check reads it under the write guard
            // on the same lock, which excludes this one, so the bump lands before
            // any conflicting txn can pass). Bump on the `Ok` path only: an `Err`
            // reply is pre-SAL or fail-stop, so there is no live-visible durable
            // change to record.
            //
            // Always the real LSN, stream included: clamping the watermark too
            // would leave `commit_lsn_of` at its boot seed, so `read_is_fresh`
            // would answer `true` forever and the batch would sit un-ticked.
            shared.record_commit_lsn([target_id], zone_lsn);
            // A stream replies `0` — what `push_with_mode` already returns for a
            // buffered transactional write, and what keeps the client's basis
            // `<= published()`. Keyed on the target rather than on whether this
            // batch happened to open a zone, so a stream push the committer
            // coalesced with a base-table push still answers `0`.
            let reply_lsn = if is_stream { 0 } else { zone_lsn };
            send_ok_response(
                shared,
                peer,
                target_id,
                None,
                client_id,
                reply_lsn as u128,
                client_version,
            );
        }
        Some(Err(fault)) => send_fault(peer, target_id, client_id, &fault),
        None => send_error(peer, target_id, client_id, b"committer shut down"),
    }
}

/// Serve a point lookup: a catalog family reads master-locally; a user relation
/// (base table or view) fans out to the owning worker by PK hash. SEEK unicasts
/// to one worker, so no replicated fork.
///
/// Takes the catalog read lock itself, through the same `read_lock` entry point
/// every other read verb uses. A base-table or system seek is the RMW hot path —
/// base state is fresh at push-apply time, so the lock is taken once and never
/// drains; a view seek drains inside `read_lock`, which requires that the drain
/// happen with no catalog lock held.
async fn serve_seek(shared: &Rc<Shared>, peer: &Peer, ctrl: &gnitz_wire::control::DecodedControl, client_version: u16) {
    let client_id = ctrl.client_id;
    let target_id = ctrl.target_id as i64;
    let pk = ctrl.seek_pk;
    let seek_pk_extra = ctrl.seek_pk_extra.as_slice();
    let Some((_g, kind)) = read_lock(shared, peer, client_id, target_id, Access::Read, ReadFreshness::Current).await
    else {
        return;
    };
    if kind == RelationKind::SystemCatalog {
        // Bound before the match: a scrutinee's temporaries live to the end of
        // the match, so the `&mut CatalogEngine` would be held across the awaits
        // in the arms — one of which mints a second borrow of its own.
        let found = guard_panic("seek", || shared.cat_mut().seek_family(target_id, pk, seek_pk_extra));
        match found {
            Ok((batch, _)) => send_ok_response(shared, peer, target_id, batch.as_ref(), client_id, pk, client_version),
            Err(e) => send_error(peer, target_id, client_id, e.as_bytes()),
        }
    } else {
        match shared
            .disp()
            .fan_out_seek(&shared.reactor, target_id, pk, seek_pk_extra, client_version)
            .await
        {
            Ok(slot) => peer.send_or_close(slot).await,
            Err(f) => send_fault(peer, target_id, client_id, &f),
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
/// precondition check under those locks, then emit it as N `Push` groups
/// inside one zone under one sentinel. Mirrors the plain-push arm's lock order
/// (catalog read lock, then the per-table lock union ascending) and its late
/// late drain check; both locks are held through the committer ACK. Every
/// rejection is pre-SAL, so an `Err` reply — and a `Conflict` outcome — mean
/// "nothing committed".
async fn handle_push_txn(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8]) {
    match push_txn_body(shared, data).await {
        Ok(outcome) => {
            let (pk, status) = match outcome {
                // Standard single-frame ACK, seek_pk = zone LSN (uncorrelated,
                // as push_ddl_txn's reply is).
                PushTxnOutcome::Committed(lsn) => (lsn, STATUS_OK),
                // OCC precondition failed: a control-only STATUS_TXN_CONFLICT
                // frame whose `seek_pk` carries the fresh basis. Empty message —
                // the client synthesizes any human-readable text from the tid it
                // sent.
                PushTxnOutcome::Conflict(fresh_basis) => (fresh_basis, gnitz_wire::STATUS_TXN_CONFLICT),
            };
            send_msg(
                peer,
                ipc::WireMsg {
                    client_id,
                    seek_pk: pk as u128,
                    status,
                    ..Default::default()
                },
            );
        }
        Err(fault) => send_fault(peer, 0, client_id, &fault),
    }
}

/// The body of `handle_push_txn`. Returns `Committed(zone_lsn)` on a durable
/// commit or `Conflict(fresh_basis)` when the OCC precondition check fails.
async fn push_txn_body(shared: &Rc<Shared>, data: &[u8]) -> Result<PushTxnOutcome, WireFault> {
    // 1. Decode + frame-local shape rules (no catalog access). The frame carries
    //    the families and the OCC preconditions (each `(tid, basis)`).
    let (raw, preconditions) =
        gnitz_wire::txn_frame::decode_push_txn(data).map_err(|e| format!("decode error: {e}"))?;
    if raw.is_empty() {
        return Err("TXN: empty family bundle".into());
    }

    // 2. Catalog read lock (excludes a concurrent DROP/DDL), then the
    //    catalog-dependent shape rules + per-family batch decode.
    let _cat = shared.catalog_rwlock.read().await;
    let mut families: Vec<TxnFamily> = Vec::with_capacity(raw.len());
    for fam in &raw {
        // The wire carries the tid as u32; the catalog addresses it as i64.
        let tid = fam.tid as i64;
        if tid < FIRST_USER_TABLE_ID {
            return Err(format!("TXN: {tid} is not a user table").into());
        }
        // Same existence + writability gate the plain-push arm applies, so a view
        // target is rejected identically. Refusing a stream keeps every transaction
        // family `recoverable`, so a transaction always opens a zone.
        // The status is dropped on purpose: this reply names no relation, so only
        // the text identifies the tid.
        if target_kind(shared, tid, Access::Write).map_err(|f| f.text)? == RelationKind::Stream {
            return Err(format!("table {tid} is a stream: a stream cannot be written inside a transaction").into());
        }
        // The schema block is always present; validate it against the catalog
        // per family (a concurrent DDL between buffer time and commit surfaces as
        // a clean error the application re-runs).
        // `false`: the frame walk already verified this block's checksum.
        let wire_schema = gnitz_store::schema::decode_schema_block(fam.schema_block, false)
            .map_err(|e| format!("TXN family {tid} schema decode error: {e}"))?;
        let catalog_schema = validate_client_schema(shared, tid, &wire_schema)?;
        let batch = decode_client_batch(fam.wal_block, &catalog_schema)
            .map_err(|e| format!("TXN family {tid} decode error: {e}"))?;
        if batch.is_empty() {
            return Err(format!("TXN: empty batch for table {tid}").into());
        }
        let mode = gnitz_wire::WireConflictMode::from_wire(fam.mode)
            .ok_or_else(|| format!("TXN family {tid}: unknown conflict mode {}", fam.mode))?;
        families.push(TxnFamily { tid, mode, batch });
    }
    // Capture the family tids BEFORE `families` is moved into the commit request,
    // for the precondition-membership check and the post-commit map bump.
    let family_tids: Vec<i64> = families.iter().map(|f| f.tid).collect();

    // 3. Acquire the per-table lock union ⋃ fk_lock_set(tid) exclusively.
    let mut union: Vec<i64> = Vec::new();
    for fam in &families {
        union.extend_from_slice(shared.cat().fk_lock_set(fam.tid));
    }
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
            return Err(format!("TXN: precondition on {tid}: not a written table").into());
        }
        if shared.commit_lsn_of(tid) > basis {
            return Ok(PushTxnOutcome::Conflict(shared.lsn_alloc.published()));
        }
    }

    // 4. Distributed bundle validation (the four rules).
    shared
        .disp()
        .validate_txn_distributed(&shared.reactor, &families)
        .await?;

    // 5. Route through the committer and wait for the zone ACK. `families` is
    //    moved here; `family_tids` was captured above for the bump. The drain
    //    check rides inside `enqueue_commit`, which is what makes it and the send
    //    one step.
    let (tx, rx) = oneshot::channel::<Result<u64, WireFault>>();
    shared.enqueue_commit(CommitRequest::Txn(PendingTxn { families, done: tx }))?;
    // Double `?`: the outer unwraps a channel cancel, the inner a committer
    // `Err` — so the bump below is reached ONLY on a successful commit.
    let lsn = rx.await.ok_or("committer shut down")??;

    // 6. Record the commit LSN for every family tid while the table locks are
    //    still held (`_tlocks` in scope), so a later same-tid txn cannot pass its
    //    precondition against a pre-this-commit basis. Bump on `Ok` only. The
    //    reply is sent by `handle_push_txn` after the locks drop, which is fine:
    //    OCC needs only the bump under the lock, and a later same-tid txn cannot
    //    acquire the lock until this one releases (after the bump).
    shared.record_commit_lsn(family_tids.iter().copied(), lsn);
    Ok(PushTxnOutcome::Committed(lsn))
}

/// Compare a client-supplied schema against the catalog's own descriptor for
/// `tid` and hand that descriptor back — the one place a claimed layout meets the
/// registered one, so both write paths reject alike.
///
/// An absent descriptor is a rejection, not a pass: every caller has already
/// proved the relation exists, so its absence is a bug, not an exemption.
fn validate_client_schema(shared: &Shared, tid: i64, client: &SchemaDescriptor) -> Result<SchemaDescriptor, String> {
    let expected = shared
        .cat()
        .registry()
        .get_schema_desc(tid)
        .ok_or_else(|| format!("table {tid} has no registered schema"))?;
    validate_schema_match(client, &expected)?;
    Ok(expected)
}

/// Decode a CLIENT-supplied frame. `decode_wire_with_ctrl` is the client-trust
/// entry: it leaves the batch `Raw`, dropping any FLAG_BATCH_CONSOLIDATED claim
/// ("already consolidated, skip the work"), which a client must never be trusted
/// to make; downstream consolidation (the catalog DDL ingest and the commit path)
/// establishes those invariants. Every
/// client-boundary decode goes through this or its sibling
/// `decode_client_batch`.
fn decode_client_wire(
    data: &[u8],
    ctrl: gnitz_wire::control::DecodedControl,
    hint: Option<&SchemaDescriptor>,
) -> Result<ipc::DecodedWire, &'static str> {
    let decoded = ipc::decode_wire_with_ctrl(data, ctrl, hint)?;
    // The decoder builds a data batch only against a resolved schema, so the
    // two are present or absent together.
    if let (Some(b), Some(schema)) = (decoded.data_batch.as_ref(), decoded.schema.as_ref()) {
        reject_not_null_bits(b, schema)?;
    }
    Ok(decoded)
}

/// A raw WAL-block family batch inside a client FLAG_DDL_TXN or FLAG_PUSH_TXN
/// bundle. `decode_from_wal_block` builds every batch `Raw`, so like
/// `decode_client_wire` this carries no client layout claim.
///
/// `verify_checksum = false`: `txn_frame`'s `checked_block_at` already verified
/// it, off the catalog lock this decode runs under.
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
    // so a per-row branch would only add work. The same branch-free fold over the
    // same contiguous count-bounded region as `Batch::all_weights_positive`.
    let acc = b
        .null_bmp_data()
        .as_chunks::<8>()
        .0
        .iter()
        .fold(0u64, |a, w| a | u64::from_le_bytes(*w));
    if acc & not_null != 0 {
        return Err("client batch sets a null bit on a NOT NULL column");
    }
    Ok(())
}

/// Which end of a relation a request wants — the discriminator of [`target_kind`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    /// Any read. Admits a system catalog family, which [`serve_seek`] serves
    /// master-locally.
    Read,
    /// A read with only a fan-out realization, which a system catalog family has
    /// no form of: every worker holds a full copy, so fanning one out would
    /// concatenate W identical trains and inflate every row's weight W-fold.
    UserRead,
    Write,
}

/// Resolve `target_id`'s kind, rejecting one that cannot serve `access`.
///
/// A stream holds no rows, so it may be written but not read. A view registers into
/// the same id space as base tables (shared `next_table_id`), so it may be read but
/// not written — a raw client push addressed to a view tid would otherwise commit
/// rows into the view's output store that its circuit never produced, permanently
/// divergent derived state.
///
/// Enforced here even though the SQL binder refuses both: the C and Python bindings
/// reach the engine directly. Every caller addresses a relation by id and owns its
/// own reply path.
///
/// **Only the absent-relation arm carries a status of its own**
/// ([`STATUS_NOT_FOUND`]): the arms below name a relation that exists, which a
/// client must not recover from the way it recovers from a vanished one.
fn target_kind(shared: &Shared, target_id: i64, access: Access) -> Result<RelationKind, WireFault> {
    let Some(kind) = shared.cat().registry().relation_kind(target_id) else {
        return Err(WireFault {
            status: STATUS_NOT_FOUND,
            text: format!("table {target_id} not found"),
        });
    };
    match access {
        Access::Read | Access::UserRead if kind == RelationKind::Stream => {
            Err(format!("table {target_id} is a stream: a stream holds no rows and cannot be read").into())
        }
        Access::UserRead if kind == RelationKind::SystemCatalog => {
            Err(format!("table {target_id} is a system catalog family: this read has only a fan-out form").into())
        }
        Access::Write if !kind.is_ingestion_point() => {
            Err(format!("table {target_id} is not writable: pushes must target a base table or a stream").into())
        }
        _ => Ok(kind),
    }
}

/// [`target_kind`] with the frame reply path. `None` means the error reply was sent
/// and the caller must return. The caller holds the catalog read lock.
async fn target_kind_or_reject(
    shared: &Shared,
    peer: &Peer,
    client_id: u64,
    target_id: i64,
    access: Access,
) -> Option<RelationKind> {
    match target_kind(shared, target_id, access) {
        Ok(kind) => Some(kind),
        Err(f) => {
            send_fault(peer, target_id, client_id, &f);
            None
        }
    }
}

/// Serve a secondary-index point lookup. [`Access::UserRead`] refuses a system
/// family, which is also how "never carries a secondary index" is enforced.
///
/// `seek_col_idx` is `pack_pk_cols(col_indices)` and the key rides in `seek_pk` +
/// `seek_pk_extra`; all three are forwarded to the fan-out verbatim.
async fn handle_seek_by_index(
    shared: &Rc<Shared>,
    peer: &Peer,
    ctrl: &gnitz_wire::control::DecodedControl,
    client_version: u16,
) {
    let client_id = ctrl.client_id;
    let target_id = ctrl.target_id as i64;
    let seek_col_idx = ctrl.seek_col_idx;
    let seek_pk = ctrl.seek_pk;
    let seek_pk_extra = ctrl.seek_pk_extra.as_slice();
    let Some((_g, _kind)) = read_lock(
        shared,
        peer,
        client_id,
        target_id,
        Access::UserRead,
        ReadFreshness::Current,
    )
    .await
    else {
        return;
    };
    // Bind the result before matching on it: an `if let Err(_)` scrutinee would
    // hold the `&mut CatalogEngine` temporary across the await below.
    let admitted = shared
        .cat()
        .registry()
        .index_cols(target_id, seek_col_idx, "seek_by_index");
    let cols = match admitted {
        Ok(cols) => cols,
        Err(e) => {
            send_error(peer, target_id, client_id, e.to_string().as_bytes());
            return;
        }
    };
    // Single catalog scan (exact list match) answers "is there an index for this
    // column list"; the borrow ends with the condition, so none is held across
    // the await below.
    if shared
        .cat()
        .registry()
        .index_circuit_for_cols(target_id, cols.as_slice())
        .is_none()
    {
        // No secondary index for this column list: a dedicated control-only
        // status, caught here with zero worker dispatch, so the SQL planner falls
        // back to a scan or a CREATE INDEX hint without a prior catalog probe.
        send_control_only(peer, target_id, client_id, STATUS_NO_INDEX);
        return;
    }
    // Forward the wire frame verbatim (packed seek_col_idx, seek_pk +
    // seek_pk_extra) to the broadcast-and-merge fan-out.
    match shared
        .disp()
        .fan_out_seek_by_index_collect(&shared.reactor, target_id, seek_col_idx, seek_pk, seek_pk_extra)
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
            );
        }
        Err(f) => send_fault(peer, target_id, client_id, &f),
    }
}

/// The relation a RESOLVE names, with its kind and its wire class — or `None`
/// when it names none. `Err` is the one hard failure a resolve has: an unusable
/// request blob, or a schema that does not exist.
///
/// The registry lookup is the gate that makes the id safe to describe. Kind and
/// class come back off it, so the caller consumes that proof instead of
/// re-resolving.
fn resolve_request_target(
    shared: &Rc<Shared>,
    target_id: i64,
    name_blob: &[u8],
) -> Result<Option<(i64, RelationKind, gnitz_wire::RelClass)>, String> {
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
    // on the raw row sign while `hook_relation_register` registers only on net-live,
    // so the two maps are not maintained on one liveness rule.
    Ok(shared
        .cat()
        .registry()
        .entry(candidate)
        .map(|e| (candidate, e.kind, e.class())))
}

/// Build the RESOLVE reply: the schema block plus the [`gnitz_wire::RelDescriptorBlob`]
/// carrying what the block cannot (kind, placement, foreign keys, secondary
/// indexes). Served entirely from the typed caches the master already
/// maintains; it writes no SAL group and wakes no worker.
fn build_resolve_reply(
    shared: &Rc<Shared>,
    client_id: u64,
    target_id: i64,
    name_blob: &[u8],
) -> Result<PooledSendBuf, String> {
    let Some((tid, kind, class)) = resolve_request_target(shared, target_id, name_blob)? else {
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

    // Only an ingestion point reports its placement. A view and a system family are
    // both *stamped* `Replicated`, but this bit is a planner hint for a reduce built
    // directly over a source, and a view's locality is settled by the compiler from
    // the view's own stamped placement instead — reporting the stamp here would
    // re-plan an aggregate over a view on a second authority. A misreport either way
    // is a silent W-fold overcount: a replicated relation read as non-replicated has
    // every worker holding a full copy *and* its partials summed.
    let replicated = kind.is_ingestion_point() && shared.cat().registry().relation_is_replicated(tid);

    // Off the FK edge cache the catalog maintains from the same COL_TAB delta as
    // the column defs, one edge per (child, child column) — so this needs no band
    // scan and no `Rc<Vec<ColumnDef>>` cache fill. Unsorted: `fk_by_child` is
    // push-ordered, and the blob's only consumer scatters by `col_idx`, so order
    // is unobservable.
    let fks: Vec<gnitz_wire::RelFk> = shared
        .cat()
        .fk_constraints_of(tid)
        .iter()
        .map(|e| gnitz_wire::RelFk {
            col_idx: e.fk_col as u32,
            fk_col_idx: e.parent_col as u32,
            fk_table_id: e.parent_tid as u64,
        })
        .collect();
    let indexes: Vec<gnitz_wire::RelIndex> = shared
        .cat()
        .registry()
        .index_circuits(tid)
        .iter()
        .map(|ic| gnitz_wire::RelIndex {
            cols: ic.col_indices,
            is_unique: ic.is_unique,
        })
        .collect();
    let blob = gnitz_wire::RelDescriptorBlob {
        class,
        replicated,
        // Answered off the registry's own `delta_bytes`, so a subscriber discovers
        // the capability here instead of probing for it with a read that errors.
        delta: shared.cat().registry().relation_has_delta_feed(tid),
        fks,
        indexes,
    }
    .encode();

    // Clone the `Rc` block out of the cache so no `cat()` borrow outlives it.
    // The reply always carries the block: a resolving client holds no descriptor
    // to validate a version against.
    let schema = shared
        .cat()
        .registry()
        .get_schema_desc(tid)
        .ok_or_else(|| format!("table {tid} not found"))?;
    let entry = shared.cat_mut().schema_wire_entry(tid, &schema);
    let (schema_block, server_version) = (entry.block, entry.version);
    Ok(encode_response_buffer(ipc::WireMsg {
        target_id: tid as u64,
        client_id,
        flags: gnitz_wire::wire_flags_set_schema_version(0, server_version),
        status: STATUS_OK,
        schema_block: Some(schema_block.as_slice()),
        seek_pk_extra: &blob,
        ..Default::default()
    }))
}

/// True when no un-ticked commit can reach `target`: every source feeding it —
/// transitively, through view sources — committed at or below the last completed
/// tick's watermark.
///
/// Sound because the committer marks a tid in `tick_rows` before publishing that
/// commit's zone LSN, a contract stated at that mark; a *missing* map entry is
/// the `boot_seed` argument. Erring towards false is harmless (one extra drain),
/// which is where a stream lands: its reserved LSN is never published.
///
/// A non-view target is vacuously fresh and answers before the closure walk.
/// Caller holds the catalog read lock; the call is synchronous throughout, so
/// `source_closure`'s `&mut` rebuild crosses no await.
fn read_is_fresh(shared: &Rc<Shared>, target: i64) -> bool {
    if !shared
        .cat()
        .registry()
        .relation_kind(target)
        .is_some_and(|k| k.is_view())
    {
        return true;
    }
    let ticked = shared.last_tick_lsn.get();
    let (dag, registry) = shared.cat_mut().dag_and_registry_mut();
    dag.source_closure(registry, vec![target])
        .into_iter()
        .all(|s| shared.commit_lsn_of(s) <= ticked)
}

/// What a read owes the writes that came before it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadFreshness {
    /// Drain pending ticks when the target is a view `read_is_fresh` reports
    /// stale — every read verb that answers "what is current".
    Current,
    /// Answer against whatever the tick loop has already run. A delta read
    /// answers "what has happened", not "what is current", so a round the tick
    /// loop has not run yet is one the next poll carries — and the gate stays
    /// sound, because it keys on the last round that *reached* the view and an
    /// unticked push has reached nothing.
    ///
    /// Draining here would also cost out of proportion to the read: a `Drain`
    /// takes *every* pending tid, not just this view's, so polling subscribers
    /// would defeat the committer's row-threshold coalescing server-wide.
    AsOfLastTick,
}

/// Drop the caller's read guard, tick everything pending, and hand back a fresh
/// guard. Taking the guard by value is the deadlock precondition made structural:
/// the drain parks on the tick loop's reply, and this writer-preferring guard held
/// across that park would block DDL writers and `tick_loop`'s own read.
///
/// The trigger goes out even when nothing looks pending — the tick loop is serial,
/// so awaiting `done` also serializes behind a concurrent `Auto`, without which a
/// read could observe a view mid-tick. A failed tick is reported, not swallowed:
/// its views are stale, and serving them under `STATUS_OK` is a silent stale read.
async fn drain_and_relock(shared: &Rc<Shared>, guard: ReadGuard) -> Result<ReadGuard, WireFault> {
    drop(guard);
    // A cancelled receiver means the tick loop is gone; treat it as done.
    if let Some(Err(e)) = request_drain(shared).await {
        return Err(e);
    }
    Ok(shared.catalog_rwlock.read().await)
}

/// Take the catalog read lock and resolve `target_id`'s kind from the same probe
/// that validated it, draining first when `freshness` asks and the target is a
/// stale view. `None` means the target was rejected and the error is already sent.
///
/// The one read-lock entry point for every single-target read verb: each passes
/// the `Access` its realization can serve and routes on the returned kind, rather
/// than re-deciding the system/user split from the id. A stale view re-resolves
/// after the drain, since a DDL may have dropped it meanwhile.
async fn read_lock(
    shared: &Rc<Shared>,
    peer: &Peer,
    client_id: u64,
    target_id: i64,
    access: Access,
    freshness: ReadFreshness,
) -> Option<(ReadGuard, RelationKind)> {
    let g = shared.catalog_rwlock.read().await;
    let kind = target_kind_or_reject(shared, peer, client_id, target_id, access).await?;
    if freshness == ReadFreshness::AsOfLastTick || read_is_fresh(shared, target_id) {
        return Some((g, kind));
    }
    // No preliminary frame has gone out yet (`schema_block_for_reply`'s block is
    // emitted later), so a failed drain is still reportable as a plain error.
    let g = match drain_and_relock(shared, g).await {
        Ok(g) => g,
        Err(f) => {
            send_fault(peer, target_id, client_id, &f);
            return None;
        }
    };
    let kind = target_kind_or_reject(shared, peer, client_id, target_id, access).await?;
    Some((g, kind))
}

/// A **master-authored** reply's schema block, through the one negotiation
/// ([`CatalogEngine::negotiated_schema_block`]) the worker's `reply_schema_block`
/// also takes. This side resolves the descriptor from the registry, and takes a
/// `tid` naming no relation as "no block" — a scan of one has nothing to reply
/// about anyway.
///
/// `(server_version, block)` — one version, not two: the effective client version
/// handed to the workers is `server_version` either way, a cache hit meaning the
/// client's already equals it.
fn schema_block_for_reply(shared: &Rc<Shared>, tid: i64, client_version: u16) -> (u16, Option<Rc<Vec<u8>>>) {
    let (block, server_version) = shared
        .cat_mut()
        .negotiated_schema_block(tid, client_version, |c| c.registry().get_schema_desc(tid));
    (server_version, block)
}

/// The preliminary schema-only frame — carrying `FLAG_CONTINUATION`, the
/// `server_version`, and the captured wire block — that precedes a scan's data
/// frames on a schema-cache miss, in place of one schema block per worker.
fn prelim_schema_msg(tid: i64, client_id: u64, server_version: u16, block: &[u8]) -> ipc::WireMsg<'_> {
    ipc::WireMsg {
        target_id: tid as u64,
        client_id,
        flags: gnitz_wire::wire_flags_set_schema_version(gnitz_wire::FLAG_CONTINUATION, server_version),
        status: STATUS_OK,
        schema_block: Some(block),
        ..Default::default()
    }
}

/// SCAN: a catalog family is served master-locally, everything else fans out to
/// the workers. The split is decided by the kind `read_lock` resolved, not by the
/// id — so a scan of an id below the user floor that names no family is rejected
/// by the same "not found" the other verbs give, rather than by `scan_family`.
async fn handle_scan(shared: &Rc<Shared>, peer: &Peer, client_id: u64, target_id: i64, client_version: u16) {
    let Some((_g, kind)) = read_lock(shared, peer, client_id, target_id, Access::Read, ReadFreshness::Current).await
    else {
        return;
    };
    if kind == RelationKind::SystemCatalog {
        scan_system_family(shared, peer, client_id, target_id, client_version).await;
        return;
    }
    let lsn = shared.last_tick_lsn.get();

    let (server_version, prelim) = schema_block_for_reply(shared, target_id, client_version);
    if let Some(block) = prelim {
        send_msg(
            peer,
            prelim_schema_msg(target_id, client_id, server_version, block.as_slice()),
        );
    }

    let unicast = read_fanout(shared.disp(), target_id, None);
    // Embed the client's schema version in wire_flags so workers can decide
    // whether to include the schema block in their response.
    let result = shared
        .disp()
        .fan_out_scan(
            &shared.reactor,
            unicast,
            target_id,
            client_id,
            peer,
            SalMessageKind::Scan,
            gnitz_wire::wire_flags_set_schema_version(0, server_version),
            &[],
        )
        .await;
    // A plain scan has only an LSN to report, so the sampled round is dropped
    // explicitly here rather than hidden inside the fan-out.
    finish_scan_fanout(peer, target_id, client_id, lsn as u128, result.map(|(ok, _)| ok)).await;
}

/// The terminal frame of a reply train: `STATUS_OK`, no schema block, no data.
///
/// `seek_pk` is the read's watermark, and the field is already a `u128`. A plain
/// scan puts the last-committed LSN there; a delta read puts the pair
/// `(cursor tag, T)`, tag in the high half — so the whole cursor a subscriber
/// stores costs no wire bytes.
fn terminal_scan_msg(target_id: i64, client_id: u64, seek_pk: u128) -> ipc::WireMsg<'static> {
    ipc::WireMsg {
        target_id: target_id as u64,
        client_id,
        seek_pk,
        status: STATUS_OK,
        ..Default::default()
    }
}

/// Finish one scan-shaped fan-out: `Ok(true)` → the terminal frame (stamped
/// with the pre-dispatch `seek_pk`), `Ok(false)` → the forward already failed
/// (close the peer), `Err` → a fault frame carrying the worker's own status.
/// Shared by the plain scan and the ScanSpec handler.
async fn finish_scan_fanout(
    peer: &Peer,
    target_id: i64,
    client_id: u64,
    seek_pk: u128,
    result: Result<bool, WorkerFault>,
) {
    match result {
        // Corked, not sent: `forward_scan_slots` corked the heads it coalesced
        // and nothing parked in between, so the whole reply leaves as one send.
        Ok(true) => {
            send_msg(peer, terminal_scan_msg(target_id, client_id, seek_pk));
        }
        Ok(false) => peer.close(),
        Err(f) => send_fault(peer, target_id, client_id, &f),
    }
}

/// Parameterized bounded read (`ReadSpec`). The scan pipeline, minus schema
/// negotiation: the client authors the reply schema and ships it in
/// `seek_pk_extra`, so the reply carries no schema block. Terminal-frame and
/// failure handling are identical to `handle_scan`; the routing differs, since a
/// bound can confine the read to one worker, and a **delta** bound differs in
/// three more ways.
///
/// The request blob is unpacked **once**, at the top, and the resulting spec
/// drives all three of the dispatch classification, the routing and the gate —
/// `read_fanout` takes that spec rather than unpacking the blob again to reach
/// the same bytes.
///
/// 1. It takes no drain ([`ReadFreshness::AsOfLastTick`]).
/// 2. Its group is a `DeltaScanSpec`, which is what makes the worker
///    defer it out of an in-flight evaluation.
/// 3. Its terminal frame reports `(cursor tag, T)` instead of the last-committed
///    LSN, and an up-to-date poll is answered here, master-locally.
async fn handle_scan_spec(shared: &Rc<Shared>, peer: &Peer, client_id: u64, target_id: i64, seek_pk_extra: &[u8]) {
    // The one unpack of the request blob on this side; `read_fanout` takes the
    // spec it yields rather than reaching the same bytes a second time. A blob
    // that does not split is left to the worker — the sole `ReadSpec` decoder and
    // trust boundary — and routes meanwhile as an empty spec, which names no PK
    // range and so confines nothing.
    let spec = gnitz_wire::unpack_scan_spec_extra(seek_pk_extra)
        .map(|(spec, _block)| spec)
        .unwrap_or(gnitz_wire::SpecBytes(&[]));
    let delta_cursor = gnitz_wire::peek_delta_bound(spec);
    let freshness = match delta_cursor {
        Some(_) => ReadFreshness::AsOfLastTick,
        None => ReadFreshness::Current,
    };
    // `UserRead`: a `ReadSpec` has only a fan-out realization, which a catalog
    // family has no form of.
    let Some((_g, _kind)) = read_lock(shared, peer, client_id, target_id, Access::UserRead, freshness).await else {
        return;
    };

    // No await between the gate and the round, so the terminal cannot report one
    // from after a tick the gate was tested against. `terminal_scan_msg` and not
    // `send_control_only`: the latter leaves `seek_pk` zero, a tag the client
    // never matches, which is the permanent re-read loop this gate avoids.
    if delta_cursor.is_some_and(|after| delta_up_to_date(shared, target_id, after)) {
        let disp = shared.disp();
        let seek_pk = delta_terminal_seek_pk(disp, target_id, disp.last_tick_round());
        send_msg(peer, terminal_scan_msg(target_id, client_id, seek_pk));
        return;
    }

    // A PK range confined to one partition unicasts: one SAL slot instead of W,
    // each of which would carry its own copy of the spec blob under the exclusive
    // SAL mutex.
    let unicast = read_fanout(shared.disp(), target_id, Some(spec));
    let msg_kind = if delta_cursor.is_some() {
        SalMessageKind::DeltaScanSpec
    } else {
        SalMessageKind::ScanSpec
    };
    let result = shared
        .disp()
        .fan_out_scan(
            &shared.reactor,
            unicast,
            target_id,
            client_id,
            peer,
            msg_kind,
            0,
            seek_pk_extra,
        )
        .await;
    let seek_pk = match (&result, delta_cursor) {
        (Ok((_, round)), Some(_)) => delta_terminal_seek_pk(shared.disp(), target_id, *round),
        _ => shared.last_tick_lsn.get() as u128,
    };
    finish_scan_fanout(peer, target_id, client_id, seek_pk, result.map(|(ok, _)| ok)).await;
}

/// DELTA_POLL: advance N mirrored views in one request, one catalog lock and —
/// for however many of them moved — one broadcast.
///
/// A fault at `target_id = 0` rejects the frame; a fault naming a view ends that
/// view's position and no other.
async fn handle_delta_poll(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8]) {
    match delta_poll_body(shared, peer, client_id, data).await {
        // Every view's train and terminal is already out.
        Ok(true) => {}
        // Client disconnected mid-stream: leases dropped in the body.
        Ok(false) => peer.close(),
        // A frame-shape rejection, before any group was written.
        Err(f) => send_fault(peer, 0, client_id, &f),
    }
}

/// What one view of a poll is answered with.
enum PollPosition {
    /// The view cannot be read at all.
    Fault(WireFault),
    /// The view is already at its last round, so its terminal is master-local.
    UpToDate,
    /// The view moved, and takes the next dispatch of the poll's cut.
    Moved,
}

/// Body of [`handle_delta_poll`]. `Ok(true)` once every view's train and
/// terminal have been sent; `Ok(false)` on client disconnect; `Err` on a
/// frame-shape rejection. A per-view failure is not an `Err` — it goes out as
/// that view's own fault frame and the rest of the poll continues.
async fn delta_poll_body(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8]) -> Result<bool, WireFault> {
    let views = gnitz_wire::txn_frame::decode_delta_poll(data).map_err(|e| format!("decode error: {e}"))?;
    validate_item_ids("DELTA_POLL", &views, |v| v.0)?;

    // ── Phase 1: classify under the catalog lock, dispatch one cut ─────────
    // No await between a view's gate test and the round its terminal reports, so
    // no tick can land in between. The guard is scoped to this phase: phase 2
    // reads no catalog state, and holding it across the drain would block DDL.
    //
    // A poll with nothing to fetch dispatches nothing, so it takes no SAL hold
    // and signals no worker — the steady state of a subscription.
    let (positions, dispatches, up_to_date_round, dispatch_round) = {
        let _g = shared.catalog_rwlock.read().await;
        let disp = shared.disp();
        let up_to_date_round = disp.last_tick_round();
        let mut positions = Vec::with_capacity(views.len());
        let mut moved: Vec<(u64, &[u8], Fanout)> = Vec::with_capacity(views.len());
        for &(view_id, extra) in &views {
            let tid = view_id as i64;
            let position = match poll_read_for_view(shared, tid, extra) {
                Err(f) => PollPosition::Fault(f),
                Ok(None) => PollPosition::UpToDate,
                Ok(Some(spec)) => {
                    moved.push((view_id, extra, read_fanout(disp, tid, Some(spec))));
                    PollPosition::Moved
                }
            };
            positions.push((tid, position));
        }

        let fanouts: Vec<Fanout> = moved.iter().map(|&(_, _, f)| f).collect();
        let (dispatches, dispatch_round) =
            dispatch_scan_multi_fanout(disp, &shared.reactor, &fanouts, |i, targets, wire_flags, round| {
                let (view_id, extra, _) = moved[i];
                disp.write_group(&DirectGroup {
                    template: ipc::WireMsg {
                        target_id: view_id,
                        client_id,
                        flags: wire_flags,
                        seek_pk: round as u128,
                        seek_pk_extra: extra,
                        ..Default::default()
                    },
                    targets,
                    ..DirectGroup::new(SalMessageKind::DeltaScanSpec)
                })
            })
            .await?;
        (positions, dispatches, up_to_date_round, dispatch_round)
    };

    // ── Phase 2: one terminal per view, in request order ───────────────────
    let disp = shared.disp();
    let mut dispatches = dispatches.into_iter();
    for (tid, position) in positions {
        let watermark = |round| delta_terminal_seek_pk(disp, tid, round);
        match position {
            PollPosition::Fault(fault) => send_fault(peer, tid, client_id, &fault),
            PollPosition::UpToDate => send_msg(peer, terminal_scan_msg(tid, client_id, watermark(up_to_date_round))),
            // Taken in step with the `Moved`s that were pushed. A dispatch left
            // undrained — an earlier return dropped it — discards the rest of
            // its train at the ring boundary.
            PollPosition::Moved => {
                let d = dispatches.next().expect("one dispatch per moved view");
                match d.await_and_forward(&shared.reactor, peer).await {
                    Ok(true) => send_msg(peer, terminal_scan_msg(tid, client_id, watermark(dispatch_round))),
                    Ok(false) => return Ok(false),
                    Err(fault) => send_fault(peer, tid, client_id, &fault),
                }
            }
        }
        // Carry no more than the budget into the next view, and learn here
        // rather than at the end if the client is gone.
        if peer.flush_if_full().await < 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The read this view needs, or `None` where it already sits at its last round
/// and its terminal is master-local. Takes the caller's catalog read lock.
///
/// A blob carrying any bound but `Delta` is a fault: served here it would answer
/// "what is current" without the drain that owes.
fn poll_read_for_view<'a>(
    shared: &Rc<Shared>,
    tid: i64,
    extra: &'a [u8],
) -> Result<Option<gnitz_wire::SpecBytes<'a>>, WireFault> {
    let fault = |e: String| -> WireFault { format!("delta_poll: {tid}: {e}").into() };
    target_kind(shared, tid, Access::UserRead)?;
    let (spec, _block) = gnitz_wire::unpack_scan_spec_extra(extra).map_err(fault)?;
    let after = gnitz_wire::peek_delta_bound(spec).ok_or_else(|| fault("read carries no delta bound".to_string()))?;
    Ok((!delta_up_to_date(shared, tid, after)).then_some(spec))
}

/// Whether a delta read after `after_tick` already sits at the view's last round,
/// so it can be answered without reaching a worker — the steady state of a
/// subscription, where a fan-out per poll would cost W wakeups.
///
/// `false` at `after_tick = 0` (the bootstrap bound) and for a relation with no
/// feed: both must reach the store, the second to be refused there.
fn delta_up_to_date(shared: &Shared, target_id: i64, after_tick: u64) -> bool {
    after_tick > 0
        && shared.cat().registry().relation_has_delta_feed(target_id)
        && after_tick >= shared.disp().last_delta_round(target_id)
}

/// A delta reply's terminal `seek_pk`: the cursor tag in the high half, `T` in
/// the low half. `MasterDispatcher::delta_cursor_tag` states why one field
/// answers both questions.
fn delta_terminal_seek_pk(disp: &MasterDispatcher, target_id: i64, round: u64) -> u128 {
    gnitz_wire::pack_delta_watermark(disp.delta_cursor_tag(target_id), round)
}

/// One relation's Phase-1 capture for `scan_multi_body`: exactly what
/// [`schema_block_for_reply`] answered, carried to the deferred Phase-2 emit.
struct ScanMultiRelPlan {
    tid: i64,
    /// Stamped into the preliminary frame, and handed to the workers as their
    /// effective client version so they omit their own schema blocks.
    server_version: u16,
    /// The wire schema block to emit before this relation's train, present iff
    /// the client's cached version missed.
    block: Option<Rc<Vec<u8>>>,
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
        Err(f) => send_fault(peer, 0, client_id, &f),
    }
}

/// Body of `handle_scan_multi`. `Ok(true)` once every relation's train and
/// terminal have been sent; `Ok(false)` on client disconnect; `Err(msg)` on a
/// shape/tid rejection or a mid-stream worker fault. All `ScanLease`s live in
/// the `dispatches` vec and drop on return, so any error/disconnect return
/// deregisters every id and discards undrained frames at the ring boundary.
async fn scan_multi_body(shared: &Rc<Shared>, peer: &Peer, client_id: u64, data: &[u8]) -> Result<bool, WireFault> {
    // ── Phase 0: decode + frame-local shape rules ──────────────────────────
    // The authoritative run of the shared shape validator — a client may skip
    // its own. tid legality is Phase 1's, under the catalog lock.
    let relations = gnitz_wire::txn_frame::decode_scan_multi(data).map_err(|e| format!("decode error: {e}"))?;
    validate_item_ids("SCAN_MULTI", &relations, |r| r.0)?;

    // Drain once if any target is a stale view — the same test `read_lock` runs
    // for a single target. Phase 1 resolves every tid's kind under the guard
    // handed back, so a DDL during the drain is caught there and an unknown tid
    // is rejected there rather than here.
    let mut cat = shared.catalog_rwlock.read().await;
    if relations.iter().any(|&(tid, _)| !read_is_fresh(shared, tid as i64)) {
        cat = drain_and_relock(shared, cat).await?;
    }
    // The shared LSN stamped into every terminal.
    let lsn = shared.last_tick_lsn.get();

    // ── Phase 1: catalog lock — resolve shapes + schemas, dispatch one cut ──
    // Resolve every relation from the same catalog snapshot (so all N are
    // consistent with the cut even if a DDL commits during the later drain),
    // then write all N groups under one `sal_writer_excl` hold. The
    // catalog-read ⊃ `sal_writer_excl` order matches every other SAL writer, so
    // no lock inversion. The guard is the one `drain_and_relock` handed back, so
    // a DDL during the drain is caught by the per-tid resolution below.
    let (dispatches, plans) = {
        let _cat = cat;
        let mut plans: Vec<ScanMultiRelPlan> = Vec::with_capacity(relations.len());
        let mut fanout: Vec<Fanout> = Vec::with_capacity(relations.len());
        for &(tid_u, client_ver) in &relations {
            let tid = tid_u as i64;
            // Base tables AND views are legal; `UserRead` refuses a catalog
            // family, which stays on the plain path that serves it
            // master-locally.
            // Status dropped for the reason `push_txn_body`'s is.
            target_kind(shared, tid, Access::UserRead).map_err(|f| f.text)?;
            fanout.push(read_fanout(shared.disp(), tid, None));
            // Capture (not emit) each relation's preliminary schema frame here so
            // Phase 2 can send it after the one-cut dispatch, in request order.
            let (server_version, block) = schema_block_for_reply(shared, tid, client_ver);
            plans.push(ScanMultiRelPlan { tid, server_version, block });
        }
        let disp = shared.disp();
        let (dispatches, _) =
            dispatch_scan_multi_fanout(disp, &shared.reactor, &fanout, |i, targets, wire_flags, _| {
                let plan = &plans[i];
                disp.write_group(&DirectGroup {
                    template: ipc::WireMsg {
                        target_id: plan.tid as u64,
                        client_id,
                        flags: gnitz_wire::wire_flags_set_schema_version(wire_flags, plan.server_version),
                        ..Default::default()
                    },
                    targets,
                    ..DirectGroup::new(SalMessageKind::Scan)
                })
            })
            .await?;
        // Release the catalog read lock here: Phase 2 touches no catalog state
        // (the snapshot is worker-frozen and the schemas are captured), so
        // holding it across the whole bulk read would needlessly block DDL.
        (dispatches, plans)
    };

    // ── Phase 2: sequential per-relation drain (no locks; holds all leases) ──
    for (plan, d) in plans.iter().zip(&dispatches) {
        // Preliminary schema-only frame first, when captured in Phase 1.
        if let Some(block) = plan.block.as_ref() {
            send_msg(
                peer,
                prelim_schema_msg(plan.tid, client_id, plan.server_version, block.as_slice()),
            );
        }
        // Drain this relation's train (all workers, ascending) before the next —
        // the FIFO reply contract makes request order == ring order.
        match d.await_and_forward(&shared.reactor, peer).await {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(f) => return Err(f),
        }
        // Terminal frame for this relation (tid + the shared LSN).
        send_msg(peer, terminal_scan_msg(plan.tid, client_id, lsn as u128));
        // This relation's reply is complete: carry no more than the budget into
        // the next, and learn here rather than at the end if the client is gone.
        if peer.flush_if_full().await < 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The master-local half of [`handle_scan`]: a SCAN of a catalog family. The rows
/// come from the catalog, never from the frame, so the frame is not a parameter.
///
/// Takes NO lock: the caller holds the catalog read guard, and `read_ok` is
/// `!has_writer && writers_waiting == 0`, so a nested read parks forever the
/// moment a DDL writer queues.
async fn scan_system_family(shared: &Rc<Shared>, peer: &Peer, client_id: u64, target_id: i64, client_version: u16) {
    match guard_panic("scan", || shared.cat_mut().scan_family(target_id)) {
        Ok((b, _)) => {
            let batch_ref = if !b.is_empty() { Some(b) } else { None };
            send_ok_response(
                shared,
                peer,
                target_id,
                batch_ref.as_deref(),
                client_id,
                shared.last_tick_lsn.get() as u128,
                client_version,
            );
        }
        Err(e) => send_error(peer, target_id, client_id, e.as_bytes()),
    }
}

// ---------------------------------------------------------------------------
// Wire-protocol response helpers
// ---------------------------------------------------------------------------

/// Frame one reply into a pooled send buffer. Callers build the [`ipc::WireMsg`]
/// with the fields they actually set and leave the rest at `Default`.
fn encode_response_buffer(msg: ipc::WireMsg<'_>) -> PooledSendBuf {
    let mut inner = gnitz_store::storage::batch_pool::acquire_buf();
    inner.reserve(8192);
    encode_response_into(&mut inner, msg);
    PooledSendBuf(inner)
}

/// Append `msg`'s framed bytes to `out`. The corking replies encode straight
/// into the peer's accumulator through this, so a reply that will be batched
/// never passes through a buffer of its own.
fn encode_response_into(out: &mut Vec<u8>, msg: ipc::WireMsg<'_>) {
    const PFX: usize = gnitz_wire::FRAME_LEN_PREFIX_BYTES;
    let sz = msg.size();
    let base = out.len();
    let total = base + PFX + sz;
    out.reserve(PFX + sz);
    // SAFETY: `encode_ipc` writes every byte of the payload and the frame length
    // prefix is written immediately below. wal::encode zeros inter-region padding
    // (Step 1), so no byte is left uninitialised regardless of column type.
    #[allow(clippy::uninit_vec)]
    unsafe {
        out.set_len(total);
    }
    out[base..base + PFX].copy_from_slice(&(sz as u32).to_le_bytes());
    let written = msg.encode_ipc(&mut out[base + PFX..total], 0);
    debug_assert_eq!(written, sz);
    out.truncate(base + PFX + written);
}

/// Cork `msg` for the client — and the one place a master-authored reply meets
/// [`ipc::FRAME_CAP`]. The master-local system-family scan and seek have no other
/// bound; the builders that encode without coming through here are structurally
/// bounded (a schema block by `MAX_COLS`, prelim frames by carrying no data, a
/// resolve descriptor by the relation's FK and index counts).
///
/// Corking rather than sending is what lets a pipelined run of these leave
/// together, and is sound because every reply through here is the last thing its
/// handler writes. Nothing is awaited: the connection loop ships it.
fn send_msg(peer: &Peer, msg: ipc::WireMsg<'_>) {
    let sz = msg.size();
    if sz > ipc::FRAME_CAP {
        let text = ipc::oversized_frame_message(sz);
        let fallback = ipc::WireMsg {
            target_id: msg.target_id,
            client_id: msg.client_id,
            status: STATUS_ERROR,
            error_msg: text.as_bytes(),
            ..Default::default()
        };
        peer.cork_with(|out| encode_response_into(out, fallback));
        return;
    }
    peer.cork_with(|out| encode_response_into(out, msg));
}

fn send_ok_response(
    shared: &Rc<Shared>,
    peer: &Peer,
    target_id: i64,
    result: Option<&Batch>,
    client_id: u64,
    seek_pk: u128,
    client_version: u16,
) {
    let (server_version, schema_block) = schema_block_for_reply(shared, target_id, client_version);
    let schema_arg = schema_block.as_ref().map(|b| b.as_slice());
    send_msg(
        peer,
        ipc::WireMsg {
            target_id: target_id as u64,
            client_id,
            flags: gnitz_wire::wire_flags_set_schema_version(0, server_version),
            seek_pk,
            status: STATUS_OK,
            data: ipc::WireData::Whole(result),
            schema_block: schema_arg,
            ..Default::default()
        },
    );
}

/// Control-only reply carrying just a status code and a target id: no schema,
/// no data, no error text. The named wrapper for the *signal* replies — the
/// schema-mismatch and no-index statuses, and an id allocation, whose answer *is*
/// the target id. Not every header-only frame goes out through here:
/// `terminal_scan_msg` and the DDL/TXN ACKs build their own, because each
/// carries a meaning in `seek_pk` this wrapper has no parameter for.
fn send_control_only(peer: &Peer, target_id: i64, client_id: u64, status: u32) {
    send_status_frame(peer, target_id, client_id, status, &[])
}

/// One control-only reply frame carrying `status` verbatim. No schema block: the
/// client ignores both it and the schema version on a failure, so `flags` stays 0
/// and the cache lookup is skipped.
fn send_status_frame(peer: &Peer, target_id: i64, client_id: u64, status: u32, error_msg: &[u8]) {
    send_msg(
        peer,
        ipc::WireMsg {
            target_id: target_id as u64,
            client_id,
            status,
            error_msg,
            ..Default::default()
        },
    )
}

/// Every failure that names its own status, master-minted or forwarded. A worker
/// mints one too — the scan-forward stack carries `(status, text)` from
/// `worker_error` down to here — so a typed refusal (a delta cursor past its
/// retention floor; a full SAL a client should retry) arrives as itself rather
/// than flattened to `STATUS_ERROR` plus prose.
fn send_fault(peer: &Peer, target_id: i64, client_id: u64, fault: &WireFault) {
    send_status_frame(peer, target_id, client_id, fault.status, fault.text.as_bytes())
}

/// A rejection that carries no status of its own, and so is `STATUS_ERROR`.
fn send_error(peer: &Peer, target_id: i64, client_id: u64, error_msg: &[u8]) {
    send_status_frame(peer, target_id, client_id, STATUS_ERROR, error_msg)
}

/// Why a stream cannot accept this push, or `None` if it can. Both rules restate
/// what a stream lacks — a unique primary key, and any retraction at all — and
/// rejecting `Error` mode is what keeps `push_reads_committed_state` false, and
/// with it the shared table lock and the unread mode field.
fn stream_push_error(target_id: i64, batch: &Batch, mode: gnitz_wire::WireConflictMode) -> Option<String> {
    if mode == gnitz_wire::WireConflictMode::Error {
        return Some(format!(
            "table {target_id} is a stream: conflict mode 'error' asserts a primary-key \
             uniqueness a stream does not have"
        ));
    }
    if batch.all_weights_positive() {
        return None;
    }
    // Located again only to name it in the message.
    let i = (0..batch.len())
        .find(|&i| batch.get_weight(i) <= 0)
        .expect("just found one");
    Some(format!(
        "table {target_id} is a stream: a stream is append-only, but row {i} of this push carries \
         weight {}",
        batch.get_weight(i)
    ))
}

#[cfg(test)]
#[path = "tests/executor.rs"]
mod tests;
