//! Server executor: the process lifecycle, the `Shared` state, the request
//! router and every read/push handler, and the reply-frame vocabulary. The
//! catalog-zone write path is the child `ddl`.
//!
//! The master owns one `Reactor` driving the accept socket, a task per
//! connection, the committer (group commit + checkpoint + fsync), the tick task
//! and the worker-crash watchdog. A tick relays its own exchange rounds while it
//! awaits its ACKs.
//!
//! A handler that splits into `handle_x` + `x_body` does so for one reason: every
//! rejection inside the body is a plain `Err`, so the handler above owns the
//! single reply path.

mod ddl;

use std::cell::{Cell, RefCell};
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use super::guard_panic;
use crate::runtime::tls::{TlsListener, TlsShared};
use gnitz_foundation::fault::Seam;

use self::ddl::{commit_serial_range_durable, handle_ddl_txn, hold_relay_for_ddl, RELAY_HOLD_FOR_DDL};
use super::TxnFamily;
use crate::catalog::CatalogEngine;
use crate::runtime::committer::{self, BarrierKind, CommitRequest, PendingPush, PendingTxn};
use crate::runtime::lsn::ZoneLsnAllocator;
use crate::runtime::master::{
    exchange::{ExchangeAccumulator, PendingRelay},
    forward_scan, MasterDispatcher, WORKER_WATCH,
};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{chan, oneshot, select2, AsyncRwLock, Either, ReadGuard, RecvBuf, WriteGuard};
use crate::runtime::sal::{DirectGroup, GroupTargets, SalFit, SalMessageKind};
use crate::runtime::wire::{self as ipc, validate_schema_match};
use gnitz_store::relation::{Relation, RelationKind};
use gnitz_store::schema::key::seek_opk_bytes;
use gnitz_store::schema::{SchemaDescriptor, SchemaFacts};
use gnitz_store::storage::Batch;
use gnitz_wire::control::DecodedControl;
use gnitz_wire::txn_frame::DeltaPollItem;
use gnitz_wire::BackfillDecision;
use gnitz_wire::{PkKeys, ReadBound, ReadSpec, WireFault, WireFlags, WireStatus};

const TICK_COALESCE_ROWS: usize = 10_000;

/// `GNITZ_INJECT_RELAY_SPACE_LOW`: report one exchange relay's SAL space as low,
/// so tests drive the reclamation protocol (worker re-epoch, master
/// `checkpoint_reset`, epoch advancing) over a small table that would never
/// approach the 1 GiB mmap.
static RELAY_SPACE_LOW: Seam = Seam::new("GNITZ_INJECT_RELAY_SPACE_LOW");

/// `GNITZ_INJECT_TICK_EMIT_ERROR`: fail the next tick emit, once, as a full SAL
/// would.
static TICK_EMIT_ERROR: Seam = Seam::new("GNITZ_INJECT_TICK_EMIT_ERROR");

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
        shared
            .disp()
            .reactor()
            .timer(Instant::now() + Duration::from_millis(1))
            .await;
    }
    gnitz_warn!("{}: seam armed but the event never arrived; releasing", what);
}

/// Hold one decoded push, before its catalog read lock, until a DDL has moved
/// `target_id`'s schema version — the window a warm push's decode-time descriptor
/// goes stale in, which production reaches when that read parks behind a queued
/// `ALTER TABLE` writer.
async fn hold_push_for_ddl(shared: &Shared, target_id: i64) {
    let seen = shared.cat().schema_version_of(target_id);
    park_until(shared, PUSH_HOLD_MAX_POLLS, "push hold", || {
        shared.cat().schema_version_of(target_id) != seen
    })
    .await
}

use gnitz_wire::ClientVerb;

/// One tick request to `tick_loop`. Minted only by `request_drain` and
/// [`Shared::note_commit_rows`].
enum TickTrigger {
    /// Fire-and-forget trigger from INSERT when a tid crosses the row
    /// coalesce threshold.  Tids come from `tick_rows`.
    Auto,
    /// Explicit drain requested by a read or by the checkpoint: tick whatever is
    /// pending — even nothing — and report the tick's verdict on `done`. A reader
    /// that waited on a failed tick must be told: its view is stale, and reporting
    /// success would serve stale rows under `WireStatus::Ok`.
    Drain {
        done: oneshot::Sender<Result<(), WireFault>>,
    },
}

/// Ask the tick loop to tick everything pending and report the tick's verdict.
pub(super) fn request_drain(shared: &Shared) -> oneshot::Receiver<Result<(), WireFault>> {
    let (done, rx) = oneshot::channel();
    shared.tick_tx.send(TickTrigger::Drain { done });
    rx
}

/// Send the committer a barrier of `kind`; the receiver resolves once it is
/// serviced.
fn request_barrier(shared: &Shared, kind: BarrierKind) -> oneshot::Receiver<()> {
    let (done, rx) = oneshot::channel();
    shared.committer_tx.send(CommitRequest::Barrier { kind, done });
    rx
}

/// Shared executor state held by every task.
pub struct Shared {
    dispatcher: Rc<MasterDispatcher>,
    committer_tx: chan::Sender<CommitRequest>,
    catalog_rwlock: AsyncRwLock,
    /// Held shared by every tick, so a writer runs with no tick in flight. Lock
    /// order: catalog, then this, then the SAL writer.
    pub(super) tick_gate: AsyncRwLock,
    /// Tick trigger sender. Reached only through `request_drain` and
    /// [`Shared::note_commit_rows`], so every trigger this process sends is minted
    /// in one place.
    tick_tx: chan::Sender<TickTrigger>,
    /// Zone-LSN allocation high-water + durability watermark, read by the
    /// committer so the read handlers report the same LSN it assigns.
    pub(super) lsn_alloc: ZoneLsnAllocator,
    last_tick_lsn: Cell<u64>,
    /// Tables with a pending delta, each with the row count feeding the tick
    /// threshold.
    tick_rows: RefCell<FxHashMap<i64, usize>>,
    /// Per-table write serialization. A push whose validation reads committed
    /// state (`push_reads_committed_state`) and every transaction take the write
    /// guard; a push that reads no committed state takes the read guard, so
    /// same-table pushes reach the committer concurrently and share one fsync.
    table_locks: RefCell<FxHashMap<i64, AsyncRwLock>>,
    /// Set true by the graceful-shutdown watcher before it sends the final
    /// Shutdown barrier. Read only by [`Shared::enqueue_commit`], which is what
    /// makes the test and the send one step.
    draining: Cell<bool>,
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
    /// seeded to the catalog's system zone — the value `lsn_alloc.published()`
    /// also starts at. Within a boot every live OCC basis is ≥ it and every
    /// commit's zone exceeds it, so a miss cannot false-pass. That rests on a
    /// basis being a read's watermark, held no longer than the connection that
    /// read it, not on this dominating every pre-crash durable zone, which it
    /// need not.
    boot_seed: u64,
    /// Set by the watchdog when it tears the node down over a dead worker. The
    /// watchdog is detached and its `Output` discarded, so this is how the
    /// verdict reaches `ServerExecutor::run`.
    worker_crashed: Cell<bool>,
    /// The data directory, where each worker's log lives.
    data_dir: String,
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

    fn table_lock(&self, tid: i64) -> AsyncRwLock {
        self.table_locks.borrow_mut().entry(tid).or_default().clone()
    }

    /// Take the write guard on every table in `tids`, sorting and deduping here
    /// so no caller can get it wrong: ascending order is what keeps a child
    /// INSERT and a parent DELETE from deadlocking on the same set, and a repeat
    /// would re-guard a lock this task holds and hang forever.
    ///
    /// Owned, because the set is read out of the catalog and this loop awaits.
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

    /// Commit `req` unless a graceful shutdown has begun, and await its verdict
    /// under the caller's catalog read guard. No await separates the test from the
    /// send: a request that saw a live server is queued ahead of the watchdog's
    /// Shutdown barrier.
    async fn commit(
        &self,
        _catalog: &ReadGuard,
        req: impl FnOnce(oneshot::Sender<Result<u64, WireFault>>) -> CommitRequest,
    ) -> Result<u64, WireFault> {
        if self.draining.get() {
            return Err("server shutting down".to_string().into());
        }
        let (done, rx) = oneshot::channel();
        self.committer_tx.send(req(done));
        rx.await
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
    /// Every per-relation master state a drop must reclaim is cleared here: ids
    /// are never reused, so nothing else would ever reclaim it.
    fn forget_relation(&self, _catalog_write: &WriteGuard, id: i64) {
        self.table_locks.borrow_mut().remove(&id);
        self.table_commit_lsn.borrow_mut().remove(&id);
        self.disp().forget_delta_round(id);
        self.disp().unique_filter_invalidate_table(id);
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
                .filter(|&tid| self.cat().registry.has_id(tid)),
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
    pub fn run(
        dispatcher: Rc<MasterDispatcher>,
        data_dir: &str,
        server_fd: i32,
        tls: Option<TlsListener>,
        lsn_seed: u64,
    ) -> i32 {
        let reactor = Rc::clone(dispatcher.reactor());

        // Every zone LSN this boot allocates exceeds `lsn_seed`; the boot
        // recovery that computes it states what it dominates.
        let initial_lsn = lsn_seed;

        let (committer_tx, committer_rx) = chan::unbounded::<CommitRequest>();
        let (tick_tx, tick_rx) = chan::unbounded::<TickTrigger>();
        let shared = Rc::new(Shared {
            dispatcher,
            committer_tx,
            catalog_rwlock: AsyncRwLock::default(),
            tick_gate: AsyncRwLock::default(),
            tick_tx,
            lsn_alloc: ZoneLsnAllocator::new(initial_lsn),
            last_tick_lsn: Cell::new(initial_lsn),
            tick_rows: RefCell::new(FxHashMap::default()),
            table_locks: RefCell::new(FxHashMap::default()),
            draining: Cell::new(false),
            table_commit_lsn: RefCell::new(FxHashMap::default()),
            boot_seed: initial_lsn,
            worker_crashed: Cell::new(false),
            data_dir: data_dir.to_string(),
        });

        // Catch SIGTERM/SIGINT so the watchdog can drive a final checkpoint
        // before exiting.
        install_shutdown_signal_handlers();

        reactor.spawn(committer::run(committer_rx, Rc::clone(&shared)));
        reactor.spawn(unix_accept_loop(Rc::clone(&shared), server_fd));
        if let Some(tl) = tls {
            reactor.spawn(tls_accept_loop(Rc::clone(&shared), tl));
        }
        reactor.spawn(tick_loop(Rc::clone(&shared), tick_rx));
        reactor.spawn(watchdog(Rc::clone(&shared)));

        reactor.block_until_shutdown();
        // `2` separates a dead worker from a failed boot's `1` and from
        // `gnitz_fatal_abort!`'s `134`.
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

async fn unix_accept_loop(shared: Rc<Shared>, listen_fd: i32) {
    let mut accepted = shared.disp().reactor().attach_listener(listen_fd);
    loop {
        let fd = accepted.recv().await;
        let peer = Peer::unix(fd, Rc::clone(shared.disp().reactor()));
        let s = Rc::clone(&shared);
        // No pre-auth deadline: access here is gated by the socket path's
        // filesystem permissions, and whoever can open it already has full
        // DDL/DML authority, so squatting gains nothing.
        shared.disp().reactor().spawn(connection_loop(peer, s, None));
    }
}

async fn tls_accept_loop(shared: Rc<Shared>, tl: TlsListener) {
    let mut accepted = shared.disp().reactor().attach_listener(tl.fd());
    loop {
        let fd = accepted.recv().await;
        let raw = fd.as_raw_fd();
        // Global connection cap: close the freshly-accepted fd before any TLS
        // work when the live count is at the cap.
        let Some(guard) = tl.admit() else {
            gnitz_warn!("tls: connection cap {} reached; closing fd={raw}", tl.max_conns());
            continue;
        };
        let conn = match TlsShared::start(
            Rc::clone(shared.disp().reactor()),
            fd,
            std::sync::Arc::clone(&tl.cfg),
            guard,
        ) {
            Ok(conn) => conn,
            Err(e) => {
                // `fd` and `guard` were moved into `start`; on the error path they
                // already dropped (closing and decrementing) inside its frame.
                gnitz_warn!("tls: session init failed for fd={raw}: {e}");
                continue;
            }
        };
        let peer = Peer::tls(conn);
        let s = Rc::clone(&shared);
        // Pre-auth first-frame deadline: HELLO must arrive within this window of
        // accept, else the connection is torn down (covers a stalled handshake
        // and a completed-handshake-no-HELLO squat alike).
        let deadline = Instant::now() + tl.pre_auth_window;
        shared.disp().reactor().spawn(connection_loop(peer, s, Some(deadline)));
    }
}

/// `first_frame_deadline` bounds the pre-auth window: `Some` where the transport
/// has not authenticated this peer and HELLO must arrive within it, `None` where
/// the socket's own permissions are the gate. Only the first recv is raced
/// against it.
async fn connection_loop(peer: Peer, shared: Rc<Shared>, first_frame_deadline: Option<Instant>) {
    serve_connection(&peer, &shared, first_frame_deadline).await;
    // The one exit: ship what is corked — a rejection is a corked reply like any
    // other — then retire the fd.
    let _ = peer.flush_egress().await;
    peer.close();
}

/// One message handled to completion before the next is received, so replies
/// leave in request order — which is how clients correlate them (`gnitz.aio`
/// gathers a mixed group onto one round-trip and rejects an out-of-order
/// `target_id`). Spawning `handle_message` to overlap requests would break that.
///
/// Returns when the peer is gone or refused; the caller closes.
async fn serve_connection(peer: &Peer, shared: &Rc<Shared>, first_frame_deadline: Option<Instant>) {
    // No HELLO in time (`Either::B`) → `None`. `select2` drops the losing timer,
    // which removes its deadline, so the happy path leaves no timer behind.
    let first = match first_frame_deadline {
        Some(deadline) => match select2(peer.recv(), shared.disp().reactor().timer(deadline)).await {
            Either::A(opt) => opt,
            Either::B(()) => None,
        },
        None => peer.recv().await,
    };
    let Some(buf) = first else { return };
    if !run_hello_handshake(peer, buf.as_slice()) {
        return;
    }

    loop {
        // Never wait on the client holding corked bytes, and never hold more
        // than the budget: between them these are the whole shipping rule.
        let mut next = peer.try_recv();
        if next.is_none() {
            if peer.flush_egress().await.is_err() {
                return;
            }
            next = peer.recv().await;
        }
        let Some(buf) = next else { return };
        handle_message(peer, buf, shared).await;
        if peer.flush_if_full().await.is_err() {
            return;
        }
    }
}

/// Validate a HELLO frame, cork the ACK, and raise the connection to the
/// established frame ceiling. `false` = refused.
fn run_hello_handshake(peer: &Peer, data: &[u8]) -> bool {
    let Ok(version) = gnitz_wire::decode_hello_payload(data) else {
        return false;
    };

    let server_version = gnitz_wire::wal::WAL_FORMAT_VERSION;
    if version != server_version {
        let msg = format!("unsupported wire version: peer={version}, server={server_version}");
        send_error(peer, 0, msg.as_bytes());
        return false;
    }

    peer.cork(&gnitz_wire::encode_hello_ack());
    peer.mark_established();
    true
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
        shared.disp().reactor().timer(Instant::now() + WORKER_WATCH).await;

        if SHUTDOWN_REQUESTED.load(std::sync::atomic::Ordering::Relaxed) {
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
            request_barrier(&shared, BarrierKind::Shutdown).await;

            // 3. Shut the workers down, then stop the reactor. The reactor/W2M
            //    receiver stays live throughout, so no `w2m()` handle dangles.
            shared.disp().shutdown_workers().await;
            shared.disp().reactor().request_shutdown();
            return;
        }

        if let Some(crashed) = shared.disp().check_workers() {
            let data_dir = &shared.data_dir;
            gnitz_error!("Worker {crashed} crashed (log: {data_dir}/worker_{crashed}.log), shutting down");
            shared.worker_crashed.set(true);
            shared.disp().kill_workers();
            shared.disp().reactor().request_shutdown();
            return;
        }

        // The only reclaim trigger on a workload with no writes, whose reads
        // still write SAL groups. Not awaited: the next tick re-sends.
        if shared.disp().sal().below_reclaim_margin() {
            drop(request_barrier(&shared, BarrierKind::Reclaim { forced: false }));
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
/// crosses `TICK_COALESCE_ROWS`, and `Drain` senders are parked on the answer —
/// so this loop never delays a tick to gather more.
///
/// A failure in one trigger fails only that trigger; SAL emission is further
/// guarded by `guard_panic` inside `run_tick`.
async fn tick_loop(shared: Rc<Shared>, mut rx: chan::Receiver<TickTrigger>) {
    let nw = shared.disp().num_workers();
    let mut triggers: Vec<TickTrigger> = Vec::new();
    // The batch's `Drain` repliers, held across the tick they are waiting on.
    let mut dones: Vec<oneshot::Sender<Result<(), WireFault>>> = Vec::new();
    // Reused across every tick; `drain_live_tick_rows_into` clears it before
    // refilling so capacity is retained.
    let mut tids_scratch: Vec<i64> = Vec::new();
    // Rounds complete within the tick that opened them, so this is empty between
    // ticks; kept to reuse its map.
    let mut acc = ExchangeAccumulator::new(nw);
    loop {
        triggers.push(rx.recv().await);

        // Drain anything already queued.
        while let Some(more) = rx.try_recv() {
            triggers.push(more);
        }

        // The batch's `Drain`s are answered after the tick below.
        for trigger in triggers.drain(..) {
            match trigger {
                TickTrigger::Drain { done } => dones.push(done),
                TickTrigger::Auto => {}
            }
        }

        let _ticking = shared.tick_gate.read().await;
        shared.drain_live_tick_rows_into(&mut tids_scratch);

        // Run the tick. Errors are reported in logs AND handed to every Drain
        // trigger's `done`: the waiting reader's view is stale, so reporting
        // success would serve stale rows under `WireStatus::Ok`.
        let tick_result = run_tick(&shared, &tids_scratch, &mut acc).await;
        if let Err(e) = &tick_result {
            gnitz_warn!("tick error: {}", e);
        }
        for done in dones.drain(..) {
            done.send(tick_result.clone());
        }
    }
}

/// Emit Tick groups for every `tid` and await the per-worker ACKs, relaying each
/// exchange round the tick opens as it completes.
async fn run_tick(shared: &Rc<Shared>, tids: &[i64], acc: &mut ExchangeAccumulator) -> Result<(), WireFault> {
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
        // never see its drain take effect.
        shared.last_tick_lsn.set(snapshot_lsn);
        return Ok(());
    }

    let mut req_ids = shared.disp().reactor().lease_acks(tids.len(), "tick");

    let excl = shared.disp().sal().lock().await;

    // Written by the closure as it goes, so the re-queue and reply-await below
    // are also correct on `guard_panic`'s panic arm, which discards the closure's
    // return value.
    let emitted = Cell::new(0usize);
    let emit = guard_panic("tick", || {
        let disp = shared.disp();
        for (i, &tid) in tids.iter().enumerate() {
            if TICK_EMIT_ERROR.take_once() {
                return Err(format!("injected tick emit error (tid={tid})").into());
            }
            disp.write_tick_group(&excl, tid, GroupTargets::all(req_ids.id(i)))?;
            emitted.set(i + 1);
        }
        Ok(())
    });
    // Whatever was written is published: the drop wakes its workers, and its
    // replies are awaited below rather than left in flight.
    drop(excl);

    let n = emitted.get();
    req_ids.truncate(n);
    // The un-emitted tids never reached a worker, so they still need ticking. An
    // emitted tid is already being ticked by the workers (its group is
    // published), and `handle_tick` has taken its delta, so re-queueing it would
    // only produce a no-op tick that then reports success and masks this failure.
    shared.requeue_tick_tids(&tids[n..]);

    let worker_err = loop {
        match shared.disp().next_relay(&req_ids, acc).await {
            Ok(Some(relay)) => relay_steady(shared, relay).await,
            Ok(None) => break Ok(()),
            Err(e) => break Err(e),
        }
    };
    if let Some(e) = emit.err().or(worker_err.err()) {
        return Err(e);
    }
    shared.last_tick_lsn.set(snapshot_lsn);
    Ok(())
}

// ---------------------------------------------------------------------------
// Steady relay
// ---------------------------------------------------------------------------

/// Write one completed steady-state exchange round back as an ExchangeRelay
/// group. A tick round never pads, so its `flags.backfill` verdict is `Continue`.
///
/// A lost relay wedges every worker in exchange wait, so every failure aborts.
async fn relay_steady(shared: &Shared, relay: PendingRelay) {
    if RELAY_HOLD_FOR_DDL.take_once() {
        hold_relay_for_ddl(shared).await;
    }

    // Phase 1: CPU work — no SAL hold.
    let prep = shared.disp().prepare_relay(relay, "steady relay");

    // Phase 2: fit and emit under one SAL hold.
    let mut reclaimed = false;
    // Spent here, not inside the retry: a genuinely low first iteration must
    // not leave the latch to fire after the reclaim, where `reclaimed` turns
    // it into the fatal "exhausted even after a forced checkpoint".
    let mut inject_low = RELAY_SPACE_LOW.take_once();
    loop {
        {
            let disp = shared.disp();
            let excl = disp.sal().lock().await;
            let mut fit = prep.with_group(BackfillDecision::Continue, |g| disp.sal().fit_relay(g));
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
                    disp.emit_relay(&excl, &prep, BackfillDecision::Continue);
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
        // With the hold dropped: the checkpoint this waits for takes the writer.
        gnitz_warn!("SAL space low before exchange relay; triggering checkpoint");
        // `forced`: this relay's own byte count says it does not fit, which
        // the committer's ambient space test cannot see.
        request_barrier(shared, BarrierKind::Reclaim { forced: true }).await;
        reclaimed = true;
    }
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

async fn handle_message(peer: &Peer, buf: RecvBuf, shared: &Rc<Shared>) {
    let data = buf.as_slice();
    // ONE control-header parse for the whole request: routing, the schema-hint
    // decision and the push decode all read this same parse.
    let ctrl = match gnitz_wire::control::peek_control_block(data) {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("decode error: {e}");
            send_error(peer, 0, msg.as_bytes());
            return;
        }
    };
    let target_id = ctrl.hdr.target_id as i64;

    let verb = match ctrl.client_verb() {
        Ok(v) => v,
        Err(e) => {
            send_error(peer, target_id, e.as_bytes());
            return;
        }
    };

    match verb {
        // The multi-item frames name no single relation in `target_id`; each
        // decodes its items from the frame's body.
        ClientVerb::DdlTxn => handle_ddl_txn(shared, peer, &data[ctrl.body]).await,
        ClientVerb::PushTxn => handle_push_txn(shared, peer, &ctrl, buf).await,
        ClientVerb::ScanMulti => handle_scan_multi(shared, peer, &data[ctrl.body]).await,
        ClientVerb::DeltaPoll => handle_delta_poll(shared, peer, &data[ctrl.body]).await,

        // `target_id` is the sequence key (= the owning table's id).
        ClientVerb::AllocSerialRange => match commit_serial_range_durable(shared, target_id, ctrl.hdr.arg1).await {
            Ok(base) => send_control_only(peer, base, WireStatus::Ok),
            Err(e) => send_error(peer, target_id, e.as_bytes()),
        },

        // An id allocation names no relation, so `target_id` is not read — and a
        // frame that sets one is still allocated, rather than falling through to
        // a scan of that id.
        ClientVerb::AllocIds => reply_allocation(peer, shared.cat_mut().allocate_ids(ctrl.hdr.arg1)).await,

        ClientVerb::ScanSpec => handle_scan_spec(shared, peer, target_id, &ctrl.blob).await,

        // A plain read guard, not `read_lock`: a resolve answers catalog shape,
        // and a view tick moves a view's rows, never its shape — so the tick
        // drain `read_lock` waits for buys nothing here.
        ClientVerb::Resolve => {
            let _g = shared.catalog_rwlock.read().await;
            if let Err(msg) = build_resolve_reply(shared, peer, target_id, &ctrl.blob) {
                send_error(peer, target_id, msg.as_bytes());
            }
        }

        ClientVerb::Push => handle_push(shared, peer, buf, ctrl).await,

        ClientVerb::Scan | ClientVerb::Seek => handle_read(shared, peer, &ctrl, verb).await,
    }
}

/// Reply to an id allocation. The new id rides back as the reply's *target* id —
/// that id is the whole answer, so the frame carries no schema and no data.
async fn reply_allocation(peer: &Peer, alloc: Result<i64, String>) {
    match alloc {
        Ok(new_id) => send_control_only(peer, new_id, WireStatus::Ok),
        Err(e) => {
            let msg = format!("id allocation failed: {e}");
            send_error(peer, 0, msg.as_bytes());
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
    let target_id = ctrl.hdr.target_id as i64;
    let client_version = ctrl.hdr.flags.schema_version;

    // A cold frame ships its own schema block and needs no hint.
    let catalog_schema = if ctrl.data.is_some() && ctrl.schema.is_none() {
        if client_version == 0 {
            return Err(PushReject::Error("a data block without a schema block".to_string()));
        }
        if client_version != shared.cat().schema_version_of(target_id) {
            return Err(PushReject::SchemaMismatch);
        }
        Some(
            shared
                .cat()
                .registry
                .relation(target_id)
                .map(Relation::schema)
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
async fn handle_push(shared: &Rc<Shared>, peer: &Peer, buf: RecvBuf, ctrl: gnitz_wire::control::DecodedControl) {
    let target_id = ctrl.hdr.target_id as i64;
    let flags = ctrl.hdr.flags;
    // A cold frame authored its own schema; `ctrl` moves into the decode below.
    let cold = ctrl.schema.is_some();
    let client_version = flags.schema_version;

    let (decoded, _charge) = buf.decode(|data| decode_push_frame(shared, data, ctrl));
    let decoded = match decoded {
        Ok(d) => d,
        Err(PushReject::SchemaMismatch) => {
            send_control_only(peer, target_id, WireStatus::SchemaMismatch);
            return;
        }
        Err(PushReject::Error(msg)) => {
            send_error(peer, target_id, msg.as_bytes());
            return;
        }
    };
    if PUSH_HOLD_FOR_DDL.take_once() {
        hold_push_for_ddl(shared, target_id).await;
    }
    let catalog = shared.catalog_rwlock.read().await;
    let Some(kind) = target_kind_or_reject(shared, peer, target_id, Access::Write).await else {
        return;
    };

    // The guard drops before the empty-batch ACK: the reply is a socket write,
    // and this lock is writer-preferring.
    let batch = match decoded.data_batch {
        Some(b) if !b.is_empty() => b,
        _ => {
            drop(catalog);
            send_ok_response(shared, peer, target_id, None, 0, client_version);
            return;
        }
    };

    // `batch.schema()` was resolved before this guard; a DDL may have replaced it since.
    if let Err(e) = validate_client_schema(shared, target_id, batch.schema()) {
        drop(catalog);
        // A cold frame authored its schema, so it gets the mismatch in words;
        // a warm one is told to evict its cache entry and retry cold, where
        // that wording is reachable.
        if cold {
            send_error(peer, target_id, e.as_bytes());
        } else {
            send_control_only(peer, target_id, WireStatus::SchemaMismatch);
        }
        return;
    }

    let mode = flags.conflict_mode;

    // Not at the decode boundary: `validate_client_batch` runs there without a
    // relation kind, and must keep admitting a base table's retractions.
    if kind == RelationKind::Stream {
        if let Some(e) = stream_push_error(target_id, &batch, mode) {
            send_error(peer, target_id, e.as_bytes());
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
        let lock_set: Vec<i64> = shared.cat().fk_lock_set(target_id).collect();
        (None, shared.lock_tables_exclusive(lock_set).await)
    };

    // Distributed validation (PK / FK / unique indices). A plain push is a
    // one-family bundle — the same four rules over the same fold — so the batch
    // rides into the family for the validation and back out for the commit
    // request. The validator itself skips a bundle no rule would read a row for.
    let family = TxnFamily { tid: target_id, mode, batch };
    if let Err(e) = shared
        .disp()
        .validate_txn_distributed(std::slice::from_ref(&family))
        .await
    {
        send_fault(peer, target_id, &e);
        return;
    }
    let TxnFamily { batch, .. } = family;

    let is_stream = kind == RelationKind::Stream;
    let committed = shared
        .commit(&catalog, |done| {
            CommitRequest::Push(PendingPush {
                tid: target_id,
                batch,
                recoverable: !is_stream,
                done,
            })
        })
        .await;
    match committed {
        Ok(zone_lsn) => {
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
            // A stream replies `0`: its push is not durable, and an ACK reports an
            // LSN only for a write a restart must recover. Keyed on the target
            // rather than on whether this batch happened to open a zone, so a
            // stream push the committer coalesced with a base-table push still
            // answers `0`.
            let reply_lsn = if is_stream { 0 } else { zone_lsn };
            send_ok_response(shared, peer, target_id, None, reply_lsn, client_version);
        }
        Err(fault) => send_fault(peer, target_id, &fault),
    }
}

/// SCAN or SEEK of one relation: a catalog family is served master-locally,
/// anything else fans out — a seek as a one-key `ScanSpec`.
async fn handle_read(shared: &Rc<Shared>, peer: &Peer, ctrl: &gnitz_wire::control::DecodedControl, verb: ClientVerb) {
    let target_id = ctrl.hdr.target_id as i64;
    let client_version = ctrl.hdr.flags.schema_version;
    let Some((g, kind)) = read_lock(shared, peer, target_id, Access::Read).await else {
        return;
    };
    let disp = shared.disp();
    let seek = match verb {
        ClientVerb::Seek => {
            let schema = disp.schema_desc_for(target_id);
            let opk = match seek_opk_bytes(&schema, &ctrl.blob) {
                Ok(k) => k,
                Err(e) => return send_error(peer, target_id, format!("seek: table {target_id}: {e}").as_bytes()),
            };
            let keys = PkKeys::from_keys(schema.pk_stride(), [opk.pk_bytes()]);
            Some((ReadSpec::all_rows(ReadBound::PkSet(keys)), schema))
        }
        _ => None,
    };

    if kind == RelationKind::SystemCatalog {
        let Some((spec, schema)) = seek else {
            scan_system_family(shared, peer, target_id, client_version).await;
            return;
        };
        match guard_panic("seek", || shared.cat_mut().scan_spec(target_id, spec, &schema)) {
            Ok(rows) => send_ok_response(
                shared,
                peer,
                target_id,
                Some(&rows),
                shared.last_tick_lsn.get(),
                client_version,
            ),
            Err(f) => send_fault(peer, target_id, &f),
        }
        return;
    }

    let (sal_kind, blob) = match seek {
        Some((spec, _)) => {
            let block = shared
                .cat()
                .schema_block(target_id)
                .expect("a read target is registered under the catalog lock");
            (SalMessageKind::ScanSpec, spec.encode(&block))
        }
        None => (SalMessageKind::Scan, Vec::new()),
    };

    let (prelim, server_version) = shared.cat().negotiated_schema_block(target_id, client_version);
    if let Some(block) = prelim {
        send_msg(peer, prelim_schema_msg(target_id, server_version, block.as_slice()));
    }

    let template = ipc::WireMsg {
        target_id: target_id as u64,
        flags: WireFlags {
            schema_version: server_version,
            ..Default::default()
        },
        blob: &blob,
        ..Default::default()
    };
    let group = DirectGroup { template, ..DirectGroup::new(sal_kind) };
    let result = fan_out_scan(shared, peer, g, kind, group).await;
    finish_scan_fanout(peer, target_id, 0, result);
}

/// PUSH_TXN: validate the bundle under the catalog read lock and its tables'
/// lock union — the plain-push arm's order — check OCC, and commit it as one
/// zone. Every rejection is pre-SAL, so a fault or a conflict commits nothing.
async fn handle_push_txn(shared: &Rc<Shared>, peer: &Peer, ctrl: &DecodedControl, buf: RecvBuf) {
    match push_txn_body(shared, ctrl, buf).await {
        // Standard single-frame ACK (uncorrelated, as the DDL_TXN reply is).
        Ok(lsn) => send_msg(
            peer,
            ipc::WireMsg {
                arg0: lsn,
                status: WireStatus::Ok,
                ..Default::default()
            },
        ),
        Err(f) => send_fault(peer, f.target_id, &f.fault),
    }
}

async fn push_txn_body(shared: &Rc<Shared>, ctrl: &DecodedControl, buf: RecvBuf) -> Result<u64, FaultAt> {
    // 1. Decode.
    let (decoded, _charge) = buf.decode(|data| decode_push_txn_frame(&data[ctrl.body.clone()]));
    let DecodedTxn { families, bases } = decoded?;

    // 2. Catalog read lock (excludes a concurrent DROP/DDL), then the
    //    catalog-dependent rules per family.
    let catalog = shared.catalog_rwlock.read().await;
    for fam in &families {
        let tid = fam.tid;
        // The plain-push arm's gate. Refusing a stream keeps every family
        // `recoverable`, so a transaction always opens a zone.
        let kind = target_kind(shared, tid, Access::Write).map_err(|f| FaultAt::relation(tid, f))?;
        if kind == RelationKind::Stream {
            let text = format!("table {tid} is a stream: a stream cannot be written inside a transaction");
            return Err(FaultAt::relation(tid, text.into()));
        }
        validate_client_schema(shared, tid, fam.batch.schema())?;
    }
    let family_tids: Vec<i64> = families.iter().map(|f| f.tid).collect();

    // 3. Acquire the per-table lock union ⋃ fk_lock_set(tid) exclusively.
    let mut union: Vec<i64> = Vec::new();
    for fam in &families {
        union.extend(shared.cat().fk_lock_set(fam.tid));
    }
    let _tlocks = shared.lock_tables_exclusive(union).await;

    // 3b. OCC under `_tlocks`, which guards every family tid (see
    //     `table_commit_lsn`). A blind family's `BLIND` basis no commit exceeds.
    if let Some(&(tid, _)) = bases.iter().find(|&&(tid, basis)| shared.commit_lsn_of(tid) > basis) {
        return Err(FaultAt::relation(
            tid,
            WireFault {
                status: WireStatus::TxnConflict,
                text: String::new(),
            },
        ));
    }

    // 4. Distributed bundle validation (the four rules).
    shared.disp().validate_txn_distributed(&families).await?;

    // 5. Commit.
    let lsn = shared
        .commit(&catalog, |done| CommitRequest::Txn(PendingTxn { families, done }))
        .await?;

    // 6. Bump while `_tlocks` still holds every family tid.
    shared.record_commit_lsn(family_tids.iter().copied(), lsn);
    Ok(lsn)
}

/// A `PUSH_TXN` frame's families, their batches decoded, and each family's
/// `(tid, basis)`.
struct DecodedTxn {
    families: Vec<TxnFamily>,
    bases: Vec<(i64, u64)>,
}

/// Decode a `PUSH_TXN` frame body, applying every rule that needs no catalog.
fn decode_push_txn_frame(body: &[u8]) -> Result<DecodedTxn, WireFault> {
    let raw = gnitz_wire::txn_frame::decode_push_txn(body).map_err(|e| format!("decode error: {e}"))?;
    let mut families: Vec<TxnFamily> = Vec::with_capacity(raw.len());
    let mut bases = Vec::with_capacity(raw.len());
    for fam in &raw {
        let tid = fam.tid as i64;
        let wire_schema = gnitz_store::schema::decode_schema_block(fam.schema_block)
            .map_err(|e| format!("TXN family {tid} schema decode error: {e}"))?;
        let batch =
            decode_client_batch(fam.data, &wire_schema).map_err(|e| format!("TXN family {tid} decode error: {e}"))?;
        if batch.is_empty() {
            return Err(format!("TXN: empty batch for table {tid}").into());
        }
        bases.push((tid, fam.basis));
        families.push(TxnFamily { tid, mode: fam.mode, batch });
    }
    Ok(DecodedTxn { families, bases })
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
        .registry
        .relation(tid)
        .map(Relation::schema)
        .ok_or_else(|| format!("table {tid} has no registered schema"))?;
    validate_schema_match(client, &expected)?;
    Ok(expected)
}

/// Decode and validate a client frame.
fn decode_client_wire(
    data: &[u8],
    ctrl: gnitz_wire::control::DecodedControl,
    hint: Option<&SchemaDescriptor>,
) -> Result<ipc::DecodedWire, String> {
    let decoded = ipc::decode_client_frame(data, ctrl, hint)?;
    if let Some(b) = decoded.data_batch.as_ref() {
        validate_client_batch(b)?;
    }
    Ok(decoded)
}

/// Decode and validate one family block of a client `DDL_TXN` or `PUSH_TXN`.
fn decode_client_batch(slice: &[u8], schema: &SchemaDescriptor) -> Result<Batch, &'static str> {
    let b = Batch::decode_foreign_wal_block(slice, schema, schema)?;
    validate_client_batch(&b)?;
    Ok(b)
}

/// Refuses a null bit on a NOT NULL column, which the null-aware readers and the
/// schema-trusting ones would read differently.
fn validate_client_batch(b: &Batch) -> Result<(), &'static str> {
    if gnitz_wire::first_not_null_violation(b.schema().not_null_payload_slots(), b.null_bmp_data()).is_some() {
        return Err("client batch sets a null bit on a NOT NULL column");
    }
    Ok(())
}

/// Which end of a relation a request wants — the discriminator of [`target_kind`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    /// Any read, a system catalog family included.
    Read,
    /// A read with only a fan-out realization, which a system catalog family has
    /// no form of: every worker holds a full copy, so fanning one out would
    /// concatenate W identical trains and inflate every row's weight W-fold.
    UserRead,
    Write,
}

/// Resolve `target_id`'s kind, rejecting one that cannot serve `access`.
///
/// A stream holds no rows, so it may be written but not read. A view may be read
/// but not written: a push would commit rows its circuit never produced.
///
/// Enforced here even though the SQL binder refuses both: the C and Python bindings
/// reach the engine directly. Every caller addresses a relation by id and owns its
/// own reply path.
///
/// **Only the absent-relation arm carries a status of its own**
/// ([`WireStatus::NotFound`]): the arms below name a relation that exists, which a
/// client must not recover from the way it recovers from a vanished one.
fn target_kind(shared: &Shared, target_id: i64, access: Access) -> Result<RelationKind, WireFault> {
    let Some(kind) = shared.cat().registry.relation(target_id).map(Relation::kind) else {
        return Err(WireFault {
            status: WireStatus::NotFound,
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
async fn target_kind_or_reject(shared: &Shared, peer: &Peer, target_id: i64, access: Access) -> Option<RelationKind> {
    match target_kind(shared, target_id, access) {
        Ok(kind) => Some(kind),
        Err(f) => {
            send_fault(peer, target_id, &f);
            None
        }
    }
}

/// The relation id a RESOLVE names, unvalidated when the client sent an id; `None`
/// when its qualified name names none. `Err` when the name's schema does not exist.
fn resolve_request_target(shared: &Rc<Shared>, target_id: i64, name_blob: &[u8]) -> Result<Option<i64>, String> {
    let candidate = if name_blob.is_empty() {
        target_id
    } else {
        let qname = std::str::from_utf8(name_blob).map_err(|_| "RESOLVE: name is not valid UTF-8".to_string())?;
        match shared.cat().entity_id_by_qname(qname) {
            Some(tid) => tid,
            None => {
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
    Ok(Some(candidate))
}

/// Answer a RESOLVE with the relation's schema block and descriptor.
fn build_resolve_reply(shared: &Rc<Shared>, peer: &Peer, target_id: i64, name_blob: &[u8]) -> Result<(), String> {
    let answer = resolve_request_target(shared, target_id, name_blob)?
        .and_then(|tid| shared.cat().resolve_answer(tid).map(|a| (tid, a)));
    let Some((tid, (desc, schema_block, version))) = answer else {
        // No such relation: a successful reply naming none.
        send_msg(peer, ipc::WireMsg::default());
        return Ok(());
    };
    send_msg(
        peer,
        ipc::WireMsg {
            target_id: tid as u64,
            flags: WireFlags {
                schema_version: version,
                ..Default::default()
            },
            schema_block: Some(schema_block.as_slice()),
            blob: &desc.encode(),
            ..Default::default()
        },
    );
    Ok(())
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
/// Caller holds the catalog read lock.
fn read_is_fresh(shared: &Rc<Shared>, target: i64) -> bool {
    if !shared
        .cat()
        .registry
        .relation(target)
        .map(Relation::kind)
        .is_some_and(|k| k.is_view())
    {
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

/// Drop the caller's read guard, tick everything pending, and hand back a fresh
/// guard, so a DDL queued on the lock does not wait out the drain.
///
/// The trigger goes out even when nothing looks pending — the tick loop is serial,
/// so awaiting `done` also serializes behind a concurrent `Auto`, without which a
/// read could observe a view mid-tick. A failed tick is reported, not swallowed:
/// its views are stale, and serving them under `WireStatus::Ok` is a silent stale read.
async fn drain_and_relock(shared: &Rc<Shared>, guard: ReadGuard) -> Result<ReadGuard, WireFault> {
    drop(guard);
    request_drain(shared).await?;
    Ok(shared.catalog_rwlock.read().await)
}

/// Take the catalog read lock and resolve `target_id`'s kind from the same probe
/// that validated it, draining first when the target is a stale view. `None`
/// means the target was rejected and the error is already sent.
///
/// The one read-lock entry point for every single-target read verb: each passes
/// the `Access` its realization can serve and routes on the returned kind, rather
/// than re-deciding the system/user split from the id. A stale view re-resolves
/// after the drain, since a DDL may have dropped it meanwhile.
async fn read_lock(
    shared: &Rc<Shared>,
    peer: &Peer,
    target_id: i64,
    access: Access,
) -> Option<(ReadGuard, RelationKind)> {
    let g = shared.catalog_rwlock.read().await;
    let kind = target_kind_or_reject(shared, peer, target_id, access).await?;
    if read_is_fresh(shared, target_id) {
        return Some((g, kind));
    }
    // No preliminary frame has gone out yet (the negotiated schema block is
    // emitted later), so a failed drain is still reportable as a plain error.
    let g = match drain_and_relock(shared, g).await {
        Ok(g) => g,
        Err(f) => {
            send_fault(peer, target_id, &f);
            return None;
        }
    };
    let kind = target_kind_or_reject(shared, peer, target_id, access).await?;
    Some((g, kind))
}

/// The preliminary schema-only frame — carrying `continuation`, the
/// `server_version`, and the captured wire block — that precedes a scan's data
/// frames on a schema-cache miss.
fn prelim_schema_msg(tid: i64, server_version: u16, block: &[u8]) -> ipc::WireMsg<'_> {
    ipc::WireMsg {
        target_id: tid as u64,
        flags: WireFlags::train_frame(server_version, false),
        schema_block: Some(block),
        ..Default::default()
    }
}

/// A reply train's terminal frame. `arg0` is the read's watermark (see
/// [`read_watermark`]), or a DELTA_POLL position's tick round, whose cursor tag
/// rides `arg1`; `arg1` is `0` for every other read.
fn terminal_scan_msg(target_id: i64, arg0: u64, arg1: u64) -> ipc::WireMsg<'static> {
    ipc::WireMsg {
        target_id: target_id as u64,
        arg0,
        arg1,
        ..Default::default()
    }
}

/// The LSN at or below which every commit is reflected in a read of a `kind`
/// relation. Called under the read's own SAL hold: every durable allocator
/// reserves and lays out its zone under that hold (DDL under the catalog write
/// lock, which the read's guard excludes), so every zone `<= reserved()` is in
/// the SAL ahead of the read or was rolled back. A view reflects its last tick.
fn read_watermark(shared: &Shared, kind: RelationKind) -> u64 {
    match kind {
        RelationKind::BaseTable => shared.lsn_alloc.reserved(),
        _ => shared.last_tick_lsn.get(),
    }
}

/// One scan-shaped fan-out of a `kind` relation: the cut under `guard`, then the
/// reply train without it — a slow client's egress must not hold the catalog
/// read lock. Answers the read's watermark, sampled inside the cut.
async fn fan_out_scan(
    shared: &Rc<Shared>,
    peer: &Peer,
    guard: ReadGuard,
    kind: RelationKind,
    group: DirectGroup<'_>,
) -> Result<u64, WireFault> {
    let mut lsn = 0;
    let mut leases = shared
        .disp()
        .scan_cut(1, |cut| {
            lsn = read_watermark(shared, kind);
            cut.read(group)
        })
        .await?;
    drop(guard);
    forward_scan(peer, &leases.pop().expect("one read, one lease")).await?;
    Ok(lsn)
}

/// Finish one scan-shaped fan-out: the terminal frame carrying the `Ok`'s `arg0`
/// (the read's watermark, or a delta read's round) and `arg1`, or the fault.
fn finish_scan_fanout(peer: &Peer, target_id: i64, arg1: u64, result: Result<u64, WireFault>) {
    match result {
        // Corked, not sent: the terminal joins whatever the forward corked.
        Ok(arg0) => send_msg(peer, terminal_scan_msg(target_id, arg0, arg1)),
        Err(f) => send_fault(peer, target_id, &f),
    }
}

/// SCAN_SPEC: the scan pipeline under a client-authored reply schema, so no schema
/// block goes back, routed by the request's bound.
async fn handle_scan_spec(shared: &Rc<Shared>, peer: &Peer, target_id: i64, blob: &[u8]) {
    // `UserRead`: a `ReadSpec` has only a fan-out realization, which a catalog
    // family has no form of.
    let Some((g, kind)) = read_lock(shared, peer, target_id, Access::UserRead).await else {
        return;
    };
    let template = ipc::WireMsg {
        target_id: target_id as u64,
        blob,
        ..Default::default()
    };
    let group = DirectGroup {
        template,
        ..DirectGroup::new(SalMessageKind::ScanSpec)
    };
    let result = fan_out_scan(shared, peer, g, kind, group).await;
    finish_scan_fanout(peer, target_id, 0, result);
}

/// DELTA_POLL: advance N mirrored views in one request, one catalog lock and —
/// for however many of them moved — one broadcast.
///
/// A fault at `target_id = 0` rejects the frame; a fault naming a view ends that
/// view's position and no other.
///
/// Never drains pending ticks: a delta read reports what has happened, and a
/// round not yet ticked is one the next poll carries.
async fn handle_delta_poll(shared: &Rc<Shared>, peer: &Peer, body: &[u8]) {
    // A frame-shape rejection, before any group was written.
    if let Err(f) = delta_poll_body(shared, peer, body).await {
        send_fault(peer, 0, &f);
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

/// Body of [`handle_delta_poll`]. A per-view failure is not an `Err` — it goes out
/// as that view's own fault frame and the rest of the poll continues.
async fn delta_poll_body(shared: &Rc<Shared>, peer: &Peer, body: &[u8]) -> Result<(), WireFault> {
    let views = gnitz_wire::txn_frame::decode_delta_poll(body).map_err(|e| format!("decode error: {e}"))?;

    // ── Phase 1: classify under the catalog lock, dispatch one cut ─────────
    // No await between a view's gate test and the round its terminal reports, so
    // no tick can land in between. The guard is scoped to this phase: phase 2
    // reads no catalog state, and holding it across the drain would block DDL.
    // A poll with nothing to fetch takes no SAL hold.
    let (positions, dispatches, up_to_date_round, dispatch_round) = {
        let _g = shared.catalog_rwlock.read().await;
        let disp = shared.disp();
        let up_to_date_round = disp.last_tick_round();
        let mut positions = Vec::with_capacity(views.len());
        let mut moved: Vec<DeltaPollItem> = Vec::with_capacity(views.len());
        for &item in &views {
            let tid = item.view_id as i64;
            let position = match target_kind(shared, tid, Access::UserRead) {
                Err(f) => PollPosition::Fault(f),
                Ok(_) if delta_up_to_date(shared, tid, item.after_tick) => PollPosition::UpToDate,
                Ok(_) => {
                    moved.push(item);
                    PollPosition::Moved
                }
            };
            positions.push((tid, position));
        }

        let mut round = 0;
        let dispatches = if moved.is_empty() {
            Vec::new()
        } else {
            disp.scan_cut(moved.len(), |cut| {
                round = disp.last_tick_round();
                for item in &moved {
                    cut.read(DirectGroup {
                        template: ipc::WireMsg {
                            target_id: item.view_id,
                            arg0: round,
                            arg1: item.after_tick,
                            blob: item.reply_block,
                            ..Default::default()
                        },
                        ..DirectGroup::new(SalMessageKind::DeltaRead)
                    })?;
                }
                Ok(())
            })
            .await?
        };
        (positions, dispatches, up_to_date_round, round)
    };

    // ── Phase 2: one terminal per view, in request order ───────────────────
    let disp = shared.disp();
    let mut dispatches = dispatches.into_iter();
    for (tid, position) in positions {
        let (round, result) = match position {
            PollPosition::Fault(fault) => (0, Err(fault)),
            PollPosition::UpToDate => (up_to_date_round, Ok(())),
            // Taken in step with the `Moved`s that were pushed. A dispatch left
            // undrained — an earlier return dropped it — discards the rest of
            // its train at the ring boundary.
            PollPosition::Moved => {
                let lease = dispatches.next().expect("one dispatch per moved view");
                (dispatch_round, forward_scan(peer, &lease).await)
            }
        };
        finish_scan_fanout(peer, tid, disp.delta_cursor_tag(tid), result.map(|()| round));
        // Carry no more than the budget into the next view, and learn here
        // rather than at the end if the client is gone.
        if peer.flush_if_full().await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// Whether a delta read after `after_tick` already sits at the view's last round,
/// so it can be answered without reaching a worker — the steady state of a
/// subscription, where a fan-out per poll would cost W wakeups.
///
/// `false` at `after_tick = 0` (the bootstrap bound) and for a relation with no
/// feed: both must reach the store, the second to be refused there.
fn delta_up_to_date(shared: &Shared, target_id: i64, after_tick: u64) -> bool {
    after_tick > 0
        && shared
            .cat()
            .registry
            .relation(target_id)
            .is_some_and(Relation::has_delta_feed)
        && after_tick >= shared.disp().last_delta_round(target_id)
}

/// One relation's Phase-1 capture for `scan_multi_body`: exactly what
/// [`CatalogEngine::negotiated_schema_block`] answered, carried to the deferred Phase-2 emit.
struct ScanMultiRelPlan {
    tid: i64,
    /// Stamped into the preliminary frame and onto the read, whose frames echo it.
    server_version: u16,
    kind: RelationKind,
    /// The read's watermark, sampled inside the cut.
    lsn: u64,
    /// The wire schema block to emit before this relation's train, present iff
    /// the client's cached version missed.
    block: Option<Rc<Vec<u8>>>,
}

/// SCAN_MULTI: snapshot N relations at one SAL cut and stream N reply trains in
/// request order. The read-side completion of the atomic multi-table write
/// story: an atomic commit is either wholly before the cut (visible in every
/// train) or wholly after (visible in none), never torn across the result set.
async fn handle_scan_multi(shared: &Rc<Shared>, peer: &Peer, body: &[u8]) {
    if let Err(f) = scan_multi_body(shared, peer, body).await {
        send_fault(peer, f.target_id, &f.fault);
    }
}

/// Body of `handle_scan_multi`. Every scan's lease lives in the `dispatches` vec
/// and drops on return, so an early return removes every route and discards
/// undrained frames at the ring boundary.
async fn scan_multi_body(shared: &Rc<Shared>, peer: &Peer, body: &[u8]) -> Result<(), FaultAt> {
    // ── Phase 0: decode; tid legality is Phase 1's, under the catalog lock ──
    let relations = gnitz_wire::txn_frame::decode_scan_multi(body).map_err(|e| format!("decode error: {e}"))?;

    // Drain once if any target is a stale view — the same test `read_lock` runs
    // for a single target. Phase 1 resolves every tid's kind under the guard
    // handed back, so a DDL during the drain is caught there and an unknown tid
    // is rejected there rather than here.
    let mut cat = shared.catalog_rwlock.read().await;
    if relations.iter().any(|&(tid, _)| !read_is_fresh(shared, tid as i64)) {
        cat = drain_and_relock(shared, cat).await?;
    }

    // ── Phase 1: catalog lock — resolve shapes + schemas, dispatch one cut ──
    // One catalog snapshot for every relation, one SAL cut for every group.
    let (dispatches, plans) = {
        let _cat = cat;
        let mut plans: Vec<ScanMultiRelPlan> = Vec::with_capacity(relations.len());
        for &(tid_u, client_ver) in &relations {
            let tid = tid_u as i64;
            // Base tables AND views are legal; `UserRead` refuses a catalog
            // family, which stays on the plain path that serves it
            // master-locally.
            let kind = target_kind(shared, tid, Access::UserRead).map_err(|f| FaultAt::relation(tid, f))?;
            // Capture (not emit) each relation's preliminary schema frame here so
            // Phase 2 can send it after the one-cut dispatch, in request order.
            let (block, server_version) = shared.cat().negotiated_schema_block(tid, client_ver);
            plans.push(ScanMultiRelPlan { tid, server_version, kind, lsn: 0, block });
        }
        let disp = shared.disp();
        let dispatches = disp
            .scan_cut(plans.len(), |cut| {
                for plan in &mut plans {
                    plan.lsn = read_watermark(shared, plan.kind);
                    cut.read(DirectGroup {
                        template: ipc::WireMsg {
                            target_id: plan.tid as u64,
                            flags: WireFlags {
                                schema_version: plan.server_version,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        ..DirectGroup::new(SalMessageKind::Scan)
                    })?;
                }
                Ok(())
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
            send_msg(peer, prelim_schema_msg(plan.tid, plan.server_version, block.as_slice()));
        }
        // Drain this relation's train (all workers, ascending) before the next —
        // the FIFO reply contract makes request order == ring order.
        forward_scan(peer, d).await?;
        // Terminal frame for this relation (tid + its watermark).
        send_msg(peer, terminal_scan_msg(plan.tid, plan.lsn, 0));
        // This relation's reply is complete: carry no more than the budget into
        // the next, and learn here rather than at the end if the client is gone.
        if peer.flush_if_full().await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// The master-local half of [`handle_read`]: a SCAN of a catalog family. The rows
/// come from the catalog, never from the frame, so the frame is not a parameter.
///
/// Takes NO lock: the caller holds the catalog read guard, and the lock is
/// writer-preferring, so a nested read parks forever the moment a DDL writer
/// queues.
async fn scan_system_family(shared: &Rc<Shared>, peer: &Peer, target_id: i64, client_version: u16) {
    match guard_panic("scan", || shared.cat_mut().scan(target_id)) {
        Ok(b) => {
            let batch_ref = if !b.is_empty() { Some(b) } else { None };
            send_ok_response(
                shared,
                peer,
                target_id,
                batch_ref.as_deref(),
                shared.last_tick_lsn.get(),
                client_version,
            );
        }
        Err(e) => send_error(peer, target_id, e.as_bytes()),
    }
}

// ---------------------------------------------------------------------------
// Wire-protocol response helpers
// ---------------------------------------------------------------------------

/// Append `msg`'s framed bytes to `out`, so a corked reply never passes through a
/// buffer of its own.
fn encode_response_into(out: &mut Vec<u8>, msg: ipc::WireMsg<'_>) {
    const PFX: usize = gnitz_wire::FRAME_LEN_PREFIX_BYTES;
    let sz = msg.size();
    let base = out.len();
    let total = base + PFX + sz;
    out.reserve(PFX + sz);
    // SAFETY: `encode` writes every byte of the payload — a block's header,
    // directory and regions pack end to end — and the length prefix is written
    // immediately below.
    #[allow(clippy::uninit_vec)]
    unsafe {
        out.set_len(total);
    }
    gnitz_wire::write_u32_le(out, base, sz as u32);
    msg.encode(&mut out[base + PFX..total]);
}

/// Cork `msg` for the client — and the one place a master-authored reply meets
/// [`ipc::FRAME_CAP`]: the master-local system-family scan and seek have no other
/// bound. The one reply not framed here is the fixed-size HELLO ACK.
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
            status: WireStatus::Error,
            blob: text.as_bytes(),
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
    arg0: u64,
    client_version: u16,
) {
    let (schema_block, server_version) = shared.cat().negotiated_schema_block(target_id, client_version);
    let schema_arg = schema_block.as_ref().map(|b| b.as_slice());
    send_msg(
        peer,
        ipc::WireMsg {
            target_id: target_id as u64,
            flags: WireFlags {
                schema_version: server_version,
                ..Default::default()
            },
            arg0,
            data: result.map_or(ipc::WireData::None, ipc::WireData::Whole),
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
/// carries a meaning in `arg0` this wrapper has no parameter for.
fn send_control_only(peer: &Peer, target_id: i64, status: WireStatus) {
    send_status_frame(peer, target_id, status, &[])
}

/// One control-only reply frame carrying `status` verbatim. No schema block: the
/// client ignores both it and the schema version on a failure, so `flags` stays at
/// its default and the cache lookup is skipped.
fn send_status_frame(peer: &Peer, target_id: i64, status: WireStatus, text: &[u8]) {
    send_msg(
        peer,
        ipc::WireMsg {
            target_id: target_id as u64,
            status,
            blob: text,
            ..Default::default()
        },
    )
}

/// A fault and the relation it concerns; `0` concerns none in particular.
struct FaultAt {
    target_id: i64,
    fault: WireFault,
}

impl FaultAt {
    fn relation(target_id: i64, fault: WireFault) -> Self {
        FaultAt { target_id, fault }
    }
}

impl<F: Into<WireFault>> From<F> for FaultAt {
    fn from(fault: F) -> Self {
        FaultAt::relation(0, fault.into())
    }
}

/// A failure carrying its own status, master-minted or forwarded from a worker.
fn send_fault(peer: &Peer, target_id: i64, fault: &WireFault) {
    send_status_frame(peer, target_id, fault.status, fault.text.as_bytes())
}

/// A rejection that carries no status of its own, and so is `WireStatus::Error`.
fn send_error(peer: &Peer, target_id: i64, text: &[u8]) {
    send_status_frame(peer, target_id, WireStatus::Error, text)
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
