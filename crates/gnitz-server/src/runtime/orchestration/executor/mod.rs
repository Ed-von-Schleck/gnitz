//! Server executor: the process lifecycle, the `Shared` state, the request
//! router and every read/push handler, and the reply-frame vocabulary. The
//! catalog-zone write path is the child `ddl`.
//!
//! The master owns one `Reactor` driving the accept sockets, a task per
//! connection, the committer (group commit + checkpoint + fsync), the tick task
//! and the worker-crash watchdog.
//!
//! A request handler rejects by returning `Err`, and the router sends it: no
//! handler writes a fault frame for its request as a whole.

mod ddl;

use gnitz_expr::SchemaFacts;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use super::guard_panic;
use gnitz_foundation::fault::Seam;

use self::ddl::{commit_serial_range_durable, handle_ddl_txn, hold_tick_for_ddl, TICK_HOLD_FOR_DDL};
use super::TxnFamily;
use crate::catalog::CatalogEngine;
use crate::runtime::committer::{self, BarrierKind, CommitRequest, PendingPush, PendingTxn};
use crate::runtime::listen::ClientListener;
use crate::runtime::lsn::ZoneLsnAllocator;
use crate::runtime::master::{forward_scan, MasterDispatcher, WORKER_WATCH};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{chan, oneshot, select2, AsyncRwLock, Either, ReadGuard, RecvBuf, WriteGuard};
use crate::runtime::sal::{DirectGroup, GroupTargets, SalMessageKind};
use crate::runtime::wire as ipc;
use gnitz_store::relation::{Relation, RelationKind};
use gnitz_store::schema::key::seek_opk_bytes;
use gnitz_store::storage::Batch;
use gnitz_wire::control::DecodedControl;
use gnitz_wire::txn_frame::DeltaPollItem;
use gnitz_wire::{PkKeys, ReadBound, ReadSpec, WireFault, WireFlags, WireStatus};

const TICK_COALESCE_ROWS: usize = 10_000;

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

/// Hold one decoded push, before its catalog read lock, until a DDL has replaced
/// the schema record `seen` its decode matched — the window a queued `ALTER TABLE`
/// writer opens between that match and the gate.
async fn hold_push_for_ddl(shared: &Shared, target_id: u64, seen: &Rc<[u8]>) {
    park_until(shared, PUSH_HOLD_MAX_POLLS, "push hold", || {
        shared
            .cat()
            .schema_record(target_id)
            .is_none_or(|now| !Rc::ptr_eq(seen, &now.bytes))
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
    tick_rows: RefCell<FxHashMap<u64, usize>>,
    /// Per-table write serialization. A push whose validation reads committed
    /// state (`push_reads_committed_state`) and every transaction take the write
    /// guard; a push that reads no committed state takes the read guard, so
    /// same-table pushes reach the committer concurrently and share one fsync.
    table_locks: RefCell<FxHashMap<u64, AsyncRwLock>>,
    /// Set true by the graceful-shutdown watcher before it sends the final
    /// Shutdown barrier. Read only by [`Shared::commit`], which is what
    /// makes the test and the send one step.
    draining: Cell<bool>,
    /// OCC per-table commit-LSN map: `tid → the zone LSN its last accepted write
    /// this boot rode`. That LSN is published for a durable write; for a stream it is
    /// a reservation the batch never published, which is what makes
    /// `read_is_fresh` answer false and drain. A missing entry reads as `boot_seed`.
    /// Single-threaded reactor — a plain `RefCell`, and no borrow is ever held across an
    /// `.await`.
    table_commit_lsn: RefCell<FxHashMap<u64, u64>>,
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
    /// How long a connection may take from accept to its HELLO, handshake
    /// included (`GNITZ_HELLO_TIMEOUT_MS`). The default outlasts the client's own
    /// `CONNECT_TIMEOUT`, so only a client that has already given up is reaped.
    hello_timeout: Duration,
}

/// The table locks one task holds.
struct HeldTables {
    tids: Vec<u64>,
    guards: TableGuards,
}

enum TableGuards {
    Shared { _guard: ReadGuard },
    Exclusive { _guards: Vec<WriteGuard> },
}

impl HeldTables {
    fn holds(&self, tid: u64) -> bool {
        self.tids.contains(&tid)
    }

    fn holds_exclusive(&self, tid: u64) -> bool {
        matches!(self.guards, TableGuards::Exclusive { .. }) && self.holds(tid)
    }
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

    fn table_lock(&self, tid: u64) -> AsyncRwLock {
        self.table_locks.borrow_mut().entry(tid).or_default().clone()
    }

    /// The read guard on `tid`.
    async fn lock_table_shared(&self, tid: u64) -> HeldTables {
        let guard = self.table_lock(tid).read().await;
        HeldTables {
            tids: vec![tid],
            guards: TableGuards::Shared { _guard: guard },
        }
    }

    /// The write guard on every table in `tids`, taken in ascending order so a
    /// child INSERT and a parent DELETE cannot deadlock on the same set, and each
    /// once, since re-guarding a lock this task holds would hang forever.
    ///
    /// Owned, because the set is read out of the catalog and this loop awaits.
    async fn lock_tables_exclusive(&self, mut tids: Vec<u64>) -> HeldTables {
        tids.sort_unstable();
        tids.dedup();
        let mut guards = Vec::with_capacity(tids.len());
        for &tid in &tids {
            guards.push(self.table_lock(tid).write().await);
        }
        HeldTables {
            tids,
            guards: TableGuards::Exclusive { _guards: guards },
        }
    }

    /// Commit `req` unless a graceful shutdown has begun, await its verdict under
    /// the caller's catalog read guard, and on success raise each of `tids`' commit
    /// LSN to it. No await separates the test from the send: a request that saw a
    /// live server is queued ahead of the watchdog's Shutdown barrier.
    async fn commit(
        &self,
        _catalog: &ReadGuard,
        held: &HeldTables,
        tids: impl IntoIterator<Item = u64>,
        req: impl FnOnce(oneshot::Sender<Result<u64, WireFault>>) -> CommitRequest,
    ) -> Result<u64, WireFault> {
        let tids: Vec<u64> = tids.into_iter().collect();
        assert!(
            tids.iter().all(|&t| held.holds(t)),
            "a commit bumps only tables whose lock it holds"
        );
        if self.draining.get() {
            return Err("server shutting down".to_string().into());
        }
        let (done, rx) = oneshot::channel();
        self.committer_tx.send(req(done));
        let lsn = rx.await?;
        let mut map = self.table_commit_lsn.borrow_mut();
        for tid in tids {
            // Pushes sharing a read guard resume from the await in any order.
            let e = map.entry(tid).or_default();
            *e = (*e).max(lsn);
        }
        Ok(lsn)
    }

    /// OCC: whether `tid` committed a write after `basis`. `held`'s write guard on
    /// `tid` keeps any other commit to it from landing before the caller's.
    fn written_since(&self, held: &HeldTables, tid: u64, basis: u64) -> bool {
        assert!(held.holds_exclusive(tid), "an OCC check holds its table's write guard");
        self.commit_lsn_of(tid) > basis
    }

    /// The zone LSN of `tid`'s last committed write this boot, or `boot_seed` for
    /// a table not written this boot. The miss default is sound for OCC because
    /// every live basis is ≥ `boot_seed` (the argument is on that field), and for
    /// read freshness because `boot_seed` is the same `initial_lsn` `last_tick_lsn`
    /// is seeded to, so an unwritten table compares as absorbed — which it is,
    /// boot finishing its recovery tick sweep first.
    fn commit_lsn_of(&self, tid: u64) -> u64 {
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
    fn forget_relation(&self, _catalog_write: &WriteGuard, id: u64) {
        self.table_locks.borrow_mut().remove(&id);
        self.table_commit_lsn.borrow_mut().remove(&id);
        self.disp().forget_delta_round(id);
        self.disp().unique_filter_invalidate_table(id);
    }

    /// Credit `rows` against each tid's pending-tick count and fire the auto-tick
    /// if any tid now stands at or above the coalesce threshold. Below it a push
    /// only accumulates, and nothing ticks until a read asks for a drain.
    pub(super) fn note_commit_rows(&self, rows: impl Iterator<Item = (u64, usize)>) {
        let crossed = {
            let mut pending = self.tick_rows.borrow_mut();
            let dag = &self.cat().dag;
            for (tid, n) in rows.filter(|&(tid, _)| dag.is_scanned(tid)) {
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
    fn drain_live_tick_rows_into(&self, out: &mut Vec<u64>) {
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
    fn requeue_tick_tids(&self, tids: &[u64]) {
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
    pub fn run(dispatcher: Rc<MasterDispatcher>, data_dir: &str, listeners: Vec<ClientListener>, lsn_seed: u64) -> i32 {
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
            hello_timeout: Duration::from_millis(gnitz_foundation::env::env_num(
                "GNITZ_HELLO_TIMEOUT_MS",
                (gnitz_wire::CONNECT_TIMEOUT * 3 / 2).as_millis() as u64,
            )),
        });

        // Catch SIGTERM/SIGINT so the watchdog can drive a final checkpoint
        // before exiting.
        install_shutdown_signal_handlers();

        reactor.spawn(committer::run(committer_rx, Rc::clone(&shared)));
        for listener in listeners {
            reactor.spawn(accept_loop(Rc::clone(&shared), listener));
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

async fn accept_loop(shared: Rc<Shared>, listener: ClientListener) {
    let ClientListener { fd, tls } = listener;
    let reactor = Rc::clone(shared.disp().reactor());
    let mut accepted = reactor.attach_listener(fd);
    loop {
        let Some(conn) = reactor.client_conn(accepted.recv().await) else {
            continue;
        };
        let peer = Peer::new(&reactor, conn, tls.as_ref());
        reactor.spawn(connection_loop(peer, Rc::clone(&shared)));
    }
}

async fn connection_loop(peer: Peer, shared: Rc<Shared>) {
    serve_connection(&peer, &shared).await;
    // The one exit: ship what is corked, a refusal included, then retire the fd.
    let _ = peer.flush_egress().await;
    peer.close();
}

/// One message handled to completion before the next is received, so replies
/// leave in request order — which is how clients correlate them (`gnitz.aio`
/// gathers a mixed group onto one round-trip and rejects an out-of-order
/// `target_id`). Spawning `handle_message` to overlap requests would break that.
///
/// Returns when the peer is gone or refused; the caller closes.
async fn serve_connection(peer: &Peer, shared: &Rc<Shared>) {
    let deadline = shared.disp().reactor().timer(Instant::now() + shared.hello_timeout);
    let hello = match select2(peer.next_request(), deadline).await {
        Either::A(hello) => hello,
        Either::B(()) => None,
    };
    let Some(hello) = hello else { return };
    peer.cork_with(|out| {
        out.extend_from_slice(&gnitz_wire::frame_len_prefix(gnitz_wire::HELLO.len()));
        out.extend_from_slice(&gnitz_wire::HELLO);
    });
    if gnitz_wire::check_hello(hello.as_slice()).is_err() {
        return;
    }

    while let Some(buf) = peer.next_request().await {
        handle_message(peer, buf, shared).await;
    }
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
/// flips `SHUTDOWN_REQUESTED`; the watchdog's timer picks it up.
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
/// exits cleanly.
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
            //    rounds complete. A just-pushed delta may still sit unticked
            //    (below the row threshold, so no `Auto` fired), so the
            //    sequence's drain is what gets it into the views.
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

        // A workload with no writes sends the committer nothing, yet its reads
        // still write SAL groups.
        if shared.disp().sal().needs_checkpoint() {
            shared.committer_tx.send(CommitRequest::Reclaim);
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
    let mut triggers: Vec<TickTrigger> = Vec::new();
    // The batch's `Drain` repliers, held across the tick they are waiting on.
    let mut dones: Vec<oneshot::Sender<Result<(), WireFault>>> = Vec::new();
    // Reused across every tick; `drain_live_tick_rows_into` clears it before
    // refilling so capacity is retained.
    let mut tids_scratch: Vec<u64> = Vec::new();
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
        let tick_result = run_tick(&shared, &tids_scratch).await;
        if let Err(e) = &tick_result {
            gnitz_warn!("tick error: {}", e);
        }
        for done in dones.drain(..) {
            done.send(tick_result.clone());
        }
    }
}

/// Emit one Tick group for every `tid` and await the per-worker ACKs.
async fn run_tick(shared: &Rc<Shared>, tids: &[u64]) -> Result<(), WireFault> {
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

    let lease = shared.disp().reactor().lease_acks("tick");
    let emit = {
        let excl = shared.disp().sal().lock().await;
        guard_panic("tick", || {
            if TICK_EMIT_ERROR.take_once() {
                return Err("injected tick emit error".into());
            }
            shared
                .disp()
                .write_tick_group(&excl, tids, GroupTargets::all(lease.id()))
        })
    };
    // A refused write publishes nothing, so no worker took any tid's delta.
    if let Err(e) = emit {
        shared.requeue_tick_tids(tids);
        return Err(e);
    }

    if TICK_HOLD_FOR_DDL.take_once() {
        hold_tick_for_ddl(shared).await;
    }
    shared.disp().collect_round(&lease).await?;
    shared.last_tick_lsn.set(snapshot_lsn);
    Ok(())
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

async fn handle_message(peer: &Peer, buf: RecvBuf, shared: &Rc<Shared>) {
    // ONE control-header parse for the whole request: routing, the schema-hint
    // decision and the push decode all read this same parse.
    let ctrl = match gnitz_wire::control::peek_control_block(buf.as_slice()) {
        Ok(c) => c,
        Err(e) => {
            send_fault(peer, 0, &format!("decode error: {e}").into());
            return;
        }
    };
    let target_id = ctrl.hdr.target_id;
    if let Err(f) = dispatch_request(peer, buf, ctrl, shared).await {
        send_fault(peer, target_id, &f);
    }
}

/// Run the verb `ctrl` names. [`handle_message`] answers an `Err` with one fault
/// frame, which fails the whole request at the client; a fault confined to one
/// position of a streamed reply is the handler's own to send.
async fn dispatch_request(
    peer: &Peer,
    buf: RecvBuf,
    ctrl: gnitz_wire::control::DecodedControl,
    shared: &Rc<Shared>,
) -> Result<(), WireFault> {
    let data = buf.as_slice();
    let target_id = ctrl.hdr.target_id;
    match ctrl.client_verb()? {
        // The multi-item frames name no single relation in `target_id`; each
        // decodes its items from the frame's body.
        ClientVerb::DdlTxn => handle_ddl_txn(shared, peer, &data[ctrl.body]).await,
        ClientVerb::PushTxn => handle_push_txn(shared, peer, &ctrl, buf).await,
        ClientVerb::ScanMulti => handle_scan_multi(shared, peer, &data[ctrl.body]).await,
        ClientVerb::DeltaPoll => handle_delta_poll(shared, peer, &data[ctrl.body]).await,

        // `target_id` is the sequence key (= the owning table's id).
        ClientVerb::AllocSerialRange => {
            send_id(
                peer,
                commit_serial_range_durable(shared, target_id, ctrl.hdr.arg1).await? as u64,
            );
            Ok(())
        }

        // An id allocation names no relation, so `target_id` is not read — and a
        // frame that sets one is still allocated, rather than falling through to
        // a scan of that id.
        ClientVerb::AllocIds => reply_allocation(peer, shared.cat_mut().allocate_ids(ctrl.hdr.arg1)),

        ClientVerb::ScanSpec => {
            handle_scan_spec(shared, peer, target_id, &data[ctrl.blob.clone()], ctrl.hdr.arg0).await
        }

        // A plain read guard, not `read_lock`: a resolve answers catalog shape,
        // and a view tick moves a view's rows, never its shape — so the tick
        // drain `read_lock` waits for buys nothing here.
        ClientVerb::Resolve => {
            let _g = shared.catalog_rwlock.read().await;
            build_resolve_reply(shared, peer, target_id, &data[ctrl.blob.clone()])
        }

        ClientVerb::Push => handle_push(shared, peer, buf, ctrl).await,

        verb @ (ClientVerb::Scan | ClientVerb::Seek) => {
            handle_read(shared, peer, &ctrl, &data[ctrl.blob.clone()], verb).await
        }
    }
}

/// Reply to an id allocation. The new id rides back as the reply's *target* id —
/// that id is the whole answer, so the frame carries no schema and no data.
fn reply_allocation(peer: &Peer, alloc: Result<u64, String>) -> Result<(), WireFault> {
    send_id(peer, alloc.map_err(|e| format!("id allocation failed: {e}"))?);
    Ok(())
}

/// Decode a push frame against the catalog: its record must lay out its target's
/// columns, and the target's record is answered for [`CatalogEngine::recheck_record`].
/// Runs before the catalog read guard, and off it: `AsyncRwLock` is
/// writer-preferring, so a bulk load's multi-megabyte decode under the guard
/// would stall every DDL writer behind it.
fn decode_push_frame(
    cat: &CatalogEngine,
    data: &[u8],
    ctrl: DecodedControl,
) -> Result<(ipc::DecodedWire, Rc<[u8]>), WireFault> {
    let tid = ctrl.hdr.target_id;
    let target = cat.schema_record(tid).ok_or_else(|| not_found(tid))?;
    let wire = ipc::decode_client_frame(data, ctrl, None, |record| target.decode_of(record).map(Some))
        .map_err(|e| format!("decode error: {e}"))?;
    Ok((wire, target.bytes))
}

/// Handle a client push: decode the frame, then commit it under the catalog read
/// lock and the target's table lock(s).
///
/// One handler for both batch shapes. An empty batch is a legitimate empty Z-Set
/// delta, so it is ACKed with the "nothing written" LSN `0` — but only after the
/// same existence + writability gate a non-empty push passes, so a client bug
/// that happens to produce an empty batch (a `delete` with an empty pk list)
/// fails the way a non-empty one would instead of being masked by a no-op ACK.
async fn handle_push(
    shared: &Rc<Shared>,
    peer: &Peer,
    buf: RecvBuf,
    ctrl: gnitz_wire::control::DecodedControl,
) -> Result<(), WireFault> {
    let target_id = ctrl.hdr.target_id;
    let flags = ctrl.hdr.flags;

    let (decoded, _charge) = buf.decode(|data| decode_push_frame(shared.cat(), data, ctrl));
    let (decoded, seen) = decoded?;
    if PUSH_HOLD_FOR_DDL.take_once() {
        hold_push_for_ddl(shared, target_id, &seen).await;
    }
    let catalog = shared.catalog_rwlock.read().await;
    let kind = target_kind(shared, target_id, Access::Write)?;

    let batch = match decoded.data_batch {
        Some(b) if !b.is_empty() => b,
        _ => {
            send_push_ack(peer, target_id, 0);
            return Ok(());
        }
    };

    shared.cat().recheck_record(target_id, &seen)?;

    let mode = flags.conflict_mode;

    // Not at the decode boundary: the block validator runs there without a
    // relation kind, and must keep admitting a base table's retractions.
    if kind == RelationKind::Stream {
        if let Some(e) = stream_push_error(target_id, &batch, mode) {
            return Err(e.into());
        }
    }

    // The validator's own predicate decides the guard: a push that reads no
    // committed state cannot be invalidated by a concurrent one, so it may share
    // its table's lock and reach the committer alongside other pushes to the same
    // table, which fold into one SAL zone and one fsync. Every other push takes
    // all FK-related table locks exclusively.
    let reads_committed = shared.cat().push_reads_committed_state(target_id, mode);
    let held = if !reads_committed {
        shared.lock_table_shared(target_id).await
    } else {
        shared
            .lock_tables_exclusive(shared.cat().fk_lock_set(target_id).collect())
            .await
    };

    // Distributed validation (PK / FK / unique indices). A plain push is a
    // one-family bundle — the same four rules over the same fold — so the batch
    // rides into the family for the validation and back out for the commit
    // request. The validator itself skips a bundle no rule would read a row for.
    let family = TxnFamily { tid: target_id, mode, batch };
    shared
        .disp()
        .validate_txn_distributed(std::slice::from_ref(&family))
        .await?;
    let TxnFamily { batch, .. } = family;

    let is_stream = kind == RelationKind::Stream;
    let zone_lsn = shared
        .commit(&catalog, &held, [target_id], |done| {
            CommitRequest::Push(PendingPush {
                tid: target_id,
                batch,
                recoverable: !is_stream,
                done,
            })
        })
        .await?;
    // A stream replies `0`: its push is not durable, and an ACK reports an LSN
    // only for a write a restart must recover. Keyed on the target rather than on
    // whether this batch happened to open a zone, so a stream push the committer
    // coalesced with a base-table push still answers `0`.
    let reply_lsn = if is_stream { 0 } else { zone_lsn };
    send_push_ack(peer, target_id, reply_lsn);
    Ok(())
}

/// SCAN or SEEK of one relation: a catalog family is served master-locally,
/// anything else fans out — a seek as a one-key `ScanSpec`.
async fn handle_read(
    shared: &Rc<Shared>,
    peer: &Peer,
    ctrl: &gnitz_wire::control::DecodedControl,
    blob: &[u8],
    verb: ClientVerb,
) -> Result<(), WireFault> {
    let target_id = ctrl.hdr.target_id;
    let client_version = ctrl.hdr.flags.schema_version;
    let (g, kind) = read_lock(shared, target_id, Access::Read).await?;
    let schema = shared.disp().schema_desc_for(target_id);
    let bound = match verb {
        ClientVerb::Seek => {
            let opk = seek_opk_bytes(&schema, blob).map_err(|e| format!("seek: table {target_id}: {e}"))?;
            ReadBound::PkSet(PkKeys::from_keys(schema.pk_stride(), [opk.pk_bytes()]))
        }
        _ => ReadBound::None,
    };
    let spec = ReadSpec::all_rows(bound);

    if kind == RelationKind::SystemCatalog {
        let rows = guard_panic("read", || {
            shared
                .cat()
                .registry
                .scan_spec(target_id, spec, schema.layout_digest(), None)
        })?;
        let (schema_block, server_version) = shared.cat().negotiated_schema_block(target_id, client_version);
        send_msg(
            peer,
            ipc::WireMsg {
                target_id,
                flags: WireFlags {
                    schema_version: server_version,
                    ..Default::default()
                },
                arg0: shared.last_tick_lsn.get(),
                data: if rows.is_empty() {
                    ipc::WireData::None
                } else {
                    ipc::WireData::Whole(&rows)
                },
                schema_block: schema_block.as_deref(),
                ..Default::default()
            },
        );
        return Ok(());
    }

    let (sal_kind, blob, reply_layout) = match verb {
        ClientVerb::Seek => (SalMessageKind::ScanSpec, spec.encode(), schema.layout_digest()),
        _ => (SalMessageKind::Scan, Vec::new(), 0),
    };

    let (prelim, server_version) = shared.cat().negotiated_schema_block(target_id, client_version);
    if let Some(block) = prelim {
        send_msg(peer, prelim_schema_msg(target_id, server_version, &block));
    }

    let template = ipc::WireMsg {
        target_id,
        flags: WireFlags {
            schema_version: server_version,
            ..Default::default()
        },
        arg0: reply_layout,
        blob: &blob,
        ..Default::default()
    };
    let group = DirectGroup { template, ..DirectGroup::new(sal_kind) };
    let result = fan_out_scan(shared, peer, g, kind, group).await;
    finish_scan_fanout(peer, target_id, 0, result);
    Ok(())
}

/// PUSH_TXN: validate the bundle under the catalog read lock and its tables'
/// lock union — the plain-push arm's order — check OCC, and commit it as one
/// zone. Every rejection is pre-SAL, so a fault or a conflict commits nothing.
async fn handle_push_txn(
    shared: &Rc<Shared>,
    peer: &Peer,
    ctrl: &DecodedControl,
    buf: RecvBuf,
) -> Result<(), WireFault> {
    // 1. Decode.
    let (decoded, _charge) = buf.decode(|data| decode_push_txn_frame(shared.cat(), &data[ctrl.body.clone()]));
    let DecodedTxn { families, heads } = decoded?;

    // 2. Catalog read lock (excludes a concurrent DROP/DDL), then the
    //    catalog-dependent rules per family.
    let catalog = shared.catalog_rwlock.read().await;
    for head in &heads {
        let tid = head.tid;
        // The plain-push arm's gate. Refusing a stream keeps every family
        // `recoverable`, so a transaction always opens a zone.
        let kind = target_kind(shared, tid, Access::Write)?;
        if kind == RelationKind::Stream {
            return Err(format!("table {tid} is a stream: a stream cannot be written inside a transaction").into());
        }
        shared.cat().recheck_record(tid, &head.seen)?;
    }

    // 3. Acquire the per-table lock union ⋃ fk_lock_set(tid) exclusively.
    let mut union: Vec<u64> = Vec::new();
    for fam in &families {
        union.extend(shared.cat().fk_lock_set(fam.tid));
    }
    let held = shared.lock_tables_exclusive(union).await;

    // 3b. OCC. A blind family's `BLIND` basis no commit exceeds.
    if let Some(head) = heads.iter().find(|h| shared.written_since(&held, h.tid, h.basis)) {
        return Err(WireFault {
            status: WireStatus::TxnConflict,
            text: format!(
                "transaction conflict on '{}': it was written after the read this write was built from; retry",
                shared.cat().qualified_name(head.tid)
            ),
        });
    }

    // 4. Distributed bundle validation (the four rules).
    shared.disp().validate_txn_distributed(&families).await?;

    // 5. Commit.
    let lsn = shared
        .commit(&catalog, &held, heads.iter().map(|h| h.tid), |done| {
            CommitRequest::Txn(PendingTxn { families, done })
        })
        .await?;
    // Standard single-frame ACK (uncorrelated, as the DDL_TXN reply is).
    send_msg(peer, ipc::WireMsg { arg0: lsn, ..Default::default() });
    Ok(())
}

/// A `PUSH_TXN` frame's families, their batches decoded, and each family's head.
struct DecodedTxn {
    families: Vec<TxnFamily>,
    heads: Vec<TxnHead>,
}

/// What the gate and the OCC check read of one `PUSH_TXN` family.
struct TxnHead {
    tid: u64,
    basis: u64,
    seen: Rc<[u8]>,
}

/// Decode a `PUSH_TXN` frame body, applying every rule that needs no catalog lock.
fn decode_push_txn_frame(cat: &CatalogEngine, body: &[u8]) -> Result<DecodedTxn, WireFault> {
    let items =
        gnitz_wire::txn_frame::decode_items(body, ClientVerb::PushTxn).map_err(|e| format!("decode error: {e}"))?;
    let mut families: Vec<TxnFamily> = Vec::with_capacity(items.len());
    let mut heads = Vec::with_capacity(items.len());
    for (frame, ctrl) in items {
        let (tid, mode, basis) = (ctrl.hdr.target_id, ctrl.hdr.flags.conflict_mode, ctrl.hdr.arg0);
        let (wire, seen) = decode_push_frame(cat, frame, ctrl).map_err(|e| WireFault {
            text: format!("TXN family {tid}: {}", e.text),
            ..e
        })?;
        let batch = wire.data_batch.expect("a PUSH_TXN item carries a data block");
        // A zero-row block passes the item shape check, and an empty family
        // would open a zone and bump its tables' commit LSN.
        if batch.is_empty() {
            return Err(format!("TXN: empty batch for table {tid}").into());
        }
        heads.push(TxnHead { tid, basis, seen });
        families.push(TxnFamily { tid, mode, batch });
    }
    Ok(DecodedTxn { families, heads })
}

fn not_found(tid: u64) -> WireFault {
    WireFault {
        status: WireStatus::NotFound,
        text: format!("relation {tid} not found"),
    }
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
/// reach the engine directly.
///
/// **Only the absent-relation arm carries a status of its own**
/// ([`WireStatus::NotFound`]): the arms below name a relation that exists, which a
/// client must not recover from the way it recovers from a vanished one.
fn target_kind(shared: &Shared, target_id: u64, access: Access) -> Result<RelationKind, WireFault> {
    let Some(kind) = shared.cat().registry.relation(target_id).map(Relation::kind) else {
        return Err(not_found(target_id));
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

/// The relation id a RESOLVE names, unvalidated when the client sent an id; `None`
/// when its qualified name names none. `Err` when the name's schema does not exist.
fn resolve_request_target(shared: &Rc<Shared>, target_id: u64, name_blob: &[u8]) -> Result<Option<u64>, WireFault> {
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
                    return Err(WireFault {
                        status: WireStatus::NotFound,
                        text: format!("schema '{schema_name}' not found"),
                    });
                }
                return Ok(None);
            }
        }
    };
    Ok(Some(candidate))
}

/// Answer a RESOLVE with the relation's schema block and descriptor.
fn build_resolve_reply(shared: &Rc<Shared>, peer: &Peer, target_id: u64, name_blob: &[u8]) -> Result<(), WireFault> {
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
            target_id: tid,
            flags: WireFlags {
                schema_version: version,
                ..Default::default()
            },
            schema_block: Some(&schema_block),
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
/// Sound because the committer marks every scanned tid in `tick_rows` before
/// publishing that commit's zone LSN, a contract stated at that mark; a *missing* map entry is
/// the `boot_seed` argument. Erring towards false is harmless (one extra drain),
/// which is where a stream lands: its reserved LSN is never published.
///
/// A non-view target is vacuously fresh and answers before the closure walk.
/// Caller holds the catalog read lock.
fn read_is_fresh(shared: &Rc<Shared>, target: u64) -> bool {
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
/// that validated it, draining first when the target is a stale view.
///
/// The one read-lock entry point for every single-target read verb: each passes
/// the `Access` its realization can serve and routes on the returned kind, rather
/// than re-deciding the system/user split from the id. A stale view re-resolves
/// after the drain, since a DDL may have dropped it meanwhile.
async fn read_lock(
    shared: &Rc<Shared>,
    target_id: u64,
    access: Access,
) -> Result<(ReadGuard, RelationKind), WireFault> {
    let g = shared.catalog_rwlock.read().await;
    let kind = target_kind(shared, target_id, access)?;
    if read_is_fresh(shared, target_id) {
        return Ok((g, kind));
    }
    let g = drain_and_relock(shared, g).await?;
    let kind = target_kind(shared, target_id, access)?;
    Ok((g, kind))
}

/// The preliminary schema-only frame — carrying `continuation`, the
/// `server_version`, and the captured wire block — that precedes a scan's data
/// frames on a schema-cache miss.
fn prelim_schema_msg(tid: u64, server_version: u16, block: &[u8]) -> ipc::WireMsg<'_> {
    ipc::WireMsg {
        target_id: tid,
        flags: WireFlags::train_frame(server_version, false),
        schema_block: Some(block),
        ..Default::default()
    }
}

/// A reply train's terminal frame. `arg0` is the read's watermark (see
/// [`read_watermark`]), or a DELTA_POLL position's tick round, whose cursor tag
/// rides `arg1`; `arg1` is `0` for every other read.
fn terminal_scan_msg(target_id: u64, arg0: u64, arg1: u64) -> ipc::WireMsg<'static> {
    ipc::WireMsg {
        target_id,
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
fn finish_scan_fanout(peer: &Peer, target_id: u64, arg1: u64, result: Result<u64, WireFault>) {
    match result {
        // Corked, not sent: the terminal joins whatever the forward corked.
        Ok(arg0) => send_msg(peer, terminal_scan_msg(target_id, arg0, arg1)),
        Err(f) => send_fault(peer, target_id, &f),
    }
}

/// SCAN_SPEC: the scan pipeline under a client-authored reply schema, so no schema
/// block goes back, routed by the request's bound.
async fn handle_scan_spec(
    shared: &Rc<Shared>,
    peer: &Peer,
    target_id: u64,
    blob: &[u8],
    reply_layout: u64,
) -> Result<(), WireFault> {
    // `UserRead`: a `ReadSpec` has only a fan-out realization, which a catalog
    // family has no form of.
    let (g, kind) = read_lock(shared, target_id, Access::UserRead).await?;
    let template = ipc::WireMsg {
        target_id,
        arg0: reply_layout,
        blob,
        ..Default::default()
    };
    let group = DirectGroup {
        template,
        ..DirectGroup::new(SalMessageKind::ScanSpec)
    };
    let result = fan_out_scan(shared, peer, g, kind, group).await;
    finish_scan_fanout(peer, target_id, 0, result);
    Ok(())
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

/// DELTA_POLL: advance N mirrored views in one request, one catalog lock and —
/// for however many of them moved — one broadcast.
///
/// An `Err` rejects the frame, at `target_id = 0`; a per-view failure is not an
/// `Err` — it goes out as that view's own fault frame, and the rest of the poll
/// continues.
///
/// Never drains pending ticks: a delta read reports what has happened, and a
/// round not yet ticked is one the next poll carries.
async fn handle_delta_poll(shared: &Rc<Shared>, peer: &Peer, body: &[u8]) -> Result<(), WireFault> {
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
            let tid = item.view_id;
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
                    let reply_layout = item.reply_layout.to_le_bytes();
                    cut.read(DirectGroup {
                        template: ipc::WireMsg {
                            target_id: item.view_id,
                            arg0: round,
                            arg1: item.after_tick,
                            blob: &reply_layout,
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
fn delta_up_to_date(shared: &Shared, target_id: u64, after_tick: u64) -> bool {
    after_tick > 0
        && shared
            .cat()
            .registry
            .relation(target_id)
            .is_some_and(Relation::has_delta_feed)
        && after_tick >= shared.disp().last_delta_round(target_id)
}

/// One relation's Phase-1 capture for `handle_scan_multi`: exactly what
/// [`CatalogEngine::negotiated_schema_block`] answered, carried to the deferred Phase-2 emit.
struct ScanMultiRelPlan {
    tid: u64,
    /// Stamped into the preliminary frame and onto the read, whose frames echo it.
    server_version: u16,
    kind: RelationKind,
    /// The read's watermark, sampled inside the cut.
    lsn: u64,
    /// The wire schema block to emit before this relation's train, present iff
    /// the client's cached version missed.
    block: Option<Rc<[u8]>>,
}

/// SCAN_MULTI: snapshot N relations at one SAL cut and stream N reply trains in
/// request order. The read-side completion of the atomic multi-table write
/// story: an atomic commit is either wholly before the cut (visible in every
/// train) or wholly after (visible in none), never torn across the result set.
///
/// Every scan's lease lives in the `dispatches` vec and drops on return, so an
/// early return removes every route and discards undrained frames at the ring
/// boundary.
async fn handle_scan_multi(shared: &Rc<Shared>, peer: &Peer, body: &[u8]) -> Result<(), WireFault> {
    // ── Phase 0: decode; tid legality is Phase 1's, under the catalog lock ──
    let relations = gnitz_wire::txn_frame::decode_scan_multi(body).map_err(|e| format!("decode error: {e}"))?;

    // Drain once if any target is a stale view — the same test `read_lock` runs
    // for a single target. Phase 1 resolves every tid's kind under the guard
    // handed back, so a DDL during the drain is caught there and an unknown tid
    // is rejected there rather than here.
    let mut cat = shared.catalog_rwlock.read().await;
    if relations.iter().any(|&(tid, _)| !read_is_fresh(shared, tid)) {
        cat = drain_and_relock(shared, cat).await?;
    }

    // ── Phase 1: catalog lock — resolve shapes + schemas, dispatch one cut ──
    // One catalog snapshot for every relation, one SAL cut for every group.
    let (dispatches, plans) = {
        let _cat = cat;
        let mut plans: Vec<ScanMultiRelPlan> = Vec::with_capacity(relations.len());
        for &(tid_u, client_ver) in &relations {
            let tid = tid_u;
            // Base tables AND views are legal; `UserRead` refuses a catalog
            // family, which stays on the plain path that serves it
            // master-locally.
            let kind = target_kind(shared, tid, Access::UserRead)?;
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
                            target_id: plan.tid,
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
            send_msg(peer, prelim_schema_msg(plan.tid, plan.server_version, block));
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
    out[base..base + PFX].copy_from_slice(&gnitz_wire::frame_len_prefix(sz));
    msg.encode(&mut out[base + PFX..total]);
}

/// Cork `msg` for the client, or a fault in its place past `MAX_FRAME_PAYLOAD`.
/// The connection loop ships it.
fn send_msg(peer: &Peer, msg: ipc::WireMsg<'_>) {
    let sz = msg.size();
    if sz > gnitz_wire::MAX_FRAME_PAYLOAD {
        send_fault(peer, msg.target_id, &ipc::oversized_frame_message(sz).into());
        return;
    }
    peer.cork_with(|out| encode_response_into(out, msg));
}

/// A push's ACK: the target and the LSN the write reports, and no schema.
fn send_push_ack(peer: &Peer, target_id: u64, lsn: u64) {
    send_msg(
        peer,
        ipc::WireMsg {
            target_id,
            arg0: lsn,
            ..Default::default()
        },
    )
}

/// A successful control-only reply whose answer is the target id.
fn send_id(peer: &Peer, id: u64) {
    send_msg(peer, ipc::WireMsg { target_id: id, ..Default::default() })
}

/// A failure carrying its own status, master-minted or forwarded from a worker.
/// A failure describes no rows, so it carries no schema block.
fn send_fault(peer: &Peer, target_id: u64, fault: &WireFault) {
    send_msg(
        peer,
        ipc::WireMsg {
            target_id,
            status: fault.status,
            blob: fault.text.as_bytes(),
            ..Default::default()
        },
    )
}

/// Why a stream cannot accept this push, or `None` if it can. Both rules restate
/// what a stream lacks — a unique primary key, and any retraction at all — and
/// rejecting `Error` mode is what keeps `push_reads_committed_state` false, and
/// with it the shared table lock and the unread mode field.
fn stream_push_error(target_id: u64, batch: &Batch, mode: gnitz_wire::WireConflictMode) -> Option<String> {
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
