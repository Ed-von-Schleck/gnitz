//! Server executor: the process lifecycle, the `Shared` state, the request
//! router and every read/push handler, and the reply-frame vocabulary. The
//! catalog-zone write path is the child `ddl`; the delta feed's readers are
//! the child `delta`.
//!
//! The master owns one `Reactor`: [`run`] spawns its tasks on it
//! and races the signal loop against a worker's death.
//!
//! A request handler rejects by returning `Err`, and the router sends it: no
//! handler writes a fault frame for its request as a whole.
//!
//! One connection's replies leave in request order, written by its own task.
//! The one thing another task sends a client is a pushed train, and it only
//! queues it: the connection's task ships it between two replies.

mod ddl;
mod delta;

use std::cell::{Cell, RefCell};
use std::os::fd::{AsFd, OwnedFd};
use std::rc::Rc;
use std::time::{Duration, Instant};

use rustc_hash::{FxHashMap, FxHashSet};

use gnitz_foundation::fault::Seam;

use self::ddl::{handle_ddl_txn, hold_tick_for_ddl, reserve_serial_range, TICK_HOLD_FOR_DDL};
use self::delta::{handle_delta_poll, Feeds, Subscriptions};
use super::TxnFamily;
use crate::catalog::CatalogEngine;
use crate::runtime::committer::{self, BarrierKind, CommitRequest, PendingWrite};
use crate::runtime::listen::ClientListener;
use crate::runtime::master::{forward_scan, MasterDispatcher, WORKER_WATCH};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{chan, oneshot, select2, AckLease, AsyncRwLock, Either, ReadGuard, RecvBuf, WriteGuard};
use crate::runtime::sal::{DirectGroup, Read, SalExcl};
use crate::runtime::wire as ipc;
use gnitz_store::relation::{Relation, RelationKind};
use gnitz_wire::control::{DecodedControl, Target};
use gnitz_wire::txn_frame::ScanItem;
use gnitz_wire::{ReadSpec, WireFault, WireStatus};
use gnitz_zset::repr::Batch;

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
        shared.disp().reactor().sleep(Duration::from_millis(1)).await;
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
            .is_none_or(|now| !Rc::ptr_eq(seen, &now))
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
        /// Asked for by a waiting sync: held to [`Shared::patient_tick_gap`] unless a
        /// trigger that is not joins the batch.
        patient: bool,
    },
}

/// Ask the tick loop to tick everything pending and report the tick's verdict.
pub(super) fn request_drain(shared: &Shared, patient: bool) -> oneshot::Receiver<Result<(), WireFault>> {
    let (done, rx) = oneshot::channel();
    shared.tick_tx.send(TickTrigger::Drain { done, patient });
    rx
}

/// Send the committer a barrier of `kind`; the receiver resolves once it is
/// serviced.
fn request_barrier(shared: &Shared, kind: BarrierKind) -> oneshot::Receiver<()> {
    let (done, rx) = oneshot::channel();
    shared.committer_tx.send(CommitRequest::Barrier { kind, done });
    rx
}

/// The SYNC_PUSHEDs held back with nothing to report, each with the relations
/// whose change ends its wait.
#[derive(Default)]
struct SyncWaiters {
    next_id: Cell<u64>,
    parked: RefCell<Vec<ParkedSync>>,
}

struct ParkedSync {
    id: u64,
    watched: FxHashSet<u64>,
    wake: oneshot::Sender<()>,
}

/// One sync's place among the [`SyncWaiters`], given up on drop.
struct Parked<'a> {
    waiters: &'a SyncWaiters,
    id: u64,
    woken: oneshot::Receiver<()>,
}

impl SyncWaiters {
    fn park(&self, watched: FxHashSet<u64>) -> Parked<'_> {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        let (wake, woken) = oneshot::channel();
        self.parked.borrow_mut().push(ParkedSync { id, watched, wake });
        Parked { waiters: self, id, woken }
    }

    /// Wake every sync watching `relation`.
    fn wake(&self, relation: u64) {
        let mut parked = self.parked.borrow_mut();
        for sync in parked.extract_if(.., |p| p.watched.contains(&relation)) {
            sync.wake.send(());
        }
    }
}

impl Drop for Parked<'_> {
    fn drop(&mut self) {
        self.waiters.parked.borrow_mut().retain(|p| p.id != self.id);
    }
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
    /// The SAL watermark the last emitted tick snapshotted: every commit at or
    /// below it is reflected in every view, as a read written to the SAL from
    /// now on sees it.
    last_tick_lsn: Cell<u64>,
    /// Tables with a pending delta, each with the row count feeding the tick
    /// threshold.
    tick_rows: RefCell<FxHashMap<u64, usize>>,
    sync_waiters: SyncWaiters,
    feeds: Feeds,
    /// The most bytes of pushed trains sent one connection between two of its
    /// SYNC_PUSHEDs, and so the most its client holds unread
    /// (`GNITZ_PUSH_QUEUE_BYTES`). A subscription whose train would pass it
    /// is ended, and continues by polling.
    push_queue_bytes: usize,
    /// Per-table write serialization. A push whose validation reads committed
    /// state (`push_reads_committed_state`) and every transaction take the write
    /// guard; a push that reads no committed state takes the read guard, so
    /// same-table pushes reach the committer concurrently and share one fsync.
    table_locks: RefCell<FxHashMap<u64, AsyncRwLock>>,
    /// Set by the signal loop before it sends the final Shutdown barrier. Read
    /// only by [`Shared::submit`].
    draining: Cell<bool>,
    /// OCC per-table commit-LSN map: `tid → the zone LSN its last accepted write
    /// this boot rode`. A missing entry reads as `boot_seed`.
    /// Single-threaded reactor — a plain `RefCell`, and no borrow is ever held across an
    /// `.await`.
    table_commit_lsn: RefCell<FxHashMap<u64, u64>>,
    /// The SAL watermark when the executor started: every read watermark this
    /// boot is at or above it, and every commit's zone above it.
    boot_seed: u64,
    /// The data directory, where each worker's log lives.
    data_dir: String,
    /// How long a connection may take from accept to its HELLO, handshake
    /// included (`GNITZ_HELLO_TIMEOUT_MS`). The default outlasts the client's own
    /// `CONNECT_TIMEOUT`, so only a client that has already given up is reaped.
    hello_timeout: Duration,
    /// The least time between a tick and one only waiting syncs ask for
    /// (`GNITZ_PATIENT_TICK_GAP_MS`): such a sync is re-issued the moment it is
    /// answered, so unpaced it would tick per commit.
    patient_tick_gap: Duration,
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
    fn cat(&self) -> &CatalogEngine {
        self.dispatcher.cat()
    }

    #[allow(clippy::mut_from_ref)]
    fn cat_mut(&self) -> &mut CatalogEngine {
        self.dispatcher.cat_mut()
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

    /// Queue `families` for commit as one write, or refuse it once a graceful
    /// shutdown has begun: a write is ahead of the Shutdown barrier or refused.
    fn submit(&self, families: Vec<TxnFamily>) -> Result<oneshot::Receiver<Result<u64, WireFault>>, WireFault> {
        if self.draining.get() {
            return Err("server shutting down".to_string().into());
        }
        let (done, rx) = oneshot::channel();
        self.committer_tx
            .send(CommitRequest::Write(PendingWrite { families, done }));
        Ok(rx)
    }

    /// Validate `families` as one bundle, commit it with the families its
    /// deletes cascade to as one write, and raise each written table's commit
    /// LSN to the write's.
    async fn commit(
        &self,
        _catalog: &ReadGuard,
        held: &HeldTables,
        mut families: Vec<TxnFamily>,
    ) -> Result<u64, WireFault> {
        debug_assert!(
            families
                .iter()
                .all(|f| held.holds_exclusive(f.tid) || !self.cat().push_reads_committed_state(f.tid, f.mode)),
            "a write whose validation reads committed state holds its table exclusively"
        );
        self.disp().validate_txn_distributed(&mut families).await?;
        let tids: Vec<u64> = families.iter().map(|f| f.tid).collect();
        assert!(
            tids.iter().all(|&t| held.holds(t)),
            "a commit bumps only tables whose lock it holds"
        );
        let lsn = self.submit(families)?.await?;
        let mut map = self.table_commit_lsn.borrow_mut();
        for tid in tids {
            // Pushes sharing a read guard resume from the await in any order.
            let e = map.entry(tid).or_default();
            *e = (*e).max(lsn);
            // After the raise, which is what a woken poll reads as staleness.
            self.sync_waiters.wake(tid);
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
    /// a table not written this boot: no basis read this boot is below it, and
    /// `last_tick_lsn` starts at it, so an unwritten table compares as absorbed —
    /// which it is, boot finishing its recovery tick sweep first.
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
        self.sync_waiters.wake(id);
        self.table_locks.borrow_mut().remove(&id);
        self.table_commit_lsn.borrow_mut().remove(&id);
        self.tick_rows.borrow_mut().remove(&id);
        self.disp().forget_relation(id);
    }

    /// Credit `rows` against each tid's pending-tick count and fire the auto-tick
    /// if any tid now stands at or above the coalesce threshold. Below it a push
    /// only accumulates, and nothing ticks until a read or a delta poll asks for
    /// a drain. `_laid_out` is the SAL hold the rows' groups were written under.
    pub(super) fn note_commit_rows(&self, _laid_out: &SalExcl<'_>, rows: impl Iterator<Item = (u64, usize)>) {
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

    /// Tick every table with a pending delta as one Tick group, and answer the
    /// lease its workers ACK on; `None` when nothing was pending. A refused emit
    /// leaves every delta pending.
    async fn emit_pending_tick(&self) -> Result<Option<AckLease>, WireFault> {
        if self.tick_rows.borrow().is_empty() {
            self.last_tick_lsn.set(self.disp().sal().watermark());
            return Ok(None);
        }
        // The hold `note_commit_rows` is called under, so the tids and the
        // watermark read here are of the same commits.
        let excl = self.disp().sal().lock().await;
        if TICK_EMIT_ERROR.take_once() {
            return Err("injected tick emit error".to_string().into());
        }
        let tids: Vec<u64> = self.tick_rows.borrow().keys().copied().collect();
        let lease = self.disp().emit_tick(&excl, &tids)?;
        self.tick_rows.borrow_mut().clear();
        self.last_tick_lsn.set(self.disp().sal().watermark());
        Ok(Some(lease))
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Serve `listeners` until a shutdown signal or a worker's death; the process's
/// exit code.
pub fn run(dispatcher: Rc<MasterDispatcher>, data_dir: &str, listeners: Vec<ClientListener>) -> i32 {
    let reactor = dispatcher.reactor().clone();
    let boot_seed = dispatcher.sal().watermark();

    let (committer_tx, committer_rx) = chan::unbounded::<CommitRequest>();
    let (tick_tx, tick_rx) = chan::unbounded::<TickTrigger>();
    let shared = Rc::new(Shared {
        dispatcher,
        committer_tx,
        catalog_rwlock: AsyncRwLock::default(),
        tick_gate: AsyncRwLock::default(),
        tick_tx,
        last_tick_lsn: Cell::new(boot_seed),
        tick_rows: RefCell::new(FxHashMap::default()),
        sync_waiters: SyncWaiters::default(),
        feeds: Feeds::default(),
        push_queue_bytes: gnitz_foundation::env::env_num("GNITZ_PUSH_QUEUE_BYTES", 8usize << 20),
        table_locks: RefCell::new(FxHashMap::default()),
        draining: Cell::new(false),
        table_commit_lsn: RefCell::new(FxHashMap::default()),
        boot_seed,
        data_dir: data_dir.to_string(),
        hello_timeout: Duration::from_millis(gnitz_foundation::env::env_num(
            "GNITZ_HELLO_TIMEOUT_MS",
            (gnitz_wire::CONNECT_TIMEOUT * 3 / 2).as_millis() as u64,
        )),
        patient_tick_gap: Duration::from_millis(gnitz_foundation::env::env_num("GNITZ_PATIENT_TICK_GAP_MS", 10)),
    });

    // Catch SIGTERM/SIGINT so the signal loop can drive a final checkpoint
    // before exiting.
    install_shutdown_signal_handlers();
    // Past the last env knob the boot reads, so one it refuses stops the
    // server before it reports ready.
    gnitz_note!("GnitzDB ready");

    reactor.spawn(committer::run(committer_rx, Rc::clone(&shared)));
    for listener in listeners {
        reactor.spawn(accept_loop(Rc::clone(&shared), listener));
    }
    reactor.spawn(tick_loop(Rc::clone(&shared), tick_rx));

    // `2` separates a dead worker from a failed boot's `1` and from
    // `gnitz_fatal_abort!`'s `134`.
    reactor.block_on(async move {
        match select2(serve_until_signalled(&shared), shared.disp().worker_death()).await {
            Either::A(()) => 0,
            Either::B(crashed) => {
                let data_dir = &shared.data_dir;
                gnitz_error!("Worker {crashed} crashed (log: {data_dir}/worker_{crashed}.log), shutting down");
                shared.disp().kill_workers();
                2
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

async fn accept_loop(shared: Rc<Shared>, listener: ClientListener) {
    let ClientListener { fd, tls } = listener;
    // Leaked: the loop never ends, and its accept SQEs name the fd throughout.
    let fd: &'static OwnedFd = Box::leak(Box::new(fd));
    let reactor = shared.disp().reactor().clone();
    loop {
        let Some(conn) = reactor.client_conn(reactor.accept(fd.as_fd()).await) else {
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

/// One message is handled to completion before the next is received, so
/// replies leave in request order, which is how a client pairs each with its
/// request. A sync's answer is no reply, and leaves between two.
///
/// Returns when the peer is gone or refused; the caller closes.
async fn serve_connection(peer: &Peer, shared: &Rc<Shared>) {
    let deadline = shared.disp().reactor().sleep(shared.hello_timeout);
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

    let mut subs = Subscriptions::default();
    let out = peer.outbox();
    loop {
        // Ahead of the next request: a client that always has one ready is
        // still answered its sync.
        if let Some(waiting) = subs.sync_wait(shared, &out) {
            let Ok(synced) = peer.until_request(waiting.as_mut()).await else {
                break;
            };
            if let Some(synced) = synced {
                subs.answer_sync(shared, peer, synced).await;
                continue;
            }
        }
        let Some(buf) = peer.next_request().await else { break };
        handle_message(peer, &mut subs, buf, shared).await;
        // Behind the reply, where a pushed train splits none. A client that
        // is gone is found out by the next request.
        let _ = peer.ship_pushed().await;
    }
    subs.leave(&shared.feeds, |_| true);
}

// ---------------------------------------------------------------------------
// Signal loop: graceful shutdown (SIGTERM / SIGINT) and SAL reclaim
// ---------------------------------------------------------------------------

/// Set by the SIGTERM/SIGINT handler; polled by the signal loop. A plain
/// `AtomicBool` store is async-signal-safe (unlike touching the reactor-thread
/// `Cell` flags), so the handler does nothing but flip this.
static SHUTDOWN_REQUESTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

extern "C" fn handle_shutdown_signal(_sig: libc::c_int) {
    SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Install async-signal-safe handlers for SIGTERM and SIGINT. The handler only
/// flips `SHUTDOWN_REQUESTED`; the signal loop's timer picks it up.
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

/// Until SIGTERM/SIGINT: poke the committer whenever the SAL wants a checkpoint.
/// Then stop admitting pushes, run one final checkpoint, and stop the workers.
async fn serve_until_signalled(shared: &Shared) {
    loop {
        shared.disp().reactor().sleep(WORKER_WATCH).await;

        if SHUTDOWN_REQUESTED.load(std::sync::atomic::Ordering::Relaxed) {
            gnitz_info!("shutdown signal received; draining, checkpointing, and stopping");
            // No push may commit after the final flush.
            shared.draining.set(true);
            // Resolves once the committer has run the final checkpoint, which
            // also drains the pushes no tick has taken yet.
            request_barrier(shared, BarrierKind::Shutdown).await;
            shared.disp().shutdown_workers().await;
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
/// so this loop delays no tick to gather more. The one tick it holds back is
/// one only patient drains ask for, to [`Shared::patient_tick_gap`] after the last.
async fn tick_loop(shared: Rc<Shared>, mut rx: chan::Receiver<TickTrigger>) {
    // The batch's `Drain` repliers.
    let mut dones: Vec<oneshot::Sender<Result<(), WireFault>>> = Vec::new();
    let mut patient_from = Instant::now();
    loop {
        let mut all_patient = true;
        let mut trigger = rx.recv().await;
        loop {
            match trigger {
                TickTrigger::Auto => all_patient = false,
                TickTrigger::Drain { done, patient } => {
                    all_patient &= patient;
                    dones.push(done);
                }
            }
            trigger = match rx.try_recv() {
                Some(t) => t,
                None => {
                    let hold = patient_from.saturating_duration_since(Instant::now());
                    if !all_patient || hold.is_zero() {
                        break;
                    }
                    match select2(rx.recv(), shared.disp().reactor().sleep(hold)).await {
                        Either::A(t) => t,
                        Either::B(()) => break,
                    }
                }
            };
        }

        let _ticking = shared.tick_gate.read().await;
        let emitted = shared.emit_pending_tick().await;
        if let Err(e) = &emitted {
            gnitz_warn!("tick error: {}", e);
        }
        // At the emit, not its ACKs: a waiting reader's scan follows the tick's
        // group in the SAL.
        let verdict = emitted.as_ref().map(|_| ()).map_err(WireFault::clone);
        for done in dones.drain(..) {
            done.send(verdict.clone());
        }
        if let Ok(Some(lease)) = emitted {
            if TICK_HOLD_FOR_DDL.take_once() {
                hold_tick_for_ddl(&shared).await;
            }
            lease.acks().await;
        }
        patient_from = Instant::now() + shared.patient_tick_gap;
    }
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

async fn handle_message(peer: &Peer, subs: &mut Subscriptions, buf: RecvBuf, shared: &Rc<Shared>) {
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
    if let Err(f) = dispatch_request(peer, subs, buf, ctrl, shared).await {
        send_fault(peer, target_id, &f);
    }
}

/// Run the verb `ctrl` names. [`handle_message`] answers an `Err` with one fault
/// frame, which fails the whole request at the client; a fault confined to one
/// position of a streamed reply is the handler's own to send.
async fn dispatch_request(
    peer: &Peer,
    subs: &mut Subscriptions,
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
        ClientVerb::ScanMulti => {
            let scans =
                gnitz_wire::txn_frame::decode_scan_multi(&data[ctrl.body]).map_err(|e| format!("decode error: {e}"))?;
            handle_scan(shared, peer, &scans).await
        }
        ClientVerb::DeltaPoll => handle_delta_poll(shared, peer, subs, &ctrl.hdr, &data[ctrl.body]).await,

        // `target_id` is the sequence key (= the owning table's id).
        ClientVerb::AllocSerialRange => {
            let base = reserve_serial_range(shared, ctrl.hdr.target(), ctrl.hdr.arg0).await?;
            send_ack(peer, target_id, base as u64);
            Ok(())
        }

        // An id allocation names no relation, so `target_id` is only echoed —
        // and a frame that sets one is still allocated, rather than falling
        // through to a scan of that id.
        ClientVerb::AllocIds => {
            let base = shared.cat_mut().allocate_ids(ctrl.hdr.arg0);
            send_ack(peer, target_id, base.map_err(|e| format!("id allocation failed: {e}"))?);
            Ok(())
        }

        ClientVerb::ScanSpec => {
            let scan = ScanItem {
                target: ctrl.hdr.target(),
                reply_layout: ctrl.hdr.arg0,
                spec: &data[ctrl.blob.clone()],
            };
            handle_scan(shared, peer, &[scan]).await
        }

        // A plain read guard, not `fresh_read_lock`: a resolve answers catalog shape,
        // and a view tick moves a view's rows, never its shape — so the tick
        // drain that one waits for buys nothing here.
        ClientVerb::Resolve => {
            let _g = shared.catalog_rwlock.read().await;
            build_resolve_reply(shared, peer, &data[ctrl.blob.clone()])
        }

        ClientVerb::Push => handle_push(shared, peer, buf, ctrl).await,

        ClientVerb::SyncPushed => {
            // Refused or not, it is answered as a sync is: out of the order of replies.
            let held = gnitz_wire::txn_frame::decode_held(&data[ctrl.blob.clone()])
                .map_err(|e| WireFault::from(format!("decode error: {e}")));
            subs.ask_sync(&shared.feeds, held, ctrl.hdr.arg0);
            Ok(())
        }
    }
}

/// Decode a push frame against the catalog: its token must still name its target,
/// its record must lay out its target's columns, and the target's record is
/// answered for [`CatalogEngine::recheck_record`]. The token comes first, as in
/// [`target_kind`], which checks it again under the guard.
/// Runs before the catalog read guard, and off it: `AsyncRwLock` is
/// writer-preferring, so a bulk load's multi-megabyte decode under the guard
/// would stall every DDL writer behind it.
fn decode_push_frame(
    cat: &CatalogEngine,
    data: &[u8],
    ctrl: DecodedControl,
) -> Result<(Option<Batch>, Rc<[u8]>), WireFault> {
    let tid = ctrl.hdr.target_id;
    cat.check_token(ctrl.hdr.target())?;
    let target = cat.schema_record(tid).ok_or_else(|| not_found(tid))?;
    let schema = cat
        .registry
        .relation(tid)
        .map(Relation::schema)
        .ok_or_else(|| not_found(tid))?;
    match &ctrl.schema {
        Some(r) => gnitz_wire::schema_block::check_same_types(&data[r.clone()], &target)
            .map_err(|e| format!("decode error: {e}"))?,
        None if ctrl.data.is_some() => return Err("decode error: a data block without a schema block".into()),
        None => {}
    }
    let batch = ipc::decode_client_rows(data, &ctrl, &schema).map_err(|e| format!("decode error: {e}"))?;
    Ok((batch, target))
}

/// Handle a client push: decode the frame, then commit it under the catalog read
/// lock and the target's table lock(s).
///
/// One handler for both batch shapes. An empty delta — a frame with no data
/// block — is ACKed with the "nothing written" LSN `0`, but only after the same
/// existence, writability and stream-mode gates a non-empty push passes, so a
/// client bug that happens to produce an empty batch (a `delete` with an empty
/// pk list) fails the way a non-empty one would instead of being masked by a
/// no-op ACK.
async fn handle_push(shared: &Rc<Shared>, peer: &Peer, buf: RecvBuf, ctrl: DecodedControl) -> Result<(), WireFault> {
    let target = ctrl.hdr.target();
    let target_id = target.tid;
    let mode = ctrl.hdr.flags.conflict_mode;

    let (decoded, _charge) = buf.decode(|data| decode_push_frame(shared.cat(), data, ctrl));
    let (batch, seen) = decoded?;
    if PUSH_HOLD_FOR_DDL.take_once() {
        hold_push_for_ddl(shared, target_id, &seen).await;
    }
    let catalog = shared.catalog_rwlock.read().await;
    // Under the catalog guard, not at decode: a dropped id can be re-created as
    // a stream between the two, and the stream rules below read the kind.
    let is_stream = target_kind(shared, target, Access::Write)? == RelationKind::Stream;
    if is_stream {
        check_stream_push(target_id, batch.as_ref(), mode)?;
    }

    let Some(batch) = batch else {
        send_ack(peer, target_id, 0);
        return Ok(());
    };

    shared.cat().recheck_record(target_id, &seen)?;

    // See `Shared::table_locks`. The exclusive set covers the tables this
    // push's deletes cascade to.
    let families = vec![TxnFamily { tid: target_id, mode, batch }];
    let held = if shared.cat().push_reads_committed_state(target_id, mode) {
        let deletes = !families[0].batch.all_weights_positive();
        let locks = shared.cat().write_lock_set(target_id, deletes);
        shared.lock_tables_exclusive(locks).await
    } else {
        shared.lock_table_shared(target_id).await
    };
    let zone_lsn = shared.commit(&catalog, &held, families).await?;
    // A stream replies `0`: its push is not durable, and an ACK reports an LSN
    // only for a write a restart must recover. Keyed on the target rather than on
    // whether this batch happened to open a zone, so a stream push the committer
    // coalesced with a base-table push still answers `0`.
    let reply_lsn = if is_stream { 0 } else { zone_lsn };
    send_ack(peer, target_id, reply_lsn);
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
        target_kind(shared, head.target, Access::TxnWrite)?;
        shared.cat().recheck_record(head.target.tid, &head.seen)?;
    }

    // 3. Acquire the per-table lock union ⋃ write_lock_set(tid) exclusively.
    let union = families
        .iter()
        .flat_map(|f| shared.cat().write_lock_set(f.tid, !f.batch.all_weights_positive()))
        .collect();
    let held = shared.lock_tables_exclusive(union).await;

    // 3b. OCC. A blind family's `BLIND` basis no commit exceeds.
    if let Some(head) = heads
        .iter()
        .find(|h| shared.written_since(&held, h.target.tid, h.basis))
    {
        return Err(WireFault {
            status: WireStatus::TxnConflict,
            text: format!(
                "transaction conflict on '{}': it was written after the read this write was built from; retry",
                shared.cat().qualified_name(head.target.tid)
            ),
        });
    }

    // 4. Distributed bundle validation, and the commit.
    let lsn = shared.commit(&catalog, &held, families).await?;
    send_ack(peer, 0, lsn);
    Ok(())
}

/// A `PUSH_TXN` frame's families, their batches decoded, and each family's head.
struct DecodedTxn {
    families: Vec<TxnFamily>,
    heads: Vec<TxnHead>,
}

/// What the gate and the OCC check read of one `PUSH_TXN` family.
struct TxnHead {
    target: Target,
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
        let (target, mode, basis) = (ctrl.hdr.target(), ctrl.hdr.flags.conflict_mode, ctrl.hdr.arg0);
        let tid = target.tid;
        let (batch, seen) = decode_push_frame(cat, frame, ctrl).map_err(|e| WireFault {
            text: format!("TXN family {tid}: {}", e.text),
            ..e
        })?;
        let batch = batch.expect("a PUSH_TXN item carries a data block");
        heads.push(TxnHead { target, basis, seen });
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
    Write,
    /// A write inside a `PUSH_TXN`. Refusing a stream keeps every family
    /// inside the zone, so a transaction always opens one.
    TxnWrite,
}

/// Resolve `target`'s kind, rejecting a request whose descriptor token is
/// stale and a target that cannot serve `access`. The token comes first: a stale
/// one names a relation whose absence or kind is not the client's error. `0` is
/// a request that carries none.
///
/// A stream holds no rows, so it may be written (outside a transaction) but not
/// read. A view may be read
/// but not written: a push would commit rows its circuit never produced.
///
/// Enforced here even though the SQL binder refuses both: a client can push to
/// or read a relation id without going through the binder.
///
/// **Only the absent-relation arm carries a status of its own**
/// ([`WireStatus::NotFound`]): the arms below name a relation that exists, which a
/// client must not recover from the way it recovers from a vanished one.
fn target_kind(shared: &Shared, target: Target, access: Access) -> Result<RelationKind, WireFault> {
    shared.cat().check_token(target)?;
    let target_id = target.tid;
    let Some(kind) = shared.cat().registry.relation(target_id).map(Relation::kind) else {
        return Err(not_found(target_id));
    };
    match access {
        Access::Read if kind == RelationKind::Stream => {
            Err(format!("table {target_id} is a stream: a stream holds no rows and cannot be read").into())
        }
        Access::Write | Access::TxnWrite if !kind.is_ingestion_point() => {
            Err(format!("table {target_id} is not writable: pushes must target a base table or a stream").into())
        }
        Access::TxnWrite if kind == RelationKind::Stream => {
            Err(format!("table {target_id} is a stream: a stream cannot be written inside a transaction").into())
        }
        _ => Ok(kind),
    }
}

/// The relation id a RESOLVE names; `None` when its qualified name names none.
/// `Err` when the name's schema does not exist.
fn resolve_request_target(shared: &Rc<Shared>, name_blob: &[u8]) -> Result<Option<u64>, WireFault> {
    let qname = std::str::from_utf8(name_blob).map_err(|_| "RESOLVE: name is not valid UTF-8".to_string())?;
    let (schema_name, name) = qname
        .split_once('.')
        .ok_or_else(|| format!("RESOLVE: '{qname}' is not a qualified relation name"))?;
    let cat = shared.cat();
    let sid = cat.schema_id(schema_name).ok_or_else(|| WireFault {
        status: WireStatus::NotFound,
        text: format!("schema '{schema_name}' not found"),
    })?;
    Ok(cat.relation_id(sid, name))
}

/// Answer a RESOLVE with the relation's schema block, descriptor and token.
fn build_resolve_reply(shared: &Rc<Shared>, peer: &Peer, name_blob: &[u8]) -> Result<(), WireFault> {
    let answer =
        resolve_request_target(shared, name_blob)?.and_then(|tid| shared.cat().resolve_answer(tid).map(|a| (tid, a)));
    let Some((tid, (desc, schema_block, token))) = answer else {
        // No such relation: a successful reply naming none.
        send_msg(peer, ipc::WireMsg::default());
        return Ok(());
    };
    send_msg(
        peer,
        ipc::WireMsg {
            target_id: tid,
            arg0: token,
            schema_block: Some(&schema_block),
            blob: &desc,
            ..Default::default()
        },
    );
    Ok(())
}

/// True when every relation of `sources` committed at or below the last
/// emitted tick's watermark, so no un-ticked commit reaches a view whose
/// source closure they are. Erring towards false costs one extra drain.
fn all_ticked(shared: &Shared, sources: &FxHashSet<u64>) -> bool {
    let ticked = shared.last_tick_lsn.get();
    sources.iter().all(|&s| shared.commit_lsn_of(s) <= ticked)
}

/// The catalog read lock for a read of `targets`, behind a drain when an
/// un-ticked commit reaches one of them. The one read-lock entry of every read
/// verb.
///
/// Only a view has sources, so any other target — an absent one included — is
/// fresh. The guard is dropped across the drain, so a DDL queued on the lock
/// does not wait it out: a caller validates its targets under the guard handed
/// back.
///
/// A failed tick is reported, not swallowed: its views are stale, and serving
/// them under `WireStatus::Ok` is a silent stale read. The tick loop is serial,
/// so the drain also puts the read behind a tick already running.
async fn fresh_read_lock(
    shared: &Rc<Shared>,
    targets: impl IntoIterator<Item = u64>,
    patient: bool,
) -> Result<ReadGuard, WireFault> {
    let g = shared.catalog_rwlock.read().await;
    if all_ticked(shared, &shared.cat().dag.source_closure(targets)) {
        return Ok(g);
    }
    drop(g);
    request_drain(shared, patient).await?;
    Ok(shared.catalog_rwlock.read().await)
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
/// relation. Called under the read's own SAL hold, so every zone at or below
/// the SAL watermark precedes the read's group in the log. A view reflects its
/// last tick.
fn read_watermark(shared: &Shared, kind: RelationKind) -> u64 {
    match kind {
        RelationKind::BaseTable => shared.disp().sal().watermark(),
        _ => shared.last_tick_lsn.get(),
    }
}

/// One scan of [`handle_scan`], resolved under the catalog guard.
struct Scanned {
    kind: RelationKind,
    /// A system catalog family's rows, read off the master's own copy; `None`
    /// for a scan that takes the next lease of the cut.
    local: Option<Rc<Batch>>,
    /// The read's watermark; a fanned scan's is sampled inside the cut.
    lsn: u64,
}

/// SCAN_SPEC and SCAN_MULTI: one reply per scan, in request order. The scans
/// its workers answer share one SAL cut, so an atomic commit is in all of
/// their replies or in none.
async fn handle_scan(shared: &Rc<Shared>, peer: &Peer, scans: &[ScanItem<'_>]) -> Result<(), WireFault> {
    // Every target's kind is resolved under this guard, so a DDL during the
    // drain is caught there and an unknown tid is rejected there.
    let catalog = fresh_read_lock(shared, scans.iter().map(|s| s.target.tid), false).await?;
    let mut scanned: Vec<Scanned> = Vec::with_capacity(scans.len());
    for scan in scans {
        let kind = target_kind(shared, scan.target, Access::Read)?;
        let (local, lsn) = match kind {
            RelationKind::SystemCatalog => {
                let spec = ReadSpec::decode(scan.spec).map_err(|e| format!("decode error: {e}"))?;
                let registry = &shared.cat().registry;
                let rows = registry.scan_spec(scan.target.tid, spec, scan.reply_layout, None)?;
                (Some(rows), read_watermark(shared, kind))
            }
            _ => (None, 0),
        };
        scanned.push(Scanned { kind, local, lsn });
    }
    let leases = if scanned.iter().all(|s| s.local.is_some()) {
        Vec::new()
    } else {
        let fanned = scans.iter().zip(&mut scanned).filter(|(_, s)| s.local.is_none());
        let cut = shared.disp().scan_cut(|cut| {
            for (scan, s) in fanned {
                s.lsn = read_watermark(shared, s.kind);
                cut.read(DirectGroup::new(Read::ScanSpec {
                    tid: scan.target.tid,
                    reply_layout: scan.reply_layout,
                    spec: scan.spec.into(),
                }))?;
            }
            Ok(())
        });
        cut.await?
    };
    // The replies read no catalog state, and a slow client's egress must not
    // hold the catalog read lock.
    drop(catalog);

    let mut leases = leases.iter();
    for (scan, s) in scans.iter().zip(&scanned) {
        let target_id = scan.target.tid;
        match &s.local {
            Some(rows) => send_msg(
                peer,
                ipc::WireMsg {
                    target_id,
                    arg0: s.lsn,
                    data: rows.wire_whole(),
                    ..Default::default()
                },
            ),
            None => {
                // This relation's train, all workers, before the next: within a
                // cut each worker's ring order is request order.
                forward_scan(peer, leases.next().expect("one lease per fanned scan")).await?;
                send_msg(peer, terminal_scan_msg(target_id, s.lsn, 0));
            }
        }
        // Carry no more than the budget into the next reply, and learn here
        // rather than at the end if the client is gone.
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
    // SAFETY: `encode` writes every byte of the payload — a block's header and
    // regions pack end to end — and the length prefix is written immediately
    // below.
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

/// A control-only ACK: the request's own `target_id`, and its one value — a
/// write's LSN or an allocation's base id — in `arg0`.
pub(super) fn send_ack(peer: &Peer, target_id: u64, value: u64) {
    send_msg(
        peer,
        ipc::WireMsg {
            target_id,
            arg0: value,
            ..Default::default()
        },
    )
}

/// A failure carrying its own status, master-minted or forwarded from a worker.
/// A failure describes no rows, so it carries no schema block.
fn send_fault(peer: &Peer, target_id: u64, fault: &WireFault) {
    send_msg(peer, ipc::WireMsg { target_id, ..ipc::WireMsg::fault(fault) })
}

/// Refuse a push a stream cannot accept; `batch` is `None` for an empty delta.
/// Both rules restate what a stream lacks — a unique primary key, and any
/// retraction at all — and rejecting `Error` mode is what keeps
/// `push_reads_committed_state` false, and with it the shared table lock and a
/// validator that would probe a stream's absent store.
fn check_stream_push(target_id: u64, batch: Option<&Batch>, mode: gnitz_wire::WireConflictMode) -> Result<(), String> {
    if mode == gnitz_wire::WireConflictMode::Error {
        return Err(format!(
            "table {target_id} is a stream: conflict mode 'error' asserts a primary-key \
             uniqueness a stream does not have"
        ));
    }
    let Some(batch) = batch.filter(|b| !b.all_weights_positive()) else {
        return Ok(());
    };
    // Located again only to name it in the message.
    let i = (0..batch.len())
        .find(|&i| batch.get_weight(i) <= 0)
        .expect("just found one");
    Err(format!(
        "table {target_id} is a stream: a stream is append-only, but row {i} of this push carries \
         weight {}",
        batch.get_weight(i)
    ))
}

#[cfg(test)]
#[path = "tests/executor.rs"]
mod tests;
