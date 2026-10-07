//! Server executor: the process lifecycle, the `Shared` state, the request
//! router and every read/push handler, and the reply-frame vocabulary. The
//! catalog-zone write path is the child `ddl`.
//!
//! The master owns one `Reactor`: `ServerExecutor::run` spawns its tasks on it
//! and races the signal loop against a worker's death.
//!
//! A request handler rejects by returning `Err`, and the router sends it: no
//! handler writes a fault frame for its request as a whole.

mod ddl;

use std::cell::{Cell, RefCell};
use std::os::fd::{AsFd, OwnedFd};
use std::rc::Rc;
use std::time::{Duration, Instant};

use rustc_hash::{FxHashMap, FxHashSet};

use super::guard_panic;
use gnitz_foundation::fault::Seam;

use self::ddl::{handle_ddl_txn, hold_tick_for_ddl, reserve_serial_range, TICK_HOLD_FOR_DDL};
use super::TxnFamily;
use crate::catalog::CatalogEngine;
use crate::runtime::committer::{self, BarrierKind, CommitRequest, PendingPush, PendingTxn};
use crate::runtime::listen::ClientListener;
use crate::runtime::master::{forward_scan, MasterDispatcher, WORKER_WATCH};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{chan, oneshot, select2, AsyncRwLock, Either, ReadGuard, RecvBuf, WriteGuard};
use crate::runtime::sal::{DirectGroup, Read};
use crate::runtime::wire as ipc;
use gnitz_store::relation::{Relation, RelationKind};
use gnitz_wire::control::{ControlHeader, DecodedControl, Target};
use gnitz_wire::txn_frame::DeltaPollItem;
use gnitz_wire::{ReadBound, ReadSpec, WireFault, WireStatus};
use gnitz_zset::repr::Batch;

const TICK_COALESCE_ROWS: usize = 10_000;

/// Views one DELTA_POLL reads at one SAL cut: the ceiling on the leases and
/// reply trains a poll puts on the master at a time. A poll naming more is
/// answered a slice at a time.
const DELTA_POLL_CUT_VIEWS: usize = 64;

/// The longest a DELTA_POLL is held, whatever wait it asks for.
const DELTA_POLL_MAX_WAIT: Duration = Duration::from_secs(3600);

/// The least time between a tick and one only waiting polls ask for: such a
/// poll is re-issued the moment it is answered, so unpaced it would tick per
/// commit.
const PATIENT_TICK_GAP: Duration = Duration::from_millis(10);

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
        /// Asked for by a waiting poll: held to [`PATIENT_TICK_GAP`] unless a
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

/// The DELTA_POLLs held back with nothing to report, each with the relations
/// whose change ends its wait.
#[derive(Default)]
struct PollWaiters {
    next_id: Cell<u64>,
    parked: RefCell<Vec<ParkedPoll>>,
}

struct ParkedPoll {
    id: u64,
    watched: FxHashSet<u64>,
    wake: oneshot::Sender<()>,
}

/// One poll's place among the [`PollWaiters`], given up on drop.
struct Parked<'a> {
    waiters: &'a PollWaiters,
    id: u64,
    woken: oneshot::Receiver<()>,
}

impl PollWaiters {
    fn park(&self, watched: FxHashSet<u64>) -> Parked<'_> {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        let (wake, woken) = oneshot::channel();
        self.parked.borrow_mut().push(ParkedPoll { id, watched, wake });
        Parked { waiters: self, id, woken }
    }

    /// Wake every poll watching `relation`.
    fn wake(&self, relation: u64) {
        let mut parked = self.parked.borrow_mut();
        for poll in parked.extract_if(.., |p| p.watched.contains(&relation)) {
            poll.wake.send(());
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
    /// The SAL watermark the last completed tick snapshotted: every commit at or
    /// below it is reflected in every view.
    last_tick_lsn: Cell<u64>,
    /// Tables with a pending delta, each with the row count feeding the tick
    /// threshold.
    tick_rows: RefCell<FxHashMap<u64, usize>>,
    poll_waiters: PollWaiters,
    /// Per-table write serialization. A push whose validation reads committed
    /// state (`push_reads_committed_state`) and every transaction take the write
    /// guard; a push that reads no committed state takes the read guard, so
    /// same-table pushes reach the committer concurrently and share one fsync.
    table_locks: RefCell<FxHashMap<u64, AsyncRwLock>>,
    /// Set true by the signal loop before it sends the final Shutdown barrier.
    /// Read only by [`Shared::commit`], which is what makes the test and the send
    /// one step.
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

    /// Commit `req` unless a graceful shutdown has begun, await its verdict under
    /// the caller's catalog read guard, and on success raise each of `tids`' commit
    /// LSN to it. No await separates the test from the send: a request that saw a
    /// live server is queued ahead of the signal loop's Shutdown barrier.
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
            // After the raise, which is what a woken poll reads as staleness.
            self.poll_waiters.wake(tid);
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
        self.poll_waiters.wake(id);
        self.table_locks.borrow_mut().remove(&id);
        self.table_commit_lsn.borrow_mut().remove(&id);
        self.disp().forget_delta_round(id);
        self.disp().unique_filter_invalidate_table(id);
    }

    /// Credit `rows` against each tid's pending-tick count and fire the auto-tick
    /// if any tid now stands at or above the coalesce threshold. Below it a push
    /// only accumulates, and nothing ticks until a read or a delta poll asks for
    /// a drain.
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
            poll_waiters: PollWaiters::default(),
            table_locks: RefCell::new(FxHashMap::default()),
            draining: Cell::new(false),
            table_commit_lsn: RefCell::new(FxHashMap::default()),
            boot_seed,
            data_dir: data_dir.to_string(),
            hello_timeout: Duration::from_millis(gnitz_foundation::env::env_num(
                "GNITZ_HELLO_TIMEOUT_MS",
                (gnitz_wire::CONNECT_TIMEOUT * 3 / 2).as_millis() as u64,
            )),
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

/// One message handled to completion before the next is received, so replies
/// leave in request order — which is how clients correlate them (`gnitz.aio`
/// gathers a mixed group onto one round-trip and rejects an out-of-order
/// `target_id`). Spawning `handle_message` to overlap requests would break that.
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

    while let Some(buf) = peer.next_request().await {
        handle_message(peer, buf, shared).await;
    }
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
/// one only patient drains ask for, to [`PATIENT_TICK_GAP`] after the last.
///
/// A failure in one trigger fails only that trigger; SAL emission is further
/// guarded by `guard_panic` inside `run_tick`.
async fn tick_loop(shared: Rc<Shared>, mut rx: chan::Receiver<TickTrigger>) {
    // The batch's `Drain` repliers, held across the tick they are waiting on.
    let mut dones: Vec<oneshot::Sender<Result<(), WireFault>>> = Vec::new();
    // Reused across every tick; `drain_live_tick_rows_into` clears it before
    // refilling so capacity is retained.
    let mut tids_scratch: Vec<u64> = Vec::new();
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
        patient_from = Instant::now() + PATIENT_TICK_GAP;
    }
}

/// Emit one Tick group for every `tid` and await the per-worker ACKs.
async fn run_tick(shared: &Rc<Shared>, tids: &[u64]) -> Result<(), WireFault> {
    // Snapshot in the same step as the caller's drain of `tick_rows`: the
    // committer queues a commit's tids as it lays the commit out, so every
    // commit at or below the snapshot was drained by this tick or an earlier
    // one. A later snapshot could cover a commit this tick never took.
    let snapshot_lsn = shared.disp().sal().watermark();
    if tids.is_empty() {
        // Nothing pending: an earlier completed tick already took every commit
        // at or below the snapshot. The watermark still advances, so a drain a
        // reader waits on brings `last_tick_lsn` up to the moment it ran.
        shared.last_tick_lsn.set(snapshot_lsn);
        return Ok(());
    }

    let emit = {
        let excl = shared.disp().sal().lock().await;
        guard_panic("tick", || {
            if TICK_EMIT_ERROR.take_once() {
                return Err("injected tick emit error".into());
            }
            shared.disp().emit_tick(&excl, tids)
        })
    };
    // A refused write publishes nothing, so no worker took any tid's delta.
    let lease = match emit {
        Ok(lease) => lease,
        Err(e) => {
            shared.requeue_tick_tids(tids);
            return Err(e);
        }
    };

    if TICK_HOLD_FOR_DDL.take_once() {
        hold_tick_for_ddl(shared).await;
    }
    lease.acks().await;
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
        ClientVerb::DeltaPoll => handle_delta_poll(shared, peer, &ctrl.hdr, &data[ctrl.body]).await,

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
            let (target, reply_layout) = (ctrl.hdr.target(), ctrl.hdr.arg0);
            handle_scan_spec(shared, peer, target, &data[ctrl.blob.clone()], reply_layout).await
        }

        // A plain read guard, not `read_lock`: a resolve answers catalog shape,
        // and a view tick moves a view's rows, never its shape — so the tick
        // drain `read_lock` waits for buys nothing here.
        ClientVerb::Resolve => {
            let _g = shared.catalog_rwlock.read().await;
            build_resolve_reply(shared, peer, &data[ctrl.blob.clone()])
        }

        ClientVerb::Push => handle_push(shared, peer, buf, ctrl).await,
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

    // A push that reads no committed state cannot be invalidated by a concurrent
    // one, so it shares its table's lock and reaches the committer alongside
    // other pushes to the same table, which fold into one SAL zone and one
    // fsync. Every other push is a one-family transaction: it takes its whole
    // write lock set exclusively and commits whatever its deletes cascade to.
    let zone_lsn = if shared.cat().push_reads_committed_state(target_id, mode) {
        let locks = shared.cat().write_lock_set(target_id, !batch.all_weights_positive());
        let held = shared.lock_tables_exclusive(locks).await;
        let families = vec![TxnFamily { tid: target_id, mode, batch }];
        commit_validated(shared, &catalog, &held, families).await?
    } else {
        let held = shared.lock_table_shared(target_id).await;
        let push = |done| {
            CommitRequest::Push(PendingPush {
                tid: target_id,
                batch,
                recoverable: !is_stream,
                done,
            })
        };
        shared.commit(&catalog, &held, [target_id], push).await?
    };
    // A stream replies `0`: its push is not durable, and an ACK reports an LSN
    // only for a write a restart must recover. Keyed on the target rather than on
    // whether this batch happened to open a zone, so a stream push the committer
    // coalesced with a base-table push still answers `0`.
    let reply_lsn = if is_stream { 0 } else { zone_lsn };
    send_ack(peer, target_id, reply_lsn);
    Ok(())
}

/// Validate `families` as one bundle and commit it, with the families its
/// deletes cascade to, as one zone. `held` is its tables' whole write lock sets.
async fn commit_validated(
    shared: &Rc<Shared>,
    catalog: &ReadGuard,
    held: &HeldTables,
    mut families: Vec<TxnFamily>,
) -> Result<u64, WireFault> {
    shared.disp().validate_txn_distributed(&mut families).await?;
    let tids: Vec<u64> = families.iter().map(|f| f.tid).collect();
    shared
        .commit(catalog, held, tids, |done| {
            CommitRequest::Txn(PendingTxn { families, done })
        })
        .await
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
    let lsn = commit_validated(shared, &catalog, &held, families).await?;
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
    /// A read with only a fan-out realization, which a system catalog family has
    /// no form of: every worker holds a full copy, so fanning one out would
    /// concatenate W identical trains and inflate every row's weight W-fold.
    UserRead,
    Write,
    /// A write inside a `PUSH_TXN`. Refusing a stream keeps every family
    /// `recoverable`, so a transaction always opens a zone.
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
        Access::Read | Access::UserRead if kind == RelationKind::Stream => {
            Err(format!("table {target_id} is a stream: a stream holds no rows and cannot be read").into())
        }
        Access::UserRead if kind == RelationKind::SystemCatalog => {
            Err(format!("table {target_id} is a system catalog family: this read has only a fan-out form").into())
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
/// completed tick's watermark, so no un-ticked commit reaches a view whose
/// source closure they are.
///
/// Sound because the committer queues a commit's tids in `tick_rows` in the
/// step that lays the commit out (see `run_tick`'s snapshot); a *missing* map
/// entry is the `boot_seed` argument. Erring towards false is harmless (one
/// extra drain).
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

/// [`fresh_read_lock`] for one target, with its kind resolved from the same
/// probe that validated it: each single-target read verb passes the `Access`
/// its realization can serve and routes on the returned kind, rather than
/// re-deciding the system/user split from the id.
async fn read_lock(
    shared: &Rc<Shared>,
    target: Target,
    access: Access,
) -> Result<(ReadGuard, RelationKind), WireFault> {
    let g = fresh_read_lock(shared, [target.tid], false).await?;
    let kind = target_kind(shared, target, access)?;
    Ok((g, kind))
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

/// One scan-shaped fan-out of a `kind` relation: the cut under `guard`, then the
/// reply train without it — a slow client's egress must not hold the catalog
/// read lock. Answers the read's watermark, sampled inside the cut.
async fn fan_out_scan(
    shared: &Rc<Shared>,
    peer: &Peer,
    guard: ReadGuard,
    kind: RelationKind,
    read: Read<'_>,
) -> Result<u64, WireFault> {
    let mut lsn = 0;
    let mut leases = shared
        .disp()
        .scan_cut(|cut| {
            lsn = read_watermark(shared, kind);
            cut.read(DirectGroup::new(read))
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

/// SCAN_SPEC: one relation, replied in the client's layout `reply_layout`.
async fn handle_scan_spec(
    shared: &Rc<Shared>,
    peer: &Peer,
    target: Target,
    blob: &[u8],
    reply_layout: u64,
) -> Result<(), WireFault> {
    let target_id = target.tid;
    let (g, kind) = read_lock(shared, target, Access::Read).await?;
    if kind == RelationKind::SystemCatalog {
        let spec = ReadSpec::decode(blob).map_err(|e| format!("decode error: {e}"))?;
        let rows = guard_panic("read", || {
            shared.cat().registry.scan_spec(target_id, spec, reply_layout, None)
        })?;
        send_msg(
            peer,
            ipc::WireMsg {
                target_id,
                arg0: read_watermark(shared, kind),
                data: rows.wire_whole(),
                ..Default::default()
            },
        );
        return Ok(());
    }
    let read = Read::ScanSpec {
        tid: target_id,
        reply_layout,
        spec: blob.into(),
    };
    let result = fan_out_scan(shared, peer, g, kind, read).await;
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

/// Where a poll of `item` stands.
fn poll_position(shared: &Shared, _catalog: &ReadGuard, item: DeltaPollItem) -> PollPosition {
    let tid = item.view.tid;
    match target_kind(shared, item.view, Access::UserRead) {
        Err(f) => PollPosition::Fault(f),
        // A relation with no feed handed out no cursor to compare.
        Ok(kind)
            if kind.has_delta_feed()
                && item.after_tick > 0
                && item.tag != shared.disp().delta_cursor_tag(tid, item.spec) =>
        {
            PollPosition::Fault(WireFault {
                status: WireStatus::DeltaExpired,
                text: format!("delta cursor of relation {tid} names another boot, relation or spec; re-read at 0"),
            })
        }
        Ok(_) if delta_up_to_date(shared, tid, item.after_tick) => PollPosition::UpToDate,
        Ok(_) => PollPosition::Moved,
    }
}

/// DELTA_POLL: advance N mirrored views in one request, holding the reply up to
/// its wait while none of them has anything to report.
///
/// A view the rounds since its cursor left no rows for has nothing to report,
/// whether no round reached it or its spec kept none of them. Where no view has
/// anything and wait is left, the poll parks again behind the rounds it read.
/// The cursor that moves across a park is this handler's own: the client's is
/// the one the terminal finally carries.
///
/// An `Err` rejects the frame, at `target_id = 0`; a per-view failure is not an
/// `Err` — it goes out as that view's own fault frame, and the rest of the poll
/// continues.
async fn handle_delta_poll(
    shared: &Rc<Shared>,
    peer: &Peer,
    prologue: &ControlHeader,
    body: &[u8],
) -> Result<(), WireFault> {
    let (wait_ms, mut views) =
        gnitz_wire::txn_frame::decode_delta_poll(prologue, body).map_err(|e| format!("decode error: {e}"))?;
    let deadline = Instant::now() + Duration::from_millis(wait_ms).min(DELTA_POLL_MAX_WAIT);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let (lock, may_hold) = delta_poll_lock(shared, peer, &views, left).await?;
        let Some(rounds) = delta_poll_pass(shared, peer, &views, lock, may_hold).await? else {
            return Ok(());
        };
        // No row of a view lies between its cursor and the round it was read
        // through, so that round is where the next pass reads it from.
        for (view, round) in views.iter_mut().zip(rounds) {
            view.tag = shared.disp().delta_cursor_tag(view.view.tid, view.spec);
            view.after_tick = round;
        }
    }
}

/// Answer `views` once, the first slice under `lock`. With `may_hold`, a view
/// with nothing to report has its terminal kept back until one that has
/// something releases it; where none has, no terminal goes out and the round
/// each view was read through is returned. `None` once the poll is answered, or
/// the client is gone.
async fn delta_poll_pass(
    shared: &Rc<Shared>,
    peer: &Peer,
    views: &[DeltaPollItem<'_>],
    lock: ReadGuard,
    may_hold: bool,
) -> Result<Option<Vec<u64>>, WireFault> {
    let disp = shared.disp();
    let terminal = |item: &DeltaPollItem, result: Result<u64, WireFault>| {
        let tag = disp.delta_cursor_tag(item.view.tid, item.spec);
        finish_scan_fanout(peer, item.view.tid, tag, result);
    };
    // The poll drained once, for `lock`; a later slice takes the lock alone.
    let mut first_lock = Some(lock);
    // The round each view so far was read through, while every one of them had
    // nothing to report. `None` once a terminal has gone out.
    let mut held: Option<Vec<u64>> = may_hold.then(Vec::new);

    // One slice at a time: one catalog lock and — for however many of its views
    // moved — one broadcast.
    for slice in views.chunks(DELTA_POLL_CUT_VIEWS) {
        // ── Phase 1: classify under the catalog lock, dispatch one cut ─────
        // No await between a view's position and the round an up-to-date one
        // reports, so no tick lands in between.
        let catalog = match first_lock.take() {
            Some(g) => g,
            None => shared.catalog_rwlock.read().await,
        };
        let positions: Vec<PollPosition> = slice.iter().map(|&v| poll_position(shared, &catalog, v)).collect();
        let up_to_date_round = disp.last_tick_round();
        let moved = || {
            let judged = slice.iter().zip(&positions);
            judged.filter_map(|(v, p)| matches!(p, PollPosition::Moved).then_some(v))
        };
        let mut dispatch_round = 0;
        let dispatches = if moved().next().is_none() {
            Vec::new()
        } else {
            disp.scan_cut(|cut| {
                dispatch_round = disp.last_tick_round();
                for item in moved() {
                    cut.read(DirectGroup::new(Read::delta(
                        item.view.tid,
                        item.after_tick,
                        dispatch_round,
                        item.spec,
                        item.reply_layout,
                    )))?;
                }
                Ok(())
            })
            .await?
        };
        // Phase 2 reads no catalog state, and holding the lock across the
        // forward would block DDL.
        drop(catalog);

        // ── Phase 2: one terminal per view, in request order ───────────────
        // Taken in step with the `Moved`s that were pushed. A dispatch left
        // undrained — an earlier return dropped it — discards the rest of its
        // train at the ring boundary.
        let mut dispatches = dispatches.into_iter();
        for (item, position) in slice.iter().zip(positions) {
            let mut lease = None;
            // The round the view's terminal carries, and the first frame of
            // its rows.
            let report = match position {
                PollPosition::Fault(fault) => Err(fault),
                PollPosition::UpToDate => Ok((up_to_date_round, None)),
                PollPosition::Moved => {
                    let lease = lease.insert(dispatches.next().expect("one dispatch per moved view"));
                    // A train with no rows is read to its end here, so it pins
                    // nothing of the ring while a later view's is read.
                    lease.next().await.map(|first| (dispatch_round, first))
                }
            };
            if let (Some(held), Ok((round, None))) = (&mut held, &report) {
                held.push(*round);
                continue;
            }
            for (view, round) in views.iter().zip(held.take().into_iter().flatten()) {
                terminal(view, Ok(round));
            }
            let result = match (report, &lease) {
                (Ok((round, Some(first))), Some(lease)) => match peer.send(first.slot).await {
                    Ok(()) => forward_scan(peer, lease).await.map(|()| round),
                    Err(_) => return Ok(None),
                },
                (report, _) => report.map(|(round, _)| round),
            };
            terminal(item, result);
            // Carry no more than the budget into the next view, and learn here
            // rather than at the end if the client is gone.
            if peer.flush_if_full().await.is_err() {
                return Ok(None);
            }
        }
    }
    Ok(held)
}

/// The catalog read lock a poll of `views` is answered under, and whether a
/// view with nothing to report may still be held: behind a drain when a commit
/// reaching one of them has not been ticked, and, with a `wait`, after holding
/// the poll while none of them has anything to report — until a relation one
/// of them reads changes, `wait` passes, or the client sends its next request
/// or goes. Only the first of those leaves the poll one that may be held on.
async fn delta_poll_lock(
    shared: &Rc<Shared>,
    peer: &Peer,
    views: &[DeltaPollItem<'_>],
    wait: Duration,
) -> Result<(ReadGuard, bool), WireFault> {
    let ids = || views.iter().map(|v| v.view.tid);
    let waiting = !wait.is_zero();
    let g = fresh_read_lock(shared, ids(), waiting).await?;
    if !waiting {
        return Ok((g, false));
    }
    // No await from the test to the park, so no commit is acknowledged between
    // them. The drain above was one: a commit it did not take is un-ticked here.
    let mut watched = shared.cat().dag.source_closure(ids());
    let quiet = all_ticked(shared, &watched)
        && views
            .iter()
            .all(|&v| matches!(poll_position(shared, &g, v), PollPosition::UpToDate));
    if !quiet {
        return Ok((g, true));
    }
    // A commit or a drop reaches a view through the view or anything it reads.
    watched.extend(ids());
    let mut parked = shared.poll_waiters.park(watched);
    drop(g);
    let released = select2(shared.disp().reactor().sleep(wait), peer.next_request_ready());
    let woken = matches!(select2(&mut parked.woken, released).await, Either::A(_));
    drop(parked);
    Ok((fresh_read_lock(shared, ids(), true).await?, woken))
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
            .is_some_and(|r| r.kind().has_delta_feed())
        && after_tick >= shared.disp().last_delta_round(target_id)
}

/// One relation's Phase-1 capture for `handle_scan_multi`, carried to the
/// deferred Phase-2 emit.
struct ScanMultiRelPlan {
    tid: u64,
    reply_layout: u64,
    kind: RelationKind,
    /// The read's watermark, sampled inside the cut.
    lsn: u64,
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

    // Phase 1 resolves every tid's kind under this guard, so a DDL during the
    // drain is caught there and an unknown tid is rejected there.
    let cat = fresh_read_lock(shared, relations.iter().map(|r| r.tid), false).await?;

    // ── Phase 1: catalog lock — resolve shapes + schemas, dispatch one cut ──
    // One catalog snapshot for every relation, one SAL cut for every group.
    let (dispatches, plans) = {
        let _cat = cat;
        let mut plans: Vec<ScanMultiRelPlan> = Vec::with_capacity(relations.len());
        for r in &relations {
            let kind = target_kind(shared, r.tid.into(), Access::UserRead)?;
            plans.push(ScanMultiRelPlan {
                tid: r.tid,
                reply_layout: r.reply_layout,
                kind,
                lsn: 0,
            });
        }
        let spec = ReadSpec::all_rows(ReadBound::None).encode();
        let disp = shared.disp();
        let dispatches = disp
            .scan_cut(|cut| {
                for plan in &mut plans {
                    plan.lsn = read_watermark(shared, plan.kind);
                    cut.read(DirectGroup::new(Read::ScanSpec {
                        tid: plan.tid,
                        reply_layout: plan.reply_layout,
                        spec: spec.as_slice().into(),
                    }))?;
                }
                Ok(())
            })
            .await?;
        // Release the catalog read lock here: Phase 2 touches no catalog state
        // (the snapshot is worker-frozen), so holding it across the whole bulk
        // read would needlessly block DDL.
        (dispatches, plans)
    };

    // ── Phase 2: sequential per-relation drain (no locks; holds all leases) ──
    for (plan, d) in plans.iter().zip(&dispatches) {
        // Drain this relation's train (all workers, ascending) before the next —
        // within a cut each worker's ring order is request order.
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
