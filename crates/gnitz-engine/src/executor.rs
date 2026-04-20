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
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::catalog::{
    CatalogEngine, FIRST_USER_TABLE_ID, MIGRATIONS_TAB_ID,
    SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES,
    MIGRATIONS_COL_PARENT_HASH, MIGRATIONS_COL_AUTHOR, MIGRATIONS_COL_MESSAGE,
    MIGRATIONS_COL_CREATED_LSN, MIGRATIONS_COL_DESIRED_STATE_SQL,
    MIGRATIONS_COL_DESIRED_STATE_CANONICAL, MIGRATIONS_COL_FORMAT_VERSION,
};
use crate::catalog::migration::MigrationRow;
use gnitz_wire::migration as mig;
use crate::committer::{self, CommitRequest};
use crate::ipc::{self, STATUS_OK, STATUS_ERROR, WireConflictMode};
use crate::master::{MasterDispatcher, first_worker_error};
use crate::reactor::{
    AsyncMutex, AsyncRwLock, Either, PendingRelay, Reactor, mpsc, oneshot, select2,
};
use crate::schema::SchemaDescriptor;
use crate::storage::Batch;
use crate::util::guard_panic;

const TICK_COALESCE_ROWS: usize = 10_000;
const TICK_DEADLINE_MS: u64 = 20;
const WORKER_WATCH_MS: u64 = 100;

const FLAG_ALLOCATE_TABLE_ID: u64  = 1;
const FLAG_ALLOCATE_SCHEMA_ID: u64 = 2;
const FLAG_SEEK: u64               = 128;
const FLAG_SEEK_BY_INDEX: u64      = 256;
const FLAG_ALLOCATE_INDEX_ID: u64  = 512;

/// Send+Sync wrapper so raw pointers can cross the `Waker` boundary.
/// The master process is single-threaded; this is a compile-time
/// shim, not runtime sharing.
#[derive(Clone, Copy)]
struct DispPtr(*mut MasterDispatcher);
unsafe impl Send for DispPtr {}
unsafe impl Sync for DispPtr {}
#[derive(Clone, Copy)]
struct CatPtr(*mut CatalogEngine);
unsafe impl Send for CatPtr {}
unsafe impl Sync for CatPtr {}

/// One tick request to `tick_loop_async`.
pub enum TickTrigger {
    /// Fire-and-forget trigger from INSERT when a tid crosses the row
    /// coalesce threshold.  Tids come from `tick_rows` / `tick_tids`.
    Auto,
    /// Explicit drain requested by SCAN: forces the listed tids to tick
    /// even if their `tick_rows` counter is empty, then signals `done`.
    Drain {
        tids: Vec<i64>,
        done: oneshot::Sender<()>,
    },
}

/// Shared executor state held by every task.
pub struct Shared {
    pub reactor: Rc<Reactor>,
    catalog: CatPtr,
    dispatcher: DispPtr,
    sal_fd: i32,
    committer_tx: mpsc::Sender<CommitRequest>,
    catalog_rwlock: Rc<AsyncRwLock>,
    /// SAL-writer exclusivity (III.3b). Held by committer (checkpoint +
    /// commit emission), tick task (per-tid emission), relay task
    /// (FLAG_EXCHANGE_RELAY), and DDL (broadcast_ddl + fsync).  Replaces
    /// the deleted `TickIdleBarrier` semantics.
    sal_writer_excl: Rc<AsyncMutex<()>>,
    /// Tick trigger sender; senders include INSERT (auto-trigger on
    /// threshold cross) and SCAN (explicit drain).
    tick_tx: mpsc::Sender<TickTrigger>,
    /// Clone of the relay channel sender. The reactor's exchange
    /// accumulator also holds a sender (installed via
    /// `attach_relay_tx`); this copy is reserved for `apply_migration`
    /// to enqueue a `PendingRelay::Fence` between Phase 2 and Phase 3.
    pub(crate) relay_tx: mpsc::Sender<PendingRelay>,
    /// Set by `apply_migration` from `false` to `true` via CAS on
    /// entry; cleared on exit (including panic) via a scope guard.
    /// While set, `handle_message` rejects external DML and external
    /// DDL pushes with a transient "another migration in flight"
    /// error. Migration pushes (target_id == MIGRATIONS_TAB_ID) are
    /// NOT gated here — their own CAS handles the "already running"
    /// case.
    pub(crate) migration_in_flight: AtomicBool,
    /// Committer-owned counter; shared here so SCAN/SEEK handlers can
    /// report the same LSN the committer assigns.
    ingest_lsn: Rc<Cell<u64>>,
    last_tick_lsn: Rc<Cell<u64>>,
    /// Per-table row counter feeding the tick threshold.
    tick_rows: Rc<RefCell<HashMap<i64, usize>>>,
    tick_tids: Rc<RefCell<Vec<i64>>>,
    t_last_push: Rc<Cell<Option<Instant>>>,
    table_locks: RefCell<HashMap<i64, Rc<AsyncMutex<()>>>>,
    schema_cache: RefCell<HashMap<i64, SchemaDescriptor>>,
    col_names_cache: RefCell<HashMap<i64, Vec<Vec<u8>>>>,
}

impl Shared {
    fn cat(&self) -> &mut CatalogEngine { unsafe { &mut *self.catalog.0 } }
    fn disp(&self) -> &mut MasterDispatcher { unsafe { &mut *self.dispatcher.0 } }

    fn get_schema_desc(&self, target_id: i64) -> SchemaDescriptor {
        if let Some(&s) = self.schema_cache.borrow().get(&target_id) { return s; }
        let desc = self.cat().get_schema_desc(target_id)
            .unwrap_or_else(SchemaDescriptor::minimal_u64);
        self.schema_cache.borrow_mut().insert(target_id, desc);
        desc
    }

    fn get_col_names_bytes(&self, target_id: i64) -> std::cell::Ref<'_, Vec<Vec<u8>>> {
        {
            let mut cache = self.col_names_cache.borrow_mut();
            if !cache.contains_key(&target_id) {
                let names = self.cat().get_column_names(target_id);
                let bytes: Vec<Vec<u8>> = names.into_iter().map(String::into_bytes).collect();
                cache.insert(target_id, bytes);
            }
        }
        std::cell::Ref::map(self.col_names_cache.borrow(), |m| m.get(&target_id).unwrap())
    }

    /// Drop all per-tid caches for `target_id`. Must be called after a
    /// DROP TABLE so a subsequent operation on a fresh table (even one
    /// that reuses the tid) doesn't see a stale schema/column list.
    fn invalidate_tid_caches(&self, target_id: i64) {
        self.schema_cache.borrow_mut().remove(&target_id);
        self.col_names_cache.borrow_mut().remove(&target_id);
        self.table_locks.borrow_mut().remove(&target_id);
    }

    fn table_lock(&self, tid: i64) -> Rc<AsyncMutex<()>> {
        let mut locks = self.table_locks.borrow_mut();
        if let Some(l) = locks.get(&tid) { return Rc::clone(l); }
        let l = Rc::new(AsyncMutex::new(()));
        locks.insert(tid, Rc::clone(&l));
        l
    }

    fn needs_table_lock(&self, tid: i64, mode: WireConflictMode) -> bool {
        let cat = self.cat();
        cat.get_fk_count(tid) > 0
            || cat.get_fk_children_count(tid) > 0
            || cat.has_any_unique_index(tid)
            || (matches!(mode, WireConflictMode::Error) && cat.table_has_unique_pk(tid))
    }

    fn bump_tick_rows(&self, tid: i64, rows: usize) {
        let mut tr = self.tick_rows.borrow_mut();
        let entry = tr.entry(tid).or_insert(0);
        if *entry == 0 {
            self.tick_tids.borrow_mut().push(tid);
        }
        *entry += rows;
        self.t_last_push.set(Some(Instant::now()));
    }

    /// True iff some pending tid has crossed the row coalesce threshold.
    /// Used by the INSERT handler to decide whether to send an immediate
    /// tick trigger, and by the tick task to skip the deadline window.
    fn any_threshold_crossed(&self) -> bool {
        self.tick_rows.borrow().values().any(|&rows| rows >= TICK_COALESCE_ROWS)
    }

    /// Drain `tick_rows` and `tick_tids`, returning the union of pending
    /// tids in stable insertion order.
    fn drain_tick_rows(&self) -> Vec<i64> {
        let mut tids = self.tick_tids.borrow_mut();
        let v = std::mem::take(&mut *tids);
        self.tick_rows.borrow_mut().clear();
        self.t_last_push.set(None);
        v
    }
}

/// RAII clear-on-drop guard for `Shared::migration_in_flight`. Takes
/// ownership on successful CAS; clears the flag when dropped — normal
/// return, early error, or panic unwind. Use
/// `MigrationInFlightGuard::acquire(&shared)` to CAS and install.
pub(crate) struct MigrationInFlightGuard {
    shared: Rc<Shared>,
}

impl MigrationInFlightGuard {
    /// Try to acquire the flag. Returns `Some(guard)` iff this caller
    /// won the CAS. Returns `None` if a migration is already in flight.
    pub(crate) fn acquire(shared: &Rc<Shared>) -> Option<Self> {
        match shared.migration_in_flight.compare_exchange(
            false, true, Ordering::AcqRel, Ordering::Acquire,
        ) {
            Ok(_) => Some(MigrationInFlightGuard { shared: Rc::clone(shared) }),
            Err(_) => None,
        }
    }
}

impl Drop for MigrationInFlightGuard {
    fn drop(&mut self) {
        self.shared.migration_in_flight.store(false, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// ServerExecutor entry point
// ---------------------------------------------------------------------------

pub struct ServerExecutor;

impl ServerExecutor {
    pub fn run(
        catalog: *mut CatalogEngine,
        dispatcher: *mut MasterDispatcher,
        server_fd: i32,
    ) -> i32 {
        let reactor = match Reactor::new(256) {
            Ok(r) => Rc::new(r),
            Err(e) => {
                eprintln!("io_uring init failed: {}", e);
                return -1;
            }
        };
        let sal_fd = unsafe { &*dispatcher }.sal_fd();
        let num_workers = unsafe { &*dispatcher }.num_workers();

        reactor.attach_w2m(unsafe { &*dispatcher }.w2m_handle());
        reactor.attach_server_fd(server_fd);

        let sal_writer_excl = Rc::new(AsyncMutex::new(()));
        let ingest_lsn = Rc::new(Cell::new(0u64));
        let last_tick_lsn = Rc::new(Cell::new(0u64));
        let tick_rows: Rc<RefCell<HashMap<i64, usize>>> = Rc::new(RefCell::new(HashMap::new()));
        let tick_tids: Rc<RefCell<Vec<i64>>> = Rc::new(RefCell::new(Vec::new()));
        let t_last_push = Rc::new(Cell::new(None));

        let (committer_tx, committer_rx) = mpsc::unbounded::<CommitRequest>();
        let (tick_tx, tick_rx) = mpsc::unbounded::<TickTrigger>();
        let (relay_tx, relay_rx) = mpsc::unbounded::<PendingRelay>();
        // Clone the sender so `apply_migration` can enqueue a Fence on
        // the same channel between Phase 2 and Phase 3.
        let relay_tx_for_shared = relay_tx.clone();
        // Wire the relay channel into the reactor so route_reply's
        // FLAG_EXCHANGE accumulator can hand off completed views.
        reactor.attach_relay_tx(relay_tx);

        let committer_shared = Rc::new(committer::Shared {
            reactor: Rc::clone(&reactor),
            disp_ptr: dispatcher,
            sal_fd,
            sal_writer_excl: Rc::clone(&sal_writer_excl),
            ingest_lsn: Rc::clone(&ingest_lsn),
            num_workers,
        });
        let shared = Rc::new(Shared {
            reactor: Rc::clone(&reactor),
            catalog: CatPtr(catalog),
            dispatcher: DispPtr(dispatcher),
            sal_fd,
            committer_tx,
            catalog_rwlock: Rc::new(AsyncRwLock::new()),
            sal_writer_excl: Rc::clone(&sal_writer_excl),
            tick_tx,
            relay_tx: relay_tx_for_shared,
            migration_in_flight: AtomicBool::new(false),
            ingest_lsn: Rc::clone(&ingest_lsn),
            last_tick_lsn: Rc::clone(&last_tick_lsn),
            tick_rows: Rc::clone(&tick_rows),
            tick_tids: Rc::clone(&tick_tids),
            t_last_push: Rc::clone(&t_last_push),
            table_locks: RefCell::new(HashMap::new()),
            schema_cache: RefCell::new(HashMap::new()),
            col_names_cache: RefCell::new(HashMap::new()),
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
        if fd < 0 { continue; }
        shared.reactor.register_conn(fd);
        let s = Rc::clone(&shared);
        shared.reactor.spawn(connection_loop(fd, s));
    }
}

async fn connection_loop(fd: i32, shared: Rc<Shared>) {
    loop {
        let next = shared.reactor.recv(fd).await;
        let (ptr, len) = match next {
            Some(v) => v,
            None => break,
        };
        let data = unsafe { std::slice::from_raw_parts(ptr, len) };
        handle_message(fd, data, &shared).await;
        if !ptr.is_null() {
            unsafe { libc::free(ptr as *mut libc::c_void); }
        }
    }
    shared.reactor.close_fd(fd);
}

// ---------------------------------------------------------------------------
// Worker-crash watcher
// ---------------------------------------------------------------------------

async fn worker_watcher(shared: Rc<Shared>) {
    loop {
        shared.reactor.timer(Instant::now() + Duration::from_millis(WORKER_WATCH_MS)).await;
        let crashed = shared.disp().check_workers();
        if crashed >= 0 {
            let base_dir = shared.cat().base_dir.clone();
            eprintln!(
                "Worker {} crashed (log: {}/worker_{}.log), shutting down",
                crashed, base_dir, crashed,
            );
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
    loop {
        let first = match rx.recv().await {
            Some(t) => t,
            None => return, // all senders dropped — clean shutdown
        };
        let mut triggers = vec![first];

        // Drain anything already queued.
        while let Some(more) = rx.try_recv() {
            triggers.push(more);
        }

        // Honour the coalesce deadline only if no trigger is row-threshold
        // urgent. Lets pipelined inserts batch into a single tick.
        //
        // The timer is pinned outside the inner loop so every iteration
        // re-polls the same TimerFuture — its SQE is submitted once on
        // first poll and re-used across all `rx.recv()` wake-ups. The
        // previous shape (`let timer = …` inside the loop) allocated a
        // fresh TimerFuture per iteration and submitted a new SQE every
        // time `rx.recv()` resolved Pending-then-Ready, which was
        // unnecessary kernel churn.
        if !shared.any_threshold_crossed() {
            let deadline = Instant::now() + Duration::from_millis(TICK_DEADLINE_MS);
            let mut timer = Box::pin(shared.reactor.timer(deadline));
            loop {
                match select2(rx.recv(), timer.as_mut()).await {
                    Either::A(Some(more)) => triggers.push(more),
                    Either::A(None) => return, // channel closed mid-coalesce
                    Either::B(()) => break,    // deadline elapsed
                }
            }
        }

        // Build the tid set: union of drained tick_rows (in INSERT order)
        // + explicit tids from SCAN triggers. The order is load-bearing:
        // anti-join semantics (EXCEPT, NOT IN, etc.) rely on processing
        // ticks in the order pushes arrived. Reordering causes the b-side
        // trace to be empty when a-side ticks (and vice versa), leaking
        // rows that should have cancelled.
        let mut tids: Vec<i64> = shared.drain_tick_rows();
        let has_drain = triggers.iter().any(|t| matches!(t, TickTrigger::Drain { .. }));
        if has_drain {
            let mut seen: std::collections::HashSet<i64> = tids.iter().copied().collect();
            for t in &triggers {
                if let TickTrigger::Drain { tids: v, .. } = t {
                    for &tid in v {
                        if seen.insert(tid) { tids.push(tid); }
                    }
                }
            }
        }
        tids.retain(|&tid| shared.cat().has_id(tid));

        // Run the tick. Errors are reported in logs; every Drain trigger's
        // `done` is signalled regardless so callers don't hang.
        if let Err(e) = run_tick(&shared, &tids).await {
            gnitz_warn!("tick error: {}", e);
        }
        for t in triggers {
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
async fn run_tick(shared: &Rc<Shared>, tids: &[i64]) -> Result<(), String> {
    if tids.is_empty() { return Ok(()); }
    let nw = unsafe { (*shared.dispatcher.0).num_workers() };

    // Allocate every (tid, worker) req_id up front so the SAL emission
    // sequence is fully prepared before we take any locks.
    let req_ids: Vec<Vec<u64>> = tids.iter()
        .map(|_| (0..nw).map(|_| shared.reactor.alloc_request_id()).collect())
        .collect();

    let _cat_read = shared.catalog_rwlock.read().await;
    let _sal_excl = shared.sal_writer_excl.lock().await;

    let emit_err = guard_panic("tick", || unsafe {
        let disp = &mut *shared.dispatcher.0;
        for (i, &tid) in tids.iter().enumerate() {
            disp.write_tick_group(tid, &req_ids[i])?;
        }
        disp.signal_all();
        Ok(())
    });
    drop(_sal_excl);
    drop(_cat_read);
    emit_err?;

    let futs: Vec<_> = req_ids.iter().flatten()
        .map(|&id| shared.reactor.await_reply(id))
        .collect();
    let replies = crate::reactor::join_all(futs).await;
    if let Some(e) = first_worker_error("tick", &replies) {
        return Err(e);
    }
    shared.last_tick_lsn.set(shared.ingest_lsn.get());
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
/// V.7 liveness: the loop never exits on inner errors; only an
/// unrecoverable channel close (all senders dropped) terminates it.
async fn relay_loop(shared: Rc<Shared>, mut rx: mpsc::Receiver<PendingRelay>) {
    loop {
        let msg = match rx.recv().await {
            Some(r) => r,
            None => return,
        };
        match msg {
            PendingRelay::Fence { done } => {
                // The fence is processed strictly after every earlier
                // Relay on the channel. Signal back to apply_migration
                // that the relay pipeline is drained so Phase 3's
                // write-lock acquisition cannot park a stale relay.
                let _ = done.send(());
            }
            relay @ PendingRelay::Relay { .. } => {
                // Phase 1: CPU work + catalog read only — no SAL mutex.
                let prep = {
                    let _cat = shared.catalog_rwlock.read().await;
                    match guard_panic("prepare_relay", || unsafe {
                        (*shared.dispatcher.0).prepare_relay(relay)
                    }) {
                        Ok(p) => p,
                        Err(e) => { gnitz_warn!("prepare_relay: {}", e); continue; }
                    }
                };
                // Phase 2: SAL write only — grab the mutex, no awaits inside.
                {
                    let _sal = shared.sal_writer_excl.lock().await;
                    if let Err(e) = guard_panic("emit_relay", || unsafe {
                        (*shared.dispatcher.0).emit_relay(prep)
                    }) {
                        gnitz_warn!("emit_relay failed: {}", e);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Migration apply
// ---------------------------------------------------------------------------

/// Decode the migration row out of an inbound `decoded` wire.
/// Returns the populated `MigrationRow` or an error describing why
/// the batch is malformed.
fn extract_migration_row(decoded: &ipc::DecodedWire) -> Result<MigrationRow, String> {
    let batch = decoded.data_batch.as_ref()
        .ok_or("migration push carries no batch")?;
    if batch.count != 1 {
        return Err(format!(
            "migration push must carry exactly one row, got {}", batch.count,
        ));
    }
    let pk = batch.get_pk(0);

    // PK is schema col 0 and lives in pk_lo/hi, so payload_col = schema_col - 1.
    let parent_hash = read_u128(batch, 0, MIGRATIONS_COL_PARENT_HASH - 1)?;
    let author = read_str(batch, 0, MIGRATIONS_COL_AUTHOR - 1)?;
    let message = read_str(batch, 0, MIGRATIONS_COL_MESSAGE - 1)?;
    let created_lsn = read_u64(batch, 0, MIGRATIONS_COL_CREATED_LSN - 1)? as i64;
    let desired_state_sql = read_str(batch, 0, MIGRATIONS_COL_DESIRED_STATE_SQL - 1)?;
    let canonical_hex_bytes = read_bytes(batch, 0, MIGRATIONS_COL_DESIRED_STATE_CANONICAL - 1)?;
    let format_version = read_u64(batch, 0, MIGRATIONS_COL_FORMAT_VERSION - 1)?;

    let canonical_hex = String::from_utf8(canonical_hex_bytes)
        .map_err(|e| format!("desired_state_canonical is not valid UTF-8: {}", e))?;
    let canonical_bytes = mig::from_hex(&canonical_hex)
        .map_err(|e| format!("desired_state_canonical hex decode: {}", e))?;

    Ok(MigrationRow {
        hash: pk,
        parent_hash,
        author,
        message,
        created_lsn,
        desired_state_sql,
        desired_state_canonical: canonical_bytes,
        format_version,
    })
}

fn read_u64(batch: &Batch, row: usize, payload_col: usize) -> Result<u64, String> {
    let data = batch.col_data(payload_col);
    let off = row * 8;
    if off + 8 > data.len() {
        return Err(format!("migration row: u64 col {} truncated", payload_col));
    }
    Ok(u64::from_le_bytes(data[off..off + 8].try_into().unwrap()))
}

fn read_u128(batch: &Batch, row: usize, payload_col: usize) -> Result<u128, String> {
    let data = batch.col_data(payload_col);
    let off = row * 16;
    if off + 16 > data.len() {
        return Err(format!("migration row: u128 col {} truncated", payload_col));
    }
    let lo = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
    let hi = u64::from_le_bytes(data[off + 8..off + 16].try_into().unwrap());
    Ok(((hi as u128) << 64) | (lo as u128))
}

fn read_str(batch: &Batch, row: usize, payload_col: usize) -> Result<String, String> {
    let bytes = read_bytes(batch, row, payload_col)?;
    String::from_utf8(bytes)
        .map_err(|e| format!("migration row: string col {} invalid UTF-8: {}", payload_col, e))
}

fn read_bytes(batch: &Batch, row: usize, payload_col: usize) -> Result<Vec<u8>, String> {
    let data = batch.col_data(payload_col);
    let off = row * 16;
    if off + 16 > data.len() {
        return Err(format!("migration row: string col {} truncated", payload_col));
    }
    let st: [u8; 16] = data[off..off + 16].try_into().unwrap();
    Ok(crate::schema::decode_german_string(&st, &batch.blob))
}

/// Read the canonical bytes of a historical migration from
/// `sys_migrations`. Returns `None` if the hash is not present.
fn read_migration_canonical(cat: &mut CatalogEngine, hash: u128) -> Option<Vec<u8>> {
    let mut cursor = cat.sys_migrations_cursor().ok()?;
    let (pk_lo, pk_hi) = (hash as u64, (hash >> 64) as u64);
    cursor.cursor.seek(crate::util::make_pk(pk_lo, pk_hi));
    if !cursor.cursor.valid
        || cursor.cursor.current_key_lo != pk_lo
        || cursor.cursor.current_key_hi != pk_hi
        || cursor.cursor.current_weight <= 0
    {
        return None;
    }
    let ptr = cursor.cursor.col_ptr(MIGRATIONS_COL_DESIRED_STATE_CANONICAL, 16);
    if ptr.is_null() { return None; }
    let st: [u8; 16] = unsafe { std::slice::from_raw_parts(ptr, 16) }
        .try_into().unwrap();
    let blob_ptr = cursor.cursor.blob_ptr();
    let blob_slice = if !blob_ptr.is_null() {
        unsafe { std::slice::from_raw_parts(blob_ptr, cursor.cursor.blob_len()) }
    } else { &[] };
    let hex_bytes = crate::schema::decode_german_string(&st, blob_slice);
    let hex_str = String::from_utf8(hex_bytes).ok()?;
    mig::from_hex(&hex_str).ok()
}

/// apply_migration: serialised atomic schema-migration commit.
///
/// Supports CREATE/DROP TABLE and CREATE/DROP INDEX; views in
/// migration SQL return an explicit "not yet implemented" error
/// because the SQL planner runs client-side.
///
/// Flow:
///   1. CAS `migration_in_flight` (scope guard clears on any exit).
///   2. drain committer + tick; verify hash, parent, decode canonical,
///      parse new, compute diff.
///   3. relay fence — defensive no-op today, symmetric with the staging
///      phase that backfill migrations will add later.
///   4. under write lock, build drop + create batches, ingest locally
///      + broadcast, single fsync.
///   5. rmtree dropped directories, respond OK.
async fn apply_migration(
    shared: Rc<Shared>, fd: i32, client_id: u64, decoded: ipc::DecodedWire,
) {
    // ---- CAS + scope guard ---------------------------------------------
    let guard = match MigrationInFlightGuard::acquire(&shared) {
        Some(g) => g,
        None => {
            let msg = b"another migration is in flight; retry shortly";
            send_error(&shared, fd, MIGRATIONS_TAB_ID, client_id, msg).await;
            return;
        }
    };

    // ---- Extract migration row -----------------------------------------
    let row = match extract_migration_row(&decoded) {
        Ok(r) => r,
        Err(e) => {
            send_error(&shared, fd, MIGRATIONS_TAB_ID, client_id, e.as_bytes()).await;
            return;
        }
    };
    apply_migration_row(shared, fd, client_id, row, guard).await;
}

/// Core of `apply_migration`: phase-1 validation + phase-3 commit, given
/// a pre-built `MigrationRow`. Entered by the client-side
/// `push_migration` path (via `apply_migration`). The in-flight guard
/// is taken by the caller and dropped when this future resolves.
async fn apply_migration_row(
    shared: Rc<Shared>, fd: i32, client_id: u64, row: MigrationRow,
    _guard: MigrationInFlightGuard,
) {
    if row.format_version != 1 {
        let msg = format!("unsupported format_version {}", row.format_version);
        send_error(&shared, fd, MIGRATIONS_TAB_ID, client_id, msg.as_bytes()).await;
        return;
    }

    // ---- Phase 1: drain barriers --------------------------------------
    let (c_tx, c_rx) = oneshot::channel::<()>();
    shared.committer_tx.send(CommitRequest::Barrier { done: c_tx });
    let _ = c_rx.await;
    let (t_tx, t_rx) = oneshot::channel::<()>();
    shared.tick_tx.send(TickTrigger::Drain { tids: vec![], done: t_tx });
    let _ = t_rx.await;

    // ---- Phase 1: validate + diff (under read lock) --------------------
    let diff_opt: Result<mig::Diff, String> = async {
        let _cat = shared.catalog_rwlock.read().await;
        let cat = shared.cat();

        // Hash verification.
        let expected = mig::compute_migration_hash(
            row.parent_hash, &row.desired_state_canonical, &row.author, &row.message,
        );
        if expected != row.hash {
            return Err("migration hash does not match its canonical payload".into());
        }

        // Parent verification (provisional — re-checked under write lock).
        if row.parent_hash != cat.current_migration_hash {
            return Err(format!(
                "stale parent_hash: expected 0x{:032x}, got 0x{:032x}",
                cat.current_migration_hash, row.parent_hash,
            ));
        }

        // Decode parent + new canonical forms.
        let parent_state = if row.parent_hash == 0 {
            mig::DesiredState::default()
        } else {
            let bytes = read_migration_canonical(cat, row.parent_hash)
                .ok_or("parent migration row not found in sys_migrations")?;
            mig::decanonicalize(&bytes)?
        };
        let new_state = mig::decanonicalize(&row.desired_state_canonical)?;

        // Diff.
        let mut diff = mig::diff_by_name(&parent_state, &new_state);
        if diff.has_modifications() {
            let desc = diff.first_modification_description().unwrap_or_default();
            return Err(format!(
                "schema modifications are not yet supported; offending object: {}", desc,
            ));
        }

        if !diff.created_views.is_empty() || !diff.dropped_views.is_empty() {
            return Err(
                "CREATE/DROP VIEW in migrations is not yet supported; \
                 use the non-migration DDL path for views".into(),
            );
        }

        // Empty diff — nothing to do, reject as the plan mandates.
        if diff.created_tables.is_empty()
            && diff.created_indices.is_empty()
            && diff.dropped_tables.is_empty()
            && diff.dropped_indices.is_empty()
        {
            return Err("migration is a no-op (empty diff)".into());
        }

        // Topo sort creates (parent-before-child) and drops (child-before-parent).
        mig::topo_sort_diff(&mut diff)?;

        Ok(diff)
    }.await;

    let diff = match diff_opt {
        Ok(d) => d,
        Err(e) => {
            send_error(&shared, fd, MIGRATIONS_TAB_ID, client_id, e.as_bytes()).await;
            return;
        }
    };

    // ---- Phase 2 → 3 relay fence --------------------------------------
    // No Phase-2 work emits relays today, but the fence is retained
    // for symmetry: once backfill lands, any FLAG_EXCHANGE relays it
    // emits must drain before Phase 3's write lock.
    let (r_tx, r_rx) = oneshot::channel::<()>();
    shared.relay_tx.send(PendingRelay::Fence { done: r_tx });
    let _ = r_rx.await;

    // ---- Phase 3: master-local apply + broadcast under write lock -----
    let dropped_dirs = match apply_phase3(&shared, &row, diff).await {
        Ok(d) => d,
        Err(e) => {
            send_error(&shared, fd, MIGRATIONS_TAB_ID, client_id, e.as_bytes()).await;
            return;
        }
    };

    // ---- Phase 4: rmtree + respond ------------------------------------
    for dir in dropped_dirs {
        // rmtree is best-effort; orphans (if any) are reaped by the
        // startup directory sweep on next boot.
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Success response.
    let schema = shared.get_schema_desc(MIGRATIONS_TAB_ID);
    send_ok_response(
        &shared, fd, MIGRATIONS_TAB_ID, None, client_id, &schema,
        shared.ingest_lsn.get(),
    ).await;
}

async fn apply_phase3(
    shared: &Rc<Shared>, row: &MigrationRow, diff: mig::Diff,
) -> Result<Vec<std::path::PathBuf>, String> {
    let _write = shared.catalog_rwlock.write().await;

    // TOCTOU re-check.
    {
        let cat = shared.cat();
        if row.parent_hash != cat.current_migration_hash {
            return Err(format!(
                "stale parent_hash at apply: another migration committed first (current 0x{:032x})",
                cat.current_migration_hash,
            ));
        }
    }

    // Build drop batches first (collect directories to rmtree later).
    let mut to_broadcast: Vec<(i64, Batch)> = Vec::new();
    let mut dropped_dirs: Vec<std::path::PathBuf> = Vec::new();
    let mut dropped_tids: Vec<i64> = Vec::new();

    // Drops are already topo-sorted (child-before-parent).
    for idx in &diff.dropped_indices {
        let cat = shared.cat();
        let batches = cat.build_drop_index_batches(&idx.name)?;
        to_broadcast.extend(batches);
    }
    for t in &diff.dropped_tables {
        let cat = shared.cat();
        let (tid, batches, dir) = cat.build_drop_table_batches(&t.schema, &t.name)?;
        to_broadcast.extend(batches);
        if let Some(d) = dir { dropped_dirs.push(d); }
        dropped_tids.push(tid);
    }

    // Creates are already topo-sorted (parent-before-child).
    for t in &diff.created_tables {
        let cat = shared.cat();
        let (_tid, batches) = cat.build_create_table_batches(t)?;
        to_broadcast.extend(batches);
    }
    for idx in &diff.created_indices {
        let cat = shared.cat();
        let (_idx_id, batches) = cat.build_create_index_batches(idx)?;
        to_broadcast.extend(batches);
    }

    // Update the durable sys_sequences HWM (covers both
    // allocate_table_id and allocate_index_id bumps above).
    {
        let cat = shared.cat();
        let seq_batch = cat.build_sequence_hwm_batch()?;
        if seq_batch.count > 0 {
            to_broadcast.push((crate::catalog::SEQ_TAB_ID, seq_batch));
        }
    }

    // Build the sys_migrations insert row.
    let migration_batch = {
        let cat = shared.cat();
        cat.build_migration_row_batch(row)
    };

    // Ingest locally + broadcast under one sal_writer_excl + single fsync.
    let fsync_fut = {
        let _sal_excl = shared.sal_writer_excl.lock().await;
        let cat_ptr = shared.catalog.0;
        let disp_ptr = shared.dispatcher.0;

        // Each batch: ingest to master's catalog (writes master WAL
        // + fires hooks), then broadcast over the SAL to workers.
        for (tid, batch) in &to_broadcast {
            let ingest_res = guard_panic("migration ingest", || {
                let cat = unsafe { &mut *cat_ptr };
                cat.ingest_to_family(*tid, batch)
            });
            if let Err(e) = ingest_res {
                gnitz_fatal_abort!("migration ingest failed mid-Phase-3: {}", e);
            }
            let bcast_res = guard_panic("migration broadcast_ddl", || unsafe {
                (*disp_ptr).broadcast_ddl(*tid, batch)
            });
            if let Err(e) = bcast_res {
                // Per plan: a partial broadcast is unrecoverable.
                gnitz_fatal_abort!("migration broadcast failed mid-Phase-3: {}", e);
            }
        }

        // sys_migrations row goes last so workers process it after
        // the swap is applied.
        let ingest_res = guard_panic("migration-row ingest", || {
            let cat = unsafe { &mut *cat_ptr };
            cat.ingest_to_family(MIGRATIONS_TAB_ID, &migration_batch)
        });
        if let Err(e) = ingest_res {
            gnitz_fatal_abort!("sys_migrations ingest failed: {}", e);
        }
        let bcast_res = guard_panic("migration-row broadcast_ddl", || unsafe {
            (*disp_ptr).broadcast_ddl(MIGRATIONS_TAB_ID, &migration_batch)
        });
        if let Err(e) = bcast_res {
            gnitz_fatal_abort!("sys_migrations broadcast failed: {}", e);
        }

        // Update head.
        unsafe { (*cat_ptr).current_migration_hash = row.hash; }

        // SQE submit, no await inside the lock.
        shared.reactor.fsync(shared.sal_fd)
    };

    let lsn = shared.ingest_lsn.get() + 1;
    shared.ingest_lsn.set(lsn);

    let fsync_rc = fsync_fut.await;
    if fsync_rc < 0 {
        gnitz_fatal_abort!("SAL fdatasync (migration) failed rc={}", fsync_rc);
    }

    // Drop per-tid caches for every dropped table — the cached
    // SchemaDescriptor / column names / table lock now describe an
    // object that no longer exists, and a subsequent tid reuse would
    // otherwise see stale values.
    for tid in dropped_tids {
        shared.invalidate_tid_caches(tid);
    }

    Ok(dropped_dirs)
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

async fn handle_message(fd: i32, data: &[u8], shared: &Rc<Shared>) {
    let decoded = {
        let target_id = ipc::peek_target_id(data).unwrap_or(-1);
        let cached = shared.schema_cache.borrow().get(&target_id).copied();
        let result = if let Some(s) = cached {
            ipc::decode_wire_with_schema(data, &s)
        } else {
            ipc::decode_wire(data)
        };
        match result {
            Ok(d) => d,
            Err(e) => {
                let msg = format!("decode error: {}", e);
                send_error(shared, fd, 0, 0, msg.as_bytes()).await;
                return;
            }
        }
    };

    let client_id = decoded.control.client_id;
    let target_id = decoded.control.target_id as i64;
    let flags = decoded.control.flags;

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
        handle_seek(shared, fd, client_id, target_id,
                    decoded.control.seek_pk_lo, decoded.control.seek_pk_hi).await;
        return;
    }
    if flags & FLAG_SEEK_BY_INDEX != 0 {
        let _g = shared.catalog_rwlock.read().await;
        handle_seek_by_index(shared, fd, client_id, target_id,
                             decoded.control.seek_col_idx as u32,
                             decoded.control.seek_pk_lo,
                             decoded.control.seek_pk_hi).await;
        return;
    }

    if target_id >= FIRST_USER_TABLE_ID && (!has_batch || batch_count == 0) {
        let _g = shared.catalog_rwlock.read().await;
        handle_scan(shared, fd, client_id, target_id).await;
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

    // ---------- Migration dispatch (goes before the in-flight gate) ----------
    // Migrations have their own CAS inside apply_migration; we do NOT
    // want the generic gate to reject a migration push while another
    // migration is in flight (the CAS gives a proper error).
    if target_id == MIGRATIONS_TAB_ID && has_batch && batch_count > 0 {
        apply_migration(Rc::clone(shared), fd, client_id, decoded).await;
        return;
    }

    // ---------- Migration-in-flight gate (rejects external DML + DDL) ----------
    // The migration dispatch branch (target_id == MIGRATIONS_TAB_ID,
    // added in handle_message before system_dml) runs before this
    // check; we never gate migration pushes themselves. Read-only
    // paths (SEEK / SEEK_BY_INDEX / empty-batch SCAN) already
    // returned above, so only mutating pushes reach here.
    if shared.migration_in_flight.load(Ordering::Acquire) {
        let msg = b"another migration is in flight; retry shortly" as &[u8];
        send_error(shared, fd, target_id, client_id, msg).await;
        return;
    }

    // ---------- User-table INSERT ----------
    if target_id >= FIRST_USER_TABLE_ID && has_batch && batch_count > 0 {
        let mode = ipc::decode_conflict_mode(flags, decoded.control.seek_col_idx);
        let batch = decoded.data_batch.unwrap();

        let _cat = shared.catalog_rwlock.read().await;
        if !shared.cat().has_id(target_id) {
            let msg = format!("table {} not found", target_id);
            send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
            return;
        }
        let needs_lock = shared.needs_table_lock(target_id, mode);
        let _tlock = if needs_lock {
            Some(shared.table_lock(target_id).lock().await)
        } else {
            None
        };

        // Local (catalog-resident) unique-index validation. Wrapped per V.4
        // so a malformed batch can't crash the server.
        let cat_ptr_raw = shared.catalog.0;
        if let Err(e) = guard_panic("validate", || unsafe {
            (*cat_ptr_raw).validate_unique_indices(target_id, &batch)
        }) {
            send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
            return;
        }
        // Distributed validation (FK / unique indices + UPSERT).
        if let Err(e) = MasterDispatcher::validate_all_distributed_async(
            shared.dispatcher.0, &shared.reactor, target_id, &batch, mode,
        ).await {
            send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
            return;
        }

        // Route through the committer and wait for commit ACK.
        let batch_count = batch.count;
        let (tx, rx) = oneshot::channel::<Result<u64, String>>();
        shared.committer_tx.send(CommitRequest::Push {
            tid: target_id, batch, mode, done: tx,
        });
        let commit_result = rx.await;
        match commit_result {
            Ok(Ok(lsn)) => {
                // Successful commit: bump tick counter for this table.
                shared.bump_tick_rows(target_id, batch_count);
                if shared.any_threshold_crossed() {
                    shared.tick_tx.send(TickTrigger::Auto);
                }
                let schema = shared.get_schema_desc(target_id);
                send_ok_response(shared, fd, target_id, None, client_id, &schema, lsn).await;
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
        handle_system_dml(shared, fd, client_id, target_id, decoded).await;
        return;
    }

    // Fallthrough: ignore (should not happen).
}

async fn handle_seek(
    shared: &Rc<Shared>, fd: i32, client_id: u64,
    target_id: i64, pk_lo: u64, pk_hi: u64,
) {
    if !shared.cat().has_id(target_id) {
        let msg = format!("table {} not found", target_id);
        send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    let schema = shared.get_schema_desc(target_id);
    let result = if target_id >= FIRST_USER_TABLE_ID {
        MasterDispatcher::fan_out_seek_async(
            shared.dispatcher.0, &shared.reactor, target_id, pk_lo, pk_hi,
        ).await
    } else {
        let cat_ptr = shared.catalog.0;
        unsafe { (*cat_ptr).seek_family(target_id, pk_lo, pk_hi) }
    };
    match result {
        Ok(batch) => send_ok_response(shared, fd, target_id, batch.as_ref(), client_id, &schema, 0).await,
        Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
    }
}

async fn handle_seek_by_index(
    shared: &Rc<Shared>, fd: i32, client_id: u64,
    target_id: i64, col_idx: u32, key_lo: u64, key_hi: u64,
) {
    if !shared.cat().has_id(target_id) {
        let msg = format!("table {} not found", target_id);
        send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    let schema = shared.get_schema_desc(target_id);
    let result = if target_id >= FIRST_USER_TABLE_ID {
        MasterDispatcher::fan_out_seek_by_index_async(
            shared.dispatcher.0, &shared.reactor, target_id, col_idx, key_lo, key_hi,
        ).await
    } else {
        let cat_ptr = shared.catalog.0;
        unsafe { (*cat_ptr).seek_by_index(target_id, col_idx, key_lo, key_hi) }
    };
    match result {
        Ok(batch) => send_ok_response(shared, fd, target_id, batch.as_ref(), client_id, &schema, 0).await,
        Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
    }
}

async fn handle_scan(
    shared: &Rc<Shared>, fd: i32, client_id: u64, target_id: i64,
) {
    if !shared.cat().has_id(target_id) {
        let msg = format!("table {} not found", target_id);
        send_error(shared, fd, target_id, client_id, msg.as_bytes()).await;
        return;
    }
    let schema = shared.get_schema_desc(target_id);
    // Drain pending ticks before reading: views derive from source-table
    // pushes through the DAG (IV.2). Loop until `tick_rows` is empty —
    // a push landing during the tick may add more rows; the outer
    // is_empty() re-check closes the race.
    loop {
        let snapshot: Vec<i64> = {
            let tr = shared.tick_rows.borrow();
            if tr.is_empty() { break; }
            tr.keys().copied().collect()
        };
        let (tx, rx) = oneshot::channel::<()>();
        shared.tick_tx.send(TickTrigger::Drain {
            tids: snapshot,
            done: tx,
        });
        let _ = rx.await;
    }
    let lsn = shared.last_tick_lsn.get();
    let result = MasterDispatcher::fan_out_scan_async(
        shared.dispatcher.0, &shared.reactor, target_id,
    ).await;
    match result {
        Ok(batch) => {
            let arc = batch.map(Arc::new);
            send_ok_response(shared, fd, target_id, arc.as_deref(), client_id, &schema, lsn).await;
        }
        Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
    }
}

/// System-table path: catalog ingest + (optionally) DDL broadcast.
/// DDL path acquires the catalog write lock and drains the committer.
async fn handle_system_dml(
    shared: &Rc<Shared>, fd: i32, client_id: u64, target_id: i64,
    decoded: ipc::DecodedWire,
) {
    let batch = decoded.data_batch;
    let non_empty = batch.as_ref().map(|b| b.count > 0).unwrap_or(false);
    let schema = shared.get_schema_desc(target_id);

    // Defence-in-depth: non-empty batches for sys_migrations must
    // go through apply_migration (which the dispatch branch in
    // handle_message routes). Reject here so a bug in the routing
    // can't silently ingest a migration row as a plain catalog
    // insert.
    if target_id == MIGRATIONS_TAB_ID && non_empty {
        let msg = b"sys_migrations inserts must go through push_migration, \
                    not a raw catalog INSERT";
        send_error(shared, fd, target_id, client_id, msg).await;
        return;
    }

    // Empty SCAN for system tables — no DDL, no lock needed.
    if !non_empty {
        let _g = shared.catalog_rwlock.read().await;
        let cat_ptr = shared.catalog.0;
        match guard_panic("scan", || unsafe { (*cat_ptr).scan_family(target_id) }) {
            Ok(b) => {
                let batch_ref = if b.count > 0 { Some(b) } else { None };
                send_ok_response(shared, fd, target_id, batch_ref.as_deref(),
                                 client_id, &schema, shared.last_tick_lsn.get()).await;
            }
            Err(e) => send_error(shared, fd, target_id, client_id, e.as_bytes()).await,
        }
        return;
    }

    // DDL path: drain committer first, then grab write lock.
    let t_ddl_start = Instant::now();
    let _write = shared.catalog_rwlock.write().await;
    let t_after_write = Instant::now();
    let (tx, rx) = oneshot::channel::<()>();
    shared.committer_tx.send(CommitRequest::Barrier { done: tx });
    let _ = rx.await;
    let t_after_barrier = Instant::now();

    let batch = batch.unwrap();
    let ddl_clone = batch.clone_batch();
    let t_mut_start = Instant::now();

    let cat_ptr_raw = shared.catalog.0;
    if let Err(e) = guard_panic("DDL", || {
        let cat = unsafe { &mut *cat_ptr_raw };
        cat.ingest_to_family(target_id, &batch)?;
        let dag = cat.get_dag_ptr();
        unsafe { (*dag).evaluate_dag(target_id, batch); }
        Ok(())
    }) {
        send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
        return;
    }
    let t_mut_done = Instant::now();
    let lsn = shared.ingest_lsn.get() + 1;
    shared.ingest_lsn.set(lsn);

    // SAL emission window: acquire III.3b mutex to serialize the
    // broadcast write against committer + tick + relay. Lock held ONLY
    // across the synchronous broadcast + fsync SQE submit; the fsync
    // .await happens after release so concurrent writers can proceed.
    let fsync_fut = {
        let _sal_excl = shared.sal_writer_excl.lock().await;
        let disp_ptr_raw = shared.dispatcher.0;
        let ddl_for_broadcast = ddl_clone;
        if let Err(e) = guard_panic("broadcast_ddl", || unsafe {
            (*disp_ptr_raw).broadcast_ddl(target_id, &ddl_for_broadcast)
        }) {
            send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
            return;
        }
        shared.reactor.fsync(shared.sal_fd)
    };
    let t_bcast_done = Instant::now();
    let fsync_rc = fsync_fut.await;
    let t_fsync_done = Instant::now();
    if fsync_rc < 0 {
        gnitz_fatal_abort!("SAL fdatasync (DDL) failed rc={}", fsync_rc);
    }

    send_ok_response(shared, fd, target_id, None, client_id, &schema, lsn).await;
    let t_ddl_done = Instant::now();
    let total = t_ddl_done.duration_since(t_ddl_start);
    if total > Duration::from_millis(20) {
        gnitz_debug!(
            "DDL tid={} SLOW total={:?} write_acq={:?} barrier={:?} mutate={:?} broadcast={:?} fsync={:?} send={:?}",
            target_id, total,
            t_after_write.duration_since(t_ddl_start),
            t_after_barrier.duration_since(t_after_write),
            t_mut_done.duration_since(t_mut_start),
            t_bcast_done.duration_since(t_mut_done),
            t_fsync_done.duration_since(t_bcast_done),
            t_ddl_done.duration_since(t_fsync_done),
        );
    }
}

// ---------------------------------------------------------------------------
// Wire-protocol response helpers
// ---------------------------------------------------------------------------

fn encode_response_buffer(
    target_id: i64, client_id: u64,
    result: Option<&Batch>, status: u32, error_msg: &[u8],
    schema: &SchemaDescriptor, col_names_opt: Option<&[&[u8]]>,
    seek_pk_lo: u64,
) -> Vec<u8> {
    let sz = ipc::wire_size(status, error_msg, Some(schema), col_names_opt, result);
    let total = 4 + sz;
    let mut buf: Vec<u8> = vec![0; total];
    buf[0..4].copy_from_slice(&(sz as u32).to_le_bytes());
    let written = ipc::encode_wire_into(
        &mut buf[4..total], 0,
        target_id as u64, client_id,
        0, seek_pk_lo, 0, 0, 0,
        status, error_msg,
        Some(schema), col_names_opt, result,
    );
    debug_assert_eq!(written, sz);
    buf
}

async fn send_ok_response(
    shared: &Rc<Shared>, fd: i32, target_id: i64,
    result: Option<&Batch>, client_id: u64,
    schema: &SchemaDescriptor, seek_pk_lo: u64,
) {
    let col_names_ref = shared.get_col_names_bytes(target_id);
    let mut name_refs_arr = [&[] as &[u8]; 64];
    for (i, n) in col_names_ref.iter().enumerate().take(64) {
        name_refs_arr[i] = n.as_slice();
    }
    let col_names_opt = Some(&name_refs_arr[..col_names_ref.len()]);
    let buf = encode_response_buffer(
        target_id, client_id, result, STATUS_OK, b"",
        schema, col_names_opt, seek_pk_lo,
    );
    drop(col_names_ref);
    let rc = shared.reactor.send_buffer(fd, buf).await;
    if rc < 0 { shared.reactor.close_fd(fd); }
}

async fn send_error(
    shared: &Rc<Shared>, fd: i32, target_id: i64, client_id: u64,
    error_msg: &[u8],
) {
    let schema = SchemaDescriptor::minimal_u64();
    let buf = encode_response_buffer(
        target_id, client_id, None, STATUS_ERROR, error_msg,
        &schema, None, 0,
    );
    let rc = shared.reactor.send_buffer(fd, buf).await;
    if rc < 0 { shared.reactor.close_fd(fd); }
}

async fn send_alloc(
    shared: &Rc<Shared>, fd: i32, new_id: i64, client_id: u64,
) {
    let sz = ipc::wire_size(STATUS_OK, b"", None, None, None);
    let total = 4 + sz;
    let mut buf: Vec<u8> = vec![0; total];
    buf[0..4].copy_from_slice(&(sz as u32).to_le_bytes());
    let written = ipc::encode_wire_into(
        &mut buf[4..total], 0,
        new_id as u64, client_id,
        0, 0, 0, 0, 0,
        STATUS_OK, b"",
        None, None, None,
    );
    debug_assert_eq!(written, sz);
    let rc = shared.reactor.send_buffer(fd, buf).await;
    if rc < 0 { shared.reactor.close_fd(fd); }
}

// ---------------------------------------------------------------------------
// Schema validation helper
// ---------------------------------------------------------------------------

fn validate_schema_match(
    wire: &SchemaDescriptor, expected: &SchemaDescriptor,
) -> Result<(), String> {
    if wire.num_columns != expected.num_columns {
        return Err(format!(
            "Schema mismatch: expected {} columns, got {}",
            expected.num_columns, wire.num_columns,
        ));
    }
    if wire.pk_index != expected.pk_index {
        return Err(format!(
            "Schema mismatch: expected pk_index={}, got {}",
            expected.pk_index, wire.pk_index,
        ));
    }
    for i in 0..wire.num_columns as usize {
        if wire.columns[i].type_code != expected.columns[i].type_code {
            return Err(format!(
                "Schema mismatch at column {}: expected type {}, got {}",
                i, expected.columns[i].type_code, wire.columns[i].type_code,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::BatchBuilder;
    use crate::catalog::sys_tab_schema_pub;

    /// Silent-zero regression: before the fix, a read past the column
    /// data returned a zero-filled value. Now it returns an error so
    /// malformed migration rows are rejected at the protocol boundary.
    #[test]
    fn read_helpers_reject_short_payload() {
        let schema = sys_tab_schema_pub(MIGRATIONS_TAB_ID);
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(1, 0, 1);
        bb.put_u128(0, 0);
        bb.put_string("a");
        bb.put_string("m");
        bb.put_u64(0);
        bb.put_string("sql");
        bb.put_string("deadbeef");
        bb.put_u64(1);
        bb.end_row();
        let batch = bb.finish();

        assert!(read_u64(&batch, 0, 3).is_ok());
        assert!(read_u128(&batch, 0, 0).is_ok());
        assert!(read_str(&batch, 0, 1).is_ok());
        assert!(read_bytes(&batch, 0, 5).is_ok());

        // Row index past the batch's row count: silent-zero would have
        // returned 0; the hardened helpers must error.
        let err = read_u64(&batch, 1, 3).unwrap_err();
        assert!(err.contains("truncated"), "got: {}", err);
        let err = read_u128(&batch, 1, 0).unwrap_err();
        assert!(err.contains("truncated"), "got: {}", err);
        let err = read_bytes(&batch, 1, 5).unwrap_err();
        assert!(err.contains("truncated"), "got: {}", err);
        let err = read_str(&batch, 7, 1).unwrap_err();
        assert!(err.contains("truncated"), "got: {}", err);
    }
}
