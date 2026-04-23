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
use std::time::{Duration, Instant};

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID, SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES};
use crate::runtime::committer::{self, CommitRequest};
use crate::runtime::wire::{self as ipc, STATUS_OK, STATUS_ERROR};
use crate::runtime::master::{MasterDispatcher, first_worker_error};
use crate::runtime::reactor::{
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
    tick_rows: Rc<RefCell<HashMap<i64, usize>>>,
    tick_tids: Rc<RefCell<Vec<i64>>>,
    t_last_push: Rc<Cell<Option<Instant>>>,
    table_locks: RefCell<HashMap<i64, Rc<AsyncMutex<()>>>>,
}

impl Shared {
    fn cat(&self) -> &mut CatalogEngine { unsafe { &mut *self.catalog.0 } }
    fn disp(&self) -> &mut MasterDispatcher { unsafe { &mut *self.dispatcher.0 } }

    fn get_schema_desc(&self, target_id: i64) -> SchemaDescriptor {
        self.cat().get_schema_desc(target_id)
            .unwrap_or_else(SchemaDescriptor::minimal_u64)
    }

    fn get_col_names_bytes(&self, target_id: i64) -> Rc<Vec<Vec<u8>>> {
        self.cat().get_col_names_bytes(target_id)
    }

    fn table_lock(&self, tid: i64) -> Rc<AsyncMutex<()>> {
        let mut locks = self.table_locks.borrow_mut();
        if let Some(l) = locks.get(&tid) { return Rc::clone(l); }
        let l = Rc::new(AsyncMutex::new(()));
        locks.insert(tid, Rc::clone(&l));
        l
    }

    fn needs_table_lock(&self, tid: i64) -> bool {
        self.cat().needs_table_lock(tid)
    }

    /// True iff some pending tid has crossed the row coalesce threshold.
    /// Used by the tick task to skip the deadline coalesce window.
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
            fire_auto_tick: Rc::new(move || { auto_tick_tx.send(TickTrigger::Auto); }),
            t_last_push: Rc::clone(&t_last_push),
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
            ingest_lsn: Rc::clone(&ingest_lsn),
            last_tick_lsn: Rc::clone(&last_tick_lsn),
            tick_rows: Rc::clone(&tick_rows),
            tick_tids: Rc::clone(&tick_tids),
            t_last_push: Rc::clone(&t_last_push),
            table_locks: RefCell::new(HashMap::new()),
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
    let replies = crate::runtime::reactor::join_all(futs).await;
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
        let relay = match rx.recv().await {
            Some(r) => r,
            None => return,
        };
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

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

async fn handle_message(fd: i32, data: &[u8], shared: &Rc<Shared>) {
    let decoded = {
        let schema_hint = ipc::peek_target_id(data).ok()
            .and_then(|tid| shared.cat().get_schema_desc(tid));
        let result = match schema_hint.as_ref() {
            Some(s) => ipc::decode_wire_with_schema(data, s),
            None    => ipc::decode_wire(data),
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
        let needs_lock = shared.needs_table_lock(target_id);
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
            shared.dispatcher.0, &shared.reactor, &shared.sal_writer_excl,
            target_id, &batch, mode,
        ).await {
            send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
            return;
        }

        // Route through the committer and wait for commit ACK.
        let (tx, rx) = oneshot::channel::<Result<u64, String>>();
        shared.committer_tx.send(CommitRequest::Push {
            tid: target_id, batch, mode, done: tx,
        });
        let commit_result = rx.await;
        match commit_result {
            Ok(Ok(lsn)) => {
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
            shared.dispatcher.0, &shared.reactor, &shared.sal_writer_excl,
            target_id, pk_lo, pk_hi,
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
            shared.dispatcher.0, &shared.reactor, &shared.sal_writer_excl,
            target_id, col_idx, key_lo, key_hi,
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
            let tids = shared.tick_tids.borrow();
            if tids.is_empty() { break; }
            tids.clone()
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
        shared.dispatcher.0, &shared.reactor, &shared.sal_writer_excl, target_id,
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
    let t_mut_start = Instant::now();

    let cat_ptr_raw = shared.catalog.0;
    // Discard any stale queue entries from a prior failed DDL so they don't
    // piggyback on this one.
    let _ = unsafe { (*cat_ptr_raw).drain_pending_broadcasts() };

    if let Err(e) = guard_panic("DDL", || {
        let cat = unsafe { &mut *cat_ptr_raw };
        cat.ingest_to_family(target_id, &batch)?;
        let dag = cat.get_dag_ptr();
        unsafe { (*dag).evaluate_dag(target_id, batch); }
        Ok(())
    }) {
        // A partial cascade may have pushed entries before the failing hook;
        // clear them so the next DDL doesn't inherit half-applied broadcasts.
        let _ = unsafe { (*cat_ptr_raw).drain_pending_broadcasts() };
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
    let drained = unsafe { (*cat_ptr_raw).drain_pending_broadcasts() };
    let fsync_fut = {
        let _sal_excl = shared.sal_writer_excl.lock().await;
        let disp_ptr_raw = shared.dispatcher.0;
        if let Err(e) = guard_panic("broadcast_ddl", || {
            for (tid, bat) in &drained {
                unsafe { (*disp_ptr_raw).broadcast_ddl(*tid, bat)?; }
            }
            Ok::<(), String>(())
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
    let col_names = shared.get_col_names_bytes(target_id);
    let mut name_refs_arr = [&[] as &[u8]; 64];
    for (i, n) in col_names.iter().enumerate().take(64) {
        name_refs_arr[i] = n.as_slice();
    }
    let col_names_opt = Some(&name_refs_arr[..col_names.len()]);
    let buf = encode_response_buffer(
        target_id, client_id, result, STATUS_OK, b"",
        schema, col_names_opt, seek_pk_lo,
    );
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
        if wire.columns[i].nullable != expected.columns[i].nullable {
            return Err(format!(
                "Schema mismatch at column {}: expected nullable={}, got {}",
                i, expected.columns[i].nullable, wire.columns[i].nullable,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, type_code};

    fn two_col_schema(col1_nullable: u8) -> SchemaDescriptor {
        let mut sd = SchemaDescriptor::default();
        sd.num_columns = 2;
        sd.pk_index = 0;
        sd.columns[0] = SchemaColumn::new(type_code::U64, 0);
        sd.columns[1] = SchemaColumn::new(type_code::I64, col1_nullable);
        sd
    }

    #[test]
    fn validate_schema_match_ok() {
        let sd = two_col_schema(0);
        assert!(validate_schema_match(&sd, &sd).is_ok());
    }

    #[test]
    fn validate_schema_match_rejects_column_count_mismatch() {
        let mut wire = two_col_schema(0);
        wire.num_columns = 1;
        assert!(validate_schema_match(&wire, &two_col_schema(0)).is_err());
    }

    #[test]
    fn validate_schema_match_rejects_pk_index_mismatch() {
        let mut wire = two_col_schema(0);
        wire.pk_index = 1;
        assert!(validate_schema_match(&wire, &two_col_schema(0)).is_err());
    }

    #[test]
    fn validate_schema_match_rejects_type_code_mismatch() {
        let mut wire = two_col_schema(0);
        wire.columns[1].type_code = type_code::F64;
        assert!(validate_schema_match(&wire, &two_col_schema(0)).is_err());
    }

    #[test]
    fn validate_schema_match_rejects_nullable_mismatch() {
        let wire     = two_col_schema(0); // col1 not-nullable
        let expected = two_col_schema(1); // col1 nullable
        assert!(validate_schema_match(&wire, &expected).is_err());
    }
}
