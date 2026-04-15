//! Server executor (Stage 4): async event loop built on the reactor.
//!
//! The master process owns a single `Reactor` that drives:
//! - the accept socket (client connections),
//! - all per-fd connection tasks (one per client),
//! - the committer task (group commit + checkpoint + fsync),
//! - the tick task (polls the existing sync tick state machine),
//! - the worker-crash watcher task.
//!
//! Sync tick code (`fan_out_tick`, `start_ticks_async_batch` +
//! `poll_tick_progress`, `TickIdleBarrier`) is retained and driven from
//! the tick task. Stage 6 retires it.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID, SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES};
use crate::committer::{self, CommitRequest};
use crate::ipc::{self, STATUS_OK, STATUS_ERROR, WireConflictMode};
use crate::master::MasterDispatcher;
use crate::reactor::{AsyncMutex, AsyncRwLock, Reactor, TickIdleBarrier, mpsc, oneshot};
use crate::schema::SchemaDescriptor;
use crate::storage::Batch;
use crate::util::guard_panic;

const TICK_COALESCE_ROWS: usize = 10_000;
const TICK_DEADLINE_MS: u64 = 20;
const WORKER_WATCH_MS: u64 = 100;
const TICK_POLL_MS: u64 = 1;

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

/// Shared executor state held by every task.
pub struct Shared {
    pub reactor: Rc<Reactor>,
    catalog: CatPtr,
    dispatcher: DispPtr,
    sal_fd: i32,
    committer_tx: mpsc::Sender<CommitRequest>,
    catalog_rwlock: Rc<AsyncRwLock>,
    tick_barrier: Rc<TickIdleBarrier>,
    /// Committer-owned counter; shared here so SCAN/SEEK handlers can
    /// report the same LSN the committer assigns.
    ingest_lsn: Rc<Cell<u64>>,
    last_tick_lsn: Rc<Cell<u64>>,
    /// Per-table row counter feeding the tick threshold.
    tick_rows: Rc<RefCell<HashMap<i64, usize>>>,
    tick_tids: Rc<RefCell<Vec<i64>>>,
    t_last_push: Rc<Cell<Option<Instant>>>,
    /// Set by SCAN handlers so the tick task fires on its next poll.
    force_tick: Rc<Cell<bool>>,
    /// Monotonic counter incremented at the END of every tick cycle.
    /// Used by SCAN to await "one tick cycle has completed since I
    /// requested it" — `tick_barrier.wait()` alone returns immediately
    /// when the barrier is idle and doesn't prove a tick ran.
    tick_generation: Rc<Cell<u64>>,
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

        let tick_barrier = Rc::new(TickIdleBarrier::new());
        let ingest_lsn = Rc::new(Cell::new(0u64));
        let last_tick_lsn = Rc::new(Cell::new(0u64));
        let tick_rows: Rc<RefCell<HashMap<i64, usize>>> = Rc::new(RefCell::new(HashMap::new()));
        let tick_tids: Rc<RefCell<Vec<i64>>> = Rc::new(RefCell::new(Vec::new()));
        let t_last_push = Rc::new(Cell::new(None));
        let force_tick = Rc::new(Cell::new(false));
        let tick_generation = Rc::new(Cell::new(0u64));

        let (committer_tx, committer_rx) = mpsc::unbounded::<CommitRequest>();

        let committer_shared = Rc::new(committer::Shared {
            reactor: Rc::clone(&reactor),
            disp_ptr: dispatcher,
            sal_fd,
            tick_barrier: Rc::clone(&tick_barrier),
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
            tick_barrier: Rc::clone(&tick_barrier),
            ingest_lsn: Rc::clone(&ingest_lsn),
            last_tick_lsn: Rc::clone(&last_tick_lsn),
            tick_rows: Rc::clone(&tick_rows),
            tick_tids: Rc::clone(&tick_tids),
            t_last_push: Rc::clone(&t_last_push),
            force_tick: Rc::clone(&force_tick),
            tick_generation: Rc::clone(&tick_generation),
            table_locks: RefCell::new(HashMap::new()),
            schema_cache: RefCell::new(HashMap::new()),
            col_names_cache: RefCell::new(HashMap::new()),
        });

        reactor.spawn(committer::run(committer_rx, committer_shared));
        reactor.spawn(accept_loop(Rc::clone(&shared)));
        reactor.spawn(tick_loop(Rc::clone(&shared)));
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
// Tick loop
// ---------------------------------------------------------------------------

async fn tick_loop(shared: Rc<Shared>) {
    loop {
        shared.reactor.timer(
            Instant::now() + Duration::from_millis(TICK_POLL_MS),
        ).await;
        let force = shared.force_tick.get();
        if force { shared.force_tick.set(false); }
        if !force && !should_fire(&shared) { continue; }

        let tids: Vec<i64> = {
            let mut tids = shared.tick_tids.borrow_mut();
            let v = std::mem::take(&mut *tids);
            shared.tick_rows.borrow_mut().clear();
            shared.t_last_push.set(None);
            v
        };
        // Empty / all-dropped tids still count as a completed tick
        // cycle for any SCAN handler waiting on tick_generation — those
        // scans don't need a tick to fire, only to observe that one
        // loop iteration ran after their request.
        if tids.is_empty() {
            shared.tick_generation.set(shared.tick_generation.get() + 1);
            continue;
        }

        let live_tids: Vec<i64> = tids.into_iter()
            .filter(|&tid| shared.cat().has_id(tid))
            .collect();
        if live_tids.is_empty() {
            shared.tick_generation.set(shared.tick_generation.get() + 1);
            continue;
        }

        shared.tick_barrier.set_active();
        if let Err(e) = shared.disp().start_ticks_async_batch(&live_tids) {
            gnitz_warn!("start_ticks_async_batch error: {}", e);
            shared.tick_barrier.notify_all();
            shared.tick_generation.set(shared.tick_generation.get() + 1);
            continue;
        }

        // Drive the existing sync tick state machine to completion, yielding
        // to the reactor between polls.
        loop {
            match shared.disp().poll_tick_progress() {
                Ok(true) => break,
                Ok(false) => {
                    shared.reactor.timer(
                        Instant::now() + Duration::from_millis(TICK_POLL_MS),
                    ).await;
                }
                Err(e) => {
                    gnitz_warn!("poll_tick_progress error: {}", e);
                    break;
                }
            }
        }
        // W2M reset requires every reader to have drained: a worker's
        // scan/push ACK written after its tick ACK would be orphaned
        // by zeroing write_cursor. See async-invariants.md §III.1.
        while !shared.reactor.all_in_flight_zero() {
            shared.reactor.timer(
                Instant::now() + Duration::from_millis(TICK_POLL_MS),
            ).await;
        }
        shared.disp().reset_w2m_cursors();
        shared.reactor.reset_w2m_cursors();
        shared.last_tick_lsn.set(shared.ingest_lsn.get());
        shared.tick_generation.set(shared.tick_generation.get() + 1);
        shared.tick_barrier.notify_all();
    }
}

fn should_fire(shared: &Rc<Shared>) -> bool {
    let tr = shared.tick_rows.borrow();
    if tr.is_empty() { return false; }
    for (_tid, &rows) in tr.iter() {
        if rows >= TICK_COALESCE_ROWS { return true; }
    }
    if let Some(t) = shared.t_last_push.get() {
        if t.elapsed().as_millis() as u64 >= TICK_DEADLINE_MS { return true; }
    }
    false
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
        // L7: gate against sync-tick W2M drain.
        shared.tick_barrier.wait().await;
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
    // L7: don't race the sync-tick W2M drain.
    shared.tick_barrier.wait().await;
    let result = if target_id >= FIRST_USER_TABLE_ID {
        MasterDispatcher::fan_out_seek_async(
            shared.dispatcher.0, &shared.reactor, target_id, pk_lo, pk_hi,
        ).await
    } else {
        // V.4 panic isolation: a panic would kill the connection task and
        // leak the fd. Real seek errors are silently mapped to None.
        let cat_ptr = shared.catalog.0;
        guard_panic("seek", || Ok(unsafe {
            (*cat_ptr).seek_family(target_id, pk_lo, pk_hi).ok().flatten()
        }))
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
    shared.tick_barrier.wait().await;
    let result = if target_id >= FIRST_USER_TABLE_ID {
        MasterDispatcher::fan_out_seek_by_index_async(
            shared.dispatcher.0, &shared.reactor, target_id, col_idx, key_lo, key_hi,
        ).await
    } else {
        let cat_ptr = shared.catalog.0;
        guard_panic("seek_by_index", || Ok(unsafe {
            (*cat_ptr).seek_by_index(target_id, col_idx, key_lo, key_hi).ok().flatten()
        }))
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
    // Drain pending ticks (on source tables — views derive through the
    // DAG). One gen-advance is insufficient: a push that landed after
    // tick_tids was drained stays pending across that tick, so loop
    // until tick_rows is empty.
    loop {
        let pending = !shared.tick_rows.borrow().is_empty();
        if !pending { break; }
        let gen_before = shared.tick_generation.get();
        shared.force_tick.set(true);
        while shared.tick_generation.get() == gen_before {
            shared.reactor.timer(
                Instant::now() + Duration::from_millis(TICK_POLL_MS),
            ).await;
        }
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

    let disp_ptr_raw = shared.dispatcher.0;
    let ddl_for_broadcast = ddl_clone;
    if let Err(e) = guard_panic("broadcast_ddl", || unsafe {
        (*disp_ptr_raw).broadcast_ddl(target_id, &ddl_for_broadcast)
    }) {
        send_error(shared, fd, target_id, client_id, e.as_bytes()).await;
        return;
    }
    let t_bcast_done = Instant::now();
    let fsync_rc = shared.reactor.fsync(shared.sal_fd).await;
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
