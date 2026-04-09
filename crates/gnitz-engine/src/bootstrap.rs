//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop. After this module,
//! main.py is reduced to arg parsing + one FFI call.

use std::collections::HashMap;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::executor::ServerExecutor;
use crate::ipc::{
    self, FLAG_PUSH, FLAG_DDL_SYNC, SAL_MMAP_SIZE, W2M_REGION_SIZE,
    SalWriter, SalReader, W2mWriter, W2mReceiver,
};
use crate::ipc_sys;
use crate::sys;
use crate::master::MasterDispatcher;
use crate::worker::WorkerProcess;

// ---------------------------------------------------------------------------
// Partition assignment (mirrors gnitz/dbsp/ops/exchange.py)
// ---------------------------------------------------------------------------

const NUM_PARTITIONS: u32 = 256;

fn partition_range(worker_id: u32, num_workers: u32) -> (u32, u32) {
    let chunk = NUM_PARTITIONS / num_workers;
    let start = worker_id * chunk;
    let end = if worker_id == num_workers - 1 {
        NUM_PARTITIONS
    } else {
        (worker_id + 1) * chunk
    };
    (start, end)
}

// ---------------------------------------------------------------------------
// SAL recovery
// ---------------------------------------------------------------------------

/// Replay unflushed SAL push blocks for this worker's partitions.
///
/// Walks the SAL from offset 0, reading group headers. For FLAG_PUSH messages
/// with LSN > flushed LSN, decodes the wire data and ingests the batch.
fn recover_from_sal(
    sal_reader: &SalReader,
    catalog: &mut CatalogEngine,
) {
    // Build per-table max_flushed_lsn map
    let table_ids = catalog.iter_user_table_ids();
    let mut family_lsns: HashMap<i64, u64> = HashMap::new();
    for &tid in &table_ids {
        let lsn = catalog.get_max_flushed_lsn(tid);
        family_lsns.insert(tid, lsn);
    }

    let mut offset: u64 = 0;
    let mut replayed: u32 = 0;
    let mut last_epoch: u32 = 0;

    while (offset as usize) + 8 < SAL_MMAP_SIZE {
        let (msg, new_offset) = match sal_reader.try_read(offset) {
            Some(v) => v,
            None => break,
        };

        // Epoch fence: decreasing epoch = stale data from before checkpoint
        if last_epoch > 0 && msg.epoch < last_epoch {
            break;
        }
        last_epoch = msg.epoch;

        offset = new_offset;

        // Only process FLAG_PUSH with data for this worker
        if msg.flags & FLAG_PUSH == 0 {
            continue;
        }
        let data = match msg.wire_data {
            Some(d) => d,
            None => continue,
        };

        let tid = msg.target_id as i64;
        let flushed = match family_lsns.get(&tid) {
            Some(&lsn) => lsn,
            None => continue,
        };
        if msg.lsn <= flushed {
            continue;
        }

        // Decode wire data and extract batch
        if let Ok(decoded) = ipc::decode_wire(data) {
            if let Some(batch) = decoded.data_batch {
                let owned = *batch;
                if owned.count > 0 {
                    let _ = catalog.raw_store_ingest(tid, owned);
                    replayed += 1;
                }
            }
        }
    }

    if replayed > 0 {
        let msg = format!("SAL recovery: replayed {} blocks\n", replayed);
        unsafe { libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len()); }
    }
}

// ---------------------------------------------------------------------------
// System table SAL recovery (master-side, before forking workers)
// ---------------------------------------------------------------------------

/// Replay unflushed DDL_SYNC SAL entries into system tables.
///
/// Called on the master before forking workers. The DDL_SYNC broadcast
/// already carries the system table batch data, so no extra SAL entries
/// are needed — we just replay the same entries workers would see.
fn recover_system_tables_from_sal(
    sal_ptr: *const u8,
    catalog: &mut CatalogEngine,
) {
    let sal_reader = SalReader::new(sal_ptr, 0, SAL_MMAP_SIZE, -1);

    let mut offset: u64 = 0;
    let mut replayed: u32 = 0;
    let mut last_epoch: u32 = 0;

    while (offset as usize) + 8 < SAL_MMAP_SIZE {
        let (msg, new_offset) = match sal_reader.try_read(offset) {
            Some(v) => v,
            None => break,
        };

        // Epoch fence: decreasing epoch = stale data from before checkpoint
        if last_epoch > 0 && msg.epoch < last_epoch {
            break;
        }
        last_epoch = msg.epoch;
        offset = new_offset;

        if msg.flags & FLAG_DDL_SYNC == 0 {
            continue;
        }

        let tid = msg.target_id as i64;
        if tid >= FIRST_USER_TABLE_ID {
            continue;
        }

        let data = match msg.wire_data {
            Some(d) => d,
            None => continue,
        };

        if let Ok(decoded) = ipc::decode_wire(data) {
            if let Some(batch) = decoded.data_batch {
                if batch.count > 0 {
                    let _ = catalog.ingest_to_family(tid, &batch);
                    replayed += 1;
                }
            }
        }
    }

    if replayed > 0 {
        let msg = format!("SAL system table recovery: replayed {} entries\n", replayed);
        unsafe { libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len()); }
    }
}

// ---------------------------------------------------------------------------
// Backfill exchange views
// ---------------------------------------------------------------------------

/// Issue fan_out_backfill for every exchange-requiring view.
fn backfill_exchange_views(
    catalog: &mut CatalogEngine,
    dispatcher: &mut MasterDispatcher,
) {
    let table_ids = catalog.iter_user_table_ids();
    for vid in table_ids {
        if !catalog.dag.view_needs_exchange(vid) {
            continue;
        }
        let source_ids = catalog.dag.get_source_ids(vid);
        for source_id in source_ids {
            if !catalog.dag.tables.contains_key(&source_id) {
                continue;
            }
            if let Err(e) = dispatcher.fan_out_backfill(vid, source_id) {
                let msg = format!("backfill error: {}\n", e);
                unsafe {
                    libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Server main entry point
// ---------------------------------------------------------------------------

/// Single entry point for the entire server bootstrap.
///
/// 1. Raises fd limit
/// 2. Opens catalog
/// 3. Allocates SAL/W2M/eventfds, forks workers, runs recovery,
///    creates dispatcher, runs executor
///
/// Returns 0 on clean exit, non-zero on error.
pub fn server_main(
    data_dir: &str,
    socket_path: &str,
    num_workers: u32,
    log_level: u32,
) -> i32 {
    // Raise fd limit (partition directories + shard files)
    sys::raise_fd_limit(65536);

    gnitz_info!("Opening database at {}", data_dir);

    let catalog = match CatalogEngine::open(data_dir) {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("Error: failed to open catalog: {}\n", e);
            unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
            return 1;
        }
    };
    let catalog_ptr = Box::into_raw(Box::new(catalog));

    let nw = num_workers as usize;
    {
        let msg = format!("Starting {} workers\n", num_workers);
        unsafe { libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len()); }
        let msg = format!(
            "Worker logs: {}/worker_N.log (N=0..{})\n",
            data_dir,
            num_workers - 1
        );
        unsafe { libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len()); }
    }

    // --- Shared Append-Only Log (file-backed, master→all workers) ---
    let sal_path = format!("{}/wal.sal\0", data_dir);
    let sal_fd = unsafe {
        libc::open(
            sal_path.as_ptr() as *const libc::c_char,
            libc::O_RDWR | libc::O_CREAT,
            0o644,
        )
    };
    if sal_fd < 0 {
        let msg = b"Error: failed to open SAL file\n";
        unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
        return 1;
    }
    sys::try_set_nocow(sal_fd);
    // Check existing size and fallocate if needed
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    unsafe { libc::fstat(sal_fd, &mut stat); }
    if (stat.st_size as usize) < SAL_MMAP_SIZE {
        sys::fallocate(sal_fd, SAL_MMAP_SIZE as i64);
    }
    let sal_ptr = ipc_sys::mmap_shared(sal_fd, SAL_MMAP_SIZE);
    if sal_ptr.is_null() {
        let msg = b"Error: failed to mmap SAL\n";
        unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
        return 1;
    }

    // --- System table SAL recovery (before forking workers) ---
    {
        let catalog = unsafe { &mut *catalog_ptr };
        recover_system_tables_from_sal(sal_ptr as *const u8, catalog);
        catalog.flush_all_system_tables();
    }

    // --- W2M regions (memfd-backed, one per worker→master) ---
    let mut w2m_ptrs: Vec<*mut u8> = Vec::with_capacity(nw);
    let mut w2m_fds: Vec<i32> = Vec::with_capacity(nw);
    let mut w2m_sizes: Vec<u64> = Vec::with_capacity(nw);
    for w in 0..nw {
        let name = format!("w2m_{}", w);
        let wfd = ipc_sys::memfd_create(name.as_bytes());
        if wfd < 0 {
            let msg = b"Error: memfd_create failed\n";
            unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
            return 1;
        }
        sys::ftruncate(wfd, W2M_REGION_SIZE as i64);
        let wptr = ipc_sys::mmap_shared(wfd, W2M_REGION_SIZE);
        if wptr.is_null() {
            let msg = b"Error: mmap W2M failed\n";
            unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
            return 1;
        }
        // Hint THP backing for the W2M region (memfd/shmem backing).
        // Requires: echo advise > /sys/kernel/mm/transparent_hugepage/shmem_enabled
        // If shmem_enabled remains "never", this call is silently inert — no harm.
        sys::madvise_hugepage(wptr, W2M_REGION_SIZE);
        // Initialize write cursor to W2M_HEADER_SIZE (skips the header)
        use std::sync::atomic::{AtomicU64, Ordering};
        unsafe {
            let atomic = &*(wptr as *const AtomicU64);
            atomic.store(ipc::W2M_HEADER_SIZE as u64, Ordering::Release);
        }
        w2m_ptrs.push(wptr);
        w2m_fds.push(wfd);
        w2m_sizes.push(W2M_REGION_SIZE as u64);
    }

    // --- Eventfds (cross-process signaling) ---
    let mut m2w_efds: Vec<i32> = Vec::with_capacity(nw);
    let mut w2m_efds: Vec<i32> = Vec::with_capacity(nw);
    for _ in 0..nw {
        let m2w = ipc_sys::eventfd_create();
        let w2m = ipc_sys::eventfd_create();
        if m2w < 0 || w2m < 0 {
            let msg = b"Error: eventfd_create failed\n";
            unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
            return 1;
        }
        m2w_efds.push(m2w);
        w2m_efds.push(w2m);
    }

    // Log fd assignments
    {
        let msg = format!("SAL fd={}\n", sal_fd);
        unsafe { libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len()); }
        for w in 0..nw {
            let msg = format!(
                "W{} m2w_efd={} w2m_efd={} w2m_fd={}\n",
                w, m2w_efds[w], w2m_efds[w], w2m_fds[w]
            );
            unsafe { libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len()); }
        }
    }

    let master_pid = unsafe { libc::getpid() };

    // --- Fork workers ---
    let mut worker_pids: Vec<i32> = vec![0; nw];
    for w in 0..nw {
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            let msg = b"Error: fork failed\n";
            unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
            return 1;
        }
        if pid == 0 {
            // --- Child process ---
            // Redirect stdout/stderr to worker log file
            let log_path = format!("{}/worker_{}.log\0", data_dir, w);
            unsafe {
                let log_fd = libc::open(
                    log_path.as_ptr() as *const libc::c_char,
                    libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
                    0o644,
                );
                if log_fd >= 0 {
                    libc::dup2(log_fd, 1);
                    libc::dup2(log_fd, 2);
                    libc::close(log_fd);
                }
            }

            // Close eventfds of OTHER workers
            for j in 0..nw {
                if j != w {
                    unsafe {
                        libc::close(m2w_efds[j]);
                        libc::close(w2m_efds[j]);
                    }
                }
            }

            let catalog = unsafe { &mut *catalog_ptr };

            // Set active partition range
            let (part_start, part_end) = partition_range(w as u32, num_workers);
            catalog.set_active_partitions(part_start, part_end);
            catalog.trim_worker_partitions(part_start, part_end);

            // Construct channel types for this worker
            let sal_reader = SalReader::new(
                sal_ptr as *const u8, w as u32, SAL_MMAP_SIZE, m2w_efds[w],
            );
            let w2m_writer = W2mWriter::new(
                w2m_ptrs[w], W2M_REGION_SIZE as u64, w2m_efds[w],
            );

            // SAL recovery — replay unflushed push data
            recover_from_sal(&sal_reader, catalog);

            catalog.invalidate_all_plans();

            let msg = format!(
                "Worker {} (pid {}) partitions [{}, {})\n",
                w,
                unsafe { libc::getpid() },
                part_start,
                part_end,
            );
            unsafe { libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len()); }

            // Re-init logging with worker tag
            let wtag = format!("W{}", w);
            crate::log::init(log_level, wtag.as_bytes());

            let mut worker = WorkerProcess::new(
                w as u32,
                nw,
                master_pid,
                catalog_ptr,
                sal_reader,
                w2m_writer,
            );
            worker.run();

            // Defensive — WorkerProcess::run() exits via libc::_exit
            unsafe { libc::_exit(0); }
        }

        worker_pids[w] = pid;
    }

    // --- Parent process ---
    let catalog = unsafe { &mut *catalog_ptr };
    catalog.close_user_table_partitions();
    catalog.set_active_partitions(0, 0);

    let sal_writer = SalWriter::new(sal_ptr, sal_fd, SAL_MMAP_SIZE as u64, m2w_efds.clone());
    let w2m_receiver = W2mReceiver::new(w2m_ptrs.clone(), w2m_efds.clone());

    let dispatcher = MasterDispatcher::new(
        nw,
        worker_pids.clone(),
        catalog_ptr,
        sal_writer,
        w2m_receiver,
    );
    let dispatcher_ptr = Box::into_raw(Box::new(dispatcher));

    // Wait for all workers to complete recovery and signal readiness
    let dispatcher = unsafe { &mut *dispatcher_ptr };
    if let Err(e) = dispatcher.collect_acks() {
        let msg = format!("Error collecting worker acks: {}\n", e);
        unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
        return 1;
    }

    // Reset SAL for fresh use (all workers have recovered)
    use std::sync::atomic::{AtomicU64, Ordering};
    unsafe {
        let atomic = &*(sal_ptr as *const AtomicU64);
        atomic.store(0, Ordering::Release);
    }
    dispatcher.reset_sal(0, 1);

    // Backfill exchange views
    backfill_exchange_views(catalog, dispatcher);

    // Create server socket and run executor
    gnitz_info!("Listening on {}", socket_path);
    let server_fd = sys::server_create(socket_path);
    if server_fd < 0 {
        let msg = b"Error: failed to create server socket\n";
        unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
        return 1;
    }
    let msg = b"GnitzDB ready\n";
    unsafe { libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len()); }

    ServerExecutor::run(catalog_ptr, dispatcher_ptr, server_fd)
}
