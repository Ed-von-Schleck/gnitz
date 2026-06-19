//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.

use std::collections::{HashMap, HashSet};

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::foundation::posix_io;
use crate::foundation::syscall;
use crate::query::RelationKind;
use crate::runtime::executor::ServerExecutor;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::sal::{SalReader, SalWriter, FLAG_DDL_SYNC, FLAG_PUSH, FLAG_TXN_COMMIT, SAL_MMAP_SIZE};
use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::w2m_ring::{self, W2M_REGION_SIZE};
use crate::runtime::wire as ipc;
use crate::runtime::worker::WorkerProcess;

// ---------------------------------------------------------------------------
// Partition assignment
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
// SAL recovery (Design 2: LSN as the atomic unit)
// ---------------------------------------------------------------------------
//
// Two-pass walk over the SAL:
//
//   Pass 1 — collect every LSN that has its commit sentinel on disk
//            (FLAG_TXN_COMMIT). Any LSN without one is "uncommitted" and
//            its groups are skipped at apply time. Half-completed DDL
//            (orphan COL_TAB) is therefore impossible: if the sentinel
//            never made it to disk, none of the zone's groups apply.
//
//   Pass 2 — walk again. For each group whose LSN is committed and
//            > family_lsns[tid] (table not already flushed past this
//            LSN), hand the decoded batch to the apply closure.
//
// The same walker handles both system-table replay (master pre-fork) and
// user-table replay (per-worker post-fork): callers vary only the
// family_lsns scope and the apply closure.

/// Pass 1: walk the SAL collecting committed LSNs. Honours the epoch
/// fence (decreasing epoch terminates the walk).
fn collect_committed_lsns(sal_reader: &SalReader) -> HashSet<u64> {
    let mut committed: HashSet<u64> = HashSet::new();
    let mut offset: u64 = 0;
    let mut last_epoch: u32 = 0;
    while (offset as usize) + 8 < SAL_MMAP_SIZE {
        let (msg, new_offset) = match sal_reader.try_read(offset) {
            Some(v) => v,
            None => break,
        };
        if last_epoch > 0 && msg.epoch < last_epoch {
            break;
        }
        last_epoch = msg.epoch;
        offset = new_offset;
        if msg.flags & FLAG_TXN_COMMIT != 0 {
            committed.insert(msg.lsn);
        }
    }
    committed
}

/// Pass 2: walk the SAL applying every committed group whose LSN is
/// in `family_lsns` and exceeds the recorded flushed LSN. The closure
/// receives the decoded batch and may filter by flag (e.g. master
/// applies only FLAG_DDL_SYNC, worker only FLAG_PUSH).
fn recover_sal<F>(
    sal_reader: &SalReader,
    catalog: &mut CatalogEngine,
    family_lsns: &HashMap<i64, u64>,
    mut apply: F,
) -> u32
where
    F: FnMut(&mut CatalogEngine, &crate::runtime::sal::SalMessage, ipc::DecodedWire) -> bool,
{
    let committed = collect_committed_lsns(sal_reader);

    let mut offset: u64 = 0;
    let mut applied: u32 = 0;
    let mut last_epoch: u32 = 0;
    while (offset as usize) + 8 < SAL_MMAP_SIZE {
        let (msg, new_offset) = match sal_reader.try_read(offset) {
            Some(v) => v,
            None => break,
        };
        if last_epoch > 0 && msg.epoch < last_epoch {
            break;
        }
        last_epoch = msg.epoch;
        offset = new_offset;

        if !committed.contains(&msg.lsn) {
            continue;
        }

        let tid = msg.target_id as i64;
        let flushed = match family_lsns.get(&tid) {
            Some(&lsn) => lsn,
            None => continue,
        };
        if msg.lsn <= flushed {
            continue;
        }

        let data = match msg.wire_data {
            Some(d) => d,
            None => continue,
        };
        let decoded = match ipc::decode_wire(data) {
            Ok(d) => d,
            Err(_) => continue,
        };
        if apply(catalog, &msg, decoded) {
            applied += 1;
        }
    }
    applied
}

/// Master pre-fork system-table replay. Builds the system-table family
/// map (Phase 5 will populate non-zero values), then walks the SAL via
/// `recover_sal`. The closure ingests every committed FLAG_DDL_SYNC
/// batch addressed to a system table — orphan COL_TAB rows from a
/// crashed DDL are skipped because their zone never closed.
fn recover_system_tables_from_sal(sal_ptr: *const u8, catalog: &mut CatalogEngine) {
    let sal_reader = SalReader::new(sal_ptr, 0, SAL_MMAP_SIZE, -1);
    let all_lsns = catalog.collect_all_flushed_lsns();
    let family_lsns: HashMap<i64, u64> = all_lsns
        .into_iter()
        .filter(|&(tid, _)| tid > 0 && tid < FIRST_USER_TABLE_ID)
        .collect();

    let replayed = recover_sal(&sal_reader, catalog, &family_lsns, |cat, msg, decoded| {
        if msg.flags & FLAG_DDL_SYNC == 0 {
            return false;
        }
        let batch = match decoded.data_batch {
            Some(b) if b.count > 0 => b,
            _ => return false,
        };
        cat.ingest_to_family(msg.target_id as i64, &batch).is_ok()
    });

    if replayed > 0 {
        let msg = format!("SAL system table recovery: replayed {replayed} entries\n");
        unsafe {
            libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len());
        }
    }
}

/// Per-worker post-fork user-table replay. The worker's `sal_reader`
/// already has the worker's per-cursor view; the apply closure decodes
/// each FLAG_PUSH group's batch and replays it through the unique-pk
/// path so retractions cancel correctly.
fn recover_from_sal(sal_reader: &SalReader, catalog: &mut CatalogEngine) {
    let all_lsns = catalog.collect_all_flushed_lsns();
    let family_lsns: HashMap<i64, u64> = all_lsns
        .into_iter()
        .filter(|&(tid, _)| tid >= FIRST_USER_TABLE_ID)
        .collect();

    let replayed = recover_sal(sal_reader, catalog, &family_lsns, |cat, msg, decoded| {
        if msg.flags & FLAG_PUSH == 0 {
            return false;
        }
        let batch = match decoded.data_batch {
            Some(b) if b.count > 0 => b,
            _ => return false,
        };
        cat.replay_ingest(msg.target_id as i64, batch).is_ok()
    });

    if replayed > 0 {
        let msg = format!("SAL recovery: replayed {replayed} blocks\n");
        unsafe {
            libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len());
        }
    }
}

// ---------------------------------------------------------------------------
// Backfill exchange views
// ---------------------------------------------------------------------------

/// Issue fan_out_backfill once per base table that roots an exchange view.
///
/// `drive_dag(base)` re-derives that base's whole closure (every view above it)
/// exactly once, so one backfill per base fills all of its exchange and
/// intermediate views without re-driving any shared closure.
fn backfill_exchange_views(catalog: &mut CatalogEngine, dispatcher: &mut MasterDispatcher) -> Result<(), String> {
    for base in catalog.dag.exchange_base_tables() {
        // view_id = 0: boot's whole-closure mode — drive every view above
        // `base` once (each starts empty at boot).
        dispatcher.fan_out_backfill(0, base)?;
    }
    Ok(())
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
pub fn server_main(data_dir: &str, socket_path: &str, num_workers: u32, log_level: u32) -> i32 {
    // Raise fd limit (partition directories + shard files)
    posix_io::raise_fd_limit(65536);

    gnitz_info!("Opening database at {}", data_dir);

    let catalog = match CatalogEngine::open(data_dir) {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("Error: failed to open catalog: {e}\n");
            unsafe {
                libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            }
            return 1;
        }
    };
    let catalog_ptr = Box::into_raw(Box::new(catalog));

    let nw = num_workers as usize;
    {
        let msg = format!("Starting {num_workers} workers\n");
        unsafe {
            libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len());
        }
        let msg = format!("Worker logs: {}/worker_N.log (N=0..{})\n", data_dir, num_workers - 1);
        unsafe {
            libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len());
        }
    }

    // --- Shared Append-Only Log (file-backed, master→all workers) ---
    let sal_path = format!("{data_dir}/wal.sal\0");
    let sal_fd = unsafe {
        libc::open(
            sal_path.as_ptr() as *const libc::c_char,
            libc::O_RDWR | libc::O_CREAT,
            0o644,
        )
    };
    if sal_fd < 0 {
        let msg = b"Error: failed to open SAL file\n";
        unsafe {
            libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
        }
        return 1;
    }
    posix_io::try_set_nocow(sal_fd);
    // Check existing size and fallocate if needed
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    unsafe {
        libc::fstat(sal_fd, &mut stat);
    }
    if (stat.st_size as usize) < SAL_MMAP_SIZE {
        posix_io::fallocate(sal_fd, SAL_MMAP_SIZE as i64);
    }
    let sal_ptr = syscall::mmap_shared(sal_fd, SAL_MMAP_SIZE);
    if sal_ptr.is_null() {
        let msg = b"Error: failed to mmap SAL\n";
        unsafe {
            libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
        }
        return 1;
    }
    // Pre-fault writable PTEs so the hot write path never page-faults.
    // MADV_POPULATE_WRITE (Linux 5.14+) installs dirty PTEs and triggers
    // the filesystem mkwrite callback upfront, without dirtying page contents.
    posix_io::madvise_populate_write(sal_ptr, SAL_MMAP_SIZE);

    // --- V2 migration: clean break, sentinel marker file ---
    //
    // Design 2 changes the SAL semantics (LSN as the atomic unit, commit
    // sentinels mark zone closure). A pre-V2 SAL has no sentinels, so
    // every uncommitted-by-the-new-rules group would be skipped at
    // recovery — which would silently drop unflushed work.
    //
    // Contract: upgrading to V2 requires a clean shutdown. On first boot
    // under V2, any pre-V2 SAL contents are discarded and shard files
    // (durable up to the last checkpoint) are authoritative. The marker
    // is touched once so subsequent boots skip the wipe.
    {
        let v2_marker_path = format!("{data_dir}/wal.sal.v2");
        if !std::path::Path::new(&v2_marker_path).exists() {
            unsafe {
                libc::memset(sal_ptr as *mut libc::c_void, 0, SAL_MMAP_SIZE);
            }
            posix_io::madvise_populate_write(sal_ptr, SAL_MMAP_SIZE);
            if let Err(e) = std::fs::File::create(&v2_marker_path) {
                let msg = format!("Error: failed to create wal.sal.v2 marker: {e}\n");
                unsafe {
                    libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
                }
                return 1;
            }
            gnitz_info!("First boot under V2: pre-V2 SAL discarded, shards are authoritative");
        }
    }

    // --- System table SAL recovery (before forking workers) ---
    {
        let catalog = unsafe { &mut *catalog_ptr };
        recover_system_tables_from_sal(sal_ptr as *const u8, catalog);
        // Abort before forking workers and long before the SAL reset: the
        // replayed DDL lives only in master memory until this flush makes it
        // durable, so a swallowed failure followed by the SAL reset destroys
        // its only durable copy (and gc_orphan_directories would later delete
        // the now-catalog-less entities' flushed shards).
        if let Err(e) = catalog.flush_all_system_tables() {
            let msg = format!("Error: {e}\n");
            unsafe {
                libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            }
            return 1;
        }
        #[cfg(debug_assertions)]
        if std::env::var("GNITZ_INJECT_SYS_FLUSH_ERROR").is_ok() {
            let msg = b"Error: injected system table flush fault\n";
            unsafe {
                libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            }
            return 1;
        }

        // Reclaim table/view/index directories whose DROP committed but whose
        // deferred deletion was lost to a crash before the next checkpoint.
        // Runs only now that both shard replay and SAL replay have populated
        // dag.tables, so a SAL-committed-but-unflushed CREATE is not mistaken
        // for an orphan.
        catalog.gc_orphan_directories();
    }

    // --- W2M regions (memfd-backed, one per worker→master) ---
    let mut w2m_ptrs: Vec<*mut u8> = Vec::with_capacity(nw);
    let mut w2m_fds: Vec<i32> = Vec::with_capacity(nw);
    let mut w2m_sizes: Vec<u64> = Vec::with_capacity(nw);
    for w in 0..nw {
        let name = format!("w2m_{w}");
        let wfd = syscall::memfd_create(name.as_bytes());
        if wfd < 0 {
            let msg = b"Error: memfd_create failed\n";
            unsafe {
                libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            }
            return 1;
        }
        posix_io::ftruncate(wfd, W2M_REGION_SIZE as i64);
        let wptr = syscall::mmap_shared(wfd, W2M_REGION_SIZE);
        if wptr.is_null() {
            let msg = b"Error: mmap W2M failed\n";
            unsafe {
                libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            }
            return 1;
        }
        // Hint THP backing for the W2M region (memfd/shmem backing).
        // Requires: echo advise > /sys/kernel/mm/transparent_hugepage/shmem_enabled
        // If shmem_enabled remains "never", this call is silently inert — no harm.
        posix_io::madvise_hugepage(wptr, W2M_REGION_SIZE);
        // Initialize the SPSC ring header (cursors at HEADER_SIZE,
        // capacity = full region).
        unsafe {
            w2m_ring::init_region(wptr, W2M_REGION_SIZE as u64);
        }
        w2m_ptrs.push(wptr);
        w2m_fds.push(wfd);
        w2m_sizes.push(W2M_REGION_SIZE as u64);
    }

    // --- M2W eventfds (master→worker signaling; W2M uses futex now) ---
    let mut m2w_efds: Vec<i32> = Vec::with_capacity(nw);
    for _ in 0..nw {
        let m2w = syscall::eventfd_create();
        if m2w < 0 {
            let msg = b"Error: eventfd_create failed\n";
            unsafe {
                libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            }
            return 1;
        }
        m2w_efds.push(m2w);
    }

    // Log fd assignments
    {
        let msg = format!("SAL fd={sal_fd}\n");
        unsafe {
            libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len());
        }
        for w in 0..nw {
            let msg = format!("W{} m2w_efd={} w2m_fd={}\n", w, m2w_efds[w], w2m_fds[w]);
            unsafe {
                libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len());
            }
        }
    }

    let master_pid = unsafe { libc::getpid() };

    // --- Fork workers ---
    let mut worker_pids: Vec<i32> = vec![0; nw];
    for w in 0..nw {
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            let msg = b"Error: fork failed\n";
            unsafe {
                libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
            }
            return 1;
        }
        if pid == 0 {
            // --- Child process ---
            // Die immediately if the master exits for any reason.  The
            // getppid() check in sal_reader.wait() is a belt-and-suspenders
            // fallback; this closes the ~30s polling gap.
            unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) };
            // Re-check: parent may have died in the fork→prctl window.
            if unsafe { libc::getppid() } != master_pid {
                unsafe { libc::_exit(0) };
            }

            // Redirect stdout/stderr to worker log file
            let log_path = format!("{data_dir}/worker_{w}.log\0");
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

            // Close M2W eventfds of OTHER workers (W2M uses futex, no fd).
            for (j, &efd) in m2w_efds.iter().enumerate().take(nw) {
                if j != w {
                    unsafe {
                        libc::close(efd);
                    }
                }
            }

            let catalog = unsafe { &mut *catalog_ptr };

            // Set active partition range
            let (part_start, part_end) = partition_range(w as u32, num_workers);
            catalog.set_active_partitions(part_start, part_end);
            catalog.trim_worker_partitions(part_start, part_end);
            // Re-home inherited single-partition (replicated / replicated-derived)
            // stores from the pre-fork master's `part_0` to THIS worker's own
            // `part_{part_start}` dir before any flush — all workers share the data
            // directory, so a fixed `part_0` would collide. The inherited store is
            // empty; FLAG_PUSH replay (below) fills the re-homed store.
            if let Err(e) = catalog.rehome_single_partition_stores(part_start) {
                let msg = format!("W{w} rehome single-partition stores failed: {e}\n");
                unsafe {
                    libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
                }
            }

            // Construct channel types for this worker
            let sal_reader = SalReader::new(sal_ptr as *const u8, w as u32, SAL_MMAP_SIZE, m2w_efds[w]);
            let w2m_writer = W2mWriter::new(w2m_ptrs[w], W2M_REGION_SIZE as u64);

            // SAL recovery — replay unflushed push data
            recover_from_sal(&sal_reader, catalog);

            // Durability: flush replayed rows to shards before accepting requests.
            // reset_sal() resets the write cursor to 0, so a second crash before
            // a checkpoint would overwrite SAL entries and make replayed data
            // unreachable (SAL walk stops at the first partially-overwritten group).
            // The verdict rides the startup ACK (see worker.run): a failed flush
            // must abort boot before the master zeroes the SAL sentinel, or the
            // replayed rows' only durable copy is destroyed.
            let mut boot_flush_err: Option<String> = None;
            for tid in catalog.iter_user_table_ids() {
                if let Err(e) = catalog.flush_family(tid) {
                    boot_flush_err = Some(format!("boot flush of table {tid} failed: {e}"));
                    break;
                }
            }
            #[cfg(debug_assertions)]
            if boot_flush_err.is_none() && std::env::var("GNITZ_INJECT_BOOT_FLUSH_ERROR").is_ok() {
                boot_flush_err = Some("injected boot flush fault".to_string());
            }
            if let Some(e) = &boot_flush_err {
                // stderr is redirected to worker_N.log above.
                eprintln!("{e}");
            }

            catalog.invalidate_all_plans();

            // Rebuild the non-exchange views the master's post-ACK exchange
            // backfill does not reach. recover_from_sal restored unflushed base
            // rows but bypassed view derivation, and the inline open-time backfill
            // is now live-only. The cascade re-derives the full dependent closure
            // of every base feeding an exchange view; fill only the non-exchange
            // views OUTSIDE that closure here, so neither pass double-counts and no
            // view is missed. Backfill sources before dependents (depth order) so a
            // view over another non-exchange view reads a populated source. Skip on
            // a doomed boot — worker.run will abort.
            if boot_flush_err.is_none() {
                let filled = catalog.dag.exchange_cascade_filled_views();
                let mut nx_views: Vec<i64> = catalog
                    .iter_user_table_ids()
                    .into_iter()
                    .filter(|&vid| {
                        !filled.contains(&vid)
                            && catalog
                                .dag
                                .tables
                                .get(&vid)
                                .is_some_and(|e| e.kind == RelationKind::View)
                            && !catalog.dag.view_needs_exchange(vid)
                    })
                    .collect();
                nx_views.sort_by_key(|&vid| catalog.dag.tables.get(&vid).map(|e| e.depth).unwrap_or(0));
                for vid in nx_views {
                    catalog.backfill_view(vid);
                }
            }

            let msg = format!(
                "Worker {} (pid {}) partitions [{}, {})\n",
                w,
                unsafe { libc::getpid() },
                part_start,
                part_end,
            );
            unsafe {
                libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len());
            }

            // Re-init logging with worker tag
            let wtag = format!("W{w}");
            crate::foundation::log::init(log_level, wtag.as_bytes());

            let mut worker = WorkerProcess::new(w as u32, nw, master_pid, catalog_ptr, sal_reader, w2m_writer);
            let rc = worker.run(boot_flush_err);

            // Defensive — WorkerProcess::run() exits via libc::_exit
            unsafe {
                libc::_exit(rc);
            }
        }

        worker_pids[w] = pid;
    }

    // --- Parent process ---
    let catalog = unsafe { &mut *catalog_ptr };
    catalog.close_user_table_partitions();
    catalog.set_active_partitions(0, 0);

    let sal_writer = SalWriter::new(sal_ptr, sal_fd, SAL_MMAP_SIZE as u64, m2w_efds.clone());
    let w2m_receiver = W2mReceiver::new(w2m_ptrs.clone());

    let dispatcher = MasterDispatcher::new(nw, worker_pids.clone(), catalog_ptr, sal_writer, w2m_receiver);
    let dispatcher_ptr = Box::into_raw(Box::new(dispatcher));

    // Wait for all workers to complete recovery and signal readiness
    let dispatcher = unsafe { &mut *dispatcher_ptr };
    if let Err(e) = dispatcher.collect_acks() {
        let msg = format!("Error collecting worker acks: {e}\n");
        unsafe {
            libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
        }
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
    if let Err(e) = backfill_exchange_views(catalog, dispatcher) {
        let msg = format!("Error: backfill failed: {e}\n");
        unsafe {
            libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
        }
        return 1;
    }

    // Create server socket and run executor
    gnitz_info!("Listening on {}", socket_path);
    let server_fd = posix_io::server_create(socket_path);
    if server_fd < 0 {
        let msg = b"Error: failed to create server socket\n";
        unsafe {
            libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len());
        }
        return 1;
    }
    let msg = b"GnitzDB ready\n";
    unsafe {
        libc::write(1, msg.as_ptr() as *const libc::c_void, msg.len());
    }

    ServerExecutor::run(catalog_ptr, dispatcher_ptr, server_fd)
}

// ---------------------------------------------------------------------------
// Tests for the two-pass recovery primitives. The full end-to-end crash
// path is covered in `crates/gnitz-py/tests/test_crash_recovery.py`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::runtime::sal::{sal_write_group, SalReader, SalWriter, MAX_WORKERS};

    unsafe fn alloc_mmap(size: usize) -> *mut u8 {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_SHARED,
            -1,
            0,
        );
        assert_ne!(ptr, libc::MAP_FAILED);
        std::ptr::write_bytes(ptr as *mut u8, 0, size);
        ptr as *mut u8
    }

    unsafe fn free_mmap(ptr: *mut u8, size: usize) {
        libc::munmap(ptr as *mut libc::c_void, size);
    }

    /// Write one DDL_SYNC-flagged group with a 1-byte payload per worker.
    /// Returns the new cursor.
    unsafe fn write_ddl_group(
        ptr: *mut u8,
        cursor: u64,
        nw: u32,
        target_id: u32,
        lsn: u64,
        epoch: u32,
        size: u64,
    ) -> u64 {
        let payload = [0u8; 64];
        let ptrs: Vec<*const u8> = (0..nw).map(|_| payload.as_ptr()).collect();
        let sizes: Vec<u32> = (0..nw).map(|_| payload.len() as u32).collect();
        let res = sal_write_group(
            ptr,
            cursor,
            nw,
            target_id,
            lsn,
            FLAG_DDL_SYNC,
            epoch,
            size,
            ptrs.as_ptr(),
            sizes.as_ptr(),
        );
        assert_eq!(res.status, 0);
        res.new_cursor
    }

    #[test]
    fn test_recover_skips_uncommitted() {
        // Two groups at lsn=K are written without a sentinel — their
        // zone never closed (simulated crash). One group at lsn=K+1 is
        // closed with a sentinel. The committed set must contain only
        // K+1 (plus any FLAG_PUSH auto-commits, of which there are none
        // here).
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);
            let nw = 2u32;

            let mut cur = write_ddl_group(ptr, 0, nw, 100, 5, 1, size as u64);
            cur = write_ddl_group(ptr, cur, nw, 101, 5, 1, size as u64);
            // No sentinel for lsn=5 — zone never closed.

            cur = write_ddl_group(ptr, cur, nw, 102, 6, 1, size as u64);
            // Sentinel for lsn=6.
            let efds: Vec<i32> = (0..nw).map(|_| syscall::eventfd_create()).collect();
            let mut writer = SalWriter::new(ptr, -1, size as u64, efds.clone());
            writer.reset(cur, 1);
            writer.write_commit_sentinel(6).unwrap();

            let reader = SalReader::new(ptr as *const u8, 0, size, efds[0]);
            let committed = collect_committed_lsns(&reader);
            assert!(!committed.contains(&5), "lsn=5 has no sentinel and must be uncommitted");
            assert!(committed.contains(&6), "lsn=6 has a sentinel and must be committed");

            for &e in &efds {
                libc::close(e);
            }
            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_recover_applies_all_committed() {
        // Three DDL groups at lsn=K plus a sentinel at lsn=K. Recovery's
        // committed set must contain K. Pass 2 (simulated by hand here)
        // would then apply each of the three groups.
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);
            let nw = 3u32;

            let mut cur = write_ddl_group(ptr, 0, nw, 200, 9, 1, size as u64);
            cur = write_ddl_group(ptr, cur, nw, 201, 9, 1, size as u64);
            cur = write_ddl_group(ptr, cur, nw, 202, 9, 1, size as u64);

            let efds: Vec<i32> = (0..nw).map(|_| syscall::eventfd_create()).collect();
            let mut writer = SalWriter::new(ptr, -1, size as u64, efds.clone());
            writer.reset(cur, 1);
            writer.write_commit_sentinel(9).unwrap();

            let reader = SalReader::new(ptr as *const u8, 0, size, efds[0]);
            let committed = collect_committed_lsns(&reader);
            assert!(
                committed.contains(&9),
                "committed sentinel at lsn=9 must mark the zone closed"
            );

            // Walk the SAL by hand to count groups at lsn=9 with
            // FLAG_DDL_SYNC and no sentinel — these are the three apply
            // candidates.
            let mut applied = 0u32;
            let mut offset = 0u64;
            while (offset as usize) + 8 < size {
                let (msg, next) = match reader.try_read(offset) {
                    Some(v) => v,
                    None => break,
                };
                offset = next;
                if msg.lsn != 9 {
                    continue;
                }
                if msg.flags & FLAG_TXN_COMMIT != 0 {
                    continue;
                }
                if msg.flags & FLAG_DDL_SYNC != 0 && msg.wire_data.is_some() {
                    applied += 1;
                }
            }
            assert_eq!(applied, 3, "all three committed DDL groups apply");

            for &e in &efds {
                libc::close(e);
            }
            free_mmap(ptr, size);
        }
    }

    #[test]
    fn test_push_without_sentinel_skipped() {
        // Phase 6 invariant: a FLAG_PUSH group with no closing
        // FLAG_TXN_COMMIT sentinel is uncommitted and must be skipped.
        // Earlier transitional code auto-committed every FLAG_PUSH group;
        // that branch is gone now that the committer emits sentinels.
        unsafe {
            let size = 1 << 20;
            let ptr = alloc_mmap(size);
            let nw = 1u32;

            let payload = [0u8; 32];
            let ptrs = [payload.as_ptr()];
            let sizes = [payload.len() as u32];
            let res = sal_write_group(
                ptr,
                0,
                nw,
                50,
                11,
                FLAG_PUSH,
                1,
                size as u64,
                ptrs.as_ptr(),
                sizes.as_ptr(),
            );
            assert_eq!(res.status, 0);
            // No sentinel — zone unclosed.

            let efd = syscall::eventfd_create();
            let reader = SalReader::new(ptr as *const u8, 0, size, efd);
            let committed = collect_committed_lsns(&reader);
            assert!(
                !committed.contains(&11),
                "uncommitted push must NOT appear in committed set"
            );

            let _ = MAX_WORKERS;
            libc::close(efd);
            free_mmap(ptr, size);
        }
    }
}
