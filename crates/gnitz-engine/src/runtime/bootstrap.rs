//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.

use std::collections::{HashMap, HashSet};

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::foundation::posix_io;
use crate::query::RelationKind;
use crate::runtime::executor::ServerExecutor;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::sal::{sal_mmap_size, SalReader, SalWriter, FLAG_DDL_SYNC, FLAG_PUSH, FLAG_TXN_COMMIT};
use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::w2m_ring::{self, W2M_REGION_SIZE};
use crate::runtime::wire as ipc;
use crate::runtime::worker::{buffer_pending_delta, WorkerProcess};
use crate::storage::{partition_range, Batch};

/// Boot-progress line on stdout (the server log). EINTR/partial-write safe;
/// used instead of the log macros where the raw, untagged line is the
/// documented boot output (e.g. the "GnitzDB ready" marker tests wait for).
fn boot_log(msg: &str) {
    let bytes = msg.as_bytes();
    let mut off = 0;
    while off < bytes.len() {
        let rc = unsafe { libc::write(1, bytes[off..].as_ptr() as *const libc::c_void, bytes.len() - off) };
        if rc > 0 {
            off += rc as usize;
        } else if rc < 0 && posix_io::errno() == libc::EINTR {
            continue;
        } else {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// SAL recovery (Design 2: LSN as the atomic unit)
// ---------------------------------------------------------------------------
//
// Two-pass walk over the SAL:
//
//   Pass 1 — collect every LSN that has its commit sentinel on disk
//            (FLAG_TXN_COMMIT). Any LSN without one is "uncommitted" and
//            its groups are skipped at apply time. A whole CREATE is now one
//            zone — its N families ride one `FLAG_DDL_TXN` bundle committed
//            under a single zone LSN — so an orphan COL_TAB is impossible for a
//            CREATE, not just intra-zone: the CREATE's groups apply iff its one
//            sentinel is durable, i.e. all of COL_TAB and TABLE_TAB or none.
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
    while offset + 8 < sal_reader.mmap_size() {
        let (msg, new_offset) = match sal_reader.try_read(offset, None) {
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
    while offset + 8 < sal_reader.mmap_size() {
        let (msg, new_offset) = match sal_reader.try_read(offset, None) {
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
/// map from the flushed LSNs, then walks the SAL via
/// `recover_sal`. The closure ingests every committed FLAG_DDL_SYNC
/// batch addressed to a system table — orphan COL_TAB rows from a
/// crashed DDL are skipped because their zone never closed.
fn recover_system_tables_from_sal(sal_ptr: *const u8, catalog: &mut CatalogEngine) {
    let sal_reader = SalReader::new(sal_ptr, 0, sal_mmap_size(), -1);
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
        boot_log(&format!("SAL system table recovery: replayed {replayed} entries\n"));
    }
}

/// Fault-injection seam for the boot-recovery pipeline; no-op in release
/// builds. In a debug build with `GNITZ_INJECT_RECOVERY_PANIC=<stage>`, panics
/// when recovery reaches the named stage, so the crash-window E2E tests can cut
/// boot at a precise point.
fn inject_recovery_panic(stage: &str) {
    #[cfg(debug_assertions)]
    if std::env::var("GNITZ_INJECT_RECOVERY_PANIC").as_deref() == Ok(stage) {
        panic!("injected recovery panic at {stage}");
    }
    #[cfg(not(debug_assertions))]
    let _ = stage;
}

/// Base tables feeding ≥1 view, sorted for a reproducible drive order. The ONE
/// definition of the recovery sweep set: each worker buffers exactly these
/// tables' effective deltas during SAL replay, and the master's post-reset tick
/// sweep drains exactly these — same function, so the swept set equals the
/// buffered set by construction (no leak, no gap).
fn swept_base_tables(catalog: &mut CatalogEngine) -> Vec<i64> {
    let view_ids: Vec<i64> = catalog
        .dag
        .tables
        .iter()
        .filter(|(_, e)| e.kind == RelationKind::View)
        .map(|(&t, _)| t)
        .collect();
    catalog.dag.base_tables_reachable_from(view_ids)
}

/// Per-worker post-fork user-table replay. The worker's `sal_reader`
/// already has the worker's per-cursor view; the apply closure decodes
/// each FLAG_PUSH group's batch and applies it through the unique-pk path
/// (`ingest_returning_effective`, the exact call `handle_push` makes) so
/// retractions cancel correctly, and — for every base table feeding ≥1 view —
/// buffers the returned effective delta into the returned map. That map seeds
/// the worker's `pending_deltas`; the master's post-reset recovery tick sweep
/// drains it into the views. Viewless bases ingest-and-discard (nothing to
/// drive), so their tail never leaks into the sweep.
fn recover_from_sal(sal_reader: &SalReader, catalog: &mut CatalogEngine) -> HashMap<i64, Batch> {
    let all_lsns = catalog.collect_all_flushed_lsns();
    let family_lsns: HashMap<i64, u64> = all_lsns
        .into_iter()
        .filter(|&(tid, _)| tid >= FIRST_USER_TABLE_ID)
        .collect();

    let buffered_bases: HashSet<i64> = swept_base_tables(catalog).into_iter().collect();

    let mut pending: HashMap<i64, Batch> = HashMap::new();
    let replayed = recover_sal(sal_reader, catalog, &family_lsns, |cat, msg, decoded| {
        if msg.flags & FLAG_PUSH == 0 {
            return false;
        }
        let batch = match decoded.data_batch {
            Some(b) if b.count > 0 => b,
            _ => return false,
        };
        let tid = msg.target_id as i64;
        // Fires pre-readiness-ACK: the master's wait_all_workers probes
        // fail_if_worker_dead and fails boot BEFORE zeroing the SAL sentinel, so
        // the replayed data's only durable copy survives. A swallowed error here
        // would zero the sentinel and orphan the un-applied committed data.
        let effective = match cat.ingest_returning_effective(tid, batch) {
            Ok(b) => b,
            Err(e) => crate::gnitz_fatal_abort!(
                "SAL replay apply failed (table_id={}, lsn={}): {} — aborting \
                 before the SAL sentinel is reset",
                msg.target_id,
                msg.lsn,
                e,
            ),
        };
        // Buffer the effective delta for the sweep; viewless bases discard it
        // (nothing to drive).
        if buffered_bases.contains(&tid) {
            buffer_pending_delta(&mut pending, tid, effective);
        }
        true
    });

    if replayed > 0 {
        boot_log(&format!("SAL recovery: replayed {replayed} blocks\n"));
    }
    pending
}

/// Worker-boot catalog recovery, in load-bearing order:
///
/// 1. Rebuild every secondary index slice-local, replacing the fork-inherited
///    full parent-dir copy, BEFORE SAL replay — replay projects the committed
///    unflushed tail into each index exactly once (`ingest_store_and_indices`),
///    so a rebuild afterwards would double-count every replayed row.
/// 2. Replay unflushed push data from the SAL.
/// 3. Flush the replayed rows to shards before accepting requests: reset_sal()
///    resets the write cursor to 0, so a second crash before a checkpoint would
///    overwrite SAL entries and make replayed data unreachable (the SAL walk
///    stops at the first partially-overwritten group).
///
/// The Err rides the startup ACK (see worker.run): a failed boot must abort
/// before the master zeroes the SAL sentinel, or the replayed rows' only
/// durable copy is destroyed.
fn worker_boot_recovery(catalog: &mut CatalogEngine, sal_reader: &SalReader) -> Result<HashMap<i64, Batch>, String> {
    catalog
        .backfill_all_indexes()
        .map_err(|e| format!("boot index backfill failed: {e}"))?;
    let pending_deltas = recover_from_sal(sal_reader, catalog);
    // Keep the boot flush: the non-windowed recovery resets the SAL before the
    // master-driven tick sweep, so the replayed base rows must be shard-durable
    // first — else the reset would drop acknowledged tail data.
    for tid in catalog.iter_user_table_ids() {
        catalog
            .flush_family(tid)
            .map_err(|e| format!("boot flush of table {tid} failed: {e}"))?;
    }
    #[cfg(debug_assertions)]
    if std::env::var("GNITZ_INJECT_BOOT_FLUSH_ERROR").is_ok() {
        return Err("injected boot flush fault".to_string());
    }
    inject_recovery_panic("bootflush");
    Ok(pending_deltas)
}

// ---------------------------------------------------------------------------
// Recovery tick sweep + invalid-view rebuild
// ---------------------------------------------------------------------------

/// Apply the un-checkpointed SAL tail — buffered as `pending_deltas` during
/// replay — to every view via one master-driven blocking tick per reachable base,
/// on the freshly-reset SAL. Each tick drains that source's buffered delta through
/// `handle_tick` → `evaluate_dag`; empty-buffer sources still tick so exchange
/// views stay in lockstep. The sweep drives ALL views: resumed (valid) views are
/// extended state-exactly (single-source-per-epoch reduces the whole-tail drive to
/// a live batched push); invalid views are harmlessly polluted and reset+rebuilt in
/// step-4. The transitive verdict guarantees no valid view reads an invalid one.
fn recovery_tick_sweep(catalog: &mut CatalogEngine, dispatcher: &mut MasterDispatcher) -> Result<(), String> {
    for src in swept_base_tables(catalog) {
        dispatcher.drain_tick_blocking(src)?;
    }
    Ok(())
}

/// Step-4: rebuild only the views the boot verdict rejected. For each invalid view
/// in ascending depth (a source view before any dependent that reads it), issue
/// one view-scoped `fan_out_backfill(vid, src)` per source. The worker resets that
/// view's output partitions + operator scratch on the FIRST such command (gated on
/// its COW-inherited `invalid_views` set), then fills — for both exchange and
/// non-exchange views. A multi-source join iterates every source: the first fills
/// its trace, the rest join against it. View-scoped (`vid != 0`) so a resumed
/// sibling's loaded shards are never re-derived and double-counted.
fn rebuild_invalid_views(catalog: &mut CatalogEngine, dispatcher: &mut MasterDispatcher) -> Result<(), String> {
    let mut invalid: Vec<i64> = catalog.invalid_views.iter().copied().collect();
    invalid.sort_by_key(|&vid| catalog.dag.tables.get(&vid).map(|e| e.depth).unwrap_or(0));
    // Resume-vs-rebuild marker (asserted by the "no backfill on clean restart"
    // E2E): 0 ⇒ every view resumed from its checkpoint.
    boot_log(&format!("recovery: rebuilding {} invalid view(s)\n", invalid.len()));
    for vid in invalid {
        for src in catalog.dag.get_source_ids(vid) {
            dispatcher.fan_out_backfill(vid, src)?;
        }
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
    // Latch the Master role before any catalog work: the pre-fork replay hooks
    // in CatalogEngine::open must see Master so they skip the index backfill
    // their forked children rebuild slice-local.
    crate::foundation::worker_ctx::set_master_role();

    // Raise fd limit (partition directories + shard files)
    posix_io::raise_fd_limit(65536);

    gnitz_info!("Opening database at {}", data_dir);

    let catalog = match CatalogEngine::open(data_dir) {
        Ok(c) => c,
        Err(e) => {
            gnitz_error!("failed to open catalog: {e}");
            return 1;
        }
    };
    let catalog_ptr = Box::into_raw(Box::new(catalog));

    let nw = num_workers as usize;
    boot_log(&format!("Starting {num_workers} workers\n"));
    boot_log(&format!(
        "Worker logs: {}/worker_N.log (N=0..{})\n",
        data_dir,
        num_workers - 1
    ));

    // --- Shared Append-Only Log (file-backed, master→all workers) ---
    // A fresh file reads all-zero through O_CREAT + fallocate, which is
    // exactly the empty-SAL state recovery expects.
    let sal_fd = {
        use std::os::unix::fs::OpenOptionsExt;
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o644)
            .open(format!("{data_dir}/wal.sal"))
        {
            Ok(f) => std::os::fd::IntoRawFd::into_raw_fd(f),
            Err(e) => {
                gnitz_error!("failed to open SAL file: {e}");
                return 1;
            }
        }
    };
    posix_io::try_set_nocow(sal_fd);
    // Check existing size and fallocate if needed
    if posix_io::fd_size(sal_fd).unwrap_or(0) < sal_mmap_size() {
        let _ = posix_io::fallocate(sal_fd, sal_mmap_size() as i64);
    }
    let sal_ptr = posix_io::mmap_shared(sal_fd, sal_mmap_size());
    if sal_ptr.is_null() {
        gnitz_error!("failed to mmap SAL");
        return 1;
    }
    // Pre-fault writable PTEs so the hot write path never page-faults.
    // MADV_POPULATE_WRITE (Linux 5.14+) installs dirty PTEs and triggers
    // the filesystem mkwrite callback upfront, without dirtying page contents.
    posix_io::madvise_populate_write(sal_ptr, sal_mmap_size());

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
            gnitz_error!("{e}");
            return 1;
        }
        #[cfg(debug_assertions)]
        if std::env::var("GNITZ_INJECT_SYS_FLUSH_ERROR").is_ok() {
            gnitz_error!("injected system table flush fault");
            return 1;
        }

        // Reclaim table/view/index directories whose DROP committed but whose
        // deferred deletion was lost to a crash before the next checkpoint.
        // Runs only now that both shard replay and SAL replay have populated
        // dag.tables, so a SAL-committed-but-unflushed CREATE is not mistaken
        // for an orphan.
        catalog.gc_orphan_directories();
    }

    // --- Boot invalid-view verdict + recovery-start generation bump ---
    //
    // Both pre-fork, while the master's active range is still full: the verdict's
    // manifest peeks see every partition, and the durable generation advance is
    // COW-inherited by every worker (and is durable long before the parent resets
    // the SAL after worker readiness).
    {
        let catalog = unsafe { &mut *catalog_ptr };
        // Per-view resume-vs-rebuild verdict against the checkpointed manifests
        // (generation + topology + transitive source validity). Reads
        // `worker_ctx::committed_generation()` (the recovered G), so it runs BEFORE
        // the recovery-start bump advances the durable generation.
        let invalid = catalog.compute_invalid_views(num_workers);
        catalog.invalid_views = invalid;

        // Durably advance the checkpoint generation G → G+1 without publishing to
        // worker_ctx, closing the reset→boot_checkpoint crash window.
        if let Err(e) = catalog.recovery_start_generation_bump() {
            gnitz_error!("recovery-start generation bump failed: {e}");
            return 1;
        }
        inject_recovery_panic("genbump");
    }

    // --- W2M regions (memfd-backed, one per worker→master) ---
    let mut w2m_ptrs: Vec<*mut u8> = Vec::with_capacity(nw);
    let mut w2m_fds: Vec<i32> = Vec::with_capacity(nw);
    for w in 0..nw {
        let name = format!("w2m_{w}");
        let wfd = posix_io::memfd_create(name.as_bytes());
        if wfd < 0 {
            gnitz_error!("memfd_create failed");
            return 1;
        }
        let _ = posix_io::ftruncate(wfd, W2M_REGION_SIZE as i64);
        let wptr = posix_io::mmap_shared(wfd, W2M_REGION_SIZE);
        if wptr.is_null() {
            gnitz_error!("mmap W2M failed");
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
    }

    // --- M2W eventfds (master→worker signaling; W2M uses futex now) ---
    let mut m2w_efds: Vec<i32> = Vec::with_capacity(nw);
    for _ in 0..nw {
        let m2w = posix_io::eventfd_create();
        if m2w < 0 {
            gnitz_error!("eventfd_create failed");
            return 1;
        }
        m2w_efds.push(m2w);
    }

    // Log fd assignments
    boot_log(&format!("SAL fd={sal_fd}\n"));
    for w in 0..nw {
        boot_log(&format!("W{} m2w_efd={} w2m_fd={}\n", w, m2w_efds[w], w2m_fds[w]));
    }

    let master_pid = unsafe { libc::getpid() };

    // --- Fork workers ---
    let mut worker_pids: Vec<i32> = vec![0; nw];
    for w in 0..nw {
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            gnitz_error!("fork failed");
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

            // Latch this worker's rank/count (and its Worker role) before ANY
            // catalog work below (trim, rehome, index rebuild, SAL replay, view
            // backfill): every plan compiled during boot must see this process's
            // real (rank, num_workers) and its index tables must home into the
            // per-rank subdir. Single owner of the rank — no longer set in
            // WorkerProcess::new.
            crate::foundation::worker_ctx::set_worker_rank(w as u32, num_workers);

            // Redirect stdout/stderr to worker log file
            {
                use std::os::unix::fs::OpenOptionsExt;
                if let Ok(f) = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o644)
                    .open(format!("{data_dir}/worker_{w}.log"))
                {
                    let log_fd = std::os::fd::IntoRawFd::into_raw_fd(f);
                    unsafe {
                        libc::dup2(log_fd, 1);
                        libc::dup2(log_fd, 2);
                        libc::close(log_fd);
                    }
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
                gnitz_error!("W{w} rehome single-partition stores failed: {e}");
            }

            // Construct channel types for this worker
            let sal_reader = SalReader::new(sal_ptr as *const u8, w as u32, sal_mmap_size(), m2w_efds[w]);
            let w2m_writer = W2mWriter::new(w2m_ptrs[w], W2M_REGION_SIZE as u64);

            // Recover: rebuild indexes, replay the SAL tail (buffering effective
            // base deltas), boot-flush the replayed rows durable. The buffered
            // deltas seed `pending_deltas`; the master's post-reset tick sweep
            // drives them into the views. All view derivation moved to the
            // master's sweep + step-4 rebuild — no child-side view backfill.
            let (pending_deltas, boot_err): (HashMap<i64, Batch>, Option<String>) =
                match worker_boot_recovery(catalog, &sal_reader) {
                    Ok(pd) => (pd, None),
                    Err(e) => {
                        // stderr is redirected to worker_N.log above.
                        eprintln!("{e}");
                        (HashMap::new(), Some(e))
                    }
                };

            catalog.invalidate_all_plans();

            boot_log(&format!(
                "Worker {} (pid {}) partitions [{}, {})\n",
                w,
                unsafe { libc::getpid() },
                part_start,
                part_end,
            ));

            // Re-init logging with worker tag
            let wtag = format!("W{w}");
            crate::foundation::log::init(log_level, wtag.as_bytes());

            let mut worker = WorkerProcess::new(
                w as u32,
                master_pid,
                catalog_ptr,
                sal_reader,
                w2m_writer,
                pending_deltas,
            );
            let rc = worker.run(boot_err);

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

    let sal_writer = SalWriter::new(sal_ptr, sal_fd, sal_mmap_size() as u64, m2w_efds.clone());
    let w2m_receiver = W2mReceiver::new(w2m_ptrs.clone());

    let dispatcher = MasterDispatcher::new(nw, worker_pids.clone(), catalog_ptr, sal_writer, w2m_receiver);
    let dispatcher_ptr = Box::into_raw(Box::new(dispatcher));

    // Wait for all workers to complete recovery and signal readiness
    let dispatcher = unsafe { &mut *dispatcher_ptr };
    if let Err(e) = dispatcher.collect_acks() {
        gnitz_error!("Error collecting worker acks: {e}");
        return 1;
    }

    // Reset SAL for fresh use (all workers have recovered)
    use std::sync::atomic::{AtomicU64, Ordering};
    unsafe {
        let atomic = &*(sal_ptr as *const AtomicU64);
        atomic.store(0, Ordering::Release);
    }
    dispatcher.reset_sal();

    inject_recovery_panic("reset");

    // Recovery tick sweep: apply the un-checkpointed SAL tail (buffered as
    // pending_deltas during replay) to every view via one master-driven tick per
    // reachable base, on the freshly-reset SAL. Resumed views are extended;
    // invalid views are polluted (reset+rebuilt next).
    if let Err(e) = recovery_tick_sweep(catalog, dispatcher) {
        gnitz_error!("recovery tick sweep failed: {e}");
        return 1;
    }

    inject_recovery_panic("sweep");

    // Step-4: reset (on the workers) and rebuild only the invalid views.
    if let Err(e) = rebuild_invalid_views(catalog, dispatcher) {
        gnitz_error!("invalid-view rebuild failed: {e}");
        return 1;
    }

    inject_recovery_panic("backfill");

    // Boot-end checkpoint: record the launched topology, bump the generation
    // (G+1 → G+2), and durably checkpoint the resumed + rebuilt view state (base +
    // ephemeral rounds) before the socket opens, so a clean restart resumes from
    // it. No drain — recovery already drained everything and no pushes are admitted
    // yet.
    if let Err(e) = dispatcher.boot_checkpoint(num_workers) {
        gnitz_error!("boot checkpoint failed: {e}");
        return 1;
    }

    // Create server socket and run executor
    gnitz_info!("Listening on {}", socket_path);
    let server_fd = match posix_io::server_create(socket_path) {
        Ok(fd) => std::os::fd::IntoRawFd::into_raw_fd(fd),
        Err(e) => {
            gnitz_error!("failed to create server socket: {e}");
            return 1;
        }
    };
    boot_log("GnitzDB ready\n");

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
    use crate::test_support::SharedRegion;

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
        let payloads: Vec<&[u8]> = (0..nw).map(|_| payload.as_slice()).collect();
        sal_write_group(ptr, cursor, target_id, lsn, FLAG_DDL_SYNC, epoch, size, &payloads).expect("group fits")
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
            let region = SharedRegion::new(size);
            let ptr = region.ptr();
            let nw = 2u32;

            let mut cur = write_ddl_group(ptr, 0, nw, 100, 5, 1, size as u64);
            cur = write_ddl_group(ptr, cur, nw, 101, 5, 1, size as u64);
            // No sentinel for lsn=5 — zone never closed.

            cur = write_ddl_group(ptr, cur, nw, 102, 6, 1, size as u64);
            // Sentinel for lsn=6.
            let efds: Vec<i32> = (0..nw).map(|_| posix_io::eventfd_create()).collect();
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
        }
    }

    #[test]
    fn test_recover_applies_all_committed() {
        // Three DDL groups at lsn=K plus a sentinel at lsn=K. Recovery's
        // committed set must contain K. Pass 2 (simulated by hand here)
        // would then apply each of the three groups.
        unsafe {
            let size = 1 << 20;
            let region = SharedRegion::new(size);
            let ptr = region.ptr();
            let nw = 3u32;

            let mut cur = write_ddl_group(ptr, 0, nw, 200, 9, 1, size as u64);
            cur = write_ddl_group(ptr, cur, nw, 201, 9, 1, size as u64);
            cur = write_ddl_group(ptr, cur, nw, 202, 9, 1, size as u64);

            let efds: Vec<i32> = (0..nw).map(|_| posix_io::eventfd_create()).collect();
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
                let (msg, next) = match reader.try_read(offset, None) {
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
            let region = SharedRegion::new(size);
            let ptr = region.ptr();
            let nw = 1u32;

            let payload = [0u8; 32];
            sal_write_group(ptr, 0, 50, 11, FLAG_PUSH, 1, size as u64, &[&payload]).expect("group fits");
            let _ = nw;
            // No sentinel — zone unclosed.

            let efd = posix_io::eventfd_create();
            let reader = SalReader::new(ptr as *const u8, 0, size, efd);
            let committed = collect_committed_lsns(&reader);
            assert!(
                !committed.contains(&11),
                "uncommitted push must NOT appear in committed set"
            );

            let _ = MAX_WORKERS;
            libc::close(efd);
        }
    }
}
