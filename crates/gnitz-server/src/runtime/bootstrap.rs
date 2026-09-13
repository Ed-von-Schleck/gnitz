//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.
//!
//! **Recovery order is a crash guard**: every step is placed so a crash at any
//! point rebuilds a view rather than silently resuming a stale one.

use std::collections::HashMap;
use std::ops::Range;
use std::rc::Rc;

use crate::catalog::CatalogEngine;
use gnitz_foundation::fault::Seam;
use gnitz_foundation::posix_io;

use crate::runtime::affinity;
use crate::runtime::executor::ServerExecutor;
use crate::runtime::m2w;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::sal::zone::CommittedTail;
use crate::runtime::sal::{sal_mmap_size, SalLog, SalMessage, SalMessageKind, SalReader, SalWriter};
use crate::runtime::tls::{setup_tls_listener, TlsCli};
use crate::runtime::w2m::{self, W2mReceiver, W2mWriter};
use crate::runtime::wire as ipc;
use crate::runtime::worker::{buffer_pending_delta, WorkerProcess};
use gnitz_store::relation::Relation;
use gnitz_store::storage::{Batch, Slot};

// ---------------------------------------------------------------------------
// SAL recovery: both drivers below read the log through `sal::zone::CommittedTail`
// and differ only in which groups are theirs and what they do with the bytes.
// ---------------------------------------------------------------------------

/// Decode one committed group slot, failing the boot rather than skipping it:
/// pass 1 demoted the last zone if it was torn, so a block that fails here has a
/// durable committed zone behind it, and skipping it would lose an ACKed write.
fn decode_group_slot(msg: &SalMessage, data: &[u8]) -> Result<ipc::DecodedWire, String> {
    ipc::decode_wire(data).map_err(|e| {
        format!(
            "SAL replay: corrupt block at offset={} lsn={} target={}: {e}",
            msg.base, msg.lsn, msg.target_id
        )
    })
}

/// Master pre-fork system-table replay. Builds the system-table family map from
/// the flushed LSNs, then ingests every committed DdlSync batch addressed
/// to a system table — orphan COL_TAB rows from a crashed DDL are skipped
/// because their zone never closed.
fn recover_system_tables_from_sal(log: SalLog, epoch: u32, catalog: &mut CatalogEngine) -> Result<(), String> {
    let family_lsns = catalog.registry().system_flushed_lsns();
    let tail = CommittedTail::open(log, epoch, SalMessageKind::DdlSync, &family_lsns)?;

    let mut replayed: u32 = 0;
    for msg in tail.groups() {
        // A system family broadcasts, so slot 0 carries the whole batch.
        let Some(data) = msg.slot(0) else { continue };
        let decoded = decode_group_slot(&msg, data)?;
        let Some(batch) = decoded.data_batch.filter(|b| !b.is_empty()) else {
            continue;
        };
        // `ddl_sync`, not `submit`: these rows are master-validated by
        // definition, and re-running the precheck would false-reject a replayed DROP
        // TABLE cascade. Hooks still fire, so an ALTER in the tail reaches the base
        // before the pushes that depend on it.
        catalog.ddl_sync(msg.target_id as i64, batch).map_err(|e| {
            format!(
                "SAL system-table recovery apply failed (table_id={}, lsn={}): {e}",
                msg.target_id, msg.lsn
            )
        })?;
        replayed += 1;
    }

    if replayed > 0 {
        gnitz_note!("SAL system table recovery: replayed {replayed} entries");
    }
    Ok(())
}

/// `GNITZ_INJECT_RECOVERY_PANIC=<stage>`: panic when recovery reaches the named
/// stage, so the crash-window E2E tests can cut boot at a precise point.
static RECOVERY_PANIC: Seam = Seam::new("GNITZ_INJECT_RECOVERY_PANIC");

/// `GNITZ_INJECT_BOOT_FLUSH_ERROR` / `GNITZ_INJECT_SYS_FLUSH_ERROR`: fail the
/// base-table or system-table boot flush, whose swallowed failure would destroy
/// the replayed DDL's only durable copy at the SAL reset.
static BOOT_FLUSH_ERROR: Seam = Seam::new("GNITZ_INJECT_BOOT_FLUSH_ERROR");
static SYS_FLUSH_ERROR: Seam = Seam::new("GNITZ_INJECT_SYS_FLUSH_ERROR");

fn inject_recovery_panic(stage: &str) {
    if RECOVERY_PANIC.at(stage) {
        panic!("injected recovery panic at {stage}");
    }
}

/// Base tables feeding ≥1 view that is keeping its checkpointed state, sorted
/// for a reproducible drive order. Keyed on the boot verdict and not on what the
/// tail contained, because the sweep is also what compiles a view at boot, and
/// the boot checkpoint publishes traces only for compiled views.
fn swept_base_tables(catalog: &mut CatalogEngine) -> Vec<i64> {
    let keeps_state: Vec<i64> = catalog
        .registry()
        .view_ids()
        .into_iter()
        .filter(|&vid| !catalog.view_is_invalid(vid))
        .collect();
    let (dag, registry) = catalog.dag_and_registry_mut();
    dag.base_tables_reachable_from(registry, keeps_state)
}

/// Which of a group's slots `slot` replays, and whether what it reads is still
/// cut for another width. `written` is the group's own slot count.
fn replay_slots(written: u32, slot: Slot, replicated: bool) -> (Range<u32>, bool) {
    if written == slot.of {
        // This rank's own slot: its share of a partitioned group, or the whole
        // copy of a replicated one.
        (slot.rank..slot.rank + 1, false)
    } else if replicated {
        // Every slot holds the same whole copy, so reading a second would add
        // those rows' weights again.
        (0..1, false)
    } else {
        // Written for another width, so no slot holds this rank's rows: walk
        // every written slot and re-cut each for the launched topology.
        (0..written, true)
    }
}

/// Per-worker post-fork user-table replay for `slot`, applying each Push group
/// through `ingest_returning_effective` — the exact call `handle_push` makes, so
/// retractions cancel correctly. The returned map seeds the worker's
/// `pending_deltas` for the master's tick sweep to drain into the views.
fn recover_from_sal(
    log: SalLog,
    slot: Slot,
    walk_epoch: u32,
    swept_bases: &[i64],
    catalog: &mut CatalogEngine,
) -> Result<HashMap<i64, Batch>, String> {
    let family_lsns = catalog.registry().user_flushed_lsns();
    let tail = CommittedTail::open(log, walk_epoch, SalMessageKind::Push, &family_lsns)?;

    let mut pending: HashMap<i64, Batch> = HashMap::new();
    // Groups that applied at least one slot, and the width a re-sliced tail was
    // written at — the boot record's re-slice marker. No boot writes at a
    // previously-used epoch, so every group a walk sees carries the same width.
    let mut replayed: u32 = 0;
    let mut resliced_from: Option<u32> = None;
    for msg in tail.groups() {
        let tid = msg.target_id as i64;
        // The catalog's schema, not the wire's: only the catalog stamps the
        // `replicated` bit the branch below reads. `SchemaDescriptor` is `Copy`, so
        // this holds no borrow on `catalog` across the `&mut` ingest.
        let schema = catalog
            .registry()
            .relation(tid)
            .map(Relation::schema)
            .ok_or_else(|| format!("SAL replay: no schema for table_id={tid} (lsn={})", msg.lsn))?;
        // Each group's own slot count, off the header in hand and inside its
        // digest. A tail-wide probe would read one header for all of them, and a
        // torn one picks the wrong mode — losing every slot above this rank.
        let (wanted, reslice) = replay_slots(msg.slots(), slot, schema.placement().is_replicated());
        if reslice {
            resliced_from = Some(msg.slots());
        }
        let mut applied = false;
        for (_, data) in msg.slots_written().filter(|(w, _)| wanted.contains(w)) {
            let decoded = decode_group_slot(&msg, data)?;
            let Some(mut batch) = decoded.data_batch.filter(|b| !b.is_empty()) else {
                continue;
            };
            // Above both arms because both need it: the re-cut reads `schema`'s
            // payload columns off the batch, and `ingest_batch` rejects a
            // payload-count mismatch outright.
            if batch.schema().num_payload_cols() < schema.num_payload_cols() {
                batch = batch.widened_with_null_tail(&schema);
            }
            let owned = if reslice {
                // Re-cut with the write path's own router, so what survives is exactly
                // what the master would have written to this rank's slot: same
                // distribution-prefix hash, same key→worker map.
                let mb = batch.as_mem_batch();
                crate::runtime::master::scatter::with_worker_indices(&batch, &schema, slot.of as usize, |wi| {
                    Batch::from_indexed_rows(&mb, &wi[slot.rank as usize], &schema)
                })
            } else {
                batch
            };
            if owned.is_empty() {
                continue;
            }
            let effective = catalog
                .registry_mut()
                .ingest_returning_effective(tid, owned)
                .map_err(|e| {
                    format!(
                        "SAL replay apply failed (table_id={}, lsn={}): {e}",
                        msg.target_id, msg.lsn
                    )
                })?;
            if swept_bases.contains(&tid) {
                buffer_pending_delta(&mut pending, tid, effective);
            }
            applied = true;
        }
        replayed += u32::from(applied);
    }

    if replayed > 0 {
        match resliced_from {
            Some(n) => gnitz_note!("SAL replay: replayed {replayed} group(s), re-sliced from a {n}-worker tail"),
            None => gnitz_note!("SAL replay: replayed {replayed} group(s)"),
        }
    }
    Ok(pending)
}

/// Worker-boot catalog recovery. The `Err` rides the startup ACK, which fails
/// the boot before the master rewinds the SAL — otherwise the rewind destroys
/// the replayed rows' only durable copy.
fn worker_boot_recovery(
    catalog: &mut CatalogEngine,
    log: SalLog,
    slot: Slot,
    walk_epoch: u32,
    swept_bases: &[i64],
) -> Result<HashMap<i64, Batch>, String> {
    // Before any store is touched: each re-homes from the pre-fork master's
    // child to this slot's own, and every plan compiled after this is baked with
    // the slot.
    catalog.become_worker(slot)?;
    // Before the replay below, which projects the tail into each index exactly
    // once: a rebuild after it would double-count every replayed row.
    let rebuilt = catalog
        .backfill_all_indexes()
        .map_err(|e| format!("boot index backfill failed: {e}"))?;
    // Resume-vs-rebuild marker, the index sibling of the invalid-view line: 0 ⇒
    // every index resumed from its checkpoint.
    gnitz_note!("recovery: rebuilding {rebuilt} index(es)");
    let pending_deltas = recover_from_sal(log, slot, walk_epoch, swept_bases, catalog)?;
    // Before the master's boot rewind puts the write cursor back to 0: these rows
    // still live only in SAL entries a second crash would then overwrite.
    debug_assert!(
        catalog.durable_generation() > catalog.registry().resume_generation(),
        "boot base flush without the recovery-start generation bump ahead of it",
    );
    catalog
        .flush_base_round()
        .map_err(|e| format!("boot flush failed: {e}"))?;
    if BOOT_FLUSH_ERROR.armed() {
        return Err("injected boot flush fault".to_string());
    }
    Ok(pending_deltas)
}

// ---------------------------------------------------------------------------
// Server main entry point
// ---------------------------------------------------------------------------

/// Single entry point for the entire server bootstrap: opens the catalog,
/// allocates the shared IPC regions, forks the workers, runs recovery, and
/// enters the executor event loop.
///
/// Returns 0 on clean exit, non-zero on error.
pub fn server_main(data_dir: &str, socket_path: &str, num_workers: u32, tls_cli: Option<TlsCli>) -> i32 {
    match run_server(data_dir, socket_path, num_workers, tls_cli) {
        Ok(rc) => rc,
        Err(e) => {
            gnitz_error!("{e}");
            1
        }
    }
}

/// Every shared region and descriptor the master and its forked workers use to
/// talk to each other.
///
/// Nothing here is reclaimed on error: the only response to a failed boot is to
/// exit the process, which returns all of it to the kernel.
struct SharedIpc {
    sal_fd: i32,
    sal_ptr: *mut u8,
    /// The mapped SAL's length — the one this boot passed to `map_file_reserved`,
    /// so every reader and the writer wrap on the bytes that actually exist.
    sal_len: usize,
    /// The epoch every reader of this mapping walks, and the floor for the next
    /// writer epoch. Derived once with the mapping: on a damaged offset-0 header
    /// it costs a full-ring sweep, and every reader must anchor on one answer.
    walk_epoch: u32,
    w2m_ptrs: Vec<*mut u8>,
    m2w_efds: Vec<i32>,
}

impl SharedIpc {
    /// The mapped SAL as a reader sees it — pointer and length from the one
    /// mapping this boot made, so no consumer re-derives the size.
    fn sal_log(&self) -> SalLog {
        // SAFETY: `sal_ptr` maps `sal_len` bytes and outlives the process.
        unsafe { SalLog::new(self.sal_ptr, self.sal_len) }
    }
}

/// Open and map the SAL, one W2M ring per worker, and the M2W eventfds.
fn acquire_shared_ipc(data_dir: &str, nw: usize) -> Result<SharedIpc, String> {
    // A fresh SAL file reads all-zero through O_CREAT + fallocate, which is
    // exactly the empty-SAL state recovery expects.
    let sal_fd = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o644)
            .open(format!("{data_dir}/wal.sal"))
            .map(std::os::fd::IntoRawFd::into_raw_fd)
            .map_err(|e| format!("failed to open SAL file: {e}"))?
    };
    posix_io::try_set_nocow(sal_fd);
    // The SAL is a real file, and reserving its blocks now is what keeps a later
    // write from failing for want of disk space.
    let sal_len = sal_mmap_size();
    let sal_ptr = posix_io::map_file_reserved(sal_fd, sal_len)
        .map_err(|e| format!("failed to map SAL ({sal_len} bytes): {e}"))?;
    // SAFETY: the mapping above is `sal_len` bytes and outlives the process.
    let walk_epoch = unsafe { SalLog::new(sal_ptr, sal_len) }.walk_epoch();

    let mut w2m_ptrs: Vec<*mut u8> = Vec::with_capacity(nw);
    let mut m2w_efds: Vec<i32> = Vec::with_capacity(nw);
    for w in 0..nw {
        let wptr = w2m::create_region().map_err(|e| format!("failed to map W2M region for W{w}: {e}"))?;
        w2m_ptrs.push(wptr);

        // M2W eventfd (master→worker signaling; W2M wakes via futex).
        let efd = m2w::eventfd_create().map_err(|e| format!("failed to create the M2W eventfd for W{w}: {e}"))?;
        m2w_efds.push(efd);
    }

    Ok(SharedIpc {
        sal_fd,
        sal_ptr,
        sal_len,
        walk_epoch,
        w2m_ptrs,
        m2w_efds,
    })
}

/// The forked child's whole life: latch its rank, redirect its logs to
/// `worker_N.log`, recover, and run the worker loop. Never returns — the worker
/// exits via `libc::_exit`.
fn run_worker_child(
    slot: Slot,
    data_dir: &str,
    master_pid: i32,
    catalog: &mut CatalogEngine,
    ipc: &SharedIpc,
    swept_bases: &[i64],
    placement: Option<&affinity::Placement>,
) -> ! {
    let w = slot.rank as usize;

    // Die immediately if the master exits for any reason. The `getppid` probe a
    // worker runs after a timed-out eventfd park is a belt-and-suspenders
    // fallback; this closes the ~30s polling gap.
    unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) };
    // Re-check: parent may have died in the fork→prctl window.
    if unsafe { libc::getppid() } != master_pid {
        unsafe { libc::_exit(0) };
    }

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

    // Re-tag logging as this worker before any boot work, so every line the
    // recovery below emits carries `W{w}` rather than the inherited master tag.
    // Only the tag: the level is a process-wide static the fork already copied.
    gnitz_foundation::log::set_tag(format!("W{w}").as_bytes());

    // Pin after the log re-tag, so a failed pin is recorded in this worker's own
    // `worker_N.log` rather than the master's stdout, and before every
    // allocation below — trim, re-home, index rebuild, SAL replay, backfill —
    // whose first touch decides which node its memory lands on.
    if let Some(p) = placement {
        p.pin_worker(w);
    }

    // Close M2W eventfds of OTHER workers (W2M uses futex, no fd).
    for (j, &efd) in ipc.m2w_efds.iter().enumerate() {
        if j != w {
            unsafe {
                libc::close(efd);
            }
        }
    }

    let w2m_writer = W2mWriter::new(ipc.w2m_ptrs[w]);
    let catalog_ptr: *mut CatalogEngine = catalog;

    let pending_deltas = match worker_boot_recovery(catalog, ipc.sal_log(), slot, ipc.walk_epoch, swept_bases) {
        Ok(pd) => pd,
        Err(e) => {
            // The master reads this frame off the shared ring, which outlives the
            // process that wrote it.
            gnitz_error!("{e}");
            w2m_writer.send_status(0, 0, gnitz_wire::STATUS_ERROR, e.as_bytes());
            unsafe { libc::_exit(1) };
        }
    };

    gnitz_note!("Worker {} (pid {}) of {}", w, unsafe { libc::getpid() }, slot.of);

    let sal_reader = SalReader::new(ipc.sal_log(), slot.rank, ipc.walk_epoch);
    let mut worker = WorkerProcess::new(
        master_pid,
        catalog_ptr,
        sal_reader,
        w2m_writer,
        ipc.m2w_efds[w],
        pending_deltas,
    );
    let rc = worker.run();

    unsafe {
        libc::_exit(rc);
    }
}

/// The master's half of recovery before any worker exists. Returns the sweep set
/// the verdict below determines, which every worker inherits across the fork.
fn master_pre_fork_recovery(catalog: &mut CatalogEngine, log: SalLog, walk_epoch: u32) -> Result<Vec<i64>, String> {
    recover_system_tables_from_sal(log, walk_epoch, catalog)?;

    // Checked, and before the gc below: the replayed DDL lives only in master
    // memory until this makes it durable, so a swallowed failure lets the SAL
    // reset destroy its only copy and the gc delete the shards it named.
    catalog.flush_all_system_tables()?;
    if SYS_FLUSH_ERROR.armed() {
        return Err("injected system table flush fault".to_string());
    }

    // Reclaim directories whose DROP committed but whose deferred deletion was
    // lost to a crash. After both replays, so a committed-but-unflushed CREATE is
    // not mistaken for an orphan.
    catalog.gc_orphan_directories();

    // Drop the child directories this boot's worker count no longer owns, before
    // each worker's `rehome` opens what is left.
    catalog.registry().reconcile_child_dirs();

    // Reads `resume_generation`, so it runs before the bump below advances the
    // durable one. Pre-fork because it peeks every launched rank's manifest on
    // behalf of workers that do not exist yet.
    catalog.compute_invalid_views();

    // Durably advance the checkpoint generation G → G+1 while the resume
    // generation stays at G, closing the reset→boot_checkpoint crash window.
    catalog
        .recovery_start_generation_bump()
        .map_err(|e| format!("recovery-start generation bump failed: {e}"))?;
    inject_recovery_panic("genbump");

    Ok(swept_base_tables(catalog))
}

/// Fork one child per worker, each of which never returns. Yields the parent's
/// pid list.
fn fork_workers(
    catalog: &mut CatalogEngine,
    data_dir: &str,
    num_workers: u32,
    ipc: &SharedIpc,
    swept_bases: &[i64],
    placement: Option<&affinity::Placement>,
) -> Result<Vec<i32>, String> {
    let master_pid = unsafe { libc::getpid() };
    let mut worker_pids: Vec<i32> = Vec::with_capacity(num_workers as usize);
    for w in 0..num_workers {
        match unsafe { libc::fork() } {
            -1 => return Err("fork failed".to_string()),
            0 => run_worker_child(
                Slot::new(w, num_workers),
                data_dir,
                master_pid,
                catalog,
                ipc,
                swept_bases,
                placement,
            ),
            pid => worker_pids.push(pid),
        }
    }
    Ok(worker_pids)
}

/// The master's half of recovery once the workers are alive, ending in the
/// checkpoint a clean restart resumes from.
fn master_post_fork_recovery(
    disp: &MasterDispatcher,
    walk_epoch: u32,
    num_workers: u32,
    swept_bases: &[i64],
) -> Result<(), String> {
    // Before the workers get anywhere: each is re-homing the very stores this
    // process inherited handles to, and two live `Table`s on one directory is
    // the hazard.
    disp.cat().registry_mut().detach();

    // Wait for all workers to complete recovery and signal readiness.
    disp.collect_acks_and_relay("recovery sync", false)
        .map_err(|e| format!("Error collecting worker acks: {e}"))?;

    // Reset the SAL for fresh use, now that every worker has recovered, above the
    // same `walk_epoch` the workers were launched with.
    disp.boot_rewind_sal(walk_epoch);

    inject_recovery_panic("reset");

    // Drive the tail each worker buffered during replay into the views, one
    // blocking tick per swept base. An empty source ticks too, so exchange views
    // stay in lockstep and every view this reaches is compiled.
    for &src in swept_bases {
        disp.drain_tick_blocking(src)
            .map_err(|e| format!("recovery tick sweep failed: {e}"))?;
    }

    inject_recovery_panic("sweep");

    // Rebuild only the views the boot verdict rejected, through the driver a live
    // CREATE VIEW uses. Each was resumed from stale shards at open, so its
    // rebuild starts by emptying the output store it loaded them into.
    let invalid: Vec<i64> = disp.cat().invalid_views().collect();
    // Resume-vs-rebuild marker (asserted by the "no backfill on clean restart"
    // E2E): 0 ⇒ every view resumed from its checkpoint.
    gnitz_note!("recovery: rebuilding {} invalid view(s)", invalid.len());
    disp.backfill_views_in_dep_order(&invalid)
        .map_err(|e| format!("invalid-view rebuild failed: {e}"))?;

    inject_recovery_panic("backfill");

    // A full checkpoint before the socket opens, so a clean restart resumes from
    // it. No drain — recovery already drained everything and no pushes are
    // admitted yet.
    disp.boot_checkpoint(num_workers)
        .map_err(|e| format!("boot checkpoint failed: {e}"))?;
    Ok(())
}

fn run_server(data_dir: &str, socket_path: &str, num_workers: u32, tls_cli: Option<TlsCli>) -> Result<i32, String> {
    // Child directories + shard files.
    posix_io::raise_fd_limit(65536);

    // Pin the master before `CatalogEngine::open`: the boot-time system-table
    // flush below creates the master's first io_uring ring, and an io-wq pool
    // keeps the mask its ring's creating thread held. Each forked child then
    // inherits this mask until its own pin narrows it. See `runtime::affinity`
    // for the placement itself and what it assumes about the host.
    let nw = num_workers as usize;
    let placement = match affinity::plan(nw) {
        Ok(p) => {
            p.log_placement();
            p.pin_master();
            Some(p)
        }
        Err(why) => {
            gnitz_note!("affinity: not applied ({why})");
            None
        }
    };

    gnitz_info!("Opening database at {}", data_dir);

    // As the master: the pre-fork replay hooks skip the index backfill their
    // forked children run slice-local.
    let catalog =
        CatalogEngine::open_master(data_dir, num_workers).map_err(|e| format!("failed to open catalog: {e}"))?;

    gnitz_note!("Starting {num_workers} workers");
    gnitz_note!("Worker logs: {}/worker_N.log (N=0..{})", data_dir, num_workers - 1);

    let ipc = acquire_shared_ipc(data_dir, nw)?;
    gnitz_debug!("SAL fd={}", ipc.sal_fd);
    for w in 0..nw {
        gnitz_debug!("W{} m2w_efd={}", w, ipc.m2w_efds[w]);
    }

    // Leaked: it outlives every borrow the dispatcher and reactor hold, and
    // dropping it would run `CircuitState::drop`, which removes directories.
    let catalog: &'static mut CatalogEngine = Box::leak(Box::new(catalog));

    let swept_bases = master_pre_fork_recovery(catalog, ipc.sal_log(), ipc.walk_epoch)?;

    let worker_pids = fork_workers(catalog, data_dir, num_workers, &ipc, &swept_bases, placement.as_ref())?;

    // --- Parent process ---
    let SharedIpc {
        sal_fd,
        sal_ptr,
        sal_len,
        walk_epoch,
        w2m_ptrs,
        m2w_efds,
    } = ipc;

    // `recovery_start_generation_bump` has already run, so this is the floor
    // every base round must publish past.
    let boot_generation = catalog.durable_generation();
    let dispatcher = Rc::new(MasterDispatcher::new(
        worker_pids,
        catalog,
        boot_generation,
        SalWriter::new(sal_ptr, sal_fd, sal_len, nw),
        Rc::new(W2mReceiver::new(w2m_ptrs)),
        m2w_efds,
    ));

    master_post_fork_recovery(&dispatcher, walk_epoch, num_workers, &swept_bases)?;

    // Create server socket and run executor
    gnitz_info!("Listening on {}", socket_path);
    // `UnixListener::bind` does not unlink; a socket left by an earlier run
    // would be EADDRINUSE.
    let _ = std::fs::remove_file(socket_path);
    let server_fd = std::os::unix::net::UnixListener::bind(socket_path)
        .and_then(|l| {
            l.set_nonblocking(true)?;
            Ok(std::os::fd::IntoRawFd::into_raw_fd(l))
        })
        .map_err(|e| format!("failed to create server socket: {e}"))?;

    // Optional TLS listener — after the worker fork (same position as
    // the AF_UNIX bind), so no fd inheritance. TLS was explicitly requested
    // via --tls-listen, so any setup failure aborts boot loudly (silently
    // continuing AF_UNIX-only would be surprising).
    let tls_init = match tls_cli {
        Some(cli) => Some(setup_tls_listener(data_dir, &cli)?),
        None => None,
    };
    gnitz_note!("GnitzDB ready");

    Ok(ServerExecutor::run(dispatcher, server_fd, tls_init))
}

#[cfg(test)]
#[path = "tests/bootstrap.rs"]
mod tests;
