//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.
//!
//! **Recovery order is a crash guard**: every step is placed so a crash at any
//! point rebuilds a view rather than silently resuming a stale one.

use std::ops::Range;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::rc::Rc;

use crate::catalog::{CatalogEngine, UnreplayedCatalog};
use gnitz_foundation::fault::Seam;
use gnitz_foundation::posix_io;

use crate::runtime::affinity;
use crate::runtime::executor::ServerExecutor;
use crate::runtime::listen;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::mesh::{self, Mesh};
use crate::runtime::reactor::{AckLease, Limits, Reactor};
use crate::runtime::sal::zone::CommittedTail;
use crate::runtime::sal::{sal_mmap_size, SalLog, SalMessage, SalMessageKind, SalReader, SalWriter};
use crate::runtime::tls::TlsArgs;
use crate::runtime::w2m::{self, SalWake, W2mReceiver, W2mWriter, BOOT_READY_REQUEST_ID};
use crate::runtime::wire as ipc;
use crate::runtime::worker::WorkerProcess;
use gnitz_store::relation::Residency;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;
use gnitz_zset::schema::Slot;

// ---------------------------------------------------------------------------
// SAL recovery: both drivers below read the log through `sal::zone::CommittedTail`
// and differ only in which groups are theirs and what they do with the bytes.
// ---------------------------------------------------------------------------

/// Decode one committed group slot's batch, `None` if it carries no rows; one
/// that does not decode fails the boot.
fn decode_group_slot(
    msg: &SalMessage,
    data: &[u8],
    known: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
) -> Result<Option<Batch>, String> {
    let decoded = ipc::decode_sal_slot(data, known).map_err(|e| {
        format!(
            "SAL replay: corrupt block at offset={} lsn={} target={}: {e}",
            msg.base, msg.lsn, msg.target_id
        )
    })?;
    Ok(decoded.data_batch.filter(|b| !b.is_empty()))
}

/// Stage every committed DdlSync group above its family's replay floor into the
/// system stores.
fn stage_system_tail(tail: CommittedTail, catalog: &mut UnreplayedCatalog) -> Result<(), String> {
    let floors = catalog.system_replay_floors();
    let mine = |msg: &SalMessage| {
        msg.kind == SalMessageKind::DdlSync && floors.get(&msg.target_id).is_some_and(|&f| msg.lsn > f)
    };

    let mut replayed: u32 = 0;
    for msg in tail.groups().filter(mine) {
        // A system family broadcasts: slot 0 carries the whole batch.
        let Some(data) = msg.slot(0) else {
            return Err(format!(
                "SAL replay: DdlSync group at offset={} (lsn={}) carries no slot 0",
                msg.base, msg.lsn
            ));
        };
        let Some(batch) = decode_group_slot(&msg, data, |_, _| None)? else {
            continue;
        };
        // Master-validated rows that already carry a drop's children.
        catalog.stage(msg.target_id, msg.lsn, batch).map_err(|e| {
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

/// `GNITZ_INJECT_BOOT_FLUSH_ERROR`: fail a worker's boot recovery after its base
/// flush, so the error rides the startup ACK as a real flush failure's would —
/// one that, swallowed, would let the SAL reset destroy the replayed rows' only
/// durable copy.
static BOOT_FLUSH_ERROR: Seam = Seam::new("GNITZ_INJECT_BOOT_FLUSH_ERROR");

fn inject_recovery_panic(stage: &str) {
    if RECOVERY_PANIC.at(stage) {
        panic!("injected recovery panic at {stage}");
    }
}

/// Base tables feeding ≥1 view that is keeping its checkpointed state, sorted
/// for a reproducible drive order. Keyed on the boot verdict and not on what the
/// tail contained, because the sweep is also what compiles a view at boot, and
/// the boot checkpoint publishes traces only for compiled views.
fn swept_base_tables(catalog: &CatalogEngine) -> Vec<u64> {
    let keeps_state: Vec<u64> = catalog
        .registry
        .view_ids()
        .into_iter()
        .filter(|&vid| !catalog.dag.awaits_rebuild(vid))
        .collect();
    catalog.dag.base_tables_reachable_from(&catalog.registry, keeps_state)
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
/// through the registry's ingest, whose PK rule makes retractions cancel. Each
/// swept base's effective delta is buffered as unticked, for the master's tick
/// sweep to drain into the views.
///
/// No LSN floor: `enforce_unique_pk` makes re-applying a group the shards
/// already hold a no-op.
fn recover_from_sal(
    tail: CommittedTail,
    slot: Slot,
    swept_bases: &[u64],
    catalog: &mut CatalogEngine,
) -> Result<(), String> {
    // Groups that applied at least one slot, and the width a re-sliced tail was
    // written at — the boot record's re-slice marker. No boot writes at a
    // previously-used epoch, so every group a walk sees carries the same width.
    let mut replayed: u32 = 0;
    let mut resliced_from: Option<u32> = None;
    for msg in tail.groups().filter(|m| m.kind == SalMessageKind::Push) {
        let tid = msg.target_id;
        // A table dropped later in the tail has no relation, and nothing to
        // recover into.
        let Some((schema, placement)) = catalog.registry.relation(tid).map(|r| (r.schema(), r.placement())) else {
            continue;
        };
        let (wanted, reslice) = replay_slots(msg.slots(), slot, placement.is_replicated());
        if reslice {
            resliced_from = Some(msg.slots());
        }
        let mut applied = false;
        for (_, data) in msg.slots_written().filter(|(w, _)| wanted.contains(w)) {
            let Some(batch) = decode_group_slot(&msg, data, |tid, record| catalog.known_decode(tid, record))? else {
                continue;
            };
            let mut owned = if reslice {
                // The write path's own router, so what survives is exactly what the master
                // would have written to this rank's slot. It reads only weights and PK
                // bytes, so it cuts before the widening below.
                gnitz_zset::algebra::ScatterPlan::native(placement).share(&batch, slot)
            } else {
                batch
            };
            if owned.is_empty() {
                continue;
            }
            // The ingest rejects a payload-count mismatch outright.
            if owned.schema().num_payload_cols() < schema.num_payload_cols() {
                owned = owned.widened_with_nulls(&schema, false);
            }
            // `base_tables_reachable_from` returns them sorted and deduplicated.
            let ingested = match swept_bases.binary_search(&tid) {
                Ok(_) => catalog.ingest_unticked(tid, owned),
                Err(_) => catalog.registry.ingest(tid, owned),
            };
            ingested.map_err(|e| {
                format!(
                    "SAL replay apply failed (table_id={}, lsn={}): {e}",
                    msg.target_id, msg.lsn
                )
            })?;
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
    Ok(())
}

/// Worker-boot catalog recovery. The `Err` rides the startup ACK, which fails
/// the boot before the master rewinds the SAL — otherwise the rewind destroys
/// the replayed rows' only durable copy.
fn worker_boot_recovery(
    catalog: &mut CatalogEngine,
    tail: CommittedTail,
    slot: Slot,
    swept_bases: &[u64],
) -> Result<(), String> {
    // Before any other catalog work, and before the replay below, which
    // projects the tail into each index exactly once.
    let rebuilt = catalog.open_stores(slot.rank, Residency::Worker)?;
    // Resume-vs-rebuild marker, the index sibling of the invalid-view line: 0 ⇒
    // every index resumed from its checkpoint.
    gnitz_note!("recovery: rebuilding {rebuilt} index(es)");
    recover_from_sal(tail, slot, swept_bases, catalog)?;
    // Before the master's boot rewind puts the write cursor back to 0: these rows
    // still live only in SAL entries a second crash would then overwrite.
    debug_assert!(
        catalog.durable_generation() > catalog.registry.resume_generation(),
        "boot base flush without the pre-fork generation advance ahead of it",
    );
    catalog
        .registry
        .checkpoint_base()
        .map_err(|e| format!("boot flush failed: {e}"))?;
    if BOOT_FLUSH_ERROR.armed() {
        return Err("injected boot flush fault".to_string());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Server main entry point
// ---------------------------------------------------------------------------

/// Single entry point for the entire server bootstrap: opens the catalog,
/// allocates the shared IPC regions, forks the workers, runs recovery, and
/// enters the executor event loop.
///
/// Returns 0 on clean exit, non-zero on error.
pub fn server_main(data_dir: &str, socket_path: &str, num_workers: u32, tls: Option<TlsArgs>) -> i32 {
    match run_server(data_dir, socket_path, num_workers, tls) {
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
    sal_fd: BorrowedFd<'static>,
    sal: SalLog,
    /// The committed tail every recovery replays, and the epoch the next writer
    /// epoch starts above. Read once with the mapping, before the fork.
    tail: CommittedTail,
    w2m_ptrs: Vec<*mut u8>,
    /// The workers' exchange mesh.
    mesh: *mut u8,
}

/// Open and map the SAL, one W2M ring per worker, and the exchange mesh.
fn acquire_shared_ipc(data_dir: &str, nw: usize) -> Result<SharedIpc, String> {
    // A fresh SAL file reads all-zero through O_CREAT + fallocate, which is
    // exactly the empty-SAL state recovery expects.
    let sal_file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o644)
            .open(format!("{data_dir}/wal.sal"))
            .map_err(|e| format!("failed to open SAL file: {e}"))?
    };
    // Leaked: the writer and the reactor's fsync SQEs name it for the process's life.
    let sal_file: &'static std::fs::File = Box::leak(Box::new(sal_file));
    let sal_fd = sal_file.as_fd();
    // The SAL is a real file, and reserving its blocks now is what keeps a later
    // write from failing for want of disk space.
    let sal_len = sal_mmap_size();
    let sal_ptr = posix_io::map_file_reserved(sal_fd.as_raw_fd(), sal_len)
        .map_err(|e| format!("failed to map SAL ({sal_len} bytes): {e}"))?;
    // SAFETY: the mapping above is `sal_len` bytes and outlives the process.
    let sal = unsafe { SalLog::new(sal_ptr, sal_len) };
    let tail = CommittedTail::read(sal)?;

    let w2m_ptrs = (0..nw)
        .map(|w| w2m::create_region().map_err(|e| format!("failed to map W2M region for W{w}: {e}")))
        .collect::<Result<Vec<_>, _>>()?;
    let mesh =
        mesh::create_region(nw, mesh::outbox_bytes()).map_err(|e| format!("failed to map the exchange mesh: {e}"))?;

    Ok(SharedIpc { sal_fd, sal, tail, w2m_ptrs, mesh })
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
    swept_bases: &[u64],
    pinning: Option<&affinity::Pinning>,
) -> ! {
    let w = slot.rank as usize;

    // Die immediately if the master exits for any reason: a worker parks on the
    // SAL with no timeout, so this is its only notice.
    unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) };
    // Re-check: parent may have died in the fork→prctl window.
    if unsafe { libc::getppid() } != master_pid {
        unsafe { libc::_exit(0) };
    }

    if let Ok(log) = std::fs::File::create(format!("{data_dir}/worker_{w}.log")) {
        unsafe {
            libc::dup2(log.as_raw_fd(), 1);
            libc::dup2(log.as_raw_fd(), 2);
        }
    }

    // Re-tag logging as this worker before any boot work, so every line the
    // recovery below emits carries `W{w}` rather than the inherited master tag.
    // Only the tag: the level is a process-wide static the fork already copied.
    gnitz_foundation::log::set_tag(gnitz_foundation::log::Tag::Worker(w as u32));

    let w2m_writer = W2mWriter::new(ipc.w2m_ptrs[w]);
    let catalog_ptr: *mut CatalogEngine = catalog;

    // Pinned before the recovery below allocates, so its pages land on this core's node.
    let boot = pinning
        .map_or(Ok(()), |p| p.enter_worker(w))
        .and_then(|()| worker_boot_recovery(catalog, ipc.tail, slot, swept_bases));
    if let Err(e) = boot {
        // The master reads this frame off the shared ring, which outlives the
        // process that wrote it.
        gnitz_error!("{e}");
        w2m_writer.send_status(BOOT_READY_REQUEST_ID, gnitz_wire::WireStatus::Error, e.as_bytes());
        unsafe { libc::_exit(1) };
    }

    gnitz_note!("Worker {} (pid {}) of {}", w, unsafe { libc::getpid() }, slot.of);

    let sal_reader = SalReader::new(ipc.sal, slot.rank, ipc.tail.live_epoch());
    // SAFETY: every ring was initialized by `create_region` and the mesh mapped
    // for `slot.of` workers, all before the fork, and none is ever unmapped.
    let mesh = unsafe {
        let wakes = ipc.w2m_ptrs.iter().map(|&p| SalWake::new(p)).collect();
        Mesh::new(ipc.mesh, w, wakes)
    };
    let mut worker = WorkerProcess::new(catalog_ptr, sal_reader, w2m_writer, mesh);
    let rc = worker.run(BOOT_READY_REQUEST_ID);

    unsafe {
        libc::_exit(rc);
    }
}

/// The master's half of recovery before any worker exists: the sweep set every
/// worker inherits.
fn master_pre_fork_recovery(catalog: &mut CatalogEngine) -> Result<Vec<u64>, String> {
    // G → G+1, the resume generation left at G: until `boot_checkpoint`
    // restamps at G+1, a crash rebuilds every view instead of resuming it.
    catalog.advance_durable_generation()?;
    inject_recovery_panic("genbump");

    catalog
        .registry
        .reconcile_child_dirs()
        .map_err(|e| format!("child-dir sweep failed: {e}"))?;

    // Pre-fork because it reads every launched rank's manifest on behalf of
    // workers that do not exist yet.
    catalog.compute_invalid_views();

    Ok(swept_base_tables(catalog))
}

/// Fork one child per worker, each of which never returns. Yields the parent's
/// pid list.
fn fork_workers(
    catalog: &mut CatalogEngine,
    data_dir: &str,
    num_workers: u32,
    ipc: &SharedIpc,
    swept_bases: &[u64],
    pinning: Option<&affinity::Pinning>,
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
                pinning,
            ),
            pid => worker_pids.push(pid),
        }
    }
    Ok(worker_pids)
}

/// The master's half of recovery once the workers are alive, ending in the
/// checkpoint a clean restart resumes from. `ready` is the lease the workers'
/// ready ACKs answer.
async fn master_post_fork_recovery(
    disp: Rc<MasterDispatcher>,
    ready: AckLease,
    live_epoch: u32,
    swept_bases: Vec<u64>,
) -> Result<(), String> {
    // Wait for all workers to complete recovery and signal readiness.
    disp.collect_round(&ready)
        .await
        .map_err(|e| format!("Error collecting worker acks: {e}"))?;
    drop(ready);

    disp.sal().lock().await.boot_rewind(live_epoch);

    inject_recovery_panic("reset");

    // Drive the tail each worker buffered during replay into the views, one
    // tick group over every swept base. An empty source ticks too, so exchange views stay in
    // lockstep and every view this reaches is compiled.
    disp.drain_tick(&swept_bases)
        .await
        .map_err(|e| format!("recovery tick sweep failed: {e}"))?;

    inject_recovery_panic("sweep");

    // Rebuild the views the boot verdict rejected, through the driver a live
    // CREATE VIEW uses; they opened empty and the sweep above skipped them.
    let invalid: Vec<u64> = disp.cat().dag.take_rebuild().into_iter().collect();
    // Resume-vs-rebuild marker (asserted by the "no backfill on clean restart"
    // E2E): 0 ⇒ every view resumed from its checkpoint.
    gnitz_note!("recovery: rebuilding {} invalid view(s)", invalid.len());
    disp.backfill_views_in_dep_order(&invalid)
        .await
        .map_err(|e| format!("invalid-view rebuild failed: {e}"))?;

    inject_recovery_panic("backfill");

    // A full checkpoint before the socket opens, so a clean restart resumes from
    // it. No drain — recovery already drained everything and no pushes are
    // admitted yet.
    disp.boot_checkpoint()
        .await
        .map_err(|e| format!("boot checkpoint failed: {e}"))?;
    Ok(())
}

fn run_server(data_dir: &str, socket_path: &str, num_workers: u32, tls: Option<TlsArgs>) -> Result<i32, String> {
    let tls = tls.map(TlsArgs::resolve).transpose()?;

    // Child directories + shard files.
    posix_io::raise_fd_limit(65536);

    // Before any io_uring worker thread exists: each keeps the mask it starts with.
    let nw = num_workers as usize;
    let pinning = affinity::pin_master(nw)?;

    gnitz_info!("Opening database at {}", data_dir);

    let mut opened =
        CatalogEngine::open_master(data_dir, num_workers).map_err(|e| format!("failed to open catalog: {e}"))?;
    listen::clear_published(data_dir)?;

    gnitz_note!("Starting {num_workers} workers");
    gnitz_note!("Worker logs: {}/worker_N.log (N=0..{})", data_dir, num_workers - 1);

    let ipc = acquire_shared_ipc(data_dir, nw)?;
    gnitz_debug!("SAL fd={}", ipc.sal_fd.as_raw_fd());

    stage_system_tail(ipc.tail, &mut opened)?;
    let catalog = opened.replay().map_err(|e| format!("failed to replay catalog: {e}"))?;

    // Leaked: the dispatcher and reactor borrow it for the life of the process.
    let catalog: &'static mut CatalogEngine = Box::leak(Box::new(catalog));

    let swept_bases = master_pre_fork_recovery(catalog)?;

    let worker_pids = fork_workers(catalog, data_dir, num_workers, &ipc, &swept_bases, pinning.as_ref())?;

    // --- Parent process ---
    let SharedIpc { sal_fd, sal, tail, w2m_ptrs, .. } = ipc;

    // Pre-fork recovery has already advanced the generation, so this is the floor
    // every base round must publish past.
    let boot_generation = catalog.durable_generation();
    // SAFETY: every ring was initialized by `create_region` and is never unmapped.
    let wakes = w2m_ptrs.iter().map(|&p| unsafe { SalWake::new(p) }).collect();
    // 256 SQEs sets submit batching, not depth: a full SQ is flushed.
    let reactor = Rc::new(
        Reactor::new(256, Limits::from_env(), W2mReceiver::new(w2m_ptrs))
            .map_err(|e| format!("io_uring init failed: {e}"))?,
    );
    let dispatcher = Rc::new(MasterDispatcher::new(
        worker_pids,
        catalog,
        boot_generation,
        SalWriter::new(sal, sal_fd, nw, wakes),
        reactor,
    ));

    // The workers' ready ACKs. Leased before the first tick, which drops every
    // W2M frame no lease routes.
    let ready = dispatcher.reactor().lease_acks("recovery sync");
    assert_eq!(
        ready.id(),
        BOOT_READY_REQUEST_ID,
        "the ready ACKs name the reactor's first lease"
    );
    dispatcher.reactor().block_on(master_post_fork_recovery(
        Rc::clone(&dispatcher),
        ready,
        tail.live_epoch(),
        swept_bases,
    ))?;

    let listeners = listen::bind_listeners(data_dir, socket_path, tls)?;
    gnitz_note!("GnitzDB ready");
    Ok(ServerExecutor::run(dispatcher, data_dir, listeners))
}

#[cfg(test)]
#[path = "tests/bootstrap.rs"]
mod tests;
