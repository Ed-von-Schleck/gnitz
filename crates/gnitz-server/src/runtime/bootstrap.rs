//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.
//!
//! **Recovery order is a crash guard**: every step is placed so a crash at any
//! point rebuilds a view rather than silently resuming a stale one.

use std::os::fd::AsRawFd;
use std::rc::Rc;

use crate::catalog::{CatalogEngine, UnreplayedCatalog};
use gnitz_foundation::fault::Seam;

use crate::runtime::affinity;
use crate::runtime::executor::ServerExecutor;
use crate::runtime::listen;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::mesh::{self, Mesh};
use crate::runtime::park::{WorkerPark, WorkerParks};
use crate::runtime::reactor::{select2, AckLease, Either, Limits, Reactor, BOOT_READY_REQUEST_ID};
use crate::runtime::sal::zone::CommittedTail;
use crate::runtime::sal::{SalLog, SalMessage, SalMessageKind, SalReader, SalWriter};
use crate::runtime::tls::TlsConfig;
use crate::runtime::w2m::{self, W2mReceiver, W2mWriter};
use crate::runtime::worker::WorkerProcess;
use gnitz_store::relation::{Residency, StoreConfig};
use gnitz_zset::algebra::Slot;

// ---------------------------------------------------------------------------
// SAL recovery: both drivers below read the log through `sal::zone::CommittedTail`
// and differ only in which groups are theirs and what they do with the bytes.
// A group whose rows do not decode fails the boot.
// ---------------------------------------------------------------------------

/// Stage every committed DdlSync group above its family's replay floor into the
/// system stores.
fn stage_system_tail(tail: CommittedTail, catalog: &mut UnreplayedCatalog) -> Result<(), String> {
    let floors = catalog.system_replay_floors();
    let mine = |msg: &SalMessage| {
        msg.kind == SalMessageKind::DdlSync && floors.get(&msg.target_id).is_some_and(|&f| msg.lsn > f)
    };

    let mut replayed: u32 = 0;
    for msg in tail.groups().filter(mine) {
        // A system family's group holds its batch as one payload.
        let Some(data) = msg.payloads().next() else {
            return Err(format!(
                "SAL replay: DdlSync group at offset={} (lsn={}) carries no payload",
                msg.base, msg.lsn
            ));
        };
        let Some(batch) = msg.rows(data, |_, _| None)? else {
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
/// flush, as a real flush failure would — one that, swallowed, would let the SAL
/// reset destroy the replayed rows' only durable copy.
static BOOT_FLUSH_ERROR: Seam = Seam::new("GNITZ_INJECT_BOOT_FLUSH_ERROR");

fn inject_recovery_panic(stage: &str) {
    if RECOVERY_PANIC.at(stage) {
        panic!("injected recovery panic at {stage}");
    }
}

/// Apply `slot`'s share of the tail's Push groups, each as a live push is. No LSN
/// floor: `enforce_unique_pk` makes re-applying a group the shards already hold a
/// no-op.
fn recover_from_sal(tail: CommittedTail, slot: Slot, catalog: &mut CatalogEngine) -> Result<(), String> {
    // Groups that applied at least one payload, and the width a re-sliced tail was
    // written at — the boot record's re-slice marker. No boot writes at a
    // previously-used epoch, so every group a walk sees carries the same width.
    let mut replayed: u32 = 0;
    let mut resliced_from: Option<u32> = None;
    for msg in tail.groups().filter(|m| m.kind == SalMessageKind::Push) {
        let tid = msg.target_id;
        // A table dropped later in the tail has no relation, and nothing to
        // recover into.
        let Some(relation) = catalog.registry.relation(tid) else {
            continue;
        };
        // A stream's push is written at LSN 0, which no tail group carries.
        debug_assert!(relation.kind().is_base_table());
        let (schema, placement) = (relation.schema(), relation.placement());
        // Written for another width, so no payload is this rank's share: every one
        // is re-cut for the launched topology.
        let reslice = msg.width() != slot.of;
        if reslice {
            resliced_from = Some(msg.width());
        }
        let own = (!reslice).then(|| msg.slot(slot.rank)).flatten();
        let all = reslice.then(|| msg.payloads()).into_iter().flatten();
        let mut applied = false;
        for data in own.into_iter().chain(all) {
            let Some(batch) = msg.rows(data, |tid, record| catalog.known_decode(tid, record))? else {
                continue;
            };
            let mut owned = if reslice {
                // The write path's own router, so what survives is exactly what the master
                // would have sent this rank. It reads only weights and PK bytes, so it
                // cuts before the widening below.
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
            catalog.ingest_unticked(tid, owned).map_err(|e| {
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

/// Worker-boot catalog recovery. On `Err` the worker exits without its ready
/// ACK, which fails the boot before the master rewinds the SAL — otherwise the
/// rewind destroys the replayed rows' only durable copy.
fn worker_boot_recovery(catalog: &mut CatalogEngine, tail: CommittedTail, slot: Slot) -> Result<(), String> {
    // Before any other catalog work, and before the replay below, which
    // projects the tail into each index exactly once.
    let rebuilt = catalog.open_stores(slot.rank, Residency::Worker)?;
    // Resume-vs-rebuild marker, the index sibling of the invalid-view line: 0 ⇒
    // every index resumed from its checkpoint.
    gnitz_note!("recovery: rebuilding {rebuilt} index(es)");
    recover_from_sal(tail, slot, catalog)?;
    // Before the master's boot rewind puts the write cursor back to 0: these rows
    // still live only in SAL entries a second crash would then overwrite.
    debug_assert!(
        !tail.holds_pushes() || catalog.durable_generation() > catalog.resume_generation,
        "boot base flush of replayed pushes without the pre-fork generation advance ahead of it",
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
pub fn server_main(data_dir: &str, socket_path: &str, num_workers: u32, tls: Option<TlsConfig>) -> i32 {
    match run_server(data_dir, socket_path, num_workers, tls) {
        Ok(rc) => rc,
        Err(e) => {
            gnitz_error!("{e}");
            1
        }
    }
}

/// One worker's share of the IPC: the SAL it reads, and its own ends.
struct WorkerIpc {
    sal: SalLog,
    tail: CommittedTail,
    w2m_writer: W2mWriter,
    park: WorkerPark,
    mesh: Mesh,
}

/// The SAL and every end of the regions the master and its forked workers talk
/// through.
///
/// Nothing here is reclaimed on error: the only response to a failed boot is to
/// exit the process, which returns all of it to the kernel.
struct SharedIpc {
    sal: SalLog,
    /// The committed tail every recovery replays, and the epoch the next writer
    /// epoch starts above. Read once with the mapping, before the fork.
    tail: CommittedTail,
    /// In rank order.
    workers: Vec<WorkerIpc>,
    /// The master's ends: the reader over every ring, and every worker's park
    /// to wake.
    receiver: W2mReceiver,
    parks: WorkerParks,
}

/// Open and map the SAL, the workers' parks, one W2M ring per worker, and the
/// exchange mesh.
fn acquire_shared_ipc(data_dir: &str, nw: usize) -> Result<SharedIpc, String> {
    let sal = SalLog::open(data_dir)?;
    let tail = CommittedTail::read(sal)?;
    let parks = WorkerParks::create(nw).map_err(|e| format!("failed to map the worker parks: {e}"))?;
    let (writers, receiver) = w2m::create(nw).map_err(|e| format!("failed to map the W2M rings: {e}"))?;
    let meshes =
        mesh::create(mesh::outbox_bytes(), parks).map_err(|e| format!("failed to map the exchange mesh: {e}"))?;
    let workers = writers
        .into_iter()
        .zip(meshes)
        .enumerate()
        .map(|(w, (w2m_writer, mesh))| WorkerIpc {
            sal,
            tail,
            w2m_writer,
            park: parks.park(w),
            mesh,
        })
        .collect();
    Ok(SharedIpc { sal, tail, workers, receiver, parks })
}

/// The forked child's whole life: latch its rank, redirect its logs to
/// `worker_N.log`, recover, and run the worker loop. Never returns — the worker
/// exits via `libc::_exit`.
fn run_worker_child(
    slot: Slot,
    data_dir: &str,
    master_pid: i32,
    catalog: &mut CatalogEngine,
    mut ipc: WorkerIpc,
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

    // Pinned before the recovery below allocates, so its pages land on this core's node.
    let boot = pinning
        .map_or(Ok(()), |p| p.enter_worker(w))
        .and_then(|()| worker_boot_recovery(catalog, ipc.tail, slot));
    if let Err(e) = boot {
        // No frame: the master's death watch fails the boot.
        gnitz_error!("{e}");
        unsafe { libc::_exit(1) };
    }

    gnitz_note!("Worker {} (pid {}) of {}", w, unsafe { libc::getpid() }, slot.of);

    let sal_reader = SalReader::new(ipc.sal, slot.rank, ipc.tail.live_epoch());
    // The id the master leases before it collects.
    ipc.w2m_writer.send_ack(BOOT_READY_REQUEST_ID);
    WorkerProcess::new(catalog, sal_reader, ipc.w2m_writer, ipc.park, ipc.mesh).run()
}

/// The master's half of recovery before any worker exists: the system families
/// made durable.
fn master_pre_fork_recovery(catalog: &mut CatalogEngine, tail: CommittedTail) -> Result<(), String> {
    if tail.holds_pushes() {
        // G → G+1, the resume generation left at G: the workers' boot flush moves
        // the base stores past what the views at G integrate, so until
        // `boot_checkpoint` restamps at G+1 a crash rebuilds every view instead
        // of resuming it.
        catalog.advance_durable_generation()?;
    } else {
        // No push is replayed, so every base store stays what the views at G
        // integrate, and a view this boot keeps is still valid at G.
        catalog
            .flush_all_system_tables()
            .map_err(|e| format!("boot system flush failed: {e}"))?;
    }
    inject_recovery_panic("genbump");
    Ok(())
}

/// Fork one child per worker, each of which never returns and takes its own
/// ends with it. Yields the parent's pid list.
fn fork_workers(
    catalog: &mut CatalogEngine,
    data_dir: &str,
    workers: Vec<WorkerIpc>,
    pinning: Option<&affinity::Pinning>,
) -> Result<Vec<i32>, String> {
    let master_pid = unsafe { libc::getpid() };
    let num_workers = workers.len() as u32;
    let mut worker_pids: Vec<i32> = Vec::with_capacity(workers.len());
    for (w, ipc) in workers.into_iter().enumerate() {
        match unsafe { libc::fork() } {
            -1 => return Err("fork failed".to_string()),
            0 => run_worker_child(
                Slot::new(w as u32, num_workers),
                data_dir,
                master_pid,
                catalog,
                ipc,
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
async fn master_post_fork_recovery(disp: &MasterDispatcher, ready: AckLease, live_epoch: u32) -> Result<(), String> {
    // Wait for all workers to complete recovery and signal readiness.
    ready.acks().await;
    drop(ready);

    disp.sal().lock().await.boot_rewind(live_epoch);

    inject_recovery_panic("reset");

    // Every scanned base, tail or none: the tick also compiles each kept view,
    // and the boot checkpoint publishes no trace of an uncompiled one.
    let scanned = {
        let cat = disp.cat();
        cat.dag.scanned_base_tables(&cat.registry)
    };
    disp.drain_tick(&scanned)
        .await
        .map_err(|e| format!("recovery tick sweep failed: {e}"))?;

    inject_recovery_panic("sweep");

    // Rebuild the views the boot verdict rejected, through the driver a live
    // CREATE VIEW uses; they opened empty and the sweep above skipped them.
    let invalid: Vec<u64> = disp.cat_mut().dag.take_rebuild().into_iter().collect();
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

/// Descriptors besides client connections: an fsync batch's chunk and a fixed
/// handful.
const FD_RESERVE: u64 = 1024;

/// Raise the `RLIMIT_NOFILE` soft limit towards `target`, capped by the hard
/// limit. Best-effort: a refusal surfaces later as `EMFILE` at the open that
/// could not be served.
fn raise_fd_limit(target: u64) {
    unsafe {
        let mut rl: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) != 0 || rl.rlim_cur >= target as libc::rlim_t {
            return;
        }
        rl.rlim_cur = (target as libc::rlim_t).min(rl.rlim_max);
        libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
    }
}

fn run_server(data_dir: &str, socket_path: &str, num_workers: u32, tls: Option<TlsConfig>) -> Result<i32, String> {
    let limits = Limits::from_env();
    // Client connections are the one descriptor demand that grows: a shard is
    // held by its mapping, and an fsync batch opens a bounded chunk.
    raise_fd_limit((limits.max_conns as u64).saturating_add(FD_RESERVE));

    // Before any io_uring worker thread exists: each keeps the mask it starts with.
    let nw = num_workers as usize;
    let pinning = affinity::pin_master(nw)?;

    gnitz_info!("Opening database at {}", data_dir);

    let mut opened = CatalogEngine::open_master(data_dir, num_workers, StoreConfig::from_env("GNITZ_"))
        .map_err(|e| format!("failed to open catalog: {e}"))?;
    listen::clear_published(data_dir)?;

    gnitz_note!("Starting {num_workers} workers");
    gnitz_note!("Worker logs: {}/worker_N.log (N=0..{})", data_dir, num_workers - 1);

    let ipc = acquire_shared_ipc(data_dir, nw)?;

    stage_system_tail(ipc.tail, &mut opened)?;
    let catalog = opened.replay().map_err(|e| format!("failed to replay catalog: {e}"))?;

    // Leaked: the dispatcher and reactor borrow it for the life of the process.
    let catalog: &'static mut CatalogEngine = Box::leak(Box::new(catalog));

    master_pre_fork_recovery(catalog, ipc.tail)?;

    let SharedIpc { sal, tail, workers, receiver, parks } = ipc;
    let worker_pids = fork_workers(catalog, data_dir, workers, pinning.as_ref())?;

    // --- Parent process ---
    // 256 SQEs sets submit batching, not depth: a full SQ is flushed.
    let reactor = Reactor::new(256, limits, receiver).map_err(|e| format!("io_uring init failed: {e}"))?;
    let dispatcher = Rc::new(MasterDispatcher::new(
        worker_pids,
        catalog,
        SalWriter::new(sal, parks),
        reactor,
    ));

    // The workers' ready ACKs. Leased before the loop first runs, which drops
    // every W2M frame no lease routes.
    let ready = dispatcher.reactor().lease_ready();
    let live_epoch = tail.live_epoch();
    let d = Rc::clone(&dispatcher);
    let logs = data_dir.to_owned();
    // Raced against a worker's death, which would leave recovery waiting forever.
    dispatcher.reactor().block_on(async move {
        let recovery = master_post_fork_recovery(&d, ready, live_epoch);
        match select2(recovery, d.worker_death()).await {
            Either::A(r) => r,
            Either::B(w) => Err(format!(
                "worker {w} exited during recovery (log: {logs}/worker_{w}.log)"
            )),
        }
    })?;

    let listeners = listen::bind_listeners(data_dir, socket_path, tls)?;
    Ok(ServerExecutor::run(dispatcher, data_dir, listeners))
}

#[cfg(test)]
#[path = "tests/bootstrap.rs"]
mod tests;
