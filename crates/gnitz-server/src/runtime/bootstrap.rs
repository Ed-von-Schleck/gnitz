//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.
//!
//! **Recovery order is a crash guard**: every step is placed so a crash at any
//! point rebuilds a view rather than silently resuming a stale one.

use std::collections::BTreeSet;
use std::num::NonZero;
use std::os::fd::AsRawFd;

use crate::catalog::{CatalogEngine, SysFamily, UnreplayedCatalog};
use gnitz_foundation::fault::Seam;

use crate::runtime::affinity;
use crate::runtime::executor;
use crate::runtime::listen;
use crate::runtime::master::{worker_death, MasterDispatcher};
use crate::runtime::mesh::{self, Mesh};
use crate::runtime::park::WorkerParks;
use crate::runtime::reactor::{check_host, Limits, Reactor};
use crate::runtime::sal::{CommittedTail, SalLog, SalMessage, SalReader, SalWriter};
use crate::runtime::signals;
use crate::runtime::sync::{select2, Either};
use crate::runtime::tls::{ResolvedTls, TlsListener};
use crate::runtime::w2m::{self, RequestId, W2mWriter};
use crate::runtime::worker::WorkerProcess;
use crate::runtime::ServerExit;
use gnitz_store::relation::{PkRule, Residency, StoreConfig};
use gnitz_zset::algebra::Slot;

// ---------------------------------------------------------------------------
// SAL recovery: both drivers below read the log through `sal::CommittedTail`
// and differ only in which groups are theirs and what they do with the bytes.
// A group whose rows do not decode fails the boot.
// ---------------------------------------------------------------------------

/// Stage every committed DdlSync group above its family's replay floor into the
/// system stores.
fn stage_system_tail(tail: CommittedTail, catalog: &mut UnreplayedCatalog) -> Result<(), String> {
    let floors = catalog.system_replay_floors();
    let mine = |msg: SalMessage| {
        let family = SysFamily::from_id(msg.head.target_id)?;
        let floor = *floors.get(&family.id())?;
        (msg.head.lsn > floor).then_some((msg, family))
    };

    let mut replayed: u32 = 0;
    for (msg, family) in tail.ddl_syncs().filter_map(mine) {
        let data = msg.shared_payload().ok_or_else(|| {
            format!(
                "SAL replay: DdlSync group at offset={} (lsn={}) carries no shared payload",
                msg.base, msg.head.lsn
            )
        })?;
        let Some(batch) = msg.rows(data, |_, _| None)? else {
            continue;
        };
        // Master-validated rows that already carry a drop's children.
        catalog.stage(family, msg.head.lsn, batch).map_err(|e| {
            format!(
                "SAL system-table recovery apply failed (table_id={}, lsn={}): {e}",
                msg.head.target_id, msg.head.lsn
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

/// Apply `slot`'s share of the tail's Push groups, each as a live push is but
/// always under [`PkRule::Enforce`], whatever rule the group carries. No LSN
/// floor: the enforced rule makes re-applying a group the shards already hold a
/// no-op, where a group's [`PkRule::Fresh`] holds only of the store it was first
/// applied to.
fn recover_from_sal(tail: CommittedTail, slot: Slot, catalog: &mut CatalogEngine) -> Result<(), String> {
    // Groups that applied at least one payload, and the width a re-sliced tail was
    // written at — the boot record's re-slice marker. No boot writes at a
    // previously-used epoch, so every group a walk sees carries the same width.
    let mut replayed: u32 = 0;
    let mut resliced_from: Option<u32> = None;
    for msg in tail.pushes() {
        let tid = msg.head.target_id;
        // A table dropped later in the tail has no relation, and nothing to
        // recover into.
        let Some(relation) = catalog.registry.relation(tid) else {
            continue;
        };
        debug_assert!(relation.kind().is_base_table());
        let (schema, placement) = (relation.schema(), relation.placement());
        // Written for another width, so no payload is this rank's share: every one
        // is re-cut for the launched topology.
        let reslice = msg.head.width() != slot.of;
        if reslice {
            resliced_from = Some(msg.head.width());
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
            catalog
                .ingest_unticked(tid, owned, PkRule::Enforce)
                .map_err(|e| format!("SAL replay apply failed (table_id={}, lsn={}): {e}", tid, msg.head.lsn))?;
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
/// ACK, which fails the boot.
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
        !tail.holds_pushes() || catalog.resume_at.is_none_or(|g| catalog.durable_generation() > g),
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

/// What argv asks the server to run.
pub(crate) struct ServerArgs {
    pub(crate) data_dir: String,
    pub(crate) socket_path: String,
    /// The worker count, or `None` (`--workers=auto`) to size it from the host.
    pub(crate) workers: Option<u32>,
    /// The directory BACKUP writes into; `None` refuses BACKUP.
    pub(crate) backup_dir: Option<String>,
}

/// Worker `w`'s log file, which its stdout and stderr are redirected to. The
/// name is also read by the test harnesses, which find a worker's log by it.
fn worker_log(data_dir: &str, w: usize) -> String {
    format!("{data_dir}/worker_{w}.log")
}

/// The forked child's whole life: latch its rank, redirect its logs to
/// `worker_N.log`, recover, and run the worker loop. Never returns — the worker
/// exits via `libc::_exit`.
fn run_worker_child(
    slot: Slot,
    data_dir: &str,
    master_pid: i32,
    catalog: &mut CatalogEngine,
    tail: CommittedTail,
    (mut w2m_writer, mut mesh): (W2mWriter, Mesh),
    pinning: Option<&affinity::Pinning>,
) -> ! {
    let w = slot.rank as usize;

    signals::detach_worker(master_pid);

    if let Ok(log) = std::fs::File::create(worker_log(data_dir, w)) {
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
        .and_then(|()| worker_boot_recovery(catalog, tail, slot));
    if let Err(e) = boot {
        // No frame: the master's death watch fails the boot.
        gnitz_error!("{e}");
        unsafe { libc::_exit(ServerExit::BootFailed.code()) };
    }

    gnitz_note!("Worker {} (pid {}) of {}", w, unsafe { libc::getpid() }, slot.of);
    // Pinned, this worker has a core of its own to spin on.
    if pinning.is_some() {
        mesh.spin_before_park();
    }

    let sal_reader = SalReader::new(tail.log(), slot.rank, tail.live_epoch());
    w2m_writer.ack(RequestId::BOOT_READY);
    WorkerProcess::new(catalog, sal_reader, w2m_writer, mesh).run()
}

/// The master's half of recovery before any worker exists: the catalog staged
/// and replayed from the tail, and the system families made durable.
fn master_pre_fork_recovery(mut opened: UnreplayedCatalog, tail: CommittedTail) -> Result<CatalogEngine, String> {
    stage_system_tail(tail, &mut opened)?;
    let mut catalog = opened.replay().map_err(|e| format!("failed to replay catalog: {e}"))?;
    if tail.holds_pushes() {
        // G → G+1, the resume point left at G: the workers' boot flush moves
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
    Ok(catalog)
}

/// Fork one child per worker, each of which never returns and takes its own
/// ends with it. Yields the parent's pid list.
fn fork_workers(
    catalog: &mut CatalogEngine,
    data_dir: &str,
    tail: CommittedTail,
    workers: Vec<(W2mWriter, Mesh)>,
    pinning: Option<&affinity::Pinning>,
) -> Result<Vec<NonZero<libc::pid_t>>, String> {
    let master_pid = unsafe { libc::getpid() };
    let num_workers = workers.len() as u32;
    let mut worker_pids = Vec::with_capacity(workers.len());
    for (w, ipc) in workers.into_iter().enumerate() {
        match unsafe { libc::fork() } {
            -1 => return Err("fork failed".to_string()),
            0 => run_worker_child(
                Slot::new(w as u32, num_workers),
                data_dir,
                master_pid,
                catalog,
                tail,
                ipc,
                pinning,
            ),
            pid => worker_pids.push(NonZero::new(pid).expect("fork answers the parent a child's pid")),
        }
    }
    Ok(worker_pids)
}

/// The master's half of recovery once the workers have recovered and the SAL is
/// rewound, ending in the checkpoint a clean restart resumes from.
/// `pushed` holds the base tables the tail pushed to.
async fn master_post_fork_recovery(disp: &MasterDispatcher, pushed: BTreeSet<u64>) -> Result<(), String> {
    // The scanned ones: only a replayed push lands rows above a base store's cut,
    // and the backfill below reads at the cut. A view's operator state needs no
    // tick: the boot checkpoint opens every resumed view's.
    let ticked: Vec<u64> = {
        let cat = disp.cat();
        pushed.into_iter().filter(|&t| cat.dag.is_scanned(t)).collect()
    };
    disp.drain_tick(&ticked)
        .await
        .map_err(|e| format!("recovery tick sweep failed: {e}"))?;

    inject_recovery_panic("sweep");

    // Rebuild the views the boot verdict rejected, through the driver a live
    // CREATE VIEW uses; they opened empty, and no tick reaches them until their
    // backfill.
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

/// Keeps freed memory in the heap for the next batch: with a fixed mmap
/// threshold and a trim threshold above any phase's working set, glibc stops
/// unmapping and trimming on free, which the next allocation would fault in
/// again. Set before the fork, so every worker inherits it.
fn keep_freed_memory_mapped() {
    // SAFETY: mallopt only adjusts glibc's allocator parameters.
    unsafe {
        libc::mallopt(libc::M_MMAP_THRESHOLD, 32 << 20);
        libc::mallopt(libc::M_TRIM_THRESHOLD, 1 << 30);
    }
}

/// Single entry point for the entire server bootstrap: opens the catalog,
/// allocates the shared IPC regions, forks the workers, runs recovery, and
/// enters the executor event loop, with a TLS listener when `tls` names one.
/// Answers how serving ended; `Err` when the boot failed.
pub(crate) fn server_main(args: ServerArgs, tls: Option<ResolvedTls>) -> Result<ServerExit, String> {
    let data_dir = args.data_dir.as_str();
    // Ahead of anything a refusal would have to undo: the reactor cannot run
    // without io_uring, and nothing else tells a container why.
    check_host()?;
    keep_freed_memory_mapped();
    signals::catch_reload();
    let limits = Limits::from_env();
    // Client connections are the one descriptor demand that grows: a shard is
    // held by its mapping, and an fsync batch opens a bounded chunk.
    raise_fd_limit((limits.max_conns as u64).saturating_add(FD_RESERVE));

    // Before any io_uring worker thread exists: each keeps the mask it starts with.
    let (num_workers, pinning) = affinity::pin_master(args.workers)?;
    let nw = num_workers as usize;

    gnitz_info!("Opening database at {}", data_dir);

    // Held for the life of the process, from before any store opens; a forked
    // worker shares it.
    let _dir_lock = gnitz_store::relation::lock_data_dir(data_dir)?;
    let opened = CatalogEngine::open_master(data_dir, num_workers, StoreConfig::from_env("GNITZ_"))
        .map_err(|e| format!("failed to open catalog: {e}"))?;
    listen::clear_published(data_dir)?;
    // Ahead of recovery, so an identity that cannot be read or minted refuses
    // the boot before it costs one.
    let tls = tls.map(|args| TlsListener::open(args, data_dir)).transpose()?;

    gnitz_note!("Starting {num_workers} workers");
    // The template of `worker_log`.
    gnitz_note!("Worker logs: {}/worker_N.log (N=0..{})", data_dir, num_workers - 1);

    // The committed tail every recovery replays, and the epoch the next writer
    // epoch starts above. Read once, before the fork.
    let log = SalLog::open(data_dir)?;
    let tail = CommittedTail::read(log)?;
    log.advise_random();
    // The workers' parks, one W2M ring per worker, and the exchange mesh. None of
    // it is reclaimed on error: a failed boot exits the process, which returns
    // all of it to the kernel.
    let parks = WorkerParks::create(nw).map_err(|e| format!("failed to map the worker parks: {e}"))?;
    let (writers, receiver) = w2m::create(parks).map_err(|e| format!("failed to map the W2M rings: {e}"))?;
    let meshes =
        mesh::create(mesh::outbox_bytes(), parks).map_err(|e| format!("failed to map the exchange mesh: {e}"))?;
    let workers: Vec<(W2mWriter, Mesh)> = writers.into_iter().zip(meshes).collect();

    let mut catalog = master_pre_fork_recovery(opened, tail)?;

    let worker_pids = fork_workers(&mut catalog, data_dir, tail, workers, pinning.as_ref())?;

    // --- Parent process ---
    let (reactor, ready) = Reactor::new(limits, receiver).map_err(|e| format!("io_uring init failed: {e}"))?;
    // Both waits race a worker's death, which would leave them waiting forever.
    let died = |w: usize| format!("worker {w} exited during recovery (log: {})", worker_log(data_dir, w));
    let acked = reactor.block_on(async {
        match select2(ready, worker_death(&reactor, &worker_pids)).await {
            Either::A(acked) => Ok(acked),
            Either::B(w) => Err(died(w)),
        }
    })?;
    // Read before the writer's rewind empties the ring the tail walks.
    let pushed = tail.pushed();
    let sal = SalWriter::boot(tail, parks, acked);
    inject_recovery_panic("reset");

    let dispatcher = MasterDispatcher::new(worker_pids, catalog, sal, reactor);
    dispatcher.reactor().block_on(async {
        match select2(
            master_post_fork_recovery(&dispatcher, pushed),
            dispatcher.worker_death(),
        )
        .await
        {
            Either::A(r) => r,
            Either::B(w) => Err(died(w)),
        }
    })?;

    let listeners = listen::bind_listeners(data_dir, &args.socket_path, tls)?;
    match executor::run(dispatcher, args.backup_dir, listeners) {
        None => Ok(ServerExit::Clean),
        Some(w) => {
            gnitz_error!("Worker {w} crashed (log: {}); workers stopped", worker_log(data_dir, w));
            Ok(ServerExit::WorkerDied)
        }
    }
}

#[cfg(test)]
#[path = "tests/bootstrap.rs"]
mod tests;
