//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.
//!
//! **Recovery order is a crash guard**: every step is placed so a crash at any
//! point rebuilds a view rather than silently resuming a stale one.

use gnitz_foundation::{gnitz_error, gnitz_info, gnitz_note};
use std::collections::BTreeSet;
use std::num::NonZero;
use std::os::fd::AsRawFd;

use crate::catalog::{CatalogEngine, SysFamily};
use gnitz_foundation::fault::{Stage, Switch};

use crate::runtime::ServerExit;
use crate::runtime::affinity;
use crate::runtime::executor;
use crate::runtime::listen;
use crate::runtime::master::{MasterDispatcher, Workers};
use crate::runtime::mesh::{self, Mesh};
use crate::runtime::park::WorkerParks;
use crate::runtime::reactor::{Limits, Reactor, check_host};
use crate::runtime::sal::{CommittedTail, Ingest, SalWriter};
use crate::runtime::signals;
use crate::runtime::sync::{Either, select2};
use crate::runtime::tls::{ResolvedTls, TlsListener};
use crate::runtime::w2m::{self, W2mWriter};
use crate::runtime::worker::WorkerProcess;
use gnitz_store::relation::{PkRule, Residency, StoreConfig};
use gnitz_zset::algebra::Slot;
use gnitz_zset::repr::Batch;

// ---------------------------------------------------------------------------
// SAL recovery: both drivers below read the log through `sal::CommittedTail`
// and differ only in which groups are theirs and what they do with the bytes.
// A group that does not decode, head or rows, fails the boot.
// ---------------------------------------------------------------------------

/// The tail's committed DdlSync groups, each with its LSN, in log order:
/// what the catalog stages at open.
fn system_tail(tail: &CommittedTail) -> impl Iterator<Item = Result<(SysFamily, u64, Batch), String>> {
    tail.ingests().filter_map(move |group| {
        let (family, msg) = match group {
            Ok((Ingest::DdlSync { family }, msg)) => (family, msg),
            Ok(_) => return None,
            Err(e) => return Some(Err(e)),
        };
        // A DdlSync group is shared: its one payload holds the rows, which the
        // master validated and which already carry a drop's children.
        msg.rows(msg.payloads().next().unwrap_or_default(), |_, _| None)
            .map(|rows| Some((family, tail.lsn(&msg), rows?)))
            .transpose()
    })
}

/// `GNITZ_INJECT_RECOVERY_PANIC=<stage>`: panic when recovery reaches the named
/// stage, so the crash-window E2E tests can cut boot at a precise point.
static RECOVERY_PANIC: Stage = Stage::new(
    "GNITZ_INJECT_RECOVERY_PANIC",
    &["genbump", "reset", "backfill", "sweep"],
);

/// `GNITZ_INJECT_BOOT_FLUSH_ERROR`: fail a worker's boot recovery after its base
/// flush, as a real flush failure would — one that, swallowed, would let the SAL
/// reset destroy the replayed rows' only durable copy.
static BOOT_FLUSH_ERROR: Switch = Switch::new("GNITZ_INJECT_BOOT_FLUSH_ERROR");

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
fn recover_from_sal(tail: &CommittedTail, slot: Slot, catalog: &mut CatalogEngine) -> Result<(), String> {
    // Groups that applied at least one payload, and the width a re-sliced tail was
    // written at — the boot record's re-slice marker. No boot writes at a
    // previously-used epoch, so every group a walk sees carries the same width.
    let mut replayed: u32 = 0;
    let mut resliced_from: Option<u32> = None;
    for group in tail.ingests() {
        let (Ingest::Push { tid, .. }, msg) = group? else {
            continue;
        };
        // A table dropped later in the tail has no relation, and nothing to
        // recover into.
        let Some(relation) = catalog.registry.relation(tid) else {
            continue;
        };
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
                owned = gnitz_zset::algebra::null_extend(&owned, &schema, false);
            }
            catalog.ingest_unticked(tid, owned, PkRule::Enforce).map_err(|e| {
                format!(
                    "SAL replay apply failed (table_id={}, lsn={}): {e}",
                    tid,
                    tail.lsn(&msg)
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

/// Worker-boot catalog recovery. On `Err` the worker exits without publishing
/// its position, which fails the boot.
fn worker_boot_recovery(catalog: &mut CatalogEngine, tail: &CommittedTail, slot: Slot) -> Result<(), String> {
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
        !tail.holds_pushes() || catalog.durable_generation() > catalog.boot.resume_at,
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
    /// The permission bits `socket_path` is created with.
    pub(crate) socket_mode: u32,
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
/// `worker_N.log`, recover, and run the worker loop. Never returns: a
/// failed boot `_exit`s, and a worker that serves runs until it is killed or
/// fail-stops.
fn run_worker_child(
    slot: Slot,
    data_dir: &str,
    master_pid: i32,
    catalog: &mut CatalogEngine,
    tail: &CommittedTail,
    (mut w2m_writer, mesh): (W2mWriter, Mesh),
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
    let sal_reader = tail.reader(slot.rank);
    // The boot ACK: the recovery above has flushed what it replayed.
    w2m_writer.applied(sal_reader.position());
    WorkerProcess::new(catalog, sal_reader, w2m_writer, mesh, pinning.is_some()).run()
}

/// The master's half of recovery before any worker exists: the catalog opened
/// over the tail's system groups, the system families made durable, and the
/// boot's epoch raised above what they hold — every worker's drain starts at
/// the epoch the writer does.
fn master_pre_fork_recovery(
    data_dir: &str,
    num_workers: u32,
    config: StoreConfig,
    tail: &mut CommittedTail,
) -> Result<CatalogEngine, String> {
    let mut catalog = CatalogEngine::open_master(data_dir, num_workers, config, system_tail(tail))
        .map_err(|e| format!("failed to open catalog: {e}"))?;
    // With a replayed push, G → G+1, the resume point left at G: the workers'
    // boot flush moves the base stores past what the views at G integrate, so
    // until the boot's ephemeral round restamps at G+1 a crash rebuilds every
    // view instead of resuming it. With none, every base store stays what the
    // views at G integrate, and a view this boot keeps is still valid at G.
    catalog
        .flush_ahead_of_base_round(tail.holds_pushes())
        .map_err(|e| format!("boot system flush failed: {e}"))?;
    inject_recovery_panic("genbump");
    tail.write_above(catalog.applied_zone());
    Ok(catalog)
}

/// Fork one child per worker, each of which never returns and takes its own
/// ends with it. Yields the parent's children; on a failed fork, the ones
/// already forked are killed and reaped.
fn fork_workers(
    catalog: &mut CatalogEngine,
    data_dir: &str,
    tail: &CommittedTail,
    workers: Vec<(W2mWriter, Mesh)>,
    pinning: Option<&affinity::Pinning>,
) -> Result<Workers, String> {
    let master_pid = unsafe { libc::getpid() };
    let num_workers = workers.len() as u32;
    let mut forked = Workers::new(Vec::with_capacity(workers.len()));
    for (w, ipc) in workers.into_iter().enumerate() {
        match unsafe { libc::fork() } {
            -1 => {
                return Err(format!(
                    "fork of worker {w} failed: {}",
                    std::io::Error::last_os_error()
                ));
            }
            0 => run_worker_child(
                Slot::new(w as u32, num_workers),
                data_dir,
                master_pid,
                catalog,
                tail,
                ipc,
                pinning,
            ),
            pid => forked.push(NonZero::new(pid).expect("fork answers the parent a child's pid")),
        }
    }
    Ok(forked)
}

/// The master's half of recovery once the workers have recovered and the SAL is
/// rewound, ending in the checkpoint a clean restart resumes from.
/// `pushed` holds the base tables the tail pushed to.
async fn master_post_fork_recovery(disp: &MasterDispatcher, pushed: BTreeSet<u64>) -> Result<(), String> {
    // Boot is the lock's only holder: no task of the executor exists yet.
    let alone = disp.apply_lock().await;
    // Rebuild the views the boot verdict rejected, through the driver a live
    // CREATE VIEW uses; they opened empty. Ahead of the tail's tick: a backfill
    // reads its sources at their cut, which a replayed push landed above.
    let invalid: Vec<u64> = std::mem::take(&mut disp.cat_mut().boot.rebuild).into_iter().collect();
    // Resume-vs-rebuild marker (asserted by the "no backfill on clean restart"
    // E2E): 0 ⇒ every view resumed from its checkpoint.
    gnitz_note!("recovery: rebuilding {} invalid view(s)", invalid.len());
    disp.backfill_views_in_dep_order(&alone, &invalid)
        .await
        .map_err(|e| format!("invalid-view rebuild failed: {e}"))?;

    inject_recovery_panic("backfill");

    // The replayed tail, above the cut of every scanned base store it pushed to:
    // one tick brings it to every view, resumed or rebuilt, each row once. A
    // view's operator state needs no tick: the boot checkpoint opens every
    // resumed view's.
    {
        let laid_out = disp.sal().lock().await;
        for tid in pushed {
            disp.credit(&laid_out, tid, 0);
        }
    }
    disp.tick(&alone)
        .await
        .map_err(|e| format!("recovery tick of the replayed tail failed: {e}"))?;

    inject_recovery_panic("sweep");

    // A full checkpoint before the socket opens, so a clean restart resumes from
    // it. No drain — recovery already drained everything and no pushes are
    // admitted yet.
    disp.checkpoint_ephemeral(&alone)
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
    listen::clear_published(data_dir)?;
    // Ahead of recovery, so an identity that cannot be read or minted refuses
    // the boot before it costs one.
    let tls = tls.map(|args| TlsListener::open(args, data_dir)).transpose()?;

    gnitz_note!("Starting {num_workers} workers");
    // The template of `worker_log`.
    gnitz_note!("Worker logs: {}/worker_N.log (N=0..{})", data_dir, num_workers - 1);

    // The committed tail every recovery replays, and the epoch this boot writes
    // at. Read once, before the fork.
    let mut tail = CommittedTail::open(data_dir)?;
    // The workers' parks, one W2M ring per worker, and the exchange mesh. None of
    // it is reclaimed on error: a failed boot exits the process, which returns
    // all of it to the kernel.
    let parks = WorkerParks::create(nw).map_err(|e| format!("failed to map the worker parks: {e}"))?;
    let (writers, receiver) = w2m::create(parks).map_err(|e| format!("failed to map the W2M rings: {e}"))?;
    let meshes =
        mesh::create(mesh::outbox_bytes(), parks).map_err(|e| format!("failed to map the exchange mesh: {e}"))?;
    let workers: Vec<(W2mWriter, Mesh)> = writers.into_iter().zip(meshes).collect();

    let mut catalog = master_pre_fork_recovery(data_dir, num_workers, StoreConfig::from_env("GNITZ_"), &mut tail)?;

    let workers = fork_workers(&mut catalog, data_dir, &tail, workers, pinning.as_ref())?;

    // --- Parent process ---
    let reactor = Reactor::new(limits, receiver).map_err(|e| format!("io_uring init failed: {e}"))?;
    // One watch, raced against both phases below: a worker's death would leave
    // a wait of the recovery, or every request a worker answers, waiting
    // forever. `workers` drops at the return, which kills them: after the final
    // checkpoint, a death, or a failed boot.
    let mut death = std::pin::pin!(workers.death(reactor));
    let recovered = async {
        let pushed = tail.pushed().clone();
        let sal = SalWriter::boot(tail, parks, reactor).await;
        inject_recovery_panic("reset");
        let dispatcher = MasterDispatcher::new(catalog, sal, reactor);
        master_post_fork_recovery(&dispatcher, pushed).await?;
        Ok::<_, String>(dispatcher)
    };
    let dispatcher = match reactor.block_on(select2(recovered, death.as_mut())) {
        Either::A(recovered) => recovered?,
        Either::B((w, end)) => {
            return Err(format!(
                "worker {w} {end} during recovery (log: {})",
                worker_log(data_dir, w)
            ));
        }
    };
    // Outside the loop: the bind may sleep on an address still in use.
    let listeners = listen::bind_listeners(data_dir, &args.socket_path, args.socket_mode, tls)?;
    match reactor.block_on(select2(executor::serve(dispatcher, args.backup_dir, listeners), death)) {
        Either::A(()) => Ok(ServerExit::Clean),
        Either::B((w, end)) => {
            gnitz_error!("Worker {w} {end} (log: {}); workers stopped", worker_log(data_dir, w));
            Ok(ServerExit::WorkerDied)
        }
    }
}

#[cfg(test)]
#[path = "tests/bootstrap.rs"]
mod tests;
