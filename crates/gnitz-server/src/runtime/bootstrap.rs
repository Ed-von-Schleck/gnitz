//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.
//!
//! **Recovery order is a crash guard.** Each step in `run_server` is placed so a
//! crash at any point rebuilds a view rather than silently resuming a stale one,
//! and the monotonic checkpoint generation is what carries that. The bumps that
//! do the carrying are commented inline, next to the code that would falsify
//! them.

use std::collections::{HashMap, HashSet};

use crate::catalog::CatalogEngine;
use gnitz_store::foundation::fault::Seam;
use gnitz_store::foundation::posix_io;

use crate::runtime::affinity;
use crate::runtime::executor::ServerExecutor;
use crate::runtime::m2w;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::sal::zone::CommittedTail;
use crate::runtime::sal::{sal_mmap_size, EpochGate, SalLog, SalMessageKind, SalReader, SalStep, SalWriter};
use crate::runtime::tls::{setup_tls_listener, TlsCli};
use crate::runtime::w2m::{self, W2mReceiver, W2mWriter};
use crate::runtime::wire as ipc;
use crate::runtime::worker::{buffer_pending_delta, WorkerProcess};
use gnitz_store::storage::Batch;

/// One boot-progress line, raw and untagged, on stderr — the master's own, or
/// `<data_dir>/worker_N.log` in a forked child.
///
/// Use it for a line that must appear whatever the log level; `gnitz_info!` is
/// silent at the default QUIET. stderr rather than stdout because the harnesses
/// discard the server's stdout and keep its stderr for post-mortem.
fn boot_log(msg: &str) {
    let _ = posix_io::write_all_fd(2, msg.as_bytes());
}

// ---------------------------------------------------------------------------
// SAL recovery: both drivers below read the log through `sal::zone::CommittedTail`
// and differ only in which groups are theirs and what they do with the bytes.
// ---------------------------------------------------------------------------

/// Master pre-fork system-table replay. Builds the system-table family map from
/// the flushed LSNs, then ingests every committed DdlSync batch addressed
/// to a system table — orphan COL_TAB rows from a crashed DDL are skipped
/// because their zone never closed.
///
/// Returns the walk's epoch, which is the floor the next writer epoch and the
/// workers' initial `expected_epoch` are taken from.
fn recover_system_tables_from_sal(
    sal_ptr: *const u8,
    catalog: &mut CatalogEngine,
) -> Result<(u32, Option<u32>), String> {
    let log = unsafe { SalLog::new(sal_ptr, sal_mmap_size()) };
    let family_lsns = catalog.registry().system_flushed_lsns();

    // Derived once, before either pass: on a boot whose offset-0 header is
    // damaged this costs a full-ring sweep, and both passes must anchor on the
    // same answer anyway.
    let epoch = log.walk_epoch();
    let tail = CommittedTail::open(log, epoch, SalMessageKind::DdlSync, &family_lsns)?;

    let mut replayed: u32 = 0;
    for msg in tail.groups() {
        // A system family broadcasts, so slot 0 carries the whole batch.
        let Some(data) = msg.slot(0) else { continue };
        // Pass 1 already demoted the last zone if it was torn, so anything that
        // fails here has a durable committed zone behind it — a hole, not a crash
        // artifact. Dropping it silently would lose an ACKed, fdatasync'd DDL.
        let decoded = ipc::decode_wire(data).map_err(|e| {
            format!(
                "SAL replay: corrupt block at offset={} lsn={} target={}: {e}",
                msg.base, msg.lsn, msg.target_id
            )
        })?;
        let batch = match decoded.data_batch {
            Some(b) if b.count > 0 => b,
            _ => continue,
        };
        // `ddl_sync`, not `ingest_to_family`: these rows are master-validated by
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
        boot_log(&format!("SAL system table recovery: replayed {replayed} entries\n"));
    }
    // The tail's slot count comes back with the epoch because this walk has
    // already read the first group's header; the boot log names it, and no
    // separate probe re-reads it.
    let tail_slots = match log.read_at(0, EpochGate::Walk(epoch)) {
        SalStep::Group(msg, _) => Some(msg.slots()),
        _ => None,
    };
    Ok((epoch, tail_slots))
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

/// Base tables feeding ≥1 view, sorted for a reproducible drive order. The ONE
/// definition of the recovery sweep set: each worker buffers exactly these
/// tables' effective deltas during SAL replay, and the master's post-reset tick
/// sweep drains exactly these — same function, so the swept set equals the
/// buffered set by construction (no leak, no gap).
fn swept_base_tables(catalog: &mut CatalogEngine) -> Vec<i64> {
    let view_ids = catalog.registry().view_ids();
    let (dag, registry) = catalog.dag_and_registry_mut();
    dag.base_tables_reachable_from(registry, view_ids)
}

/// Per-worker post-fork user-table replay for `rank` of `num_workers`. The apply
/// closure decodes each Push group's batch and applies it through the
/// PK-enforcement path (`ingest_returning_effective`, the exact call
/// `handle_push` makes) so retractions cancel correctly, and — for every base
/// table feeding ≥1
/// view — buffers the returned effective delta into the returned map. That map
/// seeds the worker's `pending_deltas`; the master's post-reset recovery tick
/// sweep drains it into the views. Viewless bases ingest-and-discard (nothing to
/// drive), so their tail never leaks into the sweep.
///
/// Storeless relations are absent from `family_lsns`, so a stream group the
/// committer coalesced into a base table's commit batch is neither validated by
/// pass 1 nor applied by pass 2 — nothing a stream ingested is ever recovered,
/// and a torn one cannot cost the base push beside it its zone.
///
/// The tail is written pre-sliced, one slot per worker of the boot that wrote it.
/// At the launched count this rank's own slot already holds exactly its rows; at
/// any other count it holds neither all of them nor only them, so every written
/// slot is walked and each partitioned group re-cut for the launched topology.
fn recover_from_sal(
    sal_ptr: *const u8,
    rank: u32,
    num_workers: u32,
    walk_epoch: u32,
    catalog: &mut CatalogEngine,
) -> Result<HashMap<i64, Batch>, String> {
    let family_lsns = catalog.registry().user_flushed_lsns();

    let buffered_bases: HashSet<i64> = swept_base_tables(catalog).into_iter().collect();

    let log = unsafe { SalLog::new(sal_ptr, sal_mmap_size()) };
    let tail = CommittedTail::open(log, walk_epoch, SalMessageKind::Push, &family_lsns)?;

    let mut pending: HashMap<i64, Batch> = HashMap::new();
    let mut replayed: u32 = 0;
    for msg in tail.groups() {
        // Each group's own slot count, off the header in hand and inside its
        // digest. A tail-wide probe reads one header for all of them, and a torn
        // one picks the wrong mode — losing every slot above `rank`.
        let reslice = msg.slots() != num_workers;
        let wanted = if reslice { 0..msg.slots() } else { rank..rank + 1 };
        let tid = msg.target_id as i64;
        // The catalog's schema, not the wire's: only the catalog stamps the
        // `replicated` bit the branch below reads. `SchemaDescriptor` is `Copy`, so
        // this holds no borrow on `catalog` across the `&mut` ingest.
        let schema = catalog
            .registry()
            .get_schema_desc(tid)
            .ok_or_else(|| format!("SAL replay: no schema for table_id={tid} (lsn={})", msg.lsn))?;
        // Broadcast, not sliced: every slot holds the whole copy, so a second slot
        // would re-ingest the same rows and add their weights again. Never re-sliced
        // either — this worker needs the full copy, not a share of it.
        let replicated = schema.placement().is_replicated();
        for (w, data) in msg.slots_written().filter(|(w, _)| wanted.contains(w)) {
            // Keyed on the range, not on the first slot the iterator happens to
            // yield, so a replicated group with an unwritten slot 0 cannot make
            // this read a second copy.
            if replicated && w != wanted.start {
                continue;
            }
            // Pass 1 demoted the last zone if it was torn, so a failure here has a
            // durable committed zone behind it — a hole, not a crash artifact.
            // Dropping it silently would lose an ACKed, fdatasync'd transaction.
            let decoded = ipc::decode_wire(data).map_err(|e| {
                format!(
                    "SAL replay: corrupt block at offset={} lsn={} target={}: {e}",
                    msg.base, msg.lsn, msg.target_id
                )
            })?;
            let mut batch = match decoded.data_batch {
                Some(b) if b.count > 0 => b,
                _ => continue,
            };
            // The one place an old-width batch enters the engine. A pre-ALTER
            // `Push` frame decodes against its own embedded schema block, but the
            // catalog is already at its final width here (the master applied every
            // catalog SAL entry pre-fork, and workers replay pushes only). Widen ahead
            // of the reslice, which rebuilds through `Batch::from_indexed_rows(&mb, …,
            // &schema)` and would read `schema`'s payload columns past the narrower
            // source's regions; the non-reslice arm's `enforce_unique_pk` accumulator
            // is already shaped by the table schema. One site covers both.
            let in_schema = decoded
                .schema
                .as_ref()
                .ok_or_else(|| format!("SAL replay: push frame carries no schema (lsn={})", msg.lsn))?;
            if in_schema.num_payload_cols() < schema.num_payload_cols() {
                batch = batch.widened_with_null_tail(in_schema, &schema);
            }
            let owned = if reslice && !replicated {
                // Re-cut with the write path's own router, so what survives is exactly
                // what the master would have written to this rank's slot: same
                // distribution-prefix hash, same key→worker map.
                let mb = batch.as_mem_batch();
                crate::runtime::master::scatter::with_worker_indices(&batch, &schema, num_workers as usize, |wi| {
                    Batch::from_indexed_rows(&mb, &wi[rank as usize], &schema)
                })
            } else {
                batch
            };
            if owned.count == 0 {
                continue;
            }
            // The error rides the startup ACK: the master fails boot BEFORE zeroing
            // the SAL is rewound, so the replayed data's only durable copy survives.
            // A swallowed error here would let the rewind orphan the un-applied
            // committed data.
            let effective = catalog
                .registry_mut()
                .ingest_returning_effective(tid, owned)
                .map_err(|e| {
                    format!(
                        "SAL replay apply failed (table_id={}, lsn={}): {e}",
                        msg.target_id, msg.lsn
                    )
                })?;
            // Buffer the effective delta for the sweep; viewless bases discard it
            // (nothing to drive).
            if buffered_bases.contains(&tid) {
                buffer_pending_delta(&mut pending, tid, effective);
            }
            replayed += 1;
        }
    }

    if replayed > 0 {
        boot_log(&format!("SAL recovery: replayed {replayed} blocks\n"));
    }
    Ok(pending)
}

/// Worker-boot catalog recovery. The order below is required:
///
/// 1. Re-home every secondary index slice-local, replacing the fork-inherited
///    full parent-dir copy, and rebuild the ones that did not resume from their
///    checkpoint — BEFORE SAL replay, which projects the committed unflushed tail
///    into each index exactly once (`ingest_store_and_indices`), so a rebuild
///    afterwards would double-count every replayed row.
/// 2. Replay unflushed push data from the SAL.
/// 3. Flush the replayed rows to shards before accepting requests: the boot rewind
///    puts the write cursor back to 0, so a second crash before a checkpoint would
///    overwrite SAL entries and make replayed data unreachable (the SAL walk
///    stops at the first partially-overwritten group).
///
/// The Err rides the startup ACK (see worker.run): a failed boot must abort
/// before the master rewinds the SAL, or the replayed rows' only durable copy is
/// destroyed.
fn worker_boot_recovery(
    catalog: &mut CatalogEngine,
    sal_ptr: *const u8,
    rank: u32,
    num_workers: u32,
    walk_epoch: u32,
) -> Result<HashMap<i64, Batch>, String> {
    let rebuilt = catalog
        .backfill_all_indexes()
        .map_err(|e| format!("boot index backfill failed: {e}"))?;
    // Resume-vs-rebuild marker, the index sibling of the invalid-view line: 0 ⇒
    // every index resumed from its checkpoint.
    boot_log(&format!("recovery: rebuilding {rebuilt} index(es)\n"));
    let pending_deltas = recover_from_sal(sal_ptr, rank, num_workers, walk_epoch, catalog)?;
    // Keep the boot flush: the non-windowed recovery resets the SAL before the
    // master-driven tick sweep, so the replayed base rows must be shard-durable
    // first — else the reset would drop acknowledged tail data. The recovery-start
    // bump is what lets it publish: it is durable pre-fork, so nothing on disk
    // resumes across it.
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
    inject_recovery_panic("bootflush");
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
pub fn server_main(
    data_dir: &str,
    socket_path: &str,
    num_workers: u32,
    log_level: u32,
    tls_cli: Option<TlsCli>,
) -> i32 {
    match run_server(data_dir, socket_path, num_workers, log_level, tls_cli) {
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
    w2m_ptrs: Vec<*mut u8>,
    m2w_efds: Vec<i32>,
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
    let sal_ptr = posix_io::map_file_reserved(sal_fd, sal_mmap_size())
        .map_err(|e| format!("failed to map SAL ({} bytes): {e}", sal_mmap_size()))?;

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
        w2m_ptrs,
        m2w_efds,
    })
}

/// The forked child's whole life: latch its rank, redirect its logs to
/// `worker_N.log`, recover, and run the worker
/// loop. Never returns — the worker exits via `libc::_exit`.
#[allow(clippy::too_many_arguments)]
fn run_worker_child(
    w: usize,
    num_workers: u32,
    log_level: u32,
    data_dir: &str,
    master_pid: i32,
    catalog_ptr: *mut CatalogEngine,
    ipc: &SharedIpc,
    walk_epoch: u32,
    live_epoch: u32,
    placement: Option<&affinity::Placement>,
) -> ! {
    // Die immediately if the master exits for any reason. The `getppid` probe a
    // worker runs after a timed-out `SalReader::wait` is a belt-and-suspenders
    // fallback; this closes the ~30s polling gap.
    unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) };
    // Re-check: parent may have died in the fork→prctl window.
    if unsafe { libc::getppid() } != master_pid {
        unsafe { libc::_exit(0) };
    }

    // Latch this worker's rank/count (and its Worker role) before ANY catalog
    // work below (trim, rehome, index rebuild, SAL replay, view backfill): every
    // plan compiled during boot must see this process's real (rank,
    // num_workers) and its index tables must home into the per-rank subdir.
    // Single owner of the rank — no longer set in WorkerProcess::new.
    gnitz_store::foundation::worker_ctx::set_worker_identity(w as u32, num_workers);
    gnitz_store::foundation::worker_ctx::set_worker_role();

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
    gnitz_store::foundation::log::init(log_level, format!("W{w}").as_bytes());

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

    let catalog = unsafe { &mut *catalog_ptr };

    let sal_reader = unsafe {
        SalReader::new(
            ipc.sal_ptr as *const u8,
            w as u32,
            sal_mmap_size(),
            ipc.m2w_efds[w],
            live_epoch,
        )
    };
    let w2m_writer = W2mWriter::new(ipc.w2m_ptrs[w]);

    // Re-home every inherited store from the pre-fork master's `w0of{W}` to THIS
    // worker's own `w{w}of{W}` dir before any flush — all workers share the data
    // directory, so a fixed rank 0 would collide. The inherited store is empty;
    // this rank's checkpointed shards under `w{w}of{W}` load on open and Push
    // replay adds the SAL tail.
    //
    // Then recover: rebuild indexes, replay the SAL tail (buffering effective base
    // deltas), boot-flush the replayed rows durable. The buffered deltas seed
    // `pending_deltas`; the master's post-reset tick sweep drives them into the
    // views. All view derivation moved to the master's sweep + step-4 rebuild — no
    // child-side view backfill.
    //
    // Either failure rides the startup ACK, which fails boot before the master
    // rewinds the SAL.
    let (pending_deltas, boot_err): (HashMap<i64, Batch>, Option<String>) = match catalog
        .registry_mut()
        .rehome_stores()
        .map_err(|e| format!("rehome stores failed: {e}"))
        .and_then(|()| worker_boot_recovery(catalog, ipc.sal_ptr as *const u8, w as u32, num_workers, walk_epoch))
    {
        Ok(pd) => (pd, None),
        Err(e) => {
            // stderr is redirected to worker_N.log above.
            gnitz_error!("{e}");
            (HashMap::new(), Some(e))
        }
    };

    catalog.dag_mut().invalidate_all();

    boot_log(&format!(
        "Worker {} (pid {}) of {}\n",
        w,
        unsafe { libc::getpid() },
        num_workers
    ));

    let mut worker = WorkerProcess::new(master_pid, catalog_ptr, sal_reader, w2m_writer, pending_deltas);
    let rc = worker.run(boot_err);

    unsafe {
        libc::_exit(rc);
    }
}

fn run_server(
    data_dir: &str,
    socket_path: &str,
    num_workers: u32,
    log_level: u32,
    tls_cli: Option<TlsCli>,
) -> Result<i32, String> {
    // Latch the Master role before any catalog work: the pre-fork replay hooks
    // in CatalogEngine::open must see Master so they skip the index backfill
    // their forked children rebuild slice-local.
    gnitz_store::foundation::worker_ctx::set_master_role();

    // Raise fd limit (child directories + shard files)
    raise_fd_limit(65536);

    // Pin the master before `CatalogEngine::open`: the boot-time system-table
    // flush below creates the master's first io_uring ring, and an io-wq pool
    // keeps the mask its ring's creating thread held. Each forked child then
    // inherits this mask until its own pin narrows it. See `runtime::affinity`
    // for the placement itself and what it assumes about the host.
    let nw = num_workers as usize;
    let placement = match affinity::plan(nw) {
        Ok(p) => {
            boot_log(&format!("affinity: {}\n", p.describe()));
            p.pin_master();
            Some(p)
        }
        Err(why) => {
            boot_log(&format!("affinity: not applied ({why})\n"));
            None
        }
    };

    gnitz_info!("Opening database at {}", data_dir);

    let mut catalog = CatalogEngine::open(data_dir, num_workers).map_err(|e| format!("failed to open catalog: {e}"))?;

    boot_log(&format!("Starting {num_workers} workers\n"));
    boot_log(&format!(
        "Worker logs: {}/worker_N.log (N=0..{})\n",
        data_dir,
        num_workers - 1
    ));

    let ipc = acquire_shared_ipc(data_dir, nw)?;

    // --- System table SAL recovery (before forking workers) ---
    //
    // The recovered walk epoch is this boot's floor, so a previous boot's
    // leftover always carries a strictly lower one than anything written now.
    let (walk_epoch, tail_slots) = recover_system_tables_from_sal(ipc.sal_ptr as *const u8, &mut catalog)?;
    // One above it: what the master rewinds to and what every worker's live drain
    // accepts. Bound once — were the two ever to disagree, every group would park
    // as another epoch, the drain would end silently, and the committer would wait
    // forever on an ACK.
    let live_epoch = walk_epoch + 1;
    {
        // Abort before forking workers and long before the SAL reset: the
        // replayed DDL lives only in master memory until this flush makes it
        // durable, so a swallowed failure followed by the SAL reset destroys
        // its only durable copy (and gc_orphan_directories would later delete
        // the now-catalog-less entities' flushed shards).
        catalog.flush_all_system_tables()?;
        if SYS_FLUSH_ERROR.armed() {
            return Err("injected system table flush fault".to_string());
        }

        // Reclaim table/view/index directories whose DROP committed but whose
        // deferred deletion was lost to a crash before the next checkpoint.
        // Runs only now that both shard replay and SAL replay have populated
        // the registry, so a SAL-committed-but-unflushed CREATE is not mistaken
        // for an orphan.
        catalog.gc_orphan_directories();

        // Drop the child directories this boot's worker count no longer owns.
        // Must run after SAL replay (so the registry is complete and dropped
        // subtrees are already gone) and before the fork, since each worker's
        // `rehome_stores` then opens what this leaves behind.
        catalog.registry().reconcile_child_dirs();
    }

    // --- Boot invalid-view verdict + recovery-start generation bump ---
    //
    // Both pre-fork: the verdict peeks every launched rank's manifest on behalf
    // of workers that do not exist yet, and the durable generation advance is
    // COW-inherited by every worker (and is durable long before the parent resets
    // the SAL after worker readiness).
    {
        // Per-view resume-vs-rebuild verdict against the checkpointed manifests
        // (generation + topology + transitive source validity). Reads the
        // catalog's `resume_generation` (the recovered G), so it runs BEFORE the
        // recovery-start bump advances the durable generation.
        catalog.compute_invalid_views();

        // Durably advance the checkpoint generation G → G+1 while the resume
        // generation stays at G, closing the reset→boot_checkpoint crash window.
        catalog
            .recovery_start_generation_bump()
            .map_err(|e| format!("recovery-start generation bump failed: {e}"))?;
        inject_recovery_panic("genbump");
    }

    // Name the replay path the workers will take, so the boot record shows
    // whether the tail was re-sliced. Pre-fork, so this still reaches the
    // master's own log (the children redirect fd 2 to their own).
    if let Some(written) = tail_slots.filter(|&n| n != num_workers) {
        boot_log(&format!(
            "SAL tail written by {written} workers, launching {num_workers}\n"
        ));
    }

    // Log fd assignments
    boot_log(&format!("SAL fd={}\n", ipc.sal_fd));
    for w in 0..nw {
        boot_log(&format!("W{} m2w_efd={}\n", w, ipc.m2w_efds[w]));
    }

    let master_pid = unsafe { libc::getpid() };

    // The catalog becomes a raw pointer only here, immediately above the fork:
    // `run_worker_child` and `MasterDispatcher` are what need one, and everything
    // above took `&mut`. `Borrowed(*mut Table)` handles point into `sys_stores`'
    // boxed `Table`s, whose addresses this move does not disturb.
    let catalog_ptr = Box::into_raw(Box::new(catalog));

    // --- Fork workers ---
    let mut worker_pids: Vec<i32> = Vec::with_capacity(nw);
    for w in 0..nw {
        match unsafe { libc::fork() } {
            -1 => return Err("fork failed".to_string()),
            0 => run_worker_child(
                w,
                num_workers,
                log_level,
                data_dir,
                master_pid,
                catalog_ptr,
                &ipc,
                walk_epoch,
                live_epoch,
                placement.as_ref(),
            ),
            pid => worker_pids.push(pid),
        }
    }

    // --- Parent process ---
    let SharedIpc {
        sal_fd,
        sal_ptr,
        w2m_ptrs,
        m2w_efds,
    } = ipc;

    let sal_writer = SalWriter::new(sal_ptr, sal_fd, sal_mmap_size() as u64, nw);
    let w2m_receiver = std::rc::Rc::new(W2mReceiver::new(w2m_ptrs));

    // `recovery_start_generation_bump` has already run, so this is the floor
    // every base round must publish past.
    let boot_generation = unsafe { (*catalog_ptr).durable_generation() };
    let dispatcher = MasterDispatcher::new(
        worker_pids,
        catalog_ptr,
        boot_generation,
        sal_writer,
        w2m_receiver,
        m2w_efds,
    );
    // Before the workers get anywhere: each is re-homing the very stores this
    // process inherited handles to, and two live `Table`s on one directory is the
    // hazard. Only constructors ran between the fork and here.
    dispatcher.cat().registry_mut().detach_user_stores();
    let dispatcher_rc = std::rc::Rc::new(dispatcher);

    // Wait for all workers to complete recovery and signal readiness
    let dispatcher = &*dispatcher_rc;
    dispatcher
        .collect_acks("recovery sync")
        .map_err(|e| format!("Error collecting worker acks: {e}"))?;

    // Reset the SAL for fresh use, now that every worker has recovered. The same
    // `live_epoch` the workers were launched with.
    dispatcher.rewind_sal(live_epoch);

    inject_recovery_panic("reset");

    // Recovery tick sweep: drive the tail each worker buffered during replay into
    // the views, one blocking tick per reachable base on the freshly-reset SAL.
    // Empty sources tick too, so exchange views stay in lockstep. Resumed views are
    // extended state-exactly; invalid ones are polluted and rebuilt below.
    for src in swept_base_tables(dispatcher.cat()) {
        dispatcher
            .drain_tick_blocking(src)
            .map_err(|e| format!("recovery tick sweep failed: {e}"))?;
    }

    inject_recovery_panic("sweep");

    // Step-4: rebuild only the views the boot verdict rejected, through the driver
    // a live CREATE VIEW uses. It is view-scoped, so a resumed sibling's loaded
    // shards are never re-derived and double-counted.
    let invalid: Vec<i64> = dispatcher.cat().invalid_views().collect();
    // Resume-vs-rebuild marker (asserted by the "no backfill on clean restart"
    // E2E): 0 ⇒ every view resumed from its checkpoint.
    boot_log(&format!("recovery: rebuilding {} invalid view(s)\n", invalid.len()));
    dispatcher
        .backfill_views_in_dep_order(&invalid)
        .map_err(|e| format!("invalid-view rebuild failed: {e}"))?;

    inject_recovery_panic("backfill");

    // Boot-end checkpoint: record the launched topology, bump the generation
    // (G+1 → G+2), and durably checkpoint the resumed + rebuilt view state (base +
    // ephemeral rounds) before the socket opens, so a clean restart resumes from
    // it. No drain — recovery already drained everything and no pushes are admitted
    // yet.
    dispatcher
        .boot_checkpoint(num_workers)
        .map_err(|e| format!("boot checkpoint failed: {e}"))?;

    // Create server socket and run executor
    gnitz_info!("Listening on {}", socket_path);
    // `UnixListener::bind` does not unlink; a socket left by an earlier run
    // would be EADDRINUSE. Its backlog is `net.core.somaxconn` (std passes -1).
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
    boot_log("GnitzDB ready\n");

    Ok(ServerExecutor::run(
        std::rc::Rc::clone(&dispatcher_rc),
        server_fd,
        tls_init,
    ))
}

/// Raise the `RLIMIT_NOFILE` soft limit towards `target`, capped by the hard
/// limit. Best-effort: the engine opens far fewer descriptors than `target` on
/// a small database, so a refusal only matters once the partition count grows,
/// and then it surfaces as `EMFILE` at the open that could not be served.
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

#[cfg(test)]
#[path = "tests/bootstrap.rs"]
mod tests;
