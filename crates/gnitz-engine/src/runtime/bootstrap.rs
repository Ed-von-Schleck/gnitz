//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.

use std::collections::{HashMap, HashSet};

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::foundation::fault::Seam;
use crate::foundation::posix_io;
use crate::runtime::executor::{ServerExecutor, TlsListener};
use crate::runtime::master::MasterDispatcher;
use crate::runtime::sal::{
    sal_mmap_size, sal_tail_slot_count, SalReader, SalWriter, FLAG_DDL_SYNC, FLAG_PUSH, FLAG_TXN_COMMIT,
};
use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::w2m_ring::{self, W2M_REGION_SIZE};
use crate::runtime::wire as ipc;
use crate::runtime::worker::{buffer_pending_delta, WorkerProcess};
use crate::storage::{partition_range, Batch};

/// Boot-progress line on stdout (the server log). Used instead of the log
/// macros where the raw, untagged line is the documented boot output (e.g. the
/// "GnitzDB ready" marker tests wait for).
fn boot_log(msg: &str) {
    let _ = posix_io::write_all_fd(1, msg.as_bytes());
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

/// Walk the SAL from offset 0, stopping at the first unreadable slot or at the
/// first group whose epoch went backwards — the wrap fence, which marks where a
/// previous epoch's leftovers begin. Quiescent-SAL only: passing `None` as the
/// expected epoch skips the gate that guards against a concurrent writer.
fn sal_groups(sal_reader: &SalReader) -> impl Iterator<Item = crate::runtime::sal::SalMessage<'static>> + '_ {
    let mut offset: u64 = 0;
    let mut last_epoch: u32 = 0;
    std::iter::from_fn(move || {
        if offset + 8 >= sal_reader.mmap_size() {
            return None;
        }
        let (msg, new_offset) = sal_reader.try_read(offset, None)?;
        if last_epoch > 0 && msg.epoch < last_epoch {
            return None;
        }
        last_epoch = msg.epoch;
        offset = new_offset;
        Some(msg)
    })
}

/// Pass 1: every LSN whose commit sentinel is on disk.
fn collect_committed_lsns(sal_reader: &SalReader) -> HashSet<u64> {
    sal_groups(sal_reader)
        .filter(|m| m.flags & FLAG_TXN_COMMIT != 0)
        .map(|m| m.lsn)
        .collect()
}

/// Pass 2: walk the SAL applying every committed group whose LSN is
/// in `family_lsns` and exceeds the recorded flushed LSN. The closure
/// receives the group's raw wire bytes and may filter by flag (e.g. master
/// applies only FLAG_DDL_SYNC, worker only FLAG_PUSH). It decodes what it
/// keeps: `ipc::decode_wire` copies the whole batch out of the SAL, so decoding
/// ahead of the flag test would pay for every group the caller drops.
fn recover_sal<F>(
    sal_reader: &SalReader,
    catalog: &mut CatalogEngine,
    committed: &HashSet<u64>,
    family_lsns: &HashMap<i64, u64>,
    mut apply: F,
) -> Result<u32, String>
where
    F: FnMut(&mut CatalogEngine, &crate::runtime::sal::SalMessage, &[u8]) -> Result<bool, String>,
{
    let mut applied: u32 = 0;
    for msg in sal_groups(sal_reader) {
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
        if apply(catalog, &msg, data)? {
            applied += 1;
        }
    }
    Ok(applied)
}

/// Master pre-fork system-table replay. Builds the system-table family
/// map from the flushed LSNs, then walks the SAL via
/// `recover_sal`. The closure ingests every committed FLAG_DDL_SYNC
/// batch addressed to a system table — orphan COL_TAB rows from a
/// crashed DDL are skipped because their zone never closed.
fn recover_system_tables_from_sal(sal_ptr: *const u8, catalog: &mut CatalogEngine) -> Result<(), String> {
    let sal_reader = SalReader::new(sal_ptr, 0, sal_mmap_size(), -1);
    let all_lsns = catalog.collect_all_flushed_lsns();
    let family_lsns: HashMap<i64, u64> = all_lsns
        .into_iter()
        .filter(|&(tid, _)| tid > 0 && tid < FIRST_USER_TABLE_ID)
        .collect();

    let committed = collect_committed_lsns(&sal_reader);
    let replayed = recover_sal(&sal_reader, catalog, &committed, &family_lsns, |cat, msg, data| {
        if msg.flags & FLAG_DDL_SYNC == 0 {
            return Ok(false);
        }
        let batch = match ipc::decode_wire(data).ok().and_then(|d| d.data_batch) {
            Some(b) if b.count > 0 => b,
            _ => return Ok(false),
        };
        // §3.2: route through `ddl_sync` (→ `apply_local`), NOT `ingest_to_family`
        // (→ `submit` → `precheck_family`). These rows are master-validated by
        // definition, and the Column precheck arm would false-reject a replayed
        // DROP TABLE cascade's independent COL `-1` groups (unpaired on a
        // still-registered owner) — the swallowed `.is_ok()` would then silently
        // orphan the column band. `apply_local` still fires hooks, so a DROP NOT
        // NULL pair in the un-checkpointed SAL tail actually swaps the base
        // FixedIntNonnull → Generic here, before the post-ALTER NULL pushes
        // replay. A replay error aborts boot, before the SAL sentinel is reset: a
        // silently-swallowed swap failure would leave the base under the stale
        // comparator → NULL-vs-0 corruption. Idempotent on a re-run (cascade
        // `count>0` guard + net-dead register gate make a re-scan of an
        // already-retracted band a no-op, not an Err), so failing here never aborts
        // a legitimate boot.
        cat.ddl_sync(msg.target_id as i64, batch).map(|()| true).map_err(|e| {
            format!(
                "SAL system-table recovery apply failed (table_id={}, lsn={}): {e}",
                msg.target_id, msg.lsn
            )
        })
    })?;

    if replayed > 0 {
        boot_log(&format!("SAL system table recovery: replayed {replayed} entries\n"));
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

/// Base tables feeding ≥1 view, sorted for a reproducible drive order. The ONE
/// definition of the recovery sweep set: each worker buffers exactly these
/// tables' effective deltas during SAL replay, and the master's post-reset tick
/// sweep drains exactly these — same function, so the swept set equals the
/// buffered set by construction (no leak, no gap).
fn swept_base_tables(catalog: &mut CatalogEngine) -> Vec<i64> {
    let view_ids = catalog.dag.view_ids();
    catalog.dag.base_tables_reachable_from(view_ids)
}

/// Per-worker post-fork user-table replay for `rank` of `num_workers`. The apply
/// closure decodes each FLAG_PUSH group's batch and applies it through the
/// PK-enforcement path (`ingest_returning_effective`, the exact call
/// `handle_push` makes) so retractions cancel correctly, and — for every base
/// table feeding ≥1
/// view — buffers the returned effective delta into the returned map. That map
/// seeds the worker's `pending_deltas`; the master's post-reset recovery tick
/// sweep drains it into the views. Viewless bases ingest-and-discard (nothing to
/// drive), so their tail never leaks into the sweep.
///
/// The tail is written pre-sliced, one slot per worker of the boot that wrote it.
/// At the launched count this rank's own slot already holds exactly its rows; at
/// any other count it holds neither all of them nor only them, so every written
/// slot is walked and each partitioned group re-cut for the launched topology.
fn recover_from_sal(
    sal_ptr: *const u8,
    rank: u32,
    num_workers: u32,
    catalog: &mut CatalogEngine,
) -> Result<HashMap<i64, Batch>, String> {
    let all_lsns = catalog.collect_all_flushed_lsns();
    let family_lsns: HashMap<i64, u64> = all_lsns
        .into_iter()
        .filter(|&(tid, _)| tid >= FIRST_USER_TABLE_ID)
        .collect();

    let buffered_bases: HashSet<i64> = swept_base_tables(catalog).into_iter().collect();

    let written = unsafe { sal_tail_slot_count(sal_ptr) }.unwrap_or(num_workers);
    let reslice = written != num_workers;
    let slots = if reslice { 0..written } else { rank..rank + 1 };

    // `-1` for the eventfd: a recovery walk never waits on one.
    let readers: Vec<SalReader> = slots.map(|s| SalReader::new(sal_ptr, s, sal_mmap_size(), -1)).collect();
    // The committed-LSN set is read from the group headers, which are shared by
    // every slot, so it is the same set for each reader.
    let committed = collect_committed_lsns(&readers[0]);

    let mut pending: HashMap<i64, Batch> = HashMap::new();
    let mut replayed: u32 = 0;
    for (idx, sal_reader) in readers.iter().enumerate() {
        replayed += recover_sal(sal_reader, catalog, &committed, &family_lsns, |cat, msg, data| {
            if msg.flags & FLAG_PUSH == 0 {
                return Ok(false);
            }
            let tid = msg.target_id as i64;
            // The catalog's schema, not the wire's: only the catalog stamps the
            // `replicated` bit the branch below reads. `SchemaDescriptor` is `Copy`,
            // so this holds no borrow on `cat` across the `&mut cat` ingest.
            let schema = cat
                .get_schema_desc(tid)
                .ok_or_else(|| format!("SAL replay: no schema for table_id={tid} (lsn={})", msg.lsn))?;
            // Broadcast, not sliced: every slot holds the whole copy, so a second
            // reader would re-ingest the same rows and add their weights again. Never
            // re-sliced either — this worker needs the full copy, not a share of it.
            let replicated = schema.placement().is_replicated();
            if replicated && idx > 0 {
                return Ok(false);
            }
            let batch = match ipc::decode_wire(data).ok().and_then(|d| d.data_batch) {
                Some(b) if b.count > 0 => b,
                _ => return Ok(false),
            };
            let owned = if reslice && !replicated {
                // Re-cut with the write path's own router, so what survives is exactly
                // what the master would have written to this rank's slot: same
                // distribution-prefix hash, same partition→worker map.
                let mb = batch.as_mem_batch();
                crate::ops::with_worker_indices(&batch, &schema, num_workers as usize, |wi| {
                    Batch::from_indexed_rows(&mb, &wi[rank as usize], &schema)
                })
            } else {
                batch
            };
            if owned.count == 0 {
                return Ok(false);
            }
            // The error rides the startup ACK: the master fails boot BEFORE zeroing
            // the SAL sentinel, so the replayed data's only durable copy survives. A
            // swallowed error here would zero the sentinel and orphan the un-applied
            // committed data.
            let effective = cat.ingest_returning_effective(tid, owned).map_err(|e| {
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
            Ok(true)
        })?;
    }

    if replayed > 0 {
        boot_log(&format!("SAL recovery: replayed {replayed} blocks\n"));
    }
    Ok(pending)
}

/// Worker-boot catalog recovery. The order below is required:
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
fn worker_boot_recovery(
    catalog: &mut CatalogEngine,
    sal_ptr: *const u8,
    rank: u32,
    num_workers: u32,
) -> Result<HashMap<i64, Batch>, String> {
    catalog
        .backfill_all_indexes()
        .map_err(|e| format!("boot index backfill failed: {e}"))?;
    let pending_deltas = recover_from_sal(sal_ptr, rank, num_workers, catalog)?;
    // Keep the boot flush: the non-windowed recovery resets the SAL before the
    // master-driven tick sweep, so the replayed base rows must be shard-durable
    // first — else the reset would drop acknowledged tail data.
    for tid in catalog.iter_user_table_ids() {
        catalog
            .flush_family(tid)
            .map_err(|e| format!("boot flush of table {tid} failed: {e}"))?;
    }
    if BOOT_FLUSH_ERROR.armed() {
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

/// Step-4: rebuild only the views the boot verdict rejected, through the same
/// dependency-ordered driver a live CREATE VIEW uses. The worker resets a view's
/// output partitions + operator scratch on the FIRST backfill command it receives
/// (gated on its COW-inherited `invalid_views` set), then fills. The driver is
/// view-scoped, so a resumed sibling's loaded shards are never re-derived and
/// double-counted.
fn rebuild_invalid_views(catalog: &mut CatalogEngine, dispatcher: &mut MasterDispatcher) -> Result<(), String> {
    let invalid: Vec<i64> = catalog.invalid_views.iter().copied().collect();
    // Resume-vs-rebuild marker (asserted by the "no backfill on clean restart"
    // E2E): 0 ⇒ every view resumed from its checkpoint.
    boot_log(&format!("recovery: rebuilding {} invalid view(s)\n", invalid.len()));
    dispatcher.backfill_views_in_depth_order(&invalid)
}

// ---------------------------------------------------------------------------
// Server main entry point
// ---------------------------------------------------------------------------

/// TLS listener request from the CLI: the address to bind, optional operator
/// cert/key PEM paths (a self-signed dev cert is minted when absent), the
/// optional client-auth CA (enables required mTLS), the
/// `--allow-unauthenticated` escape hatch, and the global connection cap.
pub struct TlsCli {
    pub listen: std::net::SocketAddr,
    pub cert_key: Option<(String, String)>,
    pub client_ca: Option<String>,
    pub allow_unauthenticated: bool,
    pub max_conns: u32,
}

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
    w2m_fds: Vec<i32>,
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
    // `Reserved`: the SAL is a real file, and reserving its blocks now is what
    // keeps a later write from failing for want of disk space.
    let sal_ptr = posix_io::map_shared_sized(sal_fd, sal_mmap_size(), posix_io::Backing::Reserved)
        .map_err(|e| format!("failed to map SAL ({} bytes): {e}", sal_mmap_size()))?;

    let mut w2m_ptrs: Vec<*mut u8> = Vec::with_capacity(nw);
    let mut w2m_fds: Vec<i32> = Vec::with_capacity(nw);
    for w in 0..nw {
        let wfd = posix_io::memfd_create(format!("w2m_{w}").as_bytes());
        if wfd < 0 {
            return Err(format!("memfd_create for W{w} failed"));
        }
        // `Sized`, not `Reserved`: a memfd's pages are RAM charged on first
        // touch, so reserving would commit the whole region per worker.
        let wptr = posix_io::map_shared_sized(wfd, W2M_REGION_SIZE, posix_io::Backing::Sized)
            .map_err(|e| format!("failed to map W2M region for W{w}: {e}"))?;
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

    // M2W eventfds (master→worker signaling; W2M wakes via futex).
    let mut m2w_efds: Vec<i32> = Vec::with_capacity(nw);
    for w in 0..nw {
        let efd = posix_io::eventfd_create();
        if efd < 0 {
            return Err(format!("eventfd_create for W{w} failed"));
        }
        m2w_efds.push(efd);
    }

    Ok(SharedIpc {
        sal_fd,
        sal_ptr,
        w2m_ptrs,
        w2m_fds,
        m2w_efds,
    })
}

/// The forked child's whole life: latch its rank, redirect its logs to
/// `worker_N.log`, trim to its partition range, recover, and run the worker
/// loop. Never returns — the worker exits via `libc::_exit`.
fn run_worker_child(
    w: usize,
    num_workers: u32,
    log_level: u32,
    data_dir: &str,
    master_pid: i32,
    catalog_ptr: *mut CatalogEngine,
    ipc: &SharedIpc,
) -> ! {
    // Die immediately if the master exits for any reason.  The getppid() check
    // in sal_reader.wait() is a belt-and-suspenders fallback; this closes the
    // ~30s polling gap.
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
    for (j, &efd) in ipc.m2w_efds.iter().enumerate() {
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

    let sal_reader = SalReader::new(ipc.sal_ptr as *const u8, w as u32, sal_mmap_size(), ipc.m2w_efds[w]);
    let w2m_writer = W2mWriter::new(ipc.w2m_ptrs[w], W2M_REGION_SIZE as u64);

    // Re-home inherited unhashed (replicated / replicated-derived) stores
    // from the pre-fork master's `rep_0` to THIS worker's own `rep_{w}` dir before
    // any flush — all workers share the data directory, so a fixed `rep_0` would
    // collide. The inherited store is empty; the pre-fork `reconcile_child_dirs`
    // already put this rank's checkpointed shards under `rep_{w}` (loaded on open)
    // and FLAG_PUSH replay adds the SAL tail.
    //
    // Then recover: rebuild indexes, replay the SAL tail (buffering effective base
    // deltas), boot-flush the replayed rows durable. The buffered deltas seed
    // `pending_deltas`; the master's post-reset tick sweep drives them into the
    // views. All view derivation moved to the master's sweep + step-4 rebuild — no
    // child-side view backfill.
    //
    // Either failure rides the startup ACK, which fails boot before the master
    // zeroes the SAL sentinel.
    let (pending_deltas, boot_err): (HashMap<i64, Batch>, Option<String>) = match catalog
        .rehome_unhashed_stores()
        .map_err(|e| format!("W{w} rehome unhashed stores failed: {e}"))
        .and_then(|()| worker_boot_recovery(catalog, ipc.sal_ptr as *const u8, w as u32, num_workers))
    {
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
    crate::foundation::worker_ctx::set_master_role();

    // Raise fd limit (partition directories + shard files)
    posix_io::raise_fd_limit(65536);

    gnitz_info!("Opening database at {}", data_dir);

    let catalog = CatalogEngine::open(data_dir).map_err(|e| format!("failed to open catalog: {e}"))?;
    let catalog_ptr = Box::into_raw(Box::new(catalog));

    let nw = num_workers as usize;
    boot_log(&format!("Starting {num_workers} workers\n"));
    boot_log(&format!(
        "Worker logs: {}/worker_N.log (N=0..{})\n",
        data_dir,
        num_workers - 1
    ));

    let ipc = acquire_shared_ipc(data_dir, nw)?;

    // --- System table SAL recovery (before forking workers) ---
    {
        let catalog = unsafe { &mut *catalog_ptr };
        recover_system_tables_from_sal(ipc.sal_ptr as *const u8, catalog)?;
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
        // dag.tables, so a SAL-committed-but-unflushed CREATE is not mistaken
        // for an orphan.
        catalog.gc_orphan_directories();

        // Bring the relation child directories into this boot's worker count and
        // seed any replicated base table copy it needs. Must run after SAL replay
        // (so `dag.tables` is complete and dropped subtrees are already gone) and
        // before the fork, since each worker's `rehome_unhashed_stores`
        // opens `rep_{rank}` unconditionally.
        catalog
            .reconcile_child_dirs(num_workers)
            .map_err(|e| format!("child-dir reconciliation failed: {e}"))?;
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
        catalog
            .recovery_start_generation_bump()
            .map_err(|e| format!("recovery-start generation bump failed: {e}"))?;
        inject_recovery_panic("genbump");

        // Name the replay path the workers will take, so the boot record shows
        // whether the tail was re-sliced. Pre-fork, so this still reaches the
        // master's stdout (the children redirect fd 1 to their own logs), and read
        // from the same place the workers read it.
        if let Some(written) = unsafe { sal_tail_slot_count(ipc.sal_ptr as *const u8) } {
            if written != num_workers {
                boot_log(&format!(
                    "SAL tail written by {written} workers, launching {num_workers}\n"
                ));
            }
        }
    }

    // Log fd assignments
    boot_log(&format!("SAL fd={}\n", ipc.sal_fd));
    for w in 0..nw {
        boot_log(&format!(
            "W{} m2w_efd={} w2m_fd={}\n",
            w, ipc.m2w_efds[w], ipc.w2m_fds[w]
        ));
    }

    let master_pid = unsafe { libc::getpid() };

    // --- Fork workers ---
    let mut worker_pids: Vec<i32> = vec![0; nw];
    for (w, slot) in worker_pids.iter_mut().enumerate() {
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err("fork failed".to_string());
        }
        if pid == 0 {
            run_worker_child(w, num_workers, log_level, data_dir, master_pid, catalog_ptr, &ipc);
        }
        *slot = pid;
    }

    // --- Parent process ---
    let catalog = unsafe { &mut *catalog_ptr };
    catalog.close_user_table_partitions();
    catalog.set_active_partitions(0, 0);

    let sal_writer = SalWriter::new(ipc.sal_ptr, ipc.sal_fd, sal_mmap_size() as u64, ipc.m2w_efds.clone());
    let w2m_receiver = W2mReceiver::new(ipc.w2m_ptrs.clone());

    let dispatcher = MasterDispatcher::new(nw, worker_pids.clone(), catalog_ptr, sal_writer, w2m_receiver);
    let dispatcher_ptr = Box::into_raw(Box::new(dispatcher));

    // Wait for all workers to complete recovery and signal readiness
    let dispatcher = unsafe { &mut *dispatcher_ptr };
    dispatcher
        .collect_acks()
        .map_err(|e| format!("Error collecting worker acks: {e}"))?;

    // Reset SAL for fresh use (all workers have recovered)
    dispatcher.reset_sal();

    inject_recovery_panic("reset");

    // Recovery tick sweep: apply the un-checkpointed SAL tail (buffered as
    // pending_deltas during replay) to every view via one master-driven tick per
    // reachable base, on the freshly-reset SAL. Resumed views are extended;
    // invalid views are polluted (reset+rebuilt next).
    recovery_tick_sweep(catalog, dispatcher).map_err(|e| format!("recovery tick sweep failed: {e}"))?;

    inject_recovery_panic("sweep");

    // Step-4: reset (on the workers) and rebuild only the invalid views.
    rebuild_invalid_views(catalog, dispatcher).map_err(|e| format!("invalid-view rebuild failed: {e}"))?;

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
    let server_fd = posix_io::server_create(socket_path)
        .map(std::os::fd::IntoRawFd::into_raw_fd)
        .map_err(|e| format!("failed to create server socket: {e}"))?;

    // Optional TLS listener — after the worker fork (same position as
    // `server_create`), so no fd inheritance. TLS was explicitly requested
    // via --tls-listen, so any setup failure aborts boot loudly (silently
    // continuing AF_UNIX-only would be surprising).
    let tls_init = match tls_cli {
        Some(cli) => Some(setup_tls_listener(data_dir, &cli)?),
        None => None,
    };
    boot_log("GnitzDB ready\n");

    Ok(ServerExecutor::run(catalog_ptr, dispatcher_ptr, server_fd, tls_init))
}

/// Build the rustls server config (minting + persisting the public dev cert
/// when no PEM pair is given), bind the TCP listener, and publish the bound
/// address to `<data_dir>/tls_endpoint` (atomically: tmp + rename, so
/// existence implies complete content).
///
/// **Bind refusal:** a non-loopback bind with neither `--tls-client-ca` nor
/// `--allow-unauthenticated` aborts boot — an unauthenticated public bind is
/// impossible by accident. `--allow-unauthenticated` is the single, loud
/// escape hatch; the CA enables required mTLS.
fn setup_tls_listener(data_dir: &str, cli: &TlsCli) -> Result<TlsListener, String> {
    // `is_loopback()` is the conservative test: `0.0.0.0`/`::` (bind-all),
    // IPv4-mapped `::ffff:127.0.0.1`, and any specific LAN/link-local IP are
    // all non-loopback → refused unless a CA or the escape hatch is present.
    if !cli.listen.ip().is_loopback() && cli.client_ca.is_none() && !cli.allow_unauthenticated {
        return Err(format!(
            "refusing to bind a non-loopback TLS listener {} without client authentication; \
             pass --tls-client-ca=PEM or --allow-unauthenticated",
            cli.listen,
        ));
    }

    let cert_key = cli.cert_key.as_ref().map(|(c, k)| (c.as_str(), k.as_str()));
    let (config, dev_pem) = crate::runtime::tls::config::server_crypto(cert_key, cli.client_ca.as_deref())?;
    if let Some(pem) = dev_pem {
        let path = format!("{data_dir}/tls_dev_cert.pem");
        std::fs::write(&path, pem).map_err(|e| format!("failed to write {path}: {e}"))?;
        gnitz_info!(
            "TLS: minted a self-signed dev certificate (identity is ephemeral, regenerated every boot); \
             public PEM at {path}"
        );
    }
    let listen_fd =
        posix_io::tcp_bind(&cli.listen).map_err(|e| format!("failed to bind TLS listener {}: {e}", cli.listen))?;
    let bound = posix_io::tcp_local_addr(listen_fd).unwrap_or(cli.listen);
    let endpoint_path = format!("{data_dir}/tls_endpoint");
    let tmp_path = format!("{endpoint_path}.tmp");
    std::fs::write(&tmp_path, format!("{bound}\n")).map_err(|e| format!("failed to write {tmp_path}: {e}"))?;
    std::fs::rename(&tmp_path, &endpoint_path).map_err(|e| format!("failed to publish {endpoint_path}: {e}"))?;
    gnitz_info!("Listening on tls://{}", bound);
    // A deliberately-unauthenticated non-loopback bind (escape hatch, no CA)
    // stays loud. With a CA the listener is authenticated — no warning.
    if !bound.ip().is_loopback() && cli.client_ca.is_none() {
        gnitz_warn!(
            "TLS listener bound to NON-LOOPBACK address {} with --allow-unauthenticated and NO client \
             authentication: anyone who can reach this port gets full DDL/DML/scan access. Prefer \
             --tls-client-ca=PEM (required mTLS). Note: even a loopback bind trusts every local UID.",
            bound,
        );
    }
    Ok(TlsListener {
        fd: listen_fd,
        cfg: config,
        max_conns: cli.max_conns,
    })
}

// ---------------------------------------------------------------------------
// Tests for the two-pass recovery primitives. The full end-to-end crash
// path is covered in `crates/gnitz-py/tests/test_crash_recovery.py`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::runtime::sal::{sal_write_group, SalReader, SalWriter};
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
            let nw = 1u32;

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
            let nw = 1u32;

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

            let payload = [0u8; 32];
            sal_write_group(ptr, 0, 50, 11, FLAG_PUSH, 1, size as u64, &[&payload]).expect("group fits");
            // No sentinel — zone unclosed.

            let efd = posix_io::eventfd_create();
            let reader = SalReader::new(ptr as *const u8, 0, size, efd);
            let committed = collect_committed_lsns(&reader);
            assert!(
                !committed.contains(&11),
                "uncommitted push must NOT appear in committed set"
            );

            libc::close(efd);
        }
    }
}
