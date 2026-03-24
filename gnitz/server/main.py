# gnitz/server/main.py
#
# Standalone server binary entry point.
# Listens on a Unix domain socket and dispatches IPC v2 messages.

import os
from rpython.rlib.rarithmetic import intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi

from gnitz import log
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import r_uint64

from gnitz.catalog.engine import open_engine
from gnitz.storage import mmap_posix
from gnitz.storage.mmap_posix import raise_fd_limit
from gnitz.catalog import system_tables as sys
from gnitz.server.executor import ServerExecutor
from gnitz.server import ipc, eventfd_ffi, sal_ffi
from gnitz.server.master import MasterDispatcher
from gnitz.dbsp.ops.exchange import PartitionAssignment
from gnitz.server.worker import WorkerProcess


def _backfill_exchange_views(engine, dispatcher):
    """Issue fan_out_backfill for every exchange-requiring view."""
    for family in engine.registry.iter_families():
        vid = family.table_id
        if vid < sys.FIRST_USER_TABLE_ID:
            continue
        plan = engine.program_cache.get_program(vid)
        if plan is None:
            continue
        if plan.exchange_post_plan is None and plan.join_shard_map is None:
            continue
        source_ids = engine.program_cache.get_source_ids(vid)
        for source_id in source_ids:
            if not engine.registry.has_id(source_id):
                continue
            src_family = engine.registry.get_by_id(source_id)
            dispatcher.fan_out_backfill(vid, source_id, src_family.schema)

HELP_TEXT = (
    "gnitz-server — GnitzDB database server\n"
    "\n"
    "Usage:\n"
    "  gnitz-server [OPTIONS] <data_dir> <socket_path>\n"
    "\n"
    "Arguments:\n"
    "  <data_dir>      Path to the database data directory (created if absent)\n"
    "  <socket_path>   Path for the Unix domain socket to listen on\n"
    "\n"
    "Options:\n"
    "  --workers=N        Number of worker processes (default: 1, single-process)\n"
    "  --log-level=LEVEL  Set log verbosity: quiet, normal, verbose (default: quiet)\n"
    "  --help, -h         Show this help message and exit\n"
    "\n"
    "Environment:\n"
    "  GNITZ_LOG_LEVEL    Same as --log-level; CLI flag takes precedence\n"
    "  GNITZ_JIT          JIT tuning (e.g. 'vec=1'); ignored on nojit builds\n"
)


def _close_user_table_partitions(engine):
    """Master closes all user-table partitions after fork."""
    for family in engine.registry.iter_families():
        if family.table_id >= sys.FIRST_USER_TABLE_ID:
            family.store.close_all_partitions()


def _trim_worker_partitions(engine, part_start, part_end):
    """Worker closes partitions outside its assigned range."""
    for family in engine.registry.iter_families():
        if family.table_id >= sys.FIRST_USER_TABLE_ID:
            family.store.close_partitions_outside(part_start, part_end)


def _disable_worker_wal(engine):
    """Disable per-partition WAL for user tables (SAL handles durability)."""
    for family in engine.registry.iter_families():
        if family.table_id >= sys.FIRST_USER_TABLE_ID:
            family.store.set_has_wal(False)


def _recover_from_sal(sal_ptr, engine, worker_id):
    """Replay unflushed SAL push blocks for this worker's partitions."""
    # Build per-family max_flushed_lsn map
    family_lsns = {}
    for family in engine.registry.iter_families():
        tid = family.table_id
        if tid >= sys.FIRST_USER_TABLE_ID:
            family_lsns[tid] = family.store.get_max_flushed_lsn()

    offset = 0
    replayed = 0
    last_epoch = 0
    while offset + 8 < ipc.SAL_MMAP_SIZE:
        size = intmask(ipc.read_u64_raw(sal_ptr, offset))
        if size == 0:
            break
        # Epoch fence: decreasing epoch = stale data from before checkpoint
        hdr_off = offset + 8
        epoch = intmask(ipc._read_u32_raw(sal_ptr, hdr_off + 28))
        if last_epoch > 0 and epoch < last_epoch:
            break
        last_epoch = epoch
        msg = ipc.read_worker_message(sal_ptr, offset, worker_id)
        offset += msg.advance
        if msg.payload is None:
            continue
        if not (msg.flags & ipc.FLAG_PUSH):
            continue
        tid = msg.target_id
        if tid not in family_lsns:
            continue
        if msg.lsn <= family_lsns[tid]:
            continue
        b = msg.payload.batch
        if b is not None and b.length() > 0:
            family = engine.registry.get_by_id(tid)
            owned = b.clone()
            family.store.ingest_batch(owned)
            owned.free()
            replayed += 1

    if replayed > 0:
        os.write(1, "SAL recovery: replayed " + str(replayed) + " blocks\n")


def _parse_workers(arg):
    """Parse --workers=N, returns N or -1 on error."""
    val = arg[10:]
    n = 0
    for ch in val:
        if ch < "0" or ch > "9":
            return -1
        n = n * 10 + (ord(ch) - ord("0"))
    if n < 1:
        return -1
    return n


def entry_point(argv):
    level = log.QUIET
    env_level = os.environ.get("GNITZ_LOG_LEVEL")
    if env_level is not None:
        level = log.parse_level(env_level)

    data_dir = ""
    socket_path = ""
    num_workers = 1
    pos = 0
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--help" or arg == "-h":
            os.write(1, HELP_TEXT)
            return 0
        elif arg.startswith("--log-level="):
            level = log.parse_level(arg[12:])
        elif arg.startswith("--workers="):
            num_workers = _parse_workers(arg)
            if num_workers < 0:
                os.write(2, "Error: invalid --workers value\n")
                return 1
        elif pos == 0:
            data_dir = arg
            pos += 1
        elif pos == 1:
            socket_path = arg
            pos += 1
        i += 1

    log.init(level)

    jit_params = os.environ.get("GNITZ_JIT")
    if jit_params is not None:
        from rpython.rlib.jit import set_user_param
        set_user_param(None, jit_params)

    if pos < 2:
        os.write(2, "Error: missing required arguments\n")
        os.write(2, "Try 'gnitz-server --help' for usage information\n")
        return 1

    # Each partitioned user table holds 256 WAL fds.  Raise the soft
    # limit so the server doesn't hit EMFILE with a handful of tables.
    raise_fd_limit(65536)

    log.info("Opening database at " + data_dir)
    engine = open_engine(data_dir)

    if num_workers == 1:
        log.info("Listening on " + socket_path)
        os.write(1, "GnitzDB ready\n")
        ServerExecutor(engine).run_socket_server(socket_path)
        return 0

    # Multi-worker mode: allocate shared resources, fork workers
    os.write(1, "Starting " + str(num_workers) + " workers\n")
    os.write(1, "Worker logs: " + data_dir + "/worker_N.log (N=0.."
             + str(num_workers - 1) + ")\n")

    # --- Shared Append-Only Log (file-backed, master→all workers) ---
    sal_path = data_dir + "/wal.sal"
    sal_fd = rposix.open(sal_path, os.O_RDWR | os.O_CREAT, 0o644)
    sal_ffi.try_set_nocow(sal_fd)
    existing_size = intmask(mmap_posix.fget_size(sal_fd))
    if existing_size < ipc.SAL_MMAP_SIZE:
        sal_ffi.fallocate_c(sal_fd, ipc.SAL_MMAP_SIZE)
    sal_ptr = mmap_posix.mmap_file(
        sal_fd, ipc.SAL_MMAP_SIZE,
        prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
        flags=mmap_posix.MAP_SHARED,
    )
    sal = ipc.SharedAppendLog(sal_ptr, sal_fd, ipc.SAL_MMAP_SIZE)

    # --- W2M regions (memfd-backed, one per worker→master) ---
    w2m_regions = []
    for w in range(num_workers):
        wfd = mmap_posix.memfd_create_c("w2m_%d" % w)
        mmap_posix.ftruncate_c(wfd, ipc.W2M_REGION_SIZE)
        wptr = mmap_posix.mmap_file(
            wfd, ipc.W2M_REGION_SIZE,
            prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE,
            flags=mmap_posix.MAP_SHARED,
        )
        w2m_regions.append(ipc.W2MRegion(wptr, wfd, ipc.W2M_REGION_SIZE))

    # --- Eventfds (cross-process signaling) ---
    m2w_efds = [0] * num_workers
    w2m_efds = [0] * num_workers
    for w in range(num_workers):
        m2w_efds[w] = eventfd_ffi.eventfd_create()
        w2m_efds[w] = eventfd_ffi.eventfd_create()

    os.write(1, "SAL fd=" + str(sal_fd) + "\n")
    for w in range(num_workers):
        os.write(1, "W" + str(w) + " m2w_efd=" + str(m2w_efds[w]) + " w2m_efd=" + str(w2m_efds[w]) + " w2m_fd=" + str(w2m_regions[w].fd) + "\n")

    master_pid = os.getpid()

    assignment = PartitionAssignment(num_workers)
    worker_pids = [0] * num_workers

    for w in range(num_workers):
        pid = os.fork()
        if pid == 0:
            # --- Child process ---
            log_path = data_dir + "/worker_" + str(w) + ".log"
            try:
                log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                os.dup2(log_fd, 1)
                os.dup2(log_fd, 2)
                os.close(log_fd)
            except OSError:
                pass

            # Close eventfds of OTHER workers
            for j in range(num_workers):
                if j != w:
                    rposix.close(m2w_efds[j])
                    rposix.close(w2m_efds[j])

            # Set active partition range
            part_start, part_end = assignment.range_for_worker(w)
            engine.registry.active_part_start = part_start
            engine.registry.active_part_end = part_end
            _trim_worker_partitions(engine, part_start, part_end)

            # Disable per-partition WAL (SAL handles durability)
            _disable_worker_wal(engine)

            # SAL recovery — replay unflushed push data
            _recover_from_sal(sal_ptr, engine, w)

            engine.program_cache.invalidate_all()

            os.write(
                1,
                "Worker " + str(w) + " (pid " + str(os.getpid())
                + ") partitions [" + str(part_start) + ", " + str(part_end)
                + ")\n",
            )

            log.set_process_tag("W" + str(w))
            WorkerProcess(w, master_pid, engine, part_start, part_end,
                          sal_ptr, m2w_efds[w], w2m_regions[w],
                          w2m_efds[w]).run()
            os._exit(0)

        worker_pids[w] = pid

    # --- Parent process ---
    _close_user_table_partitions(engine)
    engine.registry.active_part_start = 0
    engine.registry.active_part_end = 0

    dispatcher = MasterDispatcher(num_workers, worker_pids,
                                   assignment, engine.program_cache,
                                   sal, w2m_regions, m2w_efds, w2m_efds)

    # Wait for all workers to complete recovery and signal readiness
    dispatcher._collect_acks()

    # Reset SAL for fresh use (all workers have recovered)
    ipc.atomic_store_u64(sal.ptr, rffi.cast(rffi.ULONGLONG, 0))
    sal.write_cursor = 0
    sal.epoch = 1
    # Do NOT reset lsn_counter — must stay monotonic for recovery correctness

    _backfill_exchange_views(engine, dispatcher)

    log.info("Listening on " + socket_path)
    os.write(1, "GnitzDB ready\n")
    ServerExecutor(engine, dispatcher).run_socket_server(socket_path)
    return 0


def target(driver, args):
    return entry_point, None
