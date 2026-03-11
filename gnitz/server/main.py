# gnitz/server/main.py
#
# Standalone server binary entry point.
# Listens on a Unix domain socket and dispatches IPC v2 messages.

import os
from rpython.rlib.rarithmetic import intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz import log
from gnitz.catalog.engine import open_engine
from gnitz.catalog import system_tables as sys
from gnitz.server.executor import ServerExecutor
from gnitz.server import ipc_ffi
from gnitz.server.master import PartitionAssignment, MasterDispatcher
from gnitz.server.worker import WorkerProcess

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

    if pos < 2:
        os.write(2, "Error: missing required arguments\n")
        os.write(2, "Try 'gnitz-server --help' for usage information\n")
        return 1

    log.info("Opening database at " + data_dir)
    engine = open_engine(data_dir)

    if num_workers == 1:
        log.info("Listening on " + socket_path)
        os.write(1, "GnitzDB ready\n")
        ServerExecutor(engine).run_socket_server(socket_path)
        return 0

    # Multi-worker mode: create socketpairs, fork workers
    os.write(1, "Starting " + str(num_workers) + " workers\n")

    parent_fds = [0] * num_workers
    child_fds = [0] * num_workers
    for w in range(num_workers):
        p_fd, c_fd = ipc_ffi.create_socketpair()
        parent_fds[w] = p_fd
        child_fds[w] = c_fd

    assignment = PartitionAssignment(num_workers)
    worker_pids = [0] * num_workers

    for w in range(num_workers):
        pid = os.fork()
        if pid == 0:
            # --- Child process ---
            # Close parent-side fds of all socketpairs
            for j in range(num_workers):
                os.close(parent_fds[j])

            # Close child-side fds of other workers' socketpairs
            for j in range(num_workers):
                if j != w:
                    os.close(child_fds[j])

            my_fd = child_fds[w]

            # Set active partition range so hooks only create owned partitions
            part_start, part_end = assignment.range_for_worker(w)
            engine.registry.active_part_start = part_start
            engine.registry.active_part_end = part_end

            # Trim existing partitions to only this worker's range
            _trim_worker_partitions(engine, part_start, part_end)

            os.write(
                1,
                "Worker " + str(w) + " (pid " + str(os.getpid())
                + ") partitions [" + str(part_start) + ", " + str(part_end)
                + ")\n",
            )

            WorkerProcess(w, my_fd, engine, part_start, part_end).run()
            os._exit(0)

        worker_pids[w] = pid

    # --- Parent process ---
    # Close child-side fds
    for w in range(num_workers):
        os.close(child_fds[w])

    # Master doesn't own any user-table partitions
    _close_user_table_partitions(engine)
    engine.registry.active_part_start = 0
    engine.registry.active_part_end = 0

    dispatcher = MasterDispatcher(num_workers, parent_fds, worker_pids, assignment)

    log.info("Listening on " + socket_path)
    os.write(1, "GnitzDB ready\n")
    ServerExecutor(engine, dispatcher).run_socket_server(socket_path)
    return 0


def target(driver, args):
    return entry_point, None
