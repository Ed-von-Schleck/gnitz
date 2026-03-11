# gnitz/server/main.py
#
# Standalone server binary entry point.
# Listens on a Unix domain socket and dispatches IPC v2 messages.

import os
from gnitz import log
from gnitz.catalog.engine import open_engine
from gnitz.server.executor import ServerExecutor


def entry_point(argv):
    level = log.QUIET
    env_level = os.environ.get("GNITZ_LOG_LEVEL")
    if env_level is not None:
        level = log.parse_level(env_level)

    data_dir = ""
    socket_path = ""
    pos = 0
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg.startswith("--log-level="):
            level = log.parse_level(arg[12:])
        elif pos == 0:
            data_dir = arg
            pos += 1
        elif pos == 1:
            socket_path = arg
            pos += 1
        i += 1

    log.init(level)

    if pos < 2:
        os.write(
            2,
            "Usage: gnitz-server [--log-level=quiet|normal|verbose]"
            " <data_dir> <socket_path>\n",
        )
        return 1

    log.info("Opening database at " + data_dir)
    engine = open_engine(data_dir)
    log.info("Listening on " + socket_path)
    os.write(1, "GnitzDB ready\n")
    ServerExecutor(engine).run_socket_server(socket_path)
    return 0


def target(driver, args):
    return entry_point, None
