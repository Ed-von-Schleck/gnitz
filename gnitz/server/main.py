# gnitz/server/main.py
#
# Standalone server binary entry point.
# Listens on a Unix domain socket and dispatches IPC v2 messages.

import os
from gnitz.catalog.engine import open_engine
from gnitz.server.executor import ServerExecutor


def entry_point(argv):
    if len(argv) < 3:
        os.write(2, "Usage: gnitz-server <data_dir> <socket_path>\n")
        return 1
    engine = open_engine(argv[1])
    os.write(1, "GnitzDB ready. Listening on " + argv[2] + "\n")
    ServerExecutor(engine).run_socket_server(argv[2])
    return 0


def target(driver, args):
    return entry_point, None
