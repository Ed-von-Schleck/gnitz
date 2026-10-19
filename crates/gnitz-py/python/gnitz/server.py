"""A gnitz-server run by this process, for development and tests.

    import gnitz

    with gnitz.local_server() as server:
        with gnitz.connect(server.target) as conn:
            conn.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")

The server is installed separately from this client: ``pip install
"gnitz[server]"`` adds the ``gnitz-server`` package beside it, and a
``gnitz-server`` on ``PATH`` serves as well. ``GNITZ_SERVER_BIN`` names the
binary to run in place of either. The server runs on Linux only.

A server run with no data directory keeps its data in a scratch directory that
`LocalServer.stop` removes; one given ``data_dir`` leaves its data there for the
next. A server nothing stops is stopped as the interpreter exits, and one whose
starting thread ends without stopping it stops all the same.
"""

from __future__ import annotations

import atexit
import os
import sysconfig

from gnitz import _native
from gnitz._native import LocalServer

__all__ = ["LocalServer", "find_server", "local_server"]


def find_server() -> str:
    """The ``gnitz-server`` binary `local_server` runs: the one
    ``GNITZ_SERVER_BIN`` names where it is set, else the one installed into
    this Python environment, else the first on ``PATH``."""
    try:
        return _native.find_server(sysconfig.get_path("scripts"))
    except FileNotFoundError as e:
        raise FileNotFoundError(f'{e}; `pip install "gnitz[server]"` installs one') from None


def local_server(data_dir: str | os.PathLike[str] | None = None, *, workers: int = 1) -> LocalServer:
    """Start a gnitz-server, and return it once it serves. With no
    ``data_dir`` its data is removed when it stops; with one, the data stays
    there for the next server run on it."""
    server = LocalServer(find_server(), data_dir, workers)
    atexit.register(server.stop)
    return server
