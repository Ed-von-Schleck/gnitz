# gnitz/server/main.py
#
# Standalone server binary entry point.
# Parses CLI arguments and hands off to the Rust bootstrap (bootstrap.rs).

import os
from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi

from gnitz import log
from gnitz.storage import engine_ffi


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
    engine_ffi.log_init(level, "M")

    jit_params = os.environ.get("GNITZ_JIT")
    if jit_params is not None:
        from rpython.rlib.jit import set_user_param
        set_user_param(None, jit_params)

    if pos < 2:
        os.write(2, "Error: missing required arguments\n")
        os.write(2, "Try 'gnitz-server --help' for usage information\n")
        return 1

    # Hand off to Rust for the entire bootstrap sequence
    data_dir_buf = rffi.str2charp(data_dir)
    socket_path_buf = rffi.str2charp(socket_path)
    try:
        rc = engine_ffi._server_main(
            data_dir_buf, rffi.cast(rffi.UINT, len(data_dir)),
            socket_path_buf, rffi.cast(rffi.UINT, len(socket_path)),
            rffi.cast(rffi.UINT, num_workers),
            rffi.cast(rffi.UINT, level))
    finally:
        rffi.free_charp(data_dir_buf)
        rffi.free_charp(socket_path_buf)
    return intmask(rc)


def target(driver, args):
    return entry_point, None
