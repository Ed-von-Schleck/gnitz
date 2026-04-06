# gnitz/storage/engine_ffi.py
#
# RPython FFI bindings for libgnitz_engine (Rust staticlib).
# After the Rust bootstrap migration, only log_init and _server_main remain.

import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo

_lib_dir = os.environ.get("GNITZ_ENGINE_LIB", "")
_lib_path = os.path.join(_lib_dir, "libgnitz_engine.a") if _lib_dir else ""

eci = ExternalCompilationInfo(
    pre_include_bits=[
        "#include <stdint.h>",
        # logging
        "void gnitz_log_init(uint32_t level, const uint8_t *tag, uint32_t tag_len);",
        # Server bootstrap (bootstrap.rs)
        "int32_t gnitz_server_main("
        "  const uint8_t *data_dir_ptr, uint32_t data_dir_len,"
        "  const uint8_t *socket_path_ptr, uint32_t socket_path_len,"
        "  uint32_t num_workers, uint32_t log_level);",
    ],
    link_files=[_lib_path] if _lib_path else [],
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_log_init = rffi.llexternal(
    "gnitz_log_init",
    [rffi.UINT, rffi.CCHARP, rffi.UINT],
    lltype.Void,
    compilation_info=eci,
)


def log_init(level, tag):
    """Initialize Rust-side logging. Call after log.init() in main.py."""
    n = len(tag)
    tag_buf = lltype.malloc(rffi.CCHARP.TO, max(n, 1), flavor="raw")
    for i in range(n):
        tag_buf[i] = tag[i]
    _log_init(rffi.cast(rffi.UINT, level), tag_buf, rffi.cast(rffi.UINT, n))
    lltype.free(tag_buf, flavor="raw")


# ---------------------------------------------------------------------------
# Server bootstrap
# ---------------------------------------------------------------------------

_server_main = rffi.llexternal(
    "gnitz_server_main",
    [rffi.CCHARP, rffi.UINT, rffi.CCHARP, rffi.UINT, rffi.UINT, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)
