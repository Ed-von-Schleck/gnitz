# gnitz/server/rust_ffi.py
#
# RPython FFI bindings for libgnitz_transport (Rust staticlib).

import os
from rpython.rlib import jit
from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo

from gnitz.core.errors import StorageError

_lib_dir = os.environ.get("GNITZ_TRANSPORT_LIB", "")
_lib_path = os.path.join(_lib_dir, "libgnitz_transport.a") if _lib_dir else ""

eci = ExternalCompilationInfo(
    pre_include_bits=[
        "void *transport_create(int ring_size);",
        "void transport_destroy(void *transport);",
        "void transport_accept(void *transport, int server_fd);",
        "int transport_poll(void *t, int timeout_ms, int *out_fds,"
        " void **out_ptrs, unsigned int *out_lens, int out_max);",
        "int transport_send(void *t, int fd, const void *data,"
        " unsigned int len);",
        "void transport_free_recv(void *t, void *ptr);",
        "void transport_close_fd(void *t, int fd);",
    ],
    link_files=[_lib_path] if _lib_path else [],
)

# ---------------------------------------------------------------------------
# Raw FFI bindings (private)
# ---------------------------------------------------------------------------

_transport_create = rffi.llexternal(
    "transport_create", [rffi.INT], rffi.VOIDP,
    compilation_info=eci,
)

_transport_destroy = rffi.llexternal(
    "transport_destroy", [rffi.VOIDP], lltype.Void,
    compilation_info=eci,
)

_transport_accept = rffi.llexternal(
    "transport_accept", [rffi.VOIDP, rffi.INT], lltype.Void,
    compilation_info=eci,
)

_transport_poll = rffi.llexternal(
    "transport_poll",
    [rffi.VOIDP, rffi.INT,
     rffi.INTP, rffi.VOIDPP, rffi.UINTP,
     rffi.INT],
    rffi.INT,
    compilation_info=eci,
)

_transport_send = rffi.llexternal(
    "transport_send",
    [rffi.VOIDP, rffi.INT, rffi.VOIDP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_transport_free_recv = rffi.llexternal(
    "transport_free_recv", [rffi.VOIDP, rffi.VOIDP], lltype.Void,
    compilation_info=eci,
)

_transport_close_fd = rffi.llexternal(
    "transport_close_fd", [rffi.VOIDP, rffi.INT], lltype.Void,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Public RPython wrappers
# ---------------------------------------------------------------------------


@jit.dont_look_inside
def create(ring_size):
    """Create a Rust transport with the given io_uring ring size.

    Returns an opaque transport handle (rffi.VOIDP).
    Raises StorageError if creation fails.
    """
    handle = _transport_create(rffi.cast(rffi.INT, ring_size))
    if not handle:
        raise StorageError("transport_create failed (io_uring unavailable?)")
    return handle


@jit.dont_look_inside
def destroy(transport):
    """Destroy a transport, closing all connections."""
    _transport_destroy(transport)


@jit.dont_look_inside
def accept(transport, server_fd):
    """Start accepting connections on server_fd."""
    _transport_accept(transport, rffi.cast(rffi.INT, server_fd))


@jit.dont_look_inside
def poll(transport, timeout_ms, out_fds, out_ptrs, out_lens, out_max):
    """Poll for complete messages.

    Returns the number of messages written to the output arrays.
    """
    n = _transport_poll(
        transport,
        rffi.cast(rffi.INT, timeout_ms),
        out_fds, out_ptrs, out_lens,
        rffi.cast(rffi.INT, out_max),
    )
    return intmask(n)


@jit.dont_look_inside
def send(transport, fd, data_ptr, data_len):
    """Queue a response for sending.  Rust prepends the 4-byte length header.

    Returns 0 on success, -1 if the connection is closing or unknown.
    """
    ret = _transport_send(
        transport,
        rffi.cast(rffi.INT, fd),
        rffi.cast(rffi.VOIDP, data_ptr),
        rffi.cast(rffi.UINT, data_len),
    )
    return intmask(ret)


@jit.dont_look_inside
def free_recv(transport, ptr):
    """Free a recv buffer previously returned by poll."""
    _transport_free_recv(transport, ptr)


@jit.dont_look_inside
def close_fd(transport, fd):
    """Request connection close.  Actual close is deferred."""
    _transport_close_fd(transport, rffi.cast(rffi.INT, fd))
