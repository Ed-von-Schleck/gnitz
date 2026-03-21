# gnitz/server/eventfd_ffi.py
#
# eventfd helpers for cross-process signaling via inline C FFI.
# Used by the SAL (shared append-only log) transport for master↔worker
# notification without socketpair overhead.

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib import jit
from rpython.rlib.rarithmetic import intmask

from gnitz.core.errors import StorageError

EVENTFD_C_CODE = """
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/eventfd.h>
#include <poll.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>

int gnitz_eventfd_create(void) {
    return eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
}

int gnitz_eventfd_signal(int efd) {
    uint64_t v = 1;
    ssize_t n;
    do {
        n = write(efd, &v, 8);
    } while (n < 0 && errno == EINTR);
    return (n == 8) ? 0 : -1;
}

int gnitz_eventfd_wait(int efd, int timeout_ms) {
    struct pollfd pfd = { .fd = efd, .events = POLLIN };
    int r;
    do {
        r = poll(&pfd, 1, timeout_ms);
    } while (r < 0 && errno == EINTR);
    if (r > 0) {
        uint64_t v;
        (void)read(efd, &v, 8);
    }
    return r;
}

int gnitz_eventfd_wait_any(int *efds, int n, int timeout_ms) {
    struct pollfd pfds[64];
    if (n > 64) n = 64;
    for (int i = 0; i < n; i++) {
        pfds[i].fd = efds[i];
        pfds[i].events = POLLIN;
    }
    int r;
    do {
        r = poll(pfds, (nfds_t)n, timeout_ms);
    } while (r < 0 && errno == EINTR);
    if (r > 0) {
        uint64_t v;
        for (int i = 0; i < n; i++) {
            if (pfds[i].revents & POLLIN) {
                (void)read(pfds[i].fd, &v, 8);
            }
        }
    }
    return r;
}
"""

eci = ExternalCompilationInfo(
    pre_include_bits=[
        "int gnitz_eventfd_create(void);",
        "int gnitz_eventfd_signal(int efd);",
        "int gnitz_eventfd_wait(int efd, int timeout_ms);",
        "int gnitz_eventfd_wait_any(int *efds, int n, int timeout_ms);",
    ],
    separate_module_sources=[EVENTFD_C_CODE],
    includes=["sys/eventfd.h", "poll.h", "unistd.h", "errno.h", "stdint.h"],
)

_gnitz_eventfd_create = rffi.llexternal(
    "gnitz_eventfd_create", [], rffi.INT,
    compilation_info=eci,
)

_gnitz_eventfd_signal = rffi.llexternal(
    "gnitz_eventfd_signal", [rffi.INT], rffi.INT,
    compilation_info=eci,
)

_gnitz_eventfd_wait = rffi.llexternal(
    "gnitz_eventfd_wait", [rffi.INT, rffi.INT], rffi.INT,
    compilation_info=eci,
)

_gnitz_eventfd_wait_any = rffi.llexternal(
    "gnitz_eventfd_wait_any", [rffi.INTP, rffi.INT, rffi.INT], rffi.INT,
    compilation_info=eci,
)


# ---------------------------------------------------------------------------
# Public RPython Wrappers
# ---------------------------------------------------------------------------

@jit.dont_look_inside
def eventfd_create():
    """Create a non-blocking, close-on-exec eventfd. Returns fd."""
    fd = _gnitz_eventfd_create()
    if rffi.cast(lltype.Signed, fd) < 0:
        raise StorageError("eventfd_create failed")
    return intmask(fd)


@jit.dont_look_inside
def eventfd_signal(efd):
    """Signal an eventfd (increment counter by 1). Fire-and-forget."""
    _gnitz_eventfd_signal(rffi.cast(rffi.INT, efd))


@jit.dont_look_inside
def eventfd_wait(efd, timeout_ms):
    """Wait for an eventfd to become readable.

    Returns >0 if ready (counter drained), 0 on timeout, <0 on error.
    """
    return intmask(_gnitz_eventfd_wait(
        rffi.cast(rffi.INT, efd), rffi.cast(rffi.INT, timeout_ms)))


@jit.dont_look_inside
def eventfd_wait_any(efd_list, timeout_ms):
    """Wait for any of the eventfds in efd_list to become readable.

    All ready fds are drained. Returns >0 if any ready, 0 on timeout,
    <0 on error.
    """
    count = len(efd_list)
    if count == 0:
        return 0
    c_fds = lltype.malloc(rffi.INTP.TO, count, flavor='raw')
    try:
        for i in range(count):
            c_fds[i] = rffi.cast(rffi.INT, efd_list[i])
        result = _gnitz_eventfd_wait_any(
            c_fds, rffi.cast(rffi.INT, count),
            rffi.cast(rffi.INT, timeout_ms))
    finally:
        lltype.free(c_fds, flavor='raw')
    return intmask(result)
