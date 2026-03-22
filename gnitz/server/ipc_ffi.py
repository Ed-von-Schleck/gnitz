# gnitz/server/ipc_ffi.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib import jit

# ---------------------------------------------------------------------------
# Standard POSIX Poll Events
# ---------------------------------------------------------------------------
POLLIN   = 0x001
POLLPRI  = 0x002
POLLOUT  = 0x004
POLLERR  = 0x008
POLLHUP  = 0x010
POLLNVAL = 0x020

# C implementation of IPC primitives: poll, socketpair, SOCK_STREAM framing.
IPC_C_CODE = """
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <fcntl.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

int gnitz_ipc_poll_simple(int* fds, int* events, int* revents, int count, int timeout_ms) {
    if (count <= 0) return 0;

    struct pollfd stack_pfds[64];
    struct pollfd *pfds = stack_pfds;

    if (count > 64) {
        pfds = (struct pollfd *)malloc(sizeof(struct pollfd) * count);
        if (!pfds) return -1;
    }

    for (int i = 0; i < count; i++) {
        pfds[i].fd = fds[i];
        pfds[i].events = (short)events[i];
        pfds[i].revents = 0;
    }

    int res;
    do {
        res = poll(pfds, (nfds_t)count, timeout_ms);
    } while (res < 0 && errno == EINTR);

    for (int i = 0; i < count; i++) {
        revents[i] = (int)pfds[i].revents;
    }

    if (count > 64) {
        free(pfds);
    }

    return res;
}

int gnitz_socketpair(int fds_out[2]) {
    return socketpair(AF_UNIX, SOCK_SEQPACKET, 0, fds_out);
}

int gnitz_server_create(const char *socket_path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path) - 1);

    unlink(socket_path);

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -2;
    }
    if (listen(fd, 1024) < 0) {
        close(fd);
        return -3;
    }

    /* Set non-blocking for the listen socket (accept loop) */
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    return fd;
}

int gnitz_server_accept(int server_fd) {
    int client_fd;
    do {
        client_fd = accept(server_fd, NULL, NULL);
    } while (client_fd < 0 && errno == EINTR);
    return client_fd;
}

int gnitz_send_framed(int sock_fd, const char *buf, unsigned int len) {
    /* Write [u32 LE length][payload] with partial-write + EINTR handling. */
    unsigned char hdr[4];
    hdr[0] = (unsigned char)(len & 0xFF);
    hdr[1] = (unsigned char)((len >> 8) & 0xFF);
    hdr[2] = (unsigned char)((len >> 16) & 0xFF);
    hdr[3] = (unsigned char)((len >> 24) & 0xFF);

    /* Send header */
    unsigned int sent = 0;
    while (sent < 4) {
        ssize_t n = send(sock_fd, hdr + sent, 4 - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        sent += (unsigned int)n;
    }

    /* Send payload */
    sent = 0;
    while (sent < len) {
        ssize_t n = send(sock_fd, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        sent += (unsigned int)n;
    }
    return 0;
}

int gnitz_recv_header_nb(int sock_fd, char *hdr_buf, int *hdr_pos) {
    /* Non-blocking read of up to 4 header bytes.
       Returns: 0 = header complete, -2 = EAGAIN, -1 = error/EOF. */
    while (*hdr_pos < 4) {
        ssize_t n = recv(sock_fd, hdr_buf + *hdr_pos, 4 - *hdr_pos, MSG_DONTWAIT);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return -2;
            return -1;
        }
        if (n == 0) return -1;  /* EOF */
        *hdr_pos += (int)n;
    }
    return 0;
}

int gnitz_recv_exact(int sock_fd, char *buf, unsigned int len) {
    /* Blocking read of exactly len bytes. EINTR + partial-read handling.
       Returns: 0 = success, -1 = error/EOF. */
    unsigned int got = 0;
    while (got < len) {
        ssize_t n = recv(sock_fd, buf + got, len - got, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;  /* EOF */
        got += (unsigned int)n;
    }
    return 0;
}

int gnitz_unix_connect(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}
"""


eci = ExternalCompilationInfo(
    pre_include_bits=[
        "int gnitz_ipc_poll_simple(int* fds, int* events, int* revents, int count, int timeout_ms);",
        "int gnitz_socketpair(int fds_out[2]);",
        "int gnitz_server_create(const char *socket_path);",
        "int gnitz_server_accept(int server_fd);",
        "int gnitz_send_framed(int sock_fd, const char *buf, unsigned int len);",
        "int gnitz_recv_header_nb(int sock_fd, char *hdr_buf, int *hdr_pos);",
        "int gnitz_recv_exact(int sock_fd, char *buf, unsigned int len);",
        "int gnitz_unix_connect(const char *path);",
    ],
    separate_module_sources=[IPC_C_CODE],
    includes=["sys/socket.h", "sys/un.h", "string.h", "unistd.h", "errno.h", "poll.h", "stdlib.h", "fcntl.h"],
)

_gnitz_ipc_poll_simple = rffi.llexternal(
    "gnitz_ipc_poll_simple",[rffi.INTP, rffi.INTP, rffi.INTP, rffi.INT, rffi.INT],
    rffi.INT,
    compilation_info=eci,
)


@jit.dont_look_inside
def poll(fds, events, timeout_ms):
    """
    High-performance event multiplexer for RPython.

    fds: List[int] containing file descriptors.
    events: List[int] containing event masks (e.g., POLLIN).
    timeout_ms: int timeout in milliseconds.

    Returns: List[int] of `revents`, identically sized and ordered
             as the input lists.
    """
    count = len(fds)
    revents = newlist_hint(count)
    if count == 0:
        return revents

    c_fds = lltype.malloc(rffi.INTP.TO, count, flavor='raw')
    c_events = lltype.malloc(rffi.INTP.TO, count, flavor='raw')
    c_revents = lltype.malloc(rffi.INTP.TO, count, flavor='raw')

    try:
        for i in range(count):
            c_fds[i] = rffi.cast(rffi.INT, fds[i])
            c_events[i] = rffi.cast(rffi.INT, events[i])
            c_revents[i] = rffi.cast(rffi.INT, 0)

        _gnitz_ipc_poll_simple(
            c_fds,
            c_events,
            c_revents,
            rffi.cast(rffi.INT, count),
            rffi.cast(rffi.INT, timeout_ms)
        )

        # Iteratively append to avoid RPython mr-poisoning
        for i in range(count):
            revents.append(int(c_revents[i]))

    finally:
        lltype.free(c_fds, flavor='raw')
        lltype.free(c_events, flavor='raw')
        lltype.free(c_revents, flavor='raw')

    return revents


_gnitz_socketpair = rffi.llexternal(
    "gnitz_socketpair",
    [rffi.INTP],
    rffi.INT,
    compilation_info=eci,
)


def create_socketpair():
    """Creates a SEQPACKET socketpair. Returns (parent_fd, child_fd)."""
    fds = lltype.malloc(rffi.INTP.TO, 2, flavor="raw")
    try:
        fds[0] = rffi.cast(rffi.INT, 0)
        fds[1] = rffi.cast(rffi.INT, 0)
        ret = _gnitz_socketpair(fds)
        if int(ret) < 0:
            raise OSError(0, "socketpair failed")
        parent_fd = int(fds[0])
        child_fd = int(fds[1])
        return parent_fd, child_fd
    finally:
        lltype.free(fds, flavor="raw")


_gnitz_server_create = rffi.llexternal(
    "gnitz_server_create",
    [rffi.CCHARP],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_server_accept = rffi.llexternal(
    "gnitz_server_accept",
    [rffi.INT],
    rffi.INT,
    compilation_info=eci,
)


def server_create(socket_path):
    """Creates a Unix SOCK_STREAM server socket, binds, and listens.
    Returns the server fd, or a negative value on error."""
    buf = rffi.str2charp(socket_path)
    try:
        return int(_gnitz_server_create(buf))
    finally:
        rffi.free_charp(buf)


def server_accept(server_fd):
    """Accepts a new client connection. Returns client fd or -1."""
    return int(_gnitz_server_accept(rffi.cast(rffi.INT, server_fd)))


# ---------------------------------------------------------------------------
# SOCK_STREAM framed transport
# ---------------------------------------------------------------------------

_gnitz_send_framed = rffi.llexternal(
    "gnitz_send_framed",
    [rffi.INT, rffi.CCHARP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_recv_header_nb = rffi.llexternal(
    "gnitz_recv_header_nb",
    [rffi.INT, rffi.CCHARP, rffi.INTP],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_recv_exact = rffi.llexternal(
    "gnitz_recv_exact",
    [rffi.INT, rffi.CCHARP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
)


@jit.dont_look_inside
def send_framed(sock_fd, buf_ptr, buf_len):
    """Send a length-prefixed frame: [u32 LE length][payload].
    Returns 0 on success, -1 on error."""
    return int(_gnitz_send_framed(
        rffi.cast(rffi.INT, sock_fd),
        buf_ptr,
        rffi.cast(rffi.UINT, buf_len),
    ))


@jit.dont_look_inside
def recv_header_nb(sock_fd, hdr_buf, hdr_pos_ptr):
    """Non-blocking read of up to 4 header bytes.
    Returns: 0 = header complete, -2 = EAGAIN, -1 = error/EOF."""
    return int(_gnitz_recv_header_nb(
        rffi.cast(rffi.INT, sock_fd),
        hdr_buf,
        hdr_pos_ptr,
    ))


@jit.dont_look_inside
def recv_exact(sock_fd, buf_ptr, buf_len):
    """Blocking read of exactly buf_len bytes.
    Returns: 0 = success, -1 = error/EOF."""
    return int(_gnitz_recv_exact(
        rffi.cast(rffi.INT, sock_fd),
        buf_ptr,
        rffi.cast(rffi.UINT, buf_len),
    ))


# ---------------------------------------------------------------------------
# Unix domain socket connect
# ---------------------------------------------------------------------------

_gnitz_unix_connect = rffi.llexternal(
    "gnitz_unix_connect",
    [rffi.CCHARP],
    rffi.INT,
    compilation_info=eci,
)


def unix_connect(socket_path):
    """Connect to a Unix SOCK_STREAM socket. Returns client fd or -1."""
    buf = rffi.str2charp(socket_path)
    try:
        return int(_gnitz_unix_connect(buf))
    finally:
        rffi.free_charp(buf)
