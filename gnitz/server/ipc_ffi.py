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

# C implementation of FD passing using SCM_RIGHTS and safe polling.
IPC_C_CODE = """
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <fcntl.h>

/* MSG_NOSIGNAL prevents the kernel from sending SIGPIPE and killing the DB
   when writing to a client that just abruptly disconnected. */
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

int gnitz_ipc_send_fd(int sock_fd, int fd_to_send) {
    struct msghdr msg = {0};
    struct cmsghdr *cmsg;
    char dummy_data[1] = {'G'};
    struct iovec io = {
        .iov_base = dummy_data,
        .iov_len = 1
    };
    
    // Union ensures alignment for the control message buffer.
    union {
        char buf[CMSG_SPACE(sizeof(int))];
        struct cmsghdr align;
    } u;

    // Zero out control message structure to prevent passing garbage 
    // to the kernel, which strict kernels will reject.
    memset(u.buf, 0, sizeof(u.buf));

    msg.msg_iov = &io;
    msg.msg_iovlen = 1;
    msg.msg_control = u.buf;
    msg.msg_controllen = sizeof(u.buf);

    cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    *((int *) CMSG_DATA(cmsg)) = fd_to_send;

    // We use a loop for sendmsg to handle EINTR.
    ssize_t res;
    do {
        res = sendmsg(sock_fd, &msg, MSG_NOSIGNAL);
    } while (res < 0 && errno == EINTR);

    return (res < 0) ? -1 : 0;
}

int gnitz_ipc_recv_fd(int sock_fd) {
    struct msghdr msg = {0};
    char dummy_data[1];
    struct iovec io = {
        .iov_base = dummy_data,
        .iov_len = 1
    };
    
    // Buffer for one FD. Even if client sends more, we only want one.
    union {
        char buf[CMSG_SPACE(sizeof(int))];
        struct cmsghdr align;
    } u;

    memset(u.buf, 0, sizeof(u.buf));

    msg.msg_iov = &io;
    msg.msg_iovlen = 1;
    msg.msg_control = u.buf;
    msg.msg_controllen = sizeof(u.buf);

    ssize_t n;
    do {
        n = recvmsg(sock_fd, &msg, 0);
    } while (n < 0 && errno == EINTR);

    if (n <= 0) {
        return -1;
    }

    int received_fd = -1;
    struct cmsghdr *cmsg;

    // HARDENING: Iterate through all control headers. 
    // If we find an FD, we keep the first one and CLOSE all others to prevent leaks.
    for (cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
        if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
            int *fd_ptr = (int *) CMSG_DATA(cmsg);
            int payload_len = cmsg->cmsg_len - CMSG_LEN(0);
            int num_fds = payload_len / sizeof(int);

            for (int i = 0; i < num_fds; i++) {
                int fd = fd_ptr[i];
                if (received_fd == -1 && fd >= 0) {
                    received_fd = fd;
                } else if (fd >= 0) {
                    // Close extraneous FDs immediately.
                    close(fd);
                }
            }
        }
    }

    return received_fd;
}

int gnitz_ipc_recv_fd_nb(int sock_fd) {
    struct msghdr msg = {0};
    char dummy_data[1];
    struct iovec io = {
        .iov_base = dummy_data,
        .iov_len = 1
    };

    union {
        char buf[CMSG_SPACE(sizeof(int))];
        struct cmsghdr align;
    } u;

    memset(u.buf, 0, sizeof(u.buf));

    msg.msg_iov = &io;
    msg.msg_iovlen = 1;
    msg.msg_control = u.buf;
    msg.msg_controllen = sizeof(u.buf);

    ssize_t n;
    do {
        n = recvmsg(sock_fd, &msg, MSG_DONTWAIT);
    } while (n < 0 && errno == EINTR);

    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return -2;   /* sentinel: no message ready */
    if (n <= 0)
        return -1;   /* connection closed or hard error */

    int received_fd = -1;
    struct cmsghdr *cmsg;

    for (cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
        if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
            int *fd_ptr = (int *) CMSG_DATA(cmsg);
            int payload_len = cmsg->cmsg_len - CMSG_LEN(0);
            int num_fds = payload_len / sizeof(int);

            for (int i = 0; i < num_fds; i++) {
                int fd = fd_ptr[i];
                if (received_fd == -1 && fd >= 0) {
                    received_fd = fd;
                } else if (fd >= 0) {
                    close(fd);
                }
            }
        }
    }

    return received_fd;
}

int gnitz_ipc_poll_simple(int* fds, int* events, int* revents, int count, int timeout_ms) {
    if (count <= 0) return 0;

    // Stack allocation for small loads eliminates malloc overhead in the hot path.
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

    // Unconditionally copy back the revents, even if res == 0 (timeout),
    // to ensure the caller's array is correctly zeroed out.
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
    int fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
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

    /* Set non-blocking */
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
"""


eci = ExternalCompilationInfo(
    pre_include_bits=[
        "int gnitz_ipc_send_fd(int sock_fd, int fd_to_send);",
        "int gnitz_ipc_recv_fd(int sock_fd);",
        "int gnitz_ipc_recv_fd_nb(int sock_fd);",
        "int gnitz_ipc_poll_simple(int* fds, int* events, int* revents, int count, int timeout_ms);",
        "int gnitz_socketpair(int fds_out[2]);",
        "int gnitz_server_create(const char *socket_path);",
        "int gnitz_server_accept(int server_fd);",
    ],
    separate_module_sources=[IPC_C_CODE],
    includes=["sys/socket.h", "sys/un.h", "sys/uio.h", "string.h", "unistd.h", "errno.h", "poll.h", "stdlib.h", "fcntl.h"],
)

_gnitz_ipc_send_fd = rffi.llexternal(
    "gnitz_ipc_send_fd",
    [rffi.INT, rffi.INT],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_ipc_recv_fd = rffi.llexternal(
    "gnitz_ipc_recv_fd",
    [rffi.INT],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_ipc_recv_fd_nb = rffi.llexternal(
    "gnitz_ipc_recv_fd_nb",
    [rffi.INT],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_ipc_poll_simple = rffi.llexternal(
    "gnitz_ipc_poll_simple",[rffi.INTP, rffi.INTP, rffi.INTP, rffi.INT, rffi.INT],
    rffi.INT,
    compilation_info=eci,
)


def send_fd(sock_fd, fd_to_send):
    """
    Sends a file descriptor over a Unix Domain Socket.
    Returns 0 on success, -1 on failure (e.g., EPIPE due to client disconnect).
    """
    return int(_gnitz_ipc_send_fd(rffi.cast(rffi.INT, sock_fd), rffi.cast(rffi.INT, fd_to_send)))


def recv_fd(sock_fd):
    """
    Receives a file descriptor from a Unix Domain Socket.
    Safely closes extraneous descriptors to prevent FD leaks.
    Returns the file descriptor, or -1 on failure.
    """
    return int(_gnitz_ipc_recv_fd(rffi.cast(rffi.INT, sock_fd)))


def recv_fd_nb(sock_fd):
    """
    Non-blocking receive of a file descriptor from a Unix Domain Socket.
    Returns the file descriptor on success, -2 if no message is ready (EAGAIN),
    or -1 on connection close / hard error.
    """
    return int(_gnitz_ipc_recv_fd_nb(rffi.cast(rffi.INT, sock_fd)))


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
    """Creates a Unix SEQPACKET server socket, binds, and listens.
    Returns the server fd, or a negative value on error."""
    buf = rffi.str2charp(socket_path)
    try:
        return int(_gnitz_server_create(buf))
    finally:
        rffi.free_charp(buf)


def server_accept(server_fd):
    """Accepts a new client connection. Returns client fd or -1."""
    return int(_gnitz_server_accept(rffi.cast(rffi.INT, server_fd)))
