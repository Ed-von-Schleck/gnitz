# gnitz/server/ipc_ffi.py

from rpython.rtyper.lltypesystem import rffi
from rpython.translator.tool.cbuild import ExternalCompilationInfo

# C implementation of FD passing using SCM_RIGHTS.
IPC_C_CODE = """
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

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

    msg.msg_iov = &io;
    msg.msg_iovlen = 1;
    msg.msg_control = u.buf;
    msg.msg_controllen = sizeof(u.buf);

    cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    *((int *) CMSG_DATA(cmsg)) = fd_to_send;

    // We use a loop for sendmsg to handle EINTR, though less common on local IPC.
    ssize_t res;
    do {
        res = sendmsg(sock_fd, &msg, 0);
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
"""


eci = ExternalCompilationInfo(
    pre_include_bits=[
        "int gnitz_ipc_send_fd(int sock_fd, int fd_to_send);",
        "int gnitz_ipc_recv_fd(int sock_fd);"
    ],
    separate_module_sources=[IPC_C_CODE],
    includes=["sys/socket.h", "sys/uio.h", "string.h", "unistd.h", "errno.h"],
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


def send_fd(sock_fd, fd_to_send):
    """
    Sends a file descriptor over a Unix Domain Socket.
    Returns 0 on success, -1 on failure.
    """
    return int(_gnitz_ipc_send_fd(rffi.cast(rffi.INT, sock_fd), rffi.cast(rffi.INT, fd_to_send)))


def recv_fd(sock_fd):
    """
    Receives a file descriptor from a Unix Domain Socket.
    Safely closes extraneous descriptors to prevent FD leaks.
    Returns the file descriptor, or -1 on failure.
    """
    return int(_gnitz_ipc_recv_fd(rffi.cast(rffi.INT, sock_fd)))
