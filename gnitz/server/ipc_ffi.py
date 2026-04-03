# gnitz/server/ipc_ffi.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo

# C implementation of server_create (Unix domain SOCK_STREAM listener).
IPC_C_CODE = """
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

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
"""


eci = ExternalCompilationInfo(
    pre_include_bits=[
        "int gnitz_server_create(const char *socket_path);",
    ],
    separate_module_sources=[IPC_C_CODE],
    includes=["sys/socket.h", "sys/un.h", "string.h", "unistd.h", "fcntl.h"],
)

_gnitz_server_create = rffi.llexternal(
    "gnitz_server_create",
    [rffi.CCHARP],
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
