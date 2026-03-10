"""Unix socket + SCM_RIGHTS fd passing transport."""

import os
import mmap
import socket
import struct


def connect(socket_path: str) -> socket.socket:
    """Connect to the server via a Unix SEQPACKET socket."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    sock.connect(socket_path)
    return sock


def send_memfd(sock: socket.socket, data: bytes) -> None:
    """Send data via a memfd shared memory file descriptor."""
    fd = os.memfd_create("gnitz_client")
    try:
        os.ftruncate(fd, len(data))
        mm = mmap.mmap(fd, len(data))
        mm[:] = data
        mm.close()
        # Send 1-byte dummy 'G' with the fd as ancillary data
        sock.sendmsg(
            [b"G"],
            [(socket.SOL_SOCKET, socket.SCM_RIGHTS, struct.pack("i", fd))],
        )
    finally:
        os.close(fd)


def recv_memfd(sock: socket.socket) -> bytes:
    """Receive data via a memfd shared memory file descriptor."""
    msg, ancdata, flags, addr = sock.recvmsg(1, socket.CMSG_SPACE(4))
    if not msg:
        raise ConnectionError("Server closed connection")

    received_fd = -1
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            fds = struct.unpack("i" * (len(cmsg_data) // 4), cmsg_data)
            for i, fd in enumerate(fds):
                if received_fd == -1 and fd >= 0:
                    received_fd = fd
                elif fd >= 0:
                    os.close(fd)  # close extraneous fds

    if received_fd < 0:
        raise ConnectionError("No file descriptor received")

    try:
        size = os.fstat(received_fd).st_size
        mm = mmap.mmap(received_fd, size, access=mmap.ACCESS_READ)
        data = bytes(mm[:])
        mm.close()
    finally:
        os.close(received_fd)

    return data
