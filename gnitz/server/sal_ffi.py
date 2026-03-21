# gnitz/server/sal_ffi.py
#
# SAL (shared append-only log) file setup helpers via inline C FFI.
# Provides fallocate for block pre-allocation and NOCOW attribute for btrfs.

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib import jit

from gnitz.core.errors import StorageError

SAL_C_CODE = """
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <fcntl.h>
#include <errno.h>

int gnitz_try_set_nocow(int fd) {
    int flags = 0;
    if (ioctl(fd, FS_IOC_GETFLAGS, &flags) < 0) return -1;
    flags |= FS_NOCOW_FL;
    return ioctl(fd, FS_IOC_SETFLAGS, &flags);
}

int gnitz_fallocate(int fd, long long length) {
    return fallocate(fd, 0, 0, length);
}
"""

eci = ExternalCompilationInfo(
    pre_include_bits=[
        "#ifndef _GNU_SOURCE",
        "#define _GNU_SOURCE",
        "#endif",
        "int gnitz_try_set_nocow(int fd);",
        "int gnitz_fallocate(int fd, long long length);",
    ],
    separate_module_sources=[SAL_C_CODE],
    includes=["sys/ioctl.h", "linux/fs.h", "fcntl.h", "errno.h"],
)

_gnitz_try_set_nocow = rffi.llexternal(
    "gnitz_try_set_nocow", [rffi.INT], rffi.INT,
    compilation_info=eci,
)

_gnitz_fallocate = rffi.llexternal(
    "gnitz_fallocate", [rffi.INT, rffi.LONGLONG], rffi.INT,
    compilation_info=eci,
)


# ---------------------------------------------------------------------------
# Public RPython Wrappers
# ---------------------------------------------------------------------------

@jit.dont_look_inside
def try_set_nocow(fd):
    """Set FS_NOCOW_FL on fd (btrfs in-place overwrites).

    Silently ignored on non-btrfs filesystems (ioctl returns ENOTTY).
    Must be called on an empty file before any data is written.
    """
    _gnitz_try_set_nocow(rffi.cast(rffi.INT, fd))


@jit.dont_look_inside
def fallocate_c(fd, length):
    """Pre-allocate blocks for fd. Raises StorageError on failure."""
    res = _gnitz_fallocate(
        rffi.cast(rffi.INT, fd), rffi.cast(rffi.LONGLONG, length))
    if rffi.cast(lltype.Signed, res) < 0:
        raise StorageError("fallocate failed")
