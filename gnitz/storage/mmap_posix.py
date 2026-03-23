import os
import errno
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib import rposix, jit, rposix_stat
from rpython.rlib.rarithmetic import intmask

# ============================================================================
# POSIX Constants
# ============================================================================

PROT_READ       = 0x1
PROT_WRITE      = 0x2
MAP_SHARED      = 0x01
MAP_PRIVATE     = 0x02

LOCK_SH = 1
LOCK_EX = 2
LOCK_NB = 4
LOCK_UN = 8

# Linux memfd_create flags
MFD_CLOEXEC       = 0x0001
MFD_ALLOW_SEALING = 0x0002

eci = ExternalCompilationInfo(
    # Use guards to prevent redefinition warnings/errors
    pre_include_bits=[
        '#ifndef _GNU_SOURCE',
        '#define _GNU_SOURCE',
        '#endif'
    ],
    includes=['sys/mman.h', 'unistd.h', 'sys/file.h', 'sys/stat.h',
              'sys/resource.h']
)

# ============================================================================
# External C Functions (Private Raw Pointers)
# ============================================================================

# MAP_FAILED is typically (void *)-1
MAP_FAILED = rffi.cast(rffi.CCHARP, -1)

_mmap = rffi.llexternal(
    "mmap",
    [rffi.CCHARP, rffi.SIZE_T, rffi.INT, rffi.INT, rffi.INT, rffi.LONGLONG],
    rffi.CCHARP,
    compilation_info=eci,
    _nowrapper=True
)

_munmap = rffi.llexternal(
    "munmap",
    [rffi.CCHARP, rffi.SIZE_T],
    rffi.INT,
    compilation_info=eci,
    _nowrapper=True
)

_write = rffi.llexternal(
    "write",
    [rffi.INT, rffi.CCHARP, rffi.SIZE_T],
    rffi.SSIZE_T,
    compilation_info=eci,
    _nowrapper=True
)

_fsync = rffi.llexternal(
    "fsync",
    [rffi.INT],
    rffi.INT,
    compilation_info=eci,
    _nowrapper=True
)

_fdatasync = rffi.llexternal(
    "fdatasync",
    [rffi.INT],
    rffi.INT,
    compilation_info=eci,
    _nowrapper=True
)

_flock = rffi.llexternal(
    "flock",
    [rffi.INT, rffi.INT],
    rffi.INT,
    compilation_info=eci,
    _nowrapper=True
)

_memfd_create = rffi.llexternal(
    "memfd_create",
    [rffi.CCHARP, rffi.UINT],
    rffi.INT,
    compilation_info=eci,
    _nowrapper=True
)

_ftruncate = rffi.llexternal(
    "ftruncate",
    [rffi.INT, rffi.LONGLONG],
    rffi.INT,
    compilation_info=eci,
    _nowrapper=True
)

_read = rffi.llexternal(
    "read",
    [rffi.INT, rffi.CCHARP, rffi.SIZE_T],
    rffi.SSIZE_T,
    compilation_info=eci,
    _nowrapper=True
)

_rlimit_eci = ExternalCompilationInfo(
    pre_include_bits=[
        '#ifndef _GNU_SOURCE', '#define _GNU_SOURCE', '#endif',
        'long gnitz_raise_fd_limit(long target);',
    ],
    includes=['sys/resource.h'],
    separate_module_sources=["""
long gnitz_raise_fd_limit(long target) {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) != 0) return -1;
    if ((long)rl.rlim_cur >= target) return (long)rl.rlim_cur;
    rl.rlim_cur = (rlim_t)target;
    if (rl.rlim_cur > rl.rlim_max) rl.rlim_cur = rl.rlim_max;
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0) return -1;
    return (long)rl.rlim_cur;
}
"""],
)

_raise_fd_limit_c = rffi.llexternal(
    "gnitz_raise_fd_limit",
    [rffi.LONG],
    rffi.LONG,
    compilation_info=_rlimit_eci,
    _nowrapper=True,
)

_nocow_eci = ExternalCompilationInfo(
    pre_include_bits=[
        '#ifndef _GNU_SOURCE', '#define _GNU_SOURCE', '#endif',
        'int gnitz_storage_try_set_nocow(int fd);',
    ],
    includes=['sys/ioctl.h', 'linux/fs.h'],
    separate_module_sources=["""
int gnitz_storage_try_set_nocow(int fd) {
    int flags = 0;
    if (ioctl(fd, FS_IOC_GETFLAGS, &flags) < 0) return -1;
    flags |= FS_NOCOW_FL;
    return ioctl(fd, FS_IOC_SETFLAGS, &flags);
}
"""],
)

_try_set_nocow_c = rffi.llexternal(
    "gnitz_storage_try_set_nocow",
    [rffi.INT],
    rffi.INT,
    compilation_info=_nocow_eci,
    _nowrapper=True,
)

# ============================================================================
# Public High-Level Wrappers
# ============================================================================

class MMapError(Exception):
    pass


def raise_fd_limit(target):
    """Raise RLIMIT_NOFILE soft limit to target (capped by hard limit).
    Returns the new soft limit, or -1 on failure."""
    return intmask(_raise_fd_limit_c(rffi.cast(rffi.LONG, target)))


@jit.dont_look_inside
def try_set_nocow_dir(path):
    """Set FS_NOCOW_FL on a directory so new files inherit NOCOW.
    Silently ignored on non-btrfs filesystems."""
    try:
        fd = rposix.open(path, os.O_RDONLY, 0)
    except OSError:
        return
    _try_set_nocow_c(rffi.cast(rffi.INT, fd))
    rposix.close(fd)


@jit.dont_look_inside
def is_invalid_ptr(ptr):
    # -1 (MAP_FAILED) cast to unsigned is the max value.
    return rffi.cast(rffi.SIZE_T, ptr) == rffi.cast(rffi.SIZE_T, -1)

@jit.dont_look_inside
def mmap_file(fd, length, prot=PROT_READ, flags=MAP_SHARED):
    if fd < 0:
        raise MMapError()
    res = _mmap(lltype.nullptr(rffi.CCHARP.TO), 
                rffi.cast(rffi.SIZE_T, length),
                rffi.cast(rffi.INT, prot),
                rffi.cast(rffi.INT, flags),
                rffi.cast(rffi.INT, fd),
                rffi.cast(rffi.LONGLONG, 0))
    if is_invalid_ptr(res):
        raise MMapError()
    return res

@jit.dont_look_inside
def munmap_file(ptr, length):
    res = _munmap(ptr, rffi.cast(rffi.SIZE_T, length))
    if rffi.cast(lltype.Signed, res) == -1:
        raise MMapError()

@jit.dont_look_inside
def write_c(fd, ptr, length):
    return _write(rffi.cast(rffi.INT, fd), ptr, rffi.cast(rffi.SIZE_T, length))

@jit.dont_look_inside
def write_all(fd, ptr, length):
    """Write exactly length bytes to fd, looping on partial writes. Raises MMapError on error."""
    total = 0
    fd_c = rffi.cast(rffi.INT, fd)
    while total < length:
        n = rffi.cast(lltype.Signed, _write(
            fd_c,
            rffi.ptradd(ptr, total),
            rffi.cast(rffi.SIZE_T, length - total),
        ))
        if n <= 0:
            raise MMapError()
        total += n

@jit.dont_look_inside
def fsync_c(fd):
    res = _fsync(rffi.cast(rffi.INT, fd))
    return rffi.cast(lltype.Signed, res)

@jit.dont_look_inside
def fdatasync_c(fd):
    """Flush file data (not metadata) to disk. Raises MMapError on failure."""
    res = _fdatasync(rffi.cast(rffi.INT, fd))
    if rffi.cast(lltype.Signed, res) < 0:
        raise MMapError()

@jit.dont_look_inside
def fsync_dir(filepath):
    last_slash = filepath.rfind('/')
    
    if last_slash > 0:
        assert last_slash > 0
        dirname = filepath[:last_slash]
    else:
        dirname = "."
        
    try:
        fd = rposix.open(dirname, os.O_RDONLY, 0)
        try:
            fsync_c(fd)
        finally:
            rposix.close(fd)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise e

@jit.dont_look_inside
def try_lock_exclusive(fd):
    res = _flock(rffi.cast(rffi.INT, fd), 
                 rffi.cast(rffi.INT, LOCK_EX | LOCK_NB))
    return rffi.cast(lltype.Signed, res) == 0

@jit.dont_look_inside
def lock_shared(fd):
    _flock(rffi.cast(rffi.INT, fd), 
           rffi.cast(rffi.INT, LOCK_SH))

@jit.dont_look_inside
def unlock_file(fd):
    _flock(rffi.cast(rffi.INT, fd), 
           rffi.cast(rffi.INT, LOCK_UN))

@jit.dont_look_inside
def memfd_create_c(name, flags=MFD_CLOEXEC):
    """
    Creates an anonymous file and returns its file descriptor.
    """
    with rffi.scoped_str2charp(name) as name_ptr:
        res = _memfd_create(name_ptr, rffi.cast(rffi.UINT, flags))
        
    fd = rffi.cast(lltype.Signed, res)
    if fd < 0:
        raise MMapError()
    return fd

@jit.dont_look_inside
def ftruncate_c(fd, length):
    """
    Sets the size of a file descriptor.
    """
    res = _ftruncate(rffi.cast(rffi.INT, fd), rffi.cast(rffi.LONGLONG, length))
    if rffi.cast(lltype.Signed, res) < 0:
        raise MMapError()

@jit.dont_look_inside
def read_into_ptr(fd, ptr, length):
    """Read exactly `length` bytes into ptr. Returns bytes read, or -1 on error."""
    total = 0
    while total < length:
        got = _read(
            rffi.cast(rffi.INT, fd),
            rffi.ptradd(ptr, total),
            rffi.cast(rffi.SIZE_T, length - total),
        )
        n = rffi.cast(lltype.Signed, got)
        if n <= 0:
            return -1
        total += n
    return total

@jit.dont_look_inside
def fget_size(fd):
    """
    Returns the size of the file represented by the file descriptor.
    """
    try:
        st = rposix_stat.fstat(fd)
        return st.st_size
    except OSError:
        raise MMapError()
