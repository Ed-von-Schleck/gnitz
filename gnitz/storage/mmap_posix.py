import os
import errno
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib import rposix, jit

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

eci = ExternalCompilationInfo(includes=['sys/mman.h', 'unistd.h', 'sys/file.h'])

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

_flock = rffi.llexternal(
    "flock",
    [rffi.INT, rffi.INT],
    rffi.INT,
    compilation_info=eci,
    _nowrapper=True
)

# ============================================================================
# Public High-Level Wrappers (Handle Casting and JIT)
# ============================================================================

class MMapError(Exception):
    pass


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
    """
    Wraps munmap.
    """
    res = _munmap(ptr, rffi.cast(rffi.SIZE_T, length))
    if rffi.cast(lltype.Signed, res) == -1:
        raise MMapError()

@jit.dont_look_inside
def write_c(fd, ptr, length):
    """
    Public wrapper for the C 'write' function. 
    Matches the name used in wal_format.py and writer_table.py.
    """
    return _write(rffi.cast(rffi.INT, fd), ptr, rffi.cast(rffi.SIZE_T, length))

@jit.dont_look_inside
def fsync_c(fd):
    """
    Public wrapper for the C 'fsync' function.
    Matches the name used in wal_format.py and writer_table.py.
    """
    res = _fsync(rffi.cast(rffi.INT, fd))
    return rffi.cast(lltype.Signed, res)

@jit.dont_look_inside
def fsync_dir(filepath):
    """
    Fsyncs the directory containing the given file to ensure metadata durability.
    """
    last_slash = filepath.rfind('/')
    dirname = "." if last_slash <= 0 else filepath[:last_slash]
    try:
        fd = rposix.open(dirname, os.O_RDONLY, 0)
        try:
            # Call our wrapper to handle casting
            fsync_c(fd)
        finally:
            rposix.close(fd)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise e

@jit.dont_look_inside
def try_lock_exclusive(fd):
    """
    Attempts to acquire an exclusive lock without blocking.
    """
    res = _flock(rffi.cast(rffi.INT, fd), 
                 rffi.cast(rffi.INT, LOCK_EX | LOCK_NB))
    return rffi.cast(lltype.Signed, res) == 0

@jit.dont_look_inside
def lock_shared(fd):
    """
    Acquires a shared lock (blocking).
    """
    _flock(rffi.cast(rffi.INT, fd), 
           rffi.cast(rffi.INT, LOCK_SH))

@jit.dont_look_inside
def unlock_file(fd):
    """
    Releases a lock.
    """
    _flock(rffi.cast(rffi.INT, fd), 
           rffi.cast(rffi.INT, LOCK_UN))
