import os
import errno
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib import rposix

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
# External C Functions
# ============================================================================

mmap_c = rffi.llexternal(
    "mmap",
    [rffi.CCHARP, rffi.SIZE_T, rffi.INT, rffi.INT, rffi.INT, rffi.LONGLONG],
    rffi.CCHARP,
    compilation_info=eci
)

munmap_c = rffi.llexternal(
    "munmap",
    [rffi.CCHARP, rffi.SIZE_T],
    rffi.INT,
    compilation_info=eci
)

write_c = rffi.llexternal(
    "write",
    [rffi.INT, rffi.CCHARP, rffi.SIZE_T],
    rffi.SSIZE_T,
    compilation_info=eci
)

fsync_c = rffi.llexternal(
    "fsync",
    [rffi.INT],
    rffi.INT,
    compilation_info=eci
)

flock_c = rffi.llexternal(
    "flock",
    [rffi.INT, rffi.INT],
    rffi.INT,
    compilation_info=eci
)

# ============================================================================
# High-Level RPython Wrappers
# ============================================================================

class MMapError(Exception):
    pass

def mmap_file(fd, length, prot=PROT_READ, flags=MAP_SHARED):
    res = mmap_c(lltype.nullptr(rffi.CCHARP.TO), 
                 rffi.cast(rffi.SIZE_T, length),
                 rffi.cast(rffi.INT, prot),
                 rffi.cast(rffi.INT, flags),
                 rffi.cast(rffi.INT, fd),
                 rffi.cast(rffi.LONGLONG, 0))
    if rffi.cast(lltype.Signed, res) == -1:
        raise MMapError()
    return res

def munmap_file(ptr, length):
    res = munmap_c(ptr, rffi.cast(rffi.SIZE_T, length))
    if rffi.cast(lltype.Signed, res) == -1:
        raise MMapError()

def fsync_dir(filepath):
    last_slash = filepath.rfind('/')
    dirname = "." if last_slash <= 0 else filepath[:last_slash]
    try:
        fd = rposix.open(dirname, os.O_RDONLY, 0)
        try:
            fsync_c(fd)
        finally:
            rposix.close(fd)
    except OSError as e:
        # Ignore only if the directory itself is gone.
        if e.errno != errno.ENOENT:
            raise e

def try_lock_exclusive(fd):
    res = flock_c(fd, LOCK_EX | LOCK_NB)
    return rffi.cast(lltype.Signed, res) == 0

def lock_shared(fd):
    flock_c(fd, LOCK_SH)

def unlock_file(fd):
    flock_c(fd, LOCK_UN)
