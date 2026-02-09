from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib import rposix
import os

# ============================================================================
# POSIX Constants
# ============================================================================

PROT_READ       = 0x1
PROT_WRITE      = 0x2
MAP_SHARED      = 0x01
MAP_PRIVATE     = 0x02

eci = ExternalCompilationInfo(includes=['sys/mman.h', 'unistd.h'])

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
    """
    Opens the parent directory of filepath and calls fsync on it.
    Crucial for ensuring the rename() is durable.
    """
    # Manual directory extraction to avoid rposix/os.path slicing issues in RPython
    last_slash = filepath.rfind('/')
    if last_slash <= 0:
        dirname = "."
    else:
        dirname = filepath[:last_slash]
    
    try:
        fd = rposix.open(dirname, os.O_RDONLY, 0)
        fsync_c(fd)
        rposix.close(fd)
    except OSError:
        pass # Some filesystems don't support dir fsync
