import os
import errno
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib import rposix, jit, rposix_stat

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
    includes=['sys/mman.h', 'unistd.h', 'sys/file.h', 'sys/stat.h']
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

# ============================================================================
# Public High-Level Wrappers
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
    res = _munmap(ptr, rffi.cast(rffi.SIZE_T, length))
    if rffi.cast(lltype.Signed, res) == -1:
        raise MMapError()

@jit.dont_look_inside
def write_c(fd, ptr, length):
    return _write(rffi.cast(rffi.INT, fd), ptr, rffi.cast(rffi.SIZE_T, length))

@jit.dont_look_inside
def fsync_c(fd):
    res = _fsync(rffi.cast(rffi.INT, fd))
    return rffi.cast(lltype.Signed, res)

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
def fget_size(fd):
    """
    Returns the size of the file represented by the file descriptor.
    """
    try:
        st = rposix_stat.fstat(fd)
        return st.st_size
    except OSError:
        raise MMapError()
