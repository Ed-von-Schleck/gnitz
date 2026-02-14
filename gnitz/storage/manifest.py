import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix, rposix_stat
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import errors, mmap_posix, layout

MAGIC_NUMBER = r_uint64(0x4D414E49464E5447)
VERSION = 2
HEADER_SIZE = 64
ENTRY_SIZE = 184 

class ManifestEntry(object):
    """Authoritative shard metadata record."""
    _immutable_fields_ = [
        'table_id', 'shard_filename', 'pk_min_lo', 'pk_min_hi', 
        'pk_max_lo', 'pk_max_hi', 'min_lsn', 'max_lsn'
    ]
    def __init__(self, table_id, shard_filename, min_key, max_key, min_lsn, max_lsn):
        self.table_id = table_id
        self.shard_filename = shard_filename
        mk = r_uint128(min_key)
        xk = r_uint128(max_key)
        self.pk_min_lo = r_uint64(mk)
        self.pk_min_hi = r_uint64(mk >> 64)
        self.pk_max_lo = r_uint64(xk)
        self.pk_max_hi = r_uint64(xk >> 64)
        self.min_lsn = r_uint64(min_lsn)
        self.max_lsn = r_uint64(max_lsn)

    def get_min_key(self):
        return (r_uint128(self.pk_min_hi) << 64) | r_uint128(self.pk_min_lo)
    
    def get_max_key(self):
        return (r_uint128(self.pk_max_hi) << 64) | r_uint128(self.pk_max_lo)

def _write_manifest_header(fd, count, max_lsn):
    """Writes the 64-byte Manifest authority header."""
    buf = lltype.malloc(rffi.CCHARP.TO, HEADER_SIZE, flavor='raw')
    try:
        # Initialize buffer to zero
        for i in range(HEADER_SIZE): buf[i] = '\x00'
        
        # [00-07] Magic Number
        rffi.cast(rffi.ULONGLONGP, buf)[0] = MAGIC_NUMBER
        # [08-15] Version
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 8))[0] = rffi.cast(rffi.ULONGLONG, VERSION)
        # [16-23] Entry Count
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 16))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(count))
        # [24-31] Global Max LSN
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 24))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(max_lsn))
        
        if mmap_posix.write_c(fd, buf, rffi.cast(rffi.SIZE_T, HEADER_SIZE)) < HEADER_SIZE:
            raise errors.StorageError("Failed to write manifest header")
    finally:
        lltype.free(buf, flavor='raw')

def _read_manifest_header(fd):
    """Reads and validates the 64-byte Manifest authority header."""
    res = rposix.read(fd, HEADER_SIZE)
    if len(res) < HEADER_SIZE:
        raise errors.CorruptShardError("Manifest header truncated or missing")
    
    with rffi.scoped_str2charp(res) as buf:
        if rffi.cast(rffi.ULONGLONGP, buf)[0] != MAGIC_NUMBER:
            raise errors.CorruptShardError("Manifest magic number mismatch")
        
        v = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 8))[0])
        count = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 16))[0])
        lsn = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 24))[0]
        
        return v, count, r_uint64(lsn)

def _write_manifest_entry(fd, entry):
    entry_buf = lltype.malloc(rffi.CCHARP.TO, ENTRY_SIZE, flavor='raw')
    try:
        for i in range(ENTRY_SIZE): entry_buf[i] = '\x00'
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, 0))[0] = rffi.cast(rffi.ULONGLONG, entry.table_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, 8))[0] = rffi.cast(rffi.ULONGLONG, entry.pk_min_lo)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, 16))[0] = rffi.cast(rffi.ULONGLONG, entry.pk_min_hi)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, 24))[0] = rffi.cast(rffi.ULONGLONG, entry.pk_max_lo)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, 32))[0] = rffi.cast(rffi.ULONGLONG, entry.pk_max_hi)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, 40))[0] = rffi.cast(rffi.ULONGLONG, entry.min_lsn)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, 48))[0] = rffi.cast(rffi.ULONGLONG, entry.max_lsn)
        
        fn = entry.shard_filename
        limit = 127 if len(fn) > 127 else len(fn)
        for i in range(limit): entry_buf[56 + i] = fn[i]
        mmap_posix.write_c(fd, entry_buf, rffi.cast(rffi.SIZE_T, ENTRY_SIZE))
    finally:
        lltype.free(entry_buf, flavor='raw')

def _read_manifest_entry(fd):
    res = rposix.read(fd, ENTRY_SIZE)
    if len(res) < ENTRY_SIZE: return None
    with rffi.scoped_str2charp(res) as buf:
        tid = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, buf)[0])
        min_lo = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 8))[0]
        min_hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 16))[0]
        max_lo = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 24))[0]
        max_hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 32))[0]
        
        min_k = (r_uint128(min_hi) << 64) | r_uint128(min_lo)
        max_k = (r_uint128(max_hi) << 64) | r_uint128(max_lo)
        
        min_l = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 40))[0]
        max_l = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 48))[0]
        
        fn_chars = []
        for i in range(128):
            if buf[56 + i] == '\x00': break
            fn_chars.append(buf[56 + i])
        return ManifestEntry(tid, "".join(fn_chars), min_k, max_k, r_uint64(min_l), r_uint64(max_l))

class ManifestReader(object):
    def __init__(self, filename):
        self.filename = filename
        self.fd = -1
        self.last_inode = rffi.cast(rffi.ULONGLONG, 0)
        self.last_mtime = 0.0
        self.version = 0
        self.entry_count = 0
        self.global_max_lsn = r_uint64(0)
        self.reload()

    def reload(self):
        new_fd = rposix.open(self.filename, os.O_RDONLY, 0)
        try:
            st = rposix_stat.fstat(new_fd)
            v, count, lsn = _read_manifest_header(new_fd)
            
            if self.fd != -1: 
                rposix.close(self.fd)
            
            self.fd = new_fd
            self.last_inode = rffi.cast(rffi.ULONGLONG, st.st_ino)
            self.last_mtime = st.st_mtime
            self.version = v
            self.entry_count = count
            self.global_max_lsn = lsn
        except Exception:
            if new_fd != -1: rposix.close(new_fd)
            raise

    def has_changed(self):
        try:
            st = rposix_stat.stat(self.filename)
            return rffi.cast(rffi.ULONGLONG, st.st_ino) != self.last_inode or \
                   st.st_mtime != self.last_mtime
        except OSError:
            return True

    def iterate_entries(self):
        rposix.lseek(self.fd, HEADER_SIZE, 0)
        for _ in range(self.entry_count):
            e = _read_manifest_entry(self.fd)
            if e: yield e
            
    def close(self):
        if self.fd != -1: 
            rposix.close(self.fd)
            self.fd = -1

class ManifestManager(object):
    def __init__(self, path):
        self.path = path
    def exists(self): return os.path.exists(self.path)
    def load_current(self): return ManifestReader(self.path)
    def publish_new_version(self, entries, global_max_lsn=r_uint64(0)):
        tmp = self.path + ".tmp"
        fd = rposix.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            _write_manifest_header(fd, len(entries), global_max_lsn)
            for e in entries: _write_manifest_entry(fd, e)
            mmap_posix.fsync_c(fd)
        finally: rposix.close(fd)
        os.rename(tmp, self.path)

class ManifestWriter(object):
    def __init__(self, filename, global_max_lsn=r_uint64(0)):
        self.filename = filename
        self.global_max_lsn = global_max_lsn
        self.entries = []

    def add_entry(self, tid, fn, min_k, max_k, min_l, max_l):
        self.entries.append(ManifestEntry(tid, fn, min_k, max_k, min_l, max_l))

    def add_entry_obj(self, entry):
        self.entries.append(entry)

    def add_entry_values(self, tid, fn, min_k, max_k, min_l, max_l):
        self.add_entry(tid, fn, min_k, max_k, min_l, max_l)

    def finalize(self):
        ManifestManager(self.filename).publish_new_version(self.entries, self.global_max_lsn)
