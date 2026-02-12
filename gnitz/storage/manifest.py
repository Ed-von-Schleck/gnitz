import os
import errno
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import errors, mmap_posix

MAGIC_NUMBER = r_uint64(0x4D414E49464E5447)
VERSION = 2
HEADER_SIZE = 64
ENTRY_SIZE = 184 

OFF_MAGIC = 0
OFF_VERSION = 8
OFF_ENTRY_COUNT = 16
OFF_GLOBAL_MAX_LSN = 24

OFF_TABLE_ID = 0
OFF_MIN_KEY = 8   
OFF_MAX_KEY = 24  
OFF_MIN_LSN = 40
OFF_MAX_LSN = 48
OFF_FILENAME = 56
FILENAME_MAX_LEN = 128

class ManifestEntry(object):
    _immutable_fields_ = ['table_id', 'shard_filename', 'min_key', 'max_key', 'min_lsn', 'max_lsn']
    def __init__(self, table_id, shard_filename, min_key, max_key, min_lsn, max_lsn):
        self.table_id = table_id
        self.shard_filename = shard_filename
        self.min_key = r_uint128(min_key)
        self.max_key = r_uint128(max_key)
        self.min_lsn = r_uint64(min_lsn)
        self.max_lsn = r_uint64(max_lsn)

def _write_manifest_header(fd, entry_count, global_max_lsn=r_uint64(0)):
    header = lltype.malloc(rffi.CCHARP.TO, HEADER_SIZE, flavor='raw')
    try:
        for i in range(HEADER_SIZE): header[i] = '\x00'
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_MAGIC))[0] = MAGIC_NUMBER
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_VERSION))[0] = rffi.cast(rffi.ULONGLONG, VERSION)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_ENTRY_COUNT))[0] = rffi.cast(rffi.ULONGLONG, entry_count)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_GLOBAL_MAX_LSN))[0] = rffi.cast(rffi.ULONGLONG, global_max_lsn)
        mmap_posix.write_c(fd, header, rffi.cast(rffi.SIZE_T, HEADER_SIZE))
    finally: lltype.free(header, flavor='raw')

def _read_manifest_header(fd):
    header = lltype.malloc(rffi.CCHARP.TO, HEADER_SIZE, flavor='raw')
    try:
        res = rposix.read(fd, HEADER_SIZE)
        if len(res) != HEADER_SIZE: raise errors.CorruptShardError("Manifest header too short")
        for i in range(HEADER_SIZE): header[i] = res[i]
        
        magic_val = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_MAGIC))[0]
        if magic_val != MAGIC_NUMBER: raise errors.CorruptShardError("Magic mismatch")
        
        version = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_VERSION))[0])
        entry_count = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_ENTRY_COUNT))[0])
        global_max_lsn = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_GLOBAL_MAX_LSN))[0]
        return version, entry_count, r_uint64(global_max_lsn)
    finally: lltype.free(header, flavor='raw')

def _write_manifest_entry(fd, entry):
    entry_buf = lltype.malloc(rffi.CCHARP.TO, ENTRY_SIZE, flavor='raw')
    try:
        for i in range(ENTRY_SIZE): entry_buf[i] = '\x00'
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_TABLE_ID))[0] = rffi.cast(rffi.ULONGLONG, entry.table_id)
        
        mask = r_uint128(0xFFFFFFFFFFFFFFFF)
        min_lo = r_uint64(entry.min_key & mask)
        min_hi = r_uint64((entry.min_key >> 64) & mask)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_KEY))[0] = rffi.cast(rffi.ULONGLONG, min_lo)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_KEY + 8))[0] = rffi.cast(rffi.ULONGLONG, min_hi)
        
        max_lo = r_uint64(entry.max_key & mask)
        max_hi = r_uint64((entry.max_key >> 64) & mask)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_KEY))[0] = rffi.cast(rffi.ULONGLONG, max_lo)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_KEY + 8))[0] = rffi.cast(rffi.ULONGLONG, max_hi)
        
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_LSN))[0] = rffi.cast(rffi.ULONGLONG, entry.min_lsn)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_LSN))[0] = rffi.cast(rffi.ULONGLONG, entry.max_lsn)
        
        fn = entry.shard_filename
        fn_len = len(fn)
        if fn_len > FILENAME_MAX_LEN - 1: fn_len = FILENAME_MAX_LEN - 1
        for i in range(fn_len): entry_buf[OFF_FILENAME + i] = fn[i]
        mmap_posix.write_c(fd, entry_buf, rffi.cast(rffi.SIZE_T, ENTRY_SIZE))
    finally: lltype.free(entry_buf, flavor='raw')

def _read_manifest_entry(fd):
    entry_buf = lltype.malloc(rffi.CCHARP.TO, ENTRY_SIZE, flavor='raw')
    try:
        res = rposix.read(fd, ENTRY_SIZE)
        if len(res) < ENTRY_SIZE: return None
        for i in range(ENTRY_SIZE): entry_buf[i] = res[i]
        
        tid = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_TABLE_ID))[0])
        
        min_ptr = rffi.ptradd(entry_buf, OFF_MIN_KEY)
        min_low = rffi.cast(rffi.ULONGLONGP, min_ptr)[0]
        min_high = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(min_ptr, 8))[0]
        min_key = (r_uint128(min_high) << 64) | r_uint128(min_low)
        
        max_ptr = rffi.ptradd(entry_buf, OFF_MAX_KEY)
        max_low = rffi.cast(rffi.ULONGLONGP, max_ptr)[0]
        max_high = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(max_ptr, 8))[0]
        max_key = (r_uint128(max_high) << 64) | r_uint128(max_low)
        
        min_l = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_LSN))[0]
        max_l = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_LSN))[0]
        
        fn_chars = []
        for i in range(FILENAME_MAX_LEN):
            if entry_buf[OFF_FILENAME + i] == '\x00': break
            fn_chars.append(entry_buf[OFF_FILENAME + i])
        return ManifestEntry(tid, "".join(fn_chars), min_key, max_key, r_uint64(min_l), r_uint64(max_l))
    finally: lltype.free(entry_buf, flavor='raw')

class ManifestReader(object):
    def __init__(self, filename):
        self.filename = filename
        fd = rposix.open(filename, os.O_RDONLY, 0)
        try:
            st = os.fstat(fd)
            version, entry_count, global_max_lsn = _read_manifest_header(fd)
            
            self.fd = fd
            self.last_inode = st.st_ino
            self.last_mtime = st.st_mtime
            self.version = version
            self.entry_count = entry_count
            self.global_max_lsn = global_max_lsn
        except Exception:
            rposix.close(fd)
            raise

    def has_changed(self):
        try:
            st = os.stat(self.filename)
            if st.st_ino != self.last_inode or st.st_mtime != self.last_mtime:
                return True
        except OSError as e:
            if e.errno == errno.ENOENT:
                return True
            raise e
        return False

    def reload(self):
        fd = rposix.open(self.filename, os.O_RDONLY, 0)
        try:
            st = os.fstat(fd)
            version, entry_count, global_max_lsn = _read_manifest_header(fd)
            
            rposix.close(self.fd)
            
            self.fd = fd
            self.last_inode = st.st_ino
            self.last_mtime = st.st_mtime
            self.version = version
            self.entry_count = entry_count
            self.global_max_lsn = global_max_lsn
        except Exception:
            rposix.close(fd)
            raise

    def get_entry_count(self): return self.entry_count
    
    def iterate_entries(self):
        rposix.lseek(self.fd, HEADER_SIZE, 0)
        for _ in range(self.entry_count):
            entry = _read_manifest_entry(self.fd)
            if entry: yield entry
            
    def close(self):
        if self.fd != -1:
            rposix.close(self.fd)
            self.fd = -1

class ManifestWriter(object):
    def __init__(self, filename, global_max_lsn=r_uint64(0)):
        self.filename = filename
        self.entries = []
        self.global_max_lsn = r_uint64(global_max_lsn)
        self.finalized = False
    
    def add_entry_obj(self, entry):
        if self.finalized: raise errors.StorageError("Manifest already finalized")
        self.entries.append(entry)

    def add_entry_values(self, table_id, shard_filename, min_key, max_key, min_lsn, max_lsn):
        if self.finalized: raise errors.StorageError("Manifest already finalized")
        entry = ManifestEntry(table_id, shard_filename, min_key, max_key, min_lsn, max_lsn)
        self.entries.append(entry)

    def finalize(self):
        if self.finalized: raise errors.StorageError("Manifest already finalized")
        fd = rposix.open(self.filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            _write_manifest_header(fd, len(self.entries), self.global_max_lsn)
            for e in self.entries:
                _write_manifest_entry(fd, e)
            mmap_posix.fsync_c(fd)
        finally:
            rposix.close(fd)
        self.finalized = True

class ManifestManager(object):
    def __init__(self, manifest_path):
        self.manifest_path = manifest_path
        self.temp_path = manifest_path + ".tmp"
    def exists(self): return os.path.exists(self.manifest_path)
    def load_current(self): return ManifestReader(self.manifest_path)
    
    def publish_new_version(self, entries, global_max_lsn=r_uint64(0)):
        writer = ManifestWriter(self.temp_path, global_max_lsn)
        for e in entries:
            writer.add_entry_obj(e)
        writer.finalize()
        os.rename(self.temp_path, self.manifest_path)
        mmap_posix.fsync_dir(self.manifest_path)
