from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import errors, mmap_posix
import os

MAGIC_NUMBER = r_uint64(0x4D414E49464E5447)
VERSION = 1
HEADER_SIZE = 64
ENTRY_SIZE = 168

OFF_MAGIC = 0
OFF_VERSION = 8
OFF_ENTRY_COUNT = 16
OFF_GLOBAL_MAX_LSN = 24

OFF_COMPONENT_ID = 0
OFF_MIN_ENTITY_ID = 8
OFF_MAX_ENTITY_ID = 16
OFF_MIN_LSN = 24
OFF_MAX_LSN = 32
OFF_FILENAME = 40
FILENAME_MAX_LEN = 128

class ManifestEntry(object):
    _immutable_fields_ = ['component_id', 'shard_filename', 'min_entity_id', 'max_entity_id', 'min_lsn', 'max_lsn']
    def __init__(self, component_id, shard_filename, min_eid, max_eid, min_lsn, max_lsn):
        self.component_id = component_id
        self.shard_filename = shard_filename
        self.min_entity_id = min_eid
        self.max_entity_id = max_eid
        self.min_lsn = min_lsn
        self.max_lsn = max_lsn

def _write_manifest_header(fd, entry_count, global_max_lsn=r_uint64(0)):
    header = lltype.malloc(rffi.CCHARP.TO, HEADER_SIZE, flavor='raw')
    try:
        for i in range(HEADER_SIZE): header[i] = '\x00'
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_MAGIC))[0] = rffi.cast(rffi.ULONGLONG, MAGIC_NUMBER)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_VERSION))[0] = rffi.cast(rffi.ULONGLONG, VERSION)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_ENTRY_COUNT))[0] = rffi.cast(rffi.ULONGLONG, entry_count)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_GLOBAL_MAX_LSN))[0] = rffi.cast(rffi.ULONGLONG, global_max_lsn)
        os.write(fd, rffi.charpsize2str(header, HEADER_SIZE))
    finally: lltype.free(header, flavor='raw')

def _read_manifest_header(fd):
    header = lltype.malloc(rffi.CCHARP.TO, HEADER_SIZE, flavor='raw')
    try:
        read_bytes = os.read(fd, HEADER_SIZE)
        if len(read_bytes) != HEADER_SIZE: raise errors.CorruptShardError("Manifest header too short")
        for i in range(HEADER_SIZE): header[i] = read_bytes[i]
        magic_val = rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_MAGIC))[0])
        if magic_val != rffi.cast(rffi.ULONGLONG, MAGIC_NUMBER): 
            raise errors.CorruptShardError("Magic mismatch")
        version = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_VERSION))[0])
        entry_count = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_ENTRY_COUNT))[0])
        global_max_lsn = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, OFF_GLOBAL_MAX_LSN))[0])
        return version, entry_count, global_max_lsn
    finally: lltype.free(header, flavor='raw')

def _write_manifest_entry(fd, entry):
    entry_buf = lltype.malloc(rffi.CCHARP.TO, ENTRY_SIZE, flavor='raw')
    try:
        for i in range(ENTRY_SIZE): entry_buf[i] = '\x00'
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_COMPONENT_ID))[0] = rffi.cast(rffi.ULONGLONG, entry.component_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_ENTITY_ID))[0] = rffi.cast(rffi.ULONGLONG, entry.min_entity_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_ENTITY_ID))[0] = rffi.cast(rffi.ULONGLONG, entry.max_entity_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_LSN))[0] = rffi.cast(rffi.ULONGLONG, entry.min_lsn)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_LSN))[0] = rffi.cast(rffi.ULONGLONG, entry.max_lsn)
        fn = entry.shard_filename
        fn_len = len(fn)
        if fn_len > FILENAME_MAX_LEN - 1: fn_len = FILENAME_MAX_LEN - 1
        for i in range(fn_len): entry_buf[OFF_FILENAME + i] = fn[i]
        os.write(fd, rffi.charpsize2str(entry_buf, ENTRY_SIZE))
    finally: lltype.free(entry_buf, flavor='raw')

def _read_manifest_entry(fd):
    entry_buf = lltype.malloc(rffi.CCHARP.TO, ENTRY_SIZE, flavor='raw')
    try:
        read_bytes = os.read(fd, ENTRY_SIZE)
        if len(read_bytes) < ENTRY_SIZE: return None
        for i in range(ENTRY_SIZE): entry_buf[i] = read_bytes[i]
        cid = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_COMPONENT_ID))[0])
        min_e = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_ENTITY_ID))[0])
        max_e = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_ENTITY_ID))[0])
        min_l = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_LSN))[0])
        max_l = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_LSN))[0])
        fn_chars = []
        for i in range(FILENAME_MAX_LEN):
            if entry_buf[OFF_FILENAME + i] == '\x00': break
            fn_chars.append(entry_buf[OFF_FILENAME + i])
        return ManifestEntry(cid, "".join(fn_chars), min_e, max_e, min_l, max_l)
    finally: lltype.free(entry_buf, flavor='raw')

class ManifestReader(object):
    def __init__(self, filename):
        self.filename = filename
        self.fd = rposix.open(filename, os.O_RDONLY, 0)
        self.version, self.entry_count, self.global_max_lsn = _read_manifest_header(self.fd)
        st = os.fstat(self.fd)
        self.last_mtime = st.st_mtime

    def has_changed(self):
        try:
            st = os.stat(self.filename)
            return st.st_mtime > self.last_mtime
        except OSError:
            return False

    def reload(self):
        rposix.close(self.fd)
        self.fd = rposix.open(self.filename, os.O_RDONLY, 0)
        self.version, self.entry_count, self.global_max_lsn = _read_manifest_header(self.fd)
        st = os.fstat(self.fd)
        self.last_mtime = st.st_mtime

    def get_entry_count(self): return self.entry_count

    def iterate_entries(self):
        rposix.lseek(self.fd, HEADER_SIZE, 0)
        for _ in range(self.entry_count):
            yield _read_manifest_entry(self.fd)

    def close(self): rposix.close(self.fd)

class ManifestWriter(object):
    def __init__(self, filename, global_max_lsn=r_uint64(0)):
        self.filename = filename; self.entries = []; self.global_max_lsn = global_max_lsn; self.finalized = False
    
    def add_entry(self, component_id, shard_filename, min_eid, max_eid, min_lsn, max_lsn):
        if self.finalized: raise errors.StorageError()
        self.entries.append(ManifestEntry(component_id, shard_filename, min_eid, max_eid, min_lsn, max_lsn))
    
    def finalize(self):
        fd = rposix.open(self.filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            _write_manifest_header(fd, len(self.entries), self.global_max_lsn)
            for e in self.entries: _write_manifest_entry(fd, e)
        finally: rposix.close(fd)
        self.finalized = True

class ManifestManager(object):
    def __init__(self, manifest_path):
        self.manifest_path = manifest_path
        self.temp_path = manifest_path + ".tmp"
    def exists(self): return os.path.exists(self.manifest_path)
    def load_current(self): return ManifestReader(self.manifest_path)
    def publish_new_version(self, entries, global_max_lsn=r_uint64(0)):
        writer = ManifestWriter(self.temp_path, global_max_lsn)
        for e in entries: writer.add_entry(e.component_id, e.shard_filename, e.min_entity_id, e.max_entity_id, e.min_lsn, e.max_lsn)
        writer.finalize()
        os.rename(self.temp_path, self.manifest_path)
        mmap_posix.fsync_dir(self.manifest_path)
