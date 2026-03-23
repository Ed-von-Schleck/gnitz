import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix, rposix_stat
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import errors
from gnitz.storage import mmap_posix, layout
from gnitz.storage.buffer import c_memset
from gnitz.storage.metadata import ManifestEntry

MAGIC_NUMBER = r_uint64(0x4D414E49464E5447)
VERSION = 3
VERSION_LEGACY = 2
HEADER_SIZE = 64
ENTRY_SIZE_V2 = 184
ENTRY_SIZE = 208   # V3: adds level(8) + guard_key_lo(8) + guard_key_hi(8)

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

def _read_manifest_entry(fd, version):
    stride = ENTRY_SIZE if version >= VERSION else ENTRY_SIZE_V2
    res = rposix.read(fd, stride)
    if len(res) < stride: return None
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

        fn_chars = newlist_hint(128)
        for i in range(128):
            if buf[56 + i] == '\x00': break
            fn_chars.append(buf[56 + i])

        if version >= VERSION:
            level = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 184))[0])
            guard_lo = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 192))[0]
            guard_hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, 200))[0]
        else:
            level = 0
            guard_lo = rffi.cast(rffi.ULONGLONG, r_uint64(0))
            guard_hi = rffi.cast(rffi.ULONGLONG, r_uint64(0))

        return ManifestEntry(tid, "".join(fn_chars), min_k, max_k, r_uint64(min_l), r_uint64(max_l),
                             level=level, guard_key_lo=r_uint64(guard_lo), guard_key_hi=r_uint64(guard_hi))

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
            e = _read_manifest_entry(self.fd, self.version)
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
        count = len(entries)
        total_size = HEADER_SIZE + count * ENTRY_SIZE

        file_buf = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
        try:
            c_memset(
                rffi.cast(rffi.VOIDP, file_buf),
                rffi.cast(rffi.INT, 0),
                rffi.cast(rffi.SIZE_T, total_size),
            )

            rffi.cast(rffi.ULONGLONGP, file_buf)[0] = MAGIC_NUMBER
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(file_buf, 8))[0] = rffi.cast(
                rffi.ULONGLONG, VERSION
            )
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(file_buf, 16))[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(count)
            )
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(file_buf, 24))[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(global_max_lsn)
            )

            i = 0
            while i < count:
                entry = entries[i]
                base = rffi.ptradd(file_buf, HEADER_SIZE + i * ENTRY_SIZE)
                rffi.cast(rffi.ULONGLONGP, base)[0] = rffi.cast(
                    rffi.ULONGLONG, entry.table_id
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 8))[0] = rffi.cast(
                    rffi.ULONGLONG, entry.pk_min_lo
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 16))[0] = rffi.cast(
                    rffi.ULONGLONG, entry.pk_min_hi
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 24))[0] = rffi.cast(
                    rffi.ULONGLONG, entry.pk_max_lo
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 32))[0] = rffi.cast(
                    rffi.ULONGLONG, entry.pk_max_hi
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 40))[0] = rffi.cast(
                    rffi.ULONGLONG, entry.min_lsn
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 48))[0] = rffi.cast(
                    rffi.ULONGLONG, entry.max_lsn
                )
                fn = entry.shard_filename
                limit = 127 if len(fn) > 127 else len(fn)
                j = 0
                while j < limit:
                    base[56 + j] = fn[j]
                    j += 1
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 184))[0] = rffi.cast(
                    rffi.ULONGLONG, r_uint64(entry.level)
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 192))[0] = rffi.cast(
                    rffi.ULONGLONG, entry.guard_key_lo
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 200))[0] = rffi.cast(
                    rffi.ULONGLONG, entry.guard_key_hi
                )
                i += 1

            fd = rposix.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                mmap_posix.write_all(
                    fd, file_buf, rffi.cast(rffi.SIZE_T, total_size)
                )
                mmap_posix.fsync_c(fd)
            finally:
                rposix.close(fd)
        finally:
            lltype.free(file_buf, flavor="raw")

        os.rename(tmp, self.path)

class ManifestWriter(object):
    def __init__(self, filename, global_max_lsn=r_uint64(0)):
        self.filename = filename
        self.global_max_lsn = global_max_lsn
        self.entries = newlist_hint(16)

    def add_entry(self, tid, fn, min_k, max_k, min_l, max_l):
        self.entries.append(ManifestEntry(tid, fn, min_k, max_k, min_l, max_l))

    def add_entry_obj(self, entry):
        self.entries.append(entry)

    def finalize(self):
        ManifestManager(self.filename).publish_new_version(self.entries, self.global_max_lsn)
