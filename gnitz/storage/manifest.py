import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix, rposix_stat
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import errors
from gnitz.storage import mmap_posix, engine_ffi
from gnitz.storage.metadata import ManifestEntry

# On-disk entry size (must match Rust ManifestEntryRaw: 208 bytes)
ENTRY_SIZE = 208
# Maximum entries we support reading at once
MAX_ENTRIES = 4096


def _pack_entries_to_buf(entries):
    """Pack a list of ManifestEntry objects into a flat C buffer of 208-byte structs."""
    count = len(entries)
    buf_size = count * ENTRY_SIZE
    buf = lltype.malloc(rffi.CCHARP.TO, buf_size, flavor="raw")
    i = 0
    while i < count:
        entry = entries[i]
        base = rffi.ptradd(buf, i * ENTRY_SIZE)
        # Zero the entry first
        j = 0
        while j < ENTRY_SIZE:
            base[j] = '\x00'
            j += 1
        # Write fields at known offsets (matching ManifestEntryRaw layout)
        rffi.cast(rffi.ULONGLONGP, base)[0] = rffi.cast(rffi.ULONGLONG, entry.table_id)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 8))[0] = rffi.cast(rffi.ULONGLONG, entry.pk_min_lo)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 16))[0] = rffi.cast(rffi.ULONGLONG, entry.pk_min_hi)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 24))[0] = rffi.cast(rffi.ULONGLONG, entry.pk_max_lo)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 32))[0] = rffi.cast(rffi.ULONGLONG, entry.pk_max_hi)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 40))[0] = rffi.cast(rffi.ULONGLONG, entry.min_lsn)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 48))[0] = rffi.cast(rffi.ULONGLONG, entry.max_lsn)
        # Filename at offset 56, 128 bytes, null-terminated
        fn = entry.shard_filename
        limit = 127 if len(fn) > 127 else len(fn)
        k = 0
        while k < limit:
            base[56 + k] = fn[k]
            k += 1
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 184))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(entry.level))
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 192))[0] = rffi.cast(rffi.ULONGLONG, entry.guard_key_lo)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 200))[0] = rffi.cast(rffi.ULONGLONG, entry.guard_key_hi)
        i += 1
    return buf, count


def _unpack_entries_from_buf(buf, count):
    """Unpack a flat C buffer of 208-byte structs into a list of ManifestEntry objects."""
    entries = newlist_hint(count)
    i = 0
    while i < count:
        base = rffi.ptradd(buf, i * ENTRY_SIZE)
        tid = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, base)[0])
        min_lo = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 8))[0]
        min_hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 16))[0]
        max_lo = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 24))[0]
        max_hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 32))[0]
        min_k = (r_uint128(min_hi) << 64) | r_uint128(min_lo)
        max_k = (r_uint128(max_hi) << 64) | r_uint128(max_lo)
        min_l = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 40))[0]
        max_l = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 48))[0]

        fn_chars = newlist_hint(128)
        for j in range(128):
            if base[56 + j] == '\x00':
                break
            fn_chars.append(base[56 + j])

        level = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 184))[0])
        guard_lo = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 192))[0]
        guard_hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 200))[0]

        entries.append(ManifestEntry(
            tid, "".join(fn_chars), min_k, max_k, r_uint64(min_l), r_uint64(max_l),
            level=level, guard_key_lo=r_uint64(guard_lo), guard_key_hi=r_uint64(guard_hi),
        ))
        i += 1
    return entries


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
            file_size = intmask(st.st_size)
            if file_size < 64:
                raise errors.CorruptShardError("Manifest file too small")

            # Read entire file into buffer
            file_buf = lltype.malloc(rffi.CCHARP.TO, file_size, flavor="raw")
            try:
                bytes_read = mmap_posix.read_into_ptr(new_fd, file_buf, file_size)
                if bytes_read < file_size:
                    raise errors.CorruptShardError("Manifest read truncated")

                # Parse via Rust
                out_lsn = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
                out_entries = lltype.malloc(rffi.CCHARP.TO, MAX_ENTRIES * ENTRY_SIZE, flavor="raw")
                try:
                    rc = engine_ffi._manifest_parse(
                        file_buf, rffi.cast(rffi.LONGLONG, file_size),
                        out_entries, rffi.cast(rffi.UINT, MAX_ENTRIES),
                        out_lsn,
                    )
                    rc_int = intmask(rc)
                    if rc_int == -1:
                        raise errors.CorruptShardError("Manifest magic number mismatch")
                    elif rc_int < 0:
                        raise errors.CorruptShardError("Manifest file truncated or corrupt")
                    self._cached_entries = _unpack_entries_from_buf(out_entries, rc_int)
                    self.entry_count = rc_int
                    self.global_max_lsn = r_uint64(out_lsn[0])
                finally:
                    lltype.free(out_lsn, flavor="raw")
                    lltype.free(out_entries, flavor="raw")
            finally:
                lltype.free(file_buf, flavor="raw")

            if self.fd != -1:
                rposix.close(self.fd)

            self.fd = new_fd
            self.last_inode = rffi.cast(rffi.ULONGLONG, st.st_ino)
            self.last_mtime = st.st_mtime
        except Exception:
            if new_fd != -1:
                rposix.close(new_fd)
            raise

    def has_changed(self):
        try:
            st = rposix_stat.stat(self.filename)
            return rffi.cast(rffi.ULONGLONG, st.st_ino) != self.last_inode or \
                   st.st_mtime != self.last_mtime
        except OSError:
            return True

    def iterate_entries(self):
        for e in self._cached_entries:
            yield e

    def close(self):
        if self.fd != -1:
            rposix.close(self.fd)
            self.fd = -1


class ManifestManager(object):
    def __init__(self, path):
        self.path = path

    def exists(self):
        return os.path.exists(self.path)

    def load_current(self):
        return ManifestReader(self.path)

    def publish_new_version(self, entries, global_max_lsn=r_uint64(0)):
        tmp = self.path + ".tmp"
        count = len(entries)

        # Pack entries into flat C buffer
        entries_buf, _ = _pack_entries_to_buf(entries)
        try:
            # Compute total size and allocate output
            total_size = 64 + count * ENTRY_SIZE
            file_buf = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
            try:
                written = engine_ffi._manifest_serialize(
                    file_buf, rffi.cast(rffi.LONGLONG, total_size),
                    entries_buf, rffi.cast(rffi.UINT, count),
                    rffi.cast(rffi.ULONGLONG, global_max_lsn),
                )
                written_int = intmask(written)
                if written_int < 0:
                    raise errors.StorageError("Manifest serialize failed")

                fd = rposix.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                try:
                    mmap_posix.write_all(
                        fd, file_buf, rffi.cast(rffi.SIZE_T, written_int)
                    )
                    mmap_posix.fdatasync_c(fd)
                finally:
                    rposix.close(fd)
            finally:
                lltype.free(file_buf, flavor="raw")
        finally:
            lltype.free(entries_buf, flavor="raw")

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
