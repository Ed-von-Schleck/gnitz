# gnitz/storage/wal.py

import os
import errno
from rpython.rlib import rposix, rposix_stat
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from gnitz.core import errors
from gnitz.storage import wal_columnar, mmap_posix, buffer as buffer_ops, wal_layout


class WALReader(object):
    """
    Reader for the columnar Z-Set Write-Ahead Log.
    Each block is a serialized ArenaZSetBatch.
    """

    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.fd = -1
        self.last_inode = rffi.cast(rffi.ULONGLONG, 0)
        self.closed = False
        self._open_file()

    def _open_file(self):
        new_fd = rposix.open(self.filename, os.O_RDONLY, 0)
        try:
            st = rposix_stat.fstat(new_fd)
            if self.fd != -1:
                rposix.close(self.fd)
            self.fd = new_fd
            self.last_inode = rffi.cast(rffi.ULONGLONG, st.st_ino)
        except Exception:
            if new_fd != -1:
                rposix.close(new_fd)
            raise

    def _has_rotated(self):
        try:
            st = rposix_stat.stat(self.filename)
            return rffi.cast(rffi.ULONGLONG, st.st_ino) != self.last_inode
        except OSError as e:
            if e.errno == errno.ENOENT:
                return False
            raise e

    def read_next_block(self):
        """
        Reads the next columnar block from the WAL.
        Returns a WALColumnarBlock, or None at EOF.
        """
        if self.closed:
            return None

        h_str = rposix.read(self.fd, wal_layout.WAL_BLOCK_HEADER_SIZE)
        if not h_str or len(h_str) < wal_layout.WAL_BLOCK_HEADER_SIZE:
            if self._has_rotated():
                self._open_file()
                h_str = rposix.read(self.fd, wal_layout.WAL_BLOCK_HEADER_SIZE)
                if not h_str or len(h_str) < wal_layout.WAL_BLOCK_HEADER_SIZE:
                    return None
            else:
                return None

        total_size = 0
        with rffi.scoped_str2charp(h_str) as h_p:
            header = wal_layout.WALBlockHeaderView(h_p)
            total_size = header.get_total_size()

        if total_size < wal_layout.WAL_BLOCK_HEADER_SIZE:
            raise errors.CorruptShardError("Invalid WAL block size in header")

        f_ptr = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
        success = False
        try:
            for i in range(wal_layout.WAL_BLOCK_HEADER_SIZE):
                f_ptr[i] = h_str[i]

            body_sz = total_size - wal_layout.WAL_BLOCK_HEADER_SIZE
            if body_sz > 0:
                b_str = rposix.read(self.fd, body_sz)
                if not b_str or len(b_str) < body_sz:
                    return None
                for i in range(body_sz):
                    f_ptr[wal_layout.WAL_BLOCK_HEADER_SIZE + i] = b_str[i]

            res = wal_columnar.decode_batch_from_buffer(
                f_ptr, total_size, self.schema
            )
            success = True
            return res
        finally:
            if not success:
                lltype.free(f_ptr, flavor="raw")

    def iterate_blocks(self):
        while True:
            block = self.read_next_block()
            if block is None:
                break
            yield block

    def close(self):
        if not self.closed:
            if self.fd != -1:
                rposix.close(self.fd)
                self.fd = -1
            self.closed = True


class WALWriter(object):
    """
    Append-only writer for the columnar Z-Set WAL.
    Natively serializes ArenaZSetBatch buffers for zero-copy durability.
    """

    _immutable_fields_ = ["filename", "schema", "block_buf"]

    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.closed = False
        self.fd = -1
        self.block_buf = None

        fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            if not mmap_posix.try_lock_exclusive(fd):
                raise errors.StorageError("WAL file is locked by another process")
            self.fd = fd
            self.block_buf = buffer_ops.Buffer(32 * 1024 * 1024)
        except Exception:
            if fd != -1:
                rposix.close(fd)
            if self.block_buf:
                self.block_buf.free()
            raise

    def append_batch(self, lsn, table_id, batch):
        """
        Serializes an ArenaZSetBatch into the WAL as a single columnar block.
        """
        if self.closed:
            raise errors.StorageError("Attempted to write to a closed WAL")

        num_records = batch.length()
        if num_records == 0:
            return

        wal_columnar.encode_batch_to_buffer(
            self.block_buf, self.schema, lsn, table_id, batch
        )
        mmap_posix.write_c(
            self.fd, self.block_buf.base_ptr,
            rffi.cast(rffi.SIZE_T, self.block_buf.offset)
        )
        mmap_posix.fsync_c(self.fd)

    def truncate_before_lsn(self, lsn):
        if self.closed or self.fd == -1:
            return
        mmap_posix.fsync_c(self.fd)
        rposix.ftruncate(self.fd, 0)
        rposix.lseek(self.fd, 0, 0)

    def close(self):
        if not self.closed:
            if self.fd != -1:
                mmap_posix.fsync_c(self.fd)
                mmap_posix.unlock_file(self.fd)
                rposix.close(self.fd)
                self.fd = -1
            if self.block_buf:
                self.block_buf.free()
                self.block_buf = None
            self.closed = True
