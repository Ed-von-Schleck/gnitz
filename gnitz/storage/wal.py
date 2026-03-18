# gnitz/storage/wal.py

import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from gnitz.core import errors
from gnitz.storage import wal_columnar, mmap_posix, buffer as buffer_ops, wal_layout


class WALReader(object):
    """
    Reader for the columnar Z-Set Write-Ahead Log.
    Each block is a serialized ArenaZSetBatch.
    Uses mmap for zero-copy access during static startup recovery.
    """

    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.fd = -1
        self.file_size = 0
        self.ptr = lltype.nullptr(rffi.CCHARP.TO)
        self._off = 0
        self.closed = False
        self._open_file()

    def _open_file(self):
        fd = rposix.open(self.filename, os.O_RDONLY, 0)
        try:
            self.file_size = intmask(mmap_posix.fget_size(fd))
            if self.file_size > 0:
                self.ptr = mmap_posix.mmap_file(
                    fd, self.file_size,
                    prot=mmap_posix.PROT_READ,
                    flags=mmap_posix.MAP_SHARED,
                )
            # file_size == 0: ptr stays nullptr; read_next_block returns None
        except Exception:
            rposix.close(fd)
            raise
        self.fd = fd
        self._off = 0

    def read_next_block(self):
        """
        Reads the next columnar block from the WAL.
        Returns a WALColumnarBlock, or None at EOF.
        """
        if self.closed:
            return None
        if self._off >= self.file_size:
            return None
        if self._off + wal_layout.WAL_BLOCK_HEADER_SIZE > self.file_size:
            raise errors.CorruptShardError("Truncated WAL block header")
        hdr = wal_layout.WALBlockHeaderView(rffi.ptradd(self.ptr, self._off))
        block_size = hdr.get_total_size()
        if block_size < wal_layout.WAL_BLOCK_HEADER_SIZE:
            raise errors.CorruptShardError("Invalid WAL block size in header")
        if self._off + block_size > self.file_size:
            raise errors.CorruptShardError("WAL block extends beyond file")
        lsn = hdr.get_lsn()
        tid = hdr.get_table_id()
        batch = wal_columnar.decode_batch_from_ptr(
            rffi.ptradd(self.ptr, self._off), block_size, self.schema
        )
        self._off += block_size
        return wal_columnar.WALColumnarBlock(lsn, tid, batch,
                                            raw_buf=lltype.nullptr(rffi.CCHARP.TO))

    def iterate_blocks(self):
        while True:
            block = self.read_next_block()
            if block is None:
                break
            yield block

    def close(self):
        if not self.closed:
            if self.ptr:
                mmap_posix.munmap_file(self.ptr, self.file_size)
                self.ptr = lltype.nullptr(rffi.CCHARP.TO)
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
