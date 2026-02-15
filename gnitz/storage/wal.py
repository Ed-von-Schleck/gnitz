import os
import errno
from rpython.rlib import rposix, rposix_stat
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal_format, mmap_posix, errors

class WALReader(object):
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
        if self.closed: 
            return None
            
        h_str = rposix.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
        if not h_str or len(h_str) < wal_format.WAL_BLOCK_HEADER_SIZE:
            if self._has_rotated():
                self._open_file()
                h_str = rposix.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
                if not h_str or len(h_str) < wal_format.WAL_BLOCK_HEADER_SIZE:
                    return None
            else:
                return None
            
        total_size = 0
        with rffi.scoped_str2charp(h_str) as h_p:
            total_size = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(h_p, 16))[0])
            
        if total_size < wal_format.WAL_BLOCK_HEADER_SIZE:
            raise errors.CorruptShardError("Invalid WAL block size in header")

        f_ptr = lltype.malloc(rffi.CCHARP.TO, total_size, flavor='raw')
        try:
            for i in range(wal_format.WAL_BLOCK_HEADER_SIZE): 
                f_ptr[i] = h_str[i]
            
            body_sz = total_size - wal_format.WAL_BLOCK_HEADER_SIZE
            if body_sz > 0:
                b_str = rposix.read(self.fd, body_sz)
                if not b_str or len(b_str) < body_sz: 
                    return None
                for i in range(body_sz): 
                    f_ptr[wal_format.WAL_BLOCK_HEADER_SIZE + i] = b_str[i]
            
            return wal_format.decode_wal_block(f_ptr, total_size, self.schema)
        finally:
            lltype.free(f_ptr, flavor='raw')

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
    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.closed = False
        self.fd = -1
        fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            if not mmap_posix.try_lock_exclusive(fd): 
                raise errors.StorageError("WAL file is locked by another process")
            self.fd = fd
        except Exception:
            if fd != -1: 
                rposix.close(fd)
            raise
    
    def append_block(self, lsn, table_id, records):
        if self.closed: 
            raise errors.StorageError("Attempted to write to a closed WAL")
        wal_format.write_wal_block(self.fd, lsn, table_id, records, self.schema)
        mmap_posix.fsync_c(self.fd)

    def truncate_before_lsn(self, lsn):
        if self.closed or self.fd == -1:
            return
        
        mmap_posix.fsync_c(self.fd)
        # We use rposix.ftruncate to safely reset the WAL file size.
        # This allows subsequent append operations to start from the file beginning.
        rposix.ftruncate(self.fd, 0)
        # Move the file pointer back to the start.
        rposix.lseek(self.fd, 0, 0)

    def close(self):
        if not self.closed:
            if self.fd != -1: 
                mmap_posix.fsync_c(self.fd)
                mmap_posix.unlock_file(self.fd)
                rposix.close(self.fd)
                self.fd = -1
            self.closed = True
