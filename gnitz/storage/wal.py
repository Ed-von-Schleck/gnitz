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
        """
        Implements the Atomic Resource Initialization pattern to prevent 
        leaking file descriptors on initialization or rotation failure.
        """
        # 1. Open to a local variable first
        new_fd = rposix.open(self.filename, os.O_RDONLY, 0)
        try:
            st = rposix_stat.fstat(new_fd)
            
            # 2. If we are rotating, close the old descriptor safely
            if self.fd != -1:
                rposix.close(self.fd)
                
            # 3. Only assign to self once the resource is ready
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
        Reads the next Z-Set block from the WAL. 
        Uses scoped_str2charp to handle FFI safely and narrow types for the annotator.
        """
        if self.closed: 
            return (False, 0, 0, [])
        
        # rposix.read might return None or a short string. 
        # We must narrow the type to satisfy the RPython annotator.
        header_str = rposix.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
        
        if header_str is None or len(header_str) < wal_format.WAL_BLOCK_HEADER_SIZE:
            # Handle potential file rotation (common in tailing WALs)
            if self._has_rotated():
                self._open_file()
                header_str = rposix.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
                if header_str is None or len(header_str) < wal_format.WAL_BLOCK_HEADER_SIZE:
                    return (False, 0, 0, [])
            else:
                return (False, 0, 0, [])
            
        # At this point, header_str is guaranteed to be SomeString() and not None.
        with rffi.scoped_str2charp(header_str) as header_ptr:
            # Parse block metadata
            total_size_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(header_ptr, 16))
            total_size = rffi.cast(lltype.Signed, total_size_ptr[0])
            body_size = total_size - wal_format.WAL_BLOCK_HEADER_SIZE
            
            if body_size < 0:
                raise errors.CorruptShardError("Invalid WAL block size in header")

            body_str = rposix.read(self.fd, body_size)
            if body_str is None or len(body_str) < body_size: 
                return (False, 0, 0, [])
            
            # Combine for full block decoding
            full_data = header_str + body_str
            with rffi.scoped_str2charp(full_data) as full_ptr:
                lsn, table_id, records = wal_format.decode_wal_block(
                    full_ptr, len(full_data), self.schema
                )
                return (True, lsn, table_id, records)
    
    def iterate_blocks(self):
        while True:
            valid, lsn, tid, records = self.read_next_block()
            if not valid: 
                break
            yield (lsn, tid, records)
    
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
        
        # Atomic resource pattern for the writer
        fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            # Ensure exclusive access for the single-writer process
            if not mmap_posix.try_lock_exclusive(fd):
                raise errors.StorageError("WAL locked by another process")
            self.fd = fd
        except Exception:
            if fd != -1:
                rposix.close(fd)
            raise
    
    def append_block(self, lsn, table_id, records):
        if self.closed: 
            raise errors.StorageError("WAL writer closed")
        wal_format.write_wal_block(self.fd, lsn, table_id, records, self.schema)
        mmap_posix.fsync_c(self.fd)

    def truncate_before_lsn(self, target_lsn):
        """
        Reclaims space by removing blocks with LSN < target_lsn.
        Uses a swap-and-rename pattern for atomic durability.
        """
        if self.closed: 
            return
        
        tmp_name = self.filename + ".trunc"
        reader = WALReader(self.filename, self.schema)
        try:
            out_fd = rposix.open(tmp_name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                for lsn, tid, records in reader.iterate_blocks():
                    if lsn >= target_lsn:
                        wal_format.write_wal_block(out_fd, lsn, tid, records, self.schema)
                mmap_posix.fsync_c(out_fd)
            finally:
                rposix.close(out_fd)
        finally:
            reader.close()
        
        # Release the lock and close current file before replacement
        mmap_posix.unlock_file(self.fd)
        rposix.close(self.fd)
        self.fd = -1
        
        os.rename(tmp_name, self.filename)
        
        # Re-open and re-lock
        new_fd = rposix.open(self.filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            if not mmap_posix.try_lock_exclusive(new_fd):
                 raise errors.StorageError("Failed to re-lock WAL after truncation")
            self.fd = new_fd
        except Exception:
            if new_fd != -1:
                rposix.close(new_fd)
            raise

    def close(self):
        if not self.closed:
            if self.fd != -1:
                mmap_posix.fsync_c(self.fd)
                mmap_posix.unlock_file(self.fd)
                rposix.close(self.fd)
                self.fd = -1
            self.closed = True
