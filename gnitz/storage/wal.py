import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal_format, mmap_posix, errors

class WALReader(object):
    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.fd = -1
        self.last_inode = rffi.cast(rffi.LONGLONG, -1)
        self.closed = False
        self._open_file()
    
    def _open_file(self):
        """
        Opens the WAL file and records its inode. 
        If a file was already open, it is closed first.
        """
        if self.fd != -1:
            rposix.close(self.fd)
        
        self.fd = rposix.open(self.filename, os.O_RDONLY, 0)
        # Use standard os.fstat; RPython replaces this with a safe version
        st = os.fstat(self.fd)
        self.last_inode = rffi.cast(rffi.LONGLONG, st.st_ino)

    def _has_rotated(self):
        """
        Checks if the file at self.filename has a different inode 
        than the one currently open.
        """
        try:
            st = os.stat(self.filename)
            return rffi.cast(rffi.LONGLONG, st.st_ino) != self.last_inode
        except OSError:
            # If the file is temporarily missing (during rename), 
            # we assume it hasn't rotated yet.
            return False

    def read_next_block(self):
        if self.closed: return (False, 0, 0, [])
        
        # Try to read the fixed-size header
        header_str = os.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
        
        # EOF or partial read encountered
        if len(header_str) < wal_format.WAL_BLOCK_HEADER_SIZE:
            if self._has_rotated():
                # The WAL was rotated/truncated. Re-open and try again.
                self._open_file()
                header_str = os.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
                # If still short, the new file is either empty or incomplete.
                if len(header_str) < wal_format.WAL_BLOCK_HEADER_SIZE:
                    return (False, 0, 0, [])
            else:
                # Actual end of the current WAL stream
                return (False, 0, 0, [])
            
        header_ptr = rffi.str2charp(header_str)
        try:
            # Total block size is at offset 16 (stored as u32 in wal_format)
            total_size_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(header_ptr, 16))
            total_size = rffi.cast(lltype.Signed, total_size_ptr[0])
            body_size = total_size - wal_format.WAL_BLOCK_HEADER_SIZE
            
            if body_size < 0:
                raise errors.CorruptShardError("Invalid WAL block size in header")

            body_str = os.read(self.fd, body_size)
            if len(body_str) < body_size: 
                # Block is incomplete on disk.
                return (False, 0, 0, [])
            
            full_data = header_str + body_str
            full_ptr = rffi.str2charp(full_data)
            try:
                lsn, table_id, records = wal_format.decode_wal_block(full_ptr, len(full_data), self.schema)
                return (True, lsn, table_id, records)
            finally:
                rffi.free_charp(full_ptr)
        finally:
            rffi.free_charp(header_ptr)
    
    def iterate_blocks(self):
        while True:
            valid, lsn, tid, records = self.read_next_block()
            if not valid: break
            yield (lsn, tid, records)
    
    def close(self):
        if not self.closed:
            if self.fd != -1:
                rposix.close(self.fd)
            self.closed = True

class WALWriter(object):
    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.closed = False
        self.fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        if not mmap_posix.try_lock_exclusive(self.fd):
            rposix.close(self.fd)
            raise errors.StorageError("WAL locked by another process")
    
    def append_block(self, lsn, table_id, records):
        if self.closed: raise errors.StorageError("WAL writer closed")
        wal_format.write_wal_block(self.fd, lsn, table_id, records, self.schema)
        mmap_posix.fsync_c(self.fd)

    def truncate_before_lsn(self, target_lsn):
        """
        Prunes the WAL by keeping only blocks with LSN >= target_lsn.
        Uses atomic rename to ensure readers detect rotation via inode change.
        """
        if self.closed: return
        
        tmp_name = self.filename + ".trunc"
        reader = WALReader(self.filename, self.schema)
        try:
            out_fd = rposix.open(tmp_name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                for lsn, tid, records in reader.iterate_blocks():
                    if int(lsn) >= int(target_lsn):
                        wal_format.write_wal_block(out_fd, lsn, tid, records, self.schema)
                mmap_posix.fsync_c(out_fd)
            finally:
                rposix.close(out_fd)
        finally:
            reader.close()
        
        # Unlock and close current WAL before renaming
        mmap_posix.unlock_file(self.fd)
        rposix.close(self.fd)
        
        # Atomic rename changes the inode at self.filename
        os.rename(tmp_name, self.filename)
        
        # Re-open the new WAL file and re-acquire exclusive lock
        self.fd = rposix.open(self.filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        if not mmap_posix.try_lock_exclusive(self.fd):
             raise errors.StorageError("Failed to re-lock WAL after truncation")

    def close(self):
        if not self.closed:
            mmap_posix.fsync_c(self.fd)
            mmap_posix.unlock_file(self.fd)
            rposix.close(self.fd)
            self.closed = True
