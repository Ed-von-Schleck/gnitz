import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal_format, mmap_posix, errors

class WALReader(object):
    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.fd = rposix.open(filename, os.O_RDONLY, 0)
        self.closed = False
    
    def read_next_block(self):
        if self.closed: return (False, 0, 0, [])
        
        header_str = os.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
        if not header_str: return (False, 0, 0, [])
        
        if len(header_str) < wal_format.WAL_BLOCK_HEADER_SIZE:
            raise errors.CorruptShardError("Truncated WAL header")
            
        header_ptr = rffi.str2charp(header_str)
        try:
            # Read total block size from header offset 16 (added in previous fix)
            total_size = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(header_ptr, 16))[0])
            body_size = total_size - wal_format.WAL_BLOCK_HEADER_SIZE
            
            body_str = os.read(self.fd, body_size)
            if len(body_str) < body_size: 
                raise errors.CorruptShardError("Truncated WAL body")
            
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
        Uses atomic rename for crash safety.
        """
        if self.closed: return
        
        tmp_name = self.filename + ".trunc"
        # 1. Read existing and filter
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
        
        # 2. Atomic swap
        mmap_posix.unlock_file(self.fd)
        rposix.close(self.fd)
        
        os.rename(tmp_name, self.filename)
        
        # 3. Re-open and re-lock
        self.fd = rposix.open(self.filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        if not mmap_posix.try_lock_exclusive(self.fd):
             raise errors.StorageError("Failed to re-lock WAL after truncation")

    def close(self):
        if not self.closed:
            mmap_posix.fsync_c(self.fd)
            mmap_posix.unlock_file(self.fd)
            rposix.close(self.fd)
            self.closed = True
