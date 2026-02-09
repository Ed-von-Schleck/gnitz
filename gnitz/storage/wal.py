import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal_format, mmap_posix, errors

class WALReader(object):
    def __init__(self, filename, layout):
        self.filename = filename
        self.layout = layout
        self.fd = rposix.open(filename, os.O_RDONLY, 0)
        self.closed = False
    
    def read_next_block(self):
        if self.closed: return (False, 0, 0, [])
        
        header_str = os.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
        
        if not header_str or len(header_str) == 0:
            return (False, 0, 0, [])
            
        if len(header_str) < wal_format.WAL_BLOCK_HEADER_SIZE:
            raise errors.CorruptShardError("Truncated WAL header")
            
        # Reconstruct entry count using standard machine-word integers.
        # This avoids TyperErrors associated with shifting small UINT types.
        b12 = ord(header_str[12])
        b13 = ord(header_str[13])
        b14 = ord(header_str[14])
        b15 = ord(header_str[15])
        
        entry_count_val = b12 | (b13 << 8) | (b14 << 16) | (b15 << 24)
        entry_count = rffi.cast(lltype.Signed, entry_count_val)
        
        record_size = wal_format.get_record_size(self.layout)
        body_size = entry_count * record_size
        
        body_str = ""
        if body_size > 0:
            body_str = os.read(self.fd, body_size)
            if len(body_str) < body_size: 
                raise errors.CorruptShardError("Truncated WAL body")
        
        full_block_str = header_str + body_str
        block_len = len(full_block_str)
        block_ptr = rffi.str2charp(full_block_str)
        try:
            lsn, component_id, records = wal_format.decode_wal_block(block_ptr, block_len, self.layout)
            return (True, lsn, component_id, records)
        finally:
            rffi.free_charp(block_ptr)
    
    def iterate_blocks(self):
        while True:
            is_valid, lsn, component_id, records = self.read_next_block()
            if not is_valid: break
            yield (lsn, component_id, records)
    
    def close(self):
        if not self.closed:
            rposix.close(self.fd)
            self.closed = True

class WALWriter(object):
    def __init__(self, filename, layout):
        self.filename = filename
        self.layout = layout
        self.closed = False
        self.fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    
    def append_block(self, lsn, component_id, records):
        if self.closed: raise errors.StorageError()
        wal_format.write_wal_block(self.fd, lsn, component_id, records, self.layout)
        mmap_posix.fsync_c(self.fd)
            
    def truncate_before_lsn(self, target_lsn):
        temp_fn = self.filename + ".trunc"
        new_w = WALWriter(temp_fn, self.layout)
        reader = WALReader(self.filename, self.layout)
        try:
            for lsn, comp_id, records in reader.iterate_blocks():
                if lsn >= target_lsn:
                    new_w.append_block(lsn, comp_id, records)
        finally:
            reader.close()
            new_w.close()
        os.rename(temp_fn, self.filename)
        rposix.close(self.fd)
        self.fd = rposix.open(self.filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    
    def close(self):
        if not self.closed:
            mmap_posix.fsync_c(self.fd)
            rposix.close(self.fd)
            self.closed = True
