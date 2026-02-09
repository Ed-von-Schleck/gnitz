import os
from rpython.rlib import rposix
from gnitz.storage import wal_format, mmap_posix, errors

class WALReader(object):
    def __init__(self, filename, layout):
        self.filename = filename
        self.layout = layout
        self.fd = rposix.open(filename, os.O_RDONLY, 0)
        self.closed = False
    
    def read_next_block(self):
        if self.closed: return (False, 0, 0, [])
        header_read = os.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
        
        # Clean EOF
        if not header_read or len(header_read) == 0:
            return (False, 0, 0, [])
            
        # Truncated Header (Corruption)
        if len(header_read) < wal_format.WAL_BLOCK_HEADER_SIZE:
            raise errors.CorruptShardError("Truncated WAL header")
            
        entry_count = (ord(header_read[12]) | (ord(header_read[13]) << 8) | 
                       (ord(header_read[14]) << 16) | (ord(header_read[15]) << 24))
        
        body_size = entry_count * (16 + self.layout.stride)
        body_read = os.read(self.fd, body_size)
        if len(body_read) < body_size: 
            raise errors.CorruptShardError("Truncated WAL body")
        
        lsn, component_id, records = wal_format.decode_wal_block(header_read + body_read, self.layout)
        return (True, lsn, component_id, records)
    
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
                if lsn >= target_lsn: new_w.append_block(lsn, comp_id, records)
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
