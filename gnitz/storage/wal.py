"""
gnitz/storage/wal.py

Write-Ahead Log implementation for durable Z-Set ingestion.
"""
import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal_format, mmap_posix, errors


class WALWriter(object):
    """
    Append-only Write-Ahead Log writer.
    Ensures durability through fsync after each block write.
    
    The WAL serves as the source of truth for both crash recovery
    and distributed Z-Set synchronization.
    """
    def __init__(self, filename, layout):
        """
        Opens or creates a WAL file for append-only writes.
        
        Args:
            filename: Path to the WAL file
            layout: ComponentLayout for encoding records
        """
        self.filename = filename
        self.layout = layout
        self.fd = -1
        self.closed = False
        
        # Open file in append mode, create if doesn't exist
        # O_APPEND ensures atomic positioning at end of file
        self.fd = rposix.open(
            filename, 
            os.O_WRONLY | os.O_CREAT | os.O_APPEND, 
            0o644
        )
    
    def append_block(self, lsn, component_id, records):
        """
        Appends a Z-Set block to the WAL and syncs to disk.
        
        This is the critical durability path: the block is encoded,
        written to the file, and fsync'd before returning. This ensures
        that the Z-Set delta is durable even if the system crashes
        immediately after this call returns.
        
        Args:
            lsn: Log Sequence Number (monotonically increasing)
            component_id: Component type ID
            records: List of WALRecord objects (entity_id, weight, component_data)
        
        Raises:
            StorageError: If writer is closed or write/sync fails
        """
        if self.closed:
            raise errors.StorageError()
        
        # Encode the block using the WAL format
        block_bytes = wal_format.encode_wal_block(lsn, component_id, records, self.layout)
        
        # Write to file descriptor
        block_len = len(block_bytes)
        block_ptr = rffi.str2charp(block_bytes)
        try:
            written = mmap_posix.write_c(
                self.fd, 
                block_ptr, 
                rffi.cast(rffi.SIZE_T, block_len)
            )
            if rffi.cast(lltype.Signed, written) != block_len:
                raise errors.StorageError()
        finally:
            rffi.free_charp(block_ptr)
        
        # Sync to disk for durability (critical for crash safety)
        result = mmap_posix.fsync_c(self.fd)
        if rffi.cast(lltype.Signed, result) != 0:
            raise errors.StorageError()
    
    def close(self):
        """
        Closes the WAL file after final sync.
        
        Ensures all buffered data is flushed to disk before releasing
        the file descriptor.
        """
        if self.closed:
            return
        
        # Final sync to ensure all data is on disk
        mmap_posix.fsync_c(self.fd)
        
        # Close file descriptor
        rposix.close(self.fd)
        self.fd = -1
        self.closed = True
