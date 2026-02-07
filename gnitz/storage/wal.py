"""
gnitz/storage/wal.py

Write-Ahead Log implementation for durable Z-Set ingestion.
"""
import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal_format, mmap_posix, errors

class WALReader(object):
    """
    Sequential reader for Write-Ahead Log blocks.
    Validates checksums and provides iteration over Z-Set deltas.
    """
    def __init__(self, filename, layout):
        """
        Opens a WAL file for sequential reading.
        
        Args:
            filename: Path to the WAL file
            layout: ComponentLayout for decoding records
        """
        self.filename = filename
        self.layout = layout
        self.fd = -1
        self.closed = False
        
        # Open file in read-only mode
        self.fd = rposix.open(filename, os.O_RDONLY, 0)
    
    def read_next_block(self):
        """
        Reads the next block from the WAL sequentially.
        
        Automatically validates the block's checksum. If validation fails,
        raises CorruptShardError to signal data corruption.
        
        Returns:
            Tuple of (is_valid, lsn, component_id, records).
            is_valid is False if EOF reached.
            records is a list of tuples (entity_id, weight, component_data).
        
        Raises:
            CorruptShardError: If checksum validation fails or block is malformed
        """
        if self.closed:
            return (False, 0, 0, [])
        
        # Read header first to determine block size
        header_buf = lltype.malloc(rffi.CCHARP.TO, wal_format.WAL_BLOCK_HEADER_SIZE, flavor='raw')
        try:
            # Read header bytes
            header_read = os.read(self.fd, wal_format.WAL_BLOCK_HEADER_SIZE)
            
            # Check for EOF
            if len(header_read) == 0:
                return (False, 0, 0, [])
            
            # Partial header is corruption
            if len(header_read) < wal_format.WAL_BLOCK_HEADER_SIZE:
                raise errors.CorruptShardError("Truncated WAL block header")
            
            # Copy to buffer for parsing
            for i in range(wal_format.WAL_BLOCK_HEADER_SIZE):
                header_buf[i] = header_read[i]
            
            # Parse entry count to determine body size
            entry_count = rffi.cast(
                lltype.Signed,
                rffi.cast(rffi.UINTP, rffi.ptradd(header_buf, wal_format.OFF_ENTRY_COUNT))[0]
            )
        finally:
            lltype.free(header_buf, flavor='raw')
        
        # Calculate body size
        stride = self.layout.stride
        record_size = 8 + 8 + stride  # entity_id + weight + component_data
        body_size = entry_count * record_size
        
        # Read body
        body_read = os.read(self.fd, body_size)
        
        # Check for truncated body
        if len(body_read) < body_size:
            raise errors.CorruptShardError("Truncated WAL block body")
        
        # Reconstruct complete block for decoding
        complete_block = header_read + body_read
        
        # Decode and validate (decode_wal_block validates checksum internally)
        lsn, component_id, records = wal_format.decode_wal_block(complete_block, self.layout)
        
        return (True, lsn, component_id, records)
    
    def iterate_blocks(self):
        """
        Generator that yields all blocks from the WAL in sequence.
        
        Yields:
            Tuples of (lsn, component_id, records)
        
        Raises:
            CorruptShardError: If any block fails checksum validation
        """
        while True:
            # Unpack the valid flag and data
            is_valid, lsn, component_id, records = self.read_next_block()
            
            if not is_valid:
                break
                
            yield (lsn, component_id, records)
    
    def close(self):
        """Closes the WAL file."""
        if self.closed:
            return
        
        rposix.close(self.fd)
        self.fd = -1
        self.closed = True


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
            
    def truncate_before_lsn(self, target_lsn):
        """
        Removes all WAL blocks older than target_lsn.
        Rewrites the log to a temporary file and renames.
        """
        temp_filename = self.filename + ".trunc"
        new_writer = WALWriter(temp_filename, self.layout)
        
        reader = WALReader(self.filename, self.layout)
        try:
            for lsn, comp_id, records in reader.iterate_blocks():
                if lsn >= target_lsn:
                    new_writer.append_block(lsn, comp_id, records)
        finally:
            reader.close()
            new_writer.close()
        
        # Atomically replace old log
        os.rename(temp_filename, self.filename)
        # Re-open the file descriptor for the current writer
        rposix.close(self.fd)
        self.fd = rposix.open(self.filename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    
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

