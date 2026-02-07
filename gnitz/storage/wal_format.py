"""
gnitz/storage/wal_format.py

Binary format for Z-Set Write-Ahead Log blocks.
Each block represents a batch of Z-Set deltas for a single LSN.
"""
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import errors
from gnitz.core import checksum, strings as string_logic, types

# WAL Block Header Layout (32 bytes)
# [00-07] LSN (u64)
# [08-11] Component ID (u32)
# [12-15] Entry Count (u32)
# [16-23] Block Checksum (u64) - DJB2 hash of entire block body
# [24-31] Reserved (u64)

WAL_BLOCK_HEADER_SIZE = 32

# Header field offsets
OFF_LSN = 0
OFF_COMPONENT_ID = 8
OFF_ENTRY_COUNT = 12
OFF_CHECKSUM = 16
OFF_RESERVED = 24

# ============================================================================
# WAL Record Representation
# ============================================================================

def WALRecord(entity_id, weight, component_data):
    """
    Creates a WAL record tuple.
    Args:
        entity_id: 64-bit Entity ID
        weight: 64-bit signed weight (algebraic Z-Set weight)
        component_data: Raw byte string containing packed component payload
    Returns:
        Tuple of (entity_id, weight, component_data)
    """
    return (entity_id, weight, component_data)


def encode_wal_block(lsn, component_id, records, layout):
    """
    Encodes a Z-Set batch into a binary WAL block.
    """
    entry_count = len(records)
    stride = layout.stride
    
    # Calculate body size: each record is 8 (eid) + 8 (weight) + stride (component)
    record_size = 8 + 8 + stride
    body_size = entry_count * record_size
    
    # Allocate body buffer
    body_buf = lltype.malloc(rffi.CCHARP.TO, body_size, flavor='raw')
    try:
        # Encode each record into the body
        for i in range(entry_count):
            entity_id, weight, component_data = records[i]
            record_offset = i * record_size
            
            # Write Entity ID (8 bytes)
            eid_ptr = rffi.ptradd(body_buf, record_offset)
            rffi.cast(rffi.LONGLONGP, eid_ptr)[0] = rffi.cast(rffi.LONGLONG, entity_id)
            
            # Write Weight (8 bytes)
            weight_ptr = rffi.ptradd(body_buf, record_offset + 8)
            rffi.cast(rffi.LONGLONGP, weight_ptr)[0] = rffi.cast(rffi.LONGLONG, weight)
            
            # Write Component Data (stride bytes)
            component_ptr = rffi.ptradd(body_buf, record_offset + 16)
            for j in range(len(component_data)):
                if j < stride:
                    component_ptr[j] = component_data[j]
        
        # Compute checksum of body
        body_checksum = checksum.compute_checksum(body_buf, body_size)
        
        # Convert body to Python string for return
        body_str = rffi.charpsize2str(body_buf, body_size)
    finally:
        lltype.free(body_buf, flavor='raw')
    
    # Encode header
    header_buf = lltype.malloc(rffi.CCHARP.TO, WAL_BLOCK_HEADER_SIZE, flavor='raw')
    try:
        # Zero out header
        for i in range(WAL_BLOCK_HEADER_SIZE):
            header_buf[i] = '\x00'
        
        # Write LSN
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(header_buf, OFF_LSN))[0] = rffi.cast(rffi.LONGLONG, lsn)
        
        # Write Component ID
        rffi.cast(rffi.UINTP, rffi.ptradd(header_buf, OFF_COMPONENT_ID))[0] = rffi.cast(rffi.UINT, component_id)
        
        # Write Entry Count
        rffi.cast(rffi.UINTP, rffi.ptradd(header_buf, OFF_ENTRY_COUNT))[0] = rffi.cast(rffi.UINT, entry_count)
        
        # Write Checksum
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header_buf, OFF_CHECKSUM))[0] = rffi.cast(rffi.ULONGLONG, body_checksum)
        
        # Convert header to Python string
        header_str = rffi.charpsize2str(header_buf, WAL_BLOCK_HEADER_SIZE)
    finally:
        lltype.free(header_buf, flavor='raw')
    
    # Concatenate header and body
    return header_str + body_str


def decode_wal_block(raw_bytes, layout):
    """
    Decodes a binary WAL block into its components.
    """
    if len(raw_bytes) < WAL_BLOCK_HEADER_SIZE:
        raise errors.CorruptShardError("WAL block too short for header")
    
    # Parse header
    header_buf = lltype.malloc(rffi.CCHARP.TO, WAL_BLOCK_HEADER_SIZE, flavor='raw')
    try:
        for i in range(WAL_BLOCK_HEADER_SIZE):
            header_buf[i] = raw_bytes[i]
        
        # Read LSN
        lsn = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, rffi.ptradd(header_buf, OFF_LSN))[0])
        
        # Read Component ID
        component_id = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(header_buf, OFF_COMPONENT_ID))[0])
        
        # Read Entry Count
        entry_count = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(header_buf, OFF_ENTRY_COUNT))[0])
        
        # Read Expected Checksum
        expected_checksum = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header_buf, OFF_CHECKSUM))[0])
    finally:
        lltype.free(header_buf, flavor='raw')
    
    # Validate Entry Count to ensure non-negative body size (Annotator safety)
    if entry_count < 0:
        raise errors.CorruptShardError("Invalid negative entry count in WAL block")
    
    # Calculate expected body size
    stride = layout.stride
    record_size = 8 + 8 + stride
    body_size = entry_count * record_size
    
    # Further validation (though entry_count >= 0 and stride > 0 implies body_size >= 0)
    if body_size < 0:
         raise errors.CorruptShardError("Invalid body size calculation")

    if len(raw_bytes) < WAL_BLOCK_HEADER_SIZE + body_size:
        raise errors.CorruptShardError("WAL block body truncated")
    
    # Extract body
    body_start = WAL_BLOCK_HEADER_SIZE
    body_end = body_start + body_size
    
    # RPython Annotator check: Ensure body_end is strictly non-negative
    if body_end < 0:
        raise errors.CorruptShardError("Invalid block dimensions")

    body_bytes = raw_bytes[body_start:body_end]
    
    # Validate checksum
    body_buf = lltype.malloc(rffi.CCHARP.TO, body_size, flavor='raw')
    try:
        for i in range(body_size):
            body_buf[i] = body_bytes[i]
        
        actual_checksum = checksum.compute_checksum(body_buf, body_size)
        if actual_checksum != expected_checksum:
            raise errors.CorruptShardError("WAL block checksum mismatch")
        
        # Decode records - use tuples instead of objects (GC-efficient)
        records = []
        for i in range(entry_count):
            record_offset = i * record_size
            
            # Read Entity ID
            eid_ptr = rffi.ptradd(body_buf, record_offset)
            entity_id = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, eid_ptr)[0])
            
            # Read Weight
            weight_ptr = rffi.ptradd(body_buf, record_offset + 8)
            weight = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, weight_ptr)[0])
            
            # Read Component Data
            component_ptr = rffi.ptradd(body_buf, record_offset + 16)
            component_data = rffi.charpsize2str(component_ptr, stride)
            
            # Append as tuple (no object allocation)
            records.append((entity_id, weight, component_data))
    finally:
        lltype.free(body_buf, flavor='raw')
    
    return lsn, component_id, records
