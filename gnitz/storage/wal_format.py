from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import errors, mmap_posix
from gnitz.core import checksum

MAX_WAL_BLOCK_SIZE = 32 * 1024 * 1024 
WAL_BLOCK_HEADER_SIZE = 32

OFF_LSN = 0
OFF_COMPONENT_ID = 8
OFF_ENTRY_COUNT = 12
OFF_CHECKSUM = 16
OFF_RESERVED = 24

def WALRecord(entity_id, weight, component_data):
    return (entity_id, weight, component_data)

def write_wal_block(fd, lsn, component_id, records, layout):
    entry_count = len(records)
    stride = layout.stride
    record_size = 16 + stride
    body_size = entry_count * record_size
    
    if body_size > MAX_WAL_BLOCK_SIZE:
        raise errors.StorageError()

    body_buf = lltype.malloc(rffi.CCHARP.TO, body_size, flavor='raw')
    try:
        for i in range(entry_count):
            entity_id, weight, component_data = records[i]
            off = i * record_size
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(body_buf, off))[0] = rffi.cast(rffi.ULONGLONG, entity_id)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(body_buf, off + 8))[0] = rffi.cast(rffi.LONGLONG, weight)
            dest_data = rffi.ptradd(body_buf, off + 16)
            for j in range(stride):
                if j < len(component_data): dest_data[j] = component_data[j]
                else: dest_data[j] = '\x00'
        
        body_checksum = checksum.compute_checksum(body_buf, body_size)
        header_buf = lltype.malloc(rffi.CCHARP.TO, WAL_BLOCK_HEADER_SIZE, flavor='raw')
        try:
            for i in range(WAL_BLOCK_HEADER_SIZE): header_buf[i] = '\x00'
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header_buf, OFF_LSN))[0] = rffi.cast(rffi.ULONGLONG, lsn)
            rffi.cast(rffi.UINTP, rffi.ptradd(header_buf, OFF_COMPONENT_ID))[0] = rffi.cast(rffi.UINT, component_id)
            rffi.cast(rffi.UINTP, rffi.ptradd(header_buf, OFF_ENTRY_COUNT))[0] = rffi.cast(rffi.UINT, entry_count)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header_buf, OFF_CHECKSUM))[0] = rffi.cast(rffi.ULONGLONG, body_checksum)
            mmap_posix.write_c(fd, header_buf, rffi.cast(rffi.SIZE_T, WAL_BLOCK_HEADER_SIZE))
        finally:
            lltype.free(header_buf, flavor='raw')
        if body_size > 0:
            mmap_posix.write_c(fd, body_buf, rffi.cast(rffi.SIZE_T, body_size))
    finally:
        lltype.free(body_buf, flavor='raw')

def decode_wal_block(block_ptr, block_len, layout):
    """
    Decodes a WAL block from a raw memory pointer.
    Optimized for ll2ctypes performance by using array-indexing on casts.
    """
    if block_len < WAL_BLOCK_HEADER_SIZE:
        raise errors.CorruptShardError("Truncated header")
    
    # 1. Decode Header (Offsets 0, 8, 12, 16)
    lsn = rffi.cast(rffi.ULONGLONGP, block_ptr)[0]
    
    cid_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, OFF_COMPONENT_ID))
    component_id = rffi.cast(lltype.Signed, cid_ptr[0])
    
    count_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, OFF_ENTRY_COUNT))
    entry_count = rffi.cast(lltype.Signed, count_ptr[0])
    
    cs_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(block_ptr, OFF_CHECKSUM))
    expected_checksum = r_uint64(cs_ptr[0])

    if entry_count < 0:
        raise errors.CorruptShardError("Negative entry count")
    
    record_size = 16 + layout.stride
    body_size = entry_count * record_size
    if block_len < WAL_BLOCK_HEADER_SIZE + body_size:
        raise errors.CorruptShardError("Truncated body")

    # 2. Body Checksum Validation
    body_ptr = rffi.ptradd(block_ptr, WAL_BLOCK_HEADER_SIZE)
    if checksum.compute_checksum(body_ptr, body_size) != expected_checksum:
        raise errors.CorruptShardError("Checksum mismatch")
    
    # 3. Record Decoding
    records = []
    current_rec_ptr = body_ptr
    for i in range(entry_count):
        eid = rffi.cast(rffi.ULONGLONGP, current_rec_ptr)[0]
        weight = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, rffi.ptradd(current_rec_ptr, 8))[0])
        # Extract component payload
        data = rffi.charpsize2str(rffi.ptradd(current_rec_ptr, 16), layout.stride)
        records.append((eid, weight, data))
        # Advance pointer to next record
        current_rec_ptr = rffi.ptradd(current_rec_ptr, record_size)
        
    return lsn, component_id, records
