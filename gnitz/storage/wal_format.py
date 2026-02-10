from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long
from gnitz.storage import errors, mmap_posix
from gnitz.core import checksum, types, values as db_values
from collections import namedtuple

WAL_BLOCK_HEADER_SIZE = 32
WALRecord = namedtuple('WALRecord', ['entity_id', 'weight', 'component_data'])

def write_wal_block(fd, lsn, table_id, records, schema):
    """
    Writes a block of records to the WAL using the fixed-stride format
    defined by the schema.
    """
    is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
    key_size = 16 if is_u128 else 8
    stride = schema.memtable_stride
    
    # Every record in WAL is now: [Key | Weight | FixedStridePayload]
    record_size = key_size + 8 + stride
    total_body_size = len(records) * record_size

    buf = lltype.malloc(rffi.CCHARP.TO, total_body_size, flavor='raw')
    try:
        curr_off = 0
        mask = r_uint128(0xFFFFFFFFFFFFFFFF)
        
        for key, weight, values in records:
            # 1. Write Key
            if is_u128:
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, curr_off))[0] = rffi.cast(rffi.ULONGLONG, key & mask)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, curr_off + 8))[0] = rffi.cast(rffi.ULONGLONG, (key >> 64) & mask)
            else:
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, curr_off))[0] = rffi.cast(rffi.ULONGLONG, key & mask)
            curr_off += key_size
            
            # 2. Write Weight
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(buf, curr_off))[0] = rffi.cast(rffi.LONGLONG, weight)
            curr_off += 8
            
            # 3. Write Payload
            if isinstance(values, str):
                # Direct copy for pre-packed rows
                for j in range(stride): 
                    if j < len(values): buf[curr_off + j] = values[j]
                    else: buf[curr_off + j] = '\x00'
            else:
                # Manual packing for list[DBValue]
                tmp_ptr = rffi.ptradd(buf, curr_off)
                for i in range(stride): tmp_ptr[i] = '\x00'
                
                arg_idx = 0
                for i in range(len(schema.columns)):
                    if i == schema.pk_index: continue
                    val_obj = values[arg_idx]
                    arg_idx += 1
                    
                    f_off = schema.get_column_offset(i)
                    target = rffi.ptradd(tmp_ptr, f_off)
                    ftype = schema.columns[i].field_type
                    
                    if ftype == types.TYPE_STRING:
                        from gnitz.core import strings
                        s_val = val_obj.v if isinstance(val_obj, db_values.StringValue) else ""
                        strings.pack_string(target, s_val, 0)
                    else:
                        val = val_obj.v if isinstance(val_obj, db_values.IntValue) else 0
                        if ftype.size == 8:
                            rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, val)
                        elif ftype.size == 4:
                            rffi.cast(rffi.UINTP, target)[0] = rffi.cast(rffi.UINT, val)
                        elif ftype.size == 1:
                            target[0] = chr(val & 0xFF)
            curr_off += stride

        # Write Header
        header = lltype.malloc(rffi.CCHARP.TO, WAL_BLOCK_HEADER_SIZE, flavor='raw')
        try:
            for i in range(WAL_BLOCK_HEADER_SIZE): header[i] = '\x00'
            rffi.cast(rffi.ULONGLONGP, header)[0] = rffi.cast(rffi.ULONGLONG, lsn)
            rffi.cast(rffi.UINTP, rffi.ptradd(header, 8))[0] = rffi.cast(rffi.UINT, table_id)
            rffi.cast(rffi.UINTP, rffi.ptradd(header, 12))[0] = rffi.cast(rffi.UINT, len(records))
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, 16))[0] = rffi.cast(rffi.ULONGLONG, checksum.compute_checksum(buf, total_body_size))
            mmap_posix.write_c(fd, header, rffi.cast(rffi.SIZE_T, WAL_BLOCK_HEADER_SIZE))
        finally: lltype.free(header, flavor='raw')
        
        mmap_posix.write_c(fd, buf, rffi.cast(rffi.SIZE_T, total_body_size))
    finally: lltype.free(buf, flavor='raw')

def decode_wal_block(block_ptr, block_len, schema):
    """
    Decodes a fixed-stride WAL block.
    """
    lsn = rffi.cast(rffi.ULONGLONGP, block_ptr)[0]
    tid = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, 8))[0])
    cnt = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, 12))[0])
    
    stored_cs = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(block_ptr, 16))[0]
    body_ptr = rffi.ptradd(block_ptr, WAL_BLOCK_HEADER_SIZE)
    body_len = block_len - WAL_BLOCK_HEADER_SIZE
    
    if checksum.compute_checksum(body_ptr, body_len) != stored_cs:
        raise errors.CorruptShardError("WAL Checksum Mismatch")

    is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
    key_size = 16 if is_u128 else 8
    stride = schema.memtable_stride
    record_size = key_size + 8 + stride
    
    records = []
    ptr = body_ptr
    
    for _ in range(cnt):
        # 1. Read Key
        if is_u128:
            low = rffi.cast(rffi.ULONGLONGP, ptr)[0]
            high = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
            key = (r_uint128(high) << 64) | r_uint128(low)
        else:
            key = r_uint128(rffi.cast(rffi.ULONGLONGP, ptr)[0])
        ptr = rffi.ptradd(ptr, key_size)
        
        # 2. Read Weight
        weight = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, ptr)[0])
        ptr = rffi.ptradd(ptr, 8)
        
        # 3. Capture Payload
        payload_str = rffi.charpsize2str(ptr, stride)
        ptr = rffi.ptradd(ptr, stride)
        
        records.append((key, weight, payload_str))
        
    return lsn, tid, records
