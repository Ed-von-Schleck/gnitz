from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_uint32
from collections import namedtuple
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long
from gnitz.storage import errors, mmap_posix
from gnitz.core import checksum, types, strings as string_logic, values as db_values

WAL_BLOCK_HEADER_SIZE = 32
WALRecord = namedtuple('WALRecord', ['entity_id', 'weight', 'component_data'])

def write_wal_block(fd, lsn, table_id, records, schema):
    is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
    key_size = 16 if is_u128 else 8
    stride = schema.memtable_stride
    
    # 1. Calculate variable body size
    total_body_size = 0
    for _, _, values in records:
        total_body_size += (key_size + 8 + stride)
        if not isinstance(values, str):
            arg_idx = 0
            for i in range(len(schema.columns)):
                if i == schema.pk_index: continue
                val_obj = values[arg_idx]
                arg_idx += 1
                if schema.columns[i].field_type == types.TYPE_STRING:
                    if isinstance(val_obj, db_values.StringValue) and len(val_obj.v) > 12:
                        total_body_size += len(val_obj.v)

    buf = lltype.malloc(rffi.CCHARP.TO, total_body_size, flavor='raw')
    try:
        curr_off = 0
        mask = r_uint128(0xFFFFFFFFFFFFFFFF)
        for key, weight, values in records:
            # Key & Weight
            if is_u128:
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, curr_off))[0] = rffi.cast(rffi.ULONGLONG, key & mask)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, curr_off + 8))[0] = rffi.cast(rffi.ULONGLONG, (key >> 64) & mask)
            else:
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, curr_off))[0] = rffi.cast(rffi.ULONGLONG, key & mask)
            curr_off += key_size
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(buf, curr_off))[0] = rffi.cast(rffi.LONGLONG, weight)
            curr_off += 8
            
            payload_start = curr_off
            curr_off += stride
            
            arg_idx = 0
            for i in range(len(schema.columns)):
                if i == schema.pk_index: continue
                val_obj = values[arg_idx]
                arg_idx += 1
                f_off = schema.get_column_offset(i)
                target = rffi.ptradd(buf, payload_start + f_off)
                ftype = schema.columns[i].field_type
                
                if ftype == types.TYPE_STRING:
                    s_val = val_obj.v if isinstance(val_obj, db_values.StringValue) else ""
                    string_logic.pack_string(target, s_val, 0)
                    if len(s_val) > 12:
                        for j in range(len(s_val)): buf[curr_off + j] = s_val[j]
                        # Relative to block start
                        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(rffi.ULONGLONG, WAL_BLOCK_HEADER_SIZE + curr_off)
                        curr_off += len(s_val)
                else:
                    val = val_obj.v if isinstance(val_obj, db_values.IntValue) else 0
                    rffi.cast(rffi.LONGLONGP, target)[0] = rffi.cast(rffi.LONGLONG, val)

        # 2. Header: [LSN(8)|Tid(4)|Cnt(4)|Size(4)|CS(8)|Pad(4)]
        header = lltype.malloc(rffi.CCHARP.TO, WAL_BLOCK_HEADER_SIZE, flavor='raw')
        try:
            for i in range(WAL_BLOCK_HEADER_SIZE): header[i] = '\x00'
            rffi.cast(rffi.ULONGLONGP, header)[0] = rffi.cast(rffi.ULONGLONG, lsn)
            rffi.cast(rffi.UINTP, rffi.ptradd(header, 8))[0] = rffi.cast(rffi.UINT, table_id)
            rffi.cast(rffi.UINTP, rffi.ptradd(header, 12))[0] = rffi.cast(rffi.UINT, len(records))
            rffi.cast(rffi.UINTP, rffi.ptradd(header, 16))[0] = rffi.cast(rffi.UINT, WAL_BLOCK_HEADER_SIZE + total_body_size)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, 20))[0] = rffi.cast(rffi.ULONGLONG, checksum.compute_checksum(buf, total_body_size))
            mmap_posix.write_c(fd, header, rffi.cast(rffi.SIZE_T, WAL_BLOCK_HEADER_SIZE))
        finally: lltype.free(header, flavor='raw')
        mmap_posix.write_c(fd, buf, rffi.cast(rffi.SIZE_T, total_body_size))
    finally: lltype.free(buf, flavor='raw')

def decode_wal_block(block_ptr, block_len, schema):
    lsn = rffi.cast(rffi.ULONGLONGP, block_ptr)[0]
    tid = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, 8))[0])
    cnt = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, 12))[0])
    
    stored_cs = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(block_ptr, 20))[0]
    body_ptr = rffi.ptradd(block_ptr, WAL_BLOCK_HEADER_SIZE)
    body_len = block_len - WAL_BLOCK_HEADER_SIZE
    
    if checksum.compute_checksum(body_ptr, body_len) != stored_cs:
        raise errors.CorruptShardError("WAL Checksum Mismatch")

    is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
    key_size = 16 if is_u128 else 8
    stride = schema.memtable_stride
    
    records = []
    ptr = body_ptr
    for _ in range(cnt):
        if is_u128:
            low, high = rffi.cast(rffi.ULONGLONGP, ptr)[0], rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
            key = (r_uint128(high) << 64) | r_uint128(low)
        else:
            key = r_uint128(rffi.cast(rffi.ULONGLONGP, ptr)[0])
        ptr = rffi.ptradd(ptr, key_size)
        weight = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, ptr)[0])
        ptr = rffi.ptradd(ptr, 8)
        
        payload_ptr = ptr
        ptr = rffi.ptradd(ptr, stride)
        
        field_values = []
        for i in range(len(schema.columns)):
            if i == schema.pk_index: continue
            col_def = schema.columns[i]
            foff = schema.get_column_offset(i)
            fptr = rffi.ptradd(payload_ptr, foff)
            
            if col_def.field_type == types.TYPE_STRING:
                length = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, fptr)[0])
                if length <= 12:
                    # Extract inline string from payload
                    s_bytes = rffi.charpsize2str(rffi.ptradd(fptr, 4), 4) # Prefix
                    if length > 4:
                        s_bytes += rffi.charpsize2str(rffi.ptradd(fptr, 8), length - 4)
                    field_values.append(db_values.StringValue(s_bytes))
                else:
                    wal_off = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(fptr, 8))[0])
                    field_values.append(db_values.StringValue(rffi.charpsize2str(rffi.ptradd(block_ptr, wal_off), length)))
                    ptr = rffi.ptradd(ptr, length)
            else:
                field_values.append(db_values.IntValue(rffi.cast(rffi.LONGLONGP, fptr)[0]))
        records.append((key, weight, field_values))
    return lsn, tid, records
