from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_uint32
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import errors, mmap_posix
from gnitz.core import checksum, types, strings as string_logic, values as db_values

WAL_BLOCK_HEADER_SIZE = 32

def align_8(val):
    return (val + 7) & ~7

class WALRecord(object):
    # Split PK into two u64 fields to avoid alignment crashes in GC memory
    _immutable_fields_ = ["pk_lo", "pk_hi", "weight", "component_data[*]"]
    
    def __init__(self, pk_lo, pk_hi, weight, component_data):
        self.pk_lo = r_uint64(pk_lo)
        self.pk_hi = r_uint64(pk_hi)
        self.weight = weight
        self.component_data = component_data

    @staticmethod
    def from_key(key, weight, component_data):
        """
        Factory method to create a WALRecord from a 128-bit or 64-bit key.
        This provides a stable API for tests and ingestion logic.
        """
        k = r_uint128(key)
        return WALRecord(r_uint64(k), r_uint64(k >> 64), weight, component_data)

    def get_key(self):
        """Recombines split PK components into a stack-based u128."""
        return (r_uint128(self.pk_hi) << 64) | r_uint128(self.pk_lo)

class DecodedBlock(object):
    _immutable_fields_ = ['lsn', 'tid', 'records[*]']
    def __init__(self, lsn, tid, records):
        self.lsn = lsn
        self.tid = tid
        self.records = records

def write_wal_block(fd, lsn, table_id, records, schema):
    is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
    key_size = 16 if is_u128 else 8
    stride = schema.memtable_stride
    num_cols = len(schema.columns)

    total_body_size = 0
    rec_idx = 0
    num_records = len(records)
    while rec_idx < num_records:
        rec = records[rec_idx]
        total_body_size += key_size + 8 + stride
        
        arg_idx = 0
        i = 0
        while i < num_cols:
            if i != schema.pk_index:
                val_obj = rec.component_data[arg_idx]
                arg_idx += 1
                if schema.columns[i].field_type == types.TYPE_STRING:
                    s_len = len(val_obj.get_string())
                    if s_len > string_logic.SHORT_STRING_THRESHOLD:
                        total_body_size += s_len
            i += 1
        total_body_size = align_8(total_body_size)
        rec_idx += 1

    buf = lltype.malloc(rffi.CCHARP.TO, total_body_size, flavor="raw")
    try:
        for k in range(total_body_size): buf[k] = "\x00"
        
        curr_off = 0
        rec_idx = 0
        while rec_idx < num_records:
            rec = records[rec_idx]
            
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, curr_off))[0] = rffi.cast(rffi.ULONGLONG, rec.pk_lo)
            if is_u128:
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, curr_off + 8))[0] = rffi.cast(rffi.ULONGLONG, rec.pk_hi)
            curr_off += key_size

            rffi.cast(rffi.LONGLONGP, rffi.ptradd(buf, curr_off))[0] = rffi.cast(rffi.LONGLONG, rec.weight)
            curr_off += 8

            p_start = curr_off
            curr_off += stride
            
            arg_idx = 0
            i = 0
            while i < num_cols:
                if i != schema.pk_index:
                    val_obj = rec.component_data[arg_idx]
                    arg_idx += 1
                    target = rffi.ptradd(buf, p_start + schema.get_column_offset(i))
                    col_type = schema.columns[i].field_type
                    
                    if col_type == types.TYPE_STRING:
                        s_val = val_obj.get_string()
                        if len(s_val) > string_logic.SHORT_STRING_THRESHOLD:
                            string_logic.pack_string(target, s_val, WAL_BLOCK_HEADER_SIZE + curr_off)
                            for j in range(len(s_val)): buf[curr_off + j] = s_val[j]
                            curr_off += len(s_val)
                        else:
                            string_logic.pack_string(target, s_val, 0)
                    elif col_type == types.TYPE_F64:
                        rffi.cast(rffi.DOUBLEP, target)[0] = rffi.cast(rffi.DOUBLE, val_obj.get_float())
                    elif col_type == types.TYPE_U128:
                        uv = val_obj.get_u128()
                        rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(uv))
                        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(uv >> 64))
                    else:
                        rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, val_obj.get_int())
                i += 1
            curr_off = align_8(curr_off)
            rec_idx += 1

        h_buf = lltype.malloc(rffi.CCHARP.TO, WAL_BLOCK_HEADER_SIZE, flavor="raw")
        try:
            for k in range(WAL_BLOCK_HEADER_SIZE): h_buf[k] = "\x00"
            rffi.cast(rffi.ULONGLONGP, h_buf)[0] = rffi.cast(rffi.ULONGLONG, lsn)
            rffi.cast(rffi.UINTP, rffi.ptradd(h_buf, 8))[0] = rffi.cast(rffi.UINT, table_id)
            rffi.cast(rffi.UINTP, rffi.ptradd(h_buf, 12))[0] = rffi.cast(rffi.UINT, num_records)
            rffi.cast(rffi.UINTP, rffi.ptradd(h_buf, 16))[0] = rffi.cast(rffi.UINT, WAL_BLOCK_HEADER_SIZE + total_body_size)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(h_buf, 24))[0] = checksum.compute_checksum(buf, total_body_size)
            if mmap_posix.write_c(fd, h_buf, rffi.cast(rffi.SIZE_T, WAL_BLOCK_HEADER_SIZE)) < WAL_BLOCK_HEADER_SIZE:
                raise errors.StorageError("WAL header write failed")
        finally:
            lltype.free(h_buf, flavor="raw")
            
        if mmap_posix.write_c(fd, buf, rffi.cast(rffi.SIZE_T, total_body_size)) < total_body_size:
            raise errors.StorageError("WAL body write failed")
    finally:
        lltype.free(buf, flavor="raw")

def decode_wal_block(block_ptr, block_len, schema):
    lsn = rffi.cast(rffi.ULONGLONGP, block_ptr)[0]
    tid = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, 8))[0])
    cnt = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, 12))[0])
    stored_cs = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(block_ptr, 24))[0]
    
    body_ptr = rffi.ptradd(block_ptr, WAL_BLOCK_HEADER_SIZE)
    body_len = block_len - WAL_BLOCK_HEADER_SIZE
    if checksum.compute_checksum(body_ptr, body_len) != stored_cs:
        raise errors.CorruptShardError("WAL Checksum Mismatch")

    is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
    key_size = 16 if is_u128 else 8
    stride = schema.memtable_stride
    num_cols = len(schema.columns)
    num_payload_fields = num_cols - 1

    records = [None] * cnt
    curr_ptr = body_ptr
    
    rec_idx = 0
    while rec_idx < cnt:
        pk_lo = rffi.cast(rffi.ULONGLONGP, curr_ptr)[0]
        pk_hi = r_uint64(0)
        if is_u128:
            pk_hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(curr_ptr, 8))[0]
        curr_ptr = rffi.ptradd(curr_ptr, key_size)
        
        weight = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, curr_ptr)[0])
        curr_ptr = rffi.ptradd(curr_ptr, 8)

        p_ptr = curr_ptr
        curr_ptr = rffi.ptradd(curr_ptr, stride)
        
        field_values = [None] * num_payload_fields
        val_idx = 0
        i = 0
        while i < num_cols:
            if i != schema.pk_index:
                col_def = schema.columns[i]
                fptr = rffi.ptradd(p_ptr, schema.get_column_offset(i))
                val = None
                if col_def.field_type == types.TYPE_STRING:
                    length = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, fptr)[0])
                    if length <= string_logic.SHORT_STRING_THRESHOLD:
                        take = 4 if length > 4 else length
                        s = rffi.charpsize2str(rffi.ptradd(fptr, 4), take)
                        if length > 4: s += rffi.charpsize2str(rffi.ptradd(fptr, 8), length - 4)
                        val = db_values.StringValue(s)
                    else:
                        val = db_values.StringValue(rffi.charpsize2str(curr_ptr, length))
                        curr_ptr = rffi.ptradd(curr_ptr, length)
                elif col_def.field_type == types.TYPE_F64:
                    val = db_values.FloatValue(float(rffi.cast(rffi.DOUBLEP, fptr)[0]))
                elif col_def.field_type == types.TYPE_U128:
                    l = rffi.cast(rffi.ULONGLONGP, fptr)[0]
                    h = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(fptr, 8))[0]
                    val = db_values.U128Value((r_uint128(h) << 64) | r_uint128(l))
                else:
                    val = db_values.IntValue(rffi.cast(rffi.ULONGLONGP, fptr)[0])
                field_values[val_idx] = val
                val_idx += 1
            i += 1
        
        cons = rffi.cast(lltype.Signed, curr_ptr) - rffi.cast(lltype.Signed, body_ptr)
        curr_ptr = rffi.ptradd(body_ptr, align_8(cons))
        
        records[rec_idx] = WALRecord(pk_lo, pk_hi, weight, field_values)
        rec_idx += 1

    return DecodedBlock(lsn, tid, records)
