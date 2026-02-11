from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long
from gnitz.core import types, values, strings as string_logic

def node_get_next_off(base_ptr, node_off, level):
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    next_ptr = rffi.cast(rffi.UINTP, ptr)
    return rffi.cast(lltype.Signed, next_ptr[level])

def node_set_next_off(base_ptr, node_off, level, target_off):
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    next_ptr = rffi.cast(rffi.UINTP, ptr)
    next_ptr[level] = rffi.cast(rffi.UINT, target_off)

def node_get_weight(base_ptr, node_off):
    ptr = rffi.ptradd(base_ptr, node_off)
    return rffi.cast(rffi.LONGLONGP, ptr)[0]

def node_set_weight(base_ptr, node_off, weight):
    ptr = rffi.ptradd(base_ptr, node_off)
    rffi.cast(rffi.LONGLONGP, ptr)[0] = rffi.cast(rffi.LONGLONG, weight)

def node_get_key(base_ptr, node_off, key_size):
    height = ord(base_ptr[node_off + 8])
    ptr = rffi.ptradd(base_ptr, node_off + 12 + (height * 4))
    if key_size == 16:
        lo = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(hi) << 64) | r_uint128(lo)
    return r_uint128(rffi.cast(rffi.ULONGLONGP, ptr)[0])

def node_get_payload_ptr(base_ptr, node_off, key_size):
    height = ord(base_ptr[node_off + 8])
    return rffi.ptradd(base_ptr, node_off + 12 + (height * 4) + key_size)

def unpack_payload_to_values(memtable_inst, node_off):
    base, blob_base = memtable_inst.arena.base_ptr, memtable_inst.blob_arena.base_ptr
    ptr = node_get_payload_ptr(base, node_off, memtable_inst.key_size)
    schema, res = memtable_inst.schema, []
    for i in range(len(schema.columns)):
        if i == schema.pk_index: continue
        col_def, off = schema.columns[i], schema.get_column_offset(i)
        f_ptr = rffi.ptradd(ptr, off)
        if col_def.field_type == types.TYPE_STRING:
            u32_ptr = rffi.cast(rffi.UINTP, f_ptr)
            length = rffi.cast(lltype.Signed, u32_ptr[0])
            if length == 0: res.append(values.StringValue(""))
            elif length <= string_logic.SHORT_STRING_THRESHOLD:
                res.append(values.StringValue(rffi.charpsize2str(rffi.ptradd(f_ptr, 4), length)))
            else:
                blob_off = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(f_ptr, 8))[0])
                res.append(values.StringValue(rffi.charpsize2str(rffi.ptradd(blob_base, blob_off), length)))
        elif col_def.field_type == types.TYPE_F64:
            res.append(values.FloatValue(float(rffi.cast(rffi.DOUBLEP, f_ptr)[0])))
        else:
            res.append(values.IntValue(rffi.cast(rffi.LONGLONGP, f_ptr)[0]))
    return res
