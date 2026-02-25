# gnitz/core/serialize.py

from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_int64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from gnitz.core import types, strings as string_logic, xxh
from gnitz.core.values import make_payload_row


@jit.unroll_safe
def get_heap_size(schema, accessor):
    """
    Calculates the total heap (blob) size required for a row.
    Accepts any RowAccessor (PayloadRowAccessor or RawWALAccessor).
    """
    heap_sz = 0
    num_cols = len(schema.columns)
    for i in range(num_cols):
        if i == schema.pk_index:
            continue

        # Check nullability first to avoid unnecessary fetching
        if accessor.is_null(i):
            continue

        if schema.columns[i].field_type.code == types.TYPE_STRING.code:
            # Polymorphic: works for both Python strings and Raw pointers
            length, _, _, _, _ = accessor.get_str_struct(i)
            if length > string_logic.SHORT_STRING_THRESHOLD:
                heap_sz += length
    return heap_sz


@jit.unroll_safe
def serialize_row(schema, accessor, dest_ptr, blob_allocator):
    """
    Unified serialization kernel.
    Accepts generic RowAccessor and BlobAllocator.
    Handles both Python objects (PayloadRow) and Raw Pointers (WAL Recovery).
    """
    num_cols = len(schema.columns)
    for i in range(num_cols):
        if i == schema.pk_index:
            continue

        f_type = schema.columns[i].field_type
        off = schema.get_column_offset(i)
        target = rffi.ptradd(dest_ptr, off)

        if accessor.is_null(i):
            sz = f_type.size
            for b in range(sz):
                target[b] = "\x00"
            continue

        code = f_type.code
        if code == types.TYPE_STRING.code:
            length, prefix, src_struct_ptr, src_heap_ptr, py_string = accessor.get_str_struct(i)
            string_logic.relocate_string(
                target,
                length,
                prefix,
                src_struct_ptr,
                src_heap_ptr,
                py_string,
                blob_allocator,
            )

        elif code == types.TYPE_F64.code:
            rffi.cast(rffi.DOUBLEP, target)[0] = rffi.cast(rffi.DOUBLE, accessor.get_float(i))
        elif code == types.TYPE_F32.code:
            rffi.cast(rffi.FLOATP, target)[0] = rffi.cast(rffi.FLOAT, accessor.get_float(i))
        elif code == types.TYPE_U128.code:
            v = accessor.get_u128(i)
            rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(v))
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(v >> 64)
            )
        elif code == types.TYPE_U64.code:
            rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, accessor.get_int(i))
        elif code == types.TYPE_I64.code:
            rffi.cast(rffi.LONGLONGP, target)[0] = rffi.cast(rffi.LONGLONG, accessor.get_int(i))
        elif code == types.TYPE_U32.code:
            rffi.cast(rffi.UINTP, target)[0] = rffi.cast(rffi.UINT, accessor.get_int(i) & 0xFFFFFFFF)
        elif code == types.TYPE_I32.code:
            rffi.cast(rffi.UINTP, target)[0] = rffi.cast(rffi.UINT, accessor.get_int(i) & 0xFFFFFFFF)
        elif code == types.TYPE_U16.code:
            v16 = intmask(accessor.get_int(i))
            target[0] = chr(v16 & 0xFF)
            target[1] = chr((v16 >> 8) & 0xFF)
        elif code == types.TYPE_I16.code:
            v16 = intmask(accessor.get_int(i))
            target[0] = chr(v16 & 0xFF)
            target[1] = chr((v16 >> 8) & 0xFF)
        elif code == types.TYPE_U8.code:
            target[0] = chr(intmask(accessor.get_int(i)) & 0xFF)
        elif code == types.TYPE_I8.code:
            target[0] = chr(intmask(accessor.get_int(i)) & 0xFF)
        else:
            rffi.cast(rffi.LONGLONGP, target)[0] = rffi.cast(rffi.LONGLONG, accessor.get_int(i))


@jit.unroll_safe
def deserialize_row(schema, src_ptr, src_heap_ptr, null_word=r_uint64(0)):
    """
    Reconstructs a PayloadRow from an AoS block.
    Enables zero-copy recovery from WAL blocks or memory-mapped shards.
    """
    row = make_payload_row(schema)
    num_cols = len(schema.columns)
    payload_col = 0
    for i in range(num_cols):
        if i == schema.pk_index:
            continue

        # Restore null semantics from the provided bitset
        if null_word & (r_uint64(1) << payload_col):
            row.append_null(payload_col)
            payload_col += 1
            continue

        col_type = schema.columns[i].field_type
        off = schema.get_column_offset(i)
        ptr = rffi.ptradd(src_ptr, off)
        code = col_type.code

        if code == types.TYPE_STRING.code:
            row.append_string(string_logic.unpack_string(ptr, src_heap_ptr))
        elif code == types.TYPE_F64.code:
            row.append_float(longlong2float(rffi.cast(rffi.LONGLONGP, ptr)[0]))
        elif code == types.TYPE_F32.code:
            row.append_float(float(rffi.cast(rffi.FLOATP, ptr)[0]))
        elif code == types.TYPE_U128.code:
            lo = rffi.cast(rffi.ULONGLONGP, ptr)[0]
            hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
            row.append_u128(r_uint64(lo), r_uint64(hi))
        elif (
            code == types.TYPE_I64.code
            or code == types.TYPE_I32.code
            or code == types.TYPE_I16.code
            or code == types.TYPE_I8.code
        ):
            row.append_int(rffi.cast(rffi.LONGLONG, rffi.cast(rffi.LONGLONGP, ptr)[0]))
        else:
            row.append_int(rffi.cast(rffi.LONGLONG, rffi.cast(rffi.ULONGLONGP, ptr)[0]))

        payload_col += 1
    return row


@jit.unroll_safe
def compute_hash(schema, accessor, hash_buf, hash_buf_cap):
    num_cols = len(schema.columns)
    sz = 0
    # 1. Calculate required size
    for i in range(num_cols):
        if i == schema.pk_index:
            continue
        sz += 1  # null flag byte
        if not accessor.is_null(i):
            ft = schema.columns[i].field_type
            if ft.code == types.TYPE_STRING.code:
                sz = (sz + 3) & ~3
                l_s, _, _, _, _ = accessor.get_str_struct(i)
                sz += 4 + l_s
            elif ft.code == types.TYPE_U128.code:
                sz = (sz + 7) & ~7
                sz += 16
            else:
                sz = (sz + 7) & ~7
                sz += 8

    # 2. Ensure capacity and handle reallocation
    if sz > hash_buf_cap:
        if hash_buf != lltype.nullptr(rffi.CCHARP.TO):
            lltype.free(hash_buf, flavor="raw")
        new_cap = sz * 2
        hash_buf = lltype.malloc(rffi.CCHARP.TO, new_cap, flavor="raw")
        hash_buf_cap = new_cap

    # 3. CRITICAL: Zero out the buffer up to 'sz'
    # This ensures alignment padding bytes are deterministic for the hash.
    for i in range(sz):
        hash_buf[i] = "\x00"

    # 4. Write data into the deterministic buffer
    ptr = hash_buf
    offset = 0
    for i in range(num_cols):
        if i == schema.pk_index:
            continue

        if accessor.is_null(i):
            ptr[offset] = "\x00"
            offset += 1
            continue

        ptr[offset] = "\x01"
        offset += 1
        ft = schema.columns[i].field_type
        if ft.code == types.TYPE_STRING.code:
            offset = (offset + 3) & ~3
            length, _, struct_ptr, heap_ptr, py_string = accessor.get_str_struct(i)
            rffi.cast(rffi.UINTP, rffi.ptradd(ptr, offset))[0] = rffi.cast(
                rffi.UINT, length
            )
            offset += 4
            target_chars = rffi.ptradd(ptr, offset)
            if py_string is not None:
                for j in range(length):
                    target_chars[j] = py_string[j]
            else:
                if length <= string_logic.SHORT_STRING_THRESHOLD:
                    take_prefix = 4 if length > 4 else length
                    for j in range(take_prefix):
                        target_chars[j] = struct_ptr[4 + j]
                    if length > 4:
                        suffix_src = rffi.ptradd(struct_ptr, 8)
                        for j in range(length - 4):
                            target_chars[4 + j] = suffix_src[j]
                else:
                    u64_p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(struct_ptr, 8))
                    heap_off = rffi.cast(lltype.Signed, u64_p[0])
                    src = rffi.ptradd(heap_ptr, heap_off)
                    for j in range(length):
                        target_chars[j] = src[j]
            offset += length
        elif ft.code == types.TYPE_U128.code:
            offset = (offset + 7) & ~7
            v = accessor.get_u128(i)
            target = rffi.ptradd(ptr, offset)
            rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(v))
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(v >> 64)
            )
            offset += 16
        else:
            offset = (offset + 7) & ~7
            if ft.code == types.TYPE_F64.code or ft.code == types.TYPE_F32.code:
                bits = float2longlong(accessor.get_float(i))
            else:
                bits = rffi.cast(rffi.LONGLONG, accessor.get_int(i))

            rffi.cast(rffi.LONGLONGP, rffi.ptradd(ptr, offset))[0] = bits
            offset += 8

    # 5. Hold `hash_buf` in a named local across the C call so PyPy2's GC
    # cannot collect the backing allocation before gnitz_xxh3_64 finishes
    # reading it.  A bare rffi.cast(VOIDP, hash_buf) does not root the buffer
    # in ll2ctypes mode; only a live Python reference does.
    buf = hash_buf
    checksum = xxh.compute_checksum(buf, sz)
    return checksum, hash_buf, hash_buf_cap
