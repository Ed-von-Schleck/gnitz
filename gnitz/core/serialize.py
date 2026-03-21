# gnitz/core/serialize.py

from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_int64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from gnitz.core import types, strings as string_logic, xxh
from gnitz.storage.buffer import c_memset as _c_memset


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
    _c_memset(rffi.cast(rffi.VOIDP, hash_buf), rffi.cast(rffi.INT, 0), rffi.cast(rffi.SIZE_T, sz))

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
            target = rffi.ptradd(ptr, offset)
            rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(
                rffi.ULONGLONG, accessor.get_u128_lo(i)
            )
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(
                rffi.ULONGLONG, accessor.get_u128_hi(i)
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
