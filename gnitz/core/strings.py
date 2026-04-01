# gnitz/core/strings.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint, intmask

# German String Constants:
# 16-byte total: 4 (len) | 4 (prefix) | 8 (suffix OR heap offset)
SHORT_STRING_THRESHOLD = 12
NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


def compute_prefix(s):
    length = len(s)
    prefix = r_uint(0)
    max_prefix_len = 4 if length > 4 else length
    for i in range(max_prefix_len):
        prefix |= r_uint(ord(s[i])) << (i * 8)

    # Standardize on signed int
    return intmask(prefix)


@jit.unroll_safe
def unpack_string(struct_ptr, heap_base_ptr):
    u32_ptr = rffi.cast(rffi.UINTP, struct_ptr)
    length = rffi.cast(lltype.Signed, u32_ptr[0])

    if length == 0:
        return ""

    if length <= SHORT_STRING_THRESHOLD:
        take_prefix = 4 if length > 4 else length
        res = rffi.charpsize2str(rffi.ptradd(struct_ptr, 4), take_prefix)
        if length > 4:
            res += rffi.charpsize2str(rffi.ptradd(struct_ptr, 8), length - 4)
        return res
    else:
        u64_payload_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(struct_ptr, 8))
        offset = rffi.cast(lltype.Signed, u64_payload_ptr[0])
        blob_ptr = rffi.ptradd(heap_base_ptr, offset)
        return rffi.charpsize2str(blob_ptr, length)


@jit.unroll_safe
def resolve_string(struct_ptr, heap_base_ptr, py_string):
    """
    Unified extraction logic for RowAccessor string columns.
    Prioritizes the Python string (VM layer). If None, deserializes
    from the raw German String struct (Storage layer).
    """
    if py_string is not None:
        return py_string

    if struct_ptr == NULL_PTR:
        return ""

    return unpack_string(struct_ptr, heap_base_ptr)
