# gnitz/core/strings.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint, r_uint32, r_uint64, r_int64, intmask

# German String Constants:
# 16-byte total: 4 (len) | 4 (prefix) | 8 (suffix OR heap offset)
SHORT_STRING_THRESHOLD = 12
NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class BlobAllocator(object):
    def allocate(self, string_data):
        raise NotImplementedError

    def allocate_from_ptr(self, src_ptr, length):
        raise NotImplementedError


def compute_prefix(s):
    length = len(s)
    prefix = r_uint(0)
    max_prefix_len = 4 if length > 4 else length
    for i in range(max_prefix_len):
        prefix |= r_uint(ord(s[i])) << (i * 8)
    
    # Standardize on signed int
    return intmask(prefix)


@jit.unroll_safe
def relocate_string(target, length, prefix, src_struct_ptr, src_heap_ptr, py_string, allocator):
    """
    Unified German String packing kernel.
    """
    u32_target = rffi.cast(rffi.UINTP, target)

    # --- Bytes 0-3: Length ---
    u32_target[0] = rffi.cast(rffi.UINT, length)

    # --- Bytes 4-7: Prefix ---
    if py_string is not None:
        # compute_prefix returns LONGLONG, cast to UINT for the 4-byte slot
        u32_target[1] = rffi.cast(rffi.UINT, compute_prefix(py_string))
    else:
        # prefix is passed as LONGLONG, cast to UINT for the 4-byte slot
        u32_target[1] = rffi.cast(rffi.UINT, prefix)

    # --- Bytes 8-15: Payload ---
    payload_target = rffi.ptradd(target, 8)

    if length <= SHORT_STRING_THRESHOLD:
        if py_string is not None:
            suffix_len = length - 4 if length > 4 else 0
            for j in range(suffix_len):
                payload_target[j] = py_string[j + 4]
            for j in range(suffix_len, 8):
                payload_target[j] = "\x00"
        else:
            suffix_len = length - 4 if length > 4 else 0
            src_suffix = rffi.ptradd(src_struct_ptr, 8)
            for j in range(suffix_len):
                payload_target[j] = src_suffix[j]
            for j in range(suffix_len, 8):
                payload_target[j] = "\x00"
    else:
        if py_string is not None:
            new_offset = allocator.allocate(py_string)
        else:
            old_offset_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(src_struct_ptr, 8))
            old_offset = old_offset_ptr[0]
            src_data_ptr = rffi.ptradd(src_heap_ptr, rffi.cast(lltype.Signed, old_offset))
            new_offset = allocator.allocate_from_ptr(src_data_ptr, length)

        rffi.cast(rffi.ULONGLONGP, payload_target)[0] = rffi.cast(
            rffi.ULONGLONG, new_offset
        )


def pack_and_write_blob(target_ptr, string_data, allocator):
    relocate_string(
        target_ptr,
        len(string_data),
        r_int64(0),  # Corrected to signed
        NULL_PTR,
        NULL_PTR,
        string_data,
        allocator,
    )


@jit.unroll_safe
def compare_structures(
    len1, pref1, ptr1, heap1, str1, len2, pref2, ptr2, heap2, str2
):
    """
    Unified comparison kernel for German Strings.
    pref1 and pref2 MUST be normalized to rffi.LONGLONG.
    """
    # pref1 and pref2 are now both signed, ensuring type consistency
    prefixes_equal = pref1 == pref2

    min_len = len1 if len1 < len2 else len2
    limit_prefix = 4 if min_len > 4 else min_len

    if not prefixes_equal:
        s1_is_str = str1 is not None
        s2_is_str = str2 is not None

        for i in range(limit_prefix):
            c1 = ord(str1[i]) if s1_is_str else ord(ptr1[4 + i])
            c2 = ord(str2[i]) if s2_is_str else ord(ptr2[4 + i])
            if c1 != c2:
                return -1 if c1 < c2 else 1

    if min_len <= 4:
        if len1 < len2:
            return -1
        if len1 > len2:
            return 1
        return 0

    cursor1 = NULL_PTR
    offset1 = 0
    s1_is_str = str1 is not None

    if not s1_is_str:
        if len1 <= SHORT_STRING_THRESHOLD:
            cursor1 = rffi.ptradd(ptr1, 8)
            offset1 = -4
        else:
            u64_1 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr1, 8))[0]
            cursor1 = rffi.ptradd(heap1, rffi.cast(lltype.Signed, u64_1))
            offset1 = 0

    cursor2 = NULL_PTR
    offset2 = 0
    s2_is_str = str2 is not None

    if not s2_is_str:
        if len2 <= SHORT_STRING_THRESHOLD:
            cursor2 = rffi.ptradd(ptr2, 8)
            offset2 = -4
        else:
            u64_2 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr2, 8))[0]
            cursor2 = rffi.ptradd(heap2, rffi.cast(lltype.Signed, u64_2))
            offset2 = 0

    for i in range(4, min_len):
        c1 = ord(str1[i]) if s1_is_str else ord(cursor1[i + offset1])
        c2 = ord(str2[i]) if s2_is_str else ord(cursor2[i + offset2])
        if c1 != c2:
            return -1 if c1 < c2 else 1

    if len1 < len2:
        return -1
    if len1 > len2:
        return 1
    return 0


@jit.unroll_safe
def string_equals(struct_ptr, heap_base_ptr, search_str, search_len, search_prefix):
    """Compares a packed German String structure against a Python string."""
    u32_ptr = rffi.cast(rffi.UINTP, struct_ptr)
    len1 = rffi.cast(lltype.Signed, u32_ptr[0])
    # Cast prefix from physical buffer to signed
    pref1 = rffi.cast(rffi.LONGLONG, r_uint64(u32_ptr[1]))

    return (
        compare_structures(
            len1,
            pref1,
            struct_ptr,
            heap_base_ptr,
            None,
            search_len,
            rffi.cast(rffi.LONGLONG, search_prefix),
            NULL_PTR,
            NULL_PTR,
            search_str,
        )
        == 0
    )


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
