from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint

SHORT_STRING_THRESHOLD = 12

def compute_prefix(s):
    length = len(s)
    prefix = 0
    max_prefix_len = 4 if length > 4 else length
    for i in range(max_prefix_len):
        prefix |= ord(s[i]) << (i * 8)
    return rffi.cast(rffi.UINT, prefix)

def pack_string(target_ptr, string_data, heap_offset_if_long):
    length = len(string_data)
    u32_ptr = rffi.cast(rffi.UINTP, target_ptr)
    u32_ptr[0] = rffi.cast(rffi.UINT, length)
    
    if length <= SHORT_STRING_THRESHOLD:
        payload_ptr = rffi.ptradd(target_ptr, 4)
        for i in range(length):
            payload_ptr[i] = string_data[i]
        for i in range(length, SHORT_STRING_THRESHOLD):
            payload_ptr[i] = '\x00'
    else:
        u32_ptr[1] = compute_prefix(string_data)
        u64_payload_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target_ptr, 8))
        u64_payload_ptr[0] = rffi.cast(rffi.ULONGLONG, heap_offset_if_long)

def _memcmp(p1, p2, length):
    for i in range(length):
        if p1[i] != p2[i]:
            return False
    return True

def string_equals(struct_ptr, heap_base_ptr, search_str, search_len, search_prefix):
    u32_struct_ptr = rffi.cast(rffi.UINTP, struct_ptr)
    if rffi.cast(lltype.Signed, u32_struct_ptr[0]) != search_len:
        return False
    if search_len == 0:
        return True
    if r_uint(u32_struct_ptr[1]) != r_uint(search_prefix):
        return False
    if search_len <= 4:
        return True
    if search_len <= SHORT_STRING_THRESHOLD:
        suffix_ptr_struct = rffi.ptradd(struct_ptr, 8)
        for i in range(search_len - 4):
            if suffix_ptr_struct[i] != search_str[i + 4]:
                return False
        return True
    else:
        u64_struct_ptr = rffi.cast(rffi.ULONGLONGP, struct_ptr)
        offset = rffi.cast(lltype.Signed, u64_struct_ptr[1])
        heap_ptr = rffi.ptradd(heap_base_ptr, offset)
        return _memcmp(heap_ptr, search_str, search_len)

def string_equals_dual(ptr1, heap1, ptr2, heap2):
    u32_p1 = rffi.cast(rffi.UINTP, ptr1)
    u32_p2 = rffi.cast(rffi.UINTP, ptr2)
    len1 = rffi.cast(lltype.Signed, u32_p1[0])
    len2 = rffi.cast(lltype.Signed, u32_p2[0])
    if len1 != len2: return False
    if len1 == 0: return True
    if r_uint(u32_p1[1]) != r_uint(u32_p2[1]): return False
    if len1 <= 4: return True
    if len1 <= SHORT_STRING_THRESHOLD:
        p1_char = rffi.ptradd(ptr1, 8)
        p2_char = rffi.ptradd(ptr2, 8)
        for i in range(len1 - 4):
            if p1_char[i] != p2_char[i]: return False
        return True
    else:
        u64_p1 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr1, 8))
        u64_p2 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr2, 8))
        h1_ptr = rffi.ptradd(heap1, rffi.cast(lltype.Signed, u64_p1[0]))
        h2_ptr = rffi.ptradd(heap2, rffi.cast(lltype.Signed, u64_p2[0]))
        for i in range(len1):
            if h1_ptr[i] != h2_ptr[i]: return False
        return True

@jit.unroll_safe
def string_compare(ptr1, heap1, ptr2, heap2):
    u32_p1 = rffi.cast(rffi.UINTP, ptr1)
    u32_p2 = rffi.cast(rffi.UINTP, ptr2)
    len1 = rffi.cast(lltype.Signed, u32_p1[0])
    len2 = rffi.cast(lltype.Signed, u32_p2[0])
    min_len = len1 if len1 < len2 else len2

    # 1. Compare first 4 bytes (Prefix)
    # Treat as bytes to avoid endianness confusion in lexicographical order
    pref_p1 = rffi.ptradd(ptr1, 4)
    pref_p2 = rffi.ptradd(ptr2, 4)
    pref_limit = 4 if min_len > 4 else min_len
    for i in range(pref_limit):
        if pref_p1[i] < pref_p2[i]: return -1
        if pref_p1[i] > pref_p2[i]: return 1
    
    if min_len <= 4:
        if len1 < len2: return -1
        if len1 > len2: return 1
        return 0

    # 2. Resolve data sources for character sequence 4..min_len
    # Short strings store index 4 at bytes 8..15 (offset -4)
    # Long strings store index 0 at heap+offset (offset 0)
    if len1 <= SHORT_STRING_THRESHOLD:
        data1 = rffi.ptradd(ptr1, 8)
        offset1 = -4
    else:
        u64_1 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr1, 8))[0]
        data1 = rffi.ptradd(heap1, rffi.cast(lltype.Signed, u64_1))
        offset1 = 0

    if len2 <= SHORT_STRING_THRESHOLD:
        data2 = rffi.ptradd(ptr2, 8)
        offset2 = -4
    else:
        u64_2 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr2, 8))[0]
        data2 = rffi.ptradd(heap2, rffi.cast(lltype.Signed, u64_2))
        offset2 = 0

    # 3. Efficient linear comparison loop
    for i in range(4, min_len):
        c1 = data1[i + offset1]
        c2 = data2[i + offset2]
        if c1 < c2: return -1
        if c1 > c2: return 1

    if len1 < len2: return -1
    if len1 > len2: return 1
    return 0
