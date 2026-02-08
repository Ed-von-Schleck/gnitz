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
    
    # Use r_uint for safe comparison of UINT types in RPython
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
    """
    Compares two German Strings stored in memory.
    Used for Compaction and SkipList equality checks.
    """
    u32_p1 = rffi.cast(rffi.UINTP, ptr1)
    u32_p2 = rffi.cast(rffi.UINTP, ptr2)
    
    len1 = rffi.cast(lltype.Signed, u32_p1[0])
    len2 = rffi.cast(lltype.Signed, u32_p2[0])
    
    if len1 != len2: return False
    if len1 == 0: return True
    
    # Compare Prefix (bytes 4-7, encoded in u32[1])
    # Use r_uint for safe comparison
    if r_uint(u32_p1[1]) != r_uint(u32_p2[1]): return False
    
    if len1 <= 4: return True

    if len1 <= SHORT_STRING_THRESHOLD:
        # Inline comparison for bytes 8-15
        p1_char = rffi.ptradd(ptr1, 8)
        p2_char = rffi.ptradd(ptr2, 8)
        for i in range(len1 - 4):
            if p1_char[i] != p2_char[i]: return False
        return True
    else:
        # Heap comparison
        u64_p1 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr1, 8))
        u64_p2 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr2, 8))
        
        off1 = rffi.cast(lltype.Signed, u64_p1[0])
        off2 = rffi.cast(lltype.Signed, u64_p2[0])
        
        h1_ptr = rffi.ptradd(heap1, off1)
        h2_ptr = rffi.ptradd(heap2, off2)
        
        # Check full string (could optimize starting at 4, but memcmp is fast)
        for i in range(len1):
            if h1_ptr[i] != h2_ptr[i]: return False
        return True

def string_compare(ptr1, heap1, ptr2, heap2):
    """
    Lexicographical comparison of two German Strings.
    Returns -1 if s1 < s2, 1 if s1 > s2, 0 if equal.
    """
    u32_p1 = rffi.cast(rffi.UINTP, ptr1)
    u32_p2 = rffi.cast(rffi.UINTP, ptr2)
    
    len1 = rffi.cast(lltype.Signed, u32_p1[0])
    len2 = rffi.cast(lltype.Signed, u32_p2[0])
    
    min_len = len1 if len1 < len2 else len2
    
    # Prefix comparison (valid for first 4 bytes due to LE packing)
    if min_len > 0:
        pref1 = r_uint(u32_p1[1])
        pref2 = r_uint(u32_p2[1])
        if pref1 != pref2:
            if pref1 < pref2: return -1
            return 1
            
    # Iterate remaining characters
    idx = 4
    while idx < min_len:
        c1 = '\x00'
        c2 = '\x00'
        
        # Get c1
        if len1 <= SHORT_STRING_THRESHOLD:
            c1 = rffi.ptradd(ptr1, 8 + (idx - 4))[0]
        else:
             u64 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr1, 8))[0]
             off = rffi.cast(lltype.Signed, u64)
             c1 = rffi.ptradd(heap1, off)[idx]

        # Get c2
        if len2 <= SHORT_STRING_THRESHOLD:
            c2 = rffi.ptradd(ptr2, 8 + (idx - 4))[0]
        else:
             u64 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr2, 8))[0]
             off = rffi.cast(lltype.Signed, u64)
             c2 = rffi.ptradd(heap2, off)[idx]

        if c1 < c2: return -1
        if c1 > c2: return 1
        idx += 1
        
    if len1 < len2: return -1
    if len1 > len2: return 1
    return 0
