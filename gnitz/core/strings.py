from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint, r_uint32

# German String Constants:
# 16-byte total: 4 (len) | 4 (prefix) | 8 (suffix OR heap offset)
SHORT_STRING_THRESHOLD = 12

def compute_prefix(s):
    """Computes a 4-byte prefix from a string for O(1) equality failure."""
    length = len(s)
    prefix = r_uint(0)
    # Pack up to 4 bytes into a uint32
    max_prefix_len = 4 if length > 4 else length
    for i in range(max_prefix_len):
        prefix |= r_uint(ord(s[i])) << (i * 8)
    return rffi.cast(rffi.UINT, prefix)

def pack_string(target_ptr, string_data, heap_offset_if_long):
    """Packs a string into the 16-byte German String structure."""
    length = len(string_data)
    u32_ptr = rffi.cast(rffi.UINTP, target_ptr)
    
    # Bytes 0-3: Length
    u32_ptr[0] = rffi.cast(rffi.UINT, length)
    
    # Bytes 4-7: Prefix
    u32_ptr[1] = compute_prefix(string_data)
    
    if length <= SHORT_STRING_THRESHOLD:
        # Bytes 8-15: Inline Suffix (starts from index 4)
        payload_ptr = rffi.ptradd(target_ptr, 8)
        start_idx = 4
        for i in range(length - start_idx if length > start_idx else 0):
            payload_ptr[i] = string_data[i + start_idx]
        # Zero-pad remaining bytes for deterministic memory
        for i in range(length - start_idx if length > start_idx else 0, 8):
            payload_ptr[i] = '\x00'
    else:
        # Bytes 8-15: 64-bit Heap Offset
        u64_payload_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target_ptr, 8))
        u64_payload_ptr[0] = rffi.cast(rffi.ULONGLONG, heap_offset_if_long)

def _memcmp(p1, p2, length):
    """Internal helper for raw memory comparison."""
    for i in range(length):
        if p1[i] != p2[i]:
            return False
    return True

@jit.unroll_safe
def string_equals(struct_ptr, heap_base_ptr, search_str, search_len, search_prefix):
    """Compares a packed German String structure against a Python string."""
    u32_ptr = rffi.cast(rffi.UINTP, struct_ptr)
    
    # 1. Length Check
    if rffi.cast(lltype.Signed, u32_ptr[0]) != search_len:
        return False
    if search_len == 0:
        return True
    
    # 2. Prefix Check
    if rffi.cast(lltype.Signed, u32_ptr[1]) != rffi.cast(lltype.Signed, search_prefix):
        return False
    
    # 3. Content Check
    if search_len <= 4:
        return True
    
    if search_len <= SHORT_STRING_THRESHOLD:
        # Check inline suffix
        suffix_ptr = rffi.ptradd(struct_ptr, 8)
        for i in range(search_len - 4):
            if suffix_ptr[i] != search_str[i + 4]:
                return False
        return True
    else:
        # Check heap
        off_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(struct_ptr, 8))
        offset = rffi.cast(lltype.Signed, off_ptr[0])
        heap_ptr = rffi.ptradd(heap_base_ptr, offset)
        return _memcmp(heap_ptr, search_str, search_len)

@jit.unroll_safe
def string_equals_dual(ptr1, heap1, ptr2, heap2):
    """Compares two packed German String structures."""
    u32_p1 = rffi.cast(rffi.UINTP, ptr1)
    u32_p2 = rffi.cast(rffi.UINTP, ptr2)
    
    len1 = rffi.cast(lltype.Signed, u32_p1[0])
    if len1 != rffi.cast(lltype.Signed, u32_p2[0]):
        return False
    if len1 == 0:
        return True
    
    # Prefix Check
    if rffi.cast(lltype.Signed, u32_p1[1]) != rffi.cast(lltype.Signed, u32_p2[1]):
        return False
        
    if len1 <= 4:
        return True
    
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
        return _memcmp(h1_ptr, h2_ptr, len1)

@jit.unroll_safe
def compare_db_value_to_german(val_obj, german_ptr, heap_ptr):
    """
    Dry-run comparison: Compare a StringValue object against a packed structure.
    Returns: -1 if structure < value, 1 if structure > value, 0 if equal.
    """
    s2 = val_obj.get_string()
    len2 = len(s2)
    u32_ptr = rffi.cast(rffi.UINTP, german_ptr)
    len1 = rffi.cast(lltype.Signed, u32_ptr[0])
    min_len = len1 if len1 < len2 else len2

    # 1. Compare prefix bytes (first 4 bytes of the string)
    pref_ptr = rffi.ptradd(german_ptr, 4)
    i = 0
    limit = 4 if min_len > 4 else min_len
    while i < limit:
        c1 = ord(pref_ptr[i])
        c2 = ord(s2[i])
        if c1 < c2: return -1
        if c1 > c2: return 1
        i += 1
    
    # 2. Compare remaining bytes if lengths match beyond the prefix
    if min_len > 4:
        if len1 <= SHORT_STRING_THRESHOLD:
            # Short string data is in the 8-byte suffix (indices 4 to 11)
            data1_ptr = rffi.ptradd(german_ptr, 8)
            i = 4
            while i < min_len:
                c1 = ord(data1_ptr[i - 4])
                c2 = ord(s2[i])
                if c1 < c2: return -1
                if c1 > c2: return 1
                i += 1
        else:
            # Long string data is in the heap (entire string)
            off_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(german_ptr, 8))
            off1 = rffi.cast(lltype.Signed, off_ptr[0])
            h_ptr = rffi.ptradd(heap_ptr, off1)
            i = 4
            while i < min_len:
                c1 = ord(h_ptr[i])
                c2 = ord(s2[i])
                if c1 < c2: return -1
                if c1 > c2: return 1
                i += 1

    # 3. All bytes match up to min_len, the shorter string is lexicographically smaller
    if len1 < len2: return -1
    if len1 > len2: return 1
    return 0

@jit.unroll_safe
def string_compare(ptr1, heap1, ptr2, heap2):
    """Lexicographical comparison of two packed German Strings."""
    u32_p1 = rffi.cast(rffi.UINTP, ptr1)
    u32_p2 = rffi.cast(rffi.UINTP, ptr2)
    len1 = rffi.cast(lltype.Signed, u32_p1[0])
    len2 = rffi.cast(lltype.Signed, u32_p2[0])
    min_len = len1 if len1 < len2 else len2

    # Compare Prefix bytes directly
    pref_p1 = rffi.ptradd(ptr1, 4)
    pref_p2 = rffi.ptradd(ptr2, 4)
    for i in range(4 if min_len > 4 else min_len):
        c1, c2 = ord(pref_p1[i]), ord(pref_p2[i])
        if c1 < c2: return -1
        if c1 > c2: return 1
    
    if min_len <= 4:
        if len1 < len2: return -1
        if len1 > len2: return 1
        return 0

    # Locate Suffix/Heap data for remainder
    if len1 <= SHORT_STRING_THRESHOLD:
        data1, offset1 = rffi.ptradd(ptr1, 8), -4
    else:
        u64_1 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr1, 8))[0]
        data1, offset1 = rffi.ptradd(heap1, rffi.cast(lltype.Signed, u64_1)), 0

    if len2 <= SHORT_STRING_THRESHOLD:
        data2, offset2 = rffi.ptradd(ptr2, 8), -4
    else:
        u64_2 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr2, 8))[0]
        data2, offset2 = rffi.ptradd(heap2, rffi.cast(lltype.Signed, u64_2)), 0

    for i in range(4, min_len):
        c1, c2 = ord(data1[i + offset1]), ord(data2[i + offset2])
        if c1 < c2: return -1
        if c1 > c2: return 1

    if len1 < len2: return -1
    if len1 > len2: return 1
    return 0
