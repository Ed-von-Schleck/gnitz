# gnitz/core/strings.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint, r_uint32, r_uint64

# German String Constants:
# 16-byte total: 4 (len) | 4 (prefix) | 8 (suffix OR heap offset)
SHORT_STRING_THRESHOLD = 12
NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class BlobAllocator(object):
    """
    Abstract strategy for allocating long string blobs.
    Used during serialization to decouple the German String structure
    from specific storage backends (MemTable, WAL, Shards).
    """

    def allocate(self, string_data):
        """
        Allocates space for the string data in the backing store,
        copies the data, and returns the 64-bit offset to be stored
        in the struct.
        """
        raise NotImplementedError

    def allocate_from_ptr(self, src_ptr, length):
        """
        Zero-copy relocation from a raw C pointer.
        Copies `length` bytes from `src_ptr` into the backing store
        and returns the 64-bit offset to be stored in the struct.
        """
        raise NotImplementedError


def compute_prefix(s):
    """Computes a 4-byte prefix from a string for O(1) equality failure."""
    length = len(s)
    prefix = r_uint(0)
    # Pack up to 4 bytes into a uint32
    max_prefix_len = 4 if length > 4 else length
    for i in range(max_prefix_len):
        prefix |= r_uint(ord(s[i])) << (i * 8)
    return rffi.cast(rffi.UINT, prefix)


@jit.unroll_safe
def relocate_string(target, length, prefix, src_struct_ptr, src_heap_ptr, py_string, allocator):
    """
    Unified German String packing kernel.

    Writes exactly 16 bytes to `target` from either a Python string
    (VM layer) or raw struct/heap pointers (Storage layer).

    Layout written:
        Bytes  0- 3: Length   (uint32, unused bits zeroed)
        Bytes  4- 7: Prefix   (uint32, unused bits zeroed)
        Bytes  8-15: Payload  (8-byte inline suffix OR 64-bit heap offset)

    Args:
        target:        Pointer to the 16-byte destination.
        length:        String length (Python int / Signed).
        prefix:        Precomputed prefix word (Signed, as returned by
                       get_str_struct or compute_prefix).
        src_struct_ptr: Pointer to source 16-byte German String struct.
                        May be NULL_PTR when py_string is provided.
        src_heap_ptr:  Pointer to source blob heap base.
                       May be NULL_PTR when py_string is provided.
        py_string:     Python str if source is a PayloadRow, else None.
        allocator:     BlobAllocator instance; only called for long strings.
    """
    u32_target = rffi.cast(rffi.UINTP, target)

    # --- Bytes 0-3: Length ---
    # Cast to UINT so the full 4-byte slot is always written deterministically.
    u32_target[0] = rffi.cast(rffi.UINT, length)

    # --- Bytes 4-7: Prefix ---
    # Writing as uint32 guarantees unused prefix bytes are zeroed even when
    # the string is shorter than 4 characters.
    if py_string is not None:
        u32_target[1] = compute_prefix(py_string)
    else:
        # Trust the prefix already extracted by get_str_struct; copy the raw
        # uint32 word directly to avoid a redundant O(N) scan.
        u32_target[1] = rffi.cast(rffi.UINT, prefix)

    # --- Bytes 8-15: Payload ---
    payload_target = rffi.ptradd(target, 8)

    if length <= SHORT_STRING_THRESHOLD:
        # Inline suffix: logical bytes [4, length) live at payload_target[0..]
        if py_string is not None:
            # Source is a Python string.
            suffix_len = length - 4 if length > 4 else 0
            for j in range(suffix_len):
                payload_target[j] = py_string[j + 4]
            # Zero-pad the remaining bytes of the 8-byte payload slot.
            for j in range(suffix_len, 8):
                payload_target[j] = "\x00"
        else:
            # Source is a raw struct pointer.  The inline suffix lives at
            # src_struct_ptr+8 (logical bytes 4-11 of the original string).
            suffix_len = length - 4 if length > 4 else 0
            src_suffix = rffi.ptradd(src_struct_ptr, 8)
            for j in range(suffix_len):
                payload_target[j] = src_suffix[j]
            for j in range(suffix_len, 8):
                payload_target[j] = "\x00"
    else:
        # Long string: store a 64-bit offset into the blob heap.
        if py_string is not None:
            new_offset = allocator.allocate(py_string)
        else:
            # Read the old heap offset stored at src_struct_ptr+8 and perform
            # a zero-copy relocation via the raw-pointer allocator path.
            old_offset_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(src_struct_ptr, 8))
            old_offset = old_offset_ptr[0]
            src_data_ptr = rffi.ptradd(src_heap_ptr, rffi.cast(lltype.Signed, old_offset))
            new_offset = allocator.allocate_from_ptr(src_data_ptr, length)

        rffi.cast(rffi.ULONGLONGP, payload_target)[0] = rffi.cast(
            rffi.ULONGLONG, new_offset
        )


def pack_and_write_blob(target_ptr, string_data, allocator):
    """
    Centralized logic for serializing a Python string into a German String struct.

    Delegates all structural knowledge to `relocate_string`, ensuring this
    path and the raw-pointer relocation path share exactly one implementation.
    """
    relocate_string(
        target_ptr,
        len(string_data),
        0,          # prefix will be recomputed from py_string inside the kernel
        NULL_PTR,
        NULL_PTR,
        string_data,
        allocator,
    )


def pack_string(target_ptr, string_data, heap_offset_if_long):
    """
    Low-level primitive to pack a string into the 16-byte German String structure.
    If length > SHORT_STRING_THRESHOLD, the provided heap_offset_if_long is stored.

    Retained for call sites (e.g. unpack round-trip tests) that supply a
    pre-allocated heap offset rather than a BlobAllocator.  Hot ingestion paths
    should prefer pack_and_write_blob or relocate_string.
    """
    length = len(string_data)
    u32_ptr = rffi.cast(rffi.UINTP, target_ptr)

    # Bytes 0-3: Length
    u32_ptr[0] = rffi.cast(rffi.UINT, length)

    # Bytes 4-7: Prefix
    u32_ptr[1] = compute_prefix(string_data)

    if length <= SHORT_STRING_THRESHOLD:
        # Bytes 8-15: Inline Suffix (starts from index 4)
        payload_ptr = rffi.ptradd(target_ptr, 8)
        suffix_len = length - 4 if length > 4 else 0
        for i in range(suffix_len):
            payload_ptr[i] = string_data[i + 4]
        # Zero-pad remaining bytes for deterministic memory
        for i in range(suffix_len, 8):
            payload_ptr[i] = "\x00"
    else:
        # Bytes 8-15: 64-bit Heap Offset
        u64_payload_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target_ptr, 8))
        u64_payload_ptr[0] = rffi.cast(rffi.ULONGLONG, heap_offset_if_long)


@jit.unroll_safe
def compare_structures(
    len1, pref1, ptr1, heap1, str1, len2, pref2, ptr2, heap2, str2
):
    """
    Unified comparison kernel for German Strings.
    Handles (Struct vs Struct), (Struct vs String), (String vs String).
    Returns -1 if LHS < RHS, 1 if LHS > RHS, 0 if Equal.
    """
    prefixes_equal = pref1 == pref2

    min_len = len1 if len1 < len2 else len2
    limit_prefix = 4 if min_len > 4 else min_len

    # 1. Prefix Check for Optimization (Equality only)
    if not prefixes_equal:
        s1_is_str = str1 is not None
        s2_is_str = str2 is not None

        for i in range(limit_prefix):
            c1 = ord(str1[i]) if s1_is_str else ord(ptr1[4 + i])
            c2 = ord(str2[i]) if s2_is_str else ord(ptr2[4 + i])
            if c1 != c2:
                return -1 if c1 < c2 else 1

    # 2. Short String Termination
    if min_len <= 4:
        if len1 < len2:
            return -1
        if len1 > len2:
            return 1
        return 0

    # 3. Suffix / Heap Comparison (Bytes 4..min_len)

    # Resolve Cursor 1
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

    # Resolve Cursor 2
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

    # Comparison Loop
    for i in range(4, min_len):
        c1 = ord(str1[i]) if s1_is_str else ord(cursor1[i + offset1])
        c2 = ord(str2[i]) if s2_is_str else ord(cursor2[i + offset2])
        if c1 != c2:
            return -1 if c1 < c2 else 1

    # 4. Length Tie-breaker
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
    pref1 = rffi.cast(lltype.Signed, u32_ptr[1])

    return (
        compare_structures(
            len1,
            pref1,
            struct_ptr,
            heap_base_ptr,
            None,
            search_len,
            rffi.cast(lltype.Signed, search_prefix),
            NULL_PTR,
            NULL_PTR,
            search_str,
        )
        == 0
    )


@jit.unroll_safe
def unpack_string(struct_ptr, heap_base_ptr):
    """
    Reverse of pack_string. Extracts a Python string from a 16-byte German String.
    - struct_ptr: Pointer to the 16-byte structure.
    - heap_base_ptr: Pointer to the start of the blob heap (Region B or WAL tail).
    """
    u32_ptr = rffi.cast(rffi.UINTP, struct_ptr)
    length = rffi.cast(lltype.Signed, u32_ptr[0])

    if length == 0:
        return ""

    if length <= SHORT_STRING_THRESHOLD:
        # Reconstruct from Prefix (4 bytes) and Suffix (8 bytes)
        # Logical Bytes 0-3 are in the prefix slot (offset 4)
        # Logical Bytes 4-11 are in the suffix slot (offset 8)
        take_prefix = 4 if length > 4 else length
        res = rffi.charpsize2str(rffi.ptradd(struct_ptr, 4), take_prefix)
        if length > 4:
            res += rffi.charpsize2str(rffi.ptradd(struct_ptr, 8), length - 4)
        return res
    else:
        # Extract from Heap
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
