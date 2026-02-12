from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_int64

def align_64(val):
    return (val + 63) & ~63

def copy_memory(dest_ptr, src_ptr, size):
    for i in range(size):
        dest_ptr[i] = src_ptr[i]

def compare_memory(p1, p2, size):
    for i in range(size):
        if p1[i] != p2[i]:
            return False
    return True

class RawBuffer(object):
    """Raw byte buffer for columnar regions."""

    def __init__(self, item_size, initial_capacity=4096):
        self.item_size = item_size
        self.count = 0
        self.capacity = initial_capacity
        self.ptr = lltype.malloc(
            rffi.CCHARP.TO, self.capacity * self.item_size, flavor="raw"
        )

    def ensure_capacity(self, needed_slots):
        new_count = self.count + needed_slots
        if new_count > self.capacity:
            new_cap = max(self.capacity * 2, new_count)
            new_ptr = lltype.malloc(rffi.CCHARP.TO, new_cap * self.item_size, flavor="raw")
            if self.ptr:
                copy_memory(new_ptr, self.ptr, self.count * self.item_size)
                lltype.free(self.ptr, flavor="raw")
            self.ptr = new_ptr
            self.capacity = new_cap

    def append_u128(self, value):
        self.ensure_capacity(1)
        target = rffi.cast(rffi.CCHARP, rffi.ptradd(self.ptr, self.count * 16))
        rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(value))
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(
            rffi.ULONGLONG, r_uint64(value >> 64)
        )
        self.count += 1

    def append_u64(self, value):
        self.ensure_capacity(1)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, self.count * 8))[0] = rffi.cast(
            rffi.ULONGLONG, r_uint64(value)
        )
        self.count += 1

    def append_i64(self, value):
        self.ensure_capacity(1)
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(self.ptr, self.count * 8))[0] = rffi.cast(
            rffi.LONGLONG, value
        )
        self.count += 1

    def append_bytes(self, source_ptr, length_in_items):
        self.ensure_capacity(length_in_items)
        dest = rffi.ptradd(self.ptr, self.count * self.item_size)
        if source_ptr:
            copy_memory(dest, source_ptr, length_in_items * self.item_size)
        else:
            for i in range(length_in_items * self.item_size):
                dest[i] = "\x00"
        self.count += length_in_items

    def free(self):
        if self.ptr:
            lltype.free(self.ptr, flavor="raw")
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)
