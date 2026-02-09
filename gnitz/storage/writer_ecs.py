import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.jit import unrolling_iterable
from gnitz.storage import layout, mmap_posix, errors
from gnitz.core import types, strings as string_logic, checksum

FIELD_INDICES = unrolling_iterable(range(64))

def _align_64(val): return (val + 63) & ~63
def _copy_memory(dest_ptr, src_ptr, size):
    for i in range(size): dest_ptr[i] = src_ptr[i]

class RawBuffer(object):
    def __init__(self, item_size, initial_capacity=4096):
        self.item_size = item_size
        self.count = 0
        self.capacity = initial_capacity
        self.ptr = lltype.malloc(rffi.CCHARP.TO, self.capacity * self.item_size, flavor='raw')

    def ensure_capacity(self, needed_slots):
        new_count = self.count + needed_slots
        if new_count > self.capacity:
            new_cap = max(self.capacity * 2, new_count)
            new_ptr = lltype.malloc(rffi.CCHARP.TO, new_cap * self.item_size, flavor='raw')
            if self.ptr:
                _copy_memory(new_ptr, self.ptr, self.count * self.item_size)
                lltype.free(self.ptr, flavor='raw')
            self.ptr = new_ptr
            self.capacity = new_cap

    def append_i64(self, value):
        self.ensure_capacity(1)
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(self.ptr, self.count * 8))[0] = rffi.cast(rffi.LONGLONG, value)
        self.count += 1

    def append_u64(self, value):
        self.ensure_capacity(1)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, self.count * 8))[0] = rffi.cast(rffi.ULONGLONG, value)
        self.count += 1

    def append_bytes(self, source_ptr, length):
        self.ensure_capacity(length)
        _copy_memory(rffi.ptradd(self.ptr, self.count * self.item_size), source_ptr, length * self.item_size)
        self.count += length
        
    def append_from_string(self, s):
        l = len(s)
        self.ensure_capacity(l)
        dest = rffi.ptradd(self.ptr, self.count)
        for i in range(l): dest[i] = s[i]
        self.count += l

    def free(self):
        if self.ptr:
            lltype.free(self.ptr, flavor='raw')
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)

class ECSShardWriter(object):
    def __init__(self, component_layout):
        self.layout = component_layout
        self.count = 0
        self.e_buf = RawBuffer(8)
        self.w_buf = RawBuffer(8)
        self.c_buf = RawBuffer(self.layout.stride)
        self.b_buf = RawBuffer(1)
        self.scratch_row = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')

    def add_entity(self, entity_id, *field_values):
        self._add_entity_weighted(entity_id, 1, *field_values)
    
    def _add_entity_weighted(self, entity_id, weight, *field_values):
        if weight == 0: return
        self.count += 1
        self.e_buf.append_u64(entity_id) # Unsigned EID
        self.w_buf.append_i64(weight)
        stride = self.layout.stride
        for k in range(stride): self.scratch_row[k] = '\x00'
        for i in FIELD_INDICES:
            if i >= len(field_values) or i >= len(self.layout.field_types): break
            val = field_values[i]
            ft = self.layout.field_types[i]
            dest = rffi.ptradd(self.scratch_row, self.layout.field_offsets[i])
            if ft == types.TYPE_STRING:
                s_val = str(val)
                ho = 0
                if len(s_val) > string_logic.SHORT_STRING_THRESHOLD:
                    ho = self.b_buf.count
                    self.b_buf.append_from_string(s_val)
                string_logic.pack_string(dest, s_val, ho)
            elif isinstance(val, (int, long)):
                rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(rffi.LONGLONG, val)
            elif isinstance(val, float):
                rffi.cast(rffi.DOUBLEP, dest)[0] = rffi.cast(rffi.DOUBLE, val)
        self.c_buf.append_bytes(self.scratch_row, 1)

    def add_packed_row(self, entity_id, weight, source_row_ptr, source_heap_ptr):
        if weight == 0: return
        self.count += 1
        self.e_buf.append_u64(entity_id) # Unsigned EID
        self.w_buf.append_i64(weight)
        stride = self.layout.stride
        self.c_buf.ensure_capacity(1)
        dest_c = rffi.ptradd(self.c_buf.ptr, (self.c_buf.count) * stride)
        _copy_memory(dest_c, source_row_ptr, stride)
        for i in FIELD_INDICES:
            if i >= len(self.layout.field_types): break
            if self.layout.field_types[i] == types.TYPE_STRING:
                s_ptr = rffi.ptradd(dest_c, self.layout.field_offsets[i])
                length = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, s_ptr)[0])
                if length > string_logic.SHORT_STRING_THRESHOLD:
                    u64_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(s_ptr, 8))
                    old_off = rffi.cast(lltype.Signed, u64_ptr[0])
                    new_off = self.b_buf.count
                    self.b_buf.append_bytes(rffi.ptradd(source_heap_ptr, old_off), length)
                    u64_ptr[0] = rffi.cast(rffi.ULONGLONG, new_off)
        self.c_buf.count += 1

    def finalize(self, filename):
        fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            off_e = layout.HEADER_SIZE
            size_e = self.count * 8
            off_w = _align_64(off_e + size_e)
            size_w = self.count * 8
            off_c = _align_64(off_w + size_w)
            size_c = self.count * self.layout.stride
            off_b = _align_64(off_c + size_c)
            size_b = self.b_buf.count
            cs_e = checksum.compute_checksum(self.e_buf.ptr, size_e)
            cs_w = checksum.compute_checksum(self.w_buf.ptr, size_w)
            h = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor='raw')
            try:
                for i in range(layout.HEADER_SIZE): h[i] = '\x00'
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_MAGIC))[0] = rffi.cast(rffi.LONGLONG, layout.MAGIC_NUMBER)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_COUNT))[0] = rffi.cast(rffi.LONGLONG, self.count)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(h, layout.OFF_CHECKSUM_E))[0] = rffi.cast(rffi.ULONGLONG, cs_e)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(h, layout.OFF_CHECKSUM_W))[0] = rffi.cast(rffi.ULONGLONG, cs_w)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_E_ECS))[0] = rffi.cast(rffi.LONGLONG, off_e)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_W_ECS))[0] = rffi.cast(rffi.LONGLONG, off_w)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_C_ECS))[0] = rffi.cast(rffi.LONGLONG, off_c)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_B_ECS))[0] = rffi.cast(rffi.LONGLONG, off_b)
                mmap_posix.write_c(fd, h, rffi.cast(rffi.SIZE_T, layout.HEADER_SIZE))
            finally: lltype.free(h, flavor='raw')
            if size_e > 0: mmap_posix.write_c(fd, self.e_buf.ptr, rffi.cast(rffi.SIZE_T, size_e))
            self._write_padding(fd, off_w - (off_e + size_e))
            if size_w > 0: mmap_posix.write_c(fd, self.w_buf.ptr, rffi.cast(rffi.SIZE_T, size_w))
            self._write_padding(fd, off_c - (off_w + size_w))
            if size_c > 0: mmap_posix.write_c(fd, self.c_buf.ptr, rffi.cast(rffi.SIZE_T, size_c))
            self._write_padding(fd, off_b - (off_c + size_c))
            if size_b > 0: mmap_posix.write_c(fd, self.b_buf.ptr, rffi.cast(rffi.SIZE_T, size_b))
        finally:
            rposix.close(fd)
            self.e_buf.free()
            self.w_buf.free()
            self.c_buf.free()
            self.b_buf.free()
            if self.scratch_row: lltype.free(self.scratch_row, flavor='raw')

    def _write_padding(self, fd, count):
        if count <= 0: return
        p = lltype.malloc(rffi.CCHARP.TO, count, flavor='raw')
        try:
            for i in range(count): p[i] = '\x00'
            mmap_posix.write_c(fd, p, rffi.cast(rffi.SIZE_T, count))
        finally: lltype.free(p, flavor='raw')
