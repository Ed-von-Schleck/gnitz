"""
gnitz/storage/shard_ecs.py
"""
import os
from rpython.rlib import jit, rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import buffer, layout, mmap_posix, errors
from gnitz.core import strings as string_logic

class ECSShardView(object):
    _immutable_fields_ = [
        'count', 'layout', 'ptr', 'size',
        'buf_e', 'buf_c', 'buf_b', 'buf_w'
    ]

    def __init__(self, filename, component_layout):
        self.layout = component_layout
        
        fd = rposix.open(filename, os.O_RDONLY, 0)
        try:
            st = os.fstat(fd)
            self.size = st.st_size
            if self.size < layout.HEADER_SIZE:
                raise errors.CorruptShardError("File smaller than header")
            self.ptr = mmap_posix.mmap_file(fd, self.size, mmap_posix.PROT_READ, mmap_posix.MAP_SHARED)
        finally:
            rposix.close(fd)

        header = buffer.MappedBuffer(self.ptr, layout.HEADER_SIZE)
        if header.read_i64(layout.OFF_MAGIC) != layout.MAGIC_NUMBER:
            raise errors.CorruptShardError("Invalid magic number")

        self.count = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_COUNT))
        
        off_e = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_E_ECS))
        off_c = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_C_ECS))
        off_b = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_B_ECS))
        off_w = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_W_ECS))

        self.buf_e = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_e), off_w - off_e)
        self.buf_w = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_w), off_c - off_w)
        self.buf_c = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_c), off_b - off_c)
        self.buf_b = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_b), self.size - off_b)

    def get_entity_id(self, index):
        return self.buf_e.read_i64(index * 8)

    def get_weight(self, index):
        return self.buf_w.read_i64(index * 8)

    @jit.look_inside_iff(lambda self, index: jit.isconstant(self.layout))
    def get_data_ptr(self, index):
        offset = index * self.layout.stride
        return self.buf_c.get_raw_ptr(offset)
    
    @jit.elidable
    def find_entity_index(self, entity_id):
        low = 0
        high = self.count - 1
        while low <= high:
            mid = (low + high) / 2
            eid_at_mid = self.get_entity_id(mid)
            if eid_at_mid == entity_id:
                return mid
            if eid_at_mid < entity_id:
                low = mid + 1
            else:
                high = mid - 1
        return -1

    def read_field_i64(self, index, field_idx):
        ptr = self.get_data_ptr(index)
        field_off = self.layout.get_field_offset(field_idx)
        return rffi.cast(rffi.LONGLONGP, rffi.ptradd(ptr, field_off))[0]

    def string_field_equals(self, index, field_idx, search_str):
        ptr = self.get_data_ptr(index)
        field_off = self.layout.get_field_offset(field_idx)
        struct_ptr = rffi.ptradd(ptr, field_off)
        heap_base_ptr = self.buf_b.ptr
        
        # Precompute metadata for the optimized check
        search_len = len(search_str)
        search_prefix = string_logic.compute_prefix(search_str)
        
        return string_logic.string_equals(struct_ptr, heap_base_ptr, search_str, search_len, search_prefix)

    def close(self):
        mmap_posix.munmap_file(self.ptr, self.size)
