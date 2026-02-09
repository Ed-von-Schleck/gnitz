import os
from rpython.rlib import jit, rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import buffer, layout, mmap_posix, errors
from gnitz.core import strings as string_logic, checksum

class ECSShardView(object):
    # _c_validated and _b_validated MUST NOT be in _immutable_fields_
    _immutable_fields_ = ['count', 'layout', 'ptr', 'size', 'buf_e', 'buf_c', 'buf_b', 'buf_w', 'cs_c', 'cs_b']

    def __init__(self, filename, component_layout, validate_checksums=True):
        self.layout = component_layout
        fd = rposix.open(filename, os.O_RDONLY, 0)
        self.ptr = lltype.nullptr(rffi.CCHARP.TO) # Initialize to null for safety
        try:
            st = os.fstat(fd)
            self.size = st.st_size
            if self.size < (layout.HEADER_SIZE + layout.FOOTER_SIZE): 
                raise errors.CorruptShardError("File too small")
            
            # Map the file
            self.ptr = mmap_posix.mmap_file(fd, self.size, mmap_posix.PROT_READ, mmap_posix.MAP_SHARED)
            
            # Initialization logic wrapped in try-except to ensure unmap on failure
            try:
                header = buffer.MappedBuffer(self.ptr, layout.HEADER_SIZE)
                if header.read_u64(layout.OFF_MAGIC) != layout.MAGIC_NUMBER: 
                    raise errors.CorruptShardError("Magic mismatch")
                
                self.count = rffi.cast(lltype.Signed, header.read_u64(layout.OFF_COUNT))
                off_e = rffi.cast(lltype.Signed, header.read_u64(layout.OFF_REG_E_ECS))
                off_c = rffi.cast(lltype.Signed, header.read_u64(layout.OFF_REG_C_ECS))
                off_b = rffi.cast(lltype.Signed, header.read_u64(layout.OFF_REG_B_ECS))
                off_w = rffi.cast(lltype.Signed, header.read_u64(layout.OFF_REG_W_ECS))
                
                self.buf_e = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_e), off_w - off_e)
                self.buf_w = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_w), off_c - off_w)
                self.buf_c = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_c), off_b - off_c)
                self.buf_b = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_b), self.size - off_b - layout.FOOTER_SIZE)
                
                footer_ptr = rffi.ptradd(self.ptr, self.size - layout.FOOTER_SIZE)
                self.cs_c = r_uint64(rffi.cast(rffi.ULONGLONGP, footer_ptr)[0])
                self.cs_b = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(footer_ptr, 8))[0])

                self._c_validated = False
                self._b_validated = False

                if validate_checksums:
                    self.validate_region_e()
                    self.validate_region_w()
            except Exception:
                # If any validation fails, we must unmap the memory to prevent leaks
                if self.ptr:
                    mmap_posix.munmap_file(self.ptr, self.size)
                    self.ptr = lltype.nullptr(rffi.CCHARP.TO)
                raise
        finally:
            rposix.close(fd)
    
    def validate_region_e(self):
        expected = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, layout.OFF_CHECKSUM_E))[0])
        if checksum.compute_checksum(self.buf_e.ptr, self.count * 8) != expected:
            raise errors.CorruptShardError("Region E checksum mismatch")
    
    def validate_region_w(self):
        expected = r_uint64(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, layout.OFF_CHECKSUM_W))[0])
        if checksum.compute_checksum(self.buf_w.ptr, self.count * 8) != expected:
            raise errors.CorruptShardError("Region W checksum mismatch")

    def validate_region_c(self):
        if not self._c_validated:
            if checksum.compute_checksum(self.buf_c.ptr, self.count * self.layout.stride) != self.cs_c:
                raise errors.CorruptShardError("Region C checksum mismatch")
            self._c_validated = True

    def validate_region_b(self):
        if not self._b_validated:
            if checksum.compute_checksum(self.buf_b.ptr, self.buf_b.size) != self.cs_b:
                raise errors.CorruptShardError("Region B checksum mismatch")
            self._b_validated = True

    def get_entity_id(self, index): return self.buf_e.read_u64(index * 8)
    def get_weight(self, index): return self.buf_w.read_i64(index * 8)
    
    def get_data_ptr(self, index): 
        self.validate_region_c()
        return self.buf_c.get_raw_ptr(index * self.layout.stride)

    def find_entity_index(self, entity_id):
        low = 0; high = self.count - 1; ans = -1
        while low <= high:
            mid = (low + high) / 2
            eid_at_mid = self.get_entity_id(mid)
            if eid_at_mid == entity_id: ans = mid; high = mid - 1
            elif eid_at_mid < entity_id: low = mid + 1
            else: high = mid - 1
        return ans

    def read_field_i64(self, index, field_idx):
        ptr = self.get_data_ptr(index)
        field_off = self.layout.get_field_offset(field_idx)
        return rffi.cast(rffi.LONGLONGP, rffi.ptradd(ptr, field_off))[0]

    def string_field_equals(self, index, field_idx, search_str):
        if len(search_str) > string_logic.SHORT_STRING_THRESHOLD:
            self.validate_region_b()
        ptr = self.get_data_ptr(index)
        field_off = self.layout.get_field_offset(field_idx)
        struct_ptr = rffi.ptradd(ptr, field_off)
        heap_base_ptr = self.buf_b.ptr
        search_len = len(search_str)
        search_prefix = string_logic.compute_prefix(search_str)
        return string_logic.string_equals(struct_ptr, heap_base_ptr, search_str, search_len, search_prefix)
    
    def close(self):
        if self.ptr:
            mmap_posix.munmap_file(self.ptr, self.size)
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)
