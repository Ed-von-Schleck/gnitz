"""
gnitz/storage/writer_ecs.py
"""
import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.jit import unrolling_iterable
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from gnitz.storage import layout, mmap_posix
from gnitz.core import types, strings as string_logic, checksum
from rpython.rlib.rarithmetic import r_uint64


eci = ExternalCompilationInfo(includes=['string.h'])
memmove = rffi.llexternal('memmove', 
                         [rffi.CCHARP, rffi.CCHARP, rffi.SIZE_T], 
                         rffi.CCHARP, 
                         compilation_info=eci)

MAX_FIELDS = 64
FIELD_INDICES = unrolling_iterable(range(MAX_FIELDS))

class ECSShardWriter(object):
    def __init__(self, component_layout, initial_capacity=1024):
        self.layout = component_layout
        self.capacity = initial_capacity
        self.count = 0
        
        self.entities_ptr = lltype.malloc(rffi.LONGLONGP.TO, self.capacity, flavor='raw')
        self.weights_ptr = lltype.malloc(rffi.LONGLONGP.TO, self.capacity, flavor='raw')
        self.c_data_ptr = lltype.malloc(rffi.CCHARP.TO, self.capacity * self.layout.stride, flavor='raw')
        
        self.blob_capacity = initial_capacity * 32
        self.blob_ptr = lltype.malloc(rffi.CCHARP.TO, self.blob_capacity, flavor='raw')
        self.blob_offset = 0

    def _ensure_capacity(self):
        if self.count < self.capacity:
            return
        
        new_cap = self.capacity * 2
        
        new_e = lltype.malloc(rffi.LONGLONGP.TO, new_cap, flavor='raw')
        memmove(rffi.cast(rffi.CCHARP, new_e), rffi.cast(rffi.CCHARP, self.entities_ptr), rffi.cast(rffi.SIZE_T, self.count * 8))
        lltype.free(self.entities_ptr, flavor='raw')
        self.entities_ptr = new_e
        
        new_w = lltype.malloc(rffi.LONGLONGP.TO, new_cap, flavor='raw')
        memmove(rffi.cast(rffi.CCHARP, new_w), rffi.cast(rffi.CCHARP, self.weights_ptr), rffi.cast(rffi.SIZE_T, self.count * 8))
        lltype.free(self.weights_ptr, flavor='raw')
        self.weights_ptr = new_w
        
        new_c = lltype.malloc(rffi.CCHARP.TO, new_cap * self.layout.stride, flavor='raw')
        memmove(new_c, self.c_data_ptr, rffi.cast(rffi.SIZE_T, self.count * self.layout.stride))
        lltype.free(self.c_data_ptr, flavor='raw')
        self.c_data_ptr = new_c
        
        self.capacity = new_cap

    def _ensure_blob_capacity(self, additional):
        if self.blob_offset + additional <= self.blob_capacity:
            return
        
        new_cap = (self.blob_offset + additional) * 2
        new_b = lltype.malloc(rffi.CCHARP.TO, new_cap, flavor='raw')
        memmove(new_b, self.blob_ptr, rffi.cast(rffi.SIZE_T, self.blob_offset))
        lltype.free(self.blob_ptr, flavor='raw')
        self.blob_ptr = new_b
        self.blob_capacity = new_cap

    def add_entity(self, entity_id, *field_values):
        self._add_entity_weighted(entity_id, 1, *field_values)

    def _add_entity_weighted(self, entity_id, weight, *field_values):
        self._ensure_capacity()
        
        idx = self.count
        self.entities_ptr[idx] = rffi.cast(rffi.LONGLONG, entity_id)
        self.weights_ptr[idx] = rffi.cast(rffi.LONGLONG, weight)
        
        stride = self.layout.stride
        dest_row_ptr = rffi.ptradd(self.c_data_ptr, idx * stride)
        
        for k in range(stride): dest_row_ptr[k] = '\x00'
        
        for i in FIELD_INDICES:
            if i >= len(field_values) or i >= len(self.layout.field_types): 
                break
            
            val = field_values[i]
            f_type = self.layout.field_types[i]
            f_off = self.layout.field_offsets[i]
            dest = rffi.ptradd(dest_row_ptr, f_off)
            
            if f_type == types.TYPE_STRING:
                s_val = str(val)
                l_val = len(s_val)
                heap_off = 0
                if l_val > string_logic.SHORT_STRING_THRESHOLD:
                    self._ensure_blob_capacity(l_val)
                    heap_off = self.blob_offset
                    for j in range(l_val):
                        self.blob_ptr[self.blob_offset + j] = s_val[j]
                    self.blob_offset += l_val
                string_logic.pack_string(dest, s_val, heap_off)
            elif isinstance(val, int):
                rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(rffi.LONGLONG, val)
            elif isinstance(val, float):
                rffi.cast(rffi.DOUBLEP, dest)[0] = rffi.cast(rffi.DOUBLE, val)
                
        self.count += 1

    def add_packed_row(self, entity_id, weight, source_row_ptr, source_heap_ptr):
        self._ensure_capacity()
        
        idx = self.count
        self.entities_ptr[idx] = rffi.cast(rffi.LONGLONG, entity_id)
        self.weights_ptr[idx] = rffi.cast(rffi.LONGLONG, weight)
        
        stride = self.layout.stride
        dest_row_ptr = rffi.ptradd(self.c_data_ptr, idx * stride)
        memmove(dest_row_ptr, source_row_ptr, rffi.cast(rffi.SIZE_T, stride))
        
        for i in FIELD_INDICES:
            if i >= len(self.layout.field_types): break
            if self.layout.field_types[i] == types.TYPE_STRING:
                f_off = self.layout.field_offsets[i]
                s_ptr = rffi.ptradd(dest_row_ptr, f_off)
                
                length = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, s_ptr)[0])
                if length > string_logic.SHORT_STRING_THRESHOLD:
                    u64_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(s_ptr, 8))
                    old_offset = rffi.cast(lltype.Signed, u64_ptr[0])
                    
                    self._ensure_blob_capacity(length)
                    memmove(rffi.ptradd(self.blob_ptr, self.blob_offset), 
                            rffi.ptradd(source_heap_ptr, old_offset), 
                            rffi.cast(rffi.SIZE_T, length))
                    
                    u64_ptr[0] = rffi.cast(rffi.ULONGLONG, self.blob_offset)
                    self.blob_offset += length
        self.count += 1

    def _write_padding(self, fd, size):
        if size <= 0: return
        pad = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        for i in range(size): pad[i] = '\x00'
        mmap_posix.write_c(fd, pad, rffi.cast(rffi.SIZE_T, size))
        lltype.free(pad, flavor='raw')

    def finalize(self, filename):
        fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            off_e = layout.HEADER_SIZE
            size_e = self.count * 8
            
            off_w = (off_e + size_e + 63) & ~63
            size_w = self.count * 8
            
            off_c = (off_w + size_w + 63) & ~63
            size_c = self.count * self.layout.stride
            
            off_b = (off_c + size_c + 63) & ~63
            size_b = self.blob_offset

            self._write_padding(fd, layout.HEADER_SIZE)
            mmap_posix.write_c(fd, rffi.cast(rffi.CCHARP, self.entities_ptr), rffi.cast(rffi.SIZE_T, size_e))
            
            checksum_e = checksum.compute_checksum(rffi.cast(rffi.CCHARP, self.entities_ptr), size_e)
            
            self._write_padding(fd, off_w - (off_e + size_e))
            mmap_posix.write_c(fd, rffi.cast(rffi.CCHARP, self.weights_ptr), rffi.cast(rffi.SIZE_T, size_w))
            
            checksum_w = checksum.compute_checksum(rffi.cast(rffi.CCHARP, self.weights_ptr), size_w)
            
            self._write_padding(fd, off_c - (off_w + size_w))
            mmap_posix.write_c(fd, self.c_data_ptr, rffi.cast(rffi.SIZE_T, size_c))
            self._write_padding(fd, off_b - (off_c + size_c))
            if size_b > 0:
                mmap_posix.write_c(fd, self.blob_ptr, rffi.cast(rffi.SIZE_T, size_b))

            rposix.lseek(fd, 0, 0)
            h = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor='raw')
            for i in range(layout.HEADER_SIZE): h[i] = '\x00'
            
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_MAGIC))[0] = rffi.cast(rffi.LONGLONG, layout.MAGIC_NUMBER)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_COUNT))[0] = rffi.cast(rffi.LONGLONG, self.count)
            
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(h, layout.OFF_CHECKSUM_E))[0] = rffi.cast(rffi.ULONGLONG, checksum_e)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(h, layout.OFF_CHECKSUM_W))[0] = rffi.cast(rffi.ULONGLONG, checksum_w)
            
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_E_ECS))[0] = rffi.cast(rffi.LONGLONG, off_e)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_W_ECS))[0] = rffi.cast(rffi.LONGLONG, off_w)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_C_ECS))[0] = rffi.cast(rffi.LONGLONG, off_c)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_B_ECS))[0] = rffi.cast(rffi.LONGLONG, off_b)
            
            mmap_posix.write_c(fd, h, rffi.cast(rffi.SIZE_T, layout.HEADER_SIZE))
            lltype.free(h, flavor='raw')
        finally:
            rposix.close(fd)
            self.close()

    def close(self):
        if self.entities_ptr:
            lltype.free(self.entities_ptr, flavor='raw')
            self.entities_ptr = lltype.nullptr(rffi.LONGLONGP.TO)
        if self.weights_ptr:
            lltype.free(self.weights_ptr, flavor='raw')
            self.weights_ptr = lltype.nullptr(rffi.LONGLONGP.TO)
        if self.c_data_ptr:
            lltype.free(self.c_data_ptr, flavor='raw')
            self.c_data_ptr = lltype.nullptr(rffi.CCHARP.TO)
        if self.blob_ptr:
            lltype.free(self.blob_ptr, flavor='raw')
            self.blob_ptr = lltype.nullptr(rffi.CCHARP.TO)
