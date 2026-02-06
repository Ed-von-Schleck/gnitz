import os
from rpython.rlib import rposix
from rpython.rlib.objectmodel import specialize
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.jit import unrolling_iterable
from gnitz.storage import layout, mmap_posix
from gnitz.core import types, strings as string_logic

MAX_FIELDS = 64
FIELD_INDICES = unrolling_iterable(range(MAX_FIELDS))

class ECSShardWriter(object):
    def __init__(self, component_layout):
        self.layout = component_layout
        self.count = 0
        self.entities = []
        self.c_data_chunks = []
        self.blob_heap = []
        self.current_blob_offset = 0

    def add_entity(self, entity_id, *field_values):
        self.count += 1
        self.entities.append(entity_id)
        stride = self.layout.stride
        row_ptr = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        try:
            for k in range(stride): row_ptr[k] = '\x00'
            for i in FIELD_INDICES:
                if i >= len(field_values) or i >= len(self.layout.field_types): break
                val = field_values[i]
                f_type = self.layout.field_types[i]
                f_off = self.layout.field_offsets[i]
                dest = rffi.ptradd(row_ptr, f_off)
                if f_type == types.TYPE_STRING:
                    s_val = str(val)
                    l_val = len(s_val)
                    heap_off = 0
                    if l_val > string_logic.SHORT_STRING_THRESHOLD:
                        heap_off = self.current_blob_offset
                        for char in s_val: self.blob_heap.append(char)
                        self.current_blob_offset += l_val
                    string_logic.pack_string(dest, s_val, heap_off)
                elif isinstance(val, int):
                    rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(rffi.LONGLONG, val)
                elif isinstance(val, float):
                    rffi.cast(rffi.DOUBLEP, dest)[0] = rffi.cast(rffi.DOUBLE, val)
            self.c_data_chunks.append(rffi.charpsize2str(row_ptr, stride))
        finally:
            lltype.free(row_ptr, flavor='raw')

    def add_packed_row(self, entity_id, source_row_ptr, source_heap_ptr):
        """
        Adds already-serialized data from the MemTable.
        Relocates 'Long' German String pointers to the shard's heap.
        """
        self.count += 1
        self.entities.append(entity_id)
        stride = self.layout.stride
        
        # Copy row to a local buffer for modification
        row_copy = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        try:
            for i in range(stride): row_copy[i] = source_row_ptr[i]
            
            for i in FIELD_INDICES:
                if i >= len(self.layout.field_types): break
                f_type = self.layout.field_types[i]
                if f_type == types.TYPE_STRING:
                    f_off = self.layout.field_offsets[i]
                    s_ptr = rffi.ptradd(row_copy, f_off)
                    u32_ptr = rffi.cast(rffi.UINTP, s_ptr)
                    length = rffi.cast(lltype.Signed, u32_ptr[0])
                    
                    if length > string_logic.SHORT_STRING_THRESHOLD:
                        # Relocate long string
                        u64_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(s_ptr, 8))
                        old_offset = rffi.cast(lltype.Signed, u64_ptr[0])
                        
                        src_str_ptr = rffi.ptradd(source_heap_ptr, old_offset)
                        new_offset = self.current_blob_offset
                        for j in range(length):
                            self.blob_heap.append(src_str_ptr[j])
                        
                        u64_ptr[0] = rffi.cast(rffi.ULONGLONG, new_offset)
                        self.current_blob_offset += length
            
            self.c_data_chunks.append(rffi.charpsize2str(row_copy, stride))
        finally:
            lltype.free(row_copy, flavor='raw')

    def _align(self, pos):
        rem = pos % 64
        return 64 - rem if rem else 0

    def _write_padding(self, fd, count):
        if count <= 0: return
        p = lltype.malloc(rffi.CCHARP.TO, count, flavor='raw')
        for i in range(count): p[i] = '\x00'
        mmap_posix.write_c(fd, p, rffi.cast(rffi.SIZE_T, count))
        lltype.free(p, flavor='raw')

    def finalize(self, filename):
        fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            off_e = layout.HEADER_SIZE
            size_e = self.count * 8
            off_c = (off_e + size_e + 63) & ~63
            size_c = self.count * self.layout.stride
            off_b = (off_c + size_c + 63) & ~63
            size_b = len(self.blob_heap)

            self._write_padding(fd, layout.HEADER_SIZE)
            e_buf = lltype.malloc(rffi.LONGLONGP.TO, self.count, flavor='raw')
            for i in range(self.count): e_buf[i] = rffi.cast(rffi.LONGLONG, self.entities[i])
            mmap_posix.write_c(fd, rffi.cast(rffi.CCHARP, e_buf), rffi.cast(rffi.SIZE_T, size_e))
            lltype.free(e_buf, flavor='raw')
            self._write_padding(fd, off_c - (off_e + size_e))
            for chunk in self.c_data_chunks:
                c_chunk = rffi.str2charp(chunk)
                mmap_posix.write_c(fd, c_chunk, rffi.cast(rffi.SIZE_T, len(chunk)))
                rffi.free_charp(c_chunk)
            self._write_padding(fd, off_b - (off_c + size_c))
            if size_b > 0:
                b_buf = lltype.malloc(rffi.CCHARP.TO, size_b, flavor='raw')
                for i in range(size_b): b_buf[i] = self.blob_heap[i]
                mmap_posix.write_c(fd, b_buf, rffi.cast(rffi.SIZE_T, size_b))
                lltype.free(b_buf, flavor='raw')

            rposix.lseek(fd, 0, rposix.SEEK_SET)
            h = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor='raw')
            for i in range(layout.HEADER_SIZE): h[i] = '\x00'
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_MAGIC))[0] = rffi.cast(rffi.LONGLONG, layout.MAGIC_NUMBER)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_COUNT))[0] = rffi.cast(rffi.LONGLONG, self.count)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_E_ECS))[0] = rffi.cast(rffi.LONGLONG, off_e)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_C_ECS))[0] = rffi.cast(rffi.LONGLONG, off_c)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_B_ECS))[0] = rffi.cast(rffi.LONGLONG, off_b)
            mmap_posix.write_c(fd, h, rffi.cast(rffi.SIZE_T, layout.HEADER_SIZE))
            lltype.free(h, flavor='raw')
        finally:
            rposix.close(fd)
