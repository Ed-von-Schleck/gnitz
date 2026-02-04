import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import layout, mmap_posix

def encode_varint(val):
    res = []
    if val == 0:
        res.append(chr(0))
        return res
    while val >= 0x80:
        res.append(chr((val & 0x7f) | 0x80))
        val >>= 7
    res.append(chr(val & 0x7f))
    return res

class ShardWriter(object):
    def __init__(self):
        self.count = 0
        self.weights = []
        self.key_offsets = []
        self.val_offsets = []
        self.key_heap = []
        self.val_heap = []

    def add_entry(self, key, value, weight):
        self.count += 1
        self.weights.append(weight)
        
        self.key_offsets.append(len(self.key_heap))
        for c in encode_varint(len(key)):
            self.key_heap.append(c)
        for c in key:
            self.key_heap.append(c)
            
        self.val_offsets.append(len(self.val_heap))
        for c in encode_varint(len(value)):
            self.val_heap.append(c)
        for c in value:
            self.val_heap.append(c)

    def _align(self, current_pos):
        remainder = current_pos % layout.ALIGNMENT
        if remainder == 0:
            return 0
        return layout.ALIGNMENT - remainder

    def finalize(self, filename):
        fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            off_w = layout.HEADER_SIZE
            size_w = self.count * layout.STRIDE_W
            
            off_o = off_w + size_w + self._align(off_w + size_w)
            size_o = self.count * layout.STRIDE_O
            
            off_k = off_o + size_o + self._align(off_o + size_o)
            size_k = len(self.key_heap)
            
            off_v = off_k + size_k + self._align(off_k + size_k)
            size_v = len(self.val_heap)

            # 1. Write Header
            header = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor='raw')
            try:
                for i in range(layout.HEADER_SIZE): header[i] = chr(0)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_MAGIC))[0] = rffi.cast(rffi.LONGLONG, layout.MAGIC_NUMBER)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_COUNT))[0] = rffi.cast(rffi.LONGLONG, self.count)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_W))[0] = rffi.cast(rffi.LONGLONG, off_w)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_O))[0] = rffi.cast(rffi.LONGLONG, off_o)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_K))[0] = rffi.cast(rffi.LONGLONG, off_k)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_V))[0] = rffi.cast(rffi.LONGLONG, off_v)
                mmap_posix.write_c(fd, header, rffi.cast(rffi.SIZE_T, layout.HEADER_SIZE))
            finally:
                lltype.free(header, flavor='raw')

            # 2. Write Weights
            w_buf = lltype.malloc(rffi.LONGLONGP.TO, self.count, flavor='raw')
            try:
                for i in range(self.count):
                    w_buf[i] = rffi.cast(rffi.LONGLONG, self.weights[i])
                mmap_posix.write_c(fd, rffi.cast(rffi.CCHARP, w_buf), rffi.cast(rffi.SIZE_T, size_w))
            finally:
                lltype.free(w_buf, flavor='raw')

            # 3. Write Offsets (with padding)
            self._write_padding(fd, off_o - (off_w + size_w))
            o_buf = lltype.malloc(rffi.INTP.TO, self.count * 2, flavor='raw')
            try:
                for i in range(self.count):
                    o_buf[i*2] = rffi.cast(rffi.INT, self.key_offsets[i])
                    o_buf[i*2 + 1] = rffi.cast(rffi.INT, self.val_offsets[i])
                mmap_posix.write_c(fd, rffi.cast(rffi.CCHARP, o_buf), rffi.cast(rffi.SIZE_T, size_o))
            finally:
                lltype.free(o_buf, flavor='raw')

            # 4. Write Key Heap
            self._write_padding(fd, off_k - (off_o + size_o))
            self._write_list(fd, self.key_heap)

            # 5. Write Value Heap
            self._write_padding(fd, off_v - (off_k + size_k))
            self._write_list(fd, self.val_heap)

        finally:
            rposix.close(fd)

    def _write_padding(self, fd, count):
        if count <= 0: return
        pad = lltype.malloc(rffi.CCHARP.TO, count, flavor='raw')
        for i in range(count): pad[i] = chr(0)
        mmap_posix.write_c(fd, pad, rffi.cast(rffi.SIZE_T, count))
        lltype.free(pad, flavor='raw')

    def _write_list(self, fd, data_list):
        size = len(data_list)
        if size == 0: return
        buf = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        for i in range(size):
            buf[i] = data_list[i]
        mmap_posix.write_c(fd, buf, rffi.cast(rffi.SIZE_T, size))
        lltype.free(buf, flavor='raw')
