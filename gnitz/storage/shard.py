import os
from rpython.rlib import jit, rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import layout, mmap_posix, buffer, errors

def decode_varint(buf, offset):
    res = 0
    shift = 0
    idx = 0
    while True:
        b = buf.read_u8(offset + idx)
        res |= (b & 0x7f) << shift
        idx += 1
        if not (b & 0x80):
            break
        shift += 7
    return res, idx

def compare_raw_key(buf_k, heap_offset, search_key):
    length, v_len = decode_varint(buf_k, heap_offset)
    data_start = heap_offset + v_len
    search_len = len(search_key)
    min_len = length if length < search_len else search_len
    
    ptr = buf_k.get_raw_ptr(data_start)
    for i in range(min_len):
        b_heap = ord(ptr[i])
        b_search = ord(search_key[i])
        if b_heap < b_search: return -1
        if b_heap > b_search: return 1
            
    if length < search_len: return -1
    if length > search_len: return 1
    return 0

class ShardView(object):
    _immutable_fields_ = ['count', 'buf_w', 'buf_o', 'buf_k', 'buf_v', 'size', 'ptr']

    def __init__(self, filename):
        fd = rposix.open(filename, os.O_RDONLY, 0)
        try:
            st = os.fstat(fd)
            self.size = st.st_size
            if self.size < layout.HEADER_SIZE:
                raise errors.CorruptShardError()
            self.ptr = mmap_posix.mmap_file(fd, self.size, mmap_posix.PROT_READ, mmap_posix.MAP_SHARED)
        finally:
            rposix.close(fd)

        header = buffer.MappedBuffer(self.ptr, layout.HEADER_SIZE)
        if header.read_i64(layout.OFF_MAGIC) != layout.MAGIC_NUMBER:
            raise errors.CorruptShardError()

        self.count = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_COUNT))
        off_w = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_W))
        off_o = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_O))
        off_k = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_K))
        off_v = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_V))

        self.buf_w = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_w), off_o - off_w)
        self.buf_o = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_o), off_k - off_o)
        self.buf_k = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_k), off_v - off_k)
        self.buf_v = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_v), self.size - off_v)

    def get_weight(self, idx):
        return self.buf_w.read_i64(idx * layout.STRIDE_W)

    @jit.elidable
    def find_key_index(self, key):
        low = 0
        high = self.count - 1
        while low <= high:
            mid = (low + high) // 2
            heap_offset = self.buf_o.read_i32(mid * layout.STRIDE_O)
            cmp_res = compare_raw_key(self.buf_k, heap_offset, key)
            if cmp_res == 0: return mid
            if cmp_res < 0: low = mid + 1
            else: high = mid - 1
        return -1

    @jit.dont_look_inside
    def materialize_key(self, idx):
        heap_offset = self.buf_o.read_i32(idx * layout.STRIDE_O)
        length, v_len = decode_varint(self.buf_k, heap_offset)
        ptr = self.buf_k.get_raw_ptr(heap_offset + v_len)
        return rffi.charpsize2str(ptr, length)

    @jit.dont_look_inside
    def materialize_value(self, idx):
        heap_offset = self.buf_o.read_i32(idx * layout.STRIDE_O + 4)
        length, v_len = decode_varint(self.buf_v, heap_offset)
        ptr = self.buf_v.get_raw_ptr(heap_offset + v_len)
        return rffi.charpsize2str(ptr, length)

    def close(self):
        mmap_posix.munmap_file(self.ptr, self.size)
