import os
from rpython.rlib import jit, rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import layout, mmap_posix, buffer, errors

def decode_varint(buf, offset):
    """
    Decodes a 7-bit VarInt from a MappedBuffer.
    Returns (value, bytes_read).
    """
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

class ShardView(object):
    """
    High-level view of a Quad-Partition Shard.
    Maps the file and provides structured access to columns.
    """
    _immutable_fields_ = ['count', 'buf_w', 'buf_o', 'buf_k', 'buf_v', 'size', 'ptr']

    def __init__(self, filename):
        # 1. Open and Map
        fd = rposix.open(filename, os.O_RDONLY, 0)
        try:
            st = os.fstat(fd)
            self.size = st.st_size
            if self.size < layout.HEADER_SIZE:
                raise errors.CorruptShardError()
            
            self.ptr = mmap_posix.mmap_file(fd, self.size, mmap_posix.PROT_READ, mmap_posix.MAP_SHARED)
        finally:
            rposix.close(fd)

        # 2. Parse Header
        header = buffer.MappedBuffer(self.ptr, layout.HEADER_SIZE)
        magic = header.read_i64(layout.OFF_MAGIC)
        if magic != layout.MAGIC_NUMBER:
            raise errors.CorruptShardError()

        self.count = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_COUNT))
        
        off_w = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_W))
        off_o = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_O))
        off_k = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_K))
        off_v = rffi.cast(lltype.Signed, header.read_i64(layout.OFF_REG_V))

        # 3. Initialize Region Buffers
        # Note: We calculate sizes based on offsets to ensure safety
        self.buf_w = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_w), off_o - off_w)
        self.buf_o = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_o), off_k - off_o)
        self.buf_k = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_k), off_v - off_k)
        self.buf_v = buffer.MappedBuffer(rffi.ptradd(self.ptr, off_v), self.size - off_v)

    def get_weight(self, idx):
        """Read weight (JIT-visible)."""
        return self.buf_w.read_i64(idx * layout.STRIDE_W)

    @jit.dont_look_inside
    def materialize_key(self, idx):
        """Opaque retrieval of key data."""
        heap_offset = self.buf_o.read_i32(idx * layout.STRIDE_O)
        length, v_len = decode_varint(self.buf_k, heap_offset)
        
        # Convert raw memory slice to RPython string
        res = []
        start = heap_offset + v_len
        for i in range(length):
            res.append(chr(self.buf_k.read_u8(start + i)))
        return "".join(res)

    @jit.dont_look_inside
    def materialize_value(self, idx):
        """Opaque retrieval of value data."""
        # Value offset is the second i32 in the Offset Spine entry
        heap_offset = self.buf_o.read_i32(idx * layout.STRIDE_O + 4)
        length, v_len = decode_varint(self.buf_v, heap_offset)
        
        res = []
        start = heap_offset + v_len
        for i in range(length):
            res.append(chr(self.buf_v.read_u8(start + i)))
        return "".join(res)

    def close(self):
        mmap_posix.munmap_file(self.ptr, self.size)
