from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint32, r_uint64, r_int64
from gnitz.storage import errors
from rpython.rlib import jit

class MappedBuffer(object):
    _immutable_fields_ = ['ptr', 'size']

    def __init__(self, ptr, size):
        self.ptr = ptr
        self.size = size

    def _check_bounds(self, offset, length):
        """
        Safe bounds check using subtraction to prevent integer overflow.
        Ensures [offset, offset + length) is within [0, self.size).
        """
        if offset < 0 or length < 0:
            raise errors.BoundsError(offset, length, self.size)
        
        # Check if length alone is bigger than buffer
        if length > self.size:
            raise errors.BoundsError(offset, length, self.size)
            
        # Check if offset is beyond the remaining space
        # (Equivalent to offset + length > self.size, but overflow-safe)
        if offset > self.size - length:
            raise errors.BoundsError(offset, length, self.size)

    def get_raw_ptr(self, offset):
        self._check_bounds(offset, 1)
        return rffi.ptradd(self.ptr, offset)

    def read_i64(self, offset):
        self._check_bounds(offset, 8)
        val_ptr = rffi.cast(rffi.LONGLONGP, rffi.ptradd(self.ptr, offset))
        return r_int64(val_ptr[0])

    def read_u64(self, offset):
        self._check_bounds(offset, 8)
        val_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, offset))
        return r_uint64(val_ptr[0])

    def read_i32(self, offset):
        self._check_bounds(offset, 4)
        val_ptr = rffi.cast(rffi.INTP, rffi.ptradd(self.ptr, offset))
        return rffi.cast(lltype.Signed, val_ptr[0])

    def read_u32(self, offset):
        self._check_bounds(offset, 4)
        val_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, offset))
        return r_uint32(val_ptr[0])

    def read_u8(self, offset):
        self._check_bounds(offset, 1)
        return rffi.cast(lltype.Signed, self.ptr[offset])
