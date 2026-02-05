from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import errors
from rpython.rlib import jit

class MappedBuffer(object):
    _immutable_fields_ = ['ptr', 'size']

    def __init__(self, ptr, size):
        self.ptr = ptr
        self.size = size

    def _check_bounds(self, offset, length):
        if offset < 0 or (offset + length) > self.size:
            raise errors.BoundsError()

    def get_raw_ptr(self, offset):
        """Returns raw pointer after bounds check. Used for JIT optimization."""
        self._check_bounds(offset, 1)
        return rffi.ptradd(self.ptr, offset)

    def read_i64(self, offset):
        self._check_bounds(offset, 8)
        val_ptr = rffi.cast(rffi.LONGLONGP, rffi.ptradd(self.ptr, offset))
        return val_ptr[0]

    def read_i32(self, offset):
        self._check_bounds(offset, 4)
        val_ptr = rffi.cast(rffi.INTP, rffi.ptradd(self.ptr, offset))
        return rffi.cast(lltype.Signed, val_ptr[0])

    def read_u8(self, offset):
        self._check_bounds(offset, 1)
        return rffi.cast(lltype.Signed, self.ptr[offset])
