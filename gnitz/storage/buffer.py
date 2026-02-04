from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import errors

class MappedBuffer(object):
    """
    A safety wrapper around a raw mmap pointer.
    Provides bounds-checked access to underlying data.
    """
    _immutable_fields_ = ['ptr', 'size']

    def __init__(self, ptr, size):
        self.ptr = ptr
        self.size = size

    def _check_bounds(self, offset, length):
        # In RPython, we rely on the translator to optimize this check away
        # if it can prove offset + length < self.size
        if offset < 0 or (offset + length) > self.size:
            raise errors.BoundsError()

    def read_i64(self, offset):
        """Reads an 8-byte signed integer."""
        self._check_bounds(offset, 8)
        # 1. Calculate the address (ptr + offset)
        addr = rffi.ptradd(self.ptr, offset)
        # 2. Cast the raw char* to a longlong*
        val_ptr = rffi.cast(rffi.LONGLONGP, addr)
        # 3. Dereference and return
        return val_ptr[0]

    def read_i32(self, offset):
        """Reads a 4-byte signed integer."""
        self._check_bounds(offset, 4)
        addr = rffi.ptradd(self.ptr, offset)
        val_ptr = rffi.cast(rffi.INTP, addr)
        # Cast to Signed for RPython general usage
        return rffi.cast(lltype.Signed, val_ptr[0])

    def read_u8(self, offset):
        """Reads a single byte."""
        self._check_bounds(offset, 1)
        # Direct access to the CCHARP as an array
        char_val = self.ptr[offset]
        return rffi.cast(lltype.Signed, char_val)
