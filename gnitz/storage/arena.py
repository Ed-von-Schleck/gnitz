from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import errors

class Arena(object):
    _immutable_fields_ = ['size', 'base_ptr']

    def __init__(self, size):
        self.size = size
        self.base_ptr = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        self.offset = 0

    def _align_offset(self, offset, alignment):
        return (offset + (alignment - 1)) & ~(alignment - 1)

    def alloc(self, nbytes, alignment=8):
        aligned_start = self._align_offset(self.offset, alignment)
        if aligned_start + nbytes > self.size:
            raise errors.MemTableFullError()
        
        ptr = rffi.ptradd(self.base_ptr, aligned_start)
        self.offset = aligned_start + nbytes
        return ptr

    def free(self):
        if self.base_ptr:
            lltype.free(self.base_ptr, flavor='raw')
            self.base_ptr = lltype.nullptr(rffi.CCHARP.TO)
