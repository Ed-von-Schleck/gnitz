from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import errors

class Arena(object):
    """
    Manual memory allocator using raw pointers.
    Optimized for bump-pointer allocation within a pre-allocated buffer.
    """
    _immutable_fields_ = ['size', 'base_ptr']

    def __init__(self, size):
        self.size = size
        # flavor='raw' memory is outside the GC heap
        self.base_ptr = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        self.offset = 0

    def _align_offset(self, offset, alignment):
        # Bitwise alignment must use machine-word logic
        return (offset + (alignment - 1)) & ~(alignment - 1)

    def alloc(self, nbytes, alignment=16):
        """
        Allocates nbytes from the arena. 
        FIXED: Enforces 16-byte alignment as default to support u128.
        """
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
