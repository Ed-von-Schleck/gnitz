from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import errors

class Arena(object):
    """
    A monotonic (bump-pointer) allocator.
    Allocates memory from a single large raw block.
    """
    _immutable_fields_ = ['size', 'base_ptr']

    def __init__(self, size):
        self.size = size
        # Allocate the raw block from the OS heap
        self.base_ptr = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        self.offset = 0

    def _align_offset(self, offset):
        """Aligns the offset to the next 8-byte boundary."""
        return (offset + 7) & ~7

    def alloc(self, nbytes):
        """
        Allocates nbytes from the arena.
        Returns a raw rffi.CCHARP pointer.
        """
        aligned_start = self._align_offset(self.offset)
        if aligned_start + nbytes > self.size:
            raise errors.StorageError() # Arena exhausted
        
        ptr = rffi.ptradd(self.base_ptr, aligned_start)
        self.offset = aligned_start + nbytes
        return ptr

    def free(self):
        """Releases the entire block back to the OS."""
        if self.base_ptr:
            lltype.free(self.base_ptr, flavor='raw')
            self.base_ptr = lltype.nullptr(rffi.CCHARP.TO)
