from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint32, r_uint64, r_int64
from rpython.rlib.objectmodel import we_are_translated
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from gnitz.core import errors
from rpython.rlib import jit

# Correctly define the compilation info to include the C string header
eci = ExternalCompilationInfo(includes=['string.h'])

# Declare memmove as an external C function. 
c_memmove = rffi.llexternal('memmove', 
                            [rffi.VOIDP, rffi.VOIDP, rffi.SIZE_T], 
                            rffi.VOIDP, 
                            compilation_info=eci)

def align_64(val):
    """Utility to align a value to 64-byte boundaries (layout requirement)."""
    return (val + 63) & ~63

class MappedBuffer(object):
    """Immutable, non-owning view of memory (e.g., mmap)."""
    _immutable_fields_ = ['ptr', 'size']

    def __init__(self, ptr, size):
        self.ptr = ptr
        self.size = size

    def _check_bounds(self, offset, length):
        if offset < 0 or length < 0 or offset > self.size - length:
            raise errors.BoundsError(offset, length, self.size)

    def read_i64(self, offset):
        self._check_bounds(offset, 8)
        val_ptr = rffi.cast(rffi.LONGLONGP, rffi.ptradd(self.ptr, offset))
        return r_int64(val_ptr[0])

    def read_u64(self, offset):
        self._check_bounds(offset, 8)
        val_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, offset))
        return r_uint64(val_ptr[0])

    def read_u32(self, offset):
        self._check_bounds(offset, 4)
        val_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, offset))
        return r_uint32(val_ptr[0])

    def read_u8(self, offset):
        """Reads a single byte as an integer [0-255]."""
        self._check_bounds(offset, 1)
        return ord(self.ptr[offset])

    def get_raw_ptr(self, offset):
        self._check_bounds(offset, 1)
        return rffi.ptradd(self.ptr, offset)


class Buffer(object):
    """
    Unified Mutable Buffer.
    Can act as a fixed Arena or a growable RawBuffer.
    """
    _immutable_fields_ = ['growable', 'item_size']

    def __init__(self, initial_size, growable=True, item_size=1):
        # ------------------------------------------------------------------ #
        # ll2ctypes hang fix                                                   #
        #                                                                      #
        # In PyPy2 ll2ctypes mode (unit tests, non-translated), the ctypes    #
        # representation of an lltype.malloc'd array is built lazily on the   #
        # first rffi.ptradd / rffi.cast access, by iterating every element    #
        # of the underlying array in pure Python (convert_array in             #
        # ll2ctypes.py).  This is O(N) where N is the allocation size in      #
        # bytes.  A 1 MB MemTable arena costs ~1 M Python iterations; with    #
        # 7 system tables × 2 arenas each, that is 14 million iterations      #
        # before a single row is inserted — causing the multi-second 100%     #
        # CPU hang observed in catalog bootstrap tests.                        #
        #                                                                      #
        # we_are_translated() is False when running as plain Python and True   #
        # when compiled with RPython, so this branch is constant-folded away  #
        # in the translated binary and has no effect on production code.       #
        #                                                                      #
        # We cap the initial allocation to 64 bytes and force growable=True   #
        # so the buffer expands on demand.  The capacity invariants checked   #
        # by MemTable._ensure_capacity continue to work correctly because      #
        # Buffer.size returns capacity * item_size, which grows alongside the  #
        # buffer itself.                                                        #
        #                                                                      #
        # NOTE: The same O(N) convert_array trigger applies to the realloc    #
        # path in ensure_capacity — rffi.cast(VOIDP, new_ptr) on a freshly   #
        # lltype.malloc'd array also runs convert_array for every byte.       #
        # We therefore avoid c_memmove entirely in non-translated mode and    #
        # copy the live bytes manually instead (see ensure_capacity below).   #
        # ------------------------------------------------------------------ #
        if not we_are_translated():
            initial_size = min(initial_size, 64)
            growable = True

        self.capacity = initial_size
        self.item_size = item_size
        self.growable = growable
        self.offset = 0  # tracks bytes used
        self.base_ptr = lltype.malloc(rffi.CCHARP.TO, initial_size * item_size, flavor='raw')

    @property
    def ptr(self):
        """Legacy alias for base_ptr used by writer_table and wal_format."""
        return self.base_ptr

    @property
    def size(self):
        """Legacy alias for capacity used by MemTable bounds checks."""
        return self.capacity * self.item_size

    @property
    def count(self):
        """Returns number of items currently stored, used by writer_table."""
        return self.offset // self.item_size

    def _align(self, offset, alignment):
        return (offset + (alignment - 1)) & ~(alignment - 1)

    def ensure_capacity(self, needed_bytes):
        if self.offset + needed_bytes > self.capacity * self.item_size:
            if not self.growable:
                raise errors.MemTableFullError()
            
            new_cap = max(self.capacity * 2, (self.offset + needed_bytes) // self.item_size + 1)
            new_ptr = lltype.malloc(rffi.CCHARP.TO, new_cap * self.item_size, flavor='raw')
            
            if self.offset > 0:
                if we_are_translated():
                    # Production: fast C memmove.
                    c_memmove(rffi.cast(rffi.VOIDP, new_ptr),
                              rffi.cast(rffi.VOIDP, self.base_ptr),
                              rffi.cast(rffi.SIZE_T, self.offset))
                else:
                    # Test mode (ll2ctypes): rffi.cast(VOIDP, ptr) on a freshly
                    # lltype.malloc'd array triggers convert_array — O(new_cap)
                    # pure-Python iterations — causing the same hang as the
                    # initial Buffer.__init__ allocation would without the 64-byte
                    # cap.  Copy the live bytes manually instead; buffers are
                    # always small in test mode (start at 64 bytes, grow slowly).
                    for i in range(self.offset):
                        new_ptr[i] = self.base_ptr[i]
            
            lltype.free(self.base_ptr, flavor='raw')
            self.base_ptr = new_ptr
            self.capacity = new_cap

    def alloc(self, nbytes, alignment=16):
        """Bump-pointer allocation. Returns raw pointer."""
        start = self._align(self.offset, alignment)
        self.ensure_capacity((start - self.offset) + nbytes)
        res = rffi.ptradd(self.base_ptr, start)
        self.offset = start + nbytes
        return res

    def put_u64(self, val):
        p = self.alloc(8, alignment=8)
        rffi.cast(rffi.ULONGLONGP, p)[0] = rffi.cast(rffi.ULONGLONG, val)

    def put_i64(self, val):
        p = self.alloc(8, alignment=8)
        rffi.cast(rffi.LONGLONGP, p)[0] = rffi.cast(rffi.LONGLONG, val)

    def append_i64(self, val):
        """Alias for compatibility with writer_table."""
        self.put_i64(val)

    def put_u128(self, val):
        p = self.alloc(16, alignment=16)
        rffi.cast(rffi.ULONGLONGP, p)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(val))
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(p, 8))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(val >> 64))

    def put_bytes(self, src_ptr, length):
        if length <= 0: return
        dest = self.alloc(length, alignment=1)
        if src_ptr:
            if we_are_translated():
                c_memmove(rffi.cast(rffi.VOIDP, dest),
                          rffi.cast(rffi.VOIDP, src_ptr),
                          rffi.cast(rffi.SIZE_T, length))
            else:
                for i in range(length):
                    dest[i] = src_ptr[i]
        else:
            for i in range(length):
                dest[i] = '\x00'

    def free(self):
        if self.base_ptr:
            lltype.free(self.base_ptr, flavor='raw')
            self.base_ptr = lltype.nullptr(rffi.CCHARP.TO)
