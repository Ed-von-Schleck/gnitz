"""
gnitz/storage/writer_ecs.py

ECS Shard Writer with Penta-Partition format and checksums.
Writes Entity IDs, Weights, Component Data, and Blob Heap with integrity validation.
Refactored for efficient memory usage (RawBuffer) and Spec Compliance (Alignment).
"""
import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.jit import unrolling_iterable
from gnitz.storage import layout, mmap_posix, errors
from gnitz.core import types, strings as string_logic, checksum

MAX_FIELDS = 64
FIELD_INDICES = unrolling_iterable(range(MAX_FIELDS))

def _align_64(val):
    """Aligns value to the next 64-byte boundary."""
    return (val + 63) & ~63

def _copy_memory(dest_ptr, src_ptr, size):
    """
    Manual memory copy loop to avoid rffi.c_memcpy strict type issues.
    RPython JIT optimizes this well.
    """
    for i in range(size):
        dest_ptr[i] = src_ptr[i]

class RawBuffer(object):
    """
    A simplified, growable raw memory buffer using rffi.
    Replaces Python lists to avoid object boxing and overhead.
    Manages raw C memory manually.
    """
    def __init__(self, item_size, initial_capacity=4096):
        self.item_size = item_size
        self.count = 0
        self.capacity = initial_capacity
        # Allocate raw memory
        self.ptr = lltype.malloc(rffi.CCHARP.TO, self.capacity * self.item_size, flavor='raw')

    def ensure_capacity(self, needed_slots):
        new_count = self.count + needed_slots
        if new_count > self.capacity:
            # Growth strategy: Double or fit
            new_cap = max(self.capacity * 2, new_count)
            new_byte_size = new_cap * self.item_size
            old_byte_size = self.capacity * self.item_size
            
            new_ptr = lltype.malloc(rffi.CCHARP.TO, new_byte_size, flavor='raw')
            
            # Copy old data to new buffer
            if self.ptr:
                _copy_memory(new_ptr, self.ptr, old_byte_size)
                lltype.free(self.ptr, flavor='raw')
            
            self.ptr = new_ptr
            self.capacity = new_cap

    def append_i64(self, value):
        """Append a 64-bit integer."""
        # Check item_size in debug if possible, but for performance assume correct usage (size 8)
        self.ensure_capacity(1)
        offset = self.count * 8
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(self.ptr, offset))[0] = rffi.cast(rffi.LONGLONG, value)
        self.count += 1

    def append_bytes(self, source_ptr, length):
        """Append raw bytes from a pointer."""
        # Used for C-Region (fixed chunks) and B-Region (variable blobs)
        self.ensure_capacity(length)
        copy_size = length * self.item_size
        dest = rffi.ptradd(self.ptr, self.count * self.item_size)
        _copy_memory(dest, source_ptr, copy_size)
        self.count += length
        
    def append_from_string(self, s):
        """Append bytes from a Python string. Only for item_size=1."""
        length = len(s)
        self.ensure_capacity(length)
        dest = rffi.ptradd(self.ptr, self.count)
        for i in range(length):
            dest[i] = s[i]
        self.count += length

    def get_ptr_at(self, index):
        """Get pointer to element at index."""
        return rffi.ptradd(self.ptr, index * self.item_size)
    
    def free(self):
        if self.ptr:
            lltype.free(self.ptr, flavor='raw')
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)

class ECSShardWriter(object):
    def __init__(self, component_layout):
        self.layout = component_layout
        self.count = 0
        
        # Region E: Entity IDs (i64)
        self.e_buf = RawBuffer(8)
        # Region W: Weights (i64)
        self.w_buf = RawBuffer(8)
        # Region C: Component Data (fixed stride)
        self.c_buf = RawBuffer(self.layout.stride)
        # Region B: Blob Heap (byte stream)
        self.b_buf = RawBuffer(1)

        # Pre-allocated scratch buffer for packing rows in add_entity
        self.scratch_row = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')

    def add_entity(self, entity_id, *field_values):
        """Add entity with default weight of 1."""
        self._add_entity_weighted(entity_id, 1, *field_values)
    
    def _add_entity_weighted(self, entity_id, weight, *field_values):
        """Add entity with explicit weight for Z-Set operations."""
        self.count += 1
        self.e_buf.append_i64(entity_id)
        self.w_buf.append_i64(weight)
        
        # Pack fields into scratch buffer
        stride = self.layout.stride
        # Zero out scratch buffer
        for k in range(stride): 
            self.scratch_row[k] = '\x00'
            
        for i in FIELD_INDICES:
            if i >= len(field_values) or i >= len(self.layout.field_types): break
            val = field_values[i]
            f_type = self.layout.field_types[i]
            f_off = self.layout.field_offsets[i]
            dest = rffi.ptradd(self.scratch_row, f_off)
            
            if f_type == types.TYPE_STRING:
                s_val = str(val)
                l_val = len(s_val)
                heap_off = 0
                if l_val > string_logic.SHORT_STRING_THRESHOLD:
                    # Append string to Blob Heap buffer directly
                    heap_off = self.b_buf.count
                    self.b_buf.append_from_string(s_val)
                
                string_logic.pack_string(dest, s_val, heap_off)
            elif isinstance(val, int) or isinstance(val, long):
                rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(rffi.LONGLONG, val)
            elif isinstance(val, float):
                rffi.cast(rffi.DOUBLEP, dest)[0] = rffi.cast(rffi.DOUBLE, val)
        
        # Append packed scratch row to C buffer
        self.c_buf.append_bytes(self.scratch_row, 1) # scratch_row is stride bytes, item_size is stride

    def add_packed_row(self, entity_id, weight, source_row_ptr, source_heap_ptr):
        """
        Adds pre-serialized data from MemTable/Compactor.
        Relocates Long German Strings to shard's blob heap.
        Enforces Ghost Property: skips records with weight == 0.
        """
        # Ghost Property: Skip annihilated records
        if weight == 0:
            return
        
        self.count += 1
        self.e_buf.append_i64(entity_id)
        self.w_buf.append_i64(weight)
        
        stride = self.layout.stride
        
        # Ensure space in C buffer
        # c_buf has item_size=stride. ensure_capacity(1) ensures 1 stride-sized slot.
        self.c_buf.ensure_capacity(1)
        
        # Get pointer to destination in C buffer
        dest_c_ptr = self.c_buf.get_ptr_at(self.c_buf.count)
        
        # Copy raw bytes from source
        _copy_memory(dest_c_ptr, source_row_ptr, stride)
        
        # Scan and fix up German Strings (Relocation)
        for i in FIELD_INDICES:
            if i >= len(self.layout.field_types): break
            f_type = self.layout.field_types[i]
            if f_type == types.TYPE_STRING:
                f_off = self.layout.field_offsets[i]
                s_ptr = rffi.ptradd(dest_c_ptr, f_off)
                u32_ptr = rffi.cast(rffi.UINTP, s_ptr)
                length = rffi.cast(lltype.Signed, u32_ptr[0])
                
                if length > string_logic.SHORT_STRING_THRESHOLD:
                    # It's a long string.
                    # 1. Read old offset (relative to source heap)
                    u64_ptr = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(s_ptr, 8))
                    old_offset = rffi.cast(lltype.Signed, u64_ptr[0])
                    
                    # 2. Get pointer to string data in Source Heap
                    src_str_ptr = rffi.ptradd(source_heap_ptr, old_offset)
                    
                    # 3. New offset will be current end of Destination Blob Buffer
                    new_offset = self.b_buf.count
                    
                    # 4. Copy string data to Destination Blob Buffer
                    self.b_buf.append_bytes(src_str_ptr, length)
                    
                    # 5. Update offset in Destination C Record
                    u64_ptr[0] = rffi.cast(rffi.ULONGLONG, new_offset)
        
        # Manually increment count for C buffer (since we used ensure_capacity + ptr access)
        self.c_buf.count += 1

    def finalize(self, filename):
        """Write shard with Penta-Partition format, alignment, and checksums."""
        fd = rposix.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            # 1. Calculate Sizes and Offsets with 64-byte Alignment (Spec Section 5.2)
            # Header is 64 bytes (already aligned)
            off_e = layout.HEADER_SIZE
            size_e = self.count * 8
            
            # Pad after E
            aligned_end_e = _align_64(off_e + size_e)
            pad_e = aligned_end_e - (off_e + size_e)
            
            off_w = aligned_end_e
            size_w = self.count * 8
            
            # Pad after W
            aligned_end_w = _align_64(off_w + size_w)
            pad_w = aligned_end_w - (off_w + size_w)
            
            off_c = aligned_end_w
            size_c = self.count * self.layout.stride
            
            # Pad after C
            aligned_end_c = _align_64(off_c + size_c)
            pad_c = aligned_end_c - (off_c + size_c)
            
            off_b = aligned_end_c
            size_b = self.b_buf.count
            
            # 2. Compute Checksums on raw buffers (excluding padding)
            # Spec: Checksum appended to end of each individual region (implied coverage of data)
            checksum_e = checksum.compute_checksum(self.e_buf.ptr, size_e)
            checksum_w = checksum.compute_checksum(self.w_buf.ptr, size_w)
            
            # 3. Write Header
            h = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor='raw')
            try:
                for i in range(layout.HEADER_SIZE): h[i] = '\x00'
                
                # Write fields
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_MAGIC))[0] = rffi.cast(rffi.LONGLONG, layout.MAGIC_NUMBER)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_COUNT))[0] = rffi.cast(rffi.LONGLONG, self.count)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(h, layout.OFF_CHECKSUM_E))[0] = rffi.cast(rffi.ULONGLONG, checksum_e)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(h, layout.OFF_CHECKSUM_W))[0] = rffi.cast(rffi.ULONGLONG, checksum_w)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_E_ECS))[0] = rffi.cast(rffi.LONGLONG, off_e)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_W_ECS))[0] = rffi.cast(rffi.LONGLONG, off_w)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_C_ECS))[0] = rffi.cast(rffi.LONGLONG, off_c)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(h, layout.OFF_REG_B_ECS))[0] = rffi.cast(rffi.LONGLONG, off_b)
                
                mmap_posix.write_c(fd, h, rffi.cast(rffi.SIZE_T, layout.HEADER_SIZE))
            finally:
                lltype.free(h, flavor='raw')
            
            # 4. Write Region E
            if size_e > 0:
                mmap_posix.write_c(fd, self.e_buf.ptr, rffi.cast(rffi.SIZE_T, size_e))
            
            # 5. Write Padding E
            self._write_padding(fd, pad_e)
            
            # 6. Write Region W
            if size_w > 0:
                mmap_posix.write_c(fd, self.w_buf.ptr, rffi.cast(rffi.SIZE_T, size_w))
                
            # 7. Write Padding W
            self._write_padding(fd, pad_w)
            
            # 8. Write Region C
            if size_c > 0:
                # C buffer is stride * count bytes
                mmap_posix.write_c(fd, self.c_buf.ptr, rffi.cast(rffi.SIZE_T, size_c))
                
            # 9. Write Padding C
            self._write_padding(fd, pad_c)
            
            # 10. Write Region B
            if size_b > 0:
                mmap_posix.write_c(fd, self.b_buf.ptr, rffi.cast(rffi.SIZE_T, size_b))
                
        finally:
            rposix.close(fd)
            # Cleanup raw buffers
            self.e_buf.free()
            self.w_buf.free()
            self.c_buf.free()
            self.b_buf.free()
            if self.scratch_row:
                lltype.free(self.scratch_row, flavor='raw')
                self.scratch_row = lltype.nullptr(rffi.CCHARP.TO)

    def _write_padding(self, fd, count):
        """Helper to write zero-byte padding."""
        if count <= 0: return
        pad_buf = lltype.malloc(rffi.CCHARP.TO, count, flavor='raw')
        try:
            for i in range(count): pad_buf[i] = '\x00'
            mmap_posix.write_c(fd, pad_buf, rffi.cast(rffi.SIZE_T, count))
        finally:
            lltype.free(pad_buf, flavor='raw')
