import os
from rpython.rlib import jit, rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long
from gnitz.storage import buffer, layout, mmap_posix, errors
from gnitz.core import strings as string_logic, types, checksum

class TableShardView(object):
    _immutable_fields_ = ['count', 'schema', 'ptr', 'size', 'pk_buf', 'w_buf', 'col_bufs[*]', 'blob_buf', 'dir_off', 'dir_checksums[*]']

    def __init__(self, filename, schema, validate_checksums=False):
        self.schema = schema
        fd = rposix.open(filename, os.O_RDONLY, 0)
        self.ptr = lltype.nullptr(rffi.CCHARP.TO)
        self.validate_on_access = validate_checksums
        
        try:
            st = os.fstat(fd)
            self.size = st.st_size
            if self.size < layout.HEADER_SIZE:
                raise errors.CorruptShardError("File too small for header")

            self.ptr = mmap_posix.mmap_file(fd, self.size, mmap_posix.PROT_READ, mmap_posix.MAP_SHARED)
            
            # Wrap initialization in try-block to ensure munmap on failure (Resource Leak Fix)
            try:
                if rffi.cast(rffi.ULONGLONGP, self.ptr)[0] != layout.MAGIC_NUMBER:
                    raise errors.CorruptShardError("Magic mismatch")
                
                self.count = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, layout.OFF_ROW_COUNT))[0])
                self.dir_off = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, layout.OFF_DIR_OFFSET))[0])
                
                num_regions = 2 + (len(schema.columns) - 1) + 1 # PK, W, N-1 Cols, B
                self.dir_checksums = [r_uint64(0)] * num_regions
                self.region_validated = [False] * num_regions

                def init_region(idx):
                    base = rffi.ptradd(self.ptr, self.dir_off + (idx * layout.DIR_ENTRY_SIZE))
                    off = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, base)[0])
                    sz = rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 8))[0])
                    cs = rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 16))[0])
                    
                    if off + sz > self.size:
                        raise errors.CorruptShardError("Region metadata points outside file")

                    self.dir_checksums[idx] = r_uint64(cs)
                    buf_ptr = rffi.ptradd(self.ptr, off)
                    
                    # [PERF] Eagerly validate PK and W for structural integrity ONLY if requested
                    # This prevents "Stop-the-World" on file open
                    if self.validate_on_access and idx < 2 and sz > 0:
                        if checksum.compute_checksum(buf_ptr, sz) != cs:
                            raise errors.CorruptShardError("Checksum mismatch in region %d" % idx)
                        self.region_validated[idx] = True
                        
                    return buffer.MappedBuffer(buf_ptr, sz)

                self.pk_buf = init_region(0)
                self.w_buf = init_region(1)
                
                self.col_bufs = [None] * len(schema.columns)
                reg_idx = 2
                for i in range(len(schema.columns)):
                    if i == schema.pk_index: continue
                    self.col_bufs[i] = init_region(reg_idx)
                    reg_idx += 1
                
                self.blob_buf = init_region(reg_idx)
                self.blob_reg_idx = reg_idx
                self.buf_b = self.blob_buf 

            except Exception:
                # Critical: Unmap memory if header/region validation fails
                mmap_posix.munmap_file(self.ptr, self.size)
                self.ptr = lltype.nullptr(rffi.CCHARP.TO)
                raise
            
        finally:
            rposix.close(fd)

    def _check_region(self, buf, idx):
        if self.validate_on_access and not self.region_validated[idx]:
            if buf.size > 0:
                actual = checksum.compute_checksum(buf.ptr, buf.size)
                if actual != self.dir_checksums[idx]:
                    raise errors.CorruptShardError("Checksum mismatch in region %d" % idx)
            self.region_validated[idx] = True

    def get_region_offset(self, idx):
        base = rffi.ptradd(self.ptr, self.dir_off + (idx * layout.DIR_ENTRY_SIZE))
        return rffi.cast(lltype.Signed, rffi.cast(rffi.ULONGLONGP, base)[0])

    def get_pk_u64(self, index):
        if self.count == 0 or index >= self.count: return r_uint64(0)
        self._check_region(self.pk_buf, 0)
        stride = self.schema.get_pk_column().field_type.size
        return self.pk_buf.read_u64(index * stride)

    def get_pk_u128(self, index):
        if self.count == 0 or index >= self.count: return r_uint128(0)
        self._check_region(self.pk_buf, 0)
        stride = self.schema.get_pk_column().field_type.size
        ptr = rffi.ptradd(self.pk_buf.ptr, index * stride)
        low = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        high = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(high) << 64) | r_uint128(low)

    def get_weight(self, index):
        if self.count == 0 or index >= self.count: return 0
        self._check_region(self.w_buf, 1)
        return self.w_buf.read_i64(index * 8)

    def get_col_ptr(self, row_idx, col_idx):
        if col_idx == self.schema.pk_index:
             return rffi.ptradd(self.pk_buf.ptr, row_idx * self.schema.get_pk_column().field_type.size)
        buf = self.col_bufs[col_idx]
        if not buf: return lltype.nullptr(rffi.CCHARP.TO)
        
        # Calculate region index for column buffers (starts at 2, skips PK)
        actual_reg_idx = 2
        for i in range(col_idx):
            if i != self.schema.pk_index: actual_reg_idx += 1
        self._check_region(buf, actual_reg_idx)
        
        stride = self.schema.columns[col_idx].field_type.size
        return rffi.ptradd(buf.ptr, row_idx * stride)

    def read_field_i64(self, row_idx, col_idx):
        ptr = self.get_col_ptr(row_idx, col_idx)
        if not ptr: return 0
        return rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, ptr)[0])
      
    def read_field_f64(self, row_idx, col_idx):
        ptr = self.get_col_ptr(row_idx, col_idx)
        if not ptr: return 0.0
        return float(rffi.cast(rffi.DOUBLEP, ptr)[0])

    def string_field_equals(self, row_idx, col_idx, search_str):
        ptr = self.get_col_ptr(row_idx, col_idx)
        if not ptr: return False
        self._check_region(self.blob_buf, self.blob_reg_idx)
        prefix = string_logic.compute_prefix(search_str)
        return string_logic.string_equals(ptr, self.blob_buf.ptr, search_str, len(search_str), prefix)

    def find_row_index(self, key):
        low = 0
        high = self.count - 1
        res = -1
        is_u128 = self.schema.get_pk_column().field_type.size == 16
        while low <= high:
            mid = (low + high) // 2
            mid_key = self.get_pk_u128(mid) if is_u128 else r_uint128(self.get_pk_u64(mid))
            if mid_key == key:
                res = mid
                high = mid - 1
            elif mid_key < key:
                low = mid + 1
            else:
                high = mid - 1
        return res

    def close(self):
        if self.ptr:
            mmap_posix.munmap_file(self.ptr, self.size)
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)

