# gnitz/storage/shard_table.py

import os
# RPython imports
from rpython.rlib import jit, rposix, rposix_stat
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
# Project imports
from gnitz.core import errors
from gnitz.storage import buffer, layout, mmap_posix
from gnitz.core import strings as string_logic, types, xxh as checksum

DUMMY_BUF = buffer.MappedBuffer(lltype.nullptr(rffi.CCHARP.TO), 0)


class TableShardView(object):
    """
    N-Partition Columnar Shard Reader.
    Provides memory-mapped access to discrete columnar regions.
    """

    _immutable_fields_ = [
        "count",
        "schema",
        "ptr",
        "size",
        "pk_buf",
        "w_buf",
        "col_bufs",
        "blob_buf",
        "dir_off",
        "dir_checksums",
        "col_to_reg_map",
        "blob_reg_idx",
    ]

    def __init__(self, filename, schema, validate_checksums=False):
        self.schema = schema
        self.validate_on_access = validate_checksums
        self.ptr = lltype.nullptr(rffi.CCHARP.TO)
        self.size = 0 # lltype.Signed

        fd = rposix.open(filename, os.O_RDONLY, 0)
        try:
            st = rposix_stat.fstat(fd)
            # Authoritatively cast file size to Signed and guard non-negativity
            fsize = rffi.cast(lltype.Signed, st.st_size)
            if fsize < layout.HEADER_SIZE:
                raise errors.CorruptShardError("File too small for header")
            self.size = fsize

            # Mmap the file
            ptr = mmap_posix.mmap_file(
                fd, self.size, mmap_posix.PROT_READ, mmap_posix.MAP_SHARED
            )
            try:
                # Validation Barrier: check magic number
                if rffi.cast(rffi.ULONGLONGP, ptr)[0] != layout.MAGIC_NUMBER:
                    raise errors.CorruptShardError("Magic mismatch")

                self.ptr = ptr
                # Row Count (u64 on disk -> Signed in memory)
                self.count = rffi.cast(
                    lltype.Signed,
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, layout.OFF_ROW_COUNT))[0],
                )
                
                # Directory Offset (u64 on disk -> Signed in memory)
                d_off = rffi.cast(
                    lltype.Signed,
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, layout.OFF_DIR_OFFSET))[0],
                )
                
                # Tight bound check on directory offset
                if d_off < layout.HEADER_SIZE or d_off > self.size - layout.DIR_ENTRY_SIZE:
                    raise errors.CorruptShardError("Invalid directory offset")
                self.dir_off = d_off

                num_cols = len(schema.columns)
                num_regions = 2 + (num_cols - 1) + 1

                # Lists (AoS mapping / metadata)
                dir_checksums = newlist_hint(num_regions)
                for _ in range(num_regions):
                    dir_checksums.append(r_uint64(0))
                self.dir_checksums = dir_checksums

                region_validated = newlist_hint(num_regions)
                for _ in range(num_regions):
                    region_validated.append(False)
                self.region_validated = region_validated

                col_to_reg_map = newlist_hint(num_cols)
                for _ in range(num_cols):
                    col_to_reg_map.append(0)
                self.col_to_reg_map = col_to_reg_map

                # Eagerly initialize Region 0 (PK) and Region 1 (Weight)
                self.pk_buf = self._init_region(0)
                self.w_buf = self._init_region(1)

                # Map column buffers to regions
                col_bufs = newlist_hint(num_cols)
                reg_idx = 2
                i = 0
                while i < num_cols:
                    if i == schema.pk_index:
                        self.col_to_reg_map[i] = 0
                        col_bufs.append(DUMMY_BUF)
                        i += 1
                        continue

                    self.col_to_reg_map[i] = reg_idx
                    col_bufs.append(self._init_region(reg_idx))
                    reg_idx += 1
                    i += 1
                self.col_bufs = col_bufs

                # Initialize final region (Blob Heap)
                self.blob_buf = self._init_region(reg_idx)
                self.blob_reg_idx = reg_idx

            except Exception:
                mmap_posix.munmap_file(ptr, self.size)
                raise
        finally:
            rposix.close(fd)

    def _init_region(self, idx):
        """
        Maps a region from the Column Directory block.
        idx corresponds to the region index in N-Partition layout.
        """
        # 1. Verify index and calculate directory entry offset
        if idx < 0:
            raise errors.LayoutError("Negative region index")
            
        # Entry size is 24 bytes (Off u64, Sz u64, Cs u64)
        entry_offset = self.dir_off + (idx * 24)
        
        # Guard pointer arithmetic
        if entry_offset < 0 or entry_offset > (self.size - 24):
            raise errors.CorruptShardError("Region directory entry out of bounds")
            
        # 2. Get the entry pointer
        base_ptr = self.ptr
        if not base_ptr:
            raise errors.CorruptShardError("Shard pointer is null")
            
        entry_ptr = rffi.ptradd(base_ptr, entry_offset)
        
        # 3. Read metadata using fixed-width unsigned 64-bit logic
        u64_ptr = rffi.cast(rffi.ULONGLONGP, entry_ptr)
        u_off = r_uint64(u64_ptr[0])
        u_sz  = r_uint64(u64_ptr[1])
        u_cs  = r_uint64(u64_ptr[2])
        
        # 4. Perform bounds check using unsigned logic
        # u_limit must be cast from the signed self.size
        u_limit = r_uint64(self.size)
        
        if u_off > u_limit or u_sz > u_limit or (u_off + u_sz) > u_limit:
            raise errors.CorruptShardError("Region metadata points outside shard file")

        # 5. Convert to Signed for pointer application and buffer storage
        s_off = rffi.cast(lltype.Signed, u_off)
        s_sz  = rffi.cast(lltype.Signed, u_sz)
        
        self.dir_checksums[idx] = u_cs
        region_ptr = rffi.ptradd(base_ptr, s_off)

        # 6. Eager Validation for PK/Weight (Region 0/1)
        if self.validate_on_access and idx < 2 and s_sz > 0:
            if checksum.compute_checksum(region_ptr, s_sz) != u_cs:
                raise errors.CorruptShardError("Checksum mismatch in region %d" % idx)
            self.region_validated[idx] = True

        return buffer.MappedBuffer(region_ptr, s_sz)

    def _check_region(self, buf, idx):
        """Enforces the Ghost Property: Lazy validation for non-PK regions."""
        if self.validate_on_access and not self.region_validated[idx]:
            if buf.size > 0:
                actual = checksum.compute_checksum(buf.ptr, buf.size)
                if actual != self.dir_checksums[idx]:
                    raise errors.CorruptShardError(
                        "Checksum mismatch in region %d" % idx
                    )
            self.region_validated[idx] = True

    def get_pk_u64(self, index):
        if self.count == 0 or index >= self.count or index < 0:
            return r_uint64(0)
        self._check_region(self.pk_buf, 0)
        stride = self.schema.get_pk_column().field_type.size
        return self.pk_buf.read_u64(index * stride)

    def get_pk_u128(self, index):
        if self.count == 0 or index >= self.count or index < 0:
            return r_uint128(0)
        self._check_region(self.pk_buf, 0)
        stride = self.schema.get_pk_column().field_type.size
        ptr = rffi.ptradd(self.pk_buf.ptr, index * stride)
        low = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        high = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(high) << 64) | r_uint128(low)

    def get_weight(self, index):
        if self.count == 0 or index >= self.count or index < 0:
            return 0
        self._check_region(self.w_buf, 1)
        return self.w_buf.read_i64(index * 8)

    def get_col_ptr(self, row_idx, col_idx):
        if col_idx == self.schema.pk_index:
            return rffi.ptradd(
                self.pk_buf.ptr,
                row_idx * self.schema.get_pk_column().field_type.size,
            )

        buf = self.col_bufs[col_idx]
        if buf is DUMMY_BUF:
            return lltype.nullptr(rffi.CCHARP.TO)

        actual_reg_idx = self.col_to_reg_map[col_idx]
        self._check_region(buf, actual_reg_idx)

        stride = self.schema.columns[col_idx].field_type.size
        target_ptr = rffi.ptradd(buf.ptr, row_idx * stride)
        return target_ptr

    def read_field_i64(self, row_idx, col_idx):
        ptr = self.get_col_ptr(row_idx, col_idx)
        if not ptr:
            return 0

        sz = self.schema.columns[col_idx].field_type.size
        if sz == 8:
            return rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, ptr)[0])
        elif sz == 4:
            return rffi.cast(lltype.Signed, rffi.cast(rffi.INTP, ptr)[0])
        elif sz == 2:
            return rffi.cast(lltype.Signed, rffi.cast(rffi.SHORTP, ptr)[0])
        elif sz == 1:
            return rffi.cast(lltype.Signed, rffi.cast(rffi.SIGNEDCHARP, ptr)[0])
        return 0

    def read_field_f64(self, row_idx, col_idx):
        ptr = self.get_col_ptr(row_idx, col_idx)
        if not ptr:
            return 0.0

        sz = self.schema.columns[col_idx].field_type.size
        if sz == 4:
            return float(rffi.cast(rffi.FLOATP, ptr)[0])
        return float(rffi.cast(rffi.DOUBLEP, ptr)[0])

    def string_field_equals(self, row_idx, col_idx, search_str):
        ptr = self.get_col_ptr(row_idx, col_idx)
        if not ptr:
            return False
        # Lazy validate the blob heap only when a long string is accessed
        self._check_region(self.blob_buf, self.blob_reg_idx)
        prefix = string_logic.compute_prefix(search_str)
        return string_logic.string_equals(
            ptr, self.blob_buf.ptr, search_str, len(search_str), prefix
        )

    def find_row_index(self, key):
        """Binary search for the FIRST occurrence of an exact primary key."""
        idx = self.find_lower_bound(key)
        if idx < self.count:
            mid_key = (
                self.get_pk_u128(idx)
                if self.schema.get_pk_column().field_type.size == 16
                else r_uint128(self.get_pk_u64(idx))
            )
            if mid_key == key:
                return idx
        return -1

    def find_lower_bound(self, key):
        """
        Binary search for the first row index where ShardKey >= key.
        Returns self.count if no such key exists.
        """
        low = 0
        high = self.count - 1
        res = self.count
        is_u128 = self.schema.get_pk_column().field_type.size == 16

        while low <= high:
            mid = (low + high) // 2
            mid_key = (
                self.get_pk_u128(mid)
                if is_u128
                else r_uint128(self.get_pk_u64(mid))
            )

            if mid_key >= key:
                res = mid
                high = mid - 1
            else:
                low = mid + 1
        return res

    def close(self):
        if self.ptr:
            mmap_posix.munmap_file(self.ptr, self.size)
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)
