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
from gnitz.core.batch import pk_lt, pk_eq

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
        "pk_lo_buf",
        "pk_hi_buf",
        "w_buf",
        "null_buf",
        "col_bufs",
        "blob_buf",
        "dir_off",
        "dir_checksums",
        "col_to_reg_map",
        "blob_reg_idx",
        "xor8_filter",
    ]

    def __init__(self, filename, schema, validate_checksums=False):
        self.schema = schema
        self.validate_on_access = validate_checksums
        self.ptr = lltype.nullptr(rffi.CCHARP.TO)
        self.size = 0 # lltype.Signed

        try:
            fd = rposix.open(filename, os.O_RDONLY, 0)
        except OSError:
            raise errors.StorageError("shard file missing: " + filename)
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

                version = rffi.cast(
                    lltype.Signed,
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, layout.OFF_VERSION))[0],
                )
                if version != layout.VERSION:
                    raise errors.CorruptShardError("Unsupported shard version %d" % version)

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
                num_regions = 3 + 1 + (num_cols - 1) + 1  # pk_lo, pk_hi, Weight, Null, cols, blob

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

                # Eagerly initialize Region 0 (pk_lo), Region 1 (pk_hi), Region 2 (Weight), Region 3 (Null)
                self.pk_lo_buf = self._init_region(0)
                self.pk_hi_buf = self._init_region(1)
                self.w_buf = self._init_region(2)
                self.null_buf = self._init_region(3)

                # Map column buffers to regions
                col_bufs = newlist_hint(num_cols)
                reg_idx = 4
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

                # Parse embedded XOR8 filter from reserved header bytes
                xor8_off = rffi.cast(
                    lltype.Signed,
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, layout.OFF_XOR8_OFFSET))[0],
                )
                xor8_sz = rffi.cast(
                    lltype.Signed,
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, layout.OFF_XOR8_SIZE))[0],
                )
                if xor8_off > 0 and xor8_sz >= 16 and xor8_off + xor8_sz <= self.size:
                    from gnitz.storage.xor8 import parse_xor8_from_ptr
                    self.xor8_filter = parse_xor8_from_ptr(
                        rffi.ptradd(ptr, xor8_off), xor8_sz
                    )
                else:
                    self.xor8_filter = None

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

        # 6. Eager Validation for pk_lo/pk_hi/Weight (Region 0/1/2)
        if self.validate_on_access and idx < 3 and s_sz > 0:
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
        self._check_region(self.pk_lo_buf, 0)
        return self.pk_lo_buf.read_u64(index * 8)

    def get_pk_u128(self, index):
        if self.count == 0 or index >= self.count or index < 0:
            return r_uint128(0)
        self._check_region(self.pk_lo_buf, 0)
        self._check_region(self.pk_hi_buf, 1)
        lo = self.pk_lo_buf.read_u64(index * 8)
        hi = self.pk_hi_buf.read_u64(index * 8)
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_pk_lo(self, index):
        return self.pk_lo_buf.read_u64(index * 8)

    def get_pk_hi(self, index):
        return self.pk_hi_buf.read_u64(index * 8)

    def get_weight(self, index):
        if self.count == 0 or index >= self.count or index < 0:
            return 0
        self._check_region(self.w_buf, 2)
        return self.w_buf.read_i64(index * 8)

    def get_col_ptr(self, row_idx, col_idx):
        if col_idx == self.schema.pk_index:
            return rffi.ptradd(self.pk_lo_buf.ptr, row_idx * 8)

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

    def find_row_index(self, key_lo, key_hi):
        """Binary search for the FIRST occurrence of an exact primary key."""
        idx = self.find_lower_bound(key_lo, key_hi)
        if idx < self.count:
            if pk_eq(self.get_pk_lo(idx), self.get_pk_hi(idx), key_lo, key_hi):
                return idx
        return -1

    def find_lower_bound(self, key_lo, key_hi):
        """
        Binary search for the first row index where ShardKey >= key.
        Returns self.count if no such key exists.
        """
        low = 0
        high = self.count - 1
        res = self.count

        while low <= high:
            mid = (low + high) // 2
            if pk_lt(self.get_pk_lo(mid), self.get_pk_hi(mid), key_lo, key_hi):
                low = mid + 1
            else:
                res = mid
                high = mid - 1
        return res

    def close(self):
        if self.ptr:
            mmap_posix.munmap_file(self.ptr, self.size)
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)
