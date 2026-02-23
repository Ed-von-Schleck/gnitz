# gnitz/storage/writer_table.py

import os
from rpython.rlib import rposix, jit
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.longlong2float import float2longlong
from gnitz.core import errors
from gnitz.storage import layout, mmap_posix, buffer
from gnitz.storage.buffer import align_64
from gnitz.core import types, strings as string_logic, values as db_values, xxh as checksum
from gnitz.storage import comparator as storage_comparator
from gnitz.core import comparator as core_comparator

NULL_CHARP = lltype.nullptr(rffi.CCHARP.TO)


class ShardWriterBlobAllocator(string_logic.BlobAllocator):
    """
    Allocator strategy for TableShardWriter string tails.
    Wraps the shard's deduplication and blob heap logic.
    """

    def __init__(self, writer):
        self.writer = writer

    def allocate(self, string_data):
        length = len(string_data)
        # Use scoped_str2charp to safely pass Python string to C-level deduplication
        with rffi.scoped_str2charp(string_data) as blob_src:
            off = self.writer._get_or_append_blob(blob_src, length)
            return r_uint64(off)

    def allocate_from_ptr(self, src_ptr, length):
        """Zero-copy allocation from raw pointer."""
        off = self.writer._get_or_append_blob(src_ptr, length)
        return r_uint64(off)


class TableShardWriter(object):
    """
    Handles the creation of N-Partition columnar shards.
    Uses Unified Accessor API to read from MemTable nodes (Raw) or PayloadRows.
    """

    _immutable_fields_ = ["schema", "table_id"]

    def __init__(self, schema, table_id=0):
        self.schema = schema
        self.table_id = table_id
        self.count = 0
        pk_col = schema.get_pk_column()

        self.pk_buf = buffer.Buffer(pk_col.field_type.size * 1024, growable=True)
        self.w_buf = buffer.Buffer(8 * 1024, growable=True)

        num_cols = len(schema.columns)
        self.col_bufs = [None] * num_cols
        i = 0
        while i < num_cols:
            if i != schema.pk_index:
                self.col_bufs[i] = buffer.Buffer(
                    schema.columns[i].field_type.size * 1024, growable=True
                )
            i += 1

        self.b_buf = buffer.Buffer(4096, growable=True)
        self.blob_cache = {}
        # scratch_val_buf is used for non-string fixed-width types (≤ 8 bytes)
        self.scratch_val_buf = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")
        self.blob_allocator = ShardWriterBlobAllocator(self)

        # Unified Accessors for reading input
        self.raw_accessor = storage_comparator.RawWALAccessor(schema)
        self.val_accessor = core_comparator.PayloadRowAccessor(schema)

    def _get_or_append_blob(self, src_ptr, length):
        """
        Deduplicates and appends long string data to the shard-wide blob heap.
        """
        if length <= 0 or not src_ptr:
            return 0
        h = checksum.compute_checksum(src_ptr, length)
        cache_key = (h, length)
        if cache_key in self.blob_cache:
            existing_offset = self.blob_cache[cache_key]
            existing_ptr = rffi.ptradd(self.b_buf.ptr, existing_offset)

            # Binary comparison to ensure deduplication safety (collisions)
            match = True
            for i in range(length):
                if src_ptr[i] != existing_ptr[i]:
                    match = False
                    break
            if match:
                return existing_offset

        new_offset = self.b_buf.offset
        self.b_buf.put_bytes(src_ptr, length)
        self.blob_cache[cache_key] = new_offset
        return new_offset

    def _write_column(self, col_idx, accessor):
        """
        Writes a single column value from the accessor to the column buffer.
        Acts as the SoA equivalent of serialize.serialize_row.
        """
        buf = self.col_bufs[col_idx]
        col_def = self.schema.columns[col_idx]
        type_code = col_def.field_type.code

        if accessor.is_null(col_idx):
            buf.put_bytes(NULL_CHARP, col_def.field_type.size)
            return

        if type_code == types.TYPE_STRING.code:
            length, prefix, src_struct_ptr, src_heap_ptr, py_string = accessor.get_str_struct(
                col_idx
            )
            # Allocate the 16-byte slot directly inside the column buffer so
            # relocate_string can write into it without an intermediate scratch
            # copy.  8-byte alignment satisfies the u64 offset/payload field
            # requirement; the full struct is 16 bytes as per the spec.
            dest = buf.alloc(16, alignment=8)
            string_logic.relocate_string(
                dest,
                length,
                prefix,
                src_struct_ptr,
                src_heap_ptr,
                py_string,
                self.blob_allocator,
            )

        elif type_code == types.TYPE_F64.code:
            buf.put_i64(rffi.cast(rffi.LONGLONG, float2longlong(accessor.get_float(col_idx))))

        elif type_code == types.TYPE_F32.code:
            val = accessor.get_float(col_idx)
            rffi.cast(rffi.FLOATP, self.scratch_val_buf)[0] = rffi.cast(rffi.FLOAT, val)
            buf.put_bytes(self.scratch_val_buf, 4)

        elif type_code == types.TYPE_U128.code:
            buf.put_u128(accessor.get_u128(col_idx))

        elif type_code == types.TYPE_U64.code:
            buf.put_u64(accessor.get_int(col_idx))

        elif type_code == types.TYPE_I64.code:
            buf.put_i64(rffi.cast(rffi.LONGLONG, accessor.get_int(col_idx)))

        elif type_code == types.TYPE_U32.code:
            val = accessor.get_int(col_idx) & 0xFFFFFFFF
            rffi.cast(rffi.UINTP, self.scratch_val_buf)[0] = rffi.cast(rffi.UINT, val)
            buf.put_bytes(self.scratch_val_buf, 4)

        elif type_code == types.TYPE_I32.code:
            val = accessor.get_int(col_idx) & 0xFFFFFFFF
            rffi.cast(rffi.INTP, self.scratch_val_buf)[0] = rffi.cast(rffi.INT, val)
            buf.put_bytes(self.scratch_val_buf, 4)

        elif type_code == types.TYPE_U16.code:
            val = intmask(accessor.get_int(col_idx))
            self.scratch_val_buf[0] = chr(val & 0xFF)
            self.scratch_val_buf[1] = chr((val >> 8) & 0xFF)
            buf.put_bytes(self.scratch_val_buf, 2)

        elif type_code == types.TYPE_I16.code:
            val = intmask(accessor.get_int(col_idx))
            self.scratch_val_buf[0] = chr(val & 0xFF)
            self.scratch_val_buf[1] = chr((val >> 8) & 0xFF)
            buf.put_bytes(self.scratch_val_buf, 2)

        elif type_code == types.TYPE_U8.code or type_code == types.TYPE_I8.code:
            val = intmask(accessor.get_int(col_idx))
            self.scratch_val_buf[0] = chr(val & 0xFF)
            buf.put_bytes(self.scratch_val_buf, 1)

        else:
            # Fallback for I64-compatible types
            buf.put_i64(rffi.cast(rffi.LONGLONG, accessor.get_int(col_idx)))

    def _append_from_accessor(self, accessor):
        i = 0
        num_cols = len(self.schema.columns)
        while i < num_cols:
            if i == self.schema.pk_index:
                i += 1
                continue
            self._write_column(i, accessor)
            i += 1

    def add_row(self, key, weight, packed_row_ptr, source_heap_ptr):
        """Optimized path for MemTable flush (raw pointers)."""
        if weight == 0:
            return
        self.count += 1

        if self.schema.get_pk_column().field_type.size == 16:
            self.pk_buf.put_u128(key)
        else:
            self.pk_buf.put_u64(r_uint64(key))
        self.w_buf.put_i64(weight)

        # Zero-allocation wrap of raw pointers
        self.raw_accessor.set_pointers(packed_row_ptr, source_heap_ptr)
        self._append_from_accessor(self.raw_accessor)

    def add_row_from_values(self, pk, weight, row):
        """
        Ingests data from a PayloadRow.
        Used by the test suite and high-level ingestion API.
        """
        if weight == 0:
            return
        self.count += 1

        if self.schema.get_pk_column().field_type.size == 16:
            self.pk_buf.put_u128(r_uint128(pk))
        else:
            self.pk_buf.put_u64(r_uint64(pk))
        self.w_buf.put_i64(weight)

        self.val_accessor.set_row(row)
        self._append_from_accessor(self.val_accessor)

    def close(self):
        self.pk_buf.free()
        self.w_buf.free()
        i = 0
        num_bufs = len(self.col_bufs)
        while i < num_bufs:
            b = self.col_bufs[i]
            if b:
                b.free()
            i += 1
        self.b_buf.free()
        if self.scratch_val_buf:
            lltype.free(self.scratch_val_buf, flavor="raw")
            self.scratch_val_buf = lltype.nullptr(rffi.CCHARP.TO)

    def finalize(self, filename):
        tmp_filename = filename + ".tmp"
        fd = rposix.open(tmp_filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            dummy_region = (lltype.nullptr(rffi.CCHARP.TO), 0, 0)

            num_cols_with_pk_w = 2 + (len(self.col_bufs) - 1)
            region_list = [dummy_region] * (num_cols_with_pk_w + 1)

            region_list[0] = (self.pk_buf.ptr, 0, self.pk_buf.offset)
            region_list[1] = (self.w_buf.ptr, 0, self.w_buf.offset)

            reg_idx = 2
            i = 0
            num_bufs = len(self.col_bufs)
            while i < num_bufs:
                b = self.col_bufs[i]
                if b is not None:
                    region_list[reg_idx] = (b.ptr, 0, b.offset)
                    reg_idx += 1
                i += 1

            region_list[reg_idx] = (self.b_buf.ptr, 0, self.b_buf.offset)

            num_regions = len(region_list)
            dir_size = num_regions * layout.DIR_ENTRY_SIZE
            dir_offset = layout.HEADER_SIZE
            current_pos = align_64(dir_offset + dir_size)

            final_regions = [dummy_region] * num_regions
            i = 0
            while i < num_regions:
                buf_ptr, _, sz = region_list[i]
                final_regions[i] = (buf_ptr, current_pos, sz)
                current_pos = align_64(current_pos + sz)
                i += 1

            # 1. Write Header
            header = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor="raw")
            try:
                i = 0
                while i < layout.HEADER_SIZE:
                    header[i] = "\x00"
                    i += 1
                rffi.cast(rffi.ULONGLONGP, header)[0] = layout.MAGIC_NUMBER
                rffi.cast(
                    rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_VERSION)
                )[0] = rffi.cast(rffi.ULONGLONG, layout.VERSION)
                rffi.cast(
                    rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_ROW_COUNT)
                )[0] = rffi.cast(rffi.ULONGLONG, self.count)
                rffi.cast(
                    rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_DIR_OFFSET)
                )[0] = rffi.cast(rffi.ULONGLONG, dir_offset)
                rffi.cast(
                    rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_TABLE_ID)
                )[0] = rffi.cast(rffi.ULONGLONG, self.table_id)
                mmap_posix.write_c(
                    fd, header, rffi.cast(rffi.SIZE_T, layout.HEADER_SIZE)
                )
            finally:
                lltype.free(header, flavor="raw")

            # 2. Write Directory
            dir_size_bytes = dir_size
            dir_buf = lltype.malloc(rffi.CCHARP.TO, dir_size_bytes, flavor="raw")
            try:
                i = 0
                while i < num_regions:
                    buf_ptr, off, sz = final_regions[i]
                    cs = checksum.compute_checksum(buf_ptr, sz)
                    base = rffi.ptradd(dir_buf, i * layout.DIR_ENTRY_SIZE)
                    rffi.cast(rffi.ULONGLONGP, base)[0] = rffi.cast(rffi.ULONGLONG, off)
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 8))[0] = rffi.cast(
                        rffi.ULONGLONG, sz
                    )
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 16))[0] = rffi.cast(
                        rffi.ULONGLONG, cs
                    )
                    i += 1
                mmap_posix.write_c(fd, dir_buf, rffi.cast(rffi.SIZE_T, dir_size_bytes))
            finally:
                lltype.free(dir_buf, flavor="raw")

            # 3. Write Padded Regions
            last_written_pos = dir_offset + dir_size
            i = 0
            while i < num_regions:
                buf_ptr, off, sz = final_regions[i]
                padding = off - last_written_pos
                if padding > 0:
                    pad_buf = lltype.malloc(rffi.CCHARP.TO, padding, flavor="raw")
                    try:
                        j = 0
                        while j < padding:
                            pad_buf[j] = "\x00"
                            j += 1
                        mmap_posix.write_c(fd, pad_buf, rffi.cast(rffi.SIZE_T, padding))
                    finally:
                        lltype.free(pad_buf, flavor="raw")
                if sz > 0:
                    mmap_posix.write_c(fd, buf_ptr, rffi.cast(rffi.SIZE_T, sz))
                last_written_pos = off + sz
                i += 1

            mmap_posix.fsync_c(fd)
        finally:
            rposix.close(fd)
            self.close()

        os.rename(tmp_filename, filename)
        mmap_posix.fsync_dir(filename)
