import os
from rpython.rlib import rposix, jit
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import layout, mmap_posix, errors
from gnitz.storage.raw_buffer import RawBuffer, copy_memory, compare_memory, align_64
from gnitz.core import checksum
from gnitz.core import types, strings as string_logic, values as db_values

NULL_CHARP = lltype.nullptr(rffi.CCHARP.TO)

class TableShardWriter(object):
    def __init__(self, schema, table_id=0):
        self.schema = schema
        self.table_id = table_id
        self.count = 0
        pk_col = schema.get_pk_column()
        self.pk_buf = RawBuffer(pk_col.field_type.size)
        self.w_buf = RawBuffer(8)

        # Pre-allocate fixed-size list
        num_cols = len(schema.columns)
        self.col_bufs = [None] * num_cols
        i = 0
        while i < num_cols:
            if i != schema.pk_index:
                self.col_bufs[i] = RawBuffer(schema.columns[i].field_type.size)
            i += 1

        self.b_buf = RawBuffer(1)
        self.blob_cache = {}
        # Pre-allocate scratch buffer to avoid mallocs during ingestion
        self.scratch_val_buf = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")

    def _get_or_append_blob(self, src_ptr, length):
        if length <= 0 or not src_ptr:
            return 0
        h = checksum.compute_checksum(src_ptr, length)
        cache_key = (h, length)
        if cache_key in self.blob_cache:
            existing_offset = self.blob_cache[cache_key]
            existing_ptr = rffi.ptradd(self.b_buf.ptr, existing_offset)
            if compare_memory(src_ptr, existing_ptr, length):
                return existing_offset
        new_offset = self.b_buf.count
        self.b_buf.append_bytes(src_ptr, length)
        self.blob_cache[cache_key] = new_offset
        return new_offset

    def _append_value(self, col_idx, val_ptr, source_heap_ptr, string_data=None):
        """Internal helper to relocate strings and append to columnar buffers."""
        buf = self.col_bufs[col_idx]
        col_def = self.schema.columns[col_idx]

        if not val_ptr and string_data is None:
            buf.append_bytes(NULL_CHARP, 1)
            return

        if col_def.field_type == types.TYPE_STRING:
            if string_data is None:
                string_data = ""

            length = 0
            if val_ptr:
                length = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, val_ptr)[0])
            else:
                length = len(string_data)

            if length > string_logic.SHORT_STRING_THRESHOLD:
                # Relocate long string to shard blob heap
                if val_ptr:
                    u64_view = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(val_ptr, 8))
                    old_off = rffi.cast(lltype.Signed, u64_view[0])
                    if not source_heap_ptr:
                        raise errors.StorageError("Long string relocation requires source heap")
                    blob_src = rffi.ptradd(source_heap_ptr, old_off)
                    new_off = self._get_or_append_blob(blob_src, length)

                    copy_memory(self.scratch_val_buf, val_ptr, 16)
                    u64_payload = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.scratch_val_buf, 8))
                    u64_payload[0] = rffi.cast(rffi.ULONGLONG, new_off)
                else:
                    with rffi.scoped_str2charp(string_data) as blob_src:
                        new_off = self._get_or_append_blob(blob_src, length)
                    
                    string_logic.pack_string(self.scratch_val_buf, string_data, new_off)
                
                buf.append_bytes(self.scratch_val_buf, 1)
            else:
                if val_ptr:
                    buf.append_bytes(val_ptr, 1)
                else:
                    string_logic.pack_string(self.scratch_val_buf, string_data, 0)
                    buf.append_bytes(self.scratch_val_buf, 1)
        else:
            buf.append_bytes(val_ptr, 1)

    def add_row(self, key, weight, packed_row_ptr, source_heap_ptr):
        """Optimized path for MemTable flush (raw pointers)."""
        if weight == 0:
            return
        self.count += 1

        if self.schema.get_pk_column().field_type.size == 16:
            self.pk_buf.append_u128(key)
        else:
            self.pk_buf.append_u64(rffi.cast(rffi.ULONGLONG, key))
        self.w_buf.append_i64(weight)

        i = 0
        num_cols = len(self.schema.columns)
        while i < num_cols:
            if i == self.schema.pk_index:
                i += 1
                continue

            col_off = self.schema.get_column_offset(i)
            val_ptr = rffi.ptradd(packed_row_ptr, col_off) if packed_row_ptr else NULL_CHARP
            self._append_value(i, val_ptr, source_heap_ptr)
            i += 1

    def add_row_from_values(self, pk, weight, values_list):
        """Entry point for DBValue-based ingestion."""
        if weight == 0:
            return
        self.count += 1

        if self.schema.get_pk_column().field_type.size == 16:
            self.pk_buf.append_u128(r_uint128(pk))
        else:
            self.pk_buf.append_u64(rffi.cast(rffi.ULONGLONG, pk))
        self.w_buf.append_i64(weight)

        val_idx = 0
        i = 0
        num_cols = len(self.schema.columns)
        while i < num_cols:
            if i == self.schema.pk_index:
                i += 1
                continue

            if val_idx >= len(values_list):
                self._append_value(i, NULL_CHARP, NULL_CHARP)
                i += 1
                continue

            val_obj = values_list[val_idx]
            val_idx += 1
            ftype = self.schema.columns[i].field_type

            if ftype == types.TYPE_STRING:
                self._append_value(i, NULL_CHARP, NULL_CHARP, string_data=val_obj.get_string())
            elif ftype == types.TYPE_F64:
                rffi.cast(rffi.DOUBLEP, self.scratch_val_buf)[0] = rffi.cast(
                    rffi.DOUBLE, val_obj.get_float()
                )
                self._append_value(i, self.scratch_val_buf, NULL_CHARP)
            else:
                rffi.cast(rffi.ULONGLONGP, self.scratch_val_buf)[0] = rffi.cast(
                    rffi.ULONGLONG, val_obj.get_int()
                )
                self._append_value(i, self.scratch_val_buf, NULL_CHARP)
            i += 1

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

            region_list[0] = (self.pk_buf.ptr, 0, self.pk_buf.count * self.pk_buf.item_size)
            region_list[1] = (self.w_buf.ptr, 0, self.w_buf.count * 8)

            reg_idx = 2
            i = 0
            num_bufs = len(self.col_bufs)
            while i < num_bufs:
                b = self.col_bufs[i]
                if b is not None:
                    region_list[reg_idx] = (b.ptr, 0, self.count * b.item_size)
                    reg_idx += 1
                i += 1

            region_list[reg_idx] = (self.b_buf.ptr, 0, self.b_buf.count)

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
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_VERSION))[0] = (
                    rffi.cast(rffi.ULONGLONG, layout.VERSION)
                )
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_ROW_COUNT))[
                    0
                ] = rffi.cast(rffi.ULONGLONG, self.count)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_DIR_OFFSET))[
                    0
                ] = rffi.cast(rffi.ULONGLONG, dir_offset)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_TABLE_ID))[
                    0
                ] = rffi.cast(rffi.ULONGLONG, self.table_id)
                mmap_posix.write_c(fd, header, rffi.cast(rffi.SIZE_T, layout.HEADER_SIZE))
            finally:
                lltype.free(header, flavor="raw")

            # 2. Write Directory
            dir_buf = lltype.malloc(rffi.CCHARP.TO, dir_size, flavor="raw")
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
                mmap_posix.write_c(fd, dir_buf, rffi.cast(rffi.SIZE_T, dir_size))
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
