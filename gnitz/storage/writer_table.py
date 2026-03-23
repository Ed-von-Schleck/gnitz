# gnitz/storage/writer_table.py

import os
from rpython.rlib import rposix, jit
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.longlong2float import float2longlong
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import errors
from gnitz.storage import layout, mmap_posix, buffer
from gnitz.storage.buffer import align_64, c_memmove, c_memset
from gnitz.core import (
    types,
    strings as string_logic,
    xxh as checksum,
)

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
    Uses Unified Accessor API to read from MemTable nodes or batch accessors.
    """

    _immutable_fields_ = ["schema", "table_id"]

    def __init__(self, schema, table_id=0):
        self.schema = schema
        self.table_id = table_id
        self.count = 0
        pk_col = schema.get_pk_column()

        self.pk_lo_buf = buffer.Buffer(8 * 1024, growable=True)
        self.pk_hi_buf = buffer.Buffer(8 * 1024, growable=True)
        self.w_buf = buffer.Buffer(8 * 1024, growable=True)

        num_cols = len(schema.columns)
        # Initialize col_bufs list using newlist_hint to avoid mr-poisoning
        self.col_bufs = newlist_hint(num_cols)
        i = 0
        while i < num_cols:
            if i != schema.pk_index:
                self.col_bufs.append(
                    buffer.Buffer(
                        schema.columns[i].field_type.size * 1024, growable=True
                    )
                )
            else:
                self.col_bufs.append(None)
            i += 1

        self.null_buf = buffer.Buffer(8 * 1024, growable=True)
        self.b_buf = buffer.Buffer(4096, growable=True)
        self.blob_cache = {}
        # scratch_val_buf is used for non-string fixed-width types (≤ 8 bytes)
        self.scratch_val_buf = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")
        self.blob_allocator = ShardWriterBlobAllocator(self)

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
            (
                length,
                prefix,
                src_struct_ptr,
                src_heap_ptr,
                py_string,
            ) = accessor.get_str_struct(col_idx)
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
            buf.put_i64(
                rffi.cast(rffi.LONGLONG, float2longlong(accessor.get_float(col_idx)))
            )

        elif type_code == types.TYPE_F32.code:
            val = accessor.get_float(col_idx)
            rffi.cast(rffi.FLOATP, self.scratch_val_buf)[0] = rffi.cast(rffi.FLOAT, val)
            buf.put_bytes(self.scratch_val_buf, 4)

        elif type_code == types.TYPE_U128.code:
            buf.put_u128(
                (r_uint128(accessor.get_u128_hi(col_idx)) << 64)
                | r_uint128(accessor.get_u128_lo(col_idx))
            )

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
        null_word = r_uint64(0)
        pk_index = self.schema.pk_index
        i = 0
        num_cols = len(self.schema.columns)
        while i < num_cols:
            if i == pk_index:
                i += 1
                continue
            if accessor.is_null(i):
                payload_idx = i if i < pk_index else i - 1
                null_word |= r_uint64(1) << payload_idx
            self._write_column(i, accessor)
            i += 1
        self.null_buf.put_u64(null_word)

    def add_row_from_accessor(self, key, weight, accessor):
        """Direct accessor ingestion — no AoS intermediate required."""
        if weight == 0:
            return
        self.count += 1
        self.pk_lo_buf.put_u64(r_uint64(intmask(key)))
        self.pk_hi_buf.put_u64(r_uint64(intmask(key >> 64)))
        self.w_buf.put_i64(weight)
        self._append_from_accessor(accessor)

    def close(self):
        self.pk_lo_buf.free()
        self.pk_hi_buf.free()
        self.w_buf.free()
        self.null_buf.free()
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

    def finalize(self, filename, durable=True):
        regions = _build_writer_regions(self)
        try:
            _write_shard_file(
                filename, self.table_id, self.count, regions,
                self.pk_lo_buf.base_ptr, self.pk_hi_buf.base_ptr,
                durable=durable,
            )
        finally:
            self.close()


def _build_writer_regions(writer):
    """Build region list from a TableShardWriter's buffers."""
    schema = writer.schema
    num_cols = len(schema.columns)
    num_regions = 3 + 1 + (num_cols - 1) + 1
    regions = newlist_hint(num_regions)
    regions.append((writer.pk_lo_buf.base_ptr, writer.pk_lo_buf.offset))
    regions.append((writer.pk_hi_buf.base_ptr, writer.pk_hi_buf.offset))
    regions.append((writer.w_buf.base_ptr, writer.w_buf.offset))
    regions.append((writer.null_buf.base_ptr, writer.null_buf.offset))
    i = 0
    num_bufs = len(writer.col_bufs)
    while i < num_bufs:
        b = writer.col_bufs[i]
        if b is not None:
            regions.append((b.base_ptr, b.offset))
        i += 1
    regions.append((writer.b_buf.base_ptr, writer.b_buf.offset))
    return regions


def _write_shard_file(filename, table_id, count, region_list, pk_lo_ptr, pk_hi_ptr,
                      durable=True):
    """
    Write a shard file from pre-built region buffers.

    region_list: list of (ptr, size) tuples in canonical region order:
        [pk_lo, pk_hi, weight, null, col0..colN, blob]
    pk_lo_ptr, pk_hi_ptr: raw pointers for XOR8 filter construction.

    Always writes to a .tmp file first then atomically renames to filename.
    This guarantees that the final file is either complete or absent — never
    a partial write visible to concurrent readers (or readers after a crash).

    durable=True  (PersistentTable): caller does one dir fsync after rename.
    durable=False (EphemeralTable):  no fsync; crash durability is not a
                  contract, but atomicity still prevents partial-read errors.
    """
    num_regions = len(region_list)
    target_filename = filename + ".tmp"

    # Phase 1: Compute layout (no I/O, no allocation)
    dir_size = num_regions * layout.DIR_ENTRY_SIZE
    dir_offset = layout.HEADER_SIZE
    current_pos = align_64(dir_offset + dir_size)

    dummy_final = (lltype.nullptr(rffi.CCHARP.TO), 0, 0)
    final_regions = newlist_hint(num_regions)
    for _ in range(num_regions):
        final_regions.append(dummy_final)

    i = 0
    while i < num_regions:
        buf_ptr, sz = region_list[i]
        final_regions[i] = (buf_ptr, current_pos, sz)
        current_pos = align_64(current_pos + sz)
        i += 1

    if num_regions > 0:
        _, last_off, last_sz = final_regions[num_regions - 1]
        total_file_size = last_off + last_sz
    else:
        total_file_size = layout.HEADER_SIZE

    # Phase 2: Build file image in a single contiguous buffer
    file_buf = lltype.malloc(rffi.CCHARP.TO, total_file_size, flavor="raw")
    try:
        c_memset(
            rffi.cast(rffi.VOIDP, file_buf),
            rffi.cast(rffi.INT, 0),
            rffi.cast(rffi.SIZE_T, total_file_size),
        )

        # Header
        rffi.cast(rffi.ULONGLONGP, file_buf)[0] = layout.MAGIC_NUMBER
        rffi.cast(
            rffi.ULONGLONGP, rffi.ptradd(file_buf, layout.OFF_VERSION)
        )[0] = rffi.cast(rffi.ULONGLONG, layout.VERSION)
        rffi.cast(
            rffi.ULONGLONGP, rffi.ptradd(file_buf, layout.OFF_ROW_COUNT)
        )[0] = rffi.cast(rffi.ULONGLONG, count)
        rffi.cast(
            rffi.ULONGLONGP, rffi.ptradd(file_buf, layout.OFF_DIR_OFFSET)
        )[0] = rffi.cast(rffi.ULONGLONG, dir_offset)
        rffi.cast(
            rffi.ULONGLONGP, rffi.ptradd(file_buf, layout.OFF_TABLE_ID)
        )[0] = rffi.cast(rffi.ULONGLONG, table_id)

        # Directory entries + region data in one pass
        i = 0
        while i < num_regions:
            buf_ptr, off, sz = final_regions[i]
            cs = checksum.compute_checksum(buf_ptr, sz)
            base = rffi.ptradd(file_buf, dir_offset + i * layout.DIR_ENTRY_SIZE)
            rffi.cast(rffi.ULONGLONGP, base)[0] = rffi.cast(rffi.ULONGLONG, off)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 8))[0] = rffi.cast(
                rffi.ULONGLONG, sz
            )
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 16))[0] = rffi.cast(
                rffi.ULONGLONG, cs
            )
            if sz > 0:
                c_memmove(
                    rffi.cast(rffi.VOIDP, rffi.ptradd(file_buf, off)),
                    rffi.cast(rffi.VOIDP, buf_ptr),
                    rffi.cast(rffi.SIZE_T, sz),
                )
            i += 1

        # Phase 3: Single write syscall
        fd = rposix.open(target_filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            mmap_posix.write_all(
                fd, file_buf, rffi.cast(rffi.SIZE_T, total_file_size)
            )
        finally:
            rposix.close(fd)
    finally:
        lltype.free(file_buf, flavor="raw")

    os.rename(target_filename, filename)
    # fsync_dir skipped here: EphemeralTable needs none;
    # PersistentTable.flush() does one dir fsync after all shard writes.

    if count > 0:
        from gnitz.storage.xor8 import build_xor8, save_xor8

        xor8_filter = build_xor8(pk_lo_ptr, pk_hi_ptr, count)
        if xor8_filter is not None:
            save_xor8(xor8_filter, filename + ".xor8")
            xor8_filter.free()


def write_batch_to_shard(batch, filename, table_id, durable=True):
    """
    Write a consolidated ArenaZSetBatch directly to a shard file.
    Bypasses TableShardWriter — no intermediate buffer copy.
    """
    count = batch.length()
    if count == 0:
        return

    schema = batch._schema
    num_cols = len(schema.columns)
    num_regions = 3 + 1 + (num_cols - 1) + 1
    regions = newlist_hint(num_regions)
    regions.append((batch.pk_lo_buf.base_ptr, batch.pk_lo_buf.offset))
    regions.append((batch.pk_hi_buf.base_ptr, batch.pk_hi_buf.offset))
    regions.append((batch.weight_buf.base_ptr, batch.weight_buf.offset))
    regions.append((batch.null_buf.base_ptr, batch.null_buf.offset))
    for ci in range(num_cols):
        if ci != schema.pk_index:
            regions.append((batch.col_bufs[ci].base_ptr, batch.col_bufs[ci].offset))
    regions.append((batch.blob_arena.base_ptr, batch.blob_arena.offset))

    _write_shard_file(
        filename, table_id, count, regions,
        batch.pk_lo_buf.base_ptr, batch.pk_hi_buf.base_ptr,
        durable=durable,
    )
