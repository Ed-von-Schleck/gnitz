# gnitz/storage/memtable.py
#
# Thin FFI wrapper around Rust RustMemTable.

from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch, ColumnarBatchAccessor
from gnitz.storage import buffer as buffer_mod, engine_ffi
from gnitz.storage.buffer import c_memmove


NULL_HANDLE = lltype.nullptr(rffi.VOIDP.TO)


class MemTable(object):

    _immutable_fields_ = ["schema", "max_bytes"]

    def __init__(self, schema, arena_size):
        self.schema = schema
        self.max_bytes = arena_size
        schema_buf = engine_ffi.pack_schema(schema)
        try:
            handle = engine_ffi._memtable_create(
                rffi.cast(rffi.VOIDP, schema_buf),
                rffi.cast(rffi.ULONGLONG, arena_size),
            )
            if not handle:
                raise errors.StorageError("memtable_create failed")
            self._handle = handle
        finally:
            lltype.free(schema_buf, flavor="raw")

    def upsert_batch(self, batch):
        if batch.length() == 0:
            return
        num_payload = len(self.schema.columns) - 1
        desc_buf = lltype.malloc(
            rffi.CCHARP.TO, engine_ffi.BATCH_DESC_SIZE, flavor="raw"
        )
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        col_sizes = lltype.malloc(rffi.ULONGLONGP.TO, num_payload, flavor="raw")
        try:
            _pack_batch_desc(
                desc_buf, batch, self.schema, num_payload, col_ptrs, col_sizes
            )
            rc = engine_ffi._memtable_upsert_batch(
                self._handle, rffi.cast(rffi.VOIDP, desc_buf),
            )
            rc_int = intmask(rc)
            if rc_int == -10:
                raise errors.MemTableFullError()
            if rc_int < 0:
                raise errors.StorageError(
                    "memtable_upsert_batch failed (%d)" % rc_int
                )
        finally:
            lltype.free(col_ptrs, flavor="raw")
            lltype.free(col_sizes, flavor="raw")
            lltype.free(desc_buf, flavor="raw")

    def upsert_single(self, key_lo, key_hi, weight, accessor):
        num_payload = len(self.schema.columns) - 1
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        try:
            pi = 0
            pk_index = self.schema.pk_index
            ci = 0
            num_cols = len(self.schema.columns)
            while ci < num_cols:
                if ci != pk_index:
                    col_ptrs[pi] = rffi.cast(rffi.VOIDP, accessor.get_col_ptr(ci))
                    pi += 1
                ci += 1

            null_word = r_uint64(0)
            for i in range(num_cols):
                if i == pk_index:
                    continue
                if accessor.is_null(i):
                    payload_idx = i if i < pk_index else i - 1
                    null_word |= r_uint64(1) << payload_idx

            blob_ptr = lltype.nullptr(rffi.CCHARP.TO)
            blob_len = 0
            if isinstance(accessor, ColumnarBatchAccessor):
                if accessor._batch is not None:
                    blob_ptr = accessor._batch.blob_arena.base_ptr
                    blob_len = accessor._batch.blob_arena.offset

            rc = engine_ffi._memtable_append_row(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                rffi.cast(rffi.LONGLONG, weight),
                rffi.cast(rffi.ULONGLONG, null_word),
                col_ptrs,
                rffi.cast(rffi.UINT, num_payload),
                rffi.cast(rffi.CCHARP, blob_ptr),
                rffi.cast(rffi.ULONGLONG, blob_len),
            )
            rc_int = intmask(rc)
            if rc_int == -10:
                raise errors.MemTableFullError()
            if rc_int < 0:
                raise errors.StorageError(
                    "memtable_append_row failed (%d)" % rc_int
                )
        finally:
            lltype.free(col_ptrs, flavor="raw")

    def flush(self, dirfd, basename, table_id=0, durable=True):
        with rffi.scoped_str2charp(basename) as name_p:
            rc = engine_ffi._memtable_flush(
                self._handle,
                rffi.cast(rffi.INT, dirfd),
                name_p,
                rffi.cast(rffi.UINT, table_id),
                rffi.cast(rffi.INT, 1 if durable else 0),
            )
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError("memtable_flush failed (%d)" % rc_int)
        return rc_int == 1

    def get_consolidated_snapshot(self):
        out_buf = lltype.malloc(
            rffi.CCHARP.TO, engine_ffi.MERGED_BATCH_SIZE, flavor="raw"
        )
        i = 0
        while i < engine_ffi.MERGED_BATCH_SIZE:
            out_buf[i] = '\x00'
            i += 1
        try:
            rc = engine_ffi._memtable_get_snapshot(
                self._handle, rffi.cast(rffi.VOIDP, out_buf),
            )
            if intmask(rc) < 0:
                raise errors.StorageError(
                    "memtable_get_snapshot failed (%d)" % intmask(rc)
                )
            result = _unpack_merged_batch(out_buf, self.schema)
        finally:
            engine_ffi._merged_batch_free(rffi.cast(rffi.VOIDP, out_buf))
            lltype.free(out_buf, flavor="raw")
        return result

    def lookup_pk(self, key_lo, key_hi):
        out_w = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor="raw")
        out_shard = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
        out_row = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
        try:
            out_w[0] = rffi.cast(rffi.LONGLONG, 0)
            out_shard[0] = lltype.nullptr(rffi.VOIDP.TO)
            out_row[0] = rffi.cast(rffi.INT, -1)
            found = engine_ffi._memtable_lookup_pk(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                out_w, rffi.cast(rffi.VOIDPP, out_shard), out_row,
            )
            weight = rffi.cast(lltype.Signed, out_w[0])
            shard_handle = out_shard[0]
            row_idx = intmask(out_row[0])
        finally:
            lltype.free(out_w, flavor="raw")
            lltype.free(out_shard, flavor="raw")
            lltype.free(out_row, flavor="raw")

        if intmask(found) == 0:
            shard_handle = NULL_HANDLE
            row_idx = -1

        return r_int64(weight), shard_handle, row_idx

    def find_weight_for_row(self, key_lo, key_hi, accessor):
        num_payload = len(self.schema.columns) - 1
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        col_sizes = lltype.malloc(rffi.ULONGLONGP.TO, num_payload, flavor="raw")
        try:
            pi = 0
            pk_index = self.schema.pk_index
            num_cols = len(self.schema.columns)
            ci = 0
            while ci < num_cols:
                if ci != pk_index:
                    col_ptrs[pi] = rffi.cast(rffi.VOIDP, accessor.get_col_ptr(ci))
                    col_sizes[pi] = rffi.cast(
                        rffi.ULONGLONG,
                        self.schema.columns[ci].field_type.size,
                    )
                    pi += 1
                ci += 1

            null_word = r_uint64(0)
            for i in range(num_cols):
                if i == pk_index:
                    continue
                if accessor.is_null(i):
                    payload_idx = i if i < pk_index else i - 1
                    null_word |= r_uint64(1) << payload_idx

            blob_ptr = lltype.nullptr(rffi.CCHARP.TO)
            blob_len = 0
            if isinstance(accessor, ColumnarBatchAccessor):
                if accessor._batch is not None:
                    blob_ptr = accessor._batch.blob_arena.base_ptr
                    blob_len = accessor._batch.blob_arena.offset

            w = engine_ffi._memtable_find_weight(
                self._handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                rffi.cast(rffi.ULONGLONG, null_word),
                col_ptrs, col_sizes,
                rffi.cast(rffi.UINT, num_payload),
                rffi.cast(rffi.CCHARP, blob_ptr),
                rffi.cast(rffi.ULONGLONG, blob_len),
            )
            return rffi.cast(lltype.Signed, w)
        finally:
            lltype.free(col_ptrs, flavor="raw")
            lltype.free(col_sizes, flavor="raw")

    def may_contain_pk(self, key_lo, key_hi):
        return bool(intmask(engine_ffi._memtable_may_contain(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )))

    def should_flush(self):
        return bool(intmask(engine_ffi._memtable_should_flush(self._handle)))

    def is_empty(self):
        return bool(intmask(engine_ffi._memtable_is_empty(self._handle)))

    def reset(self):
        engine_ffi._memtable_reset(self._handle)

    def free(self):
        if self._handle:
            engine_ffi._memtable_free(self._handle)
            self._handle = NULL_HANDLE


def _rust_merge_batches(batches, schema):
    """Merge N ArenaZSetBatches via Rust gnitz_merge_to_batch. Public for partitioned_table."""
    num_batches = len(batches)
    if num_batches == 0:
        return ArenaZSetBatch(schema)
    if num_batches == 1:
        result = batches[0].to_consolidated()
        if result is batches[0]:
            result = batches[0].clone()
        return result

    num_payload = len(schema.columns) - 1
    desc_buf = lltype.malloc(
        rffi.CCHARP.TO, num_batches * engine_ffi.BATCH_DESC_SIZE, flavor="raw"
    )
    schema_buf = engine_ffi.pack_schema(schema)
    out_buf = lltype.malloc(
        rffi.CCHARP.TO, engine_ffi.MERGED_BATCH_SIZE, flavor="raw"
    )
    i = 0
    while i < engine_ffi.MERGED_BATCH_SIZE:
        out_buf[i] = '\x00'
        i += 1

    all_col_ptrs = newlist_hint(num_batches)
    all_col_sizes = newlist_hint(num_batches)

    try:
        for ri in range(num_batches):
            base = rffi.ptradd(desc_buf, ri * engine_ffi.BATCH_DESC_SIZE)
            cp = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
            cs = lltype.malloc(rffi.ULONGLONGP.TO, num_payload, flavor="raw")
            all_col_ptrs.append(cp)
            all_col_sizes.append(cs)
            _pack_batch_desc(base, batches[ri], schema, num_payload, cp, cs)

        rc = engine_ffi._merge_to_batch(
            rffi.cast(rffi.VOIDP, desc_buf),
            rffi.cast(rffi.UINT, num_batches),
            rffi.cast(rffi.VOIDP, schema_buf),
            rffi.cast(rffi.VOIDP, out_buf),
        )
        if intmask(rc) < 0:
            raise errors.StorageError("merge_to_batch failed (%d)" % intmask(rc))
        result = _unpack_merged_batch(out_buf, schema)
    finally:
        engine_ffi._merged_batch_free(rffi.cast(rffi.VOIDP, out_buf))
        for p in all_col_ptrs:
            lltype.free(p, flavor="raw")
        for s in all_col_sizes:
            lltype.free(s, flavor="raw")
        lltype.free(out_buf, flavor="raw")
        lltype.free(schema_buf, flavor="raw")
        lltype.free(desc_buf, flavor="raw")
    return result


def _pack_batch_desc(base, batch, schema, num_payload, col_ptrs, col_sizes):
    pi = 0
    pk_index = schema.pk_index
    num_cols = len(schema.columns)
    ci = 0
    while ci < num_cols:
        if ci != pk_index:
            col_ptrs[pi] = rffi.cast(rffi.VOIDP, batch.col_bufs[ci].base_ptr)
            col_sizes[pi] = rffi.cast(rffi.ULONGLONG, batch.col_bufs[ci].offset)
            pi += 1
        ci += 1

    off = 0
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.pk_lo_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.pk_hi_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.weight_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.null_buf.base_ptr)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, col_ptrs)
    off += 8
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, col_sizes)
    off += 8
    rffi.cast(rffi.UINTP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.UINT, num_payload)
    off += 4
    off += 4  # padding
    rffi.cast(rffi.VOIDPP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.VOIDP, batch.blob_arena.base_ptr)
    off += 8
    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.ULONGLONG, batch.blob_arena.offset)
    off += 8
    rffi.cast(rffi.UINTP, rffi.ptradd(base, off))[0] = rffi.cast(
        rffi.UINT, batch.length())


def _read_merged_ptr(out_buf, off):
    p = rffi.cast(rffi.VOIDPP, rffi.ptradd(out_buf, off))[0]
    ln = intmask(rffi.cast(rffi.ULONGLONGP, rffi.ptradd(out_buf, off + 8))[0])
    return rffi.cast(rffi.CCHARP, p), ln


def _unpack_merged_batch(out_buf, schema):
    pk_index = schema.pk_index
    count = intmask(rffi.cast(rffi.UINTP, out_buf)[0])
    off = 8

    pk_lo_ptr, pk_lo_len = _read_merged_ptr(out_buf, off); off += 16
    pk_hi_ptr, pk_hi_len = _read_merged_ptr(out_buf, off); off += 16
    w_ptr, w_len = _read_merged_ptr(out_buf, off); off += 16
    n_ptr, n_len = _read_merged_ptr(out_buf, off); off += 16

    off += 8  # num_cols(4) + pad(4)
    col_ptrs_off = off
    col_lens_off = off + 64 * 8
    off = col_lens_off + 64 * 8

    blob_ptr, blob_len = _read_merged_ptr(out_buf, off)

    pk_lo_b = _copy_to_buffer(pk_lo_ptr, pk_lo_len)
    pk_hi_b = _copy_to_buffer(pk_hi_ptr, pk_hi_len)
    w_b = _copy_to_buffer(w_ptr, w_len)
    n_b = _copy_to_buffer(n_ptr, n_len)

    num_cols = len(schema.columns)
    col_bufs = newlist_hint(num_cols)
    col_strides = newlist_hint(num_cols)
    pi = 0
    ci = 0
    while ci < num_cols:
        if ci == pk_index:
            col_bufs.append(buffer_mod.Buffer(0))
            col_strides.append(0)
        else:
            sz = schema.columns[ci].field_type.size
            c_ptr = rffi.cast(rffi.CCHARP, rffi.cast(
                rffi.VOIDPP, rffi.ptradd(out_buf, col_ptrs_off + pi * 8))[0])
            c_len = intmask(rffi.cast(
                rffi.ULONGLONGP, rffi.ptradd(out_buf, col_lens_off + pi * 8))[0])
            col_bufs.append(_copy_to_buffer(c_ptr, c_len))
            col_strides.append(sz)
            pi += 1
        ci += 1

    blob_b = _copy_to_buffer(blob_ptr, blob_len)

    result = ArenaZSetBatch.from_buffers(
        schema, pk_lo_b, pk_hi_b, w_b, n_b,
        col_bufs, col_strides, blob_b, count, is_sorted=True,
    )
    result._consolidated = True
    return result


def _copy_to_buffer(src_ptr, size):
    buf = buffer_mod.Buffer(max(size, 1))
    if size > 0 and src_ptr:
        c_memmove(
            rffi.cast(rffi.VOIDP, buf.base_ptr),
            rffi.cast(rffi.VOIDP, src_ptr),
            rffi.cast(rffi.SIZE_T, size),
        )
    buf.offset = size
    return buf
