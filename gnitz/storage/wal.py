# gnitz/storage/wal.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import errors
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage import wal_columnar, engine_ffi
from gnitz.storage import buffer as buffer_ops


class WALReader(object):
    """
    Reader for the columnar Z-Set Write-Ahead Log.
    Each block is a serialized ArenaZSetBatch.
    Uses Rust-owned mmap for zero-copy access during recovery.
    """

    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self._handle = lltype.nullptr(rffi.VOIDP.TO)
        self._base_ptr = lltype.nullptr(rffi.CCHARP.TO)
        self.file_size = 0
        self.closed = False

        out_ptr = lltype.malloc(rffi.CCHARPP.TO, 1, flavor="raw")
        out_size = lltype.malloc(rffi.LONGLONGP.TO, 1, flavor="raw")
        try:
            with rffi.scoped_str2charp(filename) as p:
                h = engine_ffi._wal_reader_open(p, out_ptr, out_size)
            if h:
                self._base_ptr = out_ptr[0]
                self.file_size = intmask(out_size[0])
        finally:
            lltype.free(out_ptr, flavor="raw")
            lltype.free(out_size, flavor="raw")

        if not h:
            raise errors.StorageError("Failed to open WAL for reading: %s"
                                      % filename)
        self._handle = h

    def read_next_block(self):
        if self.closed:
            return None

        schema = self.schema
        num_cols = len(schema.columns)
        num_non_pk = 0
        for i in range(num_cols):
            if i != schema.pk_index:
                num_non_pk += 1
        max_regions = 4 + num_non_pk + 1

        out_lsn = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_tid = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
        out_count = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
        out_num_regions = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
        out_blob_size = lltype.malloc(rffi.ULONGLONGP.TO, 1, flavor="raw")
        out_offsets = lltype.malloc(rffi.UINTP.TO, max_regions, flavor="raw")
        out_sizes = lltype.malloc(rffi.UINTP.TO, max_regions, flavor="raw")

        try:
            rc = engine_ffi._wal_reader_next(
                self._handle,
                out_lsn, out_tid, out_count,
                out_num_regions, out_blob_size,
                out_offsets, out_sizes,
                rffi.cast(rffi.UINT, max_regions),
            )
            rc_int = intmask(rc)
            if rc_int == 1:
                return None  # EOF
            if rc_int < 0:
                raise errors.CorruptShardError(
                    "Corrupt WAL block (error %d)" % rc_int
                )

            lsn = r_uint64(out_lsn[0])
            tid = intmask(out_tid[0])
            count = intmask(out_count[0])

            # Reconstruct batch from absolute region offsets
            region_idx = 0

            pk_lo_buf = buffer_ops.Buffer.from_existing_data(
                rffi.ptradd(self._base_ptr, intmask(out_offsets[region_idx])),
                intmask(out_sizes[region_idx]),
            )
            region_idx += 1

            pk_hi_buf = buffer_ops.Buffer.from_existing_data(
                rffi.ptradd(self._base_ptr, intmask(out_offsets[region_idx])),
                intmask(out_sizes[region_idx]),
            )
            region_idx += 1

            weight_buf = buffer_ops.Buffer.from_existing_data(
                rffi.ptradd(self._base_ptr, intmask(out_offsets[region_idx])),
                intmask(out_sizes[region_idx]),
            )
            region_idx += 1

            null_buf = buffer_ops.Buffer.from_existing_data(
                rffi.ptradd(self._base_ptr, intmask(out_offsets[region_idx])),
                intmask(out_sizes[region_idx]),
            )
            region_idx += 1

            col_bufs = newlist_hint(num_cols)
            col_strides = newlist_hint(num_cols)
            for ci in range(num_cols):
                if ci == schema.pk_index:
                    col_bufs.append(buffer_ops.Buffer(0, is_owned=True))
                    col_strides.append(0)
                else:
                    stride = schema.columns[ci].field_type.size
                    col_bufs.append(
                        buffer_ops.Buffer.from_existing_data(
                            rffi.ptradd(
                                self._base_ptr,
                                intmask(out_offsets[region_idx]),
                            ),
                            intmask(out_sizes[region_idx]),
                        )
                    )
                    col_strides.append(stride)
                    region_idx += 1

            blob_buf = buffer_ops.Buffer.from_existing_data(
                rffi.ptradd(self._base_ptr, intmask(out_offsets[region_idx])),
                intmask(out_sizes[region_idx]),
            )

            result_batch = ArenaZSetBatch.from_buffers(
                schema, pk_lo_buf, pk_hi_buf, weight_buf, null_buf,
                col_bufs, col_strides, blob_buf, count, is_sorted=False,
            )
            return wal_columnar.WALColumnarBlock(
                lsn, tid, result_batch,
                raw_buf=lltype.nullptr(rffi.CCHARP.TO),
            )
        finally:
            lltype.free(out_lsn, flavor="raw")
            lltype.free(out_tid, flavor="raw")
            lltype.free(out_count, flavor="raw")
            lltype.free(out_num_regions, flavor="raw")
            lltype.free(out_blob_size, flavor="raw")
            lltype.free(out_offsets, flavor="raw")
            lltype.free(out_sizes, flavor="raw")

    def iterate_blocks(self):
        while True:
            block = self.read_next_block()
            if block is None:
                break
            yield block

    def close(self):
        if not self.closed:
            if self._handle:
                engine_ffi._wal_reader_close(self._handle)
                self._handle = lltype.nullptr(rffi.VOIDP.TO)
            self._base_ptr = lltype.nullptr(rffi.CCHARP.TO)
            self.closed = True


class WALWriter(object):
    """
    Append-only writer for the columnar Z-Set WAL.
    Delegates encode + write + fdatasync to Rust via a single FFI call.
    """

    _immutable_fields_ = ["filename", "schema"]

    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.closed = False
        self._handle = lltype.nullptr(rffi.VOIDP.TO)

        out_err = lltype.malloc(rffi.INTP.TO, 1, flavor="raw")
        try:
            with rffi.scoped_str2charp(filename) as p:
                h = engine_ffi._wal_writer_open(p, out_err)
            err = intmask(out_err[0])
        finally:
            lltype.free(out_err, flavor="raw")

        if not h:
            if err == -2:
                raise errors.StorageError(
                    "WAL file is locked by another process"
                )
            raise errors.StorageError("Failed to open WAL file: %s" % filename)
        self._handle = h

    def append_batch(self, lsn, table_id, batch):
        if self.closed:
            raise errors.StorageError("Attempted to write to a closed WAL")

        if batch.length() == 0:
            return

        region_ptrs, region_sizes, num_regions, blob_size = (
            wal_columnar.gather_batch_regions(self.schema, batch)
        )
        try:
            rc = engine_ffi._wal_writer_append(
                self._handle,
                rffi.cast(rffi.ULONGLONG, lsn),
                rffi.cast(rffi.UINT, table_id),
                rffi.cast(rffi.UINT, batch.length()),
                region_ptrs,
                region_sizes,
                rffi.cast(rffi.UINT, num_regions),
                rffi.cast(rffi.ULONGLONG, blob_size),
            )
            rc_int = intmask(rc)
            if rc_int < 0:
                raise errors.StorageError(
                    "WAL append failed (error %d)" % rc_int
                )
        finally:
            lltype.free(region_ptrs, flavor="raw")
            lltype.free(region_sizes, flavor="raw")

    def truncate_before_lsn(self, lsn):
        if self.closed or not self._handle:
            return
        rc = engine_ffi._wal_writer_truncate(self._handle)
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError(
                "WAL truncate failed (error %d)" % rc_int
            )

    def close(self):
        if not self.closed:
            if self._handle:
                engine_ffi._wal_writer_close(self._handle)
                self._handle = lltype.nullptr(rffi.VOIDP.TO)
            self.closed = True
