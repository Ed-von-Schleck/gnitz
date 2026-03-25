# gnitz/storage/table.py
#
# Thin FFI wrapper around Rust RustTable (persistent mode).

from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.storage import engine_ffi
from gnitz.storage.ephemeral_table import EphemeralTable


class PersistentTable(EphemeralTable):

    def __init__(self, directory, name, schema, table_id=1,
                 memtable_arena_size=1 * 1024 * 1024, validate_checksums=False):
        self.schema = schema
        self.table_id = table_id
        self.directory = directory
        self.name = name
        self.validate_checksums = validate_checksums
        self.is_closed = False

        schema_buf = engine_ffi.pack_schema(schema)
        try:
            with rffi.scoped_str2charp(directory) as dir_p:
                with rffi.scoped_str2charp(name) as name_p:
                    handle = engine_ffi._table_create_persistent(
                        dir_p, name_p,
                        rffi.cast(rffi.VOIDP, schema_buf),
                        rffi.cast(rffi.UINT, table_id),
                        rffi.cast(rffi.ULONGLONG, memtable_arena_size),
                    )
            if not handle:
                raise errors.StorageError("table_create_persistent failed")
            self._handle = handle
        finally:
            lltype.free(schema_buf, flavor="raw")

        from gnitz.storage import comparator as storage_comparator
        self._retract_accessor = storage_comparator.RetractRowAccessor(schema)
        self.current_lsn = r_uint64(engine_ffi._table_current_lsn(self._handle))

    def ingest_batch(self, batch):
        if self.is_closed:
            raise errors.StorageError("Table '%s' is closed" % self.name)
        if batch.length() == 0:
            return
        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)

        num_payload = len(self.schema.columns) - 1
        desc_buf = lltype.malloc(
            rffi.CCHARP.TO, engine_ffi.BATCH_DESC_SIZE, flavor="raw"
        )
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        col_sizes = lltype.malloc(rffi.ULONGLONGP.TO, num_payload, flavor="raw")
        try:
            engine_ffi.pack_batch_desc(desc_buf, batch, self.schema, num_payload, col_ptrs, col_sizes)
            rc = engine_ffi._table_ingest_batch(
                self._handle,
                rffi.cast(rffi.VOIDP, desc_buf),
                rffi.cast(rffi.ULONGLONG, lsn),
            )
            if intmask(rc) < 0:
                raise errors.StorageError("table_ingest_batch failed (%d)" % intmask(rc))
        finally:
            lltype.free(col_ptrs, flavor="raw")
            lltype.free(col_sizes, flavor="raw")
            lltype.free(desc_buf, flavor="raw")

    def ingest_batch_memonly(self, batch):
        if self.is_closed:
            raise errors.StorageError("Table '%s' is closed" % self.name)
        if batch.length() == 0:
            return
        num_payload = len(self.schema.columns) - 1
        desc_buf = lltype.malloc(
            rffi.CCHARP.TO, engine_ffi.BATCH_DESC_SIZE, flavor="raw"
        )
        col_ptrs = lltype.malloc(rffi.VOIDPP.TO, num_payload, flavor="raw")
        col_sizes = lltype.malloc(rffi.ULONGLONGP.TO, num_payload, flavor="raw")
        try:
            engine_ffi.pack_batch_desc(desc_buf, batch, self.schema, num_payload, col_ptrs, col_sizes)
            rc = engine_ffi._table_ingest_batch_memonly(
                self._handle,
                rffi.cast(rffi.VOIDP, desc_buf),
            )
            if intmask(rc) < 0:
                raise errors.StorageError("table_ingest_batch_memonly failed (%d)" % intmask(rc))
        finally:
            lltype.free(col_ptrs, flavor="raw")
            lltype.free(col_sizes, flavor="raw")
            lltype.free(desc_buf, flavor="raw")

    def flush(self):
        if self.is_closed:
            raise errors.StorageError("Table '%s' is closed" % self.name)
        rc = engine_ffi._table_flush(self._handle)
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError("table_flush failed (%d)" % rc_int)
        shard_name = "shard_%d_%d.db" % (self.table_id, intmask(self.current_lsn))
        return "" if rc_int == 0 else self.directory + "/" + shard_name

    def compact_if_needed(self):
        engine_ffi._table_compact_if_needed(self._handle)
