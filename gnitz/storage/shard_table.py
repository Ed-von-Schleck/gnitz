# gnitz/storage/shard_table.py
#
# Columnar shard reader backed by Rust (libgnitz_engine).

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import errors
from gnitz.core import strings as string_logic
from gnitz.storage import engine_ffi
from gnitz.storage.xor8 import Xor8Filter


NULL_HANDLE = lltype.nullptr(rffi.VOIDP.TO)


class TableShardView(object):
    """
    N-Partition Columnar Shard Reader.
    Delegates to Rust shard_reader via FFI.
    """

    _immutable_fields_ = ["count", "schema", "_handle", "_blob_ptr"]

    def __init__(self, filename, schema, validate_checksums=False):
        self.schema = schema
        self._handle = NULL_HANDLE
        self._blob_ptr = lltype.nullptr(rffi.CCHARP.TO)
        self.xor8_filter = None
        self.count = 0

        schema_buf = engine_ffi.pack_schema(schema)
        fn_str = rffi.str2charp(filename)
        try:
            handle = engine_ffi._shard_open(
                fn_str,
                rffi.cast(rffi.VOIDP, schema_buf),
                rffi.cast(rffi.INT, 1 if validate_checksums else 0),
            )
            if not handle:
                raise errors.StorageError("shard open failed: " + filename)
            self._handle = handle
            self.count = intmask(engine_ffi._shard_row_count(handle))
            self._blob_ptr = engine_ffi._shard_blob_ptr(handle)
            has_xor8 = bool(intmask(engine_ffi._shard_has_xor8(handle)))
            if has_xor8:
                self.xor8_filter = _RustXor8Proxy(handle)
        finally:
            rffi.free_charp(fn_str)
            lltype.free(schema_buf, flavor="raw")

    def get_pk_u64(self, index):
        if self.count == 0 or index >= self.count or index < 0:
            return r_uint64(0)
        return r_uint64(engine_ffi._shard_get_pk_lo(
            self._handle, rffi.cast(rffi.INT, index)))

    def get_pk_u128(self, index):
        if self.count == 0 or index >= self.count or index < 0:
            return r_uint128(0)
        lo = r_uint64(engine_ffi._shard_get_pk_lo(
            self._handle, rffi.cast(rffi.INT, index)))
        hi = r_uint64(engine_ffi._shard_get_pk_hi(
            self._handle, rffi.cast(rffi.INT, index)))
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_pk_lo(self, index):
        return r_uint64(engine_ffi._shard_get_pk_lo(
            self._handle, rffi.cast(rffi.INT, index)))

    def get_pk_hi(self, index):
        return r_uint64(engine_ffi._shard_get_pk_hi(
            self._handle, rffi.cast(rffi.INT, index)))

    def get_weight(self, index):
        if self.count == 0 or index >= self.count or index < 0:
            return 0
        return intmask(engine_ffi._shard_get_weight(
            self._handle, rffi.cast(rffi.INT, index)))

    def get_null_word(self, index):
        return r_uint64(engine_ffi._shard_get_null_word(
            self._handle, rffi.cast(rffi.INT, index)))

    def get_col_ptr(self, row_idx, col_idx):
        stride = self.schema.columns[col_idx].field_type.size
        return engine_ffi._shard_col_ptr(
            self._handle,
            rffi.cast(rffi.INT, row_idx),
            rffi.cast(rffi.INT, col_idx),
            rffi.cast(rffi.INT, stride),
        )

    def get_blob_ptr(self):
        return self._blob_ptr

    def blob_len(self):
        return intmask(engine_ffi._shard_blob_len(self._handle))

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
        prefix = string_logic.compute_prefix(search_str)
        return string_logic.string_equals(
            ptr, self._blob_ptr, search_str, len(search_str), prefix
        )

    def find_row_index(self, key_lo, key_hi):
        """Binary search for the FIRST occurrence of an exact primary key."""
        return intmask(engine_ffi._shard_find_row(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        ))

    def find_lower_bound(self, key_lo, key_hi):
        """Binary search for the first row index where ShardKey >= key."""
        return intmask(engine_ffi._shard_lower_bound(
            self._handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        ))

    def close(self):
        if self._handle:
            engine_ffi._shard_close(self._handle)
            self._handle = NULL_HANDLE


class _RustXor8Proxy(Xor8Filter):
    """Delegates xor8 queries to the Rust shard reader's embedded filter.
    Does not own the filter — lifetime tied to the shard handle."""

    def __init__(self, shard_handle):
        Xor8Filter.__init__(self, lltype.nullptr(rffi.VOIDP.TO))
        self._shard_handle = shard_handle

    def may_contain(self, key_lo, key_hi):
        return bool(intmask(engine_ffi._shard_xor8_may_contain(
            self._shard_handle,
            rffi.cast(rffi.ULONGLONG, key_lo),
            rffi.cast(rffi.ULONGLONG, key_hi),
        )))

    def serialized_size(self):
        raise errors.StorageError("Cannot serialize shard-embedded XOR8 proxy")

    def serialize_into(self, dest_ptr, capacity):
        raise errors.StorageError("Cannot serialize shard-embedded XOR8 proxy")

    def free(self):
        pass
