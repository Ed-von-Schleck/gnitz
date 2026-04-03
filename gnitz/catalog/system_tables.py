# gnitz/catalog/system_tables.py

from rpython.rlib.rarithmetic import r_uint64, r_int64, intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import strings as string_logic, errors
from gnitz.core import comparator as core_comparator
from gnitz.core.types import (
    TYPE_U8, TYPE_I8, TYPE_U16, TYPE_I16, TYPE_U32, TYPE_I32, TYPE_F32,
    TYPE_U64, TYPE_I64, TYPE_F64, TYPE_STRING, TYPE_U128,
)


# ---------------------------------------------------------------------------
# RowBuilder — direct columnar row construction
# ---------------------------------------------------------------------------


class RowBuilder(core_comparator.RowAccessor):
    _immutable_fields_ = ["_schema", "_pk_index", "_n", "_has_nullable"]

    def __init__(self, schema, target):
        self._schema = schema
        self._target = target
        self._pk_index = schema.pk_index
        n = schema.n_payload
        self._n = n
        self._has_nullable = schema.has_nullable
        self._lo = [r_int64(0)] * n
        self._hi = [r_uint64(0)] * n if schema.has_u128 else None
        self._strs = [""] * n if schema.has_string else None
        self._null_word = r_uint64(0)
        self._pk_lo = r_uint64(0)
        self._pk_hi = r_uint64(0)
        self._weight = r_int64(0)
        self._curr = 0

    def begin(self, pk_lo, pk_hi, weight):
        self._pk_lo = rffi.cast(rffi.ULONGLONG, pk_lo)
        self._pk_hi = rffi.cast(rffi.ULONGLONG, pk_hi)
        self._weight = weight
        self._curr = 0
        self._null_word = r_uint64(0)

    def put_int(self, val_i64):
        self._lo[self._curr] = val_i64
        self._curr += 1

    def put_float(self, val_f64):
        self._lo[self._curr] = float2longlong(val_f64)
        self._curr += 1

    def put_string(self, val_str):
        self._lo[self._curr] = r_int64(0)
        if self._strs is not None:
            self._strs[self._curr] = val_str
        self._curr += 1

    def put_u128(self, lo_u64, hi_u64):
        self._lo[self._curr] = r_int64(intmask(lo_u64))
        if self._hi is not None:
            self._hi[self._curr] = hi_u64
        self._curr += 1

    def put_null(self):
        self._lo[self._curr] = r_int64(0)
        self._null_word = self._null_word | (r_uint64(1) << self._curr)
        self._curr += 1

    def commit(self):
        self._target.append_from_accessor(self._pk_lo, self._pk_hi, self._weight, self)

    def _check_overflow(self):
        if self._curr >= self._n:
            raise errors.LayoutError(
                "Map function attempted to write too many columns "
                "(Schema expects %d non-PK columns)" % self._n
            )

    def append_int(self, val):
        self._check_overflow()
        self._lo[self._curr] = val
        self._curr += 1

    def append_float(self, val_f64):
        self._check_overflow()
        self._lo[self._curr] = float2longlong(val_f64)
        self._curr += 1

    def append_string(self, val_str):
        self._check_overflow()
        self._lo[self._curr] = r_int64(0)
        if self._strs is not None:
            self._strs[self._curr] = val_str
        self._curr += 1

    def append_u128(self, lo_u64, hi_u64):
        self._check_overflow()
        self._lo[self._curr] = r_int64(intmask(lo_u64))
        if self._hi is not None:
            self._hi[self._curr] = hi_u64
        self._curr += 1

    def append_null(self, payload_col_idx):
        self._check_overflow()
        if payload_col_idx != self._curr:
            raise errors.LayoutError(
                "Out-of-order column append detected: expected payload col %d, got %d"
                % (self._curr, payload_col_idx)
            )
        self._lo[self._curr] = r_int64(0)
        self._null_word |= r_uint64(1) << payload_col_idx
        self._curr += 1

    # RowAccessor read interface

    def _payload_idx(self, col_idx):
        if col_idx == self._pk_index:
            return -1
        if col_idx < self._pk_index:
            return col_idx
        return col_idx - 1

    def is_null(self, col_idx):
        if col_idx == self._pk_index:
            return False
        if not self._has_nullable:
            return False
        return bool(self._null_word & (r_uint64(1) << self._payload_idx(col_idx)))

    def get_int(self, col_idx):
        if col_idx == self._pk_index:
            return self._pk_lo
        return r_uint64(self._lo[self._payload_idx(col_idx)])

    def get_int_signed(self, col_idx):
        return rffi.cast(rffi.LONGLONG, r_uint64(self._lo[self._payload_idx(col_idx)]))

    def get_float(self, col_idx):
        return longlong2float(self._lo[self._payload_idx(col_idx)])

    def get_u128_lo(self, col_idx):
        p_idx = self._payload_idx(col_idx)
        return r_uint64(self._lo[p_idx])

    def get_u128_hi(self, col_idx):
        p_idx = self._payload_idx(col_idx)
        return self._hi[p_idx] if self._hi is not None else r_uint64(0)

    def get_str_struct(self, col_idx):
        s = self._strs[self._payload_idx(col_idx)] if self._strs is not None else ""
        prefix = rffi.cast(lltype.Signed, string_logic.compute_prefix(s))
        return (
            len(s),
            prefix,
            lltype.nullptr(rffi.CCHARP.TO),
            lltype.nullptr(rffi.CCHARP.TO),
            s,
        )

    def get_col_ptr(self, col_idx):
        return lltype.nullptr(rffi.CCHARP.TO)


# Constants
SYSTEM_SCHEMA_ID = 1
PUBLIC_SCHEMA_ID = 2
FIRST_USER_SCHEMA_ID = 3

SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES, SEQ_ID_PROGRAMS = 1, 2, 3, 4
FIRST_USER_TABLE_ID, FIRST_USER_INDEX_ID = 16, 1


def type_code_to_field_type(code):
    """Maps a numeric type code back to a FieldType instance."""
    types_list = [
        TYPE_U8, TYPE_I8, TYPE_U16, TYPE_I16, TYPE_U32, TYPE_I32,
        TYPE_F32, TYPE_U64, TYPE_I64, TYPE_F64, TYPE_STRING, TYPE_U128,
    ]
    for t in types_list:
        if code == t.code:
            return t
    raise errors.LayoutError("Unknown type code: %d" % code)
