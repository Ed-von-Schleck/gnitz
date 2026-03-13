# gnitz/dbsp/ops/group_index.py

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.comparator import RowAccessor
from gnitz.core import strings
from gnitz.core.types import (
    ColumnDefinition, TableSchema,
    TYPE_U128, TYPE_U64, TYPE_U32, TYPE_U16, TYPE_U8,
    TYPE_I64, TYPE_I32, TYPE_I16, TYPE_I8,
)


def make_group_idx_schema():
    """Schema for the reduce group secondary index: (ck: U128 [PK], spk_hi: I64)."""
    cols = newlist_hint(2)
    cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="ck"))
    cols.append(ColumnDefinition(TYPE_I64,  is_nullable=False, name="spk_hi"))
    return TableSchema(cols, pk_index=0)


def promote_group_col_to_u64(accessor, col_idx, col_type):
    """
    Reinterpret a ≤64-bit integer column value from accessor as r_uint64.
    Handles U8/U16/U32/U64/I8/I16/I32/I64 only.
    Caller must guard against U128/STRING/FLOAT before calling.
    """
    code = col_type.code
    if (code == TYPE_U64.code or code == TYPE_U32.code
            or code == TYPE_U16.code or code == TYPE_U8.code):
        return r_uint64(accessor.get_int(col_idx))
    elif code == TYPE_I64.code:
        # bit-reinterpret signed -> unsigned bit pattern
        return r_uint64(accessor.get_int(col_idx))
    else:
        # I32 / I16 / I8: sign-extend to 64-bit, then reinterpret as unsigned
        signed_64 = accessor.get_int_signed(col_idx)
        return r_uint64(rffi.cast(rffi.ULONGLONG, signed_64))


class GroupIdxAccessor(RowAccessor):
    """
    Reusable write accessor for group-index entries.
    The group index schema has one payload column (spk_hi: I64).
    Reused across all rows in one integrate call — no per-row allocation.
    """

    def __init__(self):
        self.spk_hi = r_int64(0)

    def is_null(self, col_idx):
        return False

    def get_int(self, col_idx):
        return r_uint64(rffi.cast(rffi.ULONGLONG, self.spk_hi))

    def get_int_signed(self, col_idx):
        return self.spk_hi

    def get_u128(self, col_idx):
        return r_uint128(rffi.cast(rffi.ULONGLONG, self.spk_hi))

    def get_float(self, col_idx):
        return 0.0

    def get_str_struct(self, col_idx):
        return (0, 0,
                lltype.nullptr(rffi.CCHARP.TO),
                lltype.nullptr(rffi.CCHARP.TO),
                None)

    def get_col_ptr(self, col_idx):
        return strings.NULL_PTR


class ReduceGroupIndex(object):
    """
    Immutable descriptor for a secondary index on a reduce operator's input trace.
    Keyed by (group_col_value << 64 | source_pk_lo) so rows in the same group
    are contiguous and can be found in O(log N + k) rather than O(N).
    """

    _immutable_fields_ = ["table", "col_idx", "col_type"]

    def __init__(self, table, col_idx, col_type):
        self.table = table      # EphemeralTable using make_group_idx_schema()
        self.col_idx = col_idx  # int: group column index in input_schema
        self.col_type = col_type  # FieldType of that column

    def create_cursor(self):
        return self.table.create_cursor()
