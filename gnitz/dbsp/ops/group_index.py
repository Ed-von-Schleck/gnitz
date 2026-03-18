# gnitz/dbsp/ops/group_index.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.comparator import RowAccessor
from gnitz.core import strings, xxh
from gnitz.core.types import (
    ColumnDefinition, TableSchema,
    TYPE_U128, TYPE_U64, TYPE_U32, TYPE_U16, TYPE_U8,
    TYPE_I64, TYPE_I32, TYPE_I16, TYPE_I8,
    TYPE_F32, TYPE_F64, TYPE_STRING,
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


# ---------------------------------------------------------------------------
# AggValueIndex: O(log N + 1) MIN/MAX lookup for all group types
# ---------------------------------------------------------------------------


def make_agg_value_idx_schema():
    """Schema for AggValueIndex: U128 PK only, no payload."""
    cols = newlist_hint(1)
    cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="ck"))
    return TableSchema(cols, pk_index=0)


def _mix64(v):
    """Murmur3 64-bit finalizer."""
    v = r_uint64(v)
    v ^= v >> 33
    v = r_uint64(v * r_uint64(0xFF51AFD7ED558CCD))
    v ^= v >> 33
    v = r_uint64(v * r_uint64(0xC4CEB9FE1A85EC53))
    v ^= v >> 33
    return v


@jit.unroll_safe
def _extract_group_key(accessor, schema, col_indices):
    """Computes a 128-bit key identifying the group."""
    if len(col_indices) == 1:
        c_idx = col_indices[0]
        t = schema.columns[c_idx].field_type.code
        if t == TYPE_U64.code or t == TYPE_I64.code:
            return r_uint128(accessor.get_int(c_idx))
        if t == TYPE_U128.code:
            return accessor.get_u128(c_idx)

    h = r_uint64(0x9E3779B97F4A7C15)  # golden ratio seed
    for i in range(len(col_indices)):
        c_idx = col_indices[i]
        t = schema.columns[c_idx].field_type.code
        if t == TYPE_STRING.code:
            length, _, struct_ptr, heap_ptr, py_string = accessor.get_str_struct(c_idx)
            if length == 0:
                col_hash = r_uint64(0)
            elif py_string is not None:
                col_hash = xxh.compute_checksum_bytes(py_string)
            else:
                if length <= strings.SHORT_STRING_THRESHOLD:
                    target_ptr = rffi.ptradd(struct_ptr, 4)
                else:
                    u64_p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(struct_ptr, 8))
                    heap_off = rffi.cast(lltype.Signed, u64_p[0])
                    target_ptr = rffi.ptradd(heap_ptr, heap_off)
                col_hash = xxh.compute_checksum(target_ptr, length)
        else:
            col_hash = _mix64(accessor.get_int(c_idx))
        h = _mix64(h ^ col_hash ^ r_uint64(i))

    h_hi = _mix64(h ^ r_uint64(len(col_indices)))
    return (r_uint128(h_hi) << 64) | r_uint128(h)


def _ieee_order_bits(raw_bits):
    """Convert IEEE 754 bit pattern to order-preserving unsigned key."""
    if raw_bits >> 63:
        return r_uint64(~intmask(raw_bits))
    else:
        return r_uint64(intmask(raw_bits) ^ intmask(r_uint64(1) << 63))


def promote_agg_col_to_u64_ordered(accessor, col_idx, col_type, for_max):
    """
    Encode aggregate column value as an order-preserving u64 key.
    U8-U64: zero-extend. I8-I64: offset-binary (+2^63). F32/F64: IEEE order.
    If for_max=True, complement bits so largest value has smallest key.
    """
    code = col_type.code
    raw = accessor.get_int(col_idx)
    if (code == TYPE_U64.code or code == TYPE_U32.code
            or code == TYPE_U16.code or code == TYPE_U8.code):
        val = r_uint64(raw)
    elif (code == TYPE_I64.code or code == TYPE_I32.code
            or code == TYPE_I16.code or code == TYPE_I8.code):
        val = r_uint64(intmask(raw) + intmask(r_uint64(1) << 63))
    elif code == TYPE_F64.code:
        val = _ieee_order_bits(raw)
    elif code == TYPE_F32.code:
        val = _ieee_order_bits(r_uint64(raw) & r_uint64(0xFFFFFFFF))
    else:
        val = r_uint64(raw)
    if for_max:
        return r_uint64(~intmask(val))
    return val


@jit.unroll_safe
def _extract_gc_u64(accessor, schema, group_by_cols):
    """
    Returns the 64-bit group key used in AggValueIndex composite keys.
    Single ≤64-bit int col: exact bit pattern (zero collision risk).
    All other cases: low 64 bits of _extract_group_key (~1/2^64 collision).
    """
    if len(group_by_cols) == 1:
        col_type = schema.columns[group_by_cols[0]].field_type
        if (col_type.code != TYPE_U128.code
                and col_type.code != TYPE_STRING.code
                and col_type.code != TYPE_F32.code
                and col_type.code != TYPE_F64.code):
            return promote_group_col_to_u64(accessor, group_by_cols[0], col_type)
    return r_uint64(intmask(_extract_group_key(accessor, schema, group_by_cols)))


class AggValueIndex(object):
    """
    Immutable descriptor for a secondary index on the aggregate value.
    Keyed by (gc_u64 << 64 | ordered_av_u64) so within each group,
    entries are in ascending order of the aggregate column value
    (or descending for MAX, since we complement the bits).
    Supports all group column types via _extract_gc_u64.
    """

    _immutable_fields_ = ["table", "input_schema",
                          "agg_col_idx", "agg_col_type", "for_max"]

    def __init__(self, table, group_by_cols, input_schema,
                 agg_col_idx, agg_col_type, for_max):
        self.table = table
        self.group_by_cols = group_by_cols[:]   # list[int]
        self.input_schema = input_schema     # TableSchema
        self.agg_col_idx = agg_col_idx
        self.agg_col_type = agg_col_type
        self.for_max = for_max

    def create_cursor(self):
        return self.table.create_cursor()


_UNIT_GI_ACC = GroupIdxAccessor()   # reused for AggValueIndex upserts (no payload cols)
