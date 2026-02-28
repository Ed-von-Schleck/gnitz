# gnitz/core/keys.py

from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi
from gnitz.core.types import (
    TYPE_U128,
    TYPE_U64,
    TYPE_I64,
    TYPE_I32,
    TYPE_I16,
    TYPE_I8,
    TYPE_U32,
    TYPE_U16,
    TYPE_U8,
    TYPE_F32,
    TYPE_F64,
    TYPE_STRING,
)
from gnitz.core.errors import LayoutError


def promote_to_index_key(accessor, col_idx, source_col_type):
    """
    Extracts a value from the source table and promotes it to a uniform
    r_uint128 key suitable for an index. Signed types are bit-reinterpreted.

    Note: For signed integers, the resulting bit pattern will sort
    negatives as larger than positives in unsigned comparisons.
    """
    code = source_col_type.code
    if code == TYPE_U128.code:
        return accessor.get_u128(col_idx)

    if code == TYPE_U64.code:
        return r_uint128(accessor.get_int(col_idx))

    if code == TYPE_U32.code or code == TYPE_U16.code or code == TYPE_U8.code:
        # get_int returns r_uint64, we zero-extend to 128
        return r_uint128(accessor.get_int(col_idx))

    if code == TYPE_I64.code:
        # get_int returns r_uint64 (the bit pattern), we zero-extend to 128
        return r_uint128(accessor.get_int(col_idx))

    if code == TYPE_I32.code or code == TYPE_I16.code or code == TYPE_I8.code:
        # Sign-extend to 64 bits first via get_int_signed, then cast to unsigned pattern
        signed_64 = accessor.get_int_signed(col_idx)  # returns r_int64
        return r_uint128(rffi.cast(rffi.ULONGLONG, signed_64))

    if (
        code == TYPE_F32.code
        or code == TYPE_F64.code
        or code == TYPE_STRING.code
    ):
        raise LayoutError(
            "Secondary index on column type %d not supported in Phase B" % code
        )

    raise LayoutError("Cannot promote unknown column type code %d to index key" % code)
