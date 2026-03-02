# gnitz/core/keys.py

import os
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128, r_int64
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
    r_uint128 key suitable for an index or Foreign Key validation.
    """
    code = source_col_type.code
    res = r_uint128(0)

    if code == TYPE_U128.code:
        res = accessor.get_u128(col_idx)

    elif code == TYPE_U64.code:
        res = r_uint128(accessor.get_int(col_idx))

    elif code == TYPE_U32.code or code == TYPE_U16.code or code == TYPE_U8.code:
        # get_int returns r_uint64, zero-extend to 128
        res = r_uint128(accessor.get_int(col_idx))

    elif code == TYPE_I64.code:
        # bit-reinterpret signed pattern to unsigned
        res = r_uint128(accessor.get_int(col_idx))

    elif code == TYPE_I32.code or code == TYPE_I16.code or code == TYPE_I8.code:
        # Sign-extend to 64 bits first, then cast to unsigned bit pattern
        signed_64 = accessor.get_int_signed(col_idx)
        res = r_uint128(rffi.cast(rffi.ULONGLONG, signed_64))

    elif code == TYPE_F32.code:
        # Reinterpret bit pattern (float as int) for consistent key sorting
        res = r_uint128(accessor.get_int(col_idx))

    elif code == TYPE_F64.code:
        res = r_uint128(accessor.get_int(col_idx))

    elif code == TYPE_STRING.code:
        # Secondary index on strings requires hashing or lexicographical packing,
        # which is currently restricted to primary keys and specialized views.
        raise LayoutError(
            "Secondary index on column type STRING not supported in Phase B"
        )
    else:
        raise LayoutError("Cannot promote unknown column type code %d" % code)

    # DIAGNOSTIC: Log promotion if it results in a non-zero key to trace validation logic.
    # Note: We split u128 into two u64 for RPython-safe formatting/printing.
    # if res != r_uint128(0):
    #     hi = r_uint64(res >> 64)
    #     lo = r_uint64(res)
    #     os.write(1, " [KEY-PROMO] Col %d (Type %d) -> %016lx%016lx\n" % (col_idx, code, hi, lo))

    return res
