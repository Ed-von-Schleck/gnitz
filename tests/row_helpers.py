# tests/row_helpers.py

from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi
from gnitz.core import types, values

def create_test_row(schema, python_values):
    """
    Test utility to construct a PayloadRow from a list of Python values.
    
    Args:
        schema: The TableSchema of the target table.
        python_values: A list of values excluding the Primary Key.
                       Supported types: int, float, str, None, 
                       or (lo, hi) tuples for TYPE_U128.
    
    Returns:
        A fully populated PayloadRow object.
    """
    row = values.make_payload_row(schema)
    
    # Calculate expected number of non-PK columns
    expected_count = len(schema.columns) - 1
    if len(python_values) != expected_count:
        raise ValueError("create_test_row: expected %d values, got %d" % (
            expected_count, len(python_values)
        ))

    payload_idx = 0
    for i in range(len(schema.columns)):
        # Skip the Primary Key column as it is not part of the PayloadRow
        if i == schema.pk_index:
            continue
        
        val = python_values[payload_idx]
        col_type = schema.columns[i].field_type
        
        if val is None:
            row.append_null(payload_idx)
        elif col_type.code == types.TYPE_STRING.code:
            row.append_string(str(val))
        elif col_type.code == types.TYPE_U128.code:
            # Handle either an r_uint128 or a (lo, hi) tuple
            if isinstance(val, (tuple, list)):
                row.append_u128(r_uint64(val[0]), r_uint64(val[1]))
            else:
                u128_val = r_uint128(val)
                row.append_u128(r_uint64(u128_val), r_uint64(u128_val >> 64))
        elif col_type.code in (types.TYPE_F64.code, types.TYPE_F32.code):
            row.append_float(float(val))
        else:
            # Integer types: i8, i16, i32, i64, u8, u16, u32, u64
            # We use rffi.cast to LONGLONG to ensure Python ints map 
            # correctly to r_int64 without overflow/signedness conflicts.
            row.append_int(rffi.cast(rffi.LONGLONG, val))
        
        payload_idx += 1
        
    return row
