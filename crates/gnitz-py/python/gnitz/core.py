# System table IDs — must match the server constants in gnitz/catalog/system_tables.py
SCHEMA_TAB           = 1
TABLE_TAB            = 2
VIEW_TAB             = 3
COL_TAB              = 4
IDX_TAB              = 5
DEP_TAB              = 6
SEQ_TAB              = 7
FIRST_USER_TABLE_ID  = 16
FIRST_USER_SCHEMA_ID = 3

# Packed PK/index column-list codec — must match gnitz-wire/src/catalog.rs.
# A catalog column-list u64 (TABLE_TAB.pk_col_idx, IDX_TAB.source_cols) is
# either a bare single column index (flag bit clear) or a packed list:
# bit 63 = PK_LIST_PACKED_FLAG, bits [0..4) = count, then one 7-bit column
# index per entry starting at bit 4 + 7*i.
PK_LIST_PACKED_FLAG = 1 << 63


def unpack_pk_cols(v):
    """Decode a packed column-list u64 into a list of column indices."""
    if v & PK_LIST_PACKED_FLAG == 0:
        return [v]                      # bare single index
    n = v & 0xF
    return [(v >> (4 + 7 * i)) & 0x7F for i in range(n)]
