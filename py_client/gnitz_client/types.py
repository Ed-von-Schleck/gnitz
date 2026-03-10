"""Type system, schemas, and system table constants."""

from dataclasses import dataclass, field


class TypeCode:
    U8 = 1
    I8 = 2
    U16 = 3
    I16 = 4
    U32 = 5
    I32 = 6
    F32 = 7
    U64 = 8
    I64 = 9
    F64 = 10
    STRING = 11
    U128 = 12


# IPC wire stride per type (STRING uses 8 on wire: u32 offset + u32 length)
TYPE_STRIDES: dict[int, int] = {
    1: 1, 2: 1, 3: 2, 4: 2, 5: 4, 6: 4, 7: 4,
    8: 8, 9: 8, 10: 8, 11: 8, 12: 16,
}

# struct format chars for fixed-width types
TYPE_STRUCT_FMT: dict[int, str] = {
    1: "B", 2: "b", 3: "H", 4: "h", 5: "I", 6: "i", 7: "f",
    8: "Q", 9: "q", 10: "d",
}


@dataclass
class ColumnDef:
    name: str
    type_code: int
    is_nullable: bool = False


@dataclass
class Schema:
    columns: list[ColumnDef]
    pk_index: int


# Meta-schema: the schema ZSet has this fixed 4-column layout
META_SCHEMA = Schema(
    columns=[
        ColumnDef("col_idx", TypeCode.U64),
        ColumnDef("type_code", TypeCode.U64),
        ColumnDef("flags", TypeCode.U64),
        ColumnDef("name", TypeCode.STRING),
    ],
    pk_index=0,
)

# System table IDs
SCHEMA_TAB = 1
TABLE_TAB = 2
VIEW_TAB = 3
COL_TAB = 4
IDX_TAB = 5
DEP_TAB = 6
SEQ_TAB = 7

# Sequence IDs
SEQ_ID_SCHEMAS = 1
SEQ_ID_TABLES = 2

# Owner kind
OWNER_KIND_TABLE = 0

# System table schemas (must exactly match system_tables.py)

SCHEMA_TAB_SCHEMA = Schema(
    columns=[
        ColumnDef("schema_id", TypeCode.U64),
        ColumnDef("name", TypeCode.STRING),
    ],
    pk_index=0,
)

TABLE_TAB_SCHEMA = Schema(
    columns=[
        ColumnDef("table_id", TypeCode.U64),
        ColumnDef("schema_id", TypeCode.U64),
        ColumnDef("name", TypeCode.STRING),
        ColumnDef("directory", TypeCode.STRING),
        ColumnDef("pk_col_idx", TypeCode.U64),
        ColumnDef("created_lsn", TypeCode.U64),
    ],
    pk_index=0,
)

COL_TAB_SCHEMA = Schema(
    columns=[
        ColumnDef("column_id", TypeCode.U64),
        ColumnDef("owner_id", TypeCode.U64),
        ColumnDef("owner_kind", TypeCode.U64),
        ColumnDef("col_idx", TypeCode.U64),
        ColumnDef("name", TypeCode.STRING),
        ColumnDef("type_code", TypeCode.U64),
        ColumnDef("is_nullable", TypeCode.U64),
        ColumnDef("fk_table_id", TypeCode.U64),
        ColumnDef("fk_col_idx", TypeCode.U64),
    ],
    pk_index=0,
)

SEQ_TAB_SCHEMA = Schema(
    columns=[
        ColumnDef("seq_id", TypeCode.U64),
        ColumnDef("next_val", TypeCode.U64),
    ],
    pk_index=0,
)
