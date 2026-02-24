# gnitz/catalog/system_tables.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128

from gnitz.core.types import (
    TYPE_U8,
    TYPE_I8,
    TYPE_U16,
    TYPE_I16,
    TYPE_U32,
    TYPE_I32,
    TYPE_F32,
    TYPE_U64,
    TYPE_I64,
    TYPE_F64,
    TYPE_STRING,
    TYPE_U128,
    ColumnDefinition,
    TableSchema,
)
from gnitz.core.errors import LayoutError

# --- Schema IDs (Hardcoded) ---
SYSTEM_SCHEMA_ID = 1
PUBLIC_SCHEMA_ID = 2
FIRST_USER_SCHEMA_ID = 3

# --- System Table IDs (Hardcoded) ---
SYS_TABLE_SCHEMAS = 1
SYS_TABLE_TABLES = 2
SYS_TABLE_VIEWS = 3
SYS_TABLE_COLUMNS = 4
SYS_TABLE_INDICES = 5
SYS_TABLE_VIEW_DEPS = 6
SYS_TABLE_SEQUENCES = 7
FIRST_USER_TABLE_ID = 8

# --- Sequence IDs for internal allocators ---
SEQ_ID_SCHEMAS = 1
SEQ_ID_TABLES = 2

# --- Owner Kind Values ---
OWNER_KIND_TABLE = 0
OWNER_KIND_VIEW = 1

# --- Physical Directory Names ---
SYS_CATALOG_DIRNAME = "_system_catalog"
SYS_SUBDIR_SCHEMAS = "_schemas"
SYS_SUBDIR_TABLES = "_tables"
SYS_SUBDIR_VIEWS = "_views"
SYS_SUBDIR_COLUMNS = "_columns"
SYS_SUBDIR_INDICES = "_indices"
SYS_SUBDIR_VIEW_DEPS = "_view_deps"
SYS_SUBDIR_SEQUENCES = "_sequences"


# ---------------------------------------------------------------------------
# Schema Factory Functions
# ---------------------------------------------------------------------------


def make_schemas_schema():
    cols = newlist_hint(2)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="schema_id"))
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
    return TableSchema(cols, pk_index=0)


def make_tables_schema():
    cols = newlist_hint(6)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="table_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="schema_id"))
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="directory"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="pk_col_idx"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="created_lsn"))
    return TableSchema(cols, pk_index=0)


def make_views_schema():
    cols = newlist_hint(6)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="view_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="schema_id"))
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="sql_definition"))
    cols.append(
        ColumnDefinition(TYPE_STRING, is_nullable=False, name="cache_directory")
    )
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="created_lsn"))
    return TableSchema(cols, pk_index=0)


def make_columns_schema():
    cols = newlist_hint(9)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="column_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="owner_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="owner_kind"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="col_idx"))
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="type_code"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="is_nullable"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="fk_table_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="fk_col_idx"))
    return TableSchema(cols, pk_index=0)


def make_indices_schema():
    cols = newlist_hint(7)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="index_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="owner_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="owner_kind"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="source_col_idx"))
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="is_unique"))
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="cache_directory"))
    return TableSchema(cols, pk_index=0)


def make_view_deps_schema():
    cols = newlist_hint(4)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="dep_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="view_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="dep_view_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="dep_table_id"))
    return TableSchema(cols, pk_index=0)


def make_sequences_schema():
    cols = newlist_hint(2)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="seq_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="next_val"))
    return TableSchema(cols, pk_index=0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def pack_column_id(owner_id, col_idx):
    """
    Packs owner_id and col_idx into a 64-bit key.
    Calculated as (owner_id << 9) | col_idx.
    Matches the TYPE_U64 Primary Key of the _columns table.
    """
    # owner_id (max ~10^6) << 9 fits easily in 64-bit.
    val = (r_uint64(owner_id) << 9) | r_uint64(col_idx)
    return val


def type_code_to_field_type(code):
    """
    Maps integer type codes back to FieldType singletons.
    Flat if-chain avoids type-annotation ambiguity in RPython.
    """
    if code == TYPE_U8.code:
        return TYPE_U8
    if code == TYPE_I8.code:
        return TYPE_I8
    if code == TYPE_U16.code:
        return TYPE_U16
    if code == TYPE_I16.code:
        return TYPE_I16
    if code == TYPE_U32.code:
        return TYPE_U32
    if code == TYPE_I32.code:
        return TYPE_I32
    if code == TYPE_F32.code:
        return TYPE_F32
    if code == TYPE_U64.code:
        return TYPE_U64
    if code == TYPE_I64.code:
        return TYPE_I64
    if code == TYPE_F64.code:
        return TYPE_F64
    if code == TYPE_STRING.code:
        return TYPE_STRING
    if code == TYPE_U128.code:
        return TYPE_U128
    raise LayoutError("Unknown type code: %d" % code)
