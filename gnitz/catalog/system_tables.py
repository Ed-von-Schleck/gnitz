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
# --- New for Phase 4 ---
SYS_TABLE_INSTRUCTIONS = 8
SYS_TABLE_FUNCTIONS = 9
SYS_TABLE_SUBSCRIPTIONS = 10
FIRST_USER_TABLE_ID = 11

# --- Sequence IDs for internal allocators ---
SEQ_ID_SCHEMAS = 1
SEQ_ID_TABLES = 2
SEQ_ID_INDICES = 3
SEQ_ID_PROGRAMS = 4

# --- First user-assigned IDs ---
FIRST_USER_INDEX_ID = 1

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
SYS_SUBDIR_INSTRUCTIONS = "_instructions"
SYS_SUBDIR_FUNCTIONS = "_functions"
SYS_SUBDIR_SUBSCRIPTIONS = "_subscriptions"


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
    cols.append(
        ColumnDefinition(TYPE_STRING, is_nullable=False, name="sql_definition")
    )
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
    cols.append(
        ColumnDefinition(TYPE_STRING, is_nullable=False, name="cache_directory")
    )
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


def make_instructions_schema():
    """
    Schema for the VM Instruction Z-Set.
    PK: instr_pk (U128) -> (program_id << 64) | instr_idx
    """
    cols = newlist_hint(21)
    # PK
    cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="instr_pk"))
    # Opcodes and Registers (Packed as U64 for RPython monomorphism)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="opcode"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_in"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_out"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_in_a"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_in_b"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_trace"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_trace_in"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_trace_out"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_delta"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_history"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_a"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_b"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="reg_key"))
    # Metadata / Refs
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="target_table_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="func_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="agg_func_id"))
    # Varlen Metadata
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="group_by_cols"))
    # Control Flow
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="chunk_limit"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="jump_target"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="yield_reason"))
    return TableSchema(cols, pk_index=0)


def make_functions_schema():
    cols = newlist_hint(3)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="func_id"))
    cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="func_type"))
    return TableSchema(cols, pk_index=0)


def make_subscriptions_schema():
    cols = newlist_hint(3)
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="sub_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="view_id"))
    cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="client_id"))
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

# ============================================================================
# ADDITIONS TO gnitz/catalog/system_tables.py
#
# Add these constant blocks immediately after each make_*_schema() factory
# function, in the same position order as the columns are declared.
#
# Convention: all constants are *schema* column indices (0-based, col 0 = PK).
# This matches the calling convention of every storage accessor:
#   RawWALAccessor._get_ptr  → schema.get_column_offset(col_idx)
#   PackedNodeAccessor._get_ptr → schema.get_column_offset(col_idx)
#   SoAAccessor._get_ptr     → view.get_col_ptr(row_idx, col_idx)
#
# The PK is always col 0 with physical offset -1 (not stored in the payload).
# Non-PK fields start at col 1.
# ============================================================================

# -- _schemas: col 0 = schema_id (PK) ----------------------------------------
SCHEMAS_COL_NAME           = 1   # VARCHAR  schema name

# -- _tables: col 0 = table_id (PK) ------------------------------------------
TABLES_COL_SCHEMA_ID       = 1   # UINT64   owning schema id
TABLES_COL_NAME            = 2   # VARCHAR  table name
TABLES_COL_DIRECTORY       = 3   # VARCHAR  on-disk directory path
TABLES_COL_PK_COL_IDX      = 4   # UINT32   schema column index of the PK
TABLES_COL_CREATED_LSN     = 5   # UINT64   LSN at creation time

# -- _columns: col 0 = col_pk (PK) -------------------------------------------
COLUMNS_COL_OWNER_ID       = 1   # UINT64   owning table_id
COLUMNS_COL_OWNER_KIND     = 2   # UINT8    OWNER_KIND_TABLE / OWNER_KIND_VIEW
COLUMNS_COL_COL_IDX        = 3   # UINT32   position in owning schema
COLUMNS_COL_NAME           = 4   # VARCHAR  column name
COLUMNS_COL_FIELD_TYPE_CODE = 5  # UINT16   FieldType.code
COLUMNS_COL_IS_NULLABLE    = 6   # UINT8    0 / 1
COLUMNS_COL_FK_TABLE_ID    = 7   # UINT64   FK target table_id (0 = none)
COLUMNS_COL_FK_COL_IDX     = 8   # UINT32   FK target column index

# -- _indices: col 0 = index_id (PK) -----------------------------------------
INDICES_COL_OWNER_ID       = 1   # UINT64   owning table_id
INDICES_COL_OWNER_KIND     = 2   # UINT8    OWNER_KIND_TABLE / OWNER_KIND_VIEW
INDICES_COL_SOURCE_COL_IDX = 3   # UINT32   indexed column (schema col idx)
INDICES_COL_NAME           = 4   # VARCHAR  index name
INDICES_COL_IS_UNIQUE      = 5   # UINT8    0 / 1
INDICES_COL_CACHE_DIRECTORY = 6  # VARCHAR  optional cache dir path

# -- _sequences: col 0 = seq_id (PK) -----------------------------------------
SEQUENCES_COL_VALUE        = 1   # UINT64   current high-water mark
