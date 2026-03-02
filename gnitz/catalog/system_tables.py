from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_uint64, r_int64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import strings as string_logic
from gnitz.core.values import make_payload_row
from gnitz.core.types import (
    TYPE_U8, TYPE_I8, TYPE_U16, TYPE_I16, TYPE_U32, TYPE_I32, TYPE_F32,
    TYPE_U64, TYPE_I64, TYPE_F64, TYPE_STRING, TYPE_U128,
    ColumnDefinition, TableSchema
)
from gnitz.core.errors import LayoutError

# Constants
SYSTEM_SCHEMA_ID = 1
PUBLIC_SCHEMA_ID = 2
FIRST_USER_SCHEMA_ID = 3
OWNER_KIND_TABLE = 0
OWNER_KIND_VIEW = 1
SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES, SEQ_ID_PROGRAMS = 1, 2, 3, 4
FIRST_USER_TABLE_ID, FIRST_USER_INDEX_ID = 11, 1
SYS_CATALOG_DIRNAME = "_system_catalog"

def read_string(accessor, col_idx):
    length, prefix, struct_ptr, heap_ptr, py_string = accessor.get_str_struct(col_idx)
    if py_string is not None:
        return py_string
    if rffi.cast(rffi.SIZE_T, struct_ptr) == 0:
        return ""
    return string_logic.unpack_string(struct_ptr, heap_ptr)

def pack_column_id(owner_id, col_idx):
    return (r_uint64(owner_id) << 9) | r_uint64(col_idx)

class BaseTab(object):
    ID = 0
    SUBDIR = ""
    NAME = ""
    @staticmethod
    def schema(): return None

class SchemaTab(BaseTab):
    ID, SUBDIR, NAME = 1, "_schemas", "_schemas"
    COL_NAME = 1
    @staticmethod
    def schema():
        cols = newlist_hint(2)
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="schema_id"))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
        return TableSchema(cols, pk_index=0)
    @staticmethod
    def append(batch, schema, sid, name):
        row = make_payload_row(schema)
        row.append_string(name)
        batch.append(r_uint128(r_uint64(sid)), r_int64(1), row)

class TableTab(BaseTab):
    ID, SUBDIR, NAME = 2, "_tables", "_tables"
    COL_SCHEMA_ID, COL_NAME, COL_DIRECTORY, COL_PK_COL_IDX, COL_CREATED_LSN = 1, 2, 3, 4, 5
    @staticmethod
    def schema():
        cols = newlist_hint(6)
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="table_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="schema_id"))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="directory"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="pk_col_idx"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="created_lsn"))
        return TableSchema(cols, pk_index=0)
    @staticmethod
    def append(batch, schema, tid, sid, name, directory, pk_idx, lsn):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(sid)))
        row.append_string(name)
        row.append_string(directory)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(pk_idx)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(lsn)))
        batch.append(r_uint128(r_uint64(tid)), r_int64(1), row)
    @staticmethod
    def retract(batch, schema, tid, sid, name, directory, pk_idx, lsn):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(sid)))
        row.append_string(name)
        row.append_string(directory)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(pk_idx)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(lsn)))
        batch.append(r_uint128(r_uint64(tid)), r_int64(-1), row)

class ColTab(BaseTab):
    ID, SUBDIR, NAME = 4, "_columns", "_columns"
    COL_OWNER_ID, COL_OWNER_KIND, COL_COL_IDX, COL_NAME, COL_TYPE_CODE, COL_IS_NULLABLE, COL_FK_TABLE_ID, COL_FK_COL_IDX = 1, 2, 3, 4, 5, 6, 7, 8
    @staticmethod
    def schema():
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
    @staticmethod
    def append(batch, schema, owner_id, kind, idx, name, code, null, fk_tid, fk_cid):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_id)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(kind)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(idx)))
        row.append_string(name)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(code)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(null)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_tid)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_cid)))
        batch.append(r_uint128(pack_column_id(owner_id, idx)), r_int64(1), row)
    @staticmethod
    def retract(batch, schema, owner_id, kind, idx, name, code, null, fk_tid, fk_cid):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_id)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(kind)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(idx)))
        row.append_string(name)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(code)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(null)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_tid)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_cid)))
        batch.append(r_uint128(pack_column_id(owner_id, idx)), r_int64(-1), row)
    @staticmethod
    def append_system(batch, schema, tid, column_defs):
        for i in range(len(column_defs)):
            name, ftype = column_defs[i]
            ColTab.append(batch, schema, tid, OWNER_KIND_TABLE, i, name, ftype.code, 0, 0, 0)

class IdxTab(BaseTab):
    ID, SUBDIR, NAME = 5, "_indices", "_indices"
    COL_OWNER_ID, COL_OWNER_KIND, COL_SOURCE_COL_IDX, COL_NAME, COL_IS_UNIQUE, COL_CACHE_DIRECTORY = 1, 2, 3, 4, 5, 6
    @staticmethod
    def schema():
        cols = newlist_hint(7)
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="index_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="owner_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="owner_kind"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="source_col_idx"))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="is_unique"))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="cache_directory"))
        return TableSchema(cols, pk_index=0)
    @staticmethod
    def append(batch, schema, iid, oid, kind, src_idx, name, unique, cache_dir):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(oid)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(kind)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(src_idx)))
        row.append_string(name)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(unique)))
        row.append_string(cache_dir)
        batch.append(r_uint128(r_uint64(iid)), r_int64(1), row)
    @staticmethod
    def retract(batch, schema, iid, oid, kind, src_idx, name, unique, cache_dir):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(oid)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(kind)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(src_idx)))
        row.append_string(name)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(unique)))
        row.append_string(cache_dir)
        batch.append(r_uint128(r_uint64(iid)), r_int64(-1), row)

class ViewTab(BaseTab):
    ID, SUBDIR, NAME = 3, "_views", "_views"
    COL_SCHEMA_ID, COL_NAME, COL_SQL_DEFINITION, COL_CACHE_DIRECTORY, COL_CREATED_LSN = 1, 2, 3, 4, 5
    @staticmethod
    def schema():
        cols = newlist_hint(6)
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="view_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="schema_id"))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="sql_definition"))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="cache_directory"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="created_lsn"))
        return TableSchema(cols, pk_index=0)

class DepTab(BaseTab):
    ID, SUBDIR, NAME = 6, "_view_deps", "_view_deps"
    COL_VIEW_ID, COL_DEP_VIEW_ID, COL_DEP_TABLE_ID = 1, 2, 3
    @staticmethod
    def schema():
        cols = newlist_hint(4)
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="dep_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="view_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="dep_view_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="dep_table_id"))
        return TableSchema(cols, pk_index=0)

class SeqTab(BaseTab):
    ID, SUBDIR, NAME = 7, "_sequences" , "_sequences"
    COL_VALUE = 1
    @staticmethod
    def schema():
        cols = newlist_hint(2)
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="seq_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="next_val"))
        return TableSchema(cols, pk_index=0)
    @staticmethod
    def append(batch, schema, seq_id, val):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(val)))
        batch.append(r_uint128(r_uint64(seq_id)), r_int64(1), row)
    @staticmethod
    def retract(batch, schema, seq_id, val):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(val)))
        batch.append(r_uint128(r_uint64(seq_id)), r_int64(-1), row)

class InstrTab(BaseTab):
    ID, SUBDIR, NAME = 8, "_instructions", "_instructions"
    @staticmethod
    def schema():
        cols = newlist_hint(21)
        cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="instr_pk"))
        regs = ["opcode", "reg_in", "reg_out", "reg_in_a", "reg_in_b", "reg_trace", "reg_trace_in", "reg_trace_out", "reg_delta", "reg_history", "reg_a", "reg_b", "reg_key", "target_table_id", "func_id", "agg_func_id"]
        for r in regs: cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name=r))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="group_by_cols"))
        for r in ["chunk_limit", "jump_target", "yield_reason"]: cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name=r))
        return TableSchema(cols, pk_index=0)

class FuncTab(BaseTab):
    ID, SUBDIR, NAME = 9, "_functions", "_functions"
    @staticmethod
    def schema():
        cols = newlist_hint(3)
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="func_id"))
        cols.append(ColumnDefinition(TYPE_STRING, is_nullable=False, name="name"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="func_type"))
        return TableSchema(cols, pk_index=0)

class SubTab(BaseTab):
    ID, SUBDIR, NAME = 10, "_subscriptions", "_subscriptions"
    @staticmethod
    def schema():
        cols = newlist_hint(3)
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="sub_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="view_id"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="client_id"))
        return TableSchema(cols, pk_index=0)

def type_code_to_field_type(code):
    types = [TYPE_U8, TYPE_I8, TYPE_U16, TYPE_I16, TYPE_U32, TYPE_I32, TYPE_F32, TYPE_U64, TYPE_I64, TYPE_F64, TYPE_STRING, TYPE_U128]
    for t in types:
        if code == t.code: return t
    raise LayoutError("Unknown type code: %d" % code)
