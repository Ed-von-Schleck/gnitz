# gnitz/catalog/system_tables.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_uint64, r_int64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import strings as string_logic
from gnitz.core.values import make_payload_row
from gnitz.core.types import (
    TYPE_U8, TYPE_I8, TYPE_U16, TYPE_I16, TYPE_U32, TYPE_I32, TYPE_F32,
    TYPE_U64, TYPE_I64, TYPE_F64, TYPE_STRING, TYPE_U128,
    ColumnDefinition, TableSchema,
)
from gnitz.core.errors import LayoutError

# Constants
SYSTEM_SCHEMA_ID = 1
PUBLIC_SCHEMA_ID = 2
FIRST_USER_SCHEMA_ID = 3

OWNER_KIND_TABLE = 0
OWNER_KIND_VIEW = 1

SEQ_ID_SCHEMAS, SEQ_ID_TABLES, SEQ_ID_INDICES, SEQ_ID_PROGRAMS = 1, 2, 3, 4
FIRST_USER_TABLE_ID, FIRST_USER_INDEX_ID = 16, 1
SYS_CATALOG_DIRNAME = "_system_catalog"


def read_string(accessor, col_idx):
    """Safely extracts a string from a RowAccessor, handling German String logic."""
    length, prefix, struct_ptr, heap_ptr, py_string = accessor.get_str_struct(col_idx)
    if py_string is not None:
        return py_string
    if rffi.cast(rffi.SIZE_T, struct_ptr) == 0:
        return ""
    return string_logic.unpack_string(struct_ptr, heap_ptr)


def pack_column_id(owner_id, col_idx):
    """Synthetic PK for ColTab: (owner_id << 9) | col_idx."""
    return (r_uint64(owner_id) << 9) | r_uint64(col_idx)


def pack_node_pk(view_id, node_id):
    return (r_uint128(r_uint64(view_id)) << 64) | r_uint128(r_uint64(node_id))


def pack_edge_pk(view_id, edge_id):
    return (r_uint128(r_uint64(view_id)) << 64) | r_uint128(r_uint64(edge_id))


def pack_param_pk(view_id, node_id, slot):
    # slot fits in 8 bits; node_id in 32 bits for headroom
    lo = (r_uint64(node_id) << 8) | r_uint64(slot)
    return (r_uint128(r_uint64(view_id)) << 64) | r_uint128(lo)


def pack_gcol_pk(view_id, node_id, col_idx):
    lo = (r_uint64(node_id) << 16) | r_uint64(col_idx)
    return (r_uint128(r_uint64(view_id)) << 64) | r_uint128(lo)


class BaseTab(object):
    ID = 0
    SUBDIR = ""
    NAME = ""

    @staticmethod
    def schema():
        return None


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

    @staticmethod
    def retract(batch, schema, sid, name):
        row = make_payload_row(schema)
        row.append_string(name)
        batch.append(r_uint128(r_uint64(sid)), r_int64(-1), row)


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

    @staticmethod
    def append(batch, schema, vid, sid, name, sql_def, cache_dir, lsn):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(sid)))
        row.append_string(name)
        row.append_string(sql_def)
        row.append_string(cache_dir)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(lsn)))
        batch.append(r_uint128(r_uint64(vid)), r_int64(1), row)

    @staticmethod
    def retract(batch, schema, vid, sid, name, sql_def, cache_dir, lsn):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(sid)))
        row.append_string(name)
        row.append_string(sql_def)
        row.append_string(cache_dir)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(lsn)))
        batch.append(r_uint128(r_uint64(vid)), r_int64(-1), row)


class ColTab(BaseTab):
    ID, SUBDIR, NAME = 4, "_columns", "_columns"
    (COL_OWNER_ID, COL_OWNER_KIND, COL_COL_IDX, COL_NAME, COL_TYPE_CODE,
     COL_IS_NULLABLE, COL_FK_TABLE_ID, COL_FK_COL_IDX) = 1, 2, 3, 4, 5, 6, 7, 8

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
    (COL_OWNER_ID, COL_OWNER_KIND, COL_SOURCE_COL_IDX, COL_NAME,
     COL_IS_UNIQUE, COL_CACHE_DIRECTORY) = 1, 2, 3, 4, 5, 6

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

    @staticmethod
    def append(batch, schema, dep_id, view_id, dep_vid, dep_tid):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(view_id)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(dep_vid)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(dep_tid)))
        batch.append(r_uint128(r_uint64(dep_id)), r_int64(1), row)

    @staticmethod
    def retract(batch, schema, dep_id, view_id, dep_vid, dep_tid):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(view_id)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(dep_vid)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(dep_tid)))
        batch.append(r_uint128(r_uint64(dep_id)), r_int64(-1), row)


class SeqTab(BaseTab):
    ID, SUBDIR, NAME = 7, "_sequences", "_sequences"
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


class CircuitNodesTab(BaseTab):
    """
    One row per operator node in a compiled view circuit.
    PK: pack_node_pk(view_id, node_id)
    """

    ID, SUBDIR, NAME = 11, "_circuit_nodes", "_circuit_nodes"
    COL_OPCODE = 1

    @staticmethod
    def schema():
        cols = newlist_hint(2)
        cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="node_pk"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="opcode"))
        return TableSchema(cols, pk_index=0)

    @staticmethod
    def append(batch, schema, view_id, node_id, opcode):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(opcode)))
        batch.append(pack_node_pk(view_id, node_id), r_int64(1), row)

    @staticmethod
    def retract(batch, schema, view_id, node_id, opcode):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(opcode)))
        batch.append(pack_node_pk(view_id, node_id), r_int64(-1), row)


class CircuitEdgesTab(BaseTab):
    """
    One row per directed edge (data stream) between operator nodes.
    PK: pack_edge_pk(view_id, edge_id)
    edge_id is assigned sequentially by CircuitBuilder.
    """

    ID, SUBDIR, NAME = 12, "_circuit_edges", "_circuit_edges"
    COL_SRC_NODE = 1
    COL_DST_NODE = 2
    COL_DST_PORT = 3

    @staticmethod
    def schema():
        cols = newlist_hint(4)
        cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="edge_pk"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="src_node"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="dst_node"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="dst_port"))
        return TableSchema(cols, pk_index=0)

    @staticmethod
    def append(batch, schema, view_id, edge_id, src_node, dst_node, dst_port):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(src_node)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(dst_node)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(dst_port)))
        batch.append(pack_edge_pk(view_id, edge_id), r_int64(1), row)

    @staticmethod
    def retract(batch, schema, view_id, edge_id, src_node, dst_node, dst_port):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(src_node)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(dst_node)))
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(dst_port)))
        batch.append(pack_edge_pk(view_id, edge_id), r_int64(-1), row)


class CircuitSourcesTab(BaseTab):
    """
    Leaf nodes that read from a persistent table (TraceRegisters).
    table_id=0 means the view's input delta stream (DeltaRegister).
    PK: pack_node_pk(view_id, node_id)
    """

    ID, SUBDIR, NAME = 13, "_circuit_sources", "_circuit_sources"
    COL_TABLE_ID = 1

    @staticmethod
    def schema():
        cols = newlist_hint(2)
        cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="source_pk"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="table_id"))
        return TableSchema(cols, pk_index=0)

    @staticmethod
    def append(batch, schema, view_id, node_id, table_id):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(table_id)))
        batch.append(pack_node_pk(view_id, node_id), r_int64(1), row)

    @staticmethod
    def retract(batch, schema, view_id, node_id, table_id):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(table_id)))
        batch.append(pack_node_pk(view_id, node_id), r_int64(-1), row)


class CircuitParamsTab(BaseTab):
    """
    Non-register scalar parameters for operator nodes.
    One row per (node, slot) pair.
    PK: pack_param_pk(view_id, node_id, slot)
    """

    ID, SUBDIR, NAME = 14, "_circuit_params", "_circuit_params"
    COL_VALUE = 1

    @staticmethod
    def schema():
        cols = newlist_hint(2)
        cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="param_pk"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="value"))
        return TableSchema(cols, pk_index=0)

    @staticmethod
    def append(batch, schema, view_id, node_id, slot, value):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(value)))
        batch.append(pack_param_pk(view_id, node_id, slot), r_int64(1), row)

    @staticmethod
    def retract(batch, schema, view_id, node_id, slot, value):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(value)))
        batch.append(pack_param_pk(view_id, node_id, slot), r_int64(-1), row)


class CircuitGroupColsTab(BaseTab):
    """
    Group-by column indices for REDUCE nodes.
    One row per (node, col_idx) pair. The PK encodes both.
    PK: pack_gcol_pk(view_id, node_id, col_idx)
    """

    ID, SUBDIR, NAME = 15, "_circuit_group_cols", "_circuit_group_cols"
    COL_COL_IDX = 1

    @staticmethod
    def schema():
        cols = newlist_hint(2)
        cols.append(ColumnDefinition(TYPE_U128, is_nullable=False, name="gcol_pk"))
        cols.append(ColumnDefinition(TYPE_U64, is_nullable=False, name="col_idx"))
        return TableSchema(cols, pk_index=0)

    @staticmethod
    def append(batch, schema, view_id, node_id, col_idx):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(col_idx)))
        batch.append(pack_gcol_pk(view_id, node_id, col_idx), r_int64(1), row)

    @staticmethod
    def retract(batch, schema, view_id, node_id, col_idx):
        row = make_payload_row(schema)
        row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(col_idx)))
        batch.append(pack_gcol_pk(view_id, node_id, col_idx), r_int64(-1), row)


def type_code_to_field_type(code):
    """Maps a numeric type code back to a FieldType instance."""
    types_list = [
        TYPE_U8, TYPE_I8, TYPE_U16, TYPE_I16, TYPE_U32, TYPE_I32,
        TYPE_F32, TYPE_U64, TYPE_I64, TYPE_F64, TYPE_STRING, TYPE_U128,
    ]
    for t in types_list:
        if code == t.code:
            return t
    raise LayoutError("Unknown type code: %d" % code)
