# gnitz/catalog/engine.py

import os
import errno
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_uint64,
    r_int64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core.types import (
    ColumnDefinition,
    TableSchema,
    TYPE_U64,
    TYPE_STRING,
)
from gnitz.core.errors import LayoutError
from gnitz.core.batch import ZSetBatch
from gnitz.core.values import make_payload_row
from gnitz.core import strings as string_logic
from gnitz.storage.table import PersistentTable

from gnitz.catalog.identifiers import validate_user_identifier, parse_qualified_name
from gnitz.catalog.system_tables import (
    SYSTEM_SCHEMA_ID,
    PUBLIC_SCHEMA_ID,
    FIRST_USER_SCHEMA_ID,
    SYS_TABLE_SCHEMAS,
    SYS_TABLE_TABLES,
    SYS_TABLE_VIEWS,
    SYS_TABLE_COLUMNS,
    SYS_TABLE_INDICES,
    SYS_TABLE_VIEW_DEPS,
    SYS_TABLE_SEQUENCES,
    FIRST_USER_TABLE_ID,
    OWNER_KIND_TABLE,
    SEQ_ID_SCHEMAS,
    SEQ_ID_TABLES,
    SYS_CATALOG_DIRNAME,
    SYS_SUBDIR_SCHEMAS,
    SYS_SUBDIR_TABLES,
    SYS_SUBDIR_VIEWS,
    SYS_SUBDIR_COLUMNS,
    SYS_SUBDIR_INDICES,
    SYS_SUBDIR_VIEW_DEPS,
    SYS_SUBDIR_SEQUENCES,
    make_schemas_schema,
    make_tables_schema,
    make_views_schema,
    make_columns_schema,
    make_indices_schema,
    make_view_deps_schema,
    make_sequences_schema,
    pack_column_id,
    type_code_to_field_type,
)
from gnitz.catalog.registry import SystemTables, TableFamily, EntityRegistry


# ── Helpers ──────────────────────────────────────────────────────────────────


def _ensure_dir(path):
    """Ensures a directory exists, ignoring EEXIST."""
    try:
        rposix.mkdir(path, 0o755)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def _read_string(accessor, col_idx):
    """
    Extracts a Python str from a RowAccessor. Handles both MemTable and Shards.
    """
    length, prefix, struct_ptr, heap_ptr, py_string = accessor.get_str_struct(col_idx)
    if py_string is not None:
        return py_string
    null_ptr = lltype.nullptr(rffi.CCHARP.TO)
    if struct_ptr == null_ptr:
        return ""
    return string_logic.unpack_string(struct_ptr, heap_ptr)


def _append_schema_record(batch, schema, schema_id, name):
    row = make_payload_row(schema)
    row.append_string(name)
    batch.append(r_uint128(r_uint64(schema_id)), r_int64(1), row)


def _retract_schema_record(batch, schema, schema_id, name):
    row = make_payload_row(schema)
    row.append_string(name)
    batch.append(r_uint128(r_uint64(schema_id)), r_int64(-1), row)


def _append_table_record(
    batch, schema, table_id, schema_id, name, directory, pk_col_idx, created_lsn
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(schema_id)))
    row.append_string(name)
    row.append_string(directory)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(pk_col_idx)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(created_lsn)))
    batch.append(r_uint128(r_uint64(table_id)), r_int64(1), row)


def _retract_table_record(
    batch, schema, table_id, schema_id, name, directory, pk_col_idx, created_lsn
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(schema_id)))
    row.append_string(name)
    row.append_string(directory)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(pk_col_idx)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(created_lsn)))
    batch.append(r_uint128(r_uint64(table_id)), r_int64(-1), row)


def _append_column_record(
    batch,
    schema,
    owner_id,
    owner_kind,
    col_idx,
    name,
    type_code,
    is_nullable,
    fk_table_id,
    fk_col_idx,
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_kind)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(col_idx)))
    row.append_string(name)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(type_code)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(is_nullable)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_table_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_col_idx)))
    pk_val = pack_column_id(owner_id, col_idx)
    # batch.append takes r_uint128 logically, so we cast to the wider type 
    # for the Batch storage, but the Table logic will see it as U64.
    batch.append(r_uint128(pk_val), r_int64(1), row)


def _retract_column_record(
    batch,
    schema,
    owner_id,
    owner_kind,
    col_idx,
    name,
    type_code,
    is_nullable,
    fk_table_id,
    fk_col_idx,
):
    row = make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(owner_kind)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(col_idx)))
    row.append_string(name)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(type_code)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(is_nullable)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_table_id)))
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(fk_col_idx)))
    pk = pack_column_id(owner_id, col_idx)
    batch.append(pk, r_int64(-1), row)


# ── Internal Logic ───────────────────────────────────────────────────────────


def _create_sys_table(sys_dir, subdir, name, schema, table_id):
    directory = sys_dir + "/" + subdir
    return PersistentTable(directory, name, schema, table_id=table_id)

def _make_system_tables(sys_dir):
    return SystemTables(
        schemas=_create_sys_table(sys_dir, SYS_SUBDIR_SCHEMAS, "_schemas", make_schemas_schema(), SYS_TABLE_SCHEMAS),
        tables=_create_sys_table(sys_dir, SYS_SUBDIR_TABLES, "_tables", make_tables_schema(), SYS_TABLE_TABLES),
        views=_create_sys_table(sys_dir, SYS_SUBDIR_VIEWS, "_views", make_views_schema(), SYS_TABLE_VIEWS),
        columns=_create_sys_table(sys_dir, SYS_SUBDIR_COLUMNS, "_columns", make_columns_schema(), SYS_TABLE_COLUMNS),
        indices=_create_sys_table(sys_dir, SYS_SUBDIR_INDICES, "_indices", make_indices_schema(), SYS_TABLE_INDICES),
        view_deps=_create_sys_table(sys_dir, SYS_SUBDIR_VIEW_DEPS, "_view_deps", make_view_deps_schema(), SYS_TABLE_VIEW_DEPS),
        sequences=_create_sys_table(sys_dir, SYS_SUBDIR_SEQUENCES, "_sequences", make_sequences_schema(), SYS_TABLE_SEQUENCES),
    )


def _append_system_cols(batch, schema, table_id, column_defs):
    """Module level helper to avoid closure in bootstrap."""
    for i in range(len(column_defs)):
        col_name, field_type = column_defs[i]
        _append_column_record(
            batch, schema, table_id, OWNER_KIND_TABLE, i, col_name, field_type.code, 0, 0, 0
        )

def _bootstrap_system_tables(sys_tables, base_dir):
    sys_dir = base_dir + "/" + SYS_CATALOG_DIRNAME

    schemas_schema = sys_tables.schemas.schema
    tables_schema = sys_tables.tables.schema
    cols_schema = sys_tables.columns.schema
    seq_schema = sys_tables.sequences.schema

    # 1. Schema records
    schemas_batch = ZSetBatch(schemas_schema)
    _append_schema_record(schemas_batch, schemas_schema, SYSTEM_SCHEMA_ID, "_system")
    _append_schema_record(schemas_batch, schemas_schema, PUBLIC_SCHEMA_ID, "public")
    sys_tables.schemas.ingest_batch(schemas_batch)
    schemas_batch.free()

    # 2. Table records
    tables_batch = ZSetBatch(tables_schema)
    # Inline the directory logic to avoid closure
    _append_table_record(tables_batch, tables_schema, SYS_TABLE_SCHEMAS, SYSTEM_SCHEMA_ID, "_schemas", sys_dir + "/" + SYS_SUBDIR_SCHEMAS, 0, 0)
    _append_table_record(tables_batch, tables_schema, SYS_TABLE_TABLES, SYSTEM_SCHEMA_ID, "_tables", sys_dir + "/" + SYS_SUBDIR_TABLES, 0, 0)
    _append_table_record(tables_batch, tables_schema, SYS_TABLE_VIEWS, SYSTEM_SCHEMA_ID, "_views", sys_dir + "/" + SYS_SUBDIR_VIEWS, 0, 0)
    _append_table_record(tables_batch, tables_schema, SYS_TABLE_COLUMNS, SYSTEM_SCHEMA_ID, "_columns", sys_dir + "/" + SYS_SUBDIR_COLUMNS, 0, 0)
    _append_table_record(tables_batch, tables_schema, SYS_TABLE_INDICES, SYSTEM_SCHEMA_ID, "_indices", sys_dir + "/" + SYS_SUBDIR_INDICES, 0, 0)
    _append_table_record(tables_batch, tables_schema, SYS_TABLE_VIEW_DEPS, SYSTEM_SCHEMA_ID, "_view_deps", sys_dir + "/" + SYS_SUBDIR_VIEW_DEPS, 0, 0)
    _append_table_record(tables_batch, tables_schema, SYS_TABLE_SEQUENCES, SYSTEM_SCHEMA_ID, "_sequences", sys_dir + "/" + SYS_SUBDIR_SEQUENCES, 0, 0)
    sys_tables.tables.ingest_batch(tables_batch)
    tables_batch.free()

    # 3. Column records
    cols_batch = ZSetBatch(cols_schema)
    
    _append_system_cols(cols_batch, cols_schema, SYS_TABLE_SCHEMAS, [("schema_id", TYPE_U64), ("name", TYPE_STRING)])
    _append_system_cols(cols_batch, cols_schema, SYS_TABLE_TABLES, [
        ("table_id", TYPE_U64), ("schema_id", TYPE_U64), ("name", TYPE_STRING),
        ("directory", TYPE_STRING), ("pk_col_idx", TYPE_U64), ("created_lsn", TYPE_U64)
    ])
    _append_system_cols(cols_batch, cols_schema, SYS_TABLE_VIEWS, [
        ("view_id", TYPE_U64), ("schema_id", TYPE_U64), ("name", TYPE_STRING),
        ("sql_definition", TYPE_STRING), ("cache_directory", TYPE_STRING), ("created_lsn", TYPE_U64)
    ])
    _append_system_cols(cols_batch, cols_schema, SYS_TABLE_COLUMNS, [
        ("column_id", TYPE_U64), ("owner_id", TYPE_U64), ("owner_kind", TYPE_U64),
        ("col_idx", TYPE_U64), ("name", TYPE_STRING), ("type_code", TYPE_U64),
        ("is_nullable", TYPE_U64), ("fk_table_id", TYPE_U64), ("fk_col_idx", TYPE_U64)
    ])
    _append_system_cols(cols_batch, cols_schema, SYS_TABLE_INDICES, [
        ("index_id", TYPE_U64), ("owner_id", TYPE_U64), ("owner_kind", TYPE_U64),
        ("source_col_idx", TYPE_U64), ("name", TYPE_STRING), ("is_unique", TYPE_U64),
        ("cache_directory", TYPE_STRING)
    ])
    _append_system_cols(cols_batch, cols_schema, SYS_TABLE_VIEW_DEPS, [
        ("dep_id", TYPE_U64), ("view_id", TYPE_U64), ("dep_view_id", TYPE_U64), ("dep_table_id", TYPE_U64)
    ])
    _append_system_cols(cols_batch, cols_schema, SYS_TABLE_SEQUENCES, [
        ("seq_id", TYPE_U64), ("next_val", TYPE_U64)
    ])

    sys_tables.columns.ingest_batch(cols_batch)
    cols_batch.free()

    # 4. Sequence high-water marks
    seq_batch = ZSetBatch(seq_schema)
    row = make_payload_row(seq_schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(FIRST_USER_SCHEMA_ID - 1)))
    seq_batch.append(r_uint128(r_uint64(SEQ_ID_SCHEMAS)), r_int64(1), row)

    row = make_payload_row(seq_schema)
    row.append_int(rffi.cast(rffi.LONGLONG, r_uint64(FIRST_USER_TABLE_ID - 1)))
    seq_batch.append(r_uint128(r_uint64(SEQ_ID_TABLES)), r_int64(1), row)

    sys_tables.sequences.ingest_batch(seq_batch)
    seq_batch.free()

    # Flush all
    sys_tables.schemas.flush()
    sys_tables.tables.flush()
    sys_tables.views.flush()
    sys_tables.columns.flush()
    sys_tables.indices.flush()
    sys_tables.view_deps.flush()
    sys_tables.sequences.flush()


def _read_column_defs(cols_pt, owner_id):
    cursor = cols_pt.create_cursor()
    
    # We pass r_uint128 to seek(), but the underlying cursors 
    # will truncate to 8 bytes because the schema is TYPE_U64.
    start_key = r_uint128(pack_column_id(owner_id, 0))
    end_key = r_uint128(pack_column_id(owner_id + 1, 0))
    
    cursor.seek(start_key)

    col_defs = newlist_hint(8)
    while cursor.is_valid() and cursor.key() < end_key:
        acc = cursor.get_accessor()
        col_name = _read_string(acc, 4)
        type_code = intmask(acc.get_int(5))
        is_nullable_val = intmask(acc.get_int(6))
        field_type = type_code_to_field_type(type_code)
        is_nullable = is_nullable_val != 0
        col_defs.append(ColumnDefinition(field_type, is_nullable=is_nullable, name=col_name))
        cursor.advance()

    cursor.close()
    return col_defs


def _register_system_table(registry, pt, tbl_name, table_id, subdir, sys_dir):
    directory = sys_dir + "/" + subdir
    family = TableFamily("_system", tbl_name, table_id, SYSTEM_SCHEMA_ID, directory, 0, pt)
    registry.register(family)


def _rebuild_registry(sys_tables, base_dir, registry):
    # 1. Schemas
    cursor = sys_tables.schemas.create_cursor()
    while cursor.is_valid():
        acc = cursor.get_accessor()
        sid = intmask(r_uint64(cursor.key()))
        name = _read_string(acc, 1)
        registry.register_schema(sid, name)
        cursor.advance()
    cursor.close()

    # 2. System Table Families
    sys_dir = base_dir + "/" + SYS_CATALOG_DIRNAME
    _register_system_table(registry, sys_tables.schemas, "_schemas", SYS_TABLE_SCHEMAS, SYS_SUBDIR_SCHEMAS, sys_dir)
    _register_system_table(registry, sys_tables.tables, "_tables", SYS_TABLE_TABLES, SYS_SUBDIR_TABLES, sys_dir)
    _register_system_table(registry, sys_tables.views, "_views", SYS_TABLE_VIEWS, SYS_SUBDIR_VIEWS, sys_dir)
    _register_system_table(registry, sys_tables.columns, "_columns", SYS_TABLE_COLUMNS, SYS_SUBDIR_COLUMNS, sys_dir)
    _register_system_table(registry, sys_tables.indices, "_indices", SYS_TABLE_INDICES, SYS_SUBDIR_INDICES, sys_dir)
    _register_system_table(
        registry, sys_tables.view_deps, "_view_deps", SYS_TABLE_VIEW_DEPS, SYS_SUBDIR_VIEW_DEPS, sys_dir
    )
    _register_system_table(
        registry, sys_tables.sequences, "_sequences", SYS_TABLE_SEQUENCES, SYS_SUBDIR_SEQUENCES, sys_dir
    )

    # 3. Sequences (Counters)
    cursor = sys_tables.sequences.create_cursor()
    while cursor.is_valid():
        acc = cursor.get_accessor()
        seq_id = intmask(r_uint64(cursor.key()))
        val = intmask(acc.get_int(1))
        if seq_id == SEQ_ID_SCHEMAS:
            registry._next_schema_id = val + 1
        elif seq_id == SEQ_ID_TABLES:
            registry._next_table_id = val + 1
        cursor.advance()
    cursor.close()

    # 4. User Tables
    cursor = sys_tables.tables.create_cursor()
    while cursor.is_valid():
        tid = intmask(r_uint64(cursor.key()))
        if tid >= FIRST_USER_TABLE_ID:
            acc = cursor.get_accessor()
            sid = intmask(acc.get_int(1))
            tbl_name = _read_string(acc, 2)
            directory = _read_string(acc, 3)
            pk_col_idx = intmask(acc.get_int(4))

            col_defs = _read_column_defs(sys_tables.columns, tid)
            tbl_schema = TableSchema(col_defs, pk_col_idx)
            pt = PersistentTable(directory, tbl_name, tbl_schema, table_id=tid)

            schema_name = registry.get_schema_name(sid)
            family = TableFamily(schema_name, tbl_name, tid, sid, directory, pk_col_idx, pt)
            registry.register(family)
        cursor.advance()
    cursor.close()


# ── Public API ───────────────────────────────────────────────────────────────


class Engine(object):
    def __init__(self, base_dir, sys_tables, registry):
        self.base_dir = base_dir
        self.sys = sys_tables
        self.registry = registry

    def create_schema(self, name):
        validate_user_identifier(name)
        if self.registry.has_schema(name):
            raise LayoutError("Schema already exists: %s" % name)
        
        # Ensure schema root directory exists
        _ensure_dir(self.base_dir + "/" + name)

        sid = self.registry.allocate_schema_id()
        self._write_schema_record(sid, name)
        self.registry.register_schema(sid, name)

    def drop_schema(self, name):
        validate_user_identifier(name)
        if not self.registry.has_schema(name):
            raise LayoutError("Schema does not exist: %s" % name)
        if not self.registry.schema_is_empty(name):
            raise LayoutError("Cannot drop non-empty schema '%s'" % name)
        sid = self.registry.get_schema_id(name)
        self._retract_schema_record(sid, name)
        self.registry.unregister_schema(name, sid)

    def create_table(self, qualified_name, columns, pk_col_idx):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        validate_user_identifier(schema_name)
        validate_user_identifier(table_name)

        if not self.registry.has_schema(schema_name):
            raise LayoutError("Schema does not exist: %s" % schema_name)
        if self.registry.has(schema_name, table_name):
            raise LayoutError("Table already exists: %s" % qualified_name)

        # Ensure schema directory exists physically
        schema_dir = self.base_dir + "/" + schema_name
        _ensure_dir(schema_dir)

        tid = self.registry.allocate_table_id()
        self._advance_sequence(SEQ_ID_TABLES, tid - 1, tid)

        sid = self.registry.get_schema_id(schema_name)
        directory = schema_dir + "/" + table_name + "_" + str(tid)

        tbl_schema = TableSchema(columns, pk_col_idx)
        pt = PersistentTable(directory, table_name, tbl_schema, table_id=tid)

        self._write_column_records(tid, OWNER_KIND_TABLE, columns)
        self._write_table_record(tid, sid, table_name, directory, pk_col_idx, 0)

        family = TableFamily(schema_name, table_name, tid, sid, directory, pk_col_idx, pt)
        self.registry.register(family)
        return family

    def drop_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        if not self.registry.has(schema_name, table_name):
            raise LayoutError("Table does not exist: %s" % qualified_name)

        family = self.registry.get(schema_name, table_name)
        tid = family.table_id
        tbl_schema = family.schema

        self._retract_table_record(tid, family.schema_id, table_name, family.directory, family.pk_col_idx, family.created_lsn)
        self._retract_column_records(tid, OWNER_KIND_TABLE, tbl_schema.columns)

        family.close()
        self.registry.unregister(schema_name, table_name)

    def get_table(self, qualified_name):
        schema_name, table_name = parse_qualified_name(qualified_name, "public")
        return self.registry.get(schema_name, table_name)

    def close(self):
        # Collect keys to avoid iteration mutation
        keys = newlist_hint(len(self.registry._by_name))
        for k in self.registry._by_name:
            keys.append(k)

        for k in keys:
            family = self.registry._by_name[k]
            if family.table_id >= FIRST_USER_TABLE_ID:
                family.close()

        self.sys.close()

    # ── Private write helpers ────────────────────────────────────────────────

    def _write_schema_record(self, schema_id, name):
        s = self.sys.schemas.schema
        batch = ZSetBatch(s)
        _append_schema_record(batch, s, schema_id, name)
        self.sys.schemas.ingest_batch(batch)
        batch.free()

    def _retract_schema_record(self, schema_id, name):
        s = self.sys.schemas.schema
        batch = ZSetBatch(s)
        _retract_schema_record(batch, s, schema_id, name)
        self.sys.schemas.ingest_batch(batch)
        batch.free()

    def _write_table_record(self, table_id, schema_id, name, directory, pk_col_idx, created_lsn):
        s = self.sys.tables.schema
        batch = ZSetBatch(s)
        _append_table_record(batch, s, table_id, schema_id, name, directory, pk_col_idx, created_lsn)
        self.sys.tables.ingest_batch(batch)
        batch.free()

    def _retract_table_record(self, table_id, schema_id, name, directory, pk_col_idx, created_lsn):
        s = self.sys.tables.schema
        batch = ZSetBatch(s)
        _retract_table_record(batch, s, table_id, schema_id, name, directory, pk_col_idx, created_lsn)
        self.sys.tables.ingest_batch(batch)
        batch.free()

    def _write_column_records(self, owner_id, owner_kind, columns):
        s = self.sys.columns.schema
        batch = ZSetBatch(s)
        for i in range(len(columns)):
            col = columns[i]
            _append_column_record(batch, s, owner_id, owner_kind, i, col.name, col.field_type.code, int(col.is_nullable), 0, 0)
        self.sys.columns.ingest_batch(batch)
        batch.free()

    def _retract_column_records(self, owner_id, owner_kind, columns):
        s = self.sys.columns.schema
        batch = ZSetBatch(s)
        for i in range(len(columns)):
            col = columns[i]
            _retract_column_record(batch, s, owner_id, owner_kind, i, col.name, col.field_type.code, int(col.is_nullable), 0, 0)
        self.sys.columns.ingest_batch(batch)
        batch.free()

    def _advance_sequence(self, seq_id, old_val, new_val):
        s = self.sys.sequences.schema
        batch = ZSetBatch(s)
        # Retract old
        row_old = make_payload_row(s)
        row_old.append_int(rffi.cast(rffi.LONGLONG, r_uint64(old_val)))
        batch.append(r_uint128(r_uint64(seq_id)), r_int64(-1), row_old)
        # Append new
        row_new = make_payload_row(s)
        row_new.append_int(rffi.cast(rffi.LONGLONG, r_uint64(new_val)))
        batch.append(r_uint128(r_uint64(seq_id)), r_int64(1), row_new)

        self.sys.sequences.ingest_batch(batch)
        batch.free()


def open_engine(base_dir):
    # Ensure root directories exist
    _ensure_dir(base_dir)
    sys_dir = base_dir + "/" + SYS_CATALOG_DIRNAME
    _ensure_dir(sys_dir)
    
    # Ensure default public schema directory exists
    _ensure_dir(base_dir + "/public")

    sys_tables = _make_system_tables(sys_dir)

    # Detect first startup via empty _sequences table
    cursor = sys_tables.sequences.create_cursor()
    is_first = not cursor.is_valid()
    cursor.close()

    if is_first:
        _bootstrap_system_tables(sys_tables, base_dir)

    registry = EntityRegistry()
    _rebuild_registry(sys_tables, base_dir, registry)

    return Engine(base_dir, sys_tables, registry)
