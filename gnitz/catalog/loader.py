# gnitz/catalog/loader.py

import errno
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.jit import unrolling_iterable

from gnitz.core.types import (
    ColumnDefinition,
    TableSchema,
    TYPE_U64,
    TYPE_STRING,
    TYPE_U128,
)
from gnitz.core.batch import ZSetBatch
from gnitz.storage.table import PersistentTable

from gnitz.catalog import system_tables as sys
from gnitz.catalog.registry import (
    SystemTables,
    TableFamily,
    _wire_fk_constraints_for_family,
)
from gnitz.catalog.index_circuit import _make_index_circuit, _backfill_index


SYSTEM_TAB_LIST = unrolling_iterable([
    sys.SchemaTab,
    sys.TableTab,
    sys.ViewTab,
    sys.ColTab,
    sys.IdxTab,
    sys.DepTab,
    sys.SeqTab,
    sys.InstrTab,
    sys.FuncTab,
    sys.SubTab,
])


def _ensure_dir(path):
    """Ensures a directory exists, ignoring EEXIST."""
    try:
        rposix.mkdir(path, 0o755)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def _create_sys_table(sys_dir, tab_class):
    """Factory for persistent system tables using the Namespace class."""
    directory = sys_dir + "/" + tab_class.SUBDIR
    return PersistentTable(
        directory, tab_class.NAME, tab_class.schema(), table_id=tab_class.ID
    )


def _make_system_tables(base_dir):
    sys_dir = base_dir + "/" + sys.SYS_CATALOG_DIRNAME
    return SystemTables(
        schemas=_create_sys_table(sys_dir, sys.SchemaTab),
        tables=_create_sys_table(sys_dir, sys.TableTab),
        views=_create_sys_table(sys_dir, sys.ViewTab),
        columns=_create_sys_table(sys_dir, sys.ColTab),
        indices=_create_sys_table(sys_dir, sys.IdxTab),
        view_deps=_create_sys_table(sys_dir, sys.DepTab),
        sequences=_create_sys_table(sys_dir, sys.SeqTab),
        instructions=_create_sys_table(sys_dir, sys.InstrTab),
        functions=_create_sys_table(sys_dir, sys.FuncTab),
        subscriptions=_create_sys_table(sys_dir, sys.SubTab),
    )


def _bootstrap_system_tables(sys_tables, base_dir):
    sys_dir = base_dir + "/" + sys.SYS_CATALOG_DIRNAME
    _ensure_dir(sys_dir)

    # 1. Schema records
    schemas_batch = ZSetBatch(sys_tables.schemas.schema)
    sys.SchemaTab.append(
        schemas_batch, sys_tables.schemas.schema, sys.SYSTEM_SCHEMA_ID, "_system"
    )
    sys.SchemaTab.append(
        schemas_batch, sys_tables.schemas.schema, sys.PUBLIC_SCHEMA_ID, "public"
    )
    sys_tables.schemas.ingest_batch(schemas_batch)
    schemas_batch.free()

    # 2. Table records (Using unrolling_iterable to satisfy Typer)
    tables_batch = ZSetBatch(sys_tables.tables.schema)
    for t in SYSTEM_TAB_LIST:
        sys.TableTab.append(
            tables_batch,
            sys_tables.tables.schema,
            t.ID,
            sys.SYSTEM_SCHEMA_ID,
            t.NAME,
            sys_dir + "/" + t.SUBDIR,
            0,
            0,
        )
    sys_tables.tables.ingest_batch(tables_batch)
    tables_batch.free()

    # 3. Column records
    cols_batch = ZSetBatch(sys_tables.columns.schema)

    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.SchemaTab.ID,
        [("schema_id", TYPE_U64), ("name", TYPE_STRING)],
    )
    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.TableTab.ID,
        [
            ("table_id", TYPE_U64),
            ("schema_id", TYPE_U64),
            ("name", TYPE_STRING),
            ("directory", TYPE_STRING),
            ("pk_col_idx", TYPE_U64),
            ("created_lsn", TYPE_U64),
        ],
    )
    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.ViewTab.ID,
        [
            ("view_id", TYPE_U64),
            ("schema_id", TYPE_U64),
            ("name", TYPE_STRING),
            ("sql_definition", TYPE_STRING),
            ("cache_directory", TYPE_STRING),
            ("created_lsn", TYPE_U64),
        ],
    )
    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.ColTab.ID,
        [
            ("column_id", TYPE_U64),
            ("owner_id", TYPE_U64),
            ("owner_kind", TYPE_U64),
            ("col_idx", TYPE_U64),
            ("name", TYPE_STRING),
            ("type_code", TYPE_U64),
            ("is_nullable", TYPE_U64),
            ("fk_table_id", TYPE_U64),
            ("fk_col_idx", TYPE_U64),
        ],
    )
    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.IdxTab.ID,
        [
            ("index_id", TYPE_U64),
            ("owner_id", TYPE_U64),
            ("owner_kind", TYPE_U64),
            ("source_col_idx", TYPE_U64),
            ("name", TYPE_STRING),
            ("is_unique", TYPE_U64),
            ("cache_directory", TYPE_STRING),
        ],
    )
    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.DepTab.ID,
        [
            ("dep_id", TYPE_U64),
            ("view_id", TYPE_U64),
            ("dep_view_id", TYPE_U64),
            ("dep_table_id", TYPE_U64),
        ],
    )
    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.SeqTab.ID,
        [("seq_id", TYPE_U64), ("next_val", TYPE_U64)],
    )
    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.InstrTab.ID,
        [
            ("instr_pk", TYPE_U128),
            ("opcode", TYPE_U64),
            ("reg_in", TYPE_U64),
            ("reg_out", TYPE_U64),
            ("reg_in_a", TYPE_U64),
            ("reg_in_b", TYPE_U64),
            ("reg_trace", TYPE_U64),
            ("reg_trace_in", TYPE_U64),
            ("reg_trace_out", TYPE_U64),
            ("reg_delta", TYPE_U64),
            ("reg_history", TYPE_U64),
            ("reg_a", TYPE_U64),
            ("reg_b", TYPE_U64),
            ("reg_key", TYPE_U64),
            ("target_table_id", TYPE_U64),
            ("func_id", TYPE_U64),
            ("agg_func_id", TYPE_U64),
            ("group_by_cols", TYPE_STRING),
            ("chunk_limit", TYPE_U64),
            ("jump_target", TYPE_U64),
            ("yield_reason", TYPE_U64),
        ],
    )
    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.FuncTab.ID,
        [("func_id", TYPE_U64), ("name", TYPE_STRING), ("func_type", TYPE_U64)],
    )
    sys.ColTab.append_system(
        cols_batch,
        sys_tables.columns.schema,
        sys.SubTab.ID,
        [("sub_id", TYPE_U64), ("view_id", TYPE_U64), ("client_id", TYPE_U64)],
    )

    sys_tables.columns.ingest_batch(cols_batch)
    cols_batch.free()

    # 4. Sequence high-water marks
    seq_batch = ZSetBatch(sys_tables.sequences.schema)
    sys.SeqTab.append(
        seq_batch,
        sys_tables.sequences.schema,
        sys.SEQ_ID_SCHEMAS,
        sys.FIRST_USER_SCHEMA_ID - 1,
    )
    sys.SeqTab.append(
        seq_batch,
        sys_tables.sequences.schema,
        sys.SEQ_ID_TABLES,
        sys.FIRST_USER_TABLE_ID - 1,
    )
    sys.SeqTab.append(
        seq_batch,
        sys_tables.sequences.schema,
        sys.SEQ_ID_INDICES,
        sys.FIRST_USER_INDEX_ID - 1,
    )
    sys.SeqTab.append(
        seq_batch,
        sys_tables.sequences.schema,
        sys.SEQ_ID_PROGRAMS,
        sys.FIRST_USER_TABLE_ID - 1,
    )
    sys_tables.sequences.ingest_batch(seq_batch)
    seq_batch.free()

    # Durability flush
    sys_tables.schemas.flush()
    sys_tables.tables.flush()
    sys_tables.columns.flush()
    sys_tables.sequences.flush()


def _read_column_defs(cols_pt, owner_id):
    cursor = cols_pt.create_cursor()
    try:
        start_key = r_uint128(sys.pack_column_id(owner_id, 0))
        end_key = r_uint128(sys.pack_column_id(owner_id + 1, 0))

        cursor.seek(start_key)

        col_defs = newlist_hint(8)
        while cursor.is_valid() and cursor.key() < end_key:
            acc = cursor.get_accessor()

            col_defs.append(
                ColumnDefinition(
                    sys.type_code_to_field_type(
                        intmask(acc.get_int(sys.ColTab.COL_TYPE_CODE))
                    ),
                    is_nullable=intmask(acc.get_int(sys.ColTab.COL_IS_NULLABLE)) != 0,
                    name=sys.read_string(acc, sys.ColTab.COL_NAME),
                    fk_table_id=intmask(acc.get_int(sys.ColTab.COL_FK_TABLE_ID)),
                    fk_col_idx=intmask(acc.get_int(sys.ColTab.COL_FK_COL_IDX)),
                )
            )
            cursor.advance()
        return col_defs
    finally:
        cursor.close()


def _rebuild_registry(sys_tables, base_dir, registry):
    # 1. Schemas
    cursor = sys_tables.schemas.create_cursor()
    try:
        while cursor.is_valid():
            acc = cursor.get_accessor()
            sid = intmask(r_uint64(cursor.key()))
            name = sys.read_string(acc, sys.SchemaTab.COL_NAME)
            registry.register_schema(sid, name)
            cursor.advance()
    finally:
        cursor.close()

    # 2. System Table Families
    sys_dir = base_dir + "/" + sys.SYS_CATALOG_DIRNAME
    for pt, t in [
        (sys_tables.schemas, sys.SchemaTab),
        (sys_tables.tables, sys.TableTab),
        (sys_tables.views, sys.ViewTab),
        (sys_tables.columns, sys.ColTab),
        (sys_tables.indices, sys.IdxTab),
        (sys_tables.view_deps, sys.DepTab),
        (sys_tables.sequences, sys.SeqTab),
        (sys_tables.instructions, sys.InstrTab),
        (sys_tables.functions, sys.FuncTab),
        (sys_tables.subscriptions, sys.SubTab),
    ]:
        registry.register(
            TableFamily(
                "_system",
                t.NAME,
                t.ID,
                sys.SYSTEM_SCHEMA_ID,
                sys_dir + "/" + t.SUBDIR,
                0,
                pt,
            )
        )

    # 3. Sequences (counters)
    cursor = sys_tables.sequences.create_cursor()
    try:
        while cursor.is_valid():
            acc = cursor.get_accessor()
            seq_id = intmask(r_uint64(cursor.key()))
            val = intmask(acc.get_int(sys.SeqTab.COL_VALUE))
            if seq_id == sys.SEQ_ID_SCHEMAS:
                registry._next_schema_id = val + 1
            elif seq_id == sys.SEQ_ID_TABLES:
                registry._next_table_id = val + 1
            elif seq_id == sys.SEQ_ID_INDICES:
                registry._next_index_id = val + 1
            elif seq_id == sys.SEQ_ID_PROGRAMS:
                if val + 1 > registry._next_table_id:
                    registry._next_table_id = val + 1
            cursor.advance()
    finally:
        cursor.close()

    # 4. User Tables
    cursor = sys_tables.tables.create_cursor()
    try:
        while cursor.is_valid():
            tid = intmask(r_uint64(cursor.key()))
            if tid >= sys.FIRST_USER_TABLE_ID:
                acc = cursor.get_accessor()
                sid = intmask(acc.get_int(sys.TableTab.COL_SCHEMA_ID))
                tbl_name = sys.read_string(acc, sys.TableTab.COL_NAME)
                directory = sys.read_string(acc, sys.TableTab.COL_DIRECTORY)
                pk_col_idx = intmask(acc.get_int(sys.TableTab.COL_PK_COL_IDX))

                col_defs = _read_column_defs(sys_tables.columns, tid)
                tbl_schema = TableSchema(col_defs, pk_col_idx)
                pt = PersistentTable(directory, tbl_name, tbl_schema, table_id=tid)

                schema_name = registry.get_schema_name(sid)
                family = TableFamily(
                    schema_name, tbl_name, tid, sid, directory, pk_col_idx, pt
                )
                registry.register(family)
            cursor.advance()
    finally:
        cursor.close()

    # 4b. Wire Foreign Key constraints
    for k in registry._by_name:
        family = registry._by_name[k]
        if family.table_id >= sys.FIRST_USER_TABLE_ID:
            _wire_fk_constraints_for_family(family, registry)

    # 5. User Table Indices
    idx_cursor = sys_tables.indices.create_cursor()
    try:
        while idx_cursor.is_valid():
            idx_id = intmask(r_uint64(idx_cursor.key()))
            acc = idx_cursor.get_accessor()
            owner_id = intmask(acc.get_int(sys.IdxTab.COL_OWNER_ID))
            owner_kind = intmask(acc.get_int(sys.IdxTab.COL_OWNER_KIND))
            source_col_idx = intmask(acc.get_int(sys.IdxTab.COL_SOURCE_COL_IDX))
            idx_name = sys.read_string(acc, sys.IdxTab.COL_NAME)
            is_unique = intmask(acc.get_int(sys.IdxTab.COL_IS_UNIQUE)) != 0
            cache_dir = sys.read_string(acc, sys.IdxTab.COL_CACHE_DIRECTORY)

            if owner_kind == sys.OWNER_KIND_TABLE and registry.has_id(owner_id):
                family = registry.get_by_id(owner_id)
                if source_col_idx < len(family.schema.columns):
                    col = family.schema.columns[source_col_idx]
                    source_pk_type = family.schema.get_pk_column().field_type
                    idx_dir = family.directory + "/idx_" + str(idx_id)
                    circuit = _make_index_circuit(
                        idx_id,
                        owner_id,
                        source_col_idx,
                        col.field_type,
                        source_pk_type,
                        idx_dir,
                        idx_name,
                        is_unique,
                        cache_dir,
                    )
                    _backfill_index(circuit, family)
                    family.index_circuits.append(circuit)
                    registry.register_index(idx_id, idx_name, circuit)
            idx_cursor.advance()
    finally:
        idx_cursor.close()
